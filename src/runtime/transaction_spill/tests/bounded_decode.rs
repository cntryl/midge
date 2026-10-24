use super::*;

#[test]
fn should_retain_decoded_spill_memory_until_last_value_clone_is_released() -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let pool = Arc::new(TransactionMemoryPool::new(0));
    let mut writes = TransactionWriteSet::new(pool, &root_at(directory.path()), false, 946);
    writes.push(put(b"held", &vec![7; 64 * 1024]))?;
    let source = writes.take_source();
    let memory = ResourceBudget::new(source.streaming_memory_bound()?);
    let mut retained = None;

    // Act: retain the production decoder's Bytes beyond the streamed visit.
    source.for_each_bounded(Some(&memory), |_, op| {
        let TransactionOp::Put { value, .. } = op else {
            panic!("expected value");
        };
        retained = Some(value.clone());
        assert!(memory.used() >= value.len());
        Ok(())
    })?;

    // Assert: closing the reader and returning from iteration cannot release
    // bytes still owned by a caller, and the final clone settles the charge.
    assert_eq!(retained.as_ref().map(Bytes::len), Some(64 * 1024));
    assert!(retained.as_ref().unwrap().iter().all(|byte| *byte == 7));
    assert!(memory.used() >= 64 * 1024);
    assert!(memory.peak() <= memory.limit());
    drop(retained);
    assert_eq!(memory.used(), 0);
    Ok(())
}

#[test]
fn should_reject_decoded_spill_allocation_when_only_reader_buffer_fits() -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let pool = Arc::new(TransactionMemoryPool::new(0));
    let mut writes = TransactionWriteSet::new(pool, &root_at(directory.path()), false, 947);
    writes.push(put(b"large", &vec![9; 64 * 1024]))?;
    let source = writes.take_source();
    let memory = ResourceBudget::new(20 * 1024);
    let mut visits = 0;

    // Act
    let result = source.for_each_bounded(Some(&memory), |_, _| {
        visits += 1;
        Ok(())
    });

    // Assert: no operation escapes a failed admission, source files remain
    // available for retry, and every temporary reservation is settled.
    assert!(matches!(result, Err(MidgeError::ResourceLimit(_))));
    assert_eq!(visits, 0);
    assert_eq!(memory.used(), 0);
    assert!(memory.peak() <= memory.limit());
    assert!(host_path(&source.runs[0].root, &source.runs[0].path).exists());
    source.for_each_bounded(
        Some(&ResourceBudget::new(source.streaming_memory_bound()?)),
        |_, _| Ok(()),
    )?;
    Ok(())
}

#[test]
fn should_reject_changed_spill_frame_length_before_budgeted_payload_allocation() -> MidgeResult<()>
{
    // Arrange: the private source's accepted size bound survives handoff.
    let directory = tempfile::tempdir()?;
    let mut writes = TransactionWriteSet::new(
        Arc::new(TransactionMemoryPool::new(0)),
        &root_at(directory.path()),
        false,
        948,
    );
    writes.push(put(b"key", b"small"))?;
    let source = writes.take_source();
    let run = &source.runs[0];
    let path = host_path(&run.root, &run.path);
    let mut bytes = std::fs::read(&path)?;
    bytes[RUN_HEADER_LEN..RUN_HEADER_LEN + 4]
        .copy_from_slice(&u32::try_from(run.frame_limit + 1).unwrap().to_le_bytes());
    std::fs::write(&path, bytes)?;
    let memory = ResourceBudget::new(17 * 1024);

    // Act
    let result = source.for_each_bounded(Some(&memory), |_, _| panic!("corrupt operation escaped"));

    // Assert: corruption, rather than a larger allocation, is observed.
    assert!(matches!(result, Err(MidgeError::Corruption(_))));
    assert_eq!(memory.used(), 0);
    assert!(memory.peak() <= memory.limit());
    assert!(path.exists());
    Ok(())
}

#[test]
fn should_bound_range_lookup_memory_when_many_overlapping_nodes_are_streamed() -> MidgeResult<()> {
    // Arrange: real balanced range-tree bytes, not an abstract traversal.
    let directory = tempfile::tempdir()?;
    let mut ops = (0..2048_u64)
        .map(|ordinal| OrdinalOp {
            ordinal,
            op: delete_range(format!("{ordinal:04}").as_bytes(), b"zzzz"),
        })
        .collect::<Vec<_>>();
    let run = write_run(&SpillRoot(root_at(directory.path())), 949, 0, &mut ops)?;
    let memory = ResourceBudget::new(64 * 1024);
    let mut latest = None;

    // Act
    format::lookup_run_key_bounded(&run, b"middle", u64::MAX, &mut latest, Some(&memory))?;

    // Assert: all applicable history participates without retaining its keys.
    assert!(matches!(latest, Some((2047, IntentLookup::Deleted))));
    assert_eq!(memory.used(), 0);
    assert!(memory.peak() <= memory.limit());
    Ok(())
}

#[test]
fn should_reject_range_child_cycle_without_recursive_memory_growth() -> MidgeResult<()> {
    // Arrange: preserve valid frame CRC while making the root its own child.
    let directory = tempfile::tempdir()?;
    let mut ops = vec![OrdinalOp {
        ordinal: 0,
        op: delete_range(b"alpha", b"omega"),
    }];
    let run = write_run(&SpillRoot(root_at(directory.path())), 950, 0, &mut ops)?;
    let mut file = RunFile::open(&*run.root, &run.range_path)?;
    let header = read_range_header(&mut file)?;
    drop(file);
    let path = host_path(&run.root, &run.range_path);
    let mut bytes = std::fs::read(&path)?;
    let offset = usize::try_from(header.node_section_offset).unwrap();
    let length = usize::try_from(format::read_u32_at(&bytes, offset)?).unwrap();
    bytes[offset + 16..offset + 24].copy_from_slice(&0_u64.to_le_bytes());
    let crc = crc32c::crc32c(&bytes[offset + 8..offset + 8 + length]);
    bytes[offset + 4..offset + 8].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(&path, bytes)?;
    let memory = ResourceBudget::new(64 * 1024);

    // Act
    let result =
        format::lookup_run_key_bounded(&run, b"middle", u64::MAX, &mut None, Some(&memory));

    // Assert
    assert!(matches!(result, Err(MidgeError::Corruption(_))));
    assert_eq!(memory.used(), 0);
    assert!(memory.peak() <= memory.limit());
    assert!(path.exists());
    Ok(())
}
