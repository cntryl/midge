use super::*;

fn put(key: &[u8], value: &[u8]) -> TransactionOp {
    TransactionOp::Put {
        cf_id: 0,
        key: Bytes::copy_from_slice(key),
        value: Bytes::copy_from_slice(value),
        ttl_seconds: None,
        insert_only: false,
    }
}

fn delete_range(start: &[u8], end: &[u8]) -> TransactionOp {
    TransactionOp::DeleteRange {
        cf_id: 0,
        start_key: Bytes::copy_from_slice(start),
        end_key: Bytes::copy_from_slice(end),
    }
}

#[test]
fn should_reject_corrupt_data_frame_when_reading_spill_run() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir()?;
    let mut ops = vec![OrdinalOp {
        ordinal: 0,
        op: put(b"key", b"value"),
    }];
    let run = write_run(temp.path(), 1, 0, &mut ops)?;
    let mut bytes = fs::read(&run.path)?;
    bytes[RUN_HEADER_LEN + 8] ^= 0x55;
    fs::write(&run.path, bytes)?;

    // Act
    let result = for_each_run_ordinal(&run, |_| Ok(()));

    // Assert
    assert!(matches!(result, Err(MidgeError::Corruption(_))));
    Ok(())
}

#[test]
fn should_reject_corrupt_sparse_index_when_reading_spill_run() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir()?;
    let mut ops = vec![OrdinalOp {
        ordinal: 0,
        op: put(b"key", b"value"),
    }];
    let run = write_run(temp.path(), 1, 0, &mut ops)?;
    let mut file = File::open(&run.path)?;
    let header = read_header(&mut file)?;
    drop(file);
    let mut bytes = fs::read(&run.path)?;
    let corrupt_at = usize::try_from(header.sparse_index_offset)
        .map_err(|_| MidgeError::Corruption("index offset exceeds usize".to_string()))?
        + 8;
    bytes[corrupt_at] ^= 0x55;
    fs::write(&run.path, bytes)?;

    // Act
    let result = for_each_run_ordinal(&run, |_| Ok(()));

    // Assert
    assert!(matches!(result, Err(MidgeError::Corruption(_))));
    Ok(())
}

#[test]
fn should_resolve_range_tombstone_before_ordinal_ceiling() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir()?;
    let mut ops = vec![
        OrdinalOp {
            ordinal: 0,
            op: put(b"middle", b"before"),
        },
        OrdinalOp {
            ordinal: 1,
            op: delete_range(b"alpha", b"omega"),
        },
        OrdinalOp {
            ordinal: 2,
            op: put(b"middle", b"after"),
        },
    ];
    let run = write_run(temp.path(), 1, 0, &mut ops)?;

    // Act
    let mut before_final_put = None;
    lookup_run_key(&run, b"middle", 2, &mut before_final_put)?;
    let mut after_final_put = None;
    lookup_run_key(&run, b"middle", 3, &mut after_final_put)?;

    // Assert
    assert!(matches!(before_final_put, Some((1, IntentLookup::Deleted))));
    assert!(matches!(
        after_final_put,
        Some((2, IntentLookup::Present(value))) if value == Bytes::from_static(b"after")
    ));
    Ok(())
}

#[test]
fn should_reject_corrupt_range_index_when_reading_spill_run() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir()?;
    let mut ops = vec![OrdinalOp {
        ordinal: 0,
        op: delete_range(b"alpha", b"omega"),
    }];
    let run = write_run(temp.path(), 1, 0, &mut ops)?;
    let mut file = File::open(&run.range_path)?;
    let header = read_range_header(&mut file)?;
    drop(file);
    let mut bytes = fs::read(&run.range_path)?;
    let corrupt_at = usize::try_from(header.node_section_offset)
        .map_err(|_| MidgeError::Corruption("range offset exceeds usize".to_string()))?
        + 8;
    bytes[corrupt_at] ^= 0x55;
    fs::write(&run.range_path, bytes)?;

    // Act
    let result = lookup_run_key(&run, b"middle", u64::MAX, &mut None);

    // Assert
    assert!(matches!(result, Err(MidgeError::Corruption(_))));
    Ok(())
}

#[test]
fn should_stream_large_spill_record_with_wal_sized_bound() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir()?;
    let value = vec![b'x'; 2 * 1024 * 1024];
    let mut ops = vec![OrdinalOp {
        ordinal: 0,
        op: put(b"large", &value),
    }];

    // Act
    let run = write_run(temp.path(), 1, 0, &mut ops)?;
    let mut read_value = None;
    for_each_run_ordinal(&run, |ordinal_op| {
        if let TransactionOp::Put { value, .. } = ordinal_op.op {
            read_value = Some(value);
        }
        Ok(())
    })?;

    // Assert
    assert_eq!(read_value.as_ref().map(Bytes::len), Some(value.len()));
    Ok(())
}

#[test]
fn should_scan_spill_keys_in_both_directions_across_sparse_chunks() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir()?;
    let mut ops = (0_u64..40)
        .map(|ordinal| OrdinalOp {
            ordinal,
            op: put(format!("key-{ordinal:02}").as_bytes(), b"value"),
        })
        .collect::<Vec<_>>();
    let run = write_run(temp.path(), 1, 0, &mut ops)?;

    // Act
    let forward = RunKeyCursor::new(&run, Some(b"key-10"), Some(b"key-20"), false)?
        .collect::<MidgeResult<Vec<_>>>()?;
    let reverse = RunKeyCursor::new(&run, Some(b"key-10"), Some(b"key-20"), true)?
        .collect::<MidgeResult<Vec<_>>>()?;

    // Assert
    let expected = (10_u64..20)
        .map(|ordinal| Bytes::from(format!("key-{ordinal:02}")))
        .collect::<Vec<_>>();
    assert_eq!(forward, expected);
    assert_eq!(reverse, expected.into_iter().rev().collect::<Vec<_>>());
    Ok(())
}

#[test]
fn should_surface_late_spill_corruption_from_key_cursor_item() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir()?;
    let mut ops = vec![
        OrdinalOp {
            ordinal: 0,
            op: put(b"alpha", b"one"),
        },
        OrdinalOp {
            ordinal: 1,
            op: put(b"bravo", b"two"),
        },
    ];
    let run = write_run(temp.path(), 1, 0, &mut ops)?;
    let mut file = File::open(&run.path)?;
    file.seek(SeekFrom::Start(RUN_HEADER_LEN as u64))?;
    let (_, second_offset) = read_op_frame(&mut file)?;
    drop(file);
    let mut bytes = fs::read(&run.path)?;
    let corrupt_at = u64_to_usize(second_offset)?.saturating_add(8);
    bytes[corrupt_at] ^= 0x55;
    fs::write(&run.path, bytes)?;
    let mut scan = RunKeyCursor::new(&run, None, None, false)?;

    // Act
    let first = scan.next().transpose()?;
    let late = scan.next().expect("second item must report corruption");

    // Assert
    assert_eq!(first, Some(Bytes::from_static(b"alpha")));
    assert!(matches!(late, Err(MidgeError::Corruption(_))));
    Ok(())
}
