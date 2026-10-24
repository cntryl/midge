use super::*;
use crate::sst::data_block::DataBlockLayout;

fn unique_reader(directory: &tempfile::TempDir) -> MidgeResult<SstFileIo> {
    let fs = crate::io::real::test_root(directory.path());
    let factory = crate::sst::FsSstFactoryIo::new(fs.clone(), 64 * 1024);
    let mut writer = factory.create()?;
    for index in 0..512_u64 {
        writer.add_sorted_with_meta(
            format!("restart/{index:04}").as_bytes(),
            Some(b"value"),
            index + 1,
            EntryType::Put,
            None,
        )?;
    }
    crate::sst::fs::finish_writer_to_path(writer, &directory.path().join("restarts.sst"))?;
    SstFileIo::open("restarts.sst", fs)
}

#[test]
fn should_bound_unique_key_point_decoding_by_binary_restart_search_and_one_interval(
) -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let reader = unique_reader(&directory)?;
    assert_eq!(reader.index_entries()?.len(), 1);
    // Act and Assert
    for index in [0, 15, 16, 17, 255, 500, 511] {
        let (state, stats) = reader
            .get_raw_state_at_with_stats(format!("restart/{index:04}").as_bytes(), u64::MAX)?;
        assert!(matches!(state, KeyState::Value(_, sequence, None, _) if sequence == index + 1));
        assert!(
            stats.entries_decoded <= 16 + 7,
            "index={index} decoded={}",
            stats.entries_decoded
        );
    }
    Ok(())
}

#[test]
fn should_preserve_old_snapshot_tombstone_and_ttl_versions_across_restarts_in_both_directions(
) -> MidgeResult<()> {
    // Arrange: the target begins mid-interval and spans eight restarts.
    let directory = tempfile::tempdir()?;
    let fs = crate::io::real::test_root(directory.path());
    let factory = crate::sst::FsSstFactoryIo::new(fs.clone(), 64 * 1024);
    let mut writer = factory.create()?;
    for index in 0..8 {
        writer.add_sorted_with_meta(
            format!("a/{index}").as_bytes(),
            Some(b"before"),
            1,
            EntryType::Put,
            None,
        )?;
    }
    for sequence in (73..=200_u64).rev() {
        let deleted = sequence == 90;
        writer.add_sorted_with_meta(
            b"b/target",
            (!deleted).then_some(b"version".as_slice()),
            sequence,
            if deleted {
                EntryType::Delete
            } else {
                EntryType::Put
            },
            (sequence == 80).then_some(100),
        )?;
    }
    writer.add_sorted_with_meta(b"c/after", Some(b"after"), 1, EntryType::Put, None)?;
    crate::sst::fs::finish_writer_to_path(writer, &directory.path().join("versions.sst"))?;
    let reader = Arc::new(SstFileIo::open("versions.sst", fs)?);

    // Act and Assert: a restart on the same user key must not skip its head.
    let (visible, stats) = reader.get_raw_state_at_with_stats(b"b/target", 80)?;
    assert!(matches!(visible, KeyState::Value(_, 80, Some(100), _)));
    assert!(stats.entries_decoded <= 128 + 16 + 6);
    assert!(matches!(
        reader.get_state_at_with_time(b"b/target", 80, 101)?,
        KeyState::Tombstone(80)
    ));
    assert!(matches!(
        reader.get_state_at_with_time(b"b/target", 90, 1)?,
        KeyState::Tombstone(90)
    ));
    for reverse in [false, true] {
        let rows: Vec<_> = reader
            .state_scan(Some(b"b".to_vec()), Some(b"d".to_vec()), reverse, 85, 1)
            .collect::<MidgeResult<_>>()?;
        assert_eq!(rows.len(), 2);
        let target = rows
            .iter()
            .find(|(key, _)| key.as_ref() == b"b/target")
            .unwrap();
        assert!(matches!(target.1, KeyState::Value(_, 85, None, _)));
        assert_eq!(rows[usize::from(!reverse)].0.as_ref(), b"c/after");
    }
    Ok(())
}

#[test]
fn should_reject_malformed_restart_counts_offsets_and_entry_boundaries() -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let reader = unique_reader(&directory)?;
    let handle = reader.index_entries()?[0].1;
    let block = reader.read_block(&handle)?;
    let layout = DataBlockLayout::parse(&block)?;
    let mut malformed = Vec::new();
    for count in [0, u32::MAX] {
        let mut bytes = block.to_vec();
        let end = bytes.len();
        bytes[end - 4..].copy_from_slice(&count.to_le_bytes());
        malformed.push(bytes);
    }
    for offset in [0_u32, 1, u32::try_from(layout.data_end).unwrap()] {
        let mut bytes = block.to_vec();
        bytes[layout.data_end + 4..layout.data_end + 8].copy_from_slice(&offset.to_le_bytes());
        malformed.push(bytes);
    }
    // Act and Assert: full decoding visits and validates every boundary.
    for bytes in malformed {
        assert!(reader.decode_scan_block(&Bytes::from(bytes)).is_err());
    }
    Ok(())
}
