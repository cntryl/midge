use super::*;

fn reader_with_duplicate_successor(directory: &tempfile::TempDir) -> MidgeResult<SstFileIo> {
    let fs = Arc::new(crate::io::RealFs::new(directory.path()).map_err(FsError::into_midge)?);
    let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
    let mut writer = factory.create()?;
    for index in 0..128_u64 {
        let key = format!("tenant/shared/key/{index:04}");
        writer.add_with_meta(
            key.as_bytes(),
            Some(b"filler"),
            index + 1,
            EntryType::Put,
            None,
        )?;
    }
    writer.add_with_meta(
        b"tenant/shared/key/0064-before",
        Some(b"retained"),
        900,
        EntryType::Put,
        None,
    )?;
    let successor = b"tenant/shared/key/0064-target";
    for sequence in 1..=32_u64 {
        writer.add_with_meta(
            successor,
            Some(&vec![u8::try_from(sequence).unwrap(); 8192]),
            sequence + 1000,
            EntryType::Put,
            None,
        )?;
    }
    crate::sst::fs::finish_writer_to_path(writer, &directory.path().join("successor.sst"))?;
    let reader = SstFileIo::open(
        "successor.sst",
        Arc::new(crate::io::RealFs::new(directory.path()).map_err(FsError::into_midge)?),
    )?;
    assert_eq!(reader.index_kind, IndexKind::Trie);
    assert!(
        reader
            .index_entries()?
            .iter()
            .filter(|(key, _)| key.as_slice() == successor)
            .count()
            >= 3
    );
    Ok(reader)
}

#[test]
fn should_find_point_before_duplicate_successor_boundaries() -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let reader = reader_with_duplicate_successor(&directory)?;

    // Act
    let result = reader.get_state_at(b"tenant/shared/key/0064-before", u64::MAX)?;

    // Assert
    assert!(matches!(result, KeyState::Value(value, 900, _, _) if value.as_ref() == b"retained"));
    Ok(())
}

#[test]
fn should_scan_prefix_before_duplicate_successor_boundaries() -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let reader = reader_with_duplicate_successor(&directory)?;

    // Act
    let rows = reader.scan_range_raw_state(
        Some(b"tenant/shared/key/0064-before"),
        Some(b"tenant/shared/key/0064-beforf"),
    )?;

    // Assert
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0.as_ref(), b"tenant/shared/key/0064-before");
    Ok(())
}
