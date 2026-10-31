use super::*;

const VERSIONS: u64 = 8192;

fn value_for(sequence: u64) -> Vec<u8> {
    format!("value-{sequence:08}").into_bytes()
}

fn version_heavy_reader(directory: &tempfile::TempDir) -> MidgeResult<SstFileIo> {
    let fs = crate::io::real::test_root(directory.path());
    let factory = crate::sst::FsSstFactoryIo::new(fs.clone(), 4096);
    let mut writer = factory.create()?;
    writer.add_sorted_with_meta(b"a/before", Some(b"before"), 1, EntryType::Put, None)?;
    for sequence in (1..=VERSIONS).rev() {
        // Every 97th version is a tombstone so runs mix both kinds.
        if sequence.is_multiple_of(97) {
            writer.add_sorted_with_meta(b"m/hot", None, sequence, EntryType::Delete, None)?;
        } else {
            let value = value_for(sequence);
            writer.add_sorted_with_meta(b"m/hot", Some(&value), sequence, EntryType::Put, None)?;
        }
    }
    writer.add_sorted_with_meta(b"z/after", Some(b"after"), 1, EntryType::Put, None)?;
    crate::sst::fs::finish_writer_to_path(writer, &directory.path().join("versions.sst"))?;
    SstFileIo::open("versions.sst", fs)
}

fn expected_state(sequence: u64) -> KeyState {
    if sequence.is_multiple_of(97) {
        KeyState::Tombstone(sequence)
    } else {
        KeyState::Value(
            Bytes::from(value_for(sequence)),
            sequence,
            None,
            EntryType::Put,
        )
    }
}

#[test]
fn should_read_only_the_first_block_when_the_latest_version_is_visible() -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let reader = version_heavy_reader(&directory)?;
    let blocks = reader.index_entries()?.len();
    assert!(blocks > 32, "fixture must span many blocks, got {blocks}");

    // Act
    let (state, read_stats) = reader.get_raw_state_at_with_stats(b"m/hot", u64::MAX)?;

    // Assert: the predecessor block plus the key's first block at most.
    assert_eq!(state, expected_state(VERSIONS));
    assert!(
        read_stats.blocks_read <= 3,
        "latest version read {} blocks of {blocks}",
        read_stats.blocks_read
    );
    Ok(())
}

#[test]
fn should_return_exact_versions_when_snapshots_select_middle_and_oldest_versions() -> MidgeResult<()>
{
    // Arrange
    let directory = tempfile::tempdir()?;
    let reader = version_heavy_reader(&directory)?;

    for snapshot in [VERSIONS, VERSIONS - 1, VERSIONS / 2, 97, 98, 2, 1] {
        // Act
        let (state, _) = reader.get_raw_state_at_with_stats(b"m/hot", snapshot)?;

        // Assert
        assert_eq!(state, expected_state(snapshot), "snapshot {snapshot}");
    }
    assert_eq!(
        reader.get_raw_state_at_with_stats(b"m/hot", 0)?.0,
        KeyState::Absent
    );
    assert_eq!(
        reader.get_raw_state_at_with_stats(b"z/after", u64::MAX)?.0,
        KeyState::Value(Bytes::from_static(b"after"), 1, None, EntryType::Put)
    );
    Ok(())
}

fn straddling_duplicate_reader(
    directory: &tempfile::TempDir,
    second: &[u8],
) -> MidgeResult<SstFileIo> {
    let fs = crate::io::real::test_root(directory.path());
    let factory = crate::sst::FsSstFactoryIo::new(fs.clone(), 4096);
    let mut writer = factory.create()?;
    let first = vec![b'x'; 3000];
    writer.add_sorted_with_meta(b"dup", Some(&first), 100, EntryType::Put, None)?;
    writer.add_sorted_with_meta(b"dup", Some(second), 100, EntryType::Put, None)?;
    writer.add_sorted_with_meta(b"dup", Some(b"older"), 99, EntryType::Put, None)?;
    crate::sst::fs::finish_writer_to_path(writer, &directory.path().join("dup.sst"))?;
    let reader = SstFileIo::open("dup.sst", fs)?;
    assert!(
        reader.index_entries()?.len() >= 2,
        "duplicates must straddle a block boundary"
    );
    Ok(reader)
}

#[test]
fn should_report_conflicting_duplicates_when_equal_sequences_straddle_a_block_boundary(
) -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let reader = straddling_duplicate_reader(&directory, &vec![b'y'; 3000])?;

    // Act
    let result = reader.get_raw_state_at_with_stats(b"dup", u64::MAX);

    // Assert
    assert!(
        matches!(result, Err(MidgeError::Corruption(_))),
        "{result:?}"
    );
    Ok(())
}

#[test]
fn should_accept_identical_duplicates_when_equal_sequences_straddle_a_block_boundary(
) -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let reader = straddling_duplicate_reader(&directory, &vec![b'x'; 3000])?;

    // Act
    let (state, _) = reader.get_raw_state_at_with_stats(b"dup", u64::MAX)?;

    // Assert
    assert_eq!(
        state,
        KeyState::Value(Bytes::from(vec![b'x'; 3000]), 100, None, EntryType::Put)
    );
    Ok(())
}
