use super::*;
use crate::common::resource_budget::ResourceBudget;

const KEYS: u64 = 50_000;

fn write_metadata_heavy_sst(directory: &tempfile::TempDir) -> MidgeResult<()> {
    let fs = crate::io::real::test_root(directory.path());
    let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
    let mut writer = factory.create()?;
    for index in 0..KEYS {
        writer.add_sorted_with_meta(
            format!("tenant/{:04}/key/{index:08}", index / 7_000).as_bytes(),
            Some(&index.to_le_bytes().repeat(4)),
            index + 1,
            EntryType::Put,
            None,
        )?;
    }
    crate::sst::fs::finish_writer_to_path(writer, &directory.path().join("metadata.sst"))
}

/// Bytes the reader's parsed metadata structures actually keep allocated.
fn retained_footprint(reader: &SstFileIo) -> MidgeResult<usize> {
    Ok(reader.index_entries()?.retained_bytes()
        + reader
            .block_bloom_filter
            .as_ref()
            .map_or(0, crate::sst::bloom::BlockBloomFilter::memory_usage)
        + reader
            .trie_reader
            .as_ref()
            .map_or(0, |trie| trie.memory_usage()))
}

#[test]
fn should_charge_retained_metadata_near_its_footprint_when_a_budgeted_reader_opens(
) -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    write_metadata_heavy_sst(&directory)?;
    let budget = ResourceBudget::new(1 << 30);

    // Act
    let reader = SstFileIo::open_for_compaction(
        "metadata.sst",
        crate::io::real::test_root(directory.path()),
        budget.clone(),
    )?;
    let charged = budget.used();
    let footprint = retained_footprint(&reader)?;
    drop(reader);

    // Assert: never under-charged, and within a quarter plus fixed overhead.
    assert!(
        footprint > 64 * 1024,
        "fixture metadata is {footprint} bytes"
    );
    assert!(
        charged >= footprint,
        "charged {charged} < footprint {footprint}"
    );
    assert!(
        charged <= footprint + footprint / 4 + 8192,
        "charged {charged} for a {footprint}-byte footprint"
    );
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[test]
fn should_admit_readers_sized_by_their_footprint_when_one_pool_serves_many() -> MidgeResult<()> {
    // Arrange: one SST opened by many independent readers.
    let directory = tempfile::tempdir()?;
    write_metadata_heavy_sst(&directory)?;
    let probe = SstFileIo::open("metadata.sst", crate::io::real::test_root(directory.path()))?;
    let footprint = retained_footprint(&probe)?;
    drop(probe);
    let per_reader = footprint + footprint / 4 + 8192;
    let readers = 8;
    // Headroom for the last open's transient decode reservation.
    let budget = ResourceBudget::new(per_reader * readers + footprint * 4);

    // Act
    let mut opened = Vec::new();
    for _ in 0..readers {
        opened.push(SstFileIo::open_for_compaction(
            "metadata.sst",
            crate::io::real::test_root(directory.path()),
            budget.clone(),
        )?);
    }

    // Assert
    assert_eq!(opened.len(), readers);
    for reader in &opened {
        assert_eq!(
            reader
                .get_raw_state_at_with_stats(b"tenant/0000/key/00000010", u64::MAX)?
                .0,
            KeyState::Value(
                Bytes::from(10_u64.to_le_bytes().repeat(4)),
                11,
                None,
                EntryType::Put
            )
        );
    }
    drop(opened);
    assert_eq!(budget.used(), 0);
    Ok(())
}
