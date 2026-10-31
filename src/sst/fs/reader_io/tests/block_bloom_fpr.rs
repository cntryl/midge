use super::*;
use crate::common::resource_budget::ResourceBudget;

const KEYS: u64 = 20_000;

fn present_key(index: u64) -> Vec<u8> {
    format!("present/{index:08}").into_bytes()
}

fn write_keys(directory: &tempfile::TempDir, budget: Option<&ResourceBudget>) -> MidgeResult<()> {
    let fs = crate::io::real::test_root(directory.path());
    let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
    let mut writer = match budget {
        Some(budget) => factory.create_for_compaction(budget.clone())?,
        None => factory.create()?,
    };
    for index in 0..KEYS {
        writer.add_sorted_with_meta(
            &present_key(index),
            Some(b"v"),
            index + 1,
            EntryType::Put,
            None,
        )?;
    }
    if let Some(budget) = budget {
        // Mid-stream: every finished block's retained index and bloom bytes.
        let per_key = budget.used() / usize::try_from(KEYS).unwrap();
        assert!(
            per_key <= 8,
            "construction charges {per_key} bytes per key before finish"
        );
    }
    crate::sst::fs::finish_writer_to_path(writer, &directory.path().join("bloom.sst"))
}

#[test]
fn should_reject_nearly_all_absent_keys_when_data_block_blooms_are_checked() -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    write_keys(&directory, None)?;
    let reader = SstFileIo::open("bloom.sst", crate::io::real::test_root(directory.path()))?;
    let index = reader.index_entries()?;
    let probes = 100_000_u64;

    // Act: absent keys inside the key range, each against its candidate block.
    let mut maybe = 0_u64;
    for probe in 0..probes {
        let key = format!("present/{:08}-absent", probe % KEYS).into_bytes();
        for (block, _) in reader.candidate_data_blocks(index.as_ref(), &key) {
            if reader.check_block_bloom(block, &key) {
                maybe += 1;
            }
        }
    }

    // Assert: a 0.1% target leaves well under 0.25% false positives.
    assert!(
        maybe * 400 <= probes,
        "{maybe} of {probes} absent probes passed their block bloom"
    );
    for index in 0..KEYS {
        let key = present_key(index);
        assert_eq!(
            reader.get_raw_state_at_with_stats(&key, u64::MAX)?.0,
            KeyState::Value(Bytes::from_static(b"v"), index + 1, None, EntryType::Put),
            "false negative for {index}"
        );
    }
    Ok(())
}

#[test]
fn should_charge_retained_bloom_bytes_when_streaming_construction_finishes_blocks(
) -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let budget = ResourceBudget::new(1 << 30);

    // Act / Assert: the per-key bound is checked mid-stream.
    write_keys(&directory, Some(&budget))?;

    // Assert
    assert_eq!(budget.used(), 0);
    Ok(())
}
