use cntryl_midge::{
    CompactionLevelConfig, Engine, MidgeError, MidgeResult, OpenOptions, TransactionMode,
    WriteOptions,
};
use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

const FLUSHES: u32 = 24;
const KEYS_PER_FLUSH: u32 = 40;

fn small_levels() -> CompactionLevelConfig {
    CompactionLevelConfig {
        l1_target_bytes: 4 * 1024,
        level_multiplier: 2,
        ..CompactionLevelConfig::default()
    }
}

fn value_for(ordinal: u32) -> Vec<u8> {
    let mut state = u64::from(ordinal).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..96)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

fn key_for(ordinal: u32) -> Vec<u8> {
    // Interleave flush batches across the key space so levels overlap.
    let batch = ordinal / KEYS_PER_FLUSH;
    let slot = ordinal % KEYS_PER_FLUSH;
    format!("k{:05}", slot * FLUSHES + batch).into_bytes()
}

fn open(path: &Path, levels: CompactionLevelConfig) -> MidgeResult<Engine> {
    Engine::open(
        OpenOptions::local(path)
            .background_compaction(false)
            .compaction_levels(levels)
            .build()?,
    )
}

fn write_workload(engine: &Engine) -> MidgeResult<()> {
    let family = engine.get_column_family("default").unwrap();
    for flush in 0..FLUSHES {
        let mut tx = engine.begin_tx(family.id(), TransactionMode::ReadWrite)?;
        for slot in 0..KEYS_PER_FLUSH {
            let ordinal = flush * KEYS_PER_FLUSH + slot;
            tx.put(key_for(ordinal), value_for(ordinal), None)?;
        }
        tx.commit(WriteOptions::sync())?;
        engine.flush_cf(&family)?;
        if flush % 6 == 5 {
            engine.compact_all()?;
        }
    }
    for _ in 0..16 {
        engine.compact_all()?;
    }
    Ok(())
}

fn assert_all_keys_readable(engine: &Engine) -> MidgeResult<()> {
    let family = engine.get_column_family("default").unwrap();
    let tx = engine.begin_tx(family.id(), TransactionMode::ReadOnly)?;
    for ordinal in 0..FLUSHES * KEYS_PER_FLUSH {
        let value = tx.get(&key_for(ordinal))?;
        assert_eq!(value.as_deref(), Some(value_for(ordinal).as_slice()));
    }
    Ok(())
}

fn deepest_level(engine: &Engine) -> MidgeResult<u32> {
    let layout = engine.metrics().get_storage_layout()?;
    Ok(layout
        .levels
        .iter()
        .filter(|level| level.file_count > 0)
        .map(|level| level.level)
        .max()
        .unwrap_or(0))
}

fn sst_files(path: &Path) -> BTreeSet<(String, u64)> {
    let mut found = BTreeSet::new();
    let mut pending = vec![path.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            let metadata = entry.metadata().unwrap();
            if metadata.is_dir() {
                pending.push(entry.path());
            } else if entry.path().extension().is_some_and(|ext| ext == "sst") {
                found.insert((entry.path().display().to_string(), metadata.len()));
            }
        }
    }
    found
}

#[test]
fn should_fill_deeper_levels_when_level_targets_are_configured_small() -> MidgeResult<()> {
    // Arrange
    let small_dir = tempfile::tempdir().unwrap();
    let default_dir = tempfile::tempdir().unwrap();
    let mut small = open(small_dir.path(), small_levels())?;
    let mut default = open(default_dir.path(), CompactionLevelConfig::default())?;

    // Act
    write_workload(&small)?;
    write_workload(&default)?;

    // Assert
    assert!(deepest_level(&small)? >= 3);
    assert!(deepest_level(&default)? < 3);
    assert_all_keys_readable(&small)?;
    assert_all_keys_readable(&default)?;
    small.shutdown(Duration::from_secs(10))?;
    default.shutdown(Duration::from_secs(10))?;
    let mut reopened = open(small_dir.path(), small_levels())?;
    assert_all_keys_readable(&reopened)?;
    reopened.shutdown(Duration::from_secs(10))
}

#[test]
fn should_reject_invalid_compaction_level_config_when_building_options() {
    // Arrange
    let invalid = [
        CompactionLevelConfig {
            l1_target_bytes: 0,
            ..CompactionLevelConfig::default()
        },
        CompactionLevelConfig {
            level_multiplier: 1,
            ..CompactionLevelConfig::default()
        },
        CompactionLevelConfig {
            level_multiplier: 101,
            ..CompactionLevelConfig::default()
        },
        CompactionLevelConfig {
            max_levels: 2,
            ..CompactionLevelConfig::default()
        },
        CompactionLevelConfig {
            max_levels: 8,
            ..CompactionLevelConfig::default()
        },
    ];

    for config in invalid {
        // Act
        let result = OpenOptions::in_memory()
            .compaction_levels(config.clone())
            .build();

        // Assert
        assert!(
            matches!(result, Err(MidgeError::InvalidArgument(_))),
            "{config:?} must be rejected"
        );
    }
}

#[test]
fn should_refuse_open_without_changing_files_when_max_levels_is_below_existing_depth(
) -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let mut engine = open(directory.path(), small_levels())?;
    write_workload(&engine)?;
    assert!(deepest_level(&engine)? >= 3);
    engine.shutdown(Duration::from_secs(10))?;
    drop(engine);
    let before = sst_files(directory.path());

    // Act
    let result = open(
        directory.path(),
        CompactionLevelConfig {
            max_levels: 3,
            ..small_levels()
        },
    );

    // Assert
    let Err(MidgeError::InvalidArgument(message)) = result else {
        panic!("open must refuse a store deeper than max_levels");
    };
    assert!(message.contains("max_levels 3"), "{message}");
    assert_eq!(sst_files(directory.path()), before);
    let mut reopened = open(directory.path(), small_levels())?;
    assert_all_keys_readable(&reopened)?;
    reopened.shutdown(Duration::from_secs(10))
}

#[cfg(all(feature = "internal-testing", feature = "failpoints"))]
#[test]
fn should_report_grandparent_overlap_rolls_when_a_tiny_overlap_limit_cuts_outputs(
) -> MidgeResult<()> {
    // Arrange: small SSTs give every level several files, so outputs cross
    // grandparent file boundaries.
    let directory = tempfile::tempdir().unwrap();
    let mut engine = Engine::open(
        OpenOptions::local(directory.path())
            .background_compaction(false)
            .target_sst_size_for_testing(4096)
            .compaction_levels(CompactionLevelConfig {
                max_grandparent_overlap_bytes: 1,
                ..small_levels()
            })
            .build()?,
    )?;

    let family = engine.get_column_family("default").unwrap();

    // Act: each round's keys interleave with every earlier round's.
    for flush in 0..4 {
        let mut tx = engine.begin_tx(family.id(), TransactionMode::ReadWrite)?;
        for slot in 0..KEYS_PER_FLUSH {
            let ordinal = flush * KEYS_PER_FLUSH + slot;
            tx.put(key_for(ordinal), value_for(ordinal), None)?;
        }
        tx.commit(WriteOptions::sync())?;
        engine.flush_cf(&family)?;
        engine.compact_all()?;
    }

    // Assert
    let rolls =
        cntryl_midge::__internal::diagnostics::compaction_grandparent_overlap_roll_count(&engine);
    assert!(
        rolls > 0,
        "the engine reported no grandparent overlap rolls"
    );
    engine.shutdown(Duration::from_secs(10))
}
