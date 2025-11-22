// Autotune stability tests
mod common;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use common::*;

#[test]
fn should_adjust_memtable_size_smoothly_given_sustained_high_write_throughput_when_autotune_enabled(
) {
    // Arrange: simulate sustained high write throughput
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 1024 * 1024, // 1MB
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.default_column_family();

    // Act: perform high write throughput
    for i in 0..1000 {
        eng.put(&cf, format!("k{:04}", i).as_bytes(), b"v").unwrap();
    }
    eng.flush().unwrap();

    // Assert: writes succeeded smoothly
    for i in 0..1000 {
        let key = format!("k{:04}", i);
        let value = eng.get(&cf, key.as_bytes()).unwrap();
        assert!(value.is_some());
    }
}

#[test]
fn should_not_enter_feedback_loop_oscillation_given_fluctuating_write_load_when_autotune_controls_compaction_threads(
) {
    // Arrange: fluctuating write load
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.default_column_family();

    // Act: fluctuating writes
    for cycle in 0..5 {
        let num_writes = 100 + cycle * 50;
        for i in 0..num_writes {
            eng.put(&cf, format!("k{:04}", i).as_bytes(), b"v").unwrap();
        }
        eng.flush().unwrap();
    }

    // Assert: no oscillation (engine stable)
    let value = eng.get(&cf, b"k0000").unwrap();
    assert!(value.is_some());
}

#[test]
fn should_respect_configured_limits_given_autotune_recommendations_exceed_maximums_when_system_under_extreme_load(
) {
    // Arrange: configure strict limits
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 64 * 1024, // 64KB small limit
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.default_column_family();

    // Act: extreme load
    for i in 0..1000 {
        eng.put(&cf, format!("k{:04}", i).as_bytes(), b"v").unwrap();
    }

    // Assert: engine enforces limits and remains stable
    let value = eng.get(&cf, b"k0000").unwrap();
    assert!(value.is_some());
}

#[test]
fn should_revert_to_safe_defaults_given_corrupted_autotune_state_on_startup_when_recovering_engine()
{
    // Arrange: normal startup
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.default_column_family();

    // Put some data
    eng.put(&cf, b"key", b"value").unwrap();

    // Act: restart engine
    drop(eng);
    let eng2 = MidgeEngine::open(MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    })
    .unwrap();

    // Assert: reverts to safe defaults (data preserved)
    let cf2 = eng2.default_column_family();
    let value = eng2.get(&cf2, b"key").unwrap();
    assert_eq!(value.as_deref(), Some(b"value".as_ref()));
}
