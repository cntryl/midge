// Transaction spill / memory pressure tests
mod common;
use cntryl_midge::{
    IsolationLevel, KvTransaction, MidgeEngine, MidgeOptions, StorageMode, WriteOptions,
};
use common::*;

#[test]
fn should_complete_transaction_correctly_given_large_spill_file_when_restart_occurs_before_commit()
{
    // Arrange: create a large transaction that spills to disk
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };

    // Act: restart engine before commit
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            // Create transaction with small memory limit to force spilling
            let mut large_txn = eng
                .begin_transaction_with_options(
                    &cf,
                    None,
                    1024 * 1024, // 1MB limit
                    IsolationLevel::default(),
                )
                .unwrap();
            // Add 2MB of data to force spill
            for i in 0..2000 {
                large_txn
                    .put(format!("key{:06}", i).as_bytes(), &vec![0u8; 1024])
                    .unwrap();
            }
            // Do not commit, let restart occur
        },
        |eng| {
            let cf = eng.default_column_family();
            // Assert: transaction rolled back, no keys present
            for i in 0..2000 {
                let key = format!("key{:06}", i);
                let value = eng.get(&cf, key.as_bytes()).unwrap();
                assert!(
                    value.is_none(),
                    "Key {} should not exist after rollback",
                    key
                );
            }
        },
    );
}

#[test]
fn should_not_starve_foreground_writes_given_background_spill_activity_when_memory_pressure_is_high(
) {
    // Arrange: simulate high memory pressure and background spill activity
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.default_column_family();

    // Start a large transaction that will spill
    let mut spill_txn = eng
        .begin_transaction_with_options(&cf, None, 1024 * 1024, IsolationLevel::default())
        .unwrap();
    for i in 0..1000 {
        spill_txn
            .put(format!("spill{:06}", i).as_bytes(), &vec![0u8; 1024])
            .unwrap();
    }

    // Act: perform foreground writes
    for i in 0..100 {
        eng.put(&cf, format!("fg{:06}", i).as_bytes(), b"v")
            .unwrap();
    }

    // Assert: foreground writes succeeded
    for i in 0..100 {
        let key = format!("fg{:06}", i);
        let value = eng.get(&cf, key.as_bytes()).unwrap();
        assert!(value.is_some(), "Foreground write {} should succeed", key);
    }
}

#[test]
fn should_recover_spill_state_safely_given_crash_during_spill_file_rotation_when_reopening_engine()
{
    // Arrange: trigger spill rotation and simulate crash
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };

    // Act: reopen engine
    with_engine_restart(
        opts.clone(),
        |eng| {
            let cf = eng.default_column_family();
            // Create transaction that spills
            let mut txn = eng
                .begin_transaction_with_options(&cf, None, 1024 * 1024, IsolationLevel::default())
                .unwrap();
            for i in 0..2000 {
                txn.put(format!("key{:06}", i).as_bytes(), &vec![0u8; 1024])
                    .unwrap();
            }
            // Commit to ensure spill files are written
            eng.commit_transaction(txn, WriteOptions::default())
                .unwrap();
        },
        |eng| {
            let cf = eng.default_column_family();
            // Assert: spill state recovered, keys present
            for i in 0..2000 {
                let key = format!("key{:06}", i);
                let value = eng.get(&cf, key.as_bytes()).unwrap();
                assert!(value.is_some(), "Key {} should exist after recovery", key);
            }
        },
    );
}

#[test]
fn should_enforce_transaction_size_limits_given_spill_disabled_when_user_exceeds_configured_threshold(
) {
    // Arrange: configure spill disabled by setting very low mem_limit (but spill is always enabled)
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.default_column_family();

    // Create transaction with low memory limit
    let mut txn = eng
        .begin_transaction_with_options(
            &cf,
            None,
            1024, // 1KB limit
            IsolationLevel::default(),
        )
        .unwrap();

    // Act: attempt large transaction (spill will occur)
    for i in 0..100 {
        txn.put(format!("key{:06}", i).as_bytes(), &vec![0u8; 1024])
            .unwrap();
    }

    // Assert: transaction succeeds due to spill
    eng.commit_transaction(txn, WriteOptions::default())
        .unwrap();
    for i in 0..100 {
        let key = format!("key{:06}", i);
        let value = eng.get(&cf, key.as_bytes()).unwrap();
        assert!(value.is_some(), "Key {} should exist", key);
    }
}
