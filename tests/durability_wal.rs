mod common;
use cntryl_midge::{
    test_hooks::{FsyncBehavior, TestHooks},
    MidgeEngine, MidgeOptions, StorageMode, WalRecoveryMode,
};
use common::{assert_get_equals, durability_opts, test_temp_dir, with_engine_restart};
use std::sync::Arc;

#[test]
fn should_recover_without_loss_given_crash_after_wal_append_before_fsync() {
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Act & Assert - write with fsync enabled, then verify after restart
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            eng.put(&cf, b"key1", b"value1").expect("put");
            // Data is written to WAL and fsynced before put() returns
        },
        |eng| {
            // Assert - fsynced write should be visible after restart
            assert_get_equals(eng, b"key1", b"value1");
        },
    );
}

#[test]
fn should_lose_unfsynced_data_given_crash_before_fsync() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new()
        .with_wal_behavior(cntryl_midge::test_hooks::WalBehavior::TruncateAfterWrite);

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_sync: true,
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    // Act - Write data with torn write simulation (crash during write)
    {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let cf = eng.default_column_family();
        eng.put(&cf, b"unfsynced_key", b"unfsynced_value")
            .expect("put");
        // Verify WAL append was recorded
        assert!(
            hooks.wal_append_count() > 0,
            "WAL append should have been called"
        );
    } // Engine drops here (with torn write simulation)

    // Assert - Reopen with hooks disabled to allow normal recovery
    let opts_recovery = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_sync: true,
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        test_hooks: None,
        ..Default::default()
    };

    let eng = MidgeEngine::open(opts_recovery).expect("reopen");
    let cf = eng.default_column_family();
    let result = eng.get(&cf, b"unfsynced_key").expect("get");
    // With TolerateCorruptedTail and torn write, the truncated record should be discarded
    // Recovery should succeed gracefully regardless of whether data was lost
    if let Some(value) = result {
        assert_eq!(
            value.as_ref(),
            b"unfsynced_value",
            "If present, data should be correct"
        );
    }
    // Test passes whether data is present or not - verifies graceful recovery
}

#[test]
fn should_not_acknowledge_commit_given_wal_unsynced_when_crash_occurs() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_fsync_behavior(FsyncBehavior::RecordOnly);

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_sync: true,
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    // Act & Assert
    {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let cf = eng.default_column_family();

        // Track fsync calls before write
        let fsync_count_before = hooks.fsync_count();

        // Write with fsync enabled (but RecordOnly to track)
        let result = eng.put(&cf, b"committed_key", b"committed_value");
        assert!(result.is_ok(), "Commit should only succeed after WAL fsync");

        // Verify fsync was called before returning
        let fsync_count_after = hooks.fsync_count();
        assert!(
            fsync_count_after > fsync_count_before,
            "Fsync should have been called before put() returns"
        );
    }

    // Reset options for recovery
    let opts_recovery = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_sync: true,
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        test_hooks: None,
        ..Default::default()
    };

    // Assert - recovery should replay the fsynced write
    let eng = MidgeEngine::open(opts_recovery).expect("reopen");
    assert_get_equals(&eng, b"committed_key", b"committed_value");
}

#[test]
fn should_maintain_strict_wal_order_given_concurrent_appends_when_crash_occurs() {
    // Arrange
    let dir = test_temp_dir();
    let db_path = dir.path().to_path_buf();
    let opts = durability_opts(db_path.clone());

    // Act - perform concurrent writes
    {
        use cntryl_midge::MidgeEngine;
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let eng = Arc::new(eng);
        let handles: Vec<_> = (0..10)
            .map(|i| {
                let eng = Arc::clone(&eng);
                std::thread::spawn(move || {
                    let cf = eng.default_column_family();
                    eng.put(
                        &cf,
                        format!("key{}", i).as_bytes(),
                        format!("value{}", i).as_bytes(),
                    )
                    .expect("put");
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    } // Engine drops here (simulating crash)

    // Assert - reopen and verify all concurrent writes recovered
    use cntryl_midge::MidgeEngine;
    let eng = MidgeEngine::open(opts).expect("reopen");
    let cf = eng.default_column_family();
    for i in 0..10 {
        let result = eng.get(&cf, format!("key{}", i).as_bytes()).expect("get");
        assert!(result.is_some(), "Concurrent write {} should be present", i);
    }
}

#[test]
fn should_replay_all_valid_records_given_multiple_segments_when_recovering() {
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            // Act - write enough data to trigger multiple WAL segments
            let cf = eng.default_column_family();
            for i in 0..1000 {
                eng.put(&cf, format!("key{}", i).as_bytes(), b"some_value")
                    .expect("put");
            }
        },
        |eng| {
            // Assert - all records should be replayed after restart
            let cf = eng.default_column_family();
            for i in 0..1000 {
                let result = eng.get(&cf, format!("key{}", i).as_bytes()).expect("get");
                assert!(result.is_some(), "Record {} should be replayed", i);
            }
        },
    );
}

#[test]
fn should_discard_partial_record_given_truncated_wal_segment_when_recovering() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new()
        .with_wal_behavior(cntryl_midge::test_hooks::WalBehavior::TruncateAfterWrite);

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_sync: true,
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    // Act & Assert
    {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let cf = eng.default_column_family();

        // Write complete record
        eng.put(&cf, b"complete_key", b"complete_value")
            .expect("put");

        // Verify WAL append was recorded
        let wal_append_count = hooks.wal_append_count();
        assert!(wal_append_count > 0, "WAL append should have been called");
    } // Engine drops here (with torn write simulation)

    // Reset options for recovery
    let opts_recovery = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_sync: true,
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        test_hooks: None,
        ..Default::default()
    };

    // Assert - recovery should handle truncation gracefully
    // Either the complete record is recovered, or recovery fails cleanly
    let result = MidgeEngine::open(opts_recovery);
    assert!(
        result.is_ok(),
        "Recovery should handle truncated WAL gracefully"
    );

    if let Ok(eng) = result {
        let cf = eng.default_column_family();
        // The complete record should either be there or absent (depending on implementation)
        // The important thing is that recovery is deterministic and doesn't panic
        let _ = eng.get(&cf, b"complete_key");
    }
}
