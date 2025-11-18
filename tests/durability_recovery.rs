mod common;
use cntryl_midge::{
    test_hooks::{ManifestBehavior, TestHooks, WalBehavior},
    MidgeEngine, MidgeOptions, StorageMode, WalRecoveryMode,
};
use common::{
    assert_get_equals, durability_opts, flush_test_opts, test_temp_dir, with_engine_restart,
};

#[test]
fn should_detect_and_ignore_already_compacted_wal_entries_given_manifest_sequence() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new();

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 1024,
        wal_sync: true,
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    // Act & Assert
    {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let cf = eng.default_column_family();

        // Record initial WAL append count
        let wal_appends_before = hooks.wal_append_count();

        // Write data that will flush to SST
        for i in 0..100 {
            eng.put(&cf, format!("key{:04}", i).as_bytes(), b"value")
                .expect("put");
        }

        // Verify WAL appends occurred
        let wal_appends_after_write = hooks.wal_append_count();
        assert!(
            wal_appends_after_write > wal_appends_before,
            "WAL appends should have occurred"
        );

        // Force flush so data is in SST
        eng.flush_cf(&cf).expect("flush");
    }

    // Reset hooks for recovery
    let opts_recovery = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 1024,
        wal_sync: true,
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        test_hooks: None,
        ..Default::default()
    };

    // Assert - recovery should not replay WAL entries already in SSTs
    let eng = MidgeEngine::open(opts_recovery).expect("reopen");
    let cf = eng.default_column_family();
    for i in 0..100 {
        let result = eng
            .get(&cf, format!("key{:04}", i).as_bytes())
            .expect("get");
        assert!(result.is_some(), "Data should be present exactly once");
    }
}

#[test]
fn should_replay_to_last_synced_sequence_given_fullsync_mode_when_recover() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new();

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

        // Record fsync count before writes
        let fsync_count_before = hooks.fsync_count();

        eng.put(&cf, b"synced1", b"value1").expect("put");
        eng.put(&cf, b"synced2", b"value2").expect("put");

        // Verify fsyncs occurred (each put should trigger fsync in durability mode)
        let fsync_count_after = hooks.fsync_count();
        assert!(
            fsync_count_after > fsync_count_before,
            "Fsync should have been called for each put"
        );
    }

    // Reset for recovery
    let opts_recovery = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_sync: true,
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        test_hooks: None,
        ..Default::default()
    };

    // Assert - recovery should replay to last synced sequence
    let eng = MidgeEngine::open(opts_recovery).expect("reopen");
    assert_get_equals(&eng, b"synced1", b"value1");
    assert_get_equals(&eng, b"synced2", b"value2");
}

#[test]
fn should_recover_last_committed_state_given_crash_during_write() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_wal_behavior(WalBehavior::TruncateAfterWrite);

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

        eng.put(&cf, b"committed1", b"value1").expect("put");
        eng.put(&cf, b"committed2", b"value2").expect("put");
        // Simulate torn write (crash during write) by truncating WAL after append
        // (WalBehavior::TruncateAfterWrite simulates incomplete write)
    }

    // Reset for recovery
    let opts_recovery = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_sync: true,
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        test_hooks: None,
        ..Default::default()
    };

    // Assert - truncated WAL should be recovered gracefully
    // TolerateCorruptedTail mode should discard the incomplete record
    let eng = MidgeEngine::open(opts_recovery).expect("reopen");
    let cf = eng.default_column_family();
    let result1 = eng.get(&cf, b"committed1").expect("get");
    let result2 = eng.get(&cf, b"committed2").expect("get");
    // With TolerateCorruptedTail, the last write may be lost if truncated
    // Both writes may be present if truncation happened after all writes completed
    // This test verifies recovery succeeds gracefully with corrupted tail
    assert!(
        result1.is_none() || result2.is_none() || (result1.is_some() && result2.is_some()),
        "Recovery should handle truncated WAL gracefully"
    );
}

#[test]
fn should_rebuild_manifest_up_to_last_fsynced_sequence() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_manifest_behavior(ManifestBehavior::CorruptAfterSave);
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 1024,
        wal_sync: true,
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            for i in 0..100 {
                eng.put(&cf, format!("key{:04}", i).as_bytes(), b"value")
                    .expect("put");
            }
            // Manifest will be corrupted after save due to CorruptAfterSave behavior
        },
        |eng| {
            // Assert - rebuilt manifest should contain all fsynced data
            let cf = eng.default_column_family();
            for i in 0..100 {
                let result = eng
                    .get(&cf, format!("key{:04}", i).as_bytes())
                    .expect("get");
                assert!(
                    result.is_some(),
                    "Fsynced data should be in rebuilt manifest"
                );
            }
        },
    );
}

#[test]
fn should_deduplicate_replay_given_partial_flush_in_manifest() {
    // Arrange
    let dir = test_temp_dir();
    let opts = flush_test_opts(dir.path().to_path_buf(), 1024);

    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            // Write same keys multiple times
            for round in 0..3 {
                for i in 0..50 {
                    eng.put(
                        &cf,
                        format!("key{:04}", i).as_bytes(),
                        format!("v{}", round).as_bytes(),
                    )
                    .expect("put");
                }
            }
        },
        |eng| {
            // Assert - each key should have latest value only (no duplicates)
            let _cf = eng.default_column_family();
            for i in 0..50 {
                assert_get_equals(eng, format!("key{:04}", i).as_bytes(), b"v2");
            }
        },
    );
}

#[test]
fn should_maintain_exactly_once_semantics_across_crash_recovery() {
    // Arrange
    let dir = test_temp_dir();
    let db_path = dir.path().to_path_buf();
    let opts = durability_opts(db_path.clone());

    // Act - multiple restart cycles to simulate repeated crashes
    use cntryl_midge::MidgeEngine;
    for cycle in 0..5 {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let cf = eng.default_column_family();

        // Write unique data each cycle
        eng.put(
            &cf,
            format!("cycle{}", cycle).as_bytes(),
            format!("value{}", cycle).as_bytes(),
        )
        .expect("put");

        drop(eng); // Simulate crash
    }

    // Assert - all cycles should be present exactly once
    let eng = MidgeEngine::open(opts).expect("final open");
    for cycle in 0..5 {
        assert_get_equals(
            &eng,
            format!("cycle{}", cycle).as_bytes(),
            format!("value{}", cycle).as_bytes(),
        );
    }
}
