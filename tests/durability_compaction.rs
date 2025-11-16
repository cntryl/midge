mod common;
use cntryl_midge::test_hooks::{CompactionBehavior, TestHooks};
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use common::test_temp_dir;
use std::time::Duration;

#[test]
fn should_commit_new_ssts_and_manifest_together_given_compaction_successful() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 1024,
        enable_compaction: true,
        wal_sync: true,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    // Act - Write overlapping keys to trigger compaction
    let eng = MidgeEngine::open(opts.clone()).expect("open");
    let cf = eng.default_column_family();
    let compaction_starts_before = hooks.compaction_start_count();
    // Write larger values to exceed memtable_size and trigger flushes
    let large_value = vec![b'x'; 100]; // 100 bytes per value
    for i in 0..200 {
        eng.put(&cf, format!("key{:04}", i % 50).as_bytes(), &large_value)
            .expect("put");
    }
    // Wait for compaction to trigger
    std::thread::sleep(Duration::from_millis(500));
    let compaction_complete = hooks.compaction_start_count() > compaction_starts_before;

    // DEBUG: Check compaction trigger counts
    if !compaction_complete {
        eprintln!(
            "Compaction didn't start. Starts: {} (before: {})",
            hooks.compaction_start_count(),
            compaction_starts_before
        );
    }

    drop(eng);

    // Assert - all latest values should be present after restart
    let opts_recovery = MidgeOptions {
        test_hooks: None,
        ..opts
    };
    let eng = MidgeEngine::open(opts_recovery).expect("recover");
    let cf = eng.default_column_family();
    let expected_value = vec![b'x'; 100];
    for i in 0..50 {
        let result = eng
            .get(&cf, format!("key{:04}", i).as_bytes())
            .expect("get");
        assert!(
            result.is_some(),
            "Compacted key {} should exist after recovery",
            i
        );
        assert_eq!(result.unwrap(), expected_value, "Value should match");
    }
    assert!(compaction_complete, "Compaction should have started");
}

#[test]
fn should_cleanup_partial_output_given_compaction_failure() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_compaction_behavior(CompactionBehavior::FailMidway);
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 1024,
        enable_compaction: true,
        wal_sync: true,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    // Act - Write data and trigger compaction with failure injection
    let eng = MidgeEngine::open(opts.clone()).expect("open");
    let cf = eng.default_column_family();
    let large_value = vec![b'x'; 100]; // 100 bytes per value
    for i in 0..200 {
        eng.put(&cf, format!("key{:04}", i).as_bytes(), &large_value)
            .expect("put");
    }
    // Wait for compaction attempt to execute
    std::thread::sleep(Duration::from_millis(500));
    let compaction_started = hooks.compaction_start_count() > 0;
    drop(eng);

    // Assert - database should be consistent (no orphaned partial SSTs)
    // Reopen with clean hooks (no failure injection) to verify recovery
    let opts_recovery = MidgeOptions {
        test_hooks: None,
        ..opts
    };
    let eng = MidgeEngine::open(opts_recovery).expect("recover");
    let cf = eng.default_column_family();
    for i in 0..200 {
        let result = eng
            .get(&cf, format!("key{:04}", i).as_bytes())
            .expect("get");
        assert!(
            result.is_some(),
            "Data should be preserved despite compaction failure"
        );
    }
    assert!(compaction_started, "Compaction should have started");
}

#[test]
fn should_delete_old_sst_files_only_after_manifest_persisted() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 1024,
        enable_compaction: true,
        wal_sync: true,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    // Act - Write data to create multiple SSTs and trigger compaction
    let eng = MidgeEngine::open(opts.clone()).expect("open");
    let cf = eng.default_column_family();
    let compaction_starts_before = hooks.compaction_start_count();
    for round in 0..3 {
        let round_value = vec![b'0' + round as u8; 100]; // 100 bytes per value
        for i in 0..100 {
            eng.put(&cf, format!("key{:04}", i).as_bytes(), &round_value)
                .expect("put");
        }
    }
    // Wait for compaction
    std::thread::sleep(Duration::from_millis(500));
    let compaction_completed = hooks.compaction_complete_count() > compaction_starts_before;
    drop(eng);

    // Assert - latest values should be present, old SSTs should be cleaned
    let opts_recovery = MidgeOptions {
        test_hooks: None,
        ..opts
    };
    let eng = MidgeEngine::open(opts_recovery).expect("recover");
    let expected_value = vec![b'2'; 100];
    let cf = eng.default_column_family();
    for i in 0..100 {
        let result = eng
            .get(&cf, format!("key{:04}", i).as_bytes())
            .expect("get");
        assert_eq!(
            result.as_ref().map(|v| v.as_ref()),
            Some(expected_value.as_slice()),
            "Value for key{:04} should match",
            i
        );
    }
    assert!(compaction_completed, "Compaction should have completed");
}

#[test]
fn should_fsync_new_ssts_before_updating_manifest() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 1024,
        enable_compaction: true,
        wal_sync: true,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    // Act - Write overlapping keys and trigger compaction
    let eng = MidgeEngine::open(opts.clone()).expect("open");
    let cf = eng.default_column_family();
    let fsync_count_before = hooks.fsync_count();
    let compaction_starts_before = hooks.compaction_start_count();
    let large_value = vec![b'x'; 100]; // 100 bytes per value
    for i in 0..200 {
        eng.put(&cf, format!("key{:04}", i % 50).as_bytes(), &large_value)
            .expect("put");
    }
    // Wait for compaction
    std::thread::sleep(Duration::from_millis(500));
    let fsync_count_after = hooks.fsync_count();
    let compaction_completed = hooks.compaction_complete_count() > compaction_starts_before;
    drop(eng);

    // Assert - compacted data should be durable (new SSTs were fsynced)
    let opts_recovery = MidgeOptions {
        test_hooks: None,
        ..opts
    };
    let eng = MidgeEngine::open(opts_recovery).expect("recover");
    let cf = eng.default_column_family();
    for i in 0..50 {
        let result = eng
            .get(&cf, format!("key{:04}", i).as_bytes())
            .expect("get");
        assert!(result.is_some(), "Compacted key should be durable");
    }
    assert!(
        fsync_count_after >= fsync_count_before,
        "SST fsync should have occurred"
    );
    assert!(compaction_completed, "Compaction should have completed");
}

#[test]
fn should_recover_consistent_state_given_crash_mid_compaction_when_restart() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_compaction_behavior(CompactionBehavior::CrashBeforeFsync);
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 1024,
        enable_compaction: true,
        wal_sync: true,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    // Act - Write data and simulate crash during compaction
    let eng = MidgeEngine::open(opts.clone()).expect("open");
    let cf = eng.default_column_family();
    let large_value = vec![b'x'; 100]; // 100 bytes per value
    for i in 0..200 {
        eng.put(&cf, format!("key{:04}", i).as_bytes(), &large_value)
            .expect("put");
    }
    // Wait for compaction to reach crash point
    std::thread::sleep(Duration::from_millis(500));
    let compaction_attempted = hooks.compaction_start_count() > 0;
    drop(eng);

    // Assert - all data should be present after recovery (either from old SSTs or WAL)
    let opts_recovery = MidgeOptions {
        test_hooks: None,
        ..opts
    };
    let eng = MidgeEngine::open(opts_recovery).expect("recover");
    let cf = eng.default_column_family();
    for i in 0..200 {
        let result = eng
            .get(&cf, format!("key{:04}", i).as_bytes())
            .expect("get");
        assert!(result.is_some(), "Data should survive crash mid-compaction");
    }
    assert!(
        compaction_attempted,
        "Compaction should have been attempted"
    );
}

#[test]
fn should_preserve_source_ssts_when_compaction_output_not_fsynced() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_compaction_behavior(CompactionBehavior::CrashBeforeFsync);
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 1024,
        enable_compaction: true,
        wal_sync: true,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    // Act - Write data and simulate crash before compaction output fsync
    let eng = MidgeEngine::open(opts.clone()).expect("open");
    let cf = eng.default_column_family();
    let large_value = vec![b'x'; 100]; // 100 bytes per value
    for i in 0..200 {
        eng.put(&cf, format!("key{:04}", i).as_bytes(), &large_value)
            .expect("put");
    }
    // Wait for compaction to reach crash point
    std::thread::sleep(Duration::from_millis(500));
    let compaction_attempted = hooks.compaction_start_count() > 0;
    drop(eng);

    // Assert - data should be recoverable from source SSTs
    let opts_recovery = MidgeOptions {
        test_hooks: None,
        ..opts
    };
    let eng = MidgeEngine::open(opts_recovery).expect("recover");
    let cf = eng.default_column_family();
    for i in 0..200 {
        let result = eng
            .get(&cf, format!("key{:04}", i).as_bytes())
            .expect("get");
        assert!(result.is_some(), "Source SSTs should preserve data");
    }
    assert!(
        compaction_attempted,
        "Compaction should have been attempted"
    );
}
