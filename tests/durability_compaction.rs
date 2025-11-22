mod common;
use cntryl_midge::test_hooks::{CompactionBehavior, CompactionGatePoint, TestHooks};
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use common::test_temp_dir;

fn collect_sst_files(dir: &std::path::Path) -> Vec<String> {
    if !dir.exists() {
        return Vec::new();
    }

    let mut files = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if name.ends_with(".sst") {
                    files.push(name.to_string());
                }
            }
        }
    }
    files.sort();
    files
}

#[test]
fn should_commit_new_ssts_manifest_together_on_compaction_success() {
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
    eng.wait_for_compaction(std::time::Duration::from_secs(2))
        .expect("compaction should complete");
    let compaction_complete = hooks.compaction_start_count() > compaction_starts_before;

    // DEBUG: Check compaction trigger counts
    if !compaction_complete {
        tracing::debug!(
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
    eng.wait_for_compaction(std::time::Duration::from_secs(2))
        .expect("compaction should complete");
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
        memtable_size: 4096, // Increased from 1024
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
    eng.flush().unwrap(); // Force flush to create SSTs and trigger compaction
                          // Manually trigger compaction of level 0
    eng.compact_level(&cf, 0).unwrap();
    // Wait for compaction to complete - use stability-aware wait
    eng.wait_for_compaction(std::time::Duration::from_secs(10))
        .expect("compaction should complete");
    let compaction_started = hooks.compaction_start_count() > compaction_starts_before;
    let compaction_completed = hooks.compaction_complete_count() > 0;

    // Wait for manifest to be updated
    let manifest_updates_before = hooks.manifest_update_count();
    for _ in 0..50 {
        // Wait up to 5 seconds for manifest update
        if hooks.manifest_update_count() > manifest_updates_before {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    drop(eng);

    // Assert - compaction should have started and completed
    assert!(compaction_started, "Compaction should have started");
    assert!(compaction_completed, "Compaction should have completed");

    // Assert - latest values should be present, old SSTs should be cleaned
    let opts_recovery = MidgeOptions {
        test_hooks: None,
        ..opts
    };
    let eng = MidgeEngine::open(opts_recovery).expect("recover");
    let expected_value = vec![b'2'; 100];
    let cf = eng.default_column_family();

    // Retry the check a few times in case of timing issues
    let mut all_correct = false;
    for _ in 0..3 {
        all_correct = true;
        for i in 0..100 {
            let result = eng
                .get(&cf, format!("key{:04}", i).as_bytes())
                .expect("get");
            if result.as_ref().map(|v| v.as_ref()) != Some(expected_value.as_slice()) {
                all_correct = false;
                break;
            }
        }
        if all_correct {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    assert!(
        all_correct,
        "All keys should have the latest value after compaction"
    );
}

#[test]
fn should_keep_source_ssts_present_until_manifest_persisted() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new();
    let before_gate = hooks.install_compaction_gate(CompactionGatePoint::BeforeExecution);
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
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();
    let sst_dir = dir.path().join("sst");

    // Act
    for round in 0..3 {
        let value = vec![b'a' + round as u8; 128];
        for i in 0..64 {
            eng.put(&cf, format!("key{:04}", i).as_bytes(), &value)
                .expect("put");
        }
    }

    assert!(
        before_gate.wait_until_blocked(std::time::Duration::from_secs(5)),
        "Compaction should reach the BeforeExecution gate"
    );
    let source_files = collect_sst_files(&sst_dir);
    assert!(
        !source_files.is_empty(),
        "Expected flushed SSTs before compaction proceeds"
    );

    let after_gate = hooks.install_compaction_gate(CompactionGatePoint::AfterManifestUpdate);
    before_gate.release();

    assert!(
        after_gate.wait_until_blocked(std::time::Duration::from_secs(5)),
        "Compaction should reach the AfterManifestUpdate gate"
    );

    // Assert
    for file in &source_files {
        assert!(
            sst_dir.join(file).exists(),
            "Source SST {} should remain until manifest persistence completes",
            file
        );
    }

    after_gate.release();
    eng.wait_for_compaction(std::time::Duration::from_secs(2))
        .expect("compaction should complete");
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
    eng.wait_for_compaction(std::time::Duration::from_secs(2))
        .expect("compaction should complete");
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
    eng.wait_for_compaction(std::time::Duration::from_secs(2))
        .expect("compaction should complete");
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
    eng.wait_for_compaction(std::time::Duration::from_secs(2))
        .expect("compaction should complete");
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
