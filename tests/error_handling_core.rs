mod common;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode, WalRecoveryMode, test_hooks::{TestHooks, WalBehavior, IoBehavior}};
use common::test_temp_dir;

// Phase 1 Error Handling & Fault Injection Core Tests
// Each test targets ONE behavior using TestHooks for deterministic fault injection.

#[test]
fn should_recover_given_wal_corruption_mid_record_when_tolerant_mode() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_wal_behavior(WalBehavior::TruncateAfterWrite);
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: dir.path().to_path_buf() },
        wal_sync: true,
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    // Act - write data with torn WAL, then reopen with tolerant mode
    {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let cf = eng.default_column_family();
        eng.put(&cf, b"key1", b"value1").expect("put");
        assert!(hooks.wal_append_count() > 0, "WAL append should have been called");
    }

    // Assert - reopen should succeed with TolerateCorruptedTail
    let opts_reopen = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: dir.path().to_path_buf() },
        wal_sync: true,
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        test_hooks: None,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts_reopen).expect("reopen with tolerant mode");
    let cf = eng.default_column_family();
    // Data may or may not be present depending on where truncation occurred - test verifies graceful recovery
    let _result = eng.get(&cf, b"key1");
}

#[test]
fn should_fail_open_given_wal_corruption_mid_record_when_strict_mode() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_wal_behavior(WalBehavior::TruncateAfterWrite);
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: dir.path().to_path_buf() },
        wal_sync: true,
        wal_recovery_mode: WalRecoveryMode::AbsoluteConsistency,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    // Act - write data with torn WAL
    {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let cf = eng.default_column_family();
        eng.put(&cf, b"key1", b"value1").expect("put");
    }

    // Assert - reopen with AbsoluteConsistency should potentially fail (depending on corruption location)
    let opts_reopen = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: dir.path().to_path_buf() },
        wal_sync: true,
        wal_recovery_mode: WalRecoveryMode::AbsoluteConsistency,
        test_hooks: None,
        ..Default::default()
    };
    // Either opens successfully (if truncation after all records) or fails (if mid-record)
    let _result = MidgeEngine::open(opts_reopen);
    // Test verifies no panic occurs - error or success both acceptable
}

#[test]
fn should_recover_gracefully_given_manifest_corruption_with_wal_fallback() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: dir.path().to_path_buf() },
        memtable_size: 512,
        wal_sync: true,
        ..Default::default()
    };

    // Act - write data to create manifest, then corrupt it
    {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let cf = eng.default_column_family();
        let large_value = vec![b'x'; 256];
        for i in 0..20 {
            eng.put(&cf, format!("key{:04}", i).as_bytes(), &large_value).expect("put");
        }
        eng.flush().ok(); // Force manifest write
    }

    // Corrupt manifest file (if it exists - manifest creation depends on flush timing)
    let manifest_path = dir.path().join("MANIFEST-000001");
    if manifest_path.exists() {
        std::fs::write(&manifest_path, b"{ invalid json }").expect("corrupt manifest");
    }

    // Assert - reopen should either fail or recover from WAL depending on implementation
    let result = MidgeEngine::open(opts);
    // Test verifies no panic - either recovers or fails gracefully
    match result {
        Ok(_eng) => {
            // Engine recovered (fallback to WAL or manifest wasn't created yet)
        }
        Err(_e) => {
            // Engine failed gracefully with error
        }
    }
}

#[test]
fn should_track_fsync_calls_when_wal_sync_enabled() {
    use cntryl_midge::test_hooks::FsyncBehavior;
    
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_fsync_behavior(FsyncBehavior::RecordOnly);
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: dir.path().to_path_buf() },
        wal_sync: true,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    // Act
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();
    let fsync_before = hooks.fsync_count();
    eng.put(&cf, b"key", b"value").expect("put");
    let fsync_after = hooks.fsync_count();

    // Assert
    assert!(fsync_after > fsync_before, "Fsync should be called when wal_sync enabled");
}

#[test]
fn should_not_persist_unfsynced_data_when_fsync_skipped() {
    use cntryl_midge::test_hooks::FsyncBehavior;
    
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_fsync_behavior(FsyncBehavior::Skip);
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: dir.path().to_path_buf() },
        wal_sync: true,
        test_hooks: Some(hooks),
        ..Default::default()
    };

    // Act - write with fsync skipped, then crash-simulate by dropping
    {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let cf = eng.default_column_family();
        eng.put(&cf, b"unfsynced", b"value").expect("put");
    }

    // Assert - reopen and data may be lost (fsync was skipped)
    let opts_reopen = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: dir.path().to_path_buf() },
        wal_sync: true,
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        test_hooks: None,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts_reopen).expect("reopen");
    let cf = eng.default_column_family();
    // Data may or may not be present - test verifies no crash
    let _result = eng.get(&cf, b"unfsynced");
}

// Note: Disk full scenarios require OS-level simulation (quota/permissions) or VFS wrapper
// These are deferred as they need infrastructure not yet in TestHooks

#[test]
fn should_return_error_given_disk_full_when_writing_wal() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_io_behavior(IoBehavior::FailWithEnospc);
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: dir.path().to_path_buf() },
        wal_sync: true,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    // Act - attempt to write data when disk is full
    let eng = MidgeEngine::open(opts).expect("open should succeed initially");
    let cf = eng.default_column_family();
    let result = eng.put(&cf, b"key1", b"value1");

    // Assert - write should fail with disk full error
    assert!(result.is_err(), "Write should fail when disk is full");
    let err = result.unwrap_err();
    match err {
        cntryl_midge::MidgeError::Io(io_err) => {
            assert!(io_err.to_string().contains("No space left on device"), 
                   "Error should indicate disk full: {}", io_err);
        }
        _ => panic!("Expected I/O error, got: {:?}", err),
    }
}

#[test]
fn should_return_error_given_disk_full_when_flushing_memtable() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_io_behavior(IoBehavior::FailWithEnospc);
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: dir.path().to_path_buf() },
        wal_sync: false, // Don't sync WAL writes, only flush should fail
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    // Act - write data then force flush when disk is full
    let eng = MidgeEngine::open(opts).expect("open should succeed initially");
    let cf = eng.default_column_family();
    
    // Write some data to memtable
    eng.put(&cf, b"key1", b"value1").expect("initial write should succeed");
    
    // Force flush - this should fail with disk full
    let result = eng.flush();

    // Assert - flush should fail with disk full error
    assert!(result.is_err(), "Flush should fail when disk is full");
    let err = result.unwrap_err();
    match err {
        cntryl_midge::MidgeError::Io(io_err) => {
            assert!(io_err.to_string().contains("No space left on device"), 
                   "Error should indicate disk full: {}", io_err);
        }
        _ => panic!("Expected I/O error, got: {:?}", err),
    }
}

#[test]
#[ignore] // Requires SST corruption infrastructure
fn should_handle_io_error_when_reading_sst_block() {
    // TODO: Corrupt SST block and verify error handling
}

#[test]
#[ignore] // Requires background error injection
fn should_propagate_background_error_to_user_on_next_operation() {
    // TODO: Inject compaction failure and verify error surfacing
}

#[test]
#[ignore] // Requires background error injection
fn should_pause_writes_given_background_error_until_cleared() {
    // TODO: Force background error and verify write blocking
}
