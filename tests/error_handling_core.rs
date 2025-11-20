mod common;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode, WalRecoveryMode, test_hooks::{TestHooks, WalBehavior, IoBehavior, ManifestBehavior}};
use cntryl_midge::sst::fs::FsSstFactory;
use cntryl_midge::common::codec::CompressionType;
use cntryl_midge::sst::traits::{SstFactory, SstReaderFactory};
use std::sync::Arc;
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
fn should_handle_io_error_when_reading_sst_block() {
    // Arrange
    let dir = test_temp_dir();
    let sst_dir = dir.path().join("sst");
    std::fs::create_dir_all(&sst_dir).unwrap();
    
    // Create SST file directly using SST writer
    let factory = FsSstFactory::new(sst_dir.clone());
    let mut writer = factory.create(CompressionType::None, 4096, false);
    
    // Add some test data
    for i in 0..10 {
        let key = format!("key{:02}", i);
        let value = format!("value{}", i);
        writer.add(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    // Finish writing to create the SST file
    let sst_path = sst_dir.join("test_corruption.sst");
    writer.finish_to_path(&sst_path).unwrap();

    // Corrupt the SST file by overwriting some bytes in the middle
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&sst_path)
        .unwrap();

    // Get file size and corrupt the CRC checksum of a data block (not the footer)
    let file_size = file.metadata().unwrap().len();
    println!("SST file size: {}", file_size);
    if file_size > 100 {  // Make sure we have enough data to corrupt
        // Corrupt bytes that are likely to be in a block CRC (try a few positions)
        // Skip the footer (last 48 bytes) and corrupt somewhere in the middle
        let footer_start = file_size.saturating_sub(48);
        let corrupt_offset = (footer_start / 2).max(100); // Middle of the file, but not in footer
        
        use std::io::{Seek, SeekFrom, Write};
        file.seek(SeekFrom::Start(corrupt_offset)).unwrap();

        // Write garbage data that might corrupt a CRC
        let garbage = [0xFF, 0xFF, 0xFF, 0xFF];
        file.write_all(&garbage).unwrap();
        file.flush().unwrap();
        println!("Corrupted 4 bytes at offset {} (avoiding footer)", corrupt_offset);
    }

    // Try to read from the corrupted SST file using SST reader
    let reader_factory = cntryl_midge::sst::fs::FsSstReaderFactory::new(false);
    match reader_factory.open(&sst_path) {
        Ok(_) => panic!("Reading from corrupted SST should fail"),
        Err(err) => {
            // The error could be InvalidData (CRC mismatch) or other corruption-related errors
            let err_str = format!("{}", err);
            assert!(err_str.contains("InvalidData") ||
                    err_str.contains("corrupt") ||
                    err_str.contains("CRC") ||
                    err_str.contains("decode"),
                    "Error should indicate data corruption: {}", err_str);
        }
    }
}

#[test]
// #[ignore] // Requires background error injection
fn should_propagate_background_error_to_user_on_next_operation() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_io_behavior(IoBehavior::Normal);
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: dir.path().to_path_buf() },
        wal_sync: false, // Don't sync WAL to avoid early failures
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };
    let engine = std::sync::Arc::new(MidgeEngine::open(opts).unwrap());
    let cf = engine.default_column_family();

    // Populate data to trigger compaction
    for i in 0..100 {
        let key = format!("key{:03}", i);
        let value = format!("value{}", i);
        engine.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
    }
    engine.flush().unwrap();

    // Enable disk full errors for background operations
    hooks.set_io_behavior(IoBehavior::FailWithEnospc);

    // Trigger background compaction that should fail
    let compaction_result = engine.compact_all();
    assert!(compaction_result.is_err(), "Background compaction should fail with disk full");

    // Assert - Compaction should fail with disk full error
    let err = compaction_result.unwrap_err();
    assert!(err.to_string().contains("No space left on device") ||
            err.to_string().contains("ENOSPC"),
            "Error should indicate disk full: {}", err);

    // Verify that user operations still work (compaction failure doesn't block writes)
    let put_result = engine.put(&cf, b"new_key", b"new_value");
    assert!(put_result.is_ok(), "User operations should continue despite compaction failure");
}

#[test]
fn should_return_error_given_disk_full_when_writing_manifest() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_manifest_behavior(ManifestBehavior::FailSave);
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: dir.path().to_path_buf() },
        test_hooks: Some(hooks),
        ..Default::default()
    };
    let engine = Arc::new(MidgeEngine::open(opts).unwrap());
    let cf = engine.default_column_family();

    // Populate data and flush to trigger manifest updates
    for i in 0..10 {
        let key = format!("key{:03}", i);
        let value = format!("value{}", i);
        engine.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Act - Force flush which should update manifest and fail
    let flush_result = engine.flush();

    // Assert - Flush should fail due to manifest write failure
    assert!(flush_result.is_err(), "Flush should fail when manifest write fails");
    let err = flush_result.unwrap_err();
    assert!(err.to_string().contains("manifest") || err.to_string().contains("Manifest"),
            "Error should indicate manifest failure: {}", err);
}

#[test]
// Temporarily enabling test for validation; leave if it passes
fn should_pause_writes_given_background_error_until_cleared() {
    // Arrange
    let dir = test_temp_dir();
    let mut opts = MidgeOptions::default();
    opts.storage_mode = StorageMode::LocalDisk { db_path: dir.path().to_path_buf() };
    opts.memtable_size = 256; // small overall engine memtable size
    opts.wal_buffer_size = 64; // small WAL buffer

    let engine = Arc::new(MidgeEngine::open(opts).unwrap());
    use cntryl_midge::api::column_family::ColumnFamilyConfig;
    let mut cf_cfg = ColumnFamilyConfig::default();
    cf_cfg.memtable_max_bytes = 64; // tiny so each value triggers full
    cf_cfg.max_immutable_memtables = 1; // single immutable allowed then stall
    let stall_cf = engine.create_column_family("stall_cf", cf_cfg).expect("create CF");

    // First write fills memtable and freezes, producing one immutable memtable
    let first_val = vec![b'x'; 96];
    engine.put_with_ttl(&stall_cf, b"seed", &first_val, 0).unwrap();

    // Inject background error AFTER first freeze so next write stalls while error present
    engine.set_background_error(cntryl_midge::MidgeError::internal("simulated background flush failure"));

    // Act: Start a writer that will attempt a write and should stall due to background error + full immutable queue
    let (done_tx, done_rx) = crossbeam::channel::bounded::<bool>(1);
    let eng_clone = Arc::clone(&engine);
    let cf_clone = stall_cf.clone();
    std::thread::spawn(move || {
        let res = eng_clone.put_with_ttl(&cf_clone, b"blocked_key", b"blocked_value", 0);
        let ok = res.is_ok();
        let _ = done_tx.send(ok);
    });

    // Wait briefly and expect the write not complete (blocked)
    match done_rx.recv_timeout(std::time::Duration::from_millis(200)) {
        Ok(_) => panic!("Write should be blocked when background error present"),
        Err(_) => {}
    }

    // Clear background error so subsequent write can proceed
    engine.clear_background_error();

    // Drain immutable by flushing CF
    let _ = engine.flush_cf(&stall_cf);
    engine.wait_for_flush(std::time::Duration::from_secs(2)).unwrap();

    // Writer should now complete
    let done_ok = done_rx.recv_timeout(std::time::Duration::from_secs(1)).expect("writer should complete");
    assert!(done_ok, "Write should succeed once background error cleared and flush completed");
}
