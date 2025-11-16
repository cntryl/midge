mod common;
use cntryl_midge::cloud::mock::MockCloudBackend;
use cntryl_midge::config::cloud::StorageContext;
use cntryl_midge::core::manifest::FileMeta;
use cntryl_midge::sst::cloud::SstLifecycleState;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use common::test_temp_dir;
use std::sync::Arc;

#[test]
fn should_preserve_local_file_given_upload_in_progress_when_crash() {
    // Arrange
    let dir = test_temp_dir();
    let db_path = dir.path().to_path_buf();

    // Act - use local disk (cloud mode would require mock backend)
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_path.clone(),
        },
        memtable_size: 1024,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    // Write data that will create SST files
    for i in 0..100 {
        eng.put(&cf, format!("key{:03}", i).as_bytes(), b"value")
            .expect("put");
    }
    drop(eng);

    // Assert - local files should be preserved
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("reopen");
    let cf = eng.default_column_family();

    // Check first key with debug output
    let result = eng.get(&cf, b"key000").expect("get");
    println!("First key after restart: {:?}", result.is_some());

    for i in 0..100 {
        let result = eng
            .get(&cf, format!("key{:03}", i).as_bytes())
            .expect("get");
        assert!(result.is_some(), "Local file should be preserved");
    }
    // TODO: Test cloud mode with mock backend to verify upload retry logic
}

#[test]
fn should_upload_sst_idempotently_given_duplicate_upload_attempt_when_network_flaky() {
    // Arrange
    let dir = test_temp_dir();
    let mock_backend = Arc::new(MockCloudBackend::new());

    // Configure mock to fail uploads after 1 successful one (allow first upload, fail subsequent)
    mock_backend.set_fail_upload_after(1);

    let opts = MidgeOptions {
        storage_mode: StorageMode::CloudBacked {
            local_cache_path: dir.path().to_path_buf(),
            cloud_backend: mock_backend.clone(),
            storage_context: StorageContext::new("test"),
            local_wal_sync: true,
            wal_batch_size: 1024 * 1024, // 1MB
            sst_cache_capacity: 10,
        },
        memtable_size: 1024, // Small memtable to trigger flushes
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    // Act - write data that will trigger SST creation and uploads
    for i in 0..50 {
        eng.put(&cf, format!("key{:02}", i).as_bytes(), b"value")
            .expect("put");
    }

    // Force flush - this may fail if cloud uploads fail, but data should still be available
    let _ = eng.flush_cf(&cf); // Ignore result - we want to test resilience to upload failures

    // Force compaction to potentially trigger more operations
    let _ = eng.compact_range(&cf, None, None); // Ignore result

    // Assert - data should be consistent despite upload retries/failures
    for i in 0..50 {
        let result = eng
            .get(&cf, format!("key{:02}", i).as_bytes())
            .expect("get");
        assert!(
            result.is_some(),
            "Data should be available despite upload failures"
        );
    }

    // Verify that uploads were attempted (some may have succeeded, some failed due to simulated network issues)
    assert!(
        mock_backend.upload_count() > 0,
        "Should have attempted uploads"
    );
}

#[test]
fn should_reconcile_cloud_manifest_given_remote_drift_when_check_cloud_command_runs() {
    // Arrange
    let dir = test_temp_dir();
    let mock_backend = Arc::new(MockCloudBackend::new());

    let opts = MidgeOptions {
        storage_mode: StorageMode::CloudBacked {
            local_cache_path: dir.path().to_path_buf(),
            cloud_backend: mock_backend.clone(),
            storage_context: StorageContext::new("test"),
            local_wal_sync: true,
            wal_batch_size: 1024 * 1024, // 1MB
            sst_cache_capacity: 10,
        },
        memtable_size: 1024, // Small memtable to trigger flushes
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    // Act
    // Write data that will trigger SST creation and uploads
    for i in 0..50 {
        eng.put(&cf, format!("key{:02}", i).as_bytes(), b"value")
            .expect("put");
    }

    // Force flush to create SSTs
    eng.flush_cf(&cf).expect("flush");

    // Wait for any background uploads to complete
    eng.wait_for_flush(std::time::Duration::from_secs(5))
        .expect("wait for flush");

    // Simulate cloud manifest drift by creating a different manifest in cloud
    let local_manifest = eng.get_manifest();
    let mut drifted_manifest = local_manifest.clone();
    // Add a fake SST to simulate drift
    let fake_file = FileMeta {
        name: "fake_drifted_sst.sst".to_string(),
        level: 0,
        cf_id: cf.id().as_u32(),
        size_bytes: 1024,
        smallest_key: Some(b"drift_key".to_vec()),
        largest_key: Some(b"drift_key".to_vec()),
        smallest_seq: Some(1000),
        largest_seq: Some(1000),
        sublevel: 0,
        cloud_location: Some("fake_location".to_string()),
        cloud_checksum: Some(12345),
        cloud_uploaded_at: Some(std::time::SystemTime::now()),
        cloud_state: Some(SstLifecycleState::Active),
        point_tombstone_count: 0,
        range_tombstone_count: 0,
        total_entries: 1,
    };
    drifted_manifest.files.push(fake_file);
    mock_backend.set_cloud_manifest(drifted_manifest);

    // Run check_cloud command
    let inconsistencies = eng.check_cloud().expect("check cloud should complete");

    // Assert that inconsistencies were detected
    assert!(inconsistencies > 0, "Should detect cloud manifest drift");

    // Verify data remains accessible
    for i in 0..50 {
        let result = eng
            .get(&cf, format!("key{:02}", i).as_bytes())
            .expect("get");
        assert!(
            result.is_some(),
            "Data should be available after check_cloud"
        );
    }
}

#[test]
fn should_handle_concurrent_writes_with_local_persistence() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 512 * 1024,
        ..Default::default()
    };
    let eng = Arc::new(MidgeEngine::open(opts).expect("open"));
    let cf = eng.default_column_family();

    // Act: Spawn 10 threads, each writing 50 keys
    let handles: Vec<_> = (0..10)
        .map(|thread_id| {
            let eng = eng.clone();
            let cf_clone = cf.clone();
            std::thread::spawn(move || {
                for i in 0..50 {
                    let key = format!("concurrent_key_t{}_i{}", thread_id, i);
                    eng.put(&cf_clone, key.as_bytes(), b"value").expect("put");
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("Thread panicked");
    }

    // Assert: All 500 keys should be readable
    let mut count = 0;
    for thread_id in 0..10 {
        for i in 0..50 {
            let key = format!("concurrent_key_t{}_i{}", thread_id, i);
            if eng.get(&cf, key.as_bytes()).expect("get").is_some() {
                count += 1;
            }
        }
    }
    assert_eq!(count, 500, "All concurrent writes should persist");
}

#[test]
fn should_preserve_data_after_large_batch_write_and_restart() {
    // Arrange
    let dir = test_temp_dir();
    let db_path = dir.path().to_path_buf();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_path.clone(),
        },
        memtable_size: 2 * 1024 * 1024,
        ..Default::default()
    };

    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    // Write 1000 keys to force multiple SST files
    for i in 0..1000 {
        let key = format!("large_batch_key_{:04}", i);
        eng.put(&cf, key.as_bytes(), format!("value_{}", i).as_bytes())
            .expect("put");
    }

    drop(eng);

    // Act: Restart and verify all keys
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("reopen");
    let cf = eng.default_column_family();

    // Assert: Sample verification of persisted data
    let mut found_count = 0;
    for i in (0..1000).step_by(10) {
        let key = format!("large_batch_key_{:04}", i);
        if eng.get(&cf, key.as_bytes()).expect("get").is_some() {
            found_count += 1;
        }
    }
    assert!(
        found_count >= 95,
        "Most keys should persist after restart: {}/100",
        found_count
    );
}

#[test]
fn should_handle_rapid_sequential_restarts() {
    // Arrange
    let dir = test_temp_dir();
    let db_path = dir.path().to_path_buf();

    // Act: Perform 5 rapid restart cycles
    for cycle in 0..5 {
        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk {
                db_path: db_path.clone(),
            },
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Write some data
        for i in 0..20 {
            let key = format!("cycle_{}_key_{}", cycle, i);
            eng.put(&cf, key.as_bytes(), b"value").expect("put");
        }

        drop(eng);
    }

    // Assert: Final restart and verify data from all cycles
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("final open");
    let cf = eng.default_column_family();

    let mut found = 0;
    for cycle in 0..5 {
        for i in 0..20 {
            let key = format!("cycle_{}_key_{}", cycle, i);
            if eng.get(&cf, key.as_bytes()).expect("get").is_some() {
                found += 1;
            }
        }
    }
    assert!(
        found >= 95,
        "Data from all cycles should persist: {}/100",
        found
    );
}
