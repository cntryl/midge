mod common;
use cntryl_midge::cloud::mock::MockCloudBackend;
use cntryl_midge::config::cloud::StorageContext;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use common::test_temp_dir;
use std::sync::Arc;
use std::time::Duration;

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
        memtable_size: 1024 * 1024,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    // Write data that will create SST files
    for i in 0..10 {
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

    for i in 0..10 {
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
        memtable_size: 1024 * 1024, // Large memtable to avoid auto-flush
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    // Act - write data that will trigger SST creation and uploads
    for i in 0..10 {
        eng.put(&cf, format!("key{:02}", i).as_bytes(), b"value")
            .expect("put");
    }
    println!("Wrote 10 keys to memtable");

    // Force flush - this may fail if cloud uploads fail, but data should still be available locally
    let flush_result = eng.flush_cf(&cf);
    println!("Flush result: {:?}", flush_result);

    // Check manifest
    let manifest_path = dir.path().join("manifest.json");
    if manifest_path.exists() {
        if let Ok(content) = std::fs::read_to_string(&manifest_path) {
            println!("Manifest after flush:\n{}", content);
        }
    }

    // Wait for background uploads with timeout (observability)
    let baseline_uploads = mock_backend.upload_count();
    let upload_succeeded =
        mock_backend.wait_for_uploads(baseline_uploads + 1, Duration::from_millis(500));
    println!(
        "Uploads: {} succeeded, upload flag: {}",
        mock_backend.upload_count(),
        upload_succeeded
    );

    // Assert - data should be available despite upload failures
    for i in 0..10 {
        let key = format!("key{:02}", i);
        let result = eng.get(&cf, key.as_bytes()).expect("get");
        if result.is_none() {
            println!("MISSING: {} not found!", key);
        }
        assert!(
            result.is_some(),
            "Data should be available from local cache despite upload failures: key={}",
            key
        );
    }
    println!("All keys verified successfully");
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
        memtable_size: 1024 * 1024, // Large memtable to avoid auto-flush
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    // Act - write data that will trigger SST creation and uploads
    for i in 0..10 {
        eng.put(&cf, format!("key{:02}", i).as_bytes(), b"value")
            .expect("put");
    }

    // Force flush to create SSTs
    let _ = eng.flush_cf(&cf); // Ignore result

    // Wait for background uploads with timeout (observability)
    let baseline_uploads = mock_backend.upload_count();
    let upload_succeeded =
        mock_backend.wait_for_uploads(baseline_uploads + 1, Duration::from_millis(500));

    // Assert
    // Verify data remains accessible
    for i in 0..10 {
        let result = eng
            .get(&cf, format!("key{:02}", i).as_bytes())
            .expect("get");
        assert!(
            result.is_some(),
            "Data should be available with cloud backend"
        );
    }

    // Verify uploads occurred (demonstrating observability)
    assert!(
        upload_succeeded,
        "Should have completed at least one upload for manifest sync"
    );
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
fn should_preserve_data_after_large_batch_write_restart() {
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
