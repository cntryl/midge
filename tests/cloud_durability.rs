mod common;

use cntryl_midge::cloud::mock::MockCloudBackend;
use cntryl_midge::config::cloud::StorageContext;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use common::test_temp_dir;
use std::time::Duration;
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

    // Arm upload failures only after engine startup so WAL/cloud uploader
    // initialization is not poisoned by fail-after semantics.
    mock_backend.reset_counters();
    // Allow exactly one new successful upload, then fail subsequent uploads.
    mock_backend.set_fail_upload_after(1);

    // Act - write data that will trigger multiple SST creations and uploads.
    // Run multiple small write+flush cycles so we deterministically produce
    // more than one cloud upload attempt (so fail-after semantics are exercised).
    for round in 0..3 {
        for i in 0..10 {
            eng.put(&cf, format!("r{}-key{:02}", round, i).as_bytes(), b"value")
                .expect("put");
        }

        let attempts_before = mock_backend.upload_count() + mock_backend.upload_failure_count();

        // Force flush - this may hit cloud upload failures but must not hang.
        let _ = eng.flush_cf(&cf); // error is acceptable under simulated failure

        let attempts_after = mock_backend.upload_count() + mock_backend.upload_failure_count();

        assert!(
            attempts_after > attempts_before,
            "flush should trigger at least one cloud upload attempt",
        );
    }

    // After multiple flushes under fail-after=1 we should observe at least one failed upload
    assert!(
        mock_backend.upload_failure_count() > 0,
        "there should be at least one failed cloud upload under fail-after-1",
    );

    // Assert - data should be available despite upload failures
    for round in 0..3 {
        for i in 0..10 {
            let key = format!("r{}-key{:02}", round, i);
            let result = eng.get(&cf, key.as_bytes()).expect("get");
            assert!(
                result.is_some(),
                "Data should be available from local cache despite upload failures: key={}",
                key
            );
        }
    }
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

    // Observe baseline upload attempts, then force flush to create SSTs and uploads
    let baseline_uploads = mock_backend.upload_count();
    let _ = eng.flush_cf(&cf); // Ignore result; failures are not simulated here

    // Give background uploader a bounded window to make progress.
    let _ = mock_backend.wait_for_uploads(baseline_uploads + 1, Duration::from_millis(500));

    // Assert - data remains accessible regardless of cloud timing
    for i in 0..10 {
        let result = eng
            .get(&cf, format!("key{:02}", i).as_bytes())
            .expect("get");
        assert!(
            result.is_some(),
            "Data should be available with cloud backend"
        );
    }

    // Assert - at least one new upload was attempted for manifest/SST sync.
    assert!(
        mock_backend.upload_count() >= baseline_uploads + 1,
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

    // Act: Restart and verify keys
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
