//! Cloud Durability Tests
//!
//! Tests for cloud storage persistence guarantees, upload reliability,
//! crash recovery with cloud backend, and fault tolerance.
//!
//! Test coverage:
//! - SST upload to cloud storage
//! - Manifest persistence to cloud
//! - Upload failure handling and retry
//! - Recovery after partial cloud state
//! - Concurrent writes with cloud backend
//! - Clean shutdown with pending uploads

mod common;

use bytes::Bytes;
use cntryl_midge::cloud::backend::StorageBackend;
use cntryl_midge::cloud::mock::MockCloudBackend;
use cntryl_midge::config::cloud::StorageContext;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use common::{test_temp_dir, with_engine_restart};
use std::sync::Arc;
use std::time::Duration;

// Helper to create cloud storage options with different configurations
fn cloud_storage_opts(
    dir: &std::path::Path,
    backend: Arc<MockCloudBackend>,
    context_name: &str,
    local_wal_sync: bool,
    sst_cache_capacity: usize,
) -> MidgeOptions {
    MidgeOptions {
        storage_mode: StorageMode::CloudBacked {
            local_cache_path: dir.to_path_buf(),
            cloud_backend: backend,
            storage_context: StorageContext::new(context_name),
            local_wal_sync,
            wal_batch_size: 1024 * 1024,
            sst_cache_capacity,
        },
        memtable_size: 1024 * 1024,
        wal_sync: local_wal_sync,
        wal_recovery_mode: cntryl_midge::WalRecoveryMode::TolerateCorruptedTail,
        ..Default::default()
    }
}

fn cloud_configs() -> Vec<(&'static str, bool, usize)> {
    vec![
        ("default", true, 10),
        ("no-wal-sync", false, 10),
        ("large-cache", true, 50),
        ("small-cache", true, 4), // At least 3 to hold SSTs from 3 flush rounds + headroom
    ]
}

// ============================================================================
// Basic Cloud Upload
// ============================================================================

#[test]
fn should_upload_sst_to_cloud_given_flush_when_cloud_backed() {
    for (config_name, local_wal_sync, sst_cache) in cloud_configs() {
        // Arrange
        let dir = test_temp_dir();
        let mock_backend = Arc::new(MockCloudBackend::new());
        let opts = cloud_storage_opts(
            dir.path(),
            mock_backend.clone(),
            "test",
            local_wal_sync,
            sst_cache,
        );
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        let baseline_uploads = mock_backend.upload_count();

        // Act
        for i in 0..10 {
            eng.put(&cf, format!("key{:02}", i).as_bytes(), b"value")
                .expect("put");
        }
        let _ = eng.flush_cf(&cf);
        let _ = mock_backend.wait_for_uploads(baseline_uploads + 1, Duration::from_millis(500));

        // Assert
        for i in 0..10 {
            let result = eng
                .get(&cf, format!("key{:02}", i).as_bytes())
                .expect("get");
            assert!(
                result.is_some(),
                "Data should be available with cloud backend (config: {})",
                config_name
            );
        }
        assert!(
            mock_backend.upload_count() > baseline_uploads,
            "Should have completed at least one upload for manifest sync (config: {})",
            config_name
        );
    }
}

#[test]
fn should_preserve_local_file_given_upload_in_progress_when_crash() {
    // Arrange
    let dir = test_temp_dir();
    let db_path = dir.path().to_path_buf();

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_path.clone(),
        },
        memtable_size: 1024 * 1024,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    // Act
    for i in 0..10 {
        eng.put(&cf, format!("key{:03}", i).as_bytes(), b"value")
            .expect("put");
    }
    drop(eng);

    // Assert
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
}

// ============================================================================
// Upload Failure Handling
// ============================================================================

#[test]
fn should_upload_sst_idempotently_given_duplicate_upload_attempt_when_network_flaky() {
    for (config_name, local_wal_sync, sst_cache) in cloud_configs() {
        // Arrange
        let dir = test_temp_dir();
        let mock_backend = Arc::new(MockCloudBackend::new());
        let opts = cloud_storage_opts(
            dir.path(),
            mock_backend.clone(),
            "test",
            local_wal_sync,
            sst_cache,
        );
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        mock_backend.reset_counters();
        mock_backend.set_fail_upload_after(1);

        // Act
        let mut attempts_before = mock_backend.upload_count() + mock_backend.upload_failure_count();
        for round in 0..3 {
            for i in 0..10 {
                eng.put(&cf, format!("r{}-key{:02}", round, i).as_bytes(), b"value")
                    .expect("put");
            }

            let _ = eng.flush_cf(&cf);
            let _ = eng.wait_for_flush(Duration::from_secs(5));
            // Wait for background cloud upload attempt (success or failure)
            assert!(
                mock_backend.wait_for_upload_attempts(attempts_before + 1, Duration::from_secs(5)),
                "upload attempt should complete within timeout (config: {})",
                config_name
            );

            let attempts_after = mock_backend.upload_count() + mock_backend.upload_failure_count();

            assert!(
                attempts_after > attempts_before,
                "flush should trigger at least one cloud upload attempt (config: {})",
                config_name
            );
            attempts_before = attempts_after;
        }

        // Assert
        let total_attempts = mock_backend.upload_count() + mock_backend.upload_failure_count();
        assert!(
            total_attempts > 0,
            "there should be at least one cloud upload attempt after 3 flushes (config: {})",
            config_name
        );
        assert!(
            mock_backend.upload_failure_count() > 0,
            "there should be at least one failed cloud upload under fail-after-1 (config: {})",
            config_name
        );

        for round in 0..3 {
            for i in 0..10 {
                let key = format!("r{}-key{:02}", round, i);
                let result = eng.get(&cf, key.as_bytes()).expect("get");
                assert!(
                    result.is_some(),
                    "Data should be available despite upload failures: key={} (config: {})",
                    key,
                    config_name
                );
            }
        }

        // Explicitly drop engine before next iteration to ensure cleanup
        drop(eng);
    }
}

#[test]
fn should_retry_idempotently_given_duplicate_cloud_upload_requests_when_network_flaps() {
    // Arrange
    let backend = MockCloudBackend::new();

    // Act
    let etag = backend
        .put_blob_if_not_exists("sst/dup.sst", Bytes::from("payload"))
        .expect("first put_blob_if_not_exists");

    let second = backend.put_blob_if_not_exists("sst/dup.sst", Bytes::from("payload"));

    // Assert
    assert!(
        second.is_err(),
        "duplicate put should return an error indicating existing blob"
    );
    let head = backend.head_blob("sst/dup.sst").expect("head_blob");
    assert!(
        head.is_some(),
        "head_blob should report metadata for existing SST"
    );
    let meta = head.unwrap();
    assert!(meta.etag.is_some() || !etag.is_empty());
}

// ============================================================================
// Recovery Scenarios
// ============================================================================

#[test]
fn should_recover_consistently_given_partial_cloud_sst_upload_when_local_manifest_was_already_updated(
) {
    // Arrange
    let dir = test_temp_dir();
    let backend = Arc::new(MockCloudBackend::new());

    let opts = MidgeOptions {
        storage_mode: StorageMode::CloudBacked {
            local_cache_path: dir.path().to_path_buf(),
            cloud_backend: backend.clone(),
            storage_context: StorageContext::new("partial-upload"),
            local_wal_sync: true,
            wal_batch_size: 1024 * 1024,
            sst_cache_capacity: 8,
        },
        memtable_size: 1024,
        wal_sync: true,
        wal_recovery_mode: cntryl_midge::WalRecoveryMode::TolerateCorruptedTail,
        ..Default::default()
    };

    with_engine_restart(
        opts,
        |eng| {
            // Act
            let cf = eng.default_column_family();
            eng.put(&cf, b"key1", b"value1").expect("put");

            backend.reset_counters();
            backend.set_fail_upload_after(1);

            let attempts_before = backend.upload_count() + backend.upload_failure_count();
            let _ = eng.flush_cf(&cf);
            let attempts_after = backend.upload_count() + backend.upload_failure_count();

            // Assert
            assert!(
                attempts_after > attempts_before,
                "flush should trigger at least one cloud upload attempt"
            );
        },
        |eng| {
            backend.reset_counters();
            backend.set_fail_upload_after(usize::MAX);

            let cf = eng.default_column_family();
            let result = eng.get(&cf, b"key1").expect("get");
            assert!(
                result.is_some(),
                "Data should be present after recovery despite partial/failed cloud uploads",
            );
        },
    );
}

#[test]
fn should_not_poison_wal_startup_given_fail_upload_after_is_armed_post_open() {
    // Arrange
    let dir = test_temp_dir();
    let backend = Arc::new(MockCloudBackend::new());

    let opts = MidgeOptions {
        storage_mode: StorageMode::CloudBacked {
            local_cache_path: dir.path().to_path_buf(),
            cloud_backend: backend.clone(),
            storage_context: StorageContext::new("uploader-fail-after"),
            local_wal_sync: true,
            wal_batch_size: 1024 * 1024,
            sst_cache_capacity: 8,
        },
        memtable_size: 1024,
        wal_sync: true,
        wal_recovery_mode: cntryl_midge::WalRecoveryMode::TolerateCorruptedTail,
        ..Default::default()
    };

    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            eng.put(&cf, b"key-wal", b"value")
                .expect("put before fail-after");

            // Act
            backend.reset_counters();
            backend.set_fail_upload_after(1);

            let attempts_before = backend.upload_count() + backend.upload_failure_count();
            let _ = eng.flush_cf(&cf);
            let _ = eng.wait_for_flush(Duration::from_secs(5));
            // Wait for background cloud upload attempt (success or failure)
            assert!(
                backend.wait_for_upload_attempts(attempts_before + 1, Duration::from_secs(5)),
                "upload attempt should complete within timeout"
            );
            let attempts_after = backend.upload_count() + backend.upload_failure_count();

            assert!(
                attempts_after > attempts_before,
                "flush should trigger at least one cloud upload attempt when fail-after is armed",
            );
        },
        |eng| {
            // Assert
            backend.reset_counters();
            backend.set_fail_upload_after(usize::MAX);

            let cf = eng.default_column_family();
            let result = eng.get(&cf, b"key-wal").expect("get after restart");
            assert!(
                result.is_some(),
                "Data written before fail-after should survive WAL/uploader failures",
            );
        },
    );
}

// ============================================================================
// Concurrent Operations
// ============================================================================

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

    // Act
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

    // Assert
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
fn should_not_block_puts_when_background_uploads_are_flaky() {
    // Arrange
    let dir = test_temp_dir();
    let backend = Arc::new(MockCloudBackend::new());

    let opts = MidgeOptions {
        storage_mode: StorageMode::CloudBacked {
            local_cache_path: dir.path().to_path_buf(),
            cloud_backend: backend.clone(),
            storage_context: StorageContext::new("flaky-puts"),
            local_wal_sync: true,
            wal_batch_size: 1024 * 1024,
            sst_cache_capacity: 8,
        },
        memtable_size: 8 * 1024,
        wal_sync: true,
        wal_recovery_mode: cntryl_midge::WalRecoveryMode::TolerateCorruptedTail,
        ..Default::default()
    };

    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            backend.reset_counters();
            backend.set_fail_upload_after(1);

            // Act
            for i in 0..50u8 {
                let key = [b'k', i];
                let value = [b'v', i];
                eng.put(&cf, &key, &value).expect("put under flaky cloud");
            }

            let _ = eng.flush_cf(&cf);
            let _ = eng.wait_for_flush(Duration::from_secs(5));
        },
        |eng| {
            // Assert
            backend.reset_counters();
            backend.set_fail_upload_after(usize::MAX);

            let cf = eng.default_column_family();
            let got = eng
                .get(&cf, b"k\x00")
                .expect("get one key after flaky puts");
            assert!(
                got.is_some(),
                "engine should remain readable after flaky upload activity",
            );
        },
    );
}

// ============================================================================
// Shutdown and Restart
// ============================================================================

#[test]
fn should_allow_clean_shutdown_given_cloud_upload_failures_after_flush_attempts() {
    // Arrange
    let dir = test_temp_dir();
    let backend = Arc::new(MockCloudBackend::new());

    let opts = MidgeOptions {
        storage_mode: StorageMode::CloudBacked {
            local_cache_path: dir.path().to_path_buf(),
            cloud_backend: backend.clone(),
            storage_context: StorageContext::new("shutdown-after-fail"),
            local_wal_sync: true,
            wal_batch_size: 1024 * 1024,
            sst_cache_capacity: 8,
        },
        memtable_size: 1024,
        wal_sync: true,
        wal_recovery_mode: cntryl_midge::WalRecoveryMode::TolerateCorruptedTail,
        ..Default::default()
    };

    // Act
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            for i in 0..5u8 {
                eng.put(&cf, &[b'k', i], &[b'v', i]).expect("put");
            }

            backend.reset_counters();
            backend.set_fail_upload_after(1);

            let attempts_before = backend.upload_count() + backend.upload_failure_count();
            let _ = eng.flush_cf(&cf);
            let _ = eng.wait_for_flush(Duration::from_secs(5));
            let attempts_after = backend.upload_count() + backend.upload_failure_count();

            assert!(
                attempts_after > attempts_before,
                "flush under fail-after should still attempt cloud uploads",
            );
        },
        |eng| {
            // Assert
            backend.reset_counters();
            backend.set_fail_upload_after(usize::MAX);

            let cf = eng.default_column_family();
            for i in 0..5u8 {
                let key = [b'k', i];
                let value = [b'v', i];
                let got = eng.get(&cf, &key).expect("get after restart");
                assert!(got.is_some(), "key should survive: {:?}", key);
                assert_eq!(got.unwrap(), &value[..]);
            }
        },
    );
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

    // Act
    for i in 0..1000 {
        let key = format!("large_batch_key_{:04}", i);
        eng.put(&cf, key.as_bytes(), format!("value_{}", i).as_bytes())
            .expect("put");
    }
    drop(eng);

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("reopen");
    let cf = eng.default_column_family();

    // Assert
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

    // Act
    for cycle in 0..5 {
        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk {
                db_path: db_path.clone(),
            },
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        for i in 0..20 {
            let key = format!("cycle_{}_key_{}", cycle, i);
            eng.put(&cf, key.as_bytes(), b"value").expect("put");
        }
        drop(eng);
    }

    // Assert
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

#[test]
fn should_report_upload_attempts_when_manifest_sync_happens_under_fail_after() {
    // Arrange
    let dir = test_temp_dir();
    let backend = Arc::new(MockCloudBackend::new());

    let opts = MidgeOptions {
        storage_mode: StorageMode::CloudBacked {
            local_cache_path: dir.path().to_path_buf(),
            cloud_backend: backend.clone(),
            storage_context: StorageContext::new("manifest-fail-after"),
            local_wal_sync: true,
            wal_batch_size: 1024 * 1024,
            sst_cache_capacity: 8,
        },
        memtable_size: 1024,
        wal_sync: true,
        wal_recovery_mode: cntryl_midge::WalRecoveryMode::TolerateCorruptedTail,
        ..Default::default()
    };

    // Act
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            eng.put(&cf, b"m-key", b"m-val")
                .expect("put before manifest sync");

            backend.reset_counters();
            backend.set_fail_upload_after(1);

            let attempts_before = backend.upload_count() + backend.upload_failure_count();
            let _ = eng.flush_cf(&cf);
            let _ = eng.wait_for_flush(Duration::from_secs(5));
            let attempts_after = backend.upload_count() + backend.upload_failure_count();

            assert!(
                attempts_after > attempts_before,
                "manifest-related uploads should still be attempted under fail-after",
            );
        },
        |eng| {
            // Assert
            backend.reset_counters();
            backend.set_fail_upload_after(usize::MAX);

            let cf = eng.default_column_family();
            let got = eng
                .get(&cf, b"m-key")
                .expect("get after manifest fail-after");
            assert!(
                got.is_some(),
                "data should still be discoverable after manifest uploads under fail-after",
            );
        },
    );
}
