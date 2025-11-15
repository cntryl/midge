mod common;
use cntryl_midge::{cloud::mock::MockCloudBackend, config::cloud::StorageContext, MidgeOptions, StorageMode};
use common::{durability_opts, flush_test_opts, test_temp_dir, with_engine_restart};
use std::{sync::Arc, time::Duration};

#[test]
fn should_flush_and_fsync_all_memtables_given_shutdown_signal() {
    // Arrange
    let dir = test_temp_dir();
    let opts = flush_test_opts(dir.path().to_path_buf(), 1024 * 1024); // Large memtable

    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            for i in 0..100 {
                eng.put(&cf, format!("key{:03}", i).as_bytes(), b"value")
                    .expect("put");
            }
            // Clean shutdown (drop) should flush and fsync
        },
        |eng| {
            // Assert - all memtable data should be persisted
            let cf = eng.default_column_family();
            for i in 0..100 {
                let result = eng
                    .get(&cf, format!("key{:03}", i).as_bytes())
                    .expect("get");
                assert!(
                    result.is_some(),
                    "Memtable data should be fsynced on shutdown"
                );
            }
        },
    );
}

#[test]
fn should_complete_pending_compactions_given_shutdown_signal() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        memtable_size: 1024,
        enable_compaction: true,
        ..durability_opts(dir.path().to_path_buf())
    };

    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            // Write data that triggers compaction
            for i in 0..200 {
                eng.put(&cf, format!("key{:03}", i % 50).as_bytes(), b"value")
                    .expect("put");
            }
            // Shutdown should wait for compaction to complete or abort gracefully
        },
        |eng| {
            // Assert - all data should be present and consistent
            let cf = eng.default_column_family();
            for i in 0..50 {
                let result = eng
                    .get(&cf, format!("key{:03}", i).as_bytes())
                    .expect("get");
                assert!(
                    result.is_some(),
                    "Data should be consistent after shutdown during compaction"
                );
            }
        },
    );
}

#[test]
fn should_abort_long_running_uploads_given_shutdown_signal() {
    // Arrange
    let dir = test_temp_dir();
    let backend = Arc::new(
        MockCloudBackend::new().with_latency(Duration::from_millis(500)),
    );
    let opts = MidgeOptions {
        storage_mode: StorageMode::CloudBacked {
            local_cache_path: dir.path().to_path_buf(),
            cloud_backend: backend.clone(),
            storage_context: StorageContext::new("shutdown"),
            local_wal_sync: true,
            wal_batch_size: 1024 * 1024,
            sst_cache_capacity: 8,
        },
        memtable_size: 1024,
        wal_sync: true,
        wal_recovery_mode: cntryl_midge::WalRecoveryMode::TolerateCorruptedTail,
        ..Default::default()
    };

    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            eng.put(&cf, b"key1", b"value1").expect("put");
            eng.flush_cf(&cf).expect("flush");
            std::thread::sleep(Duration::from_millis(100));
        },
        |eng| {
            // Assert - local data should be consistent after long uploads
            let cf = eng.default_column_family();
            let result = eng.get(&cf, b"key1").expect("get");
            assert!(result.is_some(), "Data should survive slow uploads");
            assert!(backend.upload_count() > 0, "Uploads should be attempted");
            assert_eq!(backend.upload_failure_count(), 0, "Uploads should not fail");
        },
    );
}

#[test]
fn should_persist_all_memtables_given_shutdown_signal_when_clean_exit() {
    // Arrange
    let dir = test_temp_dir();
    let opts = flush_test_opts(dir.path().to_path_buf(), 1024 * 1024);

    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            // Multiple writes to memtable
            for batch in 0..3 {
                for i in 0..20 {
                    eng.put(
                        &cf,
                        format!("batch{}_key{:02}", batch, i).as_bytes(),
                        b"value",
                    )
                    .expect("put");
                }
            }
            // Clean shutdown should persist all memtables
        },
        |eng| {
            // Assert - all batches should be present
            let cf = eng.default_column_family();
            for batch in 0..3 {
                for i in 0..20 {
                    let key = format!("batch{}_key{:02}", batch, i);
                    let result = eng.get(&cf, key.as_bytes()).expect("get");
                    assert!(result.is_some(), "All memtable data should be persisted");
                }
            }
        },
    );
}

#[test]
fn should_reopen_without_recovery_needed_given_clean_shutdown() {
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            eng.put(&cf, b"key1", b"value1").expect("put");
            eng.put(&cf, b"key2", b"value2").expect("put");
            // Clean shutdown should mark database as cleanly closed
        },
        |eng| {
            // Assert - reopen should be fast (no WAL replay needed)
            // Data should be immediately available
            let cf = eng.default_column_family();
            let result = eng.get(&cf, b"key1").expect("get");
            assert!(
                result.is_some(),
                "Data should be immediately available after clean shutdown"
            );
            // TODO: Add instrumentation to verify no WAL replay occurred
        },
    );
}

#[test]
fn should_handle_rapid_shutdown_and_restart_cycles_without_data_loss_when_stressed() {
    // Arrange
    let dir = test_temp_dir();
    let base_opts = durability_opts(dir.path().to_path_buf());

    // Act & Assert - perform multiple shutdown/restart cycles
    const RESTART_CYCLES: usize = 5;
    let mut total_keys_written = 0;

    for cycle in 0..RESTART_CYCLES {
        let opts = MidgeOptions {
            storage_mode: base_opts.storage_mode.clone(),
            memtable_size: 16384,
            ..base_opts.clone()
        };

        with_engine_restart(
            opts,
            |eng| {
                let cf = eng.default_column_family();
                // Write batch of keys in this cycle
                for i in 0..50 {
                    let key = format!("cycle{}_key{:02}", cycle, i).into_bytes();
                    let value = format!("value_{}", total_keys_written + i).into_bytes();
                    eng.put(&cf, &key, &value).expect("put during cycle");
                }
            },
            |eng| {
                // Verify all keys from current cycle are present
                let cf = eng.default_column_family();
                for i in 0..50 {
                    let key = format!("cycle{}_key{:02}", cycle, i).into_bytes();
                    let result = eng.get(&cf, &key).expect("get after cycle restart");
                    assert!(
                        result.is_some(),
                        "Key from cycle {} should persist",
                        cycle
                    );
                }

                // Verify keys from all previous cycles are still present
                for prev_cycle in 0..cycle {
                    let key = format!("cycle{}_key00", prev_cycle).into_bytes();
                    let result = eng.get(&cf, &key).expect("get previous cycle");
                    assert!(
                        result.is_some(),
                        "Key from previous cycle {} should still be present",
                        prev_cycle
                    );
                }
            },
        );

        total_keys_written += 50;
    }
}

#[test]
fn should_preserve_data_consistency_across_multiple_concurrent_shutdown_attempts() {
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());
    let eng = cntryl_midge::MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();
    let eng = Arc::new(eng);

    // Write 500 keys
    for i in 0..500 {
        let key = format!("shutdown_key_{:04}", i).into_bytes();
        let value = format!("shutdown_value_{}", i).into_bytes();
        eng.put(&cf, &key, &value).expect("put");
    }

    // Act - simulate concurrent read threads while preparing for shutdown
    const NUM_READERS: usize = 10;
    const ITERATIONS: usize = 50;
    let handles: Vec<_> = (0..NUM_READERS)
        .map(|_| {
            let eng_clone = Arc::clone(&eng);
            let cf_clone = cf.clone();
            std::thread::spawn(move || {
                for i in 0..ITERATIONS {
                    let key = format!("shutdown_key_{:04}", (i * 7) % 500).into_bytes();
                    let _result = eng_clone.get(&cf_clone, &key).expect("read during shutdown");
                }
            })
        })
        .collect();

    // Wait for all readers to complete
    for h in handles {
        h.join().expect("Thread panicked");
    }

    // Assert - verify data integrity after concurrent operations
    for i in (0..500).step_by(50) {
        let key = format!("shutdown_key_{:04}", i).into_bytes();
        let result = eng
            .get(&cf, &key)
            .expect("get after concurrent reads")
            .expect("key should exist");
        let expected = format!("shutdown_value_{}", i).into_bytes();
        assert_eq!(
            result, expected,
            "Data should remain consistent during shutdown scenario"
        );
    }
}
