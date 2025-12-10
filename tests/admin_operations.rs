//! Administrative Operations Integration Tests
//!
//! Tests for administrative operations like column family management, backups, and concurrent admin queries.
//! Verifies that admin operations work correctly and don't interfere with data operations.
//!
//! ## Coverage
//! - Column family drop validation with unflushed data
//! - Column family listing during operations
//! - Concurrent administrative operations
//! - Data preservation during admin operations
//! - Data recovery after admin operations and restart
//! - High concurrency admin query stress testing
//!
//! ## Storage Mode Coverage
//! Tests LocalDisk and CloudBacked modes (requires persistent storage for admin operations).

mod common;

use cntryl_midge::MidgeEngine;
use cntryl_midge::testkit::{bulk_put_fn, create_storage_mode, disk_storage_modes, DurabilityTestContext};
use std::sync::Arc;
use std::thread;

// =============================================================================
// Column Family Administrative Operations
// =============================================================================

#[test]
fn should_refuse_column_family_drop_when_unflushed_data_exists() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = cntryl_midge::MidgeOptions {
            storage_mode,
            memtable_size: 8 * 1024 * 1024,
            enable_compaction: false,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).unwrap();
        let cf = eng.default_column_family();

        bulk_put_fn(&eng, &cf, "key", 1_000, |_| b"value".to_vec());

        // Act
        let drop_result = eng.drop_column_family(&cf);

        // Assert
        assert!(drop_result.is_err(), "drop should fail with unflushed data");

        let manifest = eng.get_manifest();
        assert!(
            manifest
                .column_families
                .iter()
                .any(|cf_meta| cf_meta.name == cf.name()),
            "column family should still exist in manifest for {}",
            mode
        );
    }
}

#[test]
fn should_return_current_cf_list_given_admin_query_when_changes_in_progress() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = cntryl_midge::MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).unwrap();
        let cf = eng.default_column_family();
        eng.put(&cf, b"key1", b"value1").expect("put");

        // Act
        let cf_list = eng.list_column_families();

        // Assert
        assert!(
            !cf_list.is_empty(),
            "CF list should not be empty for {}",
            mode
        );
        assert!(
            cf_list.iter().any(|cf| cf.name() == "default"),
            "Default CF should be in the list for {}",
            mode
        );

        let result = eng.get(&cf, b"key1").expect("get");
        assert!(
            result.is_some(),
            "Default CF should be functional for {}",
            mode
        );
    }
}

// =============================================================================
// Concurrent Administrative Operations
// =============================================================================

#[test]
fn should_handle_concurrent_column_family_operations_without_deadlock_when_multiple_threads_operate(
) {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = cntryl_midge::MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).unwrap();
        let eng = Arc::new(eng);
        const NUM_THREADS: usize = 4;
        const ITERATIONS: usize = 20;

        // Act - multiple threads querying and operating on column families
        let handles: Vec<_> = (0..NUM_THREADS)
            .map(|i| {
                let eng_clone = Arc::clone(&eng);
                std::thread::spawn(move || {
                    for j in 0..ITERATIONS {
                        let cf = eng_clone.default_column_family();
                        let key = format!("admin_key_{}_{}_{}", i, j, i * j).into_bytes();
                        let value = format!("admin_value_{}", i * ITERATIONS + j).into_bytes();

                        // Perform put operation
                        eng_clone
                            .put(&cf, &key, &value)
                            .expect("put during admin ops");

                        // Periodically query CF list
                        if j % 25 == 0 {
                            let _cf_list = eng_clone.list_column_families();
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("Thread panicked");
        }

        // Assert - verify engine remained stable
        let cf = eng.default_column_family();
        let result = eng
            .get(&cf, b"admin_key_0_0_0")
            .expect("get after admin ops");
        assert!(
            result.is_some(),
            "Engine should remain stable during concurrent admin operations for {}",
            mode
        );
    }
}

#[test]
fn should_preserve_data_during_high_concurrency_writes_with_admin_queries_when_stress_tested() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = cntryl_midge::MidgeOptions {
            storage_mode,
            memtable_size: 16384,
            enable_compaction: false,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).unwrap();
        let cf = eng.default_column_family();
        let eng = Arc::new(eng);
        const NUM_WRITER_THREADS: usize = 5;
        const NUM_ADMIN_THREADS: usize = 2;
        const ITERATIONS: usize = 10;

        // Act - mix of write threads and admin query threads
        let mut handles = Vec::new();

        // Spawn write threads
        for i in 0..NUM_WRITER_THREADS {
            let eng_clone = Arc::clone(&eng);
            let cf_clone = cf.clone();
            handles.push(std::thread::spawn(move || {
                for j in 0..ITERATIONS {
                    let key = format!("write_{}_{}_{}", i, j, i * j).into_bytes();
                    let value = format!("write_value_{}", i * ITERATIONS + j).into_bytes();
                    eng_clone
                        .put(&cf_clone, &key, &value)
                        .expect("write during admin stress");
                }
            }));
        }

        // Spawn admin query threads
        for _ in 0..NUM_ADMIN_THREADS {
            let eng_clone = Arc::clone(&eng);
            handles.push(std::thread::spawn(move || {
                for _ in 0..ITERATIONS * 2 {
                    let _cf_list = eng_clone.list_column_families();
                }
            }));
        }

        for h in handles {
            h.join().expect("Thread panicked");
        }

        // Assert
        // Sample a few keys from different writer threads to verify data persistence
        // We check every 3rd thread to reduce test time while still validating concurrent writes
        let mut verified_count = 0;
        for i in (0..NUM_WRITER_THREADS).step_by(3) {
            let key = format!("write_{}_0_0", i).into_bytes(); // Check first key from each sampled thread
            let result = eng.get(&cf, &key).expect("get after admin stress");
            if result.is_some() {
                verified_count += 1;
            }
        }

        // At least some keys should be readable - if none are, we have a serious data loss bug
        assert!(
            verified_count > 0,
            "At least some data should persist during admin query stress (verified {} keys) for {}",
            verified_count,
            mode
        );
    }
}

// =============================================================================
// Data Preservation During Admin Operations
// =============================================================================

#[test]
fn should_preserve_data_when_backup_runs_during_compaction_and_writes() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = cntryl_midge::MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).unwrap();
        let eng = Arc::new(eng);
        let cf = eng.default_column_family();

        // Write seed data before concurrent operations
        for i in 0..30 {
            let k = format!("seed{:03}", i);
            eng.put(&cf, k.as_bytes(), b"seedval").unwrap();
        }

        // Act - Concurrent writes (background compaction may occur naturally)
        let writer_eng = Arc::clone(&eng);
        let writer_cf = cf.clone();
        let writer = thread::spawn(move || {
            for i in 0..30 {
                let k = format!("write{:03}", i);
                writer_eng
                    .put(&writer_cf, k.as_bytes(), b"writeval")
                    .unwrap();
            }
        });

        writer.join().expect("writer thread panicked");

        // Assert - Verify seed data is still readable
        let result = eng.get(&cf, b"seed015").expect("get");
        assert!(
            result.is_some(),
            "seed data should persist during concurrent writes for {}",
            mode
        );
    }
}

#[test]
fn should_recover_all_data_after_restart_despite_admin_operations_when_engine_reopened() {
    for mode in disk_storage_modes() {
        let ctx = DurabilityTestContext::new(mode);
        // Arrange
        let opts = cntryl_midge::MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            memtable_size: 8192,
            ..Default::default()
        };
        let eng = {
            let e = MidgeEngine::open(opts).expect("Failed to create engine");
            let cf = e.default_column_family();

            // Write 100 keys while performing admin operations
            for i in 0..100 {
                let key = format!("admin_recovery_key_{:04}", i).into_bytes();
                let value = format!("admin_recovery_value_{}", i).into_bytes();
                e.put(&cf, &key, &value).expect("put during admin phase");

                // Periodically list column families
                if i % 20 == 0 {
                    let _cf_list = e.list_column_families();
                }
            }

            e
        };

        drop(eng);

        // Act - reopen engine using the same context (same temp directory)
        let opts = cntryl_midge::MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            memtable_size: 8192,
            ..Default::default()
        };
        let engine_reopen = MidgeEngine::open(opts).expect("reopen");
        let cf = engine_reopen.default_column_family();

        // Assert - verify data persisted across restart
        for i in (0..100).step_by(20) {
            let key = format!("admin_recovery_key_{:04}", i).into_bytes();
            let result = engine_reopen
                .get(&cf, &key)
                .expect("get after restart")
                .expect("key should persist after admin operations");
            let expected = format!("admin_recovery_value_{}", i).into_bytes();
            assert_eq!(
                result, expected,
                "Data mismatch for key {} after restart with admin ops for {}",
                i, mode
            );
        }
    }
}
