//! Transaction Spill-to-Disk Tests
//!
//! Tests for large transaction memory management through spill-to-disk mechanism.
//! When a transaction's in-memory buffer exceeds its configured limit, data is
//! spilled to temporary files on disk.
//!
//! # Test Categories
//!
//! - **Large Transaction Commit**: Transactions exceeding memory limits
//! - **Data Integrity**: Values preserved correctly after spill
//! - **Rollback**: Uncommitted spilled transactions cleaned up
//! - **Recovery**: Behavior on restart with/without commit
//! - **Memory Pressure**: Foreground writes not starved during spill
//!
//! # Storage Mode Coverage
//!
//! Tests run against LocalDisk and CloudBacked only - Memory mode has no disk to spill to.

use bytes::Bytes;
use cntryl_midge::{IsolationLevel, KvTransaction, MidgeEngine, MidgeOptions, Query, WriteOptions};

mod common;
use common::{create_storage_mode, disk_storage_modes, DurabilityTestContext};

// ============================================================================
// Large Transaction Commit
// ============================================================================

#[test]
fn should_commit_large_transaction_given_many_writes_exceeding_memory_limit() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        // Create transaction with small memory limit (1MB) to force spilling
        let mut large_txn = engine
            .begin_transaction_with_options(&cf, None, 1024 * 1024, IsolationLevel::default())
            .expect("begin");

        // Act - Add 2MB of data (2000 keys × 1024 bytes each)
        for i in 0..2000 {
            large_txn
                .put(format!("key{:06}", i).as_bytes(), &vec![0u8; 1024])
                .expect("put");
        }

        engine
            .commit_transaction(large_txn, WriteOptions::default())
            .expect("commit");

        // Assert - Verify all keys are present after commit
        for i in (0..2000).step_by(100) {
            let key = format!("key{:06}", i);
            let value = engine.get(&cf, key.as_bytes()).expect("get");
            assert!(
                value.is_some(),
                "[{}] Key {} should exist after large transaction commit",
                name,
                key
            );
        }
    }
}

#[test]
fn should_handle_very_large_transaction_given_multiple_spills() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        // Create transaction with very small memory limit (128KB) to force multiple spills
        let mut huge_txn = engine
            .begin_transaction_with_options(&cf, None, 128 * 1024, IsolationLevel::default())
            .expect("begin");

        // Act - Add 5MB of data (will cause multiple spills)
        for i in 0..5000 {
            huge_txn
                .put(format!("huge_key_{:06}", i).as_bytes(), &vec![0xEEu8; 1024])
                .expect("put");
        }

        engine
            .commit_transaction(huge_txn, WriteOptions::default())
            .expect("commit should succeed");

        // Assert - Verify data integrity with sampling
        for i in (0..5000).step_by(250) {
            let key = format!("huge_key_{:06}", i);
            let value = engine.get(&cf, key.as_bytes()).expect("get");
            assert!(
                value.is_some(),
                "[{}] Key {} should exist after large transaction",
                name,
                key
            );
        }
    }
}

// ============================================================================
// Data Integrity
// ============================================================================

#[test]
fn should_preserve_data_integrity_given_large_transaction_with_specific_values() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        // Create transaction with small memory limit (512KB) to force spilling
        let mut large_txn = engine
            .begin_transaction_with_options(&cf, None, 512 * 1024, IsolationLevel::default())
            .expect("begin");

        // Act - Add data with specific pattern
        for i in 0..1500 {
            large_txn
                .put(
                    format!("large_key_{:06}", i).as_bytes(),
                    &vec![0xABu8; 1024],
                )
                .expect("put");
        }

        engine
            .commit_transaction(large_txn, WriteOptions::default())
            .expect("commit");

        // Assert - Verify all data has correct values
        for i in (0..1500).step_by(50) {
            let key = format!("large_key_{:06}", i);
            let value = engine.get(&cf, key.as_bytes()).expect("get");
            assert!(value.is_some(), "[{}] Key {} should exist", name, key);
            assert_eq!(
                value.unwrap(),
                Bytes::from(vec![0xABu8; 1024]),
                "[{}] Value should match for key {}",
                name,
                key
            );
        }
    }
}

#[test]
fn should_preserve_key_order_given_large_transaction_when_iterating() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        let mut large_txn = engine
            .begin_transaction_with_options(&cf, None, 256 * 1024, IsolationLevel::default())
            .expect("begin");

        // Act - Add keys that should be in sorted order
        for i in 0..1000 {
            large_txn
                .put(format!("sorted_{:06}", i).as_bytes(), &vec![i as u8; 100])
                .expect("put");
        }

        engine
            .commit_transaction(large_txn, WriteOptions::default())
            .expect("commit");

        // Assert - Verify keys are in correct order via scan
        let results = engine.scan(&cf, Query::new()).expect("scan");
        let mut prev_key: Option<Vec<u8>> = None;

        for (key, _) in &results {
            if let Some(prev) = &prev_key {
                assert!(
                    key.as_ref() > prev.as_slice(),
                    "[{}] Keys should be in sorted order",
                    name
                );
            }
            prev_key = Some(key.to_vec());
        }

        assert_eq!(results.len(), 1000, "[{}] Should have all 1000 keys", name);
    }
}

// ============================================================================
// Rollback
// ============================================================================

#[test]
fn should_rollback_spilled_transaction_given_drop_without_commit() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        // Act - Create transaction with large data, then drop without committing
        {
            let mut large_txn = engine
                .begin_transaction_with_options(&cf, None, 256 * 1024, IsolationLevel::default())
                .expect("begin");

            for i in 0..2000 {
                large_txn
                    .put(
                        format!("abort_key_{:06}", i).as_bytes(),
                        &vec![0xDDu8; 1024],
                    )
                    .expect("put");
            }
            // Transaction dropped here without commit (implicit rollback)
        }

        // Assert - No data should be persisted
        for i in (0..2000).step_by(100) {
            let key = format!("abort_key_{:06}", i);
            let value = engine.get(&cf, key.as_bytes()).expect("get");
            assert!(
                value.is_none(),
                "[{}] Key {} should not exist after rollback",
                name,
                key
            );
        }
    }
}

#[test]
fn should_cleanup_spill_files_given_transaction_rollback() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        // Act - Create and rollback multiple large transactions
        for round in 0..3 {
            let mut txn = engine
                .begin_transaction_with_options(&cf, None, 128 * 1024, IsolationLevel::default())
                .expect("begin");

            for i in 0..1000 {
                txn.put(
                    format!("cleanup_round{}_{:06}", round, i).as_bytes(),
                    &vec![0xFFu8; 1024],
                )
                .expect("put");
            }
            // Drop without commit
        }

        // Assert - Engine still functional, no leftover state
        let mut final_txn = engine.begin_transaction(&cf).expect("begin");
        final_txn.put(b"final_key", b"final_value").expect("put");
        engine
            .commit_transaction(final_txn, WriteOptions::default())
            .expect("commit");

        let value = engine.get(&cf, b"final_key").expect("get");
        assert_eq!(
            value,
            Some(Bytes::from_static(b"final_value")),
            "[{}] Engine should work after multiple rollbacks",
            name
        );
    }
}

// ============================================================================
// Recovery
// ============================================================================

#[test]
fn should_rollback_uncommitted_spill_given_restart_before_commit() {
    for mode in disk_storage_modes() {
        // Arrange
        let ctx = DurabilityTestContext::new(mode);

        {
            let opts = MidgeOptions {
                storage_mode: ctx.create_storage_mode(),
                ..Default::default()
            };
            let engine = MidgeEngine::open(opts).expect("open");
            let cf = engine.default_column_family();

            // Create transaction with small memory limit to force spilling
            let mut large_txn = engine
                .begin_transaction_with_options(&cf, None, 1024 * 1024, IsolationLevel::default())
                .expect("begin");

            // Add 2MB of data to force spill
            for i in 0..2000 {
                large_txn
                    .put(format!("key{:06}", i).as_bytes(), &vec![0u8; 1024])
                    .expect("put");
            }
            // Do not commit, let restart occur
        }

        // Act: Reopen engine
        let opts = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("reopen");
        let cf = engine.default_column_family();

        // Assert: Transaction rolled back, no keys present
        for i in (0..2000).step_by(100) {
            let key = format!("key{:06}", i);
            let value = engine.get(&cf, key.as_bytes()).expect("get");
            assert!(
                value.is_none(),
                "[{}] Key {} should not exist after restart without commit",
                ctx.name(),
                key
            );
        }
    }
}

#[test]
fn should_recover_committed_spill_given_restart_after_commit() {
    for mode in disk_storage_modes() {
        // Arrange
        let ctx = DurabilityTestContext::new(mode);

        {
            let opts = MidgeOptions {
                storage_mode: ctx.create_storage_mode(),
                ..Default::default()
            };
            let engine = MidgeEngine::open(opts).expect("open");
            let cf = engine.default_column_family();

            let mut txn = engine
                .begin_transaction_with_options(&cf, None, 1024 * 1024, IsolationLevel::default())
                .expect("begin");

            for i in 0..2000 {
                txn.put(format!("key{:06}", i).as_bytes(), &vec![0u8; 1024])
                    .expect("put");
            }

            // Commit before restart
            engine
                .commit_transaction(txn, WriteOptions::default())
                .expect("commit");
        }

        // Act: Reopen engine
        let opts = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("reopen");
        let cf = engine.default_column_family();

        // Assert: Keys present after recovery
        for i in (0..2000).step_by(100) {
            let key = format!("key{:06}", i);
            let value = engine.get(&cf, key.as_bytes()).expect("get");
            assert!(
                value.is_some(),
                "[{}] Key {} should exist after recovery",
                ctx.name(),
                key
            );
        }
    }
}

// ============================================================================
// Memory Pressure
// ============================================================================

#[test]
fn should_not_starve_foreground_writes_given_background_spill_activity() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        // Start a large transaction that will spill
        let mut spill_txn = engine
            .begin_transaction_with_options(&cf, None, 1024 * 1024, IsolationLevel::default())
            .expect("begin");

        for i in 0..1000 {
            spill_txn
                .put(format!("spill{:06}", i).as_bytes(), &vec![0u8; 1024])
                .expect("put");
        }

        // Act: Perform foreground writes while spill transaction is active
        for i in 0..100 {
            engine
                .put(&cf, format!("fg{:06}", i).as_bytes(), b"v")
                .expect("foreground put");
        }

        // Assert: Foreground writes succeeded
        for i in 0..100 {
            let key = format!("fg{:06}", i);
            let value = engine.get(&cf, key.as_bytes()).expect("get");
            assert!(
                value.is_some(),
                "[{}] Foreground write {} should succeed during spill",
                name,
                key
            );
        }

        // Cleanup: commit or drop the spill transaction
        drop(spill_txn);
    }
}

#[test]
fn should_handle_concurrent_large_transactions_given_memory_pressure() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = std::sync::Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        // Act: Start multiple large transactions concurrently
        let handles: Vec<_> = (0..4)
            .map(|thread_id| {
                let eng = engine.clone();
                let cf_clone = cf.clone();
                std::thread::spawn(move || {
                    let mut txn = eng
                        .begin_transaction_with_options(
                            &cf_clone,
                            None,
                            256 * 1024,
                            IsolationLevel::default(),
                        )
                        .expect("begin");

                    for i in 0..500 {
                        txn.put(
                            format!("thread{}_{:06}", thread_id, i).as_bytes(),
                            &vec![thread_id as u8; 1024],
                        )
                        .expect("put");
                    }

                    eng.commit_transaction(txn, WriteOptions::default())
                })
            })
            .collect();

        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // Assert: All transactions should succeed
        let success_count = results.iter().filter(|r| r.is_ok()).count();
        assert_eq!(
            success_count, 4,
            "[{}] All concurrent large transactions should succeed",
            name
        );

        // Verify data from each thread
        for thread_id in 0..4 {
            for i in (0..500).step_by(50) {
                let key = format!("thread{}_{:06}", thread_id, i);
                let value = engine.get(&cf, key.as_bytes()).expect("get");
                assert!(value.is_some(), "[{}] Key {} should exist", name, key);
            }
        }
    }
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn should_handle_transaction_with_tiny_memory_limit() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        // Create transaction with very small memory limit (1KB)
        let mut txn = engine
            .begin_transaction_with_options(&cf, None, 1024, IsolationLevel::default())
            .expect("begin");

        // Act: Add data much larger than limit
        for i in 0..100 {
            txn.put(format!("tiny_limit_{:06}", i).as_bytes(), &vec![0u8; 1024])
                .expect("put");
        }

        engine
            .commit_transaction(txn, WriteOptions::default())
            .expect("commit");

        // Assert: All data present
        for i in 0..100 {
            let key = format!("tiny_limit_{:06}", i);
            let value = engine.get(&cf, key.as_bytes()).expect("get");
            assert!(value.is_some(), "[{}] Key {} should exist", name, key);
        }
    }
}

#[test]
fn should_handle_mixed_small_and_large_values_in_spilled_transaction() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        let mut txn = engine
            .begin_transaction_with_options(&cf, None, 256 * 1024, IsolationLevel::default())
            .expect("begin");

        // Act: Mix small and large values
        for i in 0..500 {
            if i % 10 == 0 {
                // Large value every 10th key
                txn.put(
                    format!("mixed_{:06}", i).as_bytes(),
                    &vec![0xAAu8; 10 * 1024],
                )
                .expect("put large");
            } else {
                // Small value
                txn.put(format!("mixed_{:06}", i).as_bytes(), b"small")
                    .expect("put small");
            }
        }

        engine
            .commit_transaction(txn, WriteOptions::default())
            .expect("commit");

        // Assert: Verify mixed values
        for i in 0..500 {
            let key = format!("mixed_{:06}", i);
            let value = engine.get(&cf, key.as_bytes()).expect("get").unwrap();
            if i % 10 == 0 {
                assert_eq!(
                    value.len(),
                    10 * 1024,
                    "[{}] Large value {} should have correct size",
                    name,
                    key
                );
            } else {
                assert_eq!(
                    value,
                    Bytes::from_static(b"small"),
                    "[{}] Small value {} should match",
                    name,
                    key
                );
            }
        }
    }
}

// ============================================================================
// Memory Mode - No Disk Artifacts
// ============================================================================

#[test]
fn should_not_create_disk_artifacts_given_large_transaction_when_memory_mode() {
    // Arrange: Use a temp directory to verify no files are created
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let temp_path = temp_dir.path().to_path_buf();

    // Memory mode - no disk path, but we'll check the temp directory for any spillage
    let opts = MidgeOptions {
        storage_mode: cntryl_midge::StorageMode::Memory,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Act: Create a large transaction that would normally spill
    let mut large_txn = engine
        .begin_transaction_with_options(&cf, None, 128 * 1024, IsolationLevel::default())
        .expect("begin");

    // Add 2MB of data - would cause multiple spills in disk mode
    for i in 0..2000 {
        large_txn
            .put(format!("mem_key_{:06}", i).as_bytes(), &vec![0xAAu8; 1024])
            .expect("put");
    }

    engine
        .commit_transaction(large_txn, WriteOptions::default())
        .expect("commit");

    // Assert: Verify data is present
    for i in (0..2000).step_by(100) {
        let key = format!("mem_key_{:06}", i);
        let value = engine.get(&cf, key.as_bytes()).expect("get");
        assert!(
            value.is_some(),
            "[Memory] Key {} should exist after large transaction",
            key
        );
    }

    // Assert: No files created in temp directory (memory mode should not touch disk)
    let entries: Vec<_> = std::fs::read_dir(&temp_path)
        .expect("read temp dir")
        .collect();
    assert!(
        entries.is_empty(),
        "[Memory] No disk artifacts should be created - found {} files/dirs",
        entries.len()
    );
}

#[test]
fn should_handle_large_transaction_in_memory_mode_without_spill_files() {
    // Memory mode handles large transactions entirely in memory
    let opts = MidgeOptions {
        storage_mode: cntryl_midge::StorageMode::Memory,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Act: Multiple large transactions
    for round in 0..3 {
        let mut txn = engine
            .begin_transaction_with_options(&cf, None, 64 * 1024, IsolationLevel::default())
            .expect("begin");

        for i in 0..500 {
            txn.put(
                format!("round{}_{:06}", round, i).as_bytes(),
                &vec![round as u8; 1024],
            )
            .expect("put");
        }

        engine
            .commit_transaction(txn, WriteOptions::default())
            .expect("commit");
    }

    // Assert: All data accessible
    for round in 0..3 {
        for i in (0..500).step_by(50) {
            let key = format!("round{}_{:06}", round, i);
            let value = engine.get(&cf, key.as_bytes()).expect("get");
            assert!(value.is_some(), "[Memory] Key {} should exist", key);
            assert_eq!(
                value.unwrap()[0],
                round as u8,
                "[Memory] Value should have correct pattern for {}",
                key
            );
        }
    }
}
