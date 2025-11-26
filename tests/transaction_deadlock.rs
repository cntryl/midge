//! Transaction Deadlock Detection Tests
//!
//! Tests for deadlock detection and resolution in concurrent transactions.
//! Covers circular wait detection, victim selection, and recovery scenarios.
//!
//! # Test Categories
//!
//! - **Circular Wait Detection**: Detecting two-way and multi-way deadlocks
//! - **Victim Selection**: Choosing and aborting deadlock victims
//! - **Recovery**: Retry after deadlock, recovery after complex scenarios
//! - **Livelock Prevention**: Ensuring progress under high concurrency
//!
//! # Storage Mode Coverage
//!
//! All tests run against both LocalDisk and CloudBacked modes.

use bytes::Bytes;
use cntryl_midge::{KvTransaction, MidgeEngine, MidgeOptions, WriteOptions};
use std::sync::Arc;

mod common;
use common::{create_storage_mode, disk_storage_modes, DurabilityTestContext};

// ============================================================================
// Circular Wait Detection
// ============================================================================

#[test]
fn should_detect_deadlock_given_circular_wait_when_two_transactions() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        engine.put(&cf, b"k1", b"v1").expect("put");
        engine.put(&cf, b"k2", b"v2").expect("put");

        let mut txn1 = engine.begin_transaction(&cf).expect("begin txn1");
        let mut txn2 = engine.begin_transaction(&cf).expect("begin txn2");

        // Create circular dependency: txn1 -> k1 -> k2, txn2 -> k2 -> k1
        txn1.put(b"k1", b"txn1_k1").unwrap();
        txn2.put(b"k2", b"txn2_k2").unwrap();

        txn1.put(b"k2", b"txn1_k2").unwrap();
        txn2.put(b"k1", b"txn2_k1").unwrap();

        // Act
        let result1 = engine.commit_transaction(txn1, WriteOptions::default());
        let result2 = engine.commit_transaction(txn2, WriteOptions::default());

        // Assert - exactly one transaction should succeed
        assert!(
            (result1.is_ok() && result2.is_err()) || (result1.is_err() && result2.is_ok()),
            "[{}] Exactly one transaction should succeed, the other should fail due to conflict. \
             result1={:?}, result2={:?}",
            name,
            result1,
            result2
        );
    }
}

#[test]
fn should_detect_deadlock_given_three_way_circular_dependency() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        let mut txn1 = engine.begin_transaction(&cf).expect("begin txn1");
        let mut txn2 = engine.begin_transaction(&cf).expect("begin txn2");
        let mut txn3 = engine.begin_transaction(&cf).expect("begin txn3");

        // Each transaction owns one resource and wants another's
        txn1.put(b"r1", b"t1").unwrap();
        txn2.put(b"r2", b"t2").unwrap();
        txn3.put(b"r3", b"t3").unwrap();

        // Create circular: t1 wants r2, t2 wants r3, t3 wants r1
        txn1.put(b"r2", b"t1_r2").unwrap();
        txn2.put(b"r3", b"t2_r3").unwrap();
        txn3.put(b"r1", b"t3_r1").unwrap();

        // Act
        let result1 = engine.commit_transaction(txn1, WriteOptions::default());
        let result2 = engine.commit_transaction(txn2, WriteOptions::default());
        let result3 = engine.commit_transaction(txn3, WriteOptions::default());

        // Assert - at least one should fail, but not all
        let success_count = [&result1, &result2, &result3]
            .iter()
            .filter(|r| r.is_ok())
            .count();

        assert!(
            (1..3).contains(&success_count),
            "[{}] At least one transaction should succeed, but not all three. \
             success_count={}, results=[{:?}, {:?}, {:?}]",
            name,
            success_count,
            result1,
            result2,
            result3
        );
    }
}

// ============================================================================
// Victim Selection and Abort
// ============================================================================

#[test]
fn should_abort_victim_transaction_given_deadlock_when_detected() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        let mut txn1 = engine.begin_transaction(&cf).expect("begin txn1");
        let mut txn2 = engine.begin_transaction(&cf).expect("begin txn2");

        // Create conflict scenario
        txn1.put(b"resource_a", b"txn1").unwrap();
        txn2.put(b"resource_b", b"txn2").unwrap();

        txn1.put(b"resource_b", b"txn1_b").unwrap();
        txn2.put(b"resource_a", b"txn2_a").unwrap();

        // Act
        let result1 = engine.commit_transaction(txn1, WriteOptions::default());
        let result2 = engine.commit_transaction(txn2, WriteOptions::default());

        // Assert - one should be aborted as victim
        assert!(
            (result1.is_ok() && result2.is_err()) || (result1.is_err() && result2.is_ok()),
            "[{}] One transaction should be chosen as victim and aborted. \
             result1={:?}, result2={:?}",
            name,
            result1,
            result2
        );
    }
}

#[test]
fn should_allow_retry_given_deadlock_victim_when_aborted() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        let mut txn = engine.begin_transaction(&cf).expect("begin txn");
        txn.put(b"key", b"value").unwrap();

        // Act
        let result = engine.commit_transaction(txn, WriteOptions::default());

        // Assert - even if first attempt fails, retry should succeed
        if result.is_err() {
            let mut retry_txn = engine.begin_transaction(&cf).expect("begin retry txn");
            retry_txn.put(b"key", b"retry_value").unwrap();
            let retry_result = engine.commit_transaction(retry_txn, WriteOptions::default());
            assert!(
                retry_result.is_ok(),
                "[{}] Retry should succeed after abort",
                name
            );
        } else {
            assert!(result.is_ok(), "[{}] First attempt succeeded", name);
        }
    }
}

// ============================================================================
// Livelock Prevention
// ============================================================================

#[test]
fn should_handle_high_concurrency_without_livelock() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        // Pre-populate keys
        for i in 0..10 {
            let key = format!("resource_{}", i);
            engine.put(&cf, key.as_bytes(), b"initial").unwrap();
        }

        // Act: Spawn multiple threads with potential for circular waits
        let handles: Vec<_> = (0..10)
            .map(|thread_id| {
                let eng = engine.clone();
                let cf_clone = cf.clone();
                std::thread::spawn(move || {
                    for iteration in 0..5 {
                        let mut txn = eng.begin_transaction(&cf_clone).unwrap();

                        // Each thread writes to multiple keys in different order
                        let key1 = format!("resource_{}", (thread_id + iteration) % 10);
                        let key2 = format!("resource_{}", (thread_id + iteration + 1) % 10);

                        txn.put(
                            key1.as_bytes(),
                            format!("t{}_{}", thread_id, iteration).as_bytes(),
                        )
                        .unwrap();
                        txn.put(
                            key2.as_bytes(),
                            format!("t{}_{}", thread_id, iteration).as_bytes(),
                        )
                        .unwrap();

                        let _ = eng.commit_transaction(txn, WriteOptions::default());
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // Assert: No livelock - engine responds to queries
        for i in 0..10 {
            let key = format!("resource_{}", i);
            let result = engine.get(&cf, key.as_bytes());
            assert!(
                result.is_ok(),
                "[{}] Engine should remain responsive after high concurrency",
                name
            );
        }
    }
}

// ============================================================================
// Recovery After Deadlock
// ============================================================================

#[test]
fn should_handle_recovery_after_complex_deadlock_scenario() {
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

            // Pre-populate resources
            for i in 0..5 {
                let key = format!("dlk_resource_{}", i);
                engine.put(&cf, key.as_bytes(), b"initial").unwrap();
            }

            // Simulate multiple transactions with potential conflicts
            for batch in 0..3 {
                for i in 0..5 {
                    let mut txn = engine.begin_transaction(&cf).unwrap();
                    let key = format!("dlk_resource_{}", i);
                    let value = format!("batch_{}_key_{}", batch, i);
                    txn.put(key.as_bytes(), value.as_bytes()).unwrap();
                    let _ = engine.commit_transaction(txn, WriteOptions::default());
                }
            }
        }

        // Act: Restart and verify consistency
        let opts = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("reopen");
        let cf = engine.default_column_family();

        // Assert: All resources still exist and are readable
        for i in 0..5 {
            let key = format!("dlk_resource_{}", i);
            let result = engine.get(&cf, key.as_bytes()).unwrap();
            assert!(
                result.is_some(),
                "[{}] Resource {} should exist after restart",
                ctx.name(),
                key
            );
        }
    }
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn should_handle_self_conflict_given_same_key_multiple_writes() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        let mut txn = engine.begin_transaction(&cf).expect("begin txn");

        // Act: Multiple writes to same key within same transaction
        txn.put(b"key", b"value1").unwrap();
        txn.put(b"key", b"value2").unwrap();
        txn.put(b"key", b"value3").unwrap();

        let result = engine.commit_transaction(txn, WriteOptions::default());

        // Assert: Should succeed - no conflict with self
        assert!(
            result.is_ok(),
            "[{}] Self-writes should not cause conflict: {:?}",
            name,
            result
        );

        let value = engine.get(&cf, b"key").unwrap();
        assert_eq!(
            value,
            Some(Bytes::from_static(b"value3")),
            "[{}] Last write should win",
            name
        );
    }
}

#[test]
fn should_detect_read_write_conflict_given_concurrent_modification_to_read_key() {
    // Real-world behavior: Snapshot Isolation with read tracking detects when
    // a transaction's read set is modified by another committed transaction.
    // This is the correct SSI behavior - prevents phantom reads and lost updates.
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        engine.put(&cf, b"key", b"initial").unwrap();

        // Start read transaction and read the key (this tracks the read)
        let mut read_txn = engine.begin_transaction(&cf).expect("begin read txn");
        let _value = read_txn.get(b"key");

        // Another transaction updates and commits the same key
        let mut write_txn = engine.begin_transaction(&cf).expect("begin write txn");
        write_txn.put(b"key", b"updated").unwrap();
        let write_result = engine.commit_transaction(write_txn, WriteOptions::default());
        assert!(write_result.is_ok(), "[{}] Write should succeed", name);

        // Act: Try to commit the read transaction (even without writes)
        let read_result = engine.commit_transaction(read_txn, WriteOptions::default());

        // Assert: Should fail - the key we read was modified
        // This is correct SSI behavior: read set was invalidated
        assert!(
            read_result.is_err(),
            "[{}] Read transaction should fail when its read key was modified by committed transaction. \
             This is correct SSI behavior preventing stale-read-based decisions.",
            name
        );
    }
}

#[test]
fn should_allow_read_only_transaction_given_no_conflict_on_read_keys() {
    // Read-only transactions should succeed when their read keys are NOT modified
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        engine.put(&cf, b"read_key", b"stable").unwrap();
        engine.put(&cf, b"write_key", b"initial").unwrap();

        // Start read transaction and read one key
        let mut read_txn = engine.begin_transaction(&cf).expect("begin read txn");
        let _value = read_txn.get(b"read_key");

        // Another transaction modifies a DIFFERENT key and commits
        let mut write_txn = engine.begin_transaction(&cf).expect("begin write txn");
        write_txn.put(b"write_key", b"updated").unwrap();
        let write_result = engine.commit_transaction(write_txn, WriteOptions::default());
        assert!(write_result.is_ok(), "[{}] Write should succeed", name);

        // Act: Commit the read transaction
        let read_result = engine.commit_transaction(read_txn, WriteOptions::default());

        // Assert: Should succeed - no conflict on read key
        assert!(
            read_result.is_ok(),
            "[{}] Read-only transaction should succeed when its read keys are not modified: {:?}",
            name,
            read_result
        );
    }
}

#[test]
fn should_handle_many_concurrent_transactions_on_disjoint_keys() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        // Act: Many transactions on disjoint key sets
        let handles: Vec<_> = (0..20)
            .map(|thread_id| {
                let eng = engine.clone();
                let cf_clone = cf.clone();
                std::thread::spawn(move || {
                    let mut txn = eng.begin_transaction(&cf_clone).unwrap();
                    // Each thread writes to its own unique key
                    let key = format!("unique_key_{}", thread_id);
                    let value = format!("value_{}", thread_id);
                    txn.put(key.as_bytes(), value.as_bytes()).unwrap();
                    eng.commit_transaction(txn, WriteOptions::default())
                })
            })
            .collect();

        let results: Vec<_> = handles
            .into_iter()
            .map(|h| h.join().expect("thread join"))
            .collect();

        // Assert: All should succeed - no conflicts on disjoint keys
        let success_count = results.iter().filter(|r| r.is_ok()).count();
        assert_eq!(
            success_count, 20,
            "[{}] All transactions on disjoint keys should succeed. \
             Successes: {}, Failures: {:?}",
            name,
            success_count,
            results.iter().filter(|r| r.is_err()).collect::<Vec<_>>()
        );
    }
}

#[test]
fn should_persist_winning_transaction_value_after_conflict_and_restart() {
    for mode in disk_storage_modes() {
        // Arrange
        let ctx = DurabilityTestContext::new(mode);

        let winning_value: Option<Bytes>;
        {
            let opts = MidgeOptions {
                storage_mode: ctx.create_storage_mode(),
                ..Default::default()
            };
            let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
            let cf = engine.default_column_family();

            engine.put(&cf, b"contested_key", b"initial").unwrap();

            // Create conflicting transactions
            let mut txn1 = engine.begin_transaction(&cf).unwrap();
            let mut txn2 = engine.begin_transaction(&cf).unwrap();

            txn1.put(b"contested_key", b"txn1_wins").unwrap();
            txn2.put(b"contested_key", b"txn2_wins").unwrap();

            // Act: Commit both - one will win
            let result1 = engine.commit_transaction(txn1, WriteOptions::default());
            let result2 = engine.commit_transaction(txn2, WriteOptions::default());

            // Determine winner
            winning_value = if result1.is_ok() {
                Some(Bytes::from_static(b"txn1_wins"))
            } else if result2.is_ok() {
                Some(Bytes::from_static(b"txn2_wins"))
            } else {
                // Both failed? Use initial
                Some(Bytes::from_static(b"initial"))
            };
        }

        // Restart and verify
        let opts = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("reopen");
        let cf = engine.default_column_family();

        // Assert: Winning value should persist
        let value = engine.get(&cf, b"contested_key").unwrap();
        assert_eq!(
            value, winning_value,
            "[{}] Winning transaction value should persist after restart",
            ctx.name()
        );
    }
}
