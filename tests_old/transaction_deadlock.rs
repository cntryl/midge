//! Transaction Concurrency Tests (INSERT Conflict Detection)
//!
//! Tests for INSERT conflict detection under concurrent transactions.
//!
//! # Conflict Model
//!
//! Midge uses selective conflict detection:
//! - PUT/DELETE: Last-write-wins (LWW), no conflict detection
//! - INSERT: Conflict if key already exists at commit time
//! - CAS: Conflict if value changed since snapshot
//!
//! These tests verify INSERT conflict behavior since INSERT is the operation
//! that has conflict detection semantics similar to traditional locking.
//!
//! # Test Categories
//!
//! - **INSERT Conflicts**: Testing concurrent inserts to same key
//! - **PUT LWW**: Verifying concurrent PUTs all succeed
//! - **Mixed Operations**: Combining INSERT and PUT behaviors
//!
//! # Storage Mode Coverage
//!
//! All tests run against both LocalDisk and CloudBacked modes.

use bytes::Bytes;
use cntryl_midge::{KvTransaction, MidgeEngine, MidgeOptions, WriteOptions};
use std::sync::Arc;

mod common;
use cntryl_midge::testkit::{create_storage_mode, disk_storage_modes, DurabilityTestContext};

// ============================================================================
// INSERT Conflict Detection (one wins, one fails)
// ============================================================================

#[test]
fn should_allow_only_one_insert_given_concurrent_inserts_to_same_key() {
    // INSERT has conflict detection - only one concurrent insert to same key succeeds
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

        // Both try to INSERT same key (key doesn't exist yet)
        txn1.insert(b"k1", b"txn1_k1").unwrap();
        txn2.insert(b"k1", b"txn2_k1").unwrap();

        // Act
        let result1 = engine.commit_transaction(txn1, WriteOptions::default());
        let result2 = engine.commit_transaction(txn2, WriteOptions::default());

        // Assert - exactly one transaction should succeed
        assert!(
            (result1.is_ok() && result2.is_err()) || (result1.is_err() && result2.is_ok()),
            "[{}] Exactly one INSERT should succeed. result1={:?}, result2={:?}",
            name,
            result1,
            result2
        );
    }
}

#[test]
fn should_allow_only_subset_given_three_concurrent_inserts_to_same_key() {
    // With INSERT, only one of multiple concurrent inserts to the same key succeeds
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

        // All three try to INSERT same key
        txn1.insert(b"shared_key", b"t1").unwrap();
        txn2.insert(b"shared_key", b"t2").unwrap();
        txn3.insert(b"shared_key", b"t3").unwrap();

        // Act
        let result1 = engine.commit_transaction(txn1, WriteOptions::default());
        let result2 = engine.commit_transaction(txn2, WriteOptions::default());
        let result3 = engine.commit_transaction(txn3, WriteOptions::default());

        // Assert - exactly one should succeed (first to commit)
        let success_count = [&result1, &result2, &result3]
            .iter()
            .filter(|r| r.is_ok())
            .count();

        assert_eq!(
            success_count, 1,
            "[{}] Exactly one INSERT should succeed. results=[{:?}, {:?}, {:?}]",
            name, result1, result2, result3
        );
    }
}

// ============================================================================
// INSERT Conflict Scenarios
// ============================================================================

#[test]
fn should_fail_second_insert_given_concurrent_inserts_to_overlapping_keys() {
    // INSERT conflict detection applies per-key
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

        // txn1 inserts key_a, txn2 inserts key_a and key_b
        txn1.insert(b"key_a", b"txn1").unwrap();
        txn2.insert(b"key_a", b"txn2").unwrap();
        txn2.insert(b"key_b", b"txn2_b").unwrap();

        // Act
        let result1 = engine.commit_transaction(txn1, WriteOptions::default());
        let result2 = engine.commit_transaction(txn2, WriteOptions::default());

        // Assert - exactly one should succeed for key_a
        assert!(
            (result1.is_ok() && result2.is_err()) || (result1.is_err() && result2.is_ok()),
            "[{}] One INSERT should fail due to key_a conflict. result1={:?}, result2={:?}",
            name,
            result1,
            result2
        );
    }
}

#[test]
fn should_succeed_insert_retry_given_key_never_existed() {
    // INSERT should succeed on a fresh key that was never created
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        // First INSERT succeeds on a fresh key
        let mut txn = engine.begin_transaction(&cf).expect("begin txn");
        txn.insert(b"retry_key", b"value").unwrap();
        let result = engine.commit_transaction(txn, WriteOptions::default());
        assert!(result.is_ok(), "[{}] First INSERT should succeed", name);

        // Act - Second INSERT to same key should fail because key exists
        let mut txn2 = engine.begin_transaction(&cf).expect("begin txn2");
        txn2.insert(b"retry_key", b"value2").unwrap();
        let result2 = engine.commit_transaction(txn2, WriteOptions::default());
        // Assert
        assert!(
            result2.is_err(),
            "[{}] Second INSERT should fail - key exists",
            name
        );

        // INSERT to a different (fresh) key should succeed
        let mut fresh_txn = engine.begin_transaction(&cf).expect("begin fresh");
        fresh_txn.insert(b"fresh_key", b"fresh_value").unwrap();
        let fresh_result = engine.commit_transaction(fresh_txn, WriteOptions::default());
        assert!(
            fresh_result.is_ok(),
            "[{}] INSERT to fresh key should succeed",
            name
        );
    }
}

// ============================================================================
// PUT LWW - All Succeed
// ============================================================================

#[test]
fn should_allow_all_concurrent_puts_given_lww_semantics() {
    // PUT uses LWW - all concurrent PUTs should succeed, last writer wins
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

        // Act: Spawn multiple threads writing to overlapping keys (LWW allows all)
        let handles: Vec<_> = (0..10)
            .map(|thread_id| {
                let eng = engine.clone();
                let cf_clone = cf.clone();
                std::thread::spawn(move || {
                    let mut success_count = 0;
                    for iteration in 0..5 {
                        let mut txn = eng.begin_transaction(&cf_clone).unwrap();

                        // Each thread writes to multiple keys
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

                        // With LWW, ALL commits should succeed
                        if eng.commit_transaction(txn, WriteOptions::default()).is_ok() {
                            success_count += 1;
                        }
                    }
                    success_count
                })
            })
            .collect();

        let total_successes: usize = handles
            .into_iter()
            .map(|h| h.join().expect("Thread panicked"))
            .sum();

        // Assert: With LWW, ALL 50 commits (10 threads Ã— 5 iterations) should succeed
        assert_eq!(
            total_successes, 50,
            "[{}] All PUT commits should succeed with LWW",
            name
        );

        // All keys should still be readable
        for i in 0..10 {
            let key = format!("resource_{}", i);
            let result = engine.get(&cf, key.as_bytes());
            assert!(
                result.is_ok() && result.unwrap().is_some(),
                "[{}] Key {} should exist after concurrent PUTs",
                name,
                key
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
        assert_eq!(value.as_deref(), Some(b"value3"),
            "[{}] Last write should win",
            name
        );
    }
}

#[test]
fn should_allow_read_txn_to_commit_given_read_key_modified_when_lww() {
    // With LWW, reading a key does NOT create a conflict when it's modified by another transaction.
    // PUT uses last-write-wins - there's no read tracking for PUT operations.
    // Only INSERT and CAS have conflict detection.
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

        // Start transaction and read the key
        let mut read_txn = engine.begin_transaction(&cf).expect("begin read txn");
        let _value = read_txn.get(b"key");

        // Another transaction updates and commits the same key
        let mut write_txn = engine.begin_transaction(&cf).expect("begin write txn");
        write_txn.put(b"key", b"updated").unwrap();
        let write_result = engine.commit_transaction(write_txn, WriteOptions::default());
        assert!(write_result.is_ok(), "[{}] Write should succeed", name);

        // Act: Try to commit the read transaction (no writes)
        let read_result = engine.commit_transaction(read_txn, WriteOptions::default());

        // Assert: Should succeed - LWW doesn't track reads for PUT operations
        assert!(
            read_result.is_ok(),
            "[{}] Read-only transaction should succeed with LWW - no read tracking for PUT: {:?}",
            name,
            read_result
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
            success_count,
            20,
            "[{}] All transactions on disjoint keys should succeed. \
             Successes: {}, Failures: {:?}",
            name,
            success_count,
            results.iter().filter(|r| r.is_err()).collect::<Vec<_>>()
        );
    }
}

#[test]
fn should_persist_last_committed_value_given_concurrent_puts_when_lww() {
    // With LWW, both concurrent PUTs succeed. The last committed value persists.
    for mode in disk_storage_modes() {
        // Arrange
        let ctx = DurabilityTestContext::new(mode);

        {
            let opts = MidgeOptions {
                storage_mode: ctx.create_storage_mode(),
                ..Default::default()
            };
            let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
            let cf = engine.default_column_family();

            engine.put(&cf, b"contested_key", b"initial").unwrap();

            // Create concurrent transactions with PUT
            let mut txn1 = engine.begin_transaction(&cf).unwrap();
            let mut txn2 = engine.begin_transaction(&cf).unwrap();

            txn1.put(b"contested_key", b"txn1_value").unwrap();
            txn2.put(b"contested_key", b"txn2_value").unwrap();

            // Act: Commit both - with LWW, BOTH should succeed
            let result1 = engine.commit_transaction(txn1, WriteOptions::default());
            let result2 = engine.commit_transaction(txn2, WriteOptions::default());

            assert!(
                result1.is_ok(),
                "[{}] First PUT should succeed with LWW",
                ctx.name()
            );
            assert!(
                result2.is_ok(),
                "[{}] Second PUT should also succeed with LWW",
                ctx.name()
            );
        }

        // Restart and verify - last committed value (txn2) should persist
        let opts = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("reopen");
        let cf = engine.default_column_family();

        // Assert: Last committed value (txn2) should persist
        let value = engine.get(&cf, b"contested_key").unwrap();
        assert_eq!(value.as_deref(), Some(b"txn2_value"),
            "[{}] Last committed PUT value should persist after restart",
            ctx.name()
        );
    }
}
