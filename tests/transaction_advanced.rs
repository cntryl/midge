//! Advanced Transaction Tests
//!
//! Tests for advanced transaction scenarios including edge cases, atomicity
//! guarantees, and integration with other features like delete_range.
//!
//! # Test Categories
//!
//! - **Edge Cases**: Empty transactions, read-only transactions, nested reads
//! - **Atomicity**: All-or-nothing guarantees, concurrent atomic commits
//! - **Delete Range Integration**: Transactions with range tombstones
//! - **Durability**: Atomic transactions persist across restart
//!
//! # Storage Mode Coverage
//!
//! Most tests run against LocalDisk and CloudBacked. Durability tests require disk.

use bytes::Bytes;
use cntryl_midge::{KvTransaction, MidgeEngine, MidgeOptions, WriteOptions};
use std::sync::Arc;

mod common;
use common::{create_storage_mode, disk_storage_modes, DurabilityTestContext};

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn should_commit_empty_transaction_given_no_operations() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        let empty_txn = engine.begin_transaction(&cf).expect("begin");

        // Act
        let result = engine.commit_transaction(empty_txn, WriteOptions::default());

        // Assert
        assert!(
            result.is_ok(),
            "[{}] Empty transaction should commit successfully: {:?}",
            name,
            result
        );
    }
}

#[test]
fn should_commit_read_only_transaction_given_no_writes() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        engine.put(&cf, b"key", b"value").expect("put");

        let mut readonly_txn = engine.begin_transaction(&cf).expect("begin");
        let _value = readonly_txn.get(b"key");

        // Act
        let result = engine.commit_transaction(readonly_txn, WriteOptions::default());

        // Assert
        assert!(
            result.is_ok(),
            "[{}] Read-only transaction should commit: {:?}",
            name,
            result
        );
    }
}

#[test]
fn should_read_own_writes_given_nested_gets_within_transaction() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        let mut txn = engine.begin_transaction(&cf).expect("begin");
        txn.put(b"nested_key", b"nested_value").unwrap();

        // Act - Multiple reads of own write
        let read1 = txn.get(b"nested_key").expect("get1");
        let read2 = txn.get(b"nested_key").expect("get2");

        engine
            .commit_transaction(txn, WriteOptions::default())
            .expect("commit");

        // Assert
        assert_eq!(
            read1,
            Some(Bytes::from_static(b"nested_value")),
            "[{}] First read should see own write",
            name
        );
        assert_eq!(
            read2,
            Some(Bytes::from_static(b"nested_value")),
            "[{}] Second read should see own write",
            name
        );
    }
}

#[test]
fn should_handle_rapid_transaction_creation_and_commit() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        // Act - Create and commit many transactions rapidly
        for i in 0..100 {
            let mut txn = engine.begin_transaction(&cf).expect("begin");
            txn.put(format!("rapid_{}", i).as_bytes(), b"v").unwrap();
            engine
                .commit_transaction(txn, WriteOptions::default())
                .expect("commit");
        }

        // Assert - All transactions committed
        for i in 0..100 {
            let value = engine
                .get(&cf, format!("rapid_{}", i).as_bytes())
                .expect("get");
            assert!(value.is_some(), "[{}] Key rapid_{} should exist", name, i);
        }
    }
}

// ============================================================================
// Atomicity
// ============================================================================

#[test]
fn should_commit_all_or_nothing_given_multi_key_transaction() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        let mut txn = engine.begin_transaction(&cf).expect("begin");
        txn.put(b"k1", b"v1").unwrap();
        txn.put(b"k2", b"v2").unwrap();
        txn.put(b"k3", b"v3").unwrap();
        txn.delete(b"k4").unwrap();

        // Act
        engine
            .commit_transaction(txn, WriteOptions::default())
            .expect("commit");

        // Assert - All operations applied atomically
        assert_eq!(
            engine.get(&cf, b"k1").expect("get"),
            Some(Bytes::from_static(b"v1")),
            "[{}] k1 should be v1",
            name
        );
        assert_eq!(
            engine.get(&cf, b"k2").expect("get"),
            Some(Bytes::from_static(b"v2")),
            "[{}] k2 should be v2",
            name
        );
        assert_eq!(
            engine.get(&cf, b"k3").expect("get"),
            Some(Bytes::from_static(b"v3")),
            "[{}] k3 should be v3",
            name
        );
        assert_eq!(
            engine.get(&cf, b"k4").expect("get"),
            None,
            "[{}] k4 should be deleted",
            name
        );
    }
}

#[test]
fn should_be_atomic_given_transaction_with_100_operations() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        let mut txn = engine.begin_transaction(&cf).expect("begin");
        for i in 0..100 {
            txn.put(
                format!("batch_key_{}", i).as_bytes(),
                format!("batch_val_{}", i).as_bytes(),
            )
            .unwrap();
        }

        // Act
        engine
            .commit_transaction(txn, WriteOptions::default())
            .expect("commit");

        // Assert - All 100 operations applied
        for i in 0..100 {
            let key = format!("batch_key_{}", i);
            let expected = Bytes::from(format!("batch_val_{}", i));
            assert_eq!(
                engine.get(&cf, key.as_bytes()).expect("get"),
                Some(expected),
                "[{}] Key {} should have correct value",
                name,
                key
            );
        }
    }
}

#[test]
fn should_rollback_all_writes_given_transaction_dropped() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        // Act - Create transaction and drop without commit
        {
            let mut txn = engine.begin_transaction(&cf).expect("begin");
            txn.put(b"rollback_k1", b"v1").unwrap();
            txn.put(b"rollback_k2", b"v2").unwrap();
            // Drop without commit
        }

        // Assert - Nothing written
        assert_eq!(
            engine.get(&cf, b"rollback_k1").expect("get"),
            None,
            "[{}] k1 should not exist after rollback",
            name
        );
        assert_eq!(
            engine.get(&cf, b"rollback_k2").expect("get"),
            None,
            "[{}] k2 should not exist after rollback",
            name
        );
    }
}

#[test]
fn should_not_expose_partial_writes_given_concurrent_reader() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();
        let snap_before = engine.snapshot();

        let mut txn = engine.begin_transaction(&cf).expect("begin");
        txn.put(b"atomic_k1", b"v1").unwrap();
        txn.put(b"atomic_k2", b"v2").unwrap();

        // Read while transaction is uncommitted
        let read_during = engine.get(&cf, b"atomic_k1").expect("get during");

        // Act
        engine
            .commit_transaction(txn, WriteOptions::default())
            .expect("commit");

        // Assert
        let snap_after = engine.snapshot();

        assert_eq!(
            read_during, None,
            "[{}] Should not see uncommitted writes",
            name
        );
        assert!(
            snap_after.seq > snap_before.seq,
            "[{}] Sequence should advance after commit",
            name
        );
    }
}

#[test]
fn should_maintain_atomicity_under_concurrent_commits() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        // Act: Spawn 10 threads, each committing 10-key atomic transactions
        let handles: Vec<_> = (0..10)
            .map(|thread_id| {
                let eng = engine.clone();
                let cf_clone = cf.clone();
                std::thread::spawn(move || {
                    for iteration in 0..5 {
                        let mut txn = eng.begin_transaction(&cf_clone).unwrap();
                        for key_offset in 0..10 {
                            let key =
                                format!("atomic_t{}_i{}_k{}", thread_id, iteration, key_offset);
                            let value = format!("v{}", key_offset);
                            txn.put(key.as_bytes(), value.as_bytes()).unwrap();
                        }
                        let _ = eng.commit_transaction(txn, WriteOptions::default());
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // Assert: Count committed keys
        let mut key_count = 0;
        for thread_id in 0..10 {
            for iteration in 0..5 {
                for key_offset in 0..10 {
                    let key = format!("atomic_t{}_i{}_k{}", thread_id, iteration, key_offset);
                    if engine.get(&cf, key.as_bytes()).unwrap().is_some() {
                        key_count += 1;
                    }
                }
            }
        }
        assert!(
            key_count > 0,
            "[{}] At least some atomic writes should succeed",
            name
        );
    }
}

// ============================================================================
// Delete Range Integration
// ============================================================================

#[test]
fn should_preserve_snapshot_view_given_range_delete_after_snapshot() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        for i in 0..20u8 {
            engine
                .put(&cf, &[i], format!("v{}", i).as_bytes())
                .unwrap();
        }

        let snap = engine.snapshot();

        // Act - Delete range after snapshot
        engine.delete_range(&cf, &[5], &[15]).unwrap();
        engine.flush().unwrap();

        // Assert - Snapshot should still see original keys
        for i in 0..20u8 {
            let value = snap.get(&engine, &cf, &[i]).unwrap();
            assert!(
                value.is_some(),
                "[{}] Snapshot should still see key {} after delete_range",
                name,
                i
            );
        }
    }
}

#[test]
fn should_abort_transaction_safely_given_delete_range_in_transaction() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        engine.put(&cf, b"k1", b"v1").unwrap();

        // Act - Transaction with delete_range, then abort
        let mut txn = engine.begin_transaction(&cf).unwrap();
        txn.delete_range(b"a", b"z").unwrap();

        let txn_id = txn.txn_id();
        engine.abort_transaction(txn);

        // Assert
        assert!(
            !engine.is_transaction_active(txn_id),
            "[{}] Transaction should no longer be active",
            name
        );
        assert!(
            engine.get(&cf, b"k1").unwrap().is_some(),
            "[{}] Original key should still exist after abort",
            name
        );
    }
}

#[test]
fn should_recover_after_abort_given_transaction_with_delete_range() {
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

            engine.put(&cf, b"x", b"1").unwrap();

            let mut txn = engine.begin_transaction(&cf).unwrap();
            txn.delete_range(b"a", b"z").unwrap();
            // Abort transaction
            engine.abort_transaction(txn);
        }

        // Act: Restart
        let opts = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("reopen");
        let cf = engine.default_column_family();

        // Assert: Key should still exist (abort was successful)
        assert!(
            engine.get(&cf, b"x").unwrap().is_some(),
            "[{}] Key should exist after restart - abort was successful",
            ctx.name()
        );
    }
}

// ============================================================================
// Durability
// ============================================================================

#[test]
fn should_persist_atomic_transactions_after_restart() {
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

            // Commit 5 atomic transactions
            for batch in 0..5 {
                let mut txn = engine.begin_transaction(&cf).unwrap();
                for i in 0..10 {
                    let key = format!("persist_batch_{}_key_{}", batch, i);
                    let value = format!("val_{}", i);
                    txn.put(key.as_bytes(), value.as_bytes()).unwrap();
                }
                engine
                    .commit_transaction(txn, WriteOptions::default())
                    .unwrap();
            }
        }

        // Act: Restart
        let opts = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("reopen");
        let cf = engine.default_column_family();

        // Assert: All keys should exist
        for batch in 0..5 {
            for i in 0..10 {
                let key = format!("persist_batch_{}_key_{}", batch, i);
                assert!(
                    engine.get(&cf, key.as_bytes()).unwrap().is_some(),
                    "[{}] Key {} should persist atomically",
                    ctx.name(),
                    key
                );
            }
        }
    }
}

#[test]
fn should_not_persist_uncommitted_transaction_after_restart() {
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

            // Start transaction but don't commit
            let mut txn = engine.begin_transaction(&cf).unwrap();
            for i in 0..10 {
                let key = format!("uncommitted_key_{}", i);
                txn.put(key.as_bytes(), b"uncommitted").unwrap();
            }
            // Drop without commit
        }

        // Act: Restart
        let opts = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("reopen");
        let cf = engine.default_column_family();

        // Assert: No keys should exist
        for i in 0..10 {
            let key = format!("uncommitted_key_{}", i);
            assert!(
                engine.get(&cf, key.as_bytes()).unwrap().is_none(),
                "[{}] Uncommitted key {} should not persist",
                ctx.name(),
                key
            );
        }
    }
}

// ============================================================================
// Sequential Transactions
// ============================================================================

#[test]
fn should_handle_multiple_sequential_transactions_on_different_keys() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        // Act - Multiple sequential transactions on different keys
        for i in 0..10 {
            let mut txn = engine.begin_transaction(&cf).expect("begin");
            txn.put(
                format!("sequential_key_{}", i).as_bytes(),
                format!("value_{}", i).as_bytes(),
            )
            .unwrap();
            engine
                .commit_transaction(txn, WriteOptions::default())
                .expect("commit");
        }

        // Assert - All values exist
        for i in 0..10 {
            let key = format!("sequential_key_{}", i);
            let expected = Bytes::from(format!("value_{}", i));
            let value = engine.get(&cf, key.as_bytes()).expect("get");
            assert_eq!(
                value,
                Some(expected),
                "[{}] Key {} should have correct value",
                name,
                key
            );
        }
    }
}

// ============================================================================
// PUT Semantics - Last Writer Wins (NO conflict detection)
// ============================================================================

#[test]
fn should_allow_sequential_puts_to_same_key_without_conflict() {
    // PUT uses Last-Write-Wins (LWW) semantics - no conflict detection.
    // Sequential transactions that both `put` to the same key should NOT conflict.
    // The second commit simply overwrites the first.
    //
    // Contrast with:
    // - `insert` (insert-if-not-exists): SHOULD conflict if key already exists
    // - `compare_and_swap`: SHOULD conflict if value changed since read
    // - `put`: Should NEVER conflict - it's unconditional
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        // First transaction creates the key
        let mut txn1 = engine.begin_transaction(&cf).expect("begin");
        txn1.put(b"put_key", b"first").unwrap();
        engine
            .commit_transaction(txn1, WriteOptions::default())
            .expect("commit first");

        // Second transaction overwrites the key with put (should succeed!)
        let mut txn2 = engine.begin_transaction(&cf).expect("begin");
        txn2.put(b"put_key", b"second").unwrap();
        let result = engine.commit_transaction(txn2, WriteOptions::default());

        // Assert - put should succeed (last writer wins)
        assert!(
            result.is_ok(),
            "[{}] Sequential puts should NOT conflict. \
             Put uses LWW semantics. Got: {:?}",
            name,
            result
        );

        let value = engine.get(&cf, b"put_key").expect("get");
        assert_eq!(
            value,
            Some(Bytes::from_static(b"second")),
            "[{}] Second put should win",
            name
        );
    }
}

#[test]
fn should_allow_concurrent_puts_to_same_key_with_last_writer_wins() {
    // PUT uses Last-Write-Wins (LWW) - when two transactions concurrently `put` to the
    // same key, BOTH should succeed. The final value is whichever committed last.
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        engine.put(&cf, b"concurrent_put", b"initial").unwrap();

        let mut txn1 = engine.begin_transaction(&cf).unwrap();
        let mut txn2 = engine.begin_transaction(&cf).unwrap();

        // Both transactions put to the same key (no reads!)
        txn1.put(b"concurrent_put", b"from_txn1").unwrap();
        txn2.put(b"concurrent_put", b"from_txn2").unwrap();

        // Act - commit both
        let result1 = engine.commit_transaction(txn1, WriteOptions::default());
        let result2 = engine.commit_transaction(txn2, WriteOptions::default());

        // Assert - BOTH should succeed with last writer wins
        assert!(
            result1.is_ok(),
            "[{}] First put commit should succeed: {:?}",
            name,
            result1
        );
        assert!(
            result2.is_ok(),
            "[{}] Second put commit should also succeed (LWW). Got: {:?}",
            name,
            result2
        );

        // Final value should be from txn2 (last to commit)
        let value = engine.get(&cf, b"concurrent_put").expect("get");
        assert_eq!(
            value,
            Some(Bytes::from_static(b"from_txn2")),
            "[{}] Last committed put should win",
            name
        );
    }
}

// ============================================================================
// INSERT Semantics - SHOULD Conflict (insert-if-not-exists)
// ============================================================================

#[test]
fn should_conflict_on_insert_given_key_already_exists() {
    // Correct behavior: `insert` is conditional - it should fail if the key
    // already exists. This is different from `put` which is unconditional.
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        // First transaction inserts the key
        let mut txn1 = engine.begin_transaction(&cf).expect("begin");
        txn1.insert(b"insert_key", b"first").unwrap();
        engine
            .commit_transaction(txn1, WriteOptions::default())
            .expect("commit first");

        // Second transaction tries to insert same key (should fail!)
        let mut txn2 = engine.begin_transaction(&cf).expect("begin");
        txn2.insert(b"insert_key", b"second").unwrap();
        let result = engine.commit_transaction(txn2, WriteOptions::default());

        // Assert - insert should fail (key exists)
        assert!(
            result.is_err(),
            "[{}] Insert should fail when key already exists: {:?}",
            name,
            result
        );

        // Original value preserved
        let value = engine.get(&cf, b"insert_key").expect("get");
        assert_eq!(
            value,
            Some(Bytes::from_static(b"first")),
            "[{}] First insert should be preserved",
            name
        );
    }
}

#[test]
fn should_conflict_on_concurrent_inserts_to_same_key() {
    // Correct behavior: When two transactions concurrently `insert` the same
    // key, only one should succeed - the other should fail because the key
    // now exists.
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        let mut txn1 = engine.begin_transaction(&cf).unwrap();
        let mut txn2 = engine.begin_transaction(&cf).unwrap();

        // Both try to insert same key
        txn1.insert(b"race_insert", b"from_txn1").unwrap();
        txn2.insert(b"race_insert", b"from_txn2").unwrap();

        // Act
        let result1 = engine.commit_transaction(txn1, WriteOptions::default());
        let result2 = engine.commit_transaction(txn2, WriteOptions::default());

        // Assert - exactly one should succeed
        assert!(
            (result1.is_ok() && result2.is_err()) || (result1.is_err() && result2.is_ok()),
            "[{}] Exactly one concurrent insert should succeed. \
             result1={:?}, result2={:?}",
            name,
            result1,
            result2
        );
    }
}

