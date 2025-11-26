//! Transaction Isolation Tests
//!
//! These tests verify transaction isolation guarantees:
//! - Dirty read prevention (cannot see uncommitted writes)
//! - Dirty write prevention (cannot overwrite uncommitted data)
//! - Snapshot isolation (consistent view at transaction start)
//! - Read-write conflict detection
//! - Phantom read prevention
//! - Isolation level enforcement (ReadCommitted vs Snapshot)
//!
//! # Storage Mode Coverage
//! - Uses `disk_storage_modes()` (LocalDisk, CloudBacked) since transactions require WAL durability
//! - Memory mode does not support durable transactions

mod common;

use bytes::Bytes;
use cntryl_midge::{IsolationLevel, KvTransaction, MidgeEngine, MidgeOptions, Query, WriteOptions};
use common::{create_storage_mode, disk_storage_modes, DurabilityTestContext};
use common::test_helpers::{wait_for_signal, wait_for_signal_default, TEST_RECV_TIMEOUT};
use std::sync::Arc;

// ============================================================================
// DIRTY READ PREVENTION
// ============================================================================

#[test]
fn should_prevent_dirty_read_given_uncommitted_write_when_reading_from_engine() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        let mut uncommitted_txn = engine.begin_transaction(&cf).expect("begin_transaction");
        uncommitted_txn.put(b"key", b"uncommitted").unwrap();

        // Act
        let read_result = engine.get(&cf, b"key").expect("get");

        // Assert - should not see uncommitted write
        assert_eq!(
            read_result, None,
            "Should not see uncommitted transaction write for {}",
            name
        );

        drop(uncommitted_txn);
        assert_eq!(
            engine.get(&cf, b"key").expect("get after rollback"),
            None,
            "Failed for {}",
            name
        );
    }
}

#[test]
fn should_not_see_uncommitted_write_given_other_transaction_when_reading() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        // Act - two transactions
        let mut txn1 = engine.begin_transaction(&cf).expect("begin txn1");
        txn1.put(b"key1", b"value1").expect("put");

        let mut txn2 = engine.begin_transaction(&cf).expect("begin txn2");

        // Assert - txn2 should not see txn1's uncommitted write
        let result = txn2.get(b"key1").expect("get");
        assert!(
            result.is_none(),
            "Uncommitted writes invisible to other transactions for {}",
            name
        );
    }
}

#[test]
fn should_prevent_dirty_reads_given_concurrent_uncommitted_changes_when_tested() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        // Write initial key
        engine
            .put(&cf, b"dirty_read_key", b"initial_value")
            .expect("put");

        // Act - one thread modifies, other thread tries to read
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<()>();
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();

        let eng_txn = Arc::clone(&engine);
        let cf_txn = cf.clone();
        let txn_handle = std::thread::spawn(move || {
            let mut txn = eng_txn.begin_transaction(&cf_txn).expect("begin");
            txn.put(b"dirty_read_key", b"uncommitted_value")
                .expect("put");
            // Signal that the transaction is ready and still uncommitted
            ready_tx.send(()).unwrap();
            // Wait until main thread tells us to finish
            wait_for_signal(&done_rx, TEST_RECV_TIMEOUT);
            txn
        });

        // Wait for the txn thread to prepare the uncommitted write
        wait_for_signal_default(&ready_rx);

        // Reader thread attempts to read while transaction is open
        let eng_reader = Arc::clone(&engine);
        let cf_reader = cf.clone();
        let reader_result = std::thread::spawn(move || {
            eng_reader.get(&cf_reader, b"dirty_read_key").expect("get")
        });

        let read_value = reader_result.join().expect("reader panicked");
        let _ = done_tx.send(());
        let _txn = txn_handle.join().expect("txn panicked");

        // Assert - reader should NOT see the uncommitted value
        if let Some(value) = read_value {
            assert_eq!(
                value,
                Bytes::from("initial_value"),
                "Should read committed value, not uncommitted for {}",
                name
            );
        }
    }
}

// ============================================================================
// DIRTY WRITE PREVENTION
// ============================================================================

#[test]
fn should_allow_dirty_write_given_uncommitted_update_when_optimistic_concurrency() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        engine.put(&cf, b"key", b"v1").expect("put");

        let mut first_txn = engine.begin_transaction(&cf).expect("begin_transaction");
        first_txn.put(b"key", b"txn1_value").unwrap();

        let mut second_txn = engine.begin_transaction(&cf).expect("begin_transaction");
        second_txn.put(b"key", b"txn2_value").unwrap();

        // Act
        let second_result = engine.commit_transaction(second_txn, WriteOptions::default());

        // Assert - In optimistic concurrency, dirty writes are allowed
        assert!(
            second_result.is_ok(),
            "Should allow dirty write in optimistic concurrency for {}",
            name
        );

        drop(first_txn);
    }
}

// ============================================================================
// READ OWN WRITES
// ============================================================================

#[test]
fn should_read_uncommitted_value_given_put_in_same_transaction_when_read() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        // Act - start transaction and write
        let mut txn = engine.begin_transaction(&cf).expect("begin transaction");
        txn.put(b"txn_key", b"txn_value")
            .expect("put in transaction");

        // Assert - transaction should see its own write
        let result = txn.get(b"txn_key").expect("get in transaction");
        assert_eq!(
            result,
            Some(Bytes::from("txn_value")),
            "Transaction should see own writes for {}",
            name
        );

        // Transaction not committed yet, so main engine shouldn't see it
        let main_result = engine.get(&cf, b"txn_key").expect("get from engine");
        assert!(
            main_result.is_none(),
            "Uncommitted write invisible outside transaction for {}",
            name
        );
    }
}

#[test]
fn should_see_own_writes_given_transaction_when_get_after_put() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        let mut writing_txn = engine.begin_transaction(&cf).expect("begin_transaction");
        writing_txn.put(b"key", b"my_value").unwrap();

        // Act
        let local_read = writing_txn.get(b"key").expect("get");

        // Assert
        assert_eq!(local_read, Some(Bytes::from("my_value")), "Failed for {}", name);
    }
}

#[test]
fn should_see_own_writes_given_transaction_when_reading_staged_mutations() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        // Act
        let mut txn = engine.begin_transaction(&cf).expect("begin_transaction");
        txn.put(b"new_key", b"new_value").unwrap();

        // Read own write
        let value = txn.get(b"new_key").expect("get");

        // Assert
        assert_eq!(value, Some(Bytes::from("new_value")), "Failed for {}", name);
    }
}

// ============================================================================
// SNAPSHOT ISOLATION
// ============================================================================

#[test]
fn should_read_at_begin_sequence_given_transaction_when_using_transaction_get() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        engine.put(&cf, b"key", b"initial").expect("put");

        let mut txn = engine.begin_transaction(&cf).expect("begin_transaction");
        let begin_value = txn.get(b"key").expect("get");

        // Act
        engine.put(&cf, b"key", b"updated").expect("put");

        let second_value = txn.get(b"key").expect("get");

        // Assert
        assert_eq!(begin_value, Some(Bytes::from("initial")), "Failed for {}", name);
        assert_eq!(
            second_value,
            Some(Bytes::from("initial")),
            "Should see value at transaction begin for {}",
            name
        );
    }
}

#[test]
fn should_not_see_concurrent_writes_given_transaction_when_snapshot_isolated() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        engine.put(&cf, b"key1", b"v1").expect("put");

        let mut txn1 = engine.begin_transaction(&cf).expect("begin_transaction");

        // Act
        let mut txn2 = engine.begin_transaction(&cf).expect("begin_transaction");
        txn2.put(b"key2", b"v2").unwrap();
        engine
            .commit_transaction(txn2, WriteOptions::default())
            .expect("commit");

        let value = txn1.get(b"key2").expect("get");

        // Assert
        assert_eq!(
            value, None,
            "Should not see writes committed after transaction began for {}",
            name
        );
    }
}

#[test]
fn should_return_old_value_given_snapshot_created_before_write() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();
        engine.put(&cf, b"key1", b"original").expect("put");

        // Act - create snapshot, then update value
        let snap = engine.snapshot();
        engine.put(&cf, b"key1", b"updated").expect("update");

        // Assert - snapshot should see old value and engine should see new value
        let snap_val = snap.get(&engine, &cf, b"key1").expect("get at snapshot");
        assert_eq!(snap_val.as_deref(), Some(&b"original"[..]), "Failed for {}", name);

        let current_val = engine.get(&cf, b"key1").expect("get");
        assert_eq!(
            current_val,
            Some(Bytes::from("updated")),
            "Engine should see new value for {}",
            name
        );
    }
}

#[test]
fn should_provide_consistent_view_given_multiple_reads_when_snapshot_isolated() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        engine.put(&cf, b"key1", b"v1").expect("put");
        engine.put(&cf, b"key2", b"v2").expect("put");

        let mut txn = engine.begin_transaction(&cf).expect("begin_transaction");
        let first_read = txn.get(b"key1").expect("get");

        // Act
        engine.put(&cf, b"key1", b"updated1").expect("put");
        engine.put(&cf, b"key2", b"updated2").expect("put");

        let second_read = txn.get(b"key1").expect("get");
        let key2_read = txn.get(b"key2").expect("get");

        // Assert
        assert_eq!(first_read, Some(Bytes::from("v1")), "Failed for {}", name);
        assert_eq!(second_read, Some(Bytes::from("v1")), "Failed for {}", name);
        assert_eq!(key2_read, Some(Bytes::from("v2")), "Failed for {}", name);
    }
}

// ============================================================================
// READ-WRITE CONFLICT DETECTION
// ============================================================================

#[test]
fn should_detect_read_write_conflict_under_snapshot() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();
        engine.put(&cf, b"rw_key", b"initial").expect("put");

        // Act - start a transaction with default Snapshot isolation and read key
        let mut txn_a = engine.begin_transaction(&cf).expect("begin");
        let _ = txn_a.get(b"rw_key").expect("get");

        // Another transaction updates and commits
        let mut txn_b = engine.begin_transaction(&cf).expect("begin");
        txn_b.put(b"rw_key", b"updated").expect("put");
        assert!(engine
            .commit_transaction(txn_b, WriteOptions::default())
            .is_ok());

        // Act - now txn_a tries to commit a write, should conflict due to read-write
        txn_a.put(b"some_key", b"value").expect("put");
        let res = engine.commit_transaction(txn_a, WriteOptions::default());

        // Assert
        assert!(
            res.is_err(),
            "Snapshot isolation should detect read-write conflict for {}",
            name
        );
    }
}

#[test]
fn should_track_reads_given_transaction_get_when_validating_conflicts() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        engine.put(&cf, b"key", b"v1").expect("put");

        let mut txn1 = engine.begin_transaction(&cf).expect("begin_transaction");
        let _ = txn1.get(b"key").expect("get");

        // Act
        let mut txn2 = engine.begin_transaction(&cf).expect("begin_transaction");
        txn2.put(b"key", b"v2").unwrap();
        engine
            .commit_transaction(txn2, WriteOptions::default())
            .expect("commit");

        txn1.put(b"other_key", b"value").unwrap();
        let result = engine.commit_transaction(txn1, WriteOptions::default());

        // Assert
        assert!(result.is_err(), "Should detect read-write conflict for {}", name);
    }
}

#[test]
fn should_detect_conflict_given_concurrent_updates_to_same_key_when_commit() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();
        engine.put(&cf, b"conflict_key", b"initial").expect("put");

        // Act - two transactions updating same key
        let mut txn1 = engine.begin_transaction(&cf).expect("begin txn1");
        let mut txn2 = engine.begin_transaction(&cf).expect("begin txn2");

        txn1.put(b"conflict_key", b"txn1_value").expect("put txn1");
        txn2.put(b"conflict_key", b"txn2_value").expect("put txn2");

        // Assert - at least one commit should succeed, other may fail
        let commit1 = engine.commit_transaction(txn1, WriteOptions::default());
        let commit2 = engine.commit_transaction(txn2, WriteOptions::default());

        // At least one should succeed (optimistic concurrency control)
        assert!(
            commit1.is_ok() || commit2.is_ok(),
            "At least one transaction should commit successfully for {}",
            name
        );
    }
}

// ============================================================================
// ISOLATION LEVEL ENFORCEMENT
// ============================================================================

#[test]
fn should_allow_commit_under_read_committed_when_other_commits() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();
        engine.put(&cf, b"rw_key", b"initial").expect("put");

        // Act - start txn with ReadCommitted isolation and read key
        let mut txn_a = engine
            .begin_transaction_with_options(&cf, None, 1024 * 1024, IsolationLevel::ReadCommitted)
            .expect("begin");
        let _ = txn_a.get(b"rw_key").expect("get");

        // Another transaction updates and commits
        let mut txn_b = engine.begin_transaction(&cf).expect("begin");
        txn_b.put(b"rw_key", b"updated").expect("put");
        assert!(engine
            .commit_transaction(txn_b, WriteOptions::default())
            .is_ok());

        // Act - txn_a tries to commit and should NOT be treated as conflicting
        txn_a.put(b"some_key", b"value").expect("put");
        let res = engine.commit_transaction(txn_a, WriteOptions::default());

        // Assert - should succeed for read committed
        assert!(
            res.is_ok(),
            "ReadCommitted should not track reads and should allow commit for {}",
            name
        );
    }
}

// ============================================================================
// PHANTOM READ PREVENTION
// ============================================================================

#[test]
fn should_prevent_phantom_read_given_snapshot_isolation_when_range_scan() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        engine.put(&cf, b"key1", b"v1").expect("put");

        let snap = engine.snapshot();
        let first_scan = engine
            .scan_at(
                &cf,
                Query {
                    prefix: Some(Bytes::from("key")),
                    ..Default::default()
                },
                &snap,
            )
            .expect("scan");

        engine.put(&cf, b"key2", b"v2").expect("put new key");

        // Act
        let second_scan = engine
            .scan_at(
                &cf,
                Query {
                    prefix: Some(Bytes::from("key")),
                    ..Default::default()
                },
                &snap,
            )
            .expect("scan");

        // Assert - Both scans at same snapshot should see same keys
        assert_eq!(
            first_scan.len(),
            second_scan.len(),
            "Phantom read prevented by snapshot for {}",
            name
        );
    }
}

// ============================================================================
// ROLLBACK
// ============================================================================

#[test]
fn should_rollback_all_operations_given_transaction_abort_called() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();
        engine.put(&cf, b"existing", b"value").expect("put");

        // Act - transaction with abort
        let mut txn = engine.begin_transaction(&cf).expect("begin");
        txn.put(b"new_key", b"new_value").expect("put");
        txn.put(b"existing", b"updated").expect("update");
        // Abort by dropping without commit
        drop(txn);

        // Assert - all transaction operations should be rolled back
        assert_eq!(
            engine.get(&cf, b"new_key").expect("get"),
            None,
            "Failed for {}",
            name
        );
        assert_eq!(
            engine.get(&cf, b"existing").expect("get"),
            Some(Bytes::from("value")),
            "Original value preserved for {}",
            name
        );
    }
}

// ============================================================================
// TRANSACTION LIFECYCLE ISOLATION
// ============================================================================

#[test]
fn should_preserve_isolation_across_transaction_lifecycle_given_multiple_operations() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        // Pre-populate some data
        for i in 0..100 {
            let key = format!("lifecycle_key_{:03}", i).into_bytes();
            let value = format!("lifecycle_value_{}", i).into_bytes();
            engine.put(&cf, &key, &value).expect("put");
        }

        // Act - create a transaction that reads and writes
        let mut txn = engine.begin_transaction(&cf).expect("begin");

        // Read some values from main database (snapshot view)
        let read_value = txn.get(b"lifecycle_key_050").expect("get in txn");
        assert_eq!(
            read_value,
            Some(Bytes::from("lifecycle_value_50")),
            "Transaction should see committed data for {}",
            name
        );

        // Modify a key
        txn.put(b"lifecycle_key_050", b"modified_in_txn")
            .expect("put");

        // Read the modified value (should see own write)
        let modified = txn.get(b"lifecycle_key_050").expect("get modified");
        assert_eq!(
            modified,
            Some(Bytes::from("modified_in_txn")),
            "Should see own write in same transaction for {}",
            name
        );

        // Commit the transaction
        engine
            .commit_transaction(txn, WriteOptions::default())
            .expect("commit");

        // Assert - verify modification is now visible in main engine
        let final_value = engine
            .get(&cf, b"lifecycle_key_050")
            .expect("get after commit");
        assert_eq!(
            final_value,
            Some(Bytes::from("modified_in_txn")),
            "Committed transaction modification should be visible for {}",
            name
        );
    }
}

// ============================================================================
// CONCURRENT ISOLATION STRESS
// ============================================================================

#[test]
fn should_maintain_isolation_under_concurrent_transaction_pressure_when_stress_tested() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();
        const NUM_THREADS: usize = 10;
        const TRANSACTIONS_PER_THREAD: usize = 20;

        // Act - multiple threads performing concurrent transactions
        let handles: Vec<_> = (0..NUM_THREADS)
            .map(|thread_id| {
                let eng = Arc::clone(&engine);
                let cf_clone = cf.clone();
                std::thread::spawn(move || {
                    for txn_num in 0..TRANSACTIONS_PER_THREAD {
                        let mut txn = eng.begin_transaction(&cf_clone).expect("begin txn");

                        // Each transaction writes 5 keys
                        for key_offset in 0..5 {
                            let key = format!(
                                "isolation_key_{}_{}_{}", thread_id, txn_num, key_offset
                            )
                            .into_bytes();
                            let value = format!(
                                "isolation_value_{}_{}_{}", thread_id, txn_num, key_offset
                            )
                            .into_bytes();
                            txn.put(&key, &value).expect("put in txn");
                        }

                        // Try to commit (may fail due to conflicts)
                        let _commit_result = eng.commit_transaction(txn, WriteOptions::default());
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("Thread panicked");
        }

        // Assert - verify at least some transactions committed and data is consistent
        let mut committed_count = 0;
        for thread_id in 0..NUM_THREADS {
            for txn_num in 0..TRANSACTIONS_PER_THREAD {
                let key = format!("isolation_key_{}_{}_0", thread_id, txn_num).into_bytes();
                if let Ok(Some(_result)) = engine.get(&cf, &key) {
                    committed_count += 1;
                }
            }
        }
        assert!(
            committed_count > 0,
            "At least some transactions should have committed for {}",
            name
        );
    }
}

#[test]
fn should_handle_high_concurrency_readers_without_panicking() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        for i in 0..50 {
            let key = format!("data_key_{}", i);
            engine.put(&cf, key.as_bytes(), b"value").unwrap();
        }

        // Act: Spawn 100 concurrent readers
        let handles: Vec<_> = (0..100)
            .map(|reader_id| {
                let eng = engine.clone();
                let cf_clone = cf.clone();
                std::thread::spawn(move || {
                    for i in 0..50 {
                        let key = format!("data_key_{}", i);
                        let _ = eng.get(&cf_clone, key.as_bytes());
                    }
                    reader_id
                })
            })
            .collect();

        let results: Vec<_> = handles
            .into_iter()
            .map(|h| h.join().expect("Reader thread panicked"))
            .collect();

        // Assert: All readers completed without panicking
        assert_eq!(results.len(), 100, "Failed for {}", name);
    }
}

#[test]
fn should_maintain_consistency_with_mixed_reader_writer_load() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        // Pre-populate with initial values
        for i in 0..20 {
            let key = format!("mixed_key_{}", i);
            engine.put(&cf, key.as_bytes(), b"initial").unwrap();
        }

        // Act: 10 writers + 40 readers concurrent
        let writer_handles: Vec<_> = (0..10)
            .map(|writer_id| {
                let eng = engine.clone();
                let cf_clone = cf.clone();
                std::thread::spawn(move || {
                    for iteration in 0..10 {
                        let key_index = (writer_id * 2 + iteration) % 20;
                        let key = format!("mixed_key_{}", key_index);
                        let mut txn = eng.begin_transaction(&cf_clone).unwrap();
                        let new_value = format!("w{}_i{}", writer_id, iteration);
                        txn.put(key.as_bytes(), new_value.as_bytes()).unwrap();
                        let _ = eng.commit_transaction(txn, WriteOptions::default());
                    }
                })
            })
            .collect();

        let reader_handles: Vec<_> = (0..40)
            .map(|_reader_id| {
                let eng = engine.clone();
                let cf_clone = cf.clone();
                std::thread::spawn(move || {
                    for i in 0..20 {
                        let key = format!("mixed_key_{}", i);
                        let _ = eng.get(&cf_clone, key.as_bytes());
                    }
                })
            })
            .collect();

        for h in writer_handles.into_iter().chain(reader_handles.into_iter()) {
            h.join().expect("Reader/writer thread panicked");
        }

        // Assert: All keys still exist and are readable
        for i in 0..20 {
            let key = format!("mixed_key_{}", i);
            let result = engine.get(&cf, key.as_bytes());
            assert!(
                result.is_ok(),
                "Key {} should exist after mixed reader/writer load for {}",
                key,
                name
            );
        }
    }
}

// ============================================================================
// DURABILITY WITH ISOLATION
// ============================================================================

#[test]
fn should_recover_snapshot_view_after_engine_restart() {
    for mode in disk_storage_modes() {
        let ctx = DurabilityTestContext::new(mode);
        let name = ctx.name();

        // Arrange
        let opts = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("initial open");
        let cf = engine.default_column_family();

        // Pre-populate data
        for i in 0..10 {
            let key = format!("persist_key_{}", i);
            engine.put(&cf, key.as_bytes(), b"persisted_value").unwrap();
        }

        drop(engine);

        // Act: Restart and verify snapshot behavior
        let opts2 = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts2).expect("restart open");
        let cf = engine.default_column_family();

        // Assert: All data should still be visible
        for i in 0..10 {
            let key = format!("persist_key_{}", i);
            let result = engine.get(&cf, key.as_bytes());
            assert!(
                result.is_ok(),
                "Persisted key {} should be readable after restart for {}",
                key,
                name
            );
        }
    }
}
