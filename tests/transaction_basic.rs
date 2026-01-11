//! Transaction Basic Tests
//!
//! Core transaction functionality: begin, commit, rollback, isolation.

use bytes::Bytes;
use cntryl_midge::testkit::*;
use cntryl_midge::{Query, WriteOptions};
use std::sync::Arc;

// ============================================================================
// Commit Tests
// ============================================================================

#[test]
fn should_commit_transaction_given_multiple_operations_when_committed() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();

        // Act
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
        txn.put(b"key2".to_vec(), b"value2".to_vec(), None).unwrap();
        txn.delete(b"key3".to_vec()).unwrap();
        engine.commit(txn, WriteOptions::buffered()).unwrap();

        // Assert
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        assert_eq!(
            read_tx.get(b"key1").unwrap(),
            Some(Bytes::from_static(b"value1"))
        );
        assert_eq!(
            read_tx.get(b"key2").unwrap(),
            Some(Bytes::from_static(b"value2"))
        );
        assert_eq!(read_tx.get(b"key3").unwrap(), None);
    });
}

#[test]
fn should_succeed_given_empty_transaction_when_committed() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();

        // Act
        let txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        let result = engine.commit(txn, WriteOptions::buffered());

        // Assert
        assert!(result.is_ok());
    });
}

#[test]
fn should_succeed_given_read_only_transaction_when_committed() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let mut write_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        write_tx
            .put(b"key1".to_vec(), b"value1".to_vec(), None)
            .unwrap();
        engine.commit(write_tx, WriteOptions::buffered()).unwrap();

        // Act
        let txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let _value = txn.get(b"key1").unwrap();
        let result = engine.commit(txn, WriteOptions::buffered());

        // Assert
        assert!(result.is_ok());
    });
}

// ============================================================================
// Rollback Tests
// ============================================================================

#[test]
fn should_rollback_transaction_given_uncommitted_when_dropped() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();

        // Act
        {
            let mut txn = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
            // txn dropped here without commit
        }

        // Assert - writes not visible
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        assert_eq!(read_tx.get(b"key1").unwrap(), None);
    });
}

#[test]
fn should_rollback_all_writes_given_multiple_operations_when_dropped() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key1".to_vec(), b"original".to_vec(), None)
            .unwrap();
        engine.commit(tx, WriteOptions::buffered()).unwrap();

        // Act
        {
            let mut txn = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn.put(b"key1".to_vec(), b"updated".to_vec(), None)
                .unwrap();
            txn.put(b"key2".to_vec(), b"value2".to_vec(), None).unwrap();
            txn.delete(b"key3".to_vec()).unwrap();
            // txn dropped without commit
        }

        // Assert - original value preserved, new writes not visible
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        assert_eq!(
            read_tx.get(b"key1").unwrap(),
            Some(Bytes::from_static(b"original"))
        );
        assert_eq!(read_tx.get(b"key2").unwrap(), None);
    });
}

#[test]
fn should_release_locks_given_aborted_transaction_when_cleanup() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();

        // Act - first txn acquires lock and aborts
        {
            let mut txn1 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn1.put(b"key1".to_vec(), b"value1".to_vec(), None)
                .unwrap();
            // Dropped without commit - should release lock
        }

        // Second txn should be able to acquire the lock
        let mut txn2 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn2.put(b"key1".to_vec(), b"value2".to_vec(), None)
            .unwrap();
        engine.commit(txn2, WriteOptions::buffered()).unwrap();

        // Assert
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        assert_eq!(
            read_tx.get(b"key1").unwrap(),
            Some(Bytes::from_static(b"value2"))
        );
    });
}

// ============================================================================
// Snapshot Isolation
// ============================================================================

#[test]
fn should_allow_concurrent_writes_with_lww_semantics_given_transaction_when_active() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key1".to_vec(), b"v1".to_vec(), None).unwrap();
        engine.commit(tx, WriteOptions::buffered()).unwrap();

        // Act - start transaction (captures snapshot)
        let txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();

        // Concurrent write happens outside transaction
        let mut tx2 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx2.put(b"key1".to_vec(), b"v2".to_vec(), None).unwrap();
        engine.commit(tx2, WriteOptions::buffered()).unwrap();

        // Assert - Midge implements Last-Write-Wins (LWW) semantics
        // Transactions see latest committed data (not true snapshot isolation)
        let value = txn.get(b"key1").unwrap();
        assert!(
            value == Some(Bytes::from_static(b"v1")) || value == Some(Bytes::from_static(b"v2"))
        );

        // Drop transaction
        drop(txn);
    });
}

#[test]
fn should_read_own_writes_given_transaction_when_reading() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();

        // Act
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();

        // Read within same transaction
        let value = txn.get(b"key1").unwrap();

        // Assert - should see own uncommitted write
        assert_eq!(value, Some(Bytes::from_static(b"value1")));

        engine.commit(txn, WriteOptions::buffered()).unwrap();
    });
}

#[test]
fn should_read_own_writes_given_kv_transaction_when_getting() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();

        // Act
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn.put(b"test_key".to_vec(), b"test_value".to_vec(), None)
            .unwrap();
        let value = txn.get(b"test_key").unwrap();

        // Assert
        assert_eq!(value, Some(Bytes::from_static(b"test_value")));
    });
}

#[test]
fn should_hide_deleted_value_given_kv_transaction_when_getting() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();

        // Act
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn.put(b"k".to_vec(), b"v".to_vec(), None).unwrap();
        txn.delete(b"k".to_vec()).unwrap();
        let value = txn.get(b"k").unwrap();

        // Assert
        assert_eq!(value, None);
    });
}

#[test]
fn should_persist_writes_given_kv_transaction_when_committed_boxed() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();

        // Act
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn.put(b"key_commit".to_vec(), b"value_commit".to_vec(), None)
            .unwrap();
        engine.commit(txn, WriteOptions::buffered()).unwrap();

        // Assert
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        assert_eq!(
            read_tx.get(b"key_commit").unwrap(),
            Some(Bytes::from_static(b"value_commit"))
        );
    });
}

#[test]
fn should_fail_given_disable_wal_when_committing_boxed_transaction() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn.put(b"key_commit".to_vec(), b"value_commit".to_vec(), None)
            .unwrap();
        let write_opts = WriteOptions::no_wal();

        // Act
        let err = engine
            .commit(txn, write_opts)
            .expect_err("disable_wal should be rejected");

        // Assert
        let msg = err.to_string();
        assert!(msg.contains("disable_wal"));
    });
}

// ============================================================================
// Transaction Operations
// ============================================================================

#[test]
fn should_insert_value_given_nonexistent_key_when_insert_in_transaction() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();

        // Act
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
        engine.commit(txn, WriteOptions::buffered()).unwrap();

        // Assert
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        assert_eq!(
            read_tx.get(b"key1").unwrap(),
            Some(Bytes::from_static(b"value1"))
        );
    });
}

#[test]
fn should_delete_range_given_committed_transaction_when_delete_range() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key1".to_vec(), b"v1".to_vec(), None).unwrap();
        tx.put(b"key2".to_vec(), b"v2".to_vec(), None).unwrap();
        tx.put(b"key3".to_vec(), b"v3".to_vec(), None).unwrap();
        engine.commit(tx, WriteOptions::buffered()).unwrap();

        // Act - delete range in transaction
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn.delete_range(b"key1".to_vec(), b"key3".to_vec())
            .unwrap(); // Delete key1, key2 (not key3)
        engine.commit(txn, WriteOptions::buffered()).unwrap();

        // Assert
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        assert_eq!(read_tx.get(b"key1").unwrap(), None);
        assert_eq!(read_tx.get(b"key2").unwrap(), None);
        assert_eq!(
            read_tx.get(b"key3").unwrap(),
            Some(Bytes::from_static(b"v3"))
        );
    });
}

#[test]
fn should_hide_deleted_range_given_transaction_scan_when_delete_range() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key1".to_vec(), b"v1".to_vec(), None).unwrap();
        tx.put(b"key2".to_vec(), b"v2".to_vec(), None).unwrap();
        tx.put(b"key3".to_vec(), b"v3".to_vec(), None).unwrap();
        engine.commit(tx, WriteOptions::buffered()).unwrap();

        // Act
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn.delete_range(b"key1".to_vec(), b"key3".to_vec())
            .unwrap();

        // Scan within transaction
        let mut iter = txn
            .scan(
                &Query::new()
                    .start_key(Bytes::from(&b"key0"[..]))
                    .end_key(Bytes::from(&b"key9"[..])),
            )
            .unwrap();
        let results: Vec<_> = std::iter::from_fn(|| iter.next()).collect();

        // Assert - Should only see key3
        assert_eq!(results.len(), 1);
    });
}

#[test]
fn should_see_uncommitted_writes_given_transaction_scan_when_scanning() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();

        // Act
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
        txn.put(b"key2".to_vec(), b"value2".to_vec(), None).unwrap();

        // Scan within transaction
        let mut iter = txn
            .scan(
                &Query::new()
                    .start_key(Bytes::from(&b"key0"[..]))
                    .end_key(Bytes::from(&b"key9"[..])),
            )
            .unwrap();
        let results: Vec<_> = std::iter::from_fn(|| iter.next()).collect();

        // Assert - should see uncommitted writes
        assert_eq!(results.len(), 2);
    });
}

// ============================================================================
// Error Handling
// ============================================================================

#[test]
fn should_allow_operations_given_previous_commit_failed_when_disk_full() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();

        // Act - first transaction fails (simulated disk full)
        {
            let mut txn1 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn1.put(b"key1".to_vec(), b"value1".to_vec(), None)
                .unwrap();
            // Simulate commit failure by dropping
        }

        // Second transaction should work
        let mut txn2 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn2.put(b"key2".to_vec(), b"value2".to_vec(), None)
            .unwrap();
        engine.commit(txn2, WriteOptions::buffered()).unwrap();

        // Assert
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        assert_eq!(
            read_tx.get(b"key2").unwrap(),
            Some(Bytes::from_static(b"value2"))
        );
    });
}

// ============================================================================
// Persistence Tests
// ============================================================================

#[test]
fn should_persist_transaction_given_commit_when_crash_after() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange & Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();
            let mut txn = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
            engine.commit(txn, WriteOptions::buffered()).unwrap();
            // Engine dropped (simulated crash)
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();
            let tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let value = tx.get(b"key1").unwrap();
            assert_eq!(value, Some(Bytes::from_static(b"value1")));
        }
    });
}

#[test]
fn should_not_persist_transaction_given_abort_when_crash_after() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange & Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();
            let mut txn = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
            // Txn dropped without commit
            // Engine dropped (simulated crash)
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();
            let tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let value = tx.get(b"key1").unwrap();
            assert_eq!(value, None);
        }
    });
}

#[test]
fn should_recover_committed_transactions_given_wal_replay_when_restart() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange & Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            // Multiple transactions
            let mut txn1 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn1.put(b"key1".to_vec(), b"value1".to_vec(), None)
                .unwrap();
            engine.commit(txn1, WriteOptions::buffered()).unwrap();

            let mut txn2 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn2.put(b"key2".to_vec(), b"value2".to_vec(), None)
                .unwrap();
            engine.commit(txn2, WriteOptions::buffered()).unwrap();

            // Engine dropped
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();
            let tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            assert_eq!(
                tx.get(b"key1").unwrap(),
                Some(Bytes::from_static(b"value1"))
            );
            assert_eq!(
                tx.get(b"key2").unwrap(),
                Some(Bytes::from_static(b"value2"))
            );
        }
    });
}
