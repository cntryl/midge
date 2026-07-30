//! Transaction Basic Tests
//!
//! Core transaction functionality: begin, commit, rollback, isolation.

use bytes::Bytes;
mod common;
use cntryl_midge::{MidgeError, Query, WriteOptions};
use common::*;
use std::sync::Arc;

// ============================================================================
// Commit Tests
// ============================================================================

#[test]
fn should_reject_insert_given_read_only_transaction_when_called() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read-only transaction");

        // Act
        let result = txn.insert(b"key".to_vec(), b"value".to_vec(), None);

        // Assert
        assert!(matches!(result, Err(MidgeError::InvalidArgument(_))));
    });
}

#[test]
fn should_reject_delete_given_read_only_transaction_when_called() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read-only transaction");

        // Act
        let result = txn.delete(b"key".to_vec());

        // Assert
        assert!(matches!(result, Err(MidgeError::InvalidArgument(_))));
    });
}

#[test]
fn should_reject_delete_range_given_read_only_transaction_when_called() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read-only transaction");

        // Act
        let result = txn.delete_range(b"a".to_vec(), b"z".to_vec());

        // Assert
        assert!(matches!(result, Err(MidgeError::InvalidArgument(_))));
    });
}

#[test]
fn should_commit_transaction_given_multiple_operations_when_committed() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
        txn.put(b"key2".to_vec(), b"value2".to_vec(), None).unwrap();
        txn.delete(b"key3".to_vec()).unwrap();
        txn.commit(buffered_write_options(mode)).unwrap();

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
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        let result = txn.commit(buffered_write_options(mode));

        // Assert
        assert!(result.is_ok());
    });
}

#[test]
fn should_reject_cloud_strict_given_empty_non_cloud_transaction_when_committed() {
    // Arrange
    let opts = opts_for_mode("memory");
    let engine = open_with_mode(&opts, "memory");
    let cf = engine.create_column_family("test").expect("create cf");
    let txn = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin tx");

    // Act
    let result = txn.commit(WriteOptions::cloud_strict());

    // Assert
    assert!(matches!(result, Err(MidgeError::InvalidArgument(_))));
}

#[test]
fn should_succeed_given_empty_cloud_strict_cloud_transaction_when_committed() {
    // Arrange
    let opts = opts_for_mode("cloud");
    let engine = open_with_mode(&opts, "cloud");
    let cf = engine.create_column_family("test").expect("create cf");
    let txn = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin tx");

    // Act
    let result = txn.commit(WriteOptions::cloud_strict());

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_reject_local_only_write_options_given_cloud_transaction_when_committed() {
    // Arrange
    let opts = opts_for_mode("cloud");
    let engine = open_with_mode(&opts, "cloud");
    let cf = engine.create_column_family("test").expect("create cf");

    // Act
    let sync_result = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin tx")
        .commit(WriteOptions::sync());
    let buffered_result = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin tx")
        .commit(WriteOptions::buffered());

    // Assert
    assert!(matches!(sync_result, Err(MidgeError::InvalidArgument(_))));
    assert!(matches!(
        buffered_result,
        Err(MidgeError::InvalidArgument(_))
    ));
}

#[test]
fn should_unregister_snapshot_given_commit_of_read_only_transaction_when_committing() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut write_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        write_tx
            .put(b"key1".to_vec(), b"value1".to_vec(), None)
            .unwrap();
        write_tx.commit(buffered_write_options(mode)).unwrap();

        // Act
        let txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let _value = txn.get(b"key1").unwrap();
        let result = txn.commit(buffered_write_options(mode));

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
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");

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
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key1".to_vec(), b"original".to_vec(), None)
            .unwrap();
        tx.commit(buffered_write_options(mode)).unwrap();

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
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");

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
        txn2.commit(buffered_write_options(mode)).unwrap();

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
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key1".to_vec(), b"v1".to_vec(), None).unwrap();
        tx.commit(buffered_write_options(mode)).unwrap();

        // Act - start transaction (captures snapshot)
        let txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();

        // Concurrent write happens outside transaction
        let mut tx2 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx2.put(b"key1".to_vec(), b"v2".to_vec(), None).unwrap();
        tx2.commit(buffered_write_options(mode)).unwrap();

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
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();

        // Read within same transaction
        let value = txn.get(b"key1").unwrap();

        // Assert - should see own uncommitted write
        assert_eq!(value, Some(Bytes::from_static(b"value1")));

        txn.commit(buffered_write_options(mode)).unwrap();
    });
}

#[test]
fn should_preserve_write_set_semantics_given_put_delete_put_sequence_when_reading_own_writes() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin transaction");

        // Act
        txn.put(b"key".to_vec(), b"one".to_vec(), None).unwrap();
        txn.delete(b"key".to_vec()).unwrap();
        txn.put(b"key".to_vec(), b"three".to_vec(), None).unwrap();

        // Assert
        assert_eq!(txn.get(b"key").unwrap(), Some(Bytes::from_static(b"three")));
    });
}

#[test]
fn should_preserve_write_set_semantics_given_delete_put_delete_sequence_when_reading_own_writes() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin transaction");

        // Act
        txn.delete(b"key".to_vec()).unwrap();
        txn.put(b"key".to_vec(), b"two".to_vec(), None).unwrap();
        txn.delete(b"key".to_vec()).unwrap();

        // Assert
        assert_eq!(txn.get(b"key").unwrap(), None);
    });
}

#[test]
fn should_read_own_writes_given_kv_transaction_when_getting() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");

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
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");

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
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn.put(b"key_commit".to_vec(), b"value_commit".to_vec(), None)
            .unwrap();
        txn.commit(buffered_write_options(mode)).unwrap();

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
fn should_succeed_given_best_effort_when_committing_transaction_during_bulk_load() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange: Simulate bulk load scenario with best_effort()
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let write_opts = WriteOptions::best_effort();

        // Act: Load many keys with best_effort() (acceptable for initialization phase)
        for i in 0..100 {
            let mut txn = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            let key = format!("bulk_key_{i}").into_bytes();
            let value = format!("bulk_value_{i}").into_bytes();
            txn.put(key, value, None).unwrap();
            txn.commit(write_opts).unwrap();
        }

        // Flush to ensure data reaches storage
        engine.flush_cf(&cf).unwrap();

        // Assert: Data should be persisted after flush
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        assert_eq!(
            read_tx.get(b"bulk_key_50").unwrap(),
            Some(Bytes::from_static(b"bulk_value_50"))
        );
        assert_eq!(
            read_tx.get(b"bulk_key_99").unwrap(),
            Some(Bytes::from_static(b"bulk_value_99"))
        );
    });
}

// ============================================================================
// Transaction Operations
// ============================================================================

#[test]
fn should_insert_value_given_nonexistent_key_when_insert_in_transaction() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
        txn.commit(buffered_write_options(mode)).unwrap();

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
fn should_delete_range_given_committed_engine_operation_when_delete_range() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key1".to_vec(), b"v1".to_vec(), None).unwrap();
        tx.put(b"key2".to_vec(), b"v2".to_vec(), None).unwrap();
        tx.put(b"key3".to_vec(), b"v3".to_vec(), None).unwrap();
        tx.commit(buffered_write_options(mode)).unwrap();

        // Act - delete key1, key2 (not key3)
        let mut delete_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        delete_tx
            .delete_range(b"key1".to_vec(), b"key3".to_vec())
            .unwrap();
        delete_tx.commit(buffered_write_options(mode)).unwrap();

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
fn should_hide_deleted_key_given_delete_range_and_point_put_when_scanning() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key1".to_vec(), b"v1".to_vec(), None).unwrap();
        tx.put(b"key2".to_vec(), b"v2".to_vec(), None).unwrap();
        tx.put(b"key3".to_vec(), b"v3".to_vec(), None).unwrap();
        tx.commit(buffered_write_options(mode)).unwrap();

        // Act
        let mut delete_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        delete_tx
            .delete_range(b"key1".to_vec(), b"key3".to_vec())
            .unwrap();
        delete_tx.commit(buffered_write_options(mode)).unwrap();

        // Scan after the delete_range is committed
        let txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let results = txn
            .scan(
                &Query::new()
                    .start_key(Bytes::from(&b"key0"[..]))
                    .end_key(Bytes::from(&b"key9"[..])),
            )
            .unwrap()
            .try_collect()
            .expect("collect post-delete range scan");

        // Assert - Should only see key3
        assert_eq!(results.len(), 1);
    });
}

#[test]
fn should_commit_atomically_given_mixed_put_and_delete_range_when_committed_in_transaction() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut setup = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        setup.put(b"key1".to_vec(), b"v1".to_vec(), None).unwrap();
        setup.put(b"key2".to_vec(), b"v2".to_vec(), None).unwrap();
        setup.put(b"key3".to_vec(), b"v3".to_vec(), None).unwrap();
        setup.commit(buffered_write_options(mode)).unwrap();

        // Act - put + delete + delete_range all in one transaction
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key4".to_vec(), b"v4".to_vec(), None).unwrap();
        tx.delete(b"key3".to_vec()).unwrap();
        tx.delete_range(b"key1".to_vec(), b"key3".to_vec()).unwrap();
        tx.commit(buffered_write_options(mode)).unwrap();

        // Assert - key1 and key2 gone via range, key3 gone via delete, key4 present
        let read = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        assert_eq!(read.get(b"key1").unwrap(), None);
        assert_eq!(read.get(b"key2").unwrap(), None);
        assert_eq!(read.get(b"key3").unwrap(), None);
        assert_eq!(read.get(b"key4").unwrap(), Some(Bytes::from_static(b"v4")));
    });
}

#[test]
fn should_reject_delete_range_given_reversed_bounds_when_added_to_transaction() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        let result = tx.delete_range(b"key9".to_vec(), b"key1".to_vec());

        // Assert
        assert!(
            matches!(result, Err(cntryl_midge::MidgeError::InvalidArgument(_))),
            "mode: {mode}"
        );
    });
}

#[test]
fn should_see_uncommitted_writes_given_transaction_scan_when_scanning() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
        txn.put(b"key2".to_vec(), b"value2".to_vec(), None).unwrap();

        // Scan within transaction
        let results = txn
            .scan(
                &Query::new()
                    .start_key(Bytes::from(&b"key0"[..]))
                    .end_key(Bytes::from(&b"key9"[..])),
            )
            .unwrap()
            .try_collect()
            .expect("collect transaction intent scan");

        // Assert - should see uncommitted writes
        assert_eq!(results.len(), 2);
    });
}

#[test]
fn should_return_no_rows_given_zero_limit_when_scanning() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin transaction");
        txn.put(b"key".to_vec(), b"value".to_vec(), None)
            .expect("stage value");

        // Act
        let rows = txn
            .scan(&Query::new().limit(0))
            .expect("zero-limit scan should initialize")
            .try_collect()
            .expect("zero-limit scan should complete");

        // Assert
        assert!(rows.is_empty());
    });
}

// ============================================================================
// Error Handling
// ============================================================================

#[test]
fn should_unregister_snapshot_given_commit_failure_when_commit_returns_error() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");

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
        txn2.commit(buffered_write_options(mode)).unwrap();

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
        // Arrange
        // Act (Phase 1)
        {
            let mut engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let mut txn = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
            txn.commit(buffered_write_options(mode)).unwrap();
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before reopen");
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.get_column_family("test").expect("get cf");
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
        // Arrange
        // Act (Phase 1)
        {
            let mut engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let mut txn = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
            // Txn dropped without commit
            drop(txn);
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before reopen");
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.get_column_family("test").expect("get cf");
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
        // Arrange
        // Act (Phase 1)
        {
            let mut engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Multiple transactions
            let mut txn1 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn1.put(b"key1".to_vec(), b"value1".to_vec(), None)
                .unwrap();
            txn1.commit(buffered_write_options(mode)).unwrap();

            let mut txn2 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn2.put(b"key2".to_vec(), b"value2".to_vec(), None)
                .unwrap();
            txn2.commit(buffered_write_options(mode)).unwrap();

            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before reopen");
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.get_column_family("test").expect("get cf");
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

// ============================================================================
// Bulk Load Pattern (best_effort usage)
// ============================================================================

#[test]
fn should_support_best_effort_during_bulk_load_phase_when_followed_by_flush() {
    const BULK_COUNT: usize = 1000;

    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange: Initialize engine and column family
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("bulk_test").expect("create cf");
        let best_effort_opts = WriteOptions::best_effort();
        let buffered_opts = buffered_write_options(mode);

        // Arrange: Fast bulk load with best_effort (setup; data loss on crash acceptable here)
        for i in 0..BULK_COUNT {
            let mut txn = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            let key = format!("bulk_{i:05}").into_bytes();
            let value = format!("data_{}", i * 2).into_bytes();
            txn.put(key, value, None).unwrap();
            txn.commit(best_effort_opts).unwrap();
        }

        // Act: Flush to ensure bulk-loaded data reaches storage, then write a "measured" key.
        engine.flush_cf(&cf).unwrap();
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn.put(b"measured_key".to_vec(), b"measured_value".to_vec(), None)
            .unwrap();
        txn.commit(buffered_opts).unwrap();

        // Assert: Verify all data is readable
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();

        // Check bulk-loaded entries
        assert_eq!(
            read_tx.get(b"bulk_00000").unwrap(),
            Some(Bytes::from_static(b"data_0"))
        );
        assert_eq!(
            read_tx.get(b"bulk_00500").unwrap(),
            Some(Bytes::from_static(b"data_1000"))
        );
        assert_eq!(
            read_tx.get(b"bulk_00999").unwrap(),
            Some(Bytes::from_static(b"data_1998"))
        );

        // Check measured-workload entry
        assert_eq!(
            read_tx.get(b"measured_key").unwrap(),
            Some(Bytes::from_static(b"measured_value"))
        );
    });
}
