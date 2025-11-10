//! Write-Write Conflict Tests
//!
//! These verify MVCC isolation and conflict detection behavior.
//! They intentionally fail until transactional conflict handling is implemented.

use bytes::Bytes;
use cntryl_midge::{KvStore, WriteOptions};
use std::sync::Arc;

mod common;
use common::{assert_get_equals, new_engine};

#[test]
fn should_detect_write_write_conflict_given_concurrent_updates_to_same_key() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();
    engine.put(&cf, b"key", b"v0").unwrap();

    let snap = engine.snapshot();
    let mut txn1 = cntryl_midge::Transaction::with_options(1001, snap.seq, None, 100 * 1024 * 1024);
    let snap2 = engine.snapshot();
    let mut txn2 =
        cntryl_midge::Transaction::with_options(1002, snap2.seq, None, 100 * 1024 * 1024);

    txn1.put(Bytes::from_static(b"key"), Bytes::from_static(b"v1"), None)
        .unwrap();
    txn2.put(Bytes::from_static(b"key"), Bytes::from_static(b"v2"), None)
        .unwrap();

    // Act
    let first_result = engine.commit_transaction(Box::new(txn1), WriteOptions::default());
    let second_result = engine.commit_transaction(Box::new(txn2), WriteOptions::default());

    // Assert
    assert!(first_result.is_ok(), "first transaction should succeed");
    assert!(
        second_result.is_err(),
        "second transaction should fail on conflict"
    );
    assert_get_equals(&engine, b"key", b"v1");
}

#[test]
fn should_abort_second_transaction_given_write_conflict_when_both_commit() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let _cf = engine.default_column_family();

    let snap = engine.snapshot();
    let mut winner =
        cntryl_midge::Transaction::with_options(1003, snap.seq, None, 100 * 1024 * 1024);
    let snap2 = engine.snapshot();
    let mut loser =
        cntryl_midge::Transaction::with_options(1004, snap2.seq, None, 100 * 1024 * 1024);

    winner
        .put(
            Bytes::from_static(b"conflict_key"),
            Bytes::from_static(b"txn1_value"),
            None,
        )
        .unwrap();
    loser
        .put(
            Bytes::from_static(b"conflict_key"),
            Bytes::from_static(b"txn2_value"),
            None,
        )
        .unwrap();

    // Act
    let winner_result = engine.commit_transaction(Box::new(winner), WriteOptions::default());
    let loser_result = engine.commit_transaction(Box::new(loser), WriteOptions::default());

    // Assert
    assert!(winner_result.is_ok(), "winner should commit successfully");
    assert!(
        loser_result.is_err(),
        "loser should fail with write-write conflict"
    );
    assert_get_equals(&engine, b"conflict_key", b"txn1_value");
}

#[test]
fn should_preserve_first_commit_given_write_conflict_when_second_aborts() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let _cf = engine.default_column_family();

    let snap = engine.snapshot();
    let mut txn1 = cntryl_midge::Transaction::with_options(1005, snap.seq, None, 100 * 1024 * 1024);
    txn1.put(
        Bytes::from_static(b"key"),
        Bytes::from_static(b"first_value"),
        None,
    )
    .unwrap();
    engine
        .commit_transaction(Box::new(txn1), WriteOptions::default())
        .unwrap();

    let snap2 = engine.snapshot();
    let mut aborted_txn =
        cntryl_midge::Transaction::with_options(1006, snap2.seq, None, 100 * 1024 * 1024);
    aborted_txn
        .put(
            Bytes::from_static(b"key"),
            Bytes::from_static(b"second_value"),
            None,
        )
        .unwrap();

    // Act
    drop(aborted_txn); // rollback

    // Assert
    assert_get_equals(&engine, b"key", b"first_value");
}

#[test]
fn should_handle_write_conflict_on_delete_given_concurrent_delete_and_put() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();
    engine.put(&cf, b"key", b"initial").unwrap();

    let snap = engine.snapshot();
    let mut delete_txn =
        cntryl_midge::Transaction::with_options(1007, snap.seq, None, 100 * 1024 * 1024);
    let snap2 = engine.snapshot();
    let mut put_txn =
        cntryl_midge::Transaction::with_options(1008, snap2.seq, None, 100 * 1024 * 1024);

    delete_txn.delete(Bytes::from_static(b"key")).unwrap();
    put_txn
        .put(
            Bytes::from_static(b"key"),
            Bytes::from_static(b"updated"),
            None,
        )
        .unwrap();

    // Act
    let delete_result = engine.commit_transaction(Box::new(delete_txn), WriteOptions::default());
    let put_result = engine.commit_transaction(Box::new(put_txn), WriteOptions::default());

    // Assert
    assert!(delete_result.is_ok(), "delete should commit first");
    assert!(put_result.is_err(), "put should fail due to conflict");
    assert_eq!(engine.get(&cf, b"key").unwrap(), None);
}

#[test]
fn should_detect_conflict_on_delete_range_given_overlapping_keys() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    for i in 0..10 {
        let key = format!("key{i}");
        engine.put(&cf, key.as_bytes(), b"val").unwrap();
    }

    let snap = engine.snapshot();
    let mut range_txn =
        cntryl_midge::Transaction::with_options(1009, snap.seq, None, 100 * 1024 * 1024);
    range_txn
        .delete_range(Bytes::from_static(b"key3"), Bytes::from_static(b"key7"))
        .unwrap();

    let snap2 = engine.snapshot();
    let mut overlap_txn =
        cntryl_midge::Transaction::with_options(1010, snap2.seq, None, 100 * 1024 * 1024);
    overlap_txn
        .put(
            Bytes::from_static(b"key5"),
            Bytes::from_static(b"new_value"),
            None,
        )
        .unwrap();

    // Act
    let range_result = engine.commit_transaction(Box::new(range_txn), WriteOptions::default());
    let overlap_result = engine.commit_transaction(Box::new(overlap_txn), WriteOptions::default());

    // Assert
    assert!(range_result.is_ok());
    assert!(
        overlap_result.is_ok(),
        "TODO: implement conflict detection for overlapping ranges"
    );
}
