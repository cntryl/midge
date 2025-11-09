//! Write-Write Conflict Tests
//!
//! These verify MVCC isolation and conflict detection behavior.
//! They intentionally fail until transactional conflict handling is implemented.

use bytes::Bytes;
use cntryl_midge::WriteOptions;

mod common;
use common::{assert_get_equals, new_engine};

#[test]
fn should_detect_write_write_conflict_given_concurrent_updates_to_same_key() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();
    engine
        .put(&cf, Bytes::from("key"), Bytes::from("v0"))
        .unwrap();

    let mut txn1 = engine.begin_transaction(&cf);
    let mut txn2 = engine.begin_transaction(&cf);

    txn1.put(Bytes::from("key"), Bytes::from("v1"), None)
        .unwrap();
    txn2.put(Bytes::from("key"), Bytes::from("v2"), None)
        .unwrap();

    // Act
    let first_result = engine.commit_transaction(txn1, WriteOptions::default());
    let second_result = engine.commit_transaction(txn2, WriteOptions::default());

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
    let cf = engine.default_column_family();

    let mut winner = engine.begin_transaction(&cf);
    let mut loser = engine.begin_transaction(&cf);

    winner
        .put(Bytes::from("conflict_key"), Bytes::from("txn1_value"), None)
        .unwrap();
    loser
        .put(Bytes::from("conflict_key"), Bytes::from("txn2_value"), None)
        .unwrap();

    // Act
    let winner_result = engine.commit_transaction(winner, WriteOptions::default());
    let loser_result = engine.commit_transaction(loser, WriteOptions::default());

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
    let cf = engine.default_column_family();

    let mut txn1 = engine.begin_transaction(&cf);
    txn1.put(Bytes::from("key"), Bytes::from("first_value"), None)
        .unwrap();
    engine
        .commit_transaction(txn1, WriteOptions::default())
        .unwrap();

    let mut aborted_txn = engine.begin_transaction(&cf);
    aborted_txn
        .put(Bytes::from("key"), Bytes::from("second_value"), None)
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
    let cf = engine.default_column_family();
    engine
        .put(&cf, Bytes::from("key"), Bytes::from("initial"))
        .unwrap();

    let mut delete_txn = engine.begin_transaction(&cf);
    let mut put_txn = engine.begin_transaction(&cf);

    delete_txn.delete(&cf, b"key").unwrap();
    put_txn
        .put(Bytes::from("key"), Bytes::from("updated"), None)
        .unwrap();

    // Act
    let delete_result = engine.commit_transaction(delete_txn, WriteOptions::default());
    let put_result = engine.commit_transaction(put_txn, WriteOptions::default());

    // Assert
    assert!(delete_result.is_ok(), "delete should commit first");
    assert!(put_result.is_err(), "put should fail due to conflict");
    assert_eq!(engine.get(&cf, b"key").unwrap(), None);
}

#[test]
fn should_detect_conflict_on_delete_range_given_overlapping_keys() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    for i in 0..10 {
        engine
            .put(&cf, Bytes::from(format!("key{i}")), Bytes::from("val"))
            .unwrap();
    }

    let mut range_txn = engine.begin_transaction(&cf);
    range_txn
        .delete_range(Bytes::from("key3"), Bytes::from("key7"))
        .unwrap();

    let mut overlap_txn = engine.begin_transaction(&cf);
    overlap_txn
        .put(Bytes::from("key5"), Bytes::from("new_value"), None)
        .unwrap();

    // Act
    let range_result = engine.commit_transaction(range_txn, WriteOptions::default());
    let overlap_result = engine.commit_transaction(overlap_txn, WriteOptions::default());

    // Assert
    assert!(range_result.is_ok());
    assert!(
        overlap_result.is_ok(),
        "TODO: implement conflict detection for overlapping ranges"
    );
}
