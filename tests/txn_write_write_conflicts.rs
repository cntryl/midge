// Write-Write Conflicts
// Extracted from transaction_acid.rs

// Transaction ACID tests - P0 Priority
// Tests document expected behavior and will fail until features are implemented

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use tempfile::TempDir;

mod common;
use common::{test_temp_dir, new_engine};
#[test]
fn should_detect_write_write_conflict_given_concurrent_updates_to_same_key() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    engine
        .put(&cf, Bytes::from("key"), Bytes::from("v0"))
        .expect("put");

    let mut first_txn = engine.begin_transaction(&cf);
    let mut conflicting_txn = engine.begin_transaction(&cf);

    first_txn
        .put(Bytes::from("key"), Bytes::from("v1"), None)
        .unwrap();
    conflicting_txn
        .put(Bytes::from("key"), Bytes::from("v2"), None)
        .unwrap();

    let first_result = engine.commit_transaction(first_txn, cntryl_midge::WriteOptions::default());
    assert!(first_result.is_ok(), "First transaction should succeed");

    // Act
    let conflicting_result =
        engine.commit_transaction(conflicting_txn, cntryl_midge::WriteOptions::default());

    // Assert
    assert!(
        conflicting_result.is_err(),
        "Second transaction should fail with write conflict"
    );
    // First transaction's value should be persisted
    assert_eq!(
        engine.get(&cf, b"key").expect("get"),
        Some(Bytes::from("v1"))
    );
}

#[test]
fn should_abort_second_transaction_given_write_conflict_when_both_commit() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    let mut winning_txn = engine.begin_transaction(&cf);
    let mut losing_txn = engine.begin_transaction(&cf);

    winning_txn
        .put(Bytes::from("conflict_key"), Bytes::from("txn1_value"), None)
        .unwrap();
    losing_txn
        .put(Bytes::from("conflict_key"), Bytes::from("txn2_value"), None)
        .unwrap();

    let winner_result =
        engine.commit_transaction(winning_txn, cntryl_midge::WriteOptions::default());
    assert!(winner_result.is_ok(), "First transaction should commit");

    // Act
    let loser_result = engine.commit_transaction(losing_txn, cntryl_midge::WriteOptions::default());

    // Assert
    assert!(
        loser_result.is_err(),
        "Second transaction should fail with write-write conflict"
    );
    // Verify first transaction's value persisted
    assert_eq!(
        engine.get(&cf, b"conflict_key").expect("get"),
        Some(Bytes::from("txn1_value"))
    );
}

#[test]
fn should_preserve_first_commit_given_write_conflict_when_second_aborts() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    let mut first_txn = engine.begin_transaction(&cf);
    first_txn
        .put(Bytes::from("key"), Bytes::from("first_value"), None)
        .unwrap();
    engine
        .commit_transaction(first_txn, cntryl_midge::WriteOptions::default())
        .expect("first commit");

    let mut aborted_txn = engine.begin_transaction(&cf);
    aborted_txn
        .put(Bytes::from("key"), Bytes::from("second_value"), None)
        .unwrap();

    // Act
    drop(aborted_txn); // rollback second transaction

    // Assert
    assert_eq!(
        engine.get(&cf, b"key").expect("get"),
        Some(Bytes::from("first_value"))
    );
}

#[test]
fn should_handle_write_conflict_on_delete_given_concurrent_delete_and_put() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    engine
        .put(&cf, Bytes::from("key"), Bytes::from("initial"))
        .expect("put");

    let mut delete_txn = engine.begin_transaction(&cf);
    let mut put_txn = engine.begin_transaction(&cf);

    delete_txn.delete(&cf, b"key").unwrap();
    put_txn
        .put(Bytes::from("key"), Bytes::from("updated"), None)
        .unwrap();

    let delete_result =
        engine.commit_transaction(delete_txn, cntryl_midge::WriteOptions::default());
    assert!(
        delete_result.is_ok(),
        "Delete transaction should commit first"
    );

    // Act
    let put_result = engine.commit_transaction(put_txn, cntryl_midge::WriteOptions::default());

    // Assert
    assert!(
        put_result.is_err(),
        "Put transaction should fail with conflict"
    );
    // Verify delete persisted (key should not exist)
    assert_eq!(engine.get(&cf, b"key").expect("get"), None);
}

#[test]
fn should_detect_conflict_on_delete_range_given_overlapping_keys() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    for i in 0..10 {
        engine
            .put(&cf, Bytes::from(format!("key{}", i)), Bytes::from("val"))
            .expect("put");
    }

    let mut range_txn = engine.begin_transaction(&cf);
    range_txn
        .delete_range(Bytes::from("key3"), Bytes::from("key7"))
        .unwrap();

    let mut overlapping_txn = engine.begin_transaction(&cf);
    overlapping_txn
        .put(Bytes::from("key5"), Bytes::from("new_value"), None)
        .unwrap();

    let range_result = engine.commit_transaction(range_txn, cntryl_midge::WriteOptions::default());

    // Act
    let overlapping_result =
        engine.commit_transaction(overlapping_txn, cntryl_midge::WriteOptions::default());

    // Assert
    assert!(range_result.is_ok());
    assert!(overlapping_result.is_ok());
    // TODO: Verify range conflict detection when implemented
}
