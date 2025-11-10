// Edge Cases
// Extracted from transaction_acid.rs

// Transaction ACID tests - P0 Priority
// Tests document expected behavior and will fail until features are implemented

use bytes::Bytes;
use cntryl_midge::{KvStore, KvTransaction};
use std::sync::Arc;

mod common;
use common::new_engine;

#[test]
fn should_handle_empty_transaction_given_commit_without_operations() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    let empty_txn = engine
        .begin_transaction(&cf)
        .expect("Transaction creation failed");

    // Act
    let result = engine.commit_transaction(empty_txn, cntryl_midge::WriteOptions::default());

    // Assert
    assert!(
        result.is_ok(),
        "Empty transaction should commit successfully"
    );
}

#[test]
fn should_handle_read_only_transaction_given_no_writes_when_commit() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    engine.put(&cf, b"key", b"value").expect("put");

    let readonly_txn = engine
        .begin_transaction(&cf)
        .expect("Transaction creation failed");
    let snap = engine.snapshot();
    let _value = engine.get_at(b"key", &snap).expect("get_at");

    // Act
    let result = engine.commit_transaction(readonly_txn, cntryl_midge::WriteOptions::default());

    // Assert
    assert!(result.is_ok(), "Read-only transaction should commit");
}

#[test]
fn should_allow_nested_get_given_transaction_when_reading_own_writes() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    let mut nested_read_txn = engine
        .begin_transaction(&cf)
        .expect("Transaction creation failed");
    nested_read_txn.put(b"nested_key", b"nested_value").unwrap();

    // Act
    let read1 = nested_read_txn.get(b"nested_key").ok();
    let read2 = nested_read_txn.get(b"nested_key").ok();

    // Assert
    assert_eq!(read1, Some(Some(Bytes::from("nested_value"))));
    assert_eq!(read2, Some(Some(Bytes::from("nested_value"))));
}

#[test]
fn should_handle_transaction_on_dropped_cf_given_cf_deleted_during_transaction() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    let mut cf_txn = engine
        .begin_transaction(&cf)
        .expect("Transaction creation failed");
    cf_txn.put(b"cf_key", b"cf_value").unwrap();

    // Act
    let result = engine.commit_transaction(cf_txn, cntryl_midge::WriteOptions::default());

    // Assert
    // Default CF cannot be dropped, transaction should succeed
    assert!(result.is_ok());
    // TODO: Test with multi-CF when CF API is available
}
