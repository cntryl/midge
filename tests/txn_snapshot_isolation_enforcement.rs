// Snapshot Isolation Enforcement
// Extracted from transaction_acid.rs

// Transaction ACID tests - P0 Priority
// Tests document expected behavior and will fail until features are implemented

use bytes::Bytes;
use cntryl_midge::{KvStore, KvTransaction};
use std::sync::Arc;

mod common;
use common::new_engine;

#[test]
fn should_read_at_begin_sequence_given_transaction_when_using_transaction_get() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    engine.put(&cf, b"key", b"initial").expect("put");

    let mut txn = engine.begin_transaction(&cf).expect("begin_transaction");
    let begin_value = txn.get(b"key").expect("get");

    // Act
    engine.put(&cf, b"key", b"updated").expect("put");

    let second_value = txn.get(b"key").expect("get");

    // Assert
    assert_eq!(begin_value, Some(Bytes::from("initial")));
    assert_eq!(second_value, Some(Bytes::from("initial")));
}

#[test]
fn should_not_see_concurrent_writes_given_transaction_when_snapshot_isolated() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    engine.put(&cf, b"key1", b"v1").expect("put");

    let mut txn1 = engine.begin_transaction(&cf).expect("begin_transaction");

    // Act
    let mut txn2 = engine.begin_transaction(&cf).expect("begin_transaction");
    txn2.put(b"key2", b"v2").unwrap();
    engine
        .commit_transaction(txn2, cntryl_midge::WriteOptions::default())
        .expect("commit");

    let value = txn1.get(b"key2").expect("get");

    // Assert
    assert_eq!(
        value, None,
        "Should not see writes committed after transaction began"
    );
}

#[test]
fn should_see_own_writes_given_transaction_when_reading_staged_mutations() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    // Act
    let mut txn = engine.begin_transaction(&cf).expect("begin_transaction");
    txn.put(b"new_key", b"new_value").unwrap();

    // Read own write
    let value = txn.get(b"new_key").expect("get");

    // Assert
    assert_eq!(value, Some(Bytes::from("new_value")));
}

#[test]
fn should_track_reads_given_transaction_get_when_validating_conflicts() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    engine.put(&cf, b"key", b"v1").expect("put");

    let mut txn1 = engine.begin_transaction(&cf).expect("begin_transaction");
    let _ = txn1.get(b"key").expect("get");

    // Act
    let mut txn2 = engine.begin_transaction(&cf).expect("begin_transaction");
    txn2.put(b"key", b"v2").unwrap();
    engine
        .commit_transaction(txn2, cntryl_midge::WriteOptions::default())
        .expect("commit");

    txn1.put(b"other_key", b"value").unwrap();
    let result = engine.commit_transaction(txn1, cntryl_midge::WriteOptions::default());

    // Assert
    assert!(result.is_err(), "Should detect read-write conflict");
}

#[test]
fn should_provide_consistent_view_given_multiple_reads_when_snapshot_isolated() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
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
    assert_eq!(first_read, Some(Bytes::from("v1")));
    assert_eq!(second_read, Some(Bytes::from("v1")));
    assert_eq!(key2_read, Some(Bytes::from("v2")));
}
