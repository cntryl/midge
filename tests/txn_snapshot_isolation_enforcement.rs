// Snapshot Isolation Enforcement
// Extracted from transaction_acid.rs

// Transaction ACID tests - P0 Priority
// Tests document expected behavior and will fail until features are implemented

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use tempfile::TempDir;

mod common;
use common::{test_temp_dir, new_engine};
#[test]
fn should_read_at_begin_sequence_given_transaction_when_using_transaction_get() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    engine
        .put(&cf, Bytes::from("key"), Bytes::from("initial"))
        .expect("put");

    let mut txn = engine.begin_transaction(&cf);
    let begin_value = engine.transaction_get(&mut txn, b"key").expect("get");

    // Act
    engine
        .put(&cf, Bytes::from("key"), Bytes::from("updated"))
        .expect("put");

    let second_value = engine.transaction_get(&mut txn, b"key").expect("get");

    // Assert
    assert_eq!(begin_value, Some(Bytes::from("initial")));
    assert_eq!(second_value, Some(Bytes::from("initial")));
}

#[test]
fn should_not_see_concurrent_writes_given_transaction_when_snapshot_isolated() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    engine
        .put(&cf, Bytes::from("key1"), Bytes::from("v1"))
        .expect("put");

    let mut txn1 = engine.begin_transaction(&cf);

    // Act
    let mut txn2 = engine.begin_transaction(&cf);
    txn2.put(Bytes::from("key2"), Bytes::from("v2"), None)
        .unwrap();
    engine
        .commit_transaction(txn2, cntryl_midge::WriteOptions::default())
        .expect("commit");

    let value = engine.transaction_get(&mut txn1, b"key2").expect("get");

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
    let cf = engine.default_column_family();

    // Act
    let mut txn = engine.begin_transaction(&cf);
    txn.put(Bytes::from("new_key"), Bytes::from("new_value"), None)
        .unwrap();

    // Read own write
    let value = engine.transaction_get(&mut txn, b"new_key").expect("get");

    // Assert
    assert_eq!(value, Some(Bytes::from("new_value")));
}

#[test]
fn should_track_reads_given_transaction_get_when_validating_conflicts() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    engine
        .put(&cf, Bytes::from("key"), Bytes::from("v1"))
        .expect("put");

    let mut txn1 = engine.begin_transaction(&cf);
    let _ = engine.transaction_get(&mut txn1, b"key").expect("get");

    // Act
    let mut txn2 = engine.begin_transaction(&cf);
    txn2.put(Bytes::from("key"), Bytes::from("v2"), None)
        .unwrap();
    engine
        .commit_transaction(txn2, cntryl_midge::WriteOptions::default())
        .expect("commit");

    txn1.put(Bytes::from("other_key"), Bytes::from("value"), None)
        .unwrap();
    let result = engine.commit_transaction(txn1, cntryl_midge::WriteOptions::default());

    // Assert
    assert!(result.is_err(), "Should detect read-write conflict");
}

#[test]
fn should_provide_consistent_view_given_multiple_reads_when_snapshot_isolated() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    engine
        .put(&cf, Bytes::from("key1"), Bytes::from("v1"))
        .expect("put");
    engine
        .put(&cf, Bytes::from("key2"), Bytes::from("v2"))
        .expect("put");

    let mut txn = engine.begin_transaction(&cf);
    let first_read = engine.transaction_get(&mut txn, b"key1").expect("get");

    // Act
    engine
        .put(&cf, Bytes::from("key1"), Bytes::from("updated1"))
        .expect("put");
    engine
        .put(&cf, Bytes::from("key2"), Bytes::from("updated2"))
        .expect("put");

    let second_read = engine.transaction_get(&mut txn, b"key1").expect("get");
    let key2_read = engine.transaction_get(&mut txn, b"key2").expect("get");

    // Assert
    assert_eq!(first_read, Some(Bytes::from("v1")));
    assert_eq!(second_read, Some(Bytes::from("v1")));
    assert_eq!(key2_read, Some(Bytes::from("v2")));
}
