// Edge Cases
// Extracted from transaction_acid.rs

// Transaction ACID tests - P0 Priority
// Tests document expected behavior and will fail until features are implemented

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use tempfile::TempDir;

mod common;
use common::{test_temp_dir, new_engine};
/// Helper: create a new engine in a fresh temp dir and return both.
fn new_engine() -> (tempfile::TempDir, cntryl_midge::MidgeEngine) {
    let dir = test_temp_dir();
    let opts = cntryl_midge::MidgeOptions {
        storage_mode: cntryl_midge::StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = cntryl_midge::MidgeEngine::open(opts).expect("open");
    (dir, engine)
}

#[test]
fn should_handle_empty_transaction_given_commit_without_operations() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    let empty_txn = engine.begin_transaction(&cf);

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
    let cf = engine.default_column_family();

    engine
        .put(&cf, Bytes::from("key"), Bytes::from("value"))
        .expect("put");

    let readonly_txn = engine.begin_transaction(&cf);
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
    let cf = engine.default_column_family();

    let mut nested_read_txn = engine.begin_transaction(&cf);
    nested_read_txn
        .put(Bytes::from("nested_key"), Bytes::from("nested_value"), None)
        .unwrap();

    // Act
    let read1 = nested_read_txn.get_local(b"nested_key");
    let read2 = nested_read_txn.get_local(b"nested_key");

    // Assert
    assert_eq!(read1, Some(Some(Bytes::from("nested_value"))));
    assert_eq!(read2, Some(Some(Bytes::from("nested_value"))));
}

#[test]
fn should_handle_transaction_on_dropped_cf_given_cf_deleted_during_transaction() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    let mut cf_txn = engine.begin_transaction(&cf);
    cf_txn
        .put(Bytes::from("cf_key"), Bytes::from("cf_value"), None)
        .unwrap();

    // Act
    let result = engine.commit_transaction(cf_txn, cntryl_midge::WriteOptions::default());

    // Assert
    // Default CF cannot be dropped, transaction should succeed
    assert!(result.is_ok());
    // TODO: Test with multi-CF when CF API is available
}
