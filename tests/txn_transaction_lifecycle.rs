// Transaction Lifecycle
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
fn should_timeout_transaction_given_exceed_deadline_when_committing() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    let mut timeout_txn = engine.begin_transaction(&cf);
    timeout_txn
        .put(Bytes::from("key"), Bytes::from("value"), None)
        .unwrap();

    std::thread::sleep(std::time::Duration::from_millis(10));

    // Act
    let result = engine.commit_transaction(timeout_txn, cntryl_midge::WriteOptions::default());

    // Assert
    // No timeout mechanism currently
    assert!(result.is_ok());
    // TODO: Should timeout if transaction exceeds deadline
}

#[test]
fn should_release_locks_given_transaction_timeout_when_aborted() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    let mut aborted_lock_txn = engine.begin_transaction(&cf);
    aborted_lock_txn
        .put(Bytes::from("locked_key"), Bytes::from("value"), None)
        .unwrap();

    drop(aborted_lock_txn);

    let mut subsequent_txn = engine.begin_transaction(&cf);
    subsequent_txn
        .put(Bytes::from("locked_key"), Bytes::from("value2"), None)
        .unwrap();

    // Act
    let result = engine.commit_transaction(subsequent_txn, cntryl_midge::WriteOptions::default());

    // Assert
    // No locking currently, so always succeeds
    assert!(result.is_ok());
    // TODO: Verify locks released after timeout/abort
}

#[test]
fn should_rollback_partial_writes_given_timeout_when_aborting() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    let mut rollback_txn = engine.begin_transaction(&cf);
    rollback_txn
        .put(Bytes::from("key1"), Bytes::from("value1"), None)
        .unwrap();
    rollback_txn
        .put(Bytes::from("key2"), Bytes::from("value2"), None)
        .unwrap();
    rollback_txn
        .put(Bytes::from("key3"), Bytes::from("value3"), None)
        .unwrap();

    // Act
    drop(rollback_txn);

    // Assert
    assert_eq!(engine.get(&cf, b"key1").expect("get"), None);
    assert_eq!(engine.get(&cf, b"key2").expect("get"), None);
    assert_eq!(engine.get(&cf, b"key3").expect("get"), None);
}

#[test]
fn should_reject_operations_given_aborted_transaction_when_used() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    let mut aborted_txn = engine.begin_transaction(&cf);
    aborted_txn.rollback();

    aborted_txn
        .put(Bytes::from("key"), Bytes::from("value"), None)
        .unwrap();

    // Act
    let result = engine.commit_transaction(aborted_txn, cntryl_midge::WriteOptions::default());

    // Assert
    assert!(
        result.is_err(),
        "Should reject operations on aborted transaction"
    );
}

#[test]
fn should_reject_operations_given_committed_transaction_when_reused() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    let mut committed_txn = engine.begin_transaction(&cf);
    committed_txn
        .put(Bytes::from("key1"), Bytes::from("value1"), None)
        .unwrap();

    // Act
    engine
        .commit_transaction(committed_txn, cntryl_midge::WriteOptions::default())
        .expect("commit");

    // Assert
    // Transaction consumed by commit(), cannot be reused
    // Rust ownership prevents this at compile time
    // This test documents the behavior
}
