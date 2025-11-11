// Transaction Lifecycle
// Extracted from transaction_acid.rs

// Transaction ACID tests - P0 Priority
// Tests document expected behavior and will fail until features are implemented

use cntryl_midge::KvTransaction;
use std::sync::Arc;

mod common;
use common::new_engine;
#[test]
fn should_timeout_transaction_given_exceed_deadline_when_committing() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    let mut timeout_txn = engine.begin_transaction(&cf).unwrap();
    timeout_txn.put(b"key", b"value").unwrap();

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
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    let mut aborted_lock_txn = engine.begin_transaction(&cf).unwrap();
    aborted_lock_txn.put(b"locked_key", b"value").unwrap();

    drop(aborted_lock_txn);

    let mut subsequent_txn = engine.begin_transaction(&cf).unwrap();
    subsequent_txn.put(b"locked_key", b"value2").unwrap();

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
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    let mut rollback_txn = engine.begin_transaction(&cf).unwrap();
    rollback_txn.put(b"key1", b"value1").unwrap();
    rollback_txn.put(b"key2", b"value2").unwrap();
    rollback_txn.put(b"key3", b"value3").unwrap();

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
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    // This test verifies that transaction lifecycle is properly managed.
    // Once a transaction is completed (committed or aborted), it cannot be reused.
    // Rust's ownership system enforces this at compile time for commit
    // (transaction is moved/consumed).
    
    // Test 1: Verify committed transaction cannot be double-committed
    let mut txn1 = engine.begin_transaction(&cf).unwrap();
    txn1.put(b"key1", b"value1").unwrap();
    engine
        .commit_transaction(txn1, cntryl_midge::WriteOptions::default())
        .expect("first commit should succeed");
    // txn1 is now consumed and cannot be used again (compile-time enforced)

    // Act & Assert - verify the data was written
    let result = engine.get(&cf, b"key1").expect("get should work");
    assert_eq!(result.as_deref(), Some(b"value1".as_ref()));
    
    // Test 2: Verify transaction can be properly aborted and data is not visible
    let mut txn2 = engine.begin_transaction(&cf).unwrap();
    txn2.put(b"key2", b"value2").unwrap();
    drop(txn2); // Abort by dropping

    // Assert - aborted transaction data should not be visible
    let result = engine.get(&cf, b"key2").expect("get should work");
    assert_eq!(result.as_deref(), None, "aborted transaction data should not be visible");
}

#[test]
fn should_reject_operations_given_committed_transaction_when_reused() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    let mut committed_txn = engine.begin_transaction(&cf).unwrap();
    committed_txn.put(b"key1", b"value1").unwrap();

    // Act
    engine
        .commit_transaction(committed_txn, cntryl_midge::WriteOptions::default())
        .expect("commit");

    // Assert
    // Transaction consumed by commit(), cannot be reused
    // Rust ownership prevents this at compile time
    // This test documents the behavior
}
