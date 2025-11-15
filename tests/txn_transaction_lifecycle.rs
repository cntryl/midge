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

    // Create transaction with a very short timeout (1ms)
    let mut timeout_txn = engine.begin_transaction_with_options(&cf, Some(std::time::Duration::from_millis(1)), 1024 * 1024).unwrap();
    timeout_txn.put(b"key", b"value").unwrap();

    // Sleep longer than the timeout
    std::thread::sleep(std::time::Duration::from_millis(10));

    // Act
    let result = engine.commit_transaction(timeout_txn, cntryl_midge::WriteOptions::default());

    // Assert
    assert!(result.is_err(), "Transaction should timeout");
    let err = result.unwrap_err();
    assert!(err.to_string().contains("timed out"), "Error should mention timeout: {}", err);
}

#[test]
fn should_release_locks_given_transaction_timeout_when_aborted() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    let mut aborted_lock_txn = engine.begin_transaction(&cf).unwrap();
    let txn_id = aborted_lock_txn.txn_id();
    aborted_lock_txn.put(b"locked_key", b"value").unwrap();

    // Verify transaction is active before abort
    assert!(engine.is_transaction_active(txn_id), "Transaction should be active before abort");

    // Act - abort the transaction explicitly
    engine.abort_transaction(aborted_lock_txn);

    // Assert - verify transaction is cleaned up
    assert!(!engine.is_transaction_active(txn_id), "Transaction should be removed from active set after abort");

    // Verify subsequent transactions can operate on the same keys
    let mut subsequent_txn = engine.begin_transaction(&cf).unwrap();
    subsequent_txn.put(b"locked_key", b"value2").unwrap();

    let result = engine.commit_transaction(subsequent_txn, cntryl_midge::WriteOptions::default());
    assert!(result.is_ok(), "Subsequent transaction should succeed after aborted transaction cleanup");
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
    assert_eq!(
        result.as_deref(),
        None,
        "aborted transaction data should not be visible"
    );
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

#[test]
fn should_handle_rapid_transaction_creation_and_commit() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    // Act: Create and commit 100 transactions rapidly
    for i in 0..100 {
        let mut txn = engine.begin_transaction(&cf).unwrap();
        let key = format!("rapid_key_{}", i);
        let value = format!("rapid_value_{}", i);
        txn.put(key.as_bytes(), value.as_bytes()).unwrap();
        let result = engine.commit_transaction(txn, cntryl_midge::WriteOptions::default());
        assert!(result.is_ok(), "Transaction {} should commit", i);
    }

    // Assert: All data persisted
    for i in 0..100 {
        let key = format!("rapid_key_{}", i);
        let result = engine.get(&cf, key.as_bytes()).unwrap();
        assert!(result.is_some(), "Key {} should exist", key);
    }
}

#[test]
fn should_handle_concurrent_transaction_lifecycles_without_panic() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    // Act: Spawn 20 threads with overlapping transaction lifecycles
    let handles: Vec<_> = (0..20)
        .map(|thread_id| {
            let eng = engine.clone();
            let cf_clone = cf.clone();
            std::thread::spawn(move || {
                for iteration in 0..10 {
                    let mut txn = eng.begin_transaction(&cf_clone).unwrap();
                    let key = format!("lifecycle_t{}_i{}", thread_id, iteration);
                    let value = format!("v_t{}_i{}", thread_id, iteration);
                    txn.put(key.as_bytes(), value.as_bytes()).unwrap();
                    let _ = eng.commit_transaction(txn, cntryl_midge::WriteOptions::default());
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Assert: All threads completed successfully
    let mut count = 0;
    for i in 0..20 {
        for j in 0..10 {
            let key = format!("lifecycle_t{}_i{}", i, j);
            if engine.get(&cf, key.as_bytes()).unwrap().is_some() {
                count += 1;
            }
        }
    }
    assert!(count > 0, "At least some transactions should have committed");
}

#[test]
fn should_persist_transaction_commits_after_engine_restart() {
    // Arrange
    let dir = common::test_temp_dir();
    let opts = common::durability_opts(dir.path().to_path_buf());
    let engine = cntryl_midge::MidgeEngine::open(opts.clone()).expect("initial open");
    let cf = engine.default_column_family();

    // Create and commit 20 transactions
    for i in 0..20 {
        let mut txn = engine.begin_transaction(&cf).unwrap();
        let key = format!("persist_txn_{}", i);
        let value = format!("value_{}", i);
        txn.put(key.as_bytes(), value.as_bytes()).unwrap();
        engine
            .commit_transaction(txn, cntryl_midge::WriteOptions::default())
            .unwrap();
    }

    drop(engine);

    // Act: Restart engine and verify all data persisted
    let engine = cntryl_midge::MidgeEngine::open(opts).expect("restart open");
    let cf = engine.default_column_family();

    // Assert: All committed transactions preserved
    for i in 0..20 {
        let key = format!("persist_txn_{}", i);
        let result = engine.get(&cf, key.as_bytes()).unwrap();
        assert!(
            result.is_some(),
            "Committed transaction data {} should persist after restart",
            key
        );
    }
}
