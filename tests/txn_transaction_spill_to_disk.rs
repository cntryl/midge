// Transaction Large Data Tests
// Tests that transactions can handle large amounts of data through spill-to-disk mechanism
// These tests verify observable behavior (correctness) without relying on internal implementation details

use bytes::Bytes;
use cntryl_midge::KvTransaction;

mod common;
use common::new_engine;

#[test]
fn should_commit_large_transaction_given_many_writes() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    // Create transaction with small memory limit (1MB) to force spilling
    let mut large_txn = engine
        .begin_transaction_with_options(&cf, None, 1024 * 1024, cntryl_midge::IsolationLevel::default())
        .expect("begin");

    // Act - Add 2MB of data (2000 keys × 1024 bytes each)
    for i in 0..2000 {
        large_txn
            .put(format!("key{:06}", i).as_bytes(), &vec![0u8; 1024])
            .expect("put");
    }

    // Commit the transaction
    engine
        .commit_transaction(large_txn, cntryl_midge::WriteOptions::default())
        .expect("commit");

    // Assert - Verify all keys are present after commit
    for i in 0..2000 {
        let key = format!("key{:06}", i);
        let value = engine.get(&cf, key.as_bytes()).expect("get");
        assert!(
            value.is_some(),
            "Key {} should exist after large transaction commit",
            key
        );
    }
}

#[test]
fn should_preserve_data_integrity_given_large_transaction_with_values() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    // Create transaction with small memory limit (512KB) to force spilling
    let mut large_txn = engine
        .begin_transaction_with_options(&cf, None, 512 * 1024, cntryl_midge::IsolationLevel::default())
        .expect("begin");

    // Act - Add 1.5MB of data with specific pattern
    for i in 0..1500 {
        large_txn
            .put(
                format!("large_key_{:06}", i).as_bytes(),
                &vec![0xABu8; 1024],
            )
            .expect("put");
    }

    engine
        .commit_transaction(large_txn, cntryl_midge::WriteOptions::default())
        .expect("commit");

    // Assert - Verify all data is correct after commit
    for i in 0..1500 {
        let key = format!("large_key_{:06}", i);
        let value = engine.get(&cf, key.as_bytes()).expect("get");
        assert!(value.is_some(), "Key {} should exist after commit", key);
        assert_eq!(
            value.unwrap(),
            Bytes::from(vec![0xABu8; 1024]),
            "Value should match for key {}",
            key
        );
    }
}

#[test]
fn should_commit_successfully_given_large_transaction() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    // Create transaction with small memory limit (256KB)
    let mut large_txn = engine
        .begin_transaction_with_options(&cf, None, 256 * 1024, cntryl_midge::IsolationLevel::default())
        .expect("begin");

    // Act - Add 2MB of data
    for i in 0..2000 {
        large_txn
            .put(
                format!("cleanup_key_{:06}", i).as_bytes(),
                &vec![0xCCu8; 1024],
            )
            .expect("put");
    }

    // Commit should succeed
    engine
        .commit_transaction(large_txn, cntryl_midge::WriteOptions::default())
        .expect("commit");

    // Assert - Verify data is accessible
    for i in (0..2000).step_by(100) {
        let key = format!("cleanup_key_{:06}", i);
        let value = engine.get(&cf, key.as_bytes()).expect("get");
        assert!(value.is_some(), "Key {} should exist", key);
    }
}

#[test]
fn should_rollback_given_transaction_dropped_without_commit() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    // Act - Create transaction with large data, then drop without committing
    {
        let mut large_txn = engine
            .begin_transaction_with_options(&cf, None, 256 * 1024, cntryl_midge::IsolationLevel::default())
            .expect("begin");

        for i in 0..2000 {
            large_txn
                .put(
                    format!("abort_key_{:06}", i).as_bytes(),
                    &vec![0xDDu8; 1024],
                )
                .expect("put");
        }
        // Transaction dropped here without commit (implicit rollback)
    }

    // Assert - No data should be persisted
    for i in (0..2000).step_by(100) {
        let key = format!("abort_key_{:06}", i);
        let value = engine.get(&cf, key.as_bytes()).expect("get");
        assert!(
            value.is_none(),
            "Key {} should not exist after rollback",
            key
        );
    }
}

#[test]
fn should_handle_very_large_transaction_given_many_writes() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    // Create transaction with very small memory limit (128KB) to force multiple spills
    let mut huge_txn = engine
        .begin_transaction_with_options(&cf, None, 128 * 1024, cntryl_midge::IsolationLevel::default())
        .expect("begin");

    // Act - Add 10MB of data (will cause multiple spills)
    for i in 0..10000 {
        huge_txn
            .put(format!("huge_key_{:06}", i).as_bytes(), &vec![0xEEu8; 1024])
            .expect("put");
    }

    engine
        .commit_transaction(huge_txn, cntryl_midge::WriteOptions::default())
        .expect("commit should succeed");

    // Assert - Verify data integrity with sampling (checking every 100th key for performance)
    for i in (0..10000).step_by(100) {
        let key = format!("huge_key_{:06}", i);
        let value = engine.get(&cf, key.as_bytes()).expect("get");
        assert!(
            value.is_some(),
            "Key {} should exist after large transaction",
            key
        );
    }
}
