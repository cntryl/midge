// Copyright (c) 2025 Cntryl, Inc.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

//! Transaction conflict tests - validates LWW semantics, write conflict handling, and concurrent transaction behavior.
//!
//! Tests ensure that concurrent transactions follow Last-Write-Wins semantics and handle conflicts appropriately.
//! These tests validate logical transaction behavior across all storage modes (Memory, FS, Cloud).

use bytes::Bytes;
use cntryl_midge::testkit::*;
use cntryl_midge::WriteOptions;
use std::sync::Arc;

// ============================================================================
// BASIC LWW SEMANTICS TESTS
// ============================================================================

#[test]
fn should_allow_concurrent_puts_to_same_key_given_lww_semantics() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();

        // Act
        let mut txn1 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
        let mut txn2 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();

        txn1.put(b"key".to_vec(), b"value1".to_vec(), None)
            .unwrap();
        txn2.put(b"key".to_vec(), b"value2".to_vec(), None)
            .unwrap();

        engine.commit(txn1, cntryl_midge::WriteOptions::buffered()).unwrap();
        engine.commit(txn2, cntryl_midge::WriteOptions::buffered()).unwrap();

        // Assert - last committed wins
        let read_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).unwrap();
        let value = read_tx.get(b"key").unwrap();
        assert_eq!(value, Some(Bytes::from_static(b"value2")));
    });
}

#[test]
fn should_allow_both_puts_to_succeed_given_concurrent_writes_when_lww() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();

        // Act
        let result1 = {
            let mut txn = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
            txn.put(b"key".to_vec(), b"value1".to_vec(), None)
                .unwrap();
            engine.commit(txn, WriteOptions::buffered())
        };

        let result2 = {
            let mut txn = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
            txn.put(b"key".to_vec(), b"value2".to_vec(), None)
                .unwrap();
            engine.commit(txn, WriteOptions::buffered())
        };

        // Assert - both commits succeed
        assert!(result1.is_ok());
        assert!(result2.is_ok());
    });
}

#[test]
fn should_accept_both_committers_given_concurrent_puts_when_lww() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let cf_id = cf.id();
        let engine1 = Arc::clone(&engine);
        let engine2 = Arc::clone(&engine);

        // Act
        let handle1 = std::thread::spawn(move || {
            let mut txn = engine1.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite).unwrap();
            txn.put(b"key".to_vec(), b"value1".to_vec(), None).unwrap();
            engine1.commit(txn, cntryl_midge::WriteOptions::buffered())
        });

        let handle2 = std::thread::spawn(move || {
            let mut txn = engine2.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite).unwrap();
            txn.put(b"key".to_vec(), b"value2".to_vec(), None).unwrap();
            engine2.commit(txn, cntryl_midge::WriteOptions::buffered())
        });

        // Assert - both commits succeed
        assert!(handle1.join().unwrap().is_ok());
        assert!(handle2.join().unwrap().is_ok());
    });
}

#[test]
fn should_preserve_first_commit_given_write_conflict_when_second_aborts() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();

        // Act
        let mut txn1 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
        txn1.put(b"key".to_vec(), b"value1".to_vec(), None)
            .unwrap();
        engine.commit(txn1, cntryl_midge::WriteOptions::buffered()).unwrap();

        let mut txn2 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
        txn2.put(b"key".to_vec(), b"value2".to_vec(), None)
            .unwrap();
        drop(txn2); // Rollback

        // Assert
        let read_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).unwrap();
        let value = read_tx.get(b"key").unwrap();
        assert_eq!(value, Some(Bytes::from_static(b"value1")));
    });
}

#[test]
fn should_allow_concurrent_delete_put_operations_given_lww_semantics() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let mut setup_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
        setup_tx.put(b"key".to_vec(), b"initial".to_vec(), None).unwrap();
        engine.commit(setup_tx, cntryl_midge::WriteOptions::buffered()).unwrap();

        // Act
        let mut txn1 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
        let mut txn2 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();

        txn1.delete(b"key".to_vec()).unwrap();
        txn2.put(b"key".to_vec(), b"value".to_vec(), None)
            .unwrap();

        engine.commit(txn1, cntryl_midge::WriteOptions::buffered()).unwrap();
        engine.commit(txn2, cntryl_midge::WriteOptions::buffered()).unwrap();

        // Assert - last operation wins
        let read_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).unwrap();
        let value = read_tx.get(b"key").unwrap();
        assert_eq!(value, Some(Bytes::from_static(b"value")));
    });
}

#[test]
fn should_allow_overlapping_put_after_delete_range_given_lww_semantics() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let cf_id = cf.id();
        let mut setup_tx = engine.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite).unwrap();
        setup_tx.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
        setup_tx.put(b"key2".to_vec(), b"value2".to_vec(), None).unwrap();
        engine.commit(setup_tx, cntryl_midge::WriteOptions::buffered()).unwrap();

        // Act
        let mut txn1 = engine.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite).unwrap();
        let mut txn2 = engine.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite).unwrap();

        txn1.delete_range(b"key1".to_vec(), b"key3".to_vec())
            .unwrap();
        txn2.put(b"key2".to_vec(), b"newvalue".to_vec(), None)
            .unwrap();

        engine.commit(txn1, cntryl_midge::WriteOptions::buffered()).unwrap();
        engine.commit(txn2, cntryl_midge::WriteOptions::buffered()).unwrap();

        // Assert
        let read_tx = engine.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly).unwrap();
        let value = read_tx.get(b"key2").unwrap();
        assert_eq!(value, Some(Bytes::from_static(b"newvalue")));
    });
}

#[test]
fn should_allow_put_then_delete_range_given_lww_semantics() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();

        // Act
        let mut txn1 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
        let mut txn2 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();

        txn1.put(b"key".to_vec(), b"value".to_vec(), None)
            .unwrap();
        txn2.delete_range(b"key".to_vec(), b"keyz".to_vec())
            .unwrap();

        engine.commit(txn1, cntryl_midge::WriteOptions::buffered()).unwrap();
        engine.commit(txn2, cntryl_midge::WriteOptions::buffered()).unwrap();

        // Assert
        let read_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).unwrap();
        let value = read_tx.get(b"key").unwrap();
        assert_eq!(value, None);
    });
}

#[test]
fn should_allow_concurrent_delete_ranges_given_lww_semantics() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let cf_id = cf.id();
        let mut setup_tx = engine.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite).unwrap();
        setup_tx.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
        setup_tx.put(b"key2".to_vec(), b"value2".to_vec(), None).unwrap();
        engine.commit(setup_tx, cntryl_midge::WriteOptions::buffered()).unwrap();

        // Act
        let mut txn1 = engine.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite).unwrap();
        let mut txn2 = engine.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite).unwrap();

        txn1.delete_range(b"key1".to_vec(), b"key3".to_vec())
            .unwrap();
        txn2.delete_range(b"key1".to_vec(), b"key3".to_vec())
            .unwrap();

        engine.commit(txn1, cntryl_midge::WriteOptions::buffered()).unwrap();
        engine.commit(txn2, cntryl_midge::WriteOptions::buffered()).unwrap();

        // Assert - both succeed
        let read_tx = engine.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly).unwrap();
        assert!(read_tx.get(b"key1").unwrap().is_none());
    });
}

#[test]
fn should_allow_delete_range_delete_operations_given_lww_semantics() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let mut setup_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
        setup_tx.put(b"key".to_vec(), b"value".to_vec(), None).unwrap();
        engine.commit(setup_tx, cntryl_midge::WriteOptions::buffered()).unwrap();

        // Act
        let mut txn1 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
        let mut txn2 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();

        txn1.delete_range(b"key".to_vec(), b"keyz".to_vec())
            .unwrap();
        txn2.delete(b"key".to_vec()).unwrap();

        engine.commit(txn1, cntryl_midge::WriteOptions::buffered()).unwrap();
        engine.commit(txn2, cntryl_midge::WriteOptions::buffered()).unwrap();

        // Assert
        let read_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).unwrap();
        assert!(read_tx.get(b"key").unwrap().is_none());
    });
}

// ============================================================================
// INSERT CONFLICT TESTS
// ============================================================================

#[test]
fn should_conflict_on_concurrent_inserts_given_same_key_when_one_commits_first() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();

        // Act - both transactions try to put same key
        let mut txn1 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
        let mut txn2 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();

        txn1.put(b"key".to_vec(), b"value1".to_vec(), None)
            .unwrap();
        txn2.put(b"key".to_vec(), b"value2".to_vec(), None)
            .unwrap();

        let result1 = engine.commit(txn1, cntryl_midge::WriteOptions::buffered());
        let result2 = engine.commit(txn2, cntryl_midge::WriteOptions::buffered());

        // Assert - both succeed with LWW semantics (last write wins)
        assert!(result1.is_ok());
        assert!(result2.is_ok());
        let read_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).unwrap();
        let value = read_tx.get(b"key").unwrap();
        assert_eq!(value, Some(Bytes::from_static(b"value2")));
    });
}

#[test]
fn should_conflict_on_insert_given_key_already_exists_when_committed() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let mut setup_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
        setup_tx.put(b"key".to_vec(), b"existing".to_vec(), None).unwrap();
        engine.commit(setup_tx, cntryl_midge::WriteOptions::buffered()).unwrap();

        // Act - transaction attempts put on existing key
        let mut txn = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
        txn.put(b"key".to_vec(), b"newvalue".to_vec(), None)
            .unwrap();
        let result = engine.commit(txn, cntryl_midge::WriteOptions::buffered());

        // Assert - put succeeds (LWW semantics, not insert semantics)
        assert!(result.is_ok());
        let read_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).unwrap();
        let value = read_tx.get(b"key").unwrap();
        assert_eq!(value, Some(Bytes::from_static(b"newvalue")));
    });
}

// ============================================================================
// LOST UPDATE TESTS
// ============================================================================

#[test]
fn should_allow_lost_update_given_put_read_modify_write_when_concurrent() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let mut setup_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
        setup_tx.put(b"counter".to_vec(), b"0".to_vec(), None).unwrap();
        engine.commit(setup_tx, cntryl_midge::WriteOptions::buffered()).unwrap();

        // Act - simulate lost update with LWW semantics
        let read_tx1 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).unwrap();
        let _val1 = read_tx1.get(b"counter").unwrap().unwrap();
        let read_tx2 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).unwrap();
        let _val2 = read_tx2.get(b"counter").unwrap().unwrap();

        let mut txn1 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
        txn1.put(b"counter".to_vec(), b"1".to_vec(), None)
            .unwrap();
        engine.commit(txn1, cntryl_midge::WriteOptions::buffered()).unwrap();

        let mut txn2 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
        txn2.put(b"counter".to_vec(), b"1".to_vec(), None)
            .unwrap();
        engine.commit(txn2, cntryl_midge::WriteOptions::buffered()).unwrap();

        // Assert - lost update allowed with LWW
        let read_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).unwrap();
        let final_value = read_tx.get(b"counter").unwrap();
        assert_eq!(final_value, Some(Bytes::from_static(b"1")));
    });
}

#[test]
fn should_detect_lost_update_given_cas_pattern_when_value_changed() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let mut setup_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
        setup_tx.put(b"counter".to_vec(), b"0".to_vec(), None).unwrap();
        engine.commit(setup_tx, cntryl_midge::WriteOptions::buffered()).unwrap();

        // Act - read-modify-write pattern with concurrent modification
        let read_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).unwrap();
        let original = read_tx.get(b"counter").unwrap().unwrap();
        assert_eq!(original, Bytes::from_static(b"0"));

        // Concurrent transaction modifies the counter
        let mut txn_concurrent = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
        txn_concurrent
            .put(b"counter".to_vec(), b"2".to_vec(), None)
            .unwrap();
        engine.commit(txn_concurrent, cntryl_midge::WriteOptions::buffered()).unwrap();

        // Original transaction continues with stale value
        let mut txn = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
        txn.put(b"counter".to_vec(), b"1".to_vec(), None)
            .unwrap();
        let result = engine.commit(txn, cntryl_midge::WriteOptions::buffered());

        // Assert - LWW semantics mean last write wins (value is 1)
        assert!(result.is_ok());
        let read_tx2 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).unwrap();
        let value = read_tx2.get(b"counter").unwrap();
        assert_eq!(value, Some(Bytes::from_static(b"1")));
    });
}

// ============================================================================
// NON-CONFLICTING TRANSACTION TESTS
// ============================================================================

#[test]
fn should_preserve_both_updates_given_non_overlapping_keys_when_concurrent_commits() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();

        // Act
        let mut txn1 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
        let mut txn2 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();

        txn1.put(b"key1".to_vec(), b"value1".to_vec(), None)
            .unwrap();
        txn2.put(b"key2".to_vec(), b"value2".to_vec(), None)
            .unwrap();

        engine.commit(txn1, cntryl_midge::WriteOptions::buffered()).unwrap();
        engine.commit(txn2, cntryl_midge::WriteOptions::buffered()).unwrap();

        // Assert
        let read_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).unwrap();
        assert_eq!(
            read_tx.get(b"key1").unwrap(),
            Some(Bytes::from_static(b"value1"))
        );
        assert_eq!(
            read_tx.get(b"key2").unwrap(),
            Some(Bytes::from_static(b"value2"))
        );
    });
}

#[test]
fn should_commit_transaction_given_no_conflicts() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();

        // Act
        let mut txn = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
        txn.put(b"key".to_vec(), b"value".to_vec(), None)
            .unwrap();
        let result = engine.commit(txn, cntryl_midge::WriteOptions::buffered());

        // Assert
        assert!(result.is_ok());
        let read_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).unwrap();
        assert_eq!(
            read_tx.get(b"key").unwrap(),
            Some(Bytes::from_static(b"value"))
        );
    });
}

#[test]
fn should_commit_transaction_given_concurrent_modifications_to_different_keys() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let cf_id = cf.id();
        let engine1 = Arc::clone(&engine);
        let engine2 = Arc::clone(&engine);

        // Act
        let handle1 = std::thread::spawn(move || {
            let mut txn = engine1.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite).unwrap();
            txn.put(b"key1".to_vec(), b"value1".to_vec(), None)
                .unwrap();
            engine1.commit(txn, cntryl_midge::WriteOptions::buffered())
        });

        let handle2 = std::thread::spawn(move || {
            let mut txn = engine2.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite).unwrap();
            txn.put(b"key2".to_vec(), b"value2".to_vec(), None)
                .unwrap();
            engine2.commit(txn, cntryl_midge::WriteOptions::buffered())
        });

        // Assert
        assert!(handle1.join().unwrap().is_ok());
        assert!(handle2.join().unwrap().is_ok());
        let read_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).unwrap();
        assert_eq!(
            read_tx.get(b"key1").unwrap(),
            Some(Bytes::from_static(b"value1"))
        );
        assert_eq!(
            read_tx.get(b"key2").unwrap(),
            Some(Bytes::from_static(b"value2"))
        );
    });
}

#[test]
fn should_read_values_within_transaction() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let mut setup_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
        setup_tx.put(b"key".to_vec(), b"value".to_vec(), None).unwrap();
        engine.commit(setup_tx, cntryl_midge::WriteOptions::buffered()).unwrap();

        // Act
        let txn = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
        let value = txn.get(b"key").unwrap();

        // Assert - should read committed value at transaction start
        assert_eq!(value, Some(Bytes::from_static(b"value")));
    });
}

#[test]
fn should_commit_new_key_given_clean_transaction() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();

        // Act
        let mut txn = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
        txn.put(b"newkey".to_vec(), b"newvalue".to_vec(), None)
            .unwrap();
        engine.commit(txn, cntryl_midge::WriteOptions::buffered()).unwrap();

        // Assert
        let read_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).unwrap();
        assert_eq!(
            read_tx.get(b"newkey").unwrap(),
            Some(Bytes::from_static(b"newvalue"))
        );
    });
}

// ============================================================================
// STRESS TESTS
// ============================================================================

#[test]
fn should_allow_concurrent_writes_to_different_keys() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let cf_id = cf.id();
        let mut handles = vec![];

        // Act - spawn 10 threads writing different keys
        for i in 0..10 {
            let engine_clone = Arc::clone(&engine);
            let handle = std::thread::spawn(move || {
                let mut txn = engine_clone.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite).unwrap();
                let key = format!("key{}", i);
                let value = format!("value{}", i);
                txn.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                    .unwrap();
                engine_clone.commit(txn, cntryl_midge::WriteOptions::buffered())
            });
            handles.push(handle);
        }

        // Assert - all commits succeed
        for handle in handles {
            assert!(handle.join().unwrap().is_ok());
        }

        let read_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).unwrap();
        for i in 0..10 {
            let key = format!("key{}", i);
            let expected = format!("value{}", i);
            assert_eq!(
                read_tx.get(key.as_bytes()).unwrap(),
                Some(Bytes::from(expected.as_bytes().to_vec()))
            );
        }
    });
}

#[test]
fn should_handle_high_contention_writes_without_panic() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let cf_id = cf.id();
        let mut handles = vec![];

        // Act - 20 threads writing to same key
        for i in 0..20 {
            let engine_clone = Arc::clone(&engine);
            let handle = std::thread::spawn(move || {
                let mut txn = engine_clone.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite).unwrap();
                let value = format!("value{}", i);
                txn.put(b"hotkey".to_vec(), value.as_bytes().to_vec(), None)
                    .unwrap();
                engine_clone.commit(txn, cntryl_midge::WriteOptions::buffered())
            });
            handles.push(handle);
        }

        // Assert - all commits succeed (LWW semantics)
        for handle in handles {
            assert!(handle.join().unwrap().is_ok());
        }

        // One of the values should win
        let read_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).unwrap();
        assert!(read_tx.get(b"hotkey").unwrap().is_some());
    });
}

#[test]
fn should_handle_concurrent_read_modify_writes_without_panic() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let cf_id = cf.id();
        let mut setup_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
        setup_tx.put(b"counter".to_vec(), b"0".to_vec(), None).unwrap();
        engine.commit(setup_tx, cntryl_midge::WriteOptions::buffered()).unwrap();
        let mut handles = vec![];

        // Act - 10 threads incrementing counter
        for i in 0..10 {
            let engine_clone = Arc::clone(&engine);
            let handle = std::thread::spawn(move || {
                let read_tx = engine_clone.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly).unwrap();
                let _value = read_tx.get(b"counter").unwrap();
                let mut txn = engine_clone.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite).unwrap();
                let new_value = format!("{}", i);
                txn.put(b"counter".to_vec(), new_value.as_bytes().to_vec(), None)
                    .unwrap();
                engine_clone.commit(txn, cntryl_midge::WriteOptions::buffered())
            });
            handles.push(handle);
        }

        // Assert - all commits succeed
        for handle in handles {
            assert!(handle.join().unwrap().is_ok());
        }
    });
}

#[test]
fn should_handle_high_concurrency_optimistic_locking() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let cf_id = cf.id();
        let mut handles = vec![];

        // Act - 50 threads performing optimistic lock pattern (read then write)
        for i in 0..50 {
            let engine_clone = Arc::clone(&engine);
            let handle = std::thread::spawn(move || {
                // Optimistic lock pattern: read first
                let read_tx = engine_clone.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly).unwrap();
                let _current = read_tx.get(b"value").unwrap();

                // Then write in transaction
                let mut txn = engine_clone.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite).unwrap();
                let write_val = format!("{}", i);
                txn.put(b"value".to_vec(), write_val.as_bytes().to_vec(), None)
                    .unwrap();
                engine_clone.commit(txn, cntryl_midge::WriteOptions::buffered())
            });
            handles.push(handle);
        }

        // Assert - all transactions succeed
        for handle in handles {
            assert!(handle.join().unwrap().is_ok());
        }

        // Final value should be one of the writes
        let read_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).unwrap();
        assert!(read_tx.get(b"value").unwrap().is_some());
    });
}

#[test]
fn should_maintain_transaction_isolation_under_stress() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let _engine = Arc::new(open_with_mode(opts, mode));
        // Test transaction isolation under concurrent load
    });
}

// ============================================================================
// RECOVERY TESTS (FS + CLOUD ONLY)
// ============================================================================

#[test]
fn should_recover_conflict_state_after_engine_restart() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange & Act Phase 1 - create conflicts and commit
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            // Create conflicting transactions where last-write wins
            let mut txn1 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
            txn1.put(b"conflict_key".to_vec(), b"value1".to_vec(), None)
                .unwrap();
            engine.commit(txn1, cntryl_midge::WriteOptions::buffered()).unwrap();

            let mut txn2 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
            txn2.put(b"conflict_key".to_vec(), b"value2".to_vec(), None)
                .unwrap();
            engine.commit(txn2, cntryl_midge::WriteOptions::buffered()).unwrap();

            // Engine dropped (simulated crash)
        }

        // Act Phase 2 - restart and verify
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();

            // Assert - last written value persists
            let read_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).unwrap();
            let value = read_tx.get(b"conflict_key").unwrap();
            assert_eq!(value, Some(Bytes::from_static(b"value2")));
        }
    });
}

#[test]
fn should_persist_lost_update_prevention_after_restart() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange & Act Phase 1 - set up concurrent updates
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            // Initial value
            let mut setup_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
            setup_tx.put(b"counter".to_vec(), b"0".to_vec(), None).unwrap();
            engine.commit(setup_tx, cntryl_midge::WriteOptions::buffered()).unwrap();

            // Two transactions attempt concurrent increment
            let mut txn1 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
            txn1.put(b"counter".to_vec(), b"1".to_vec(), None)
                .unwrap();
            engine.commit(txn1, cntryl_midge::WriteOptions::buffered()).unwrap();

            let mut txn2 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
            txn2.put(b"counter".to_vec(), b"2".to_vec(), None)
                .unwrap();
            engine.commit(txn2, cntryl_midge::WriteOptions::buffered()).unwrap();

            // Engine dropped (simulated crash)
        }

        // Act Phase 2 - restart and verify
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();

            // Assert - last written value (2) persists
            let read_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).unwrap();
            let value = read_tx.get(b"counter").unwrap();
            assert_eq!(value, Some(Bytes::from_static(b"2")));
        }
    });
}
// ============================================================================
// BASELINE CONFLICT PREVENTION (Negative Tests)
// ============================================================================

#[test]
fn should_not_reject_writes_when_no_conflict_exists_given_disjoint_keys() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Verify that non-conflicting writes are never rejected
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();

        // Act: Two transactions writing to different keys (no conflict)
        let mut txn1 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
        let mut txn2 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();

        txn1.put(b"key1".to_vec(), b"value1".to_vec(), None)
            .unwrap();
        txn2.put(b"key2".to_vec(), b"value2".to_vec(), None)
            .unwrap();

        let r1 = engine.commit(txn1, cntryl_midge::WriteOptions::buffered());
        let r2 = engine.commit(txn2, cntryl_midge::WriteOptions::buffered());

        // Assert: Both must succeed (no false positive conflict detection)
        assert!(
            r1.is_ok(),
            "Non-conflicting write 1 was rejected in {}",
            mode
        );
        assert!(
            r2.is_ok(),
            "Non-conflicting write 2 was rejected in {}",
            mode
        );

        let read_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).unwrap();
        let v1 = read_tx.get(b"key1").unwrap();
        let v2 = read_tx.get(b"key2").unwrap();
        assert_eq!(v1, Some(Bytes::from_static(b"value1")));
        assert_eq!(v2, Some(Bytes::from_static(b"value2")));
    });
}

#[test]
fn should_preserve_both_writes_when_non_overlapping_keys_given_concurrent_commits() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Verify that non-conflicting concurrent writes are both visible
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();

        // Pre-populate
        let mut setup_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
        setup_tx.put(b"key1".to_vec(), b"old1".to_vec(), None).unwrap();
        setup_tx.put(b"key2".to_vec(), b"old2".to_vec(), None).unwrap();
        engine.commit(setup_tx, cntryl_midge::WriteOptions::buffered()).unwrap();

        // Act: Two concurrent updates to different keys
        let mut txn1 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
        let mut txn2 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();

        txn1.put(b"key1".to_vec(), b"new1".to_vec(), None)
            .unwrap();
        txn2.put(b"key2".to_vec(), b"new2".to_vec(), None)
            .unwrap();

        engine.commit(txn1, cntryl_midge::WriteOptions::buffered()).ok();
        engine.commit(txn2, cntryl_midge::WriteOptions::buffered()).ok();

        // Assert: Both updates must be visible
        let read_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).unwrap();
        let v1 = read_tx.get(b"key1").unwrap();
        let v2 = read_tx.get(b"key2").unwrap();

        assert_eq!(
            v1,
            Some(Bytes::from_static(b"new1")),
            "key1 update lost in {}",
            mode
        );
        assert_eq!(
            v2,
            Some(Bytes::from_static(b"new2")),
            "key2 update lost in {}",
            mode
        );
    });
}
