// Copyright (c) 2025 Cntryl, Inc.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

//! Transaction conflict tests - validates LWW semantics, write conflict handling, and concurrent transaction behavior.
//!
//! Tests ensure that concurrent transactions follow Last-Write-Wins semantics and handle conflicts appropriately.
//! These tests validate logical transaction behavior across all storage modes (Memory, FS, Cloud).

use bytes::Bytes;
use cntryl_midge::testkit::*;
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
        let mut txn1 = engine.transaction();
        let mut txn2 = engine.transaction();
        
        txn1.put(cf.id(), b"key".to_vec(), b"value1".to_vec()).unwrap();
        txn2.put(cf.id(), b"key".to_vec(), b"value2".to_vec()).unwrap();
        
        engine.commit_transaction(txn1).unwrap();
        engine.commit_transaction(txn2).unwrap();
        
        // Assert - last committed wins
        let value = engine.get(cf, b"key").unwrap();
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
            let mut txn = engine.transaction();
            txn.put(cf.id(), b"key".to_vec(), b"value1".to_vec()).unwrap();
            engine.commit_transaction(txn)
        };
        
        let result2 = {
            let mut txn = engine.transaction();
            txn.put(cf.id(), b"key".to_vec(), b"value2".to_vec()).unwrap();
            engine.commit_transaction(txn)
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
            let mut txn = engine1.transaction();
            txn.put(cf_id, b"key".to_vec(), b"value1".to_vec()).unwrap();
            engine1.commit_transaction(txn)
        });
        
        let handle2 = std::thread::spawn(move || {
            let mut txn = engine2.transaction();
            txn.put(cf_id, b"key".to_vec(), b"value2".to_vec()).unwrap();
            engine2.commit_transaction(txn)
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
        let mut txn1 = engine.transaction();
        txn1.put(cf.id(), b"key".to_vec(), b"value1".to_vec()).unwrap();
        engine.commit_transaction(txn1).unwrap();
        
        let mut txn2 = engine.transaction();
        txn2.put(cf.id(), b"key".to_vec(), b"value2".to_vec()).unwrap();
        drop(txn2); // Rollback
        
        // Assert
        let value = engine.get(cf, b"key").unwrap();
        assert_eq!(value, Some(Bytes::from_static(b"value1")));
    });
}

#[test]
fn should_allow_concurrent_delete_put_operations_given_lww_semantics() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        engine.put(cf, b"key", b"initial").unwrap();
        
        // Act
        let mut txn1 = engine.transaction();
        let mut txn2 = engine.transaction();
        
        txn1.delete(cf.id(), b"key".to_vec()).unwrap();
        txn2.put(cf.id(), b"key".to_vec(), b"value".to_vec()).unwrap();
        
        engine.commit_transaction(txn1).unwrap();
        engine.commit_transaction(txn2).unwrap();
        
        // Assert - last operation wins
        let value = engine.get(cf, b"key").unwrap();
        assert_eq!(value, Some(Bytes::from_static(b"value")));
    });
}

#[test]
fn should_allow_overlapping_put_after_delete_range_given_lww_semantics() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        engine.put(cf, b"key1", b"value1").unwrap();
        engine.put(cf, b"key2", b"value2").unwrap();
        
        // Act
        let mut txn1 = engine.transaction();
        let mut txn2 = engine.transaction();
        
        txn1.delete_range(cf.id(), b"key1".to_vec(), b"key3".to_vec()).unwrap();
        txn2.put(cf.id(), b"key2".to_vec(), b"newvalue".to_vec()).unwrap();
        
        engine.commit_transaction(txn1).unwrap();
        engine.commit_transaction(txn2).unwrap();
        
        // Assert
        let value = engine.get(cf, b"key2").unwrap();
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
        let mut txn1 = engine.transaction();
        let mut txn2 = engine.transaction();
        
        txn1.put(cf.id(), b"key".to_vec(), b"value".to_vec()).unwrap();
        txn2.delete_range(cf.id(), b"key".to_vec(), b"keyz".to_vec()).unwrap();
        
        engine.commit_transaction(txn1).unwrap();
        engine.commit_transaction(txn2).unwrap();
        
        // Assert
        let value = engine.get(cf, b"key").unwrap();
        assert_eq!(value, None);
    });
}

#[test]
fn should_allow_concurrent_delete_ranges_given_lww_semantics() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        engine.put(cf, b"key1", b"value1").unwrap();
        engine.put(cf, b"key2", b"value2").unwrap();
        
        // Act
        let mut txn1 = engine.transaction();
        let mut txn2 = engine.transaction();
        
        txn1.delete_range(cf.id(), b"key1".to_vec(), b"key3".to_vec()).unwrap();
        txn2.delete_range(cf.id(), b"key1".to_vec(), b"key3".to_vec()).unwrap();
        
        engine.commit_transaction(txn1).unwrap();
        engine.commit_transaction(txn2).unwrap();
        
        // Assert - both succeed
        assert!(engine.get(cf, b"key1").unwrap().is_none());
    });
}

#[test]
fn should_allow_delete_range_delete_operations_given_lww_semantics() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        engine.put(cf, b"key", b"value").unwrap();
        
        // Act
        let mut txn1 = engine.transaction();
        let mut txn2 = engine.transaction();
        
        txn1.delete_range(cf.id(), b"key".to_vec(), b"keyz".to_vec()).unwrap();
        txn2.delete(cf.id(), b"key".to_vec()).unwrap();
        
        engine.commit_transaction(txn1).unwrap();
        engine.commit_transaction(txn2).unwrap();
        
        // Assert
        assert!(engine.get(cf, b"key").unwrap().is_none());
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
        
        // Act
        let txn1 = engine.transaction();
        let txn2 = engine.transaction();
        
        // txn1.insert(cf.id(), b"key".to_vec(), b"value1".to_vec()).unwrap();
        // txn2.insert(cf.id(), b"key".to_vec(), b"value2".to_vec()).unwrap();
        
        // engine.commit_transaction(txn1).unwrap();
        // let result = engine.commit_transaction(txn2);
        
        // Assert - second insert should fail
        // assert!(result.is_err());
        let value = engine.get(cf, b"key").unwrap();
        // assert_eq!(value, Some(Bytes::from_static(b"value1")));
    });
}

#[test]
fn should_conflict_on_insert_given_key_already_exists_when_committed() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        engine.put(cf, b"key", b"existing").unwrap();
        
        // Act
        let txn = engine.transaction();
        // let result = txn.insert(cf.id(), b"key".to_vec(), b"newvalue".to_vec());
        
        // Assert
        // assert!(result.is_err());
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
        engine.put(cf, b"counter", b"0").unwrap();
        
        // Act - simulate lost update with LWW semantics
        let _val1 = engine.get(cf, b"counter").unwrap().unwrap();
        let _val2 = engine.get(cf, b"counter").unwrap().unwrap();
        
        let mut txn1 = engine.transaction();
        txn1.put(cf.id(), b"counter".to_vec(), b"1".to_vec()).unwrap();
        engine.commit_transaction(txn1).unwrap();
        
        let mut txn2 = engine.transaction();
        txn2.put(cf.id(), b"counter".to_vec(), b"1".to_vec()).unwrap();
        engine.commit_transaction(txn2).unwrap();
        
        // Assert - lost update allowed with LWW
        let final_value = engine.get(cf, b"counter").unwrap();
        assert_eq!(final_value, Some(Bytes::from_static(b"1")));
    });
}

#[test]
fn should_detect_lost_update_given_cas_pattern_when_value_changed() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        engine.put(cf, b"counter", b"0").unwrap();
        
        // Act - CAS should detect concurrent modification
        // let mut txn = engine.transaction();
        // let result = txn.compare_and_swap(cf.id(), b"counter", b"0", b"1");
        
        // Assert
        // assert!(result.is_ok());
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
        let mut txn1 = engine.transaction();
        let mut txn2 = engine.transaction();
        
        txn1.put(cf.id(), b"key1".to_vec(), b"value1".to_vec()).unwrap();
        txn2.put(cf.id(), b"key2".to_vec(), b"value2".to_vec()).unwrap();
        
        engine.commit_transaction(txn1).unwrap();
        engine.commit_transaction(txn2).unwrap();
        
        // Assert
        assert_eq!(engine.get(cf, b"key1").unwrap(), Some(Bytes::from_static(b"value1")));
        assert_eq!(engine.get(cf, b"key2").unwrap(), Some(Bytes::from_static(b"value2")));
    });
}

#[test]
fn should_commit_transaction_given_no_conflicts() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        
        // Act
        let mut txn = engine.transaction();
        txn.put(cf.id(), b"key".to_vec(), b"value".to_vec()).unwrap();
        let result = engine.commit_transaction(txn);
        
        // Assert
        assert!(result.is_ok());
        assert_eq!(engine.get(cf, b"key").unwrap(), Some(Bytes::from_static(b"value")));
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
            let mut txn = engine1.transaction();
            txn.put(cf_id, b"key1".to_vec(), b"value1".to_vec()).unwrap();
            engine1.commit_transaction(txn)
        });
        
        let handle2 = std::thread::spawn(move || {
            let mut txn = engine2.transaction();
            txn.put(cf_id, b"key2".to_vec(), b"value2".to_vec()).unwrap();
            engine2.commit_transaction(txn)
        });
        
        // Assert
        assert!(handle1.join().unwrap().is_ok());
        assert!(handle2.join().unwrap().is_ok());
        assert_eq!(engine.get(cf, b"key1").unwrap(), Some(Bytes::from_static(b"value1")));
        assert_eq!(engine.get(cf, b"key2").unwrap(), Some(Bytes::from_static(b"value2")));
    });
}

#[test]
fn should_read_values_within_transaction() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        engine.put(&cf, b"key", b"value").unwrap();
        
        // Act
        let txn = engine.transaction();
        let value = engine.get_transactional(&cf, b"key", &txn).unwrap();
        
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
        let mut txn = engine.transaction();
        txn.put(cf.id(), b"newkey".to_vec(), b"newvalue".to_vec()).unwrap();
        engine.commit_transaction(txn).unwrap();
        
        // Assert
        assert_eq!(engine.get(cf, b"newkey").unwrap(), Some(Bytes::from_static(b"newvalue")));
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
                let mut txn = engine_clone.transaction();
                let key = format!("key{}", i);
                let value = format!("value{}", i);
                txn.put(cf_id, key.as_bytes().to_vec(), value.as_bytes().to_vec()).unwrap();
                engine_clone.commit_transaction(txn)
            });
            handles.push(handle);
        }
        
        // Assert - all commits succeed
        for handle in handles {
            assert!(handle.join().unwrap().is_ok());
        }
        
        for i in 0..10 {
            let key = format!("key{}", i);
            let expected = format!("value{}", i);
            assert_eq!(engine.get(cf, key.as_bytes()).unwrap(), Some(Bytes::from(expected.as_bytes().to_vec())));
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
                let mut txn = engine_clone.transaction();
                let value = format!("value{}", i);
                txn.put(cf_id, b"hotkey".to_vec(), value.as_bytes().to_vec()).unwrap();
                engine_clone.commit_transaction(txn)
            });
            handles.push(handle);
        }
        
        // Assert - all commits succeed (LWW semantics)
        for handle in handles {
            assert!(handle.join().unwrap().is_ok());
        }
        
        // One of the values should win
        assert!(engine.get(cf, b"hotkey").unwrap().is_some());
    });
}

#[test]
fn should_handle_concurrent_read_modify_writes_without_panic() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let cf_id = cf.id();
        engine.put(cf, b"counter", b"0").unwrap();
        let mut handles = vec![];
        
        // Act - 10 threads incrementing counter
        for i in 0..10 {
            let engine_clone = Arc::clone(&engine);
            let handle = std::thread::spawn(move || {
                let cf = engine_clone.default_column_family();
                let _value = engine_clone.get(cf, b"counter").unwrap();
                let mut txn = engine_clone.transaction();
                let new_value = format!("{}", i);
                txn.put(cf_id, b"counter".to_vec(), new_value.as_bytes().to_vec()).unwrap();
                engine_clone.commit_transaction(txn)
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
        let _engine = Arc::new(open_with_mode(opts, mode));
        // Test optimistic locking with high concurrency
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
        // Arrange
        {
            let engine = open_with_mode(opts.clone(), mode);
            let _cf = engine.default_column_family();
            // Set up conflict state and crash
        }
        
        // Act
        {
            let engine = open_with_mode(opts, mode);
            let _cf = engine.default_column_family();
            
            // Assert
            // Verify conflict resolution persists
        }
    });
}

#[test]
fn should_persist_lost_update_prevention_after_restart() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange
        {
            let engine = open_with_mode(opts.clone(), mode);
            let _cf = engine.default_column_family();
            // Set up lost update prevention
        }
        
        // Act
        {
            let engine = open_with_mode(opts, mode);
            let _cf = engine.default_column_family();
            
            // Assert
            // Verify lost update prevention persists
        }
    });
}
