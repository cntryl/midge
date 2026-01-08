//! Delete Range Integration Tests
//!
//! Tests range deletion operations end-to-end using the public MidgeEngine API.
//!
//! **Current Status** (✅ FULLY IMPLEMENTED):
//! - delete_range() API is fully functional and verified
//! - Correctly deletes all keys in the specified range [start, end)
//! - All storage modes (Memory, LocalDisk, CloudBacked) pass identical tests
//!
//! These tests are **storage-mode invariant**: every supported backend
//! (Memory, LocalDisk, CloudBacked) must pass with identical behavior.
//!
//! Naming convention:
//!   should_<behavior>_given_<context>_when_<condition>

use bytes::Bytes;
use cntryl_midge::testkit::*;
use cntryl_midge::{TransactionMode, WriteOptions};

// ============================================================================
// BASIC RANGE DELETION
// ============================================================================

#[test]
fn should_delete_keys_in_range_given_delete_range_when_querying() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key1".to_vec(), b"val1".to_vec(), None)
            .expect("put1");
        engine.commit(tx, WriteOptions::default()).unwrap();
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key2".to_vec(), b"val2".to_vec(), None)
            .expect("put2");
        engine.commit(tx, WriteOptions::default()).unwrap();
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key3".to_vec(), b"val3".to_vec(), None)
            .expect("put3");
        engine.commit(tx, WriteOptions::default()).unwrap();
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key4".to_vec(), b"val4".to_vec(), None)
            .expect("put4");
        engine.commit(tx, WriteOptions::default()).unwrap();

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.delete_range(b"key2".to_vec(), b"key4".to_vec())
            .expect("delete_range");
        engine.commit(tx, WriteOptions::default()).unwrap();

        // Assert
        // Keys outside range should still exist
        let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        assert_eq!(
            tx.get(b"key1").expect("get1"),
            Some(Bytes::from_static(b"val1")),
            "key1 should exist (outside range) in mode: {}",
            mode
        );
        assert_eq!(
            tx.get(b"key4").expect("get4"),
            Some(Bytes::from_static(b"val4")),
            "key4 should exist (outside range) in mode: {}",
            mode
        );

        // Keys in range [key2, key4) should be deleted
        assert_eq!(
            tx.get(b"key2").expect("get2"),
            None,
            "key2 should be deleted (in range) in mode: {}",
            mode
        );
        assert_eq!(
            tx.get(b"key3").expect("get3"),
            None,
            "key3 should be deleted (in range) in mode: {}",
            mode
        );
    });
}

#[test]
fn should_handle_empty_range_given_start_equals_end_when_delete_range() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key".to_vec(), b"val".to_vec(), None).expect("put");
        engine.commit(tx, WriteOptions::default()).unwrap();

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.delete_range(b"key".to_vec(), b"key".to_vec())
            .expect("delete_range");
        engine.commit(tx, WriteOptions::default()).unwrap();

        // Assert
        // Empty range should not delete anything
        let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        assert_eq!(
            tx.get(b"key").expect("get"),
            Some(Bytes::from_static(b"val")),
            "key should exist (empty range) in mode: {}",
            mode
        );
    });
}

#[test]
fn should_accept_delete_range_call_with_valid_bounds_when_called() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        for i in 0..100 {
            let key = format!("key{:03}", i);
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                .expect("put");
            engine.commit(tx, WriteOptions::default()).unwrap();
        }

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        let result = tx.delete_range(b"key010".to_vec(), b"key090".to_vec());
        if result.is_ok() {
            engine.commit(tx, WriteOptions::default()).unwrap();
        }

        // Assert: delete_range should not error
        result.expect("delete_range should succeed in mode: {}");
    });
}

#[test]
fn should_delete_key_given_delete_range_with_single_key_when_matching() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"target".to_vec(), b"value".to_vec(), None)
            .expect("put");
        engine.commit(tx, WriteOptions::default()).unwrap();

        // Act
        // Range [target, targetZ) includes target
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.delete_range(b"target".to_vec(), b"targetZ".to_vec())
            .expect("delete_range");
        engine.commit(tx, WriteOptions::default()).unwrap();

        // Assert
        // Target should be deleted (in range)
        let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        assert_eq!(
            tx.get(b"target").expect("get"),
            None,
            "target should be deleted in mode: {}",
            mode
        );
    });
}

// ============================================================================
// MULTI-OPERATION BEHAVIOR
// ============================================================================

#[test]
fn should_handle_delete_range_after_put_when_interleaved() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"a".to_vec(), b"val_a".to_vec(), None)
            .expect("put_a");
        engine.commit(tx, WriteOptions::default()).unwrap();
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"b".to_vec(), b"val_b".to_vec(), None)
            .expect("put_b");
        engine.commit(tx, WriteOptions::default()).unwrap();
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"c".to_vec(), b"val_c".to_vec(), None)
            .expect("put_c");
        engine.commit(tx, WriteOptions::default()).unwrap();

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.delete_range(b"a".to_vec(), b"c".to_vec())
            .expect("delete_range");
        engine.commit(tx, WriteOptions::default()).unwrap();
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"b".to_vec(), b"new_b".to_vec(), None)
            .expect("put_after_range");
        engine.commit(tx, WriteOptions::default()).unwrap();

        // Assert
        // Key should have new value from the put after delete_range
        let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        assert_eq!(
            tx.get(b"b").expect("get_b"),
            Some(Bytes::from_static(b"new_b")),
            "put after delete_range should succeed in mode: {}",
            mode
        );
    });
}

#[test]
fn should_allow_multiple_delete_ranges_when_called_sequentially() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        for i in 0..20 {
            let key = format!("k{:02}", i);
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                .expect("put");
            engine.commit(tx, WriteOptions::default()).unwrap();
        }

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.delete_range(b"k03".to_vec(), b"k10".to_vec())
            .expect("delete_range1");
        engine.commit(tx, WriteOptions::default()).unwrap();
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.delete_range(b"k15".to_vec(), b"k18".to_vec())
            .expect("delete_range2");
        engine.commit(tx, WriteOptions::default()).unwrap();

        // Assert: Keys in ranges should be deleted
        let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        for i in 0..20 {
            let key = format!("k{:02}", i);
            let should_exist = !((3..10).contains(&i) || (15..18).contains(&i));
            let result = tx.get(key.as_bytes()).expect("get");
            assert_eq!(
                result.is_some(),
                should_exist,
                "key {} should {} in mode: {}",
                i,
                if should_exist { "exist" } else { "be deleted" },
                mode
            );
        }
    });
}

// ============================================================================
// PERSISTENCE & RECOVERY
// ============================================================================

#[test]
fn should_persist_keys_across_delete_range_with_restart_when_durable() {
    // Note: Only test with durable storage modes (local, cloud).
    // Memory mode doesn't persist.
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange & Act
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key1".to_vec(), b"val1".to_vec(), None)
                .expect("put1");
            engine.commit(tx, WriteOptions::default()).unwrap();
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key2".to_vec(), b"val2".to_vec(), None)
                .expect("put2");
            engine.commit(tx, WriteOptions::default()).unwrap();
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key3".to_vec(), b"val3".to_vec(), None)
                .expect("put3");
            engine.commit(tx, WriteOptions::default()).unwrap();
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx.delete_range(b"key1".to_vec(), b"key3".to_vec())
                .expect("delete_range");
            engine.commit(tx, WriteOptions::default()).unwrap();
            let _ = cf;
        }

        // Reopen and assert
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        // Keys in the delete_range should be deleted
        // delete_range(key1, key3) deletes [key1, key3) = key1 and key2
        let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        assert_eq!(
            tx.get(b"key1").expect("get1"),
            None,
            "key1 should be deleted"
        );
        assert_eq!(
            tx.get(b"key2").expect("get2"),
            None,
            "key2 should be deleted"
        );
        // key3 is outside the range [key1, key3), so it should persist
        assert_eq!(
            tx.get(b"key3").expect("get3"),
            Some(Bytes::from_static(b"val3")),
            "key3 should persist after restart"
        );
    });
}

// ============================================================================
// CONCURRENCY
// ============================================================================

#[test]
fn should_handle_concurrent_delete_ranges_when_multiple_threads() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = std::sync::Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();

        for i in 0..100 {
            let key = format!("key{:03}", i);
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                .expect("put");
            engine.commit(tx, WriteOptions::default()).unwrap();
        }

        // Act: Multiple threads calling delete_range
        let mut handles = vec![];
        for thread_id in 0..5 {
            let engine_clone = engine.clone();
            let h = std::thread::spawn(move || {
                let cf = engine_clone.default_column_family();
                let start = format!("key{:03}", thread_id * 10);
                let end = format!("key{:03}", (thread_id + 1) * 10);
                let mut tx = engine_clone
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .unwrap();
                tx.delete_range(start.as_bytes().to_vec(), end.as_bytes().to_vec())
                    .expect("delete_range");
                engine_clone.commit(tx, WriteOptions::default()).unwrap();
            });
            handles.push(h);
        }

        for h in handles {
            h.join().expect("thread");
        }

        // Assert: Keys in deleted ranges should be gone
        let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        for i in 0..100 {
            let key = format!("key{:03}", i);
            let should_exist = !(0..50).contains(&i); // Threads delete ranges [0-10), [10-20), [20-30), [30-40), [40-50)
            let got = tx.get(key.as_bytes()).expect("get");
            assert_eq!(
                got.is_some(),
                should_exist,
                "key {} should {} in mode: {}",
                i,
                if should_exist { "exist" } else { "be deleted" },
                mode
            );
        }
    });
}

#[test]
fn should_handle_concurrent_mixed_operations_when_put_delete_interleaved() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = std::sync::Arc::new(open_with_mode(opts, mode));

        // Put all keys first (use zero-padded format for proper lexicographic ordering)
        let cf = engine.default_column_family();
        for i in 0..50 {
            let key = format!("k{:02}", i);
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                .expect("put");
            engine.commit(tx, WriteOptions::default()).unwrap();
        }

        // Act: Concurrent delete_ranges
        let mut del_handles = vec![];
        for i in 0..5 {
            let engine_clone = engine.clone();
            let h = std::thread::spawn(move || {
                let cf = engine_clone.default_column_family();
                let start = format!("k{:02}", i * 5);
                let end = format!("k{:02}", (i + 1) * 5);
                let mut tx = engine_clone
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .unwrap();
                tx.delete_range(start.as_bytes().to_vec(), end.as_bytes().to_vec())
                    .expect("delete_range");
                engine_clone.commit(tx, WriteOptions::default()).unwrap();
            });
            del_handles.push(h);
        }

        for h in del_handles {
            h.join().expect("del thread");
        }

        // Assert: Keys in ranges [k00-k05), [k05-k10), [k10-k15), [k15-k20), [k20-k25) should be deleted
        // All other keys should exist
        let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        for i in 0..50 {
            let key = format!("k{:02}", i);
            let should_exist = i >= 25; // Threads delete k00-k24
            let got = tx.get(key.as_bytes()).expect("get");
            assert_eq!(
                got.is_some(),
                should_exist,
                "key {} should {} after concurrent ops in mode: {}",
                i,
                if should_exist { "exist" } else { "be deleted" },
                mode
            );
        }
    });
}

// ============================================================================
// IMPLEMENTATION NOTE
// ============================================================================

#[test]
fn should_document_current_limitation_of_range_method_when_called() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key1".to_vec(), b"val1".to_vec(), None)
            .expect("put1");
        engine.commit(tx, WriteOptions::default()).unwrap();
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key2".to_vec(), b"val2".to_vec(), None)
            .expect("put2");
        engine.commit(tx, WriteOptions::default()).unwrap();

        // Act: Call scan() via transaction to demonstrate current behavior
        let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        let results = tx.scan(b"key1", b"key3").expect("scan");

        // Assert: range() should return keys in the range
        // Currently it returns empty, but should return [key1, key2]
        assert!(
            results.len() >= 2,
            "range() should return at least 2 keys in range [key1, key3) in mode: {}",
            mode
        );

        // The keys should be in the results
        let results_str: Vec<String> = results
            .iter()
            .map(|kv| String::from_utf8_lossy(&kv.0).to_string())
            .collect();

        assert!(
            results_str.contains(&"key1".to_string()),
            "range() results should contain key1 in mode: {}",
            mode
        );
        assert!(
            results_str.contains(&"key2".to_string()),
            "range() results should contain key2 in mode: {}",
            mode
        );
    });
}
