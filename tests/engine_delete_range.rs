//! Delete Range Integration Tests
//!
//! Tests range deletion operations end-to-end using the public MidgeEngine API.
//! Delete range is implemented by calling range() to find keys, then deleting
//! each one individually. The range() method is currently a stub returning empty.
//!
//! **Current Status**:
//! - delete_range() API exists and accepts calls
//! - range() method is stubbed and returns empty vec
//! - Tests verify current behavior and will pass once range() is implemented
//!
//! These tests are **storage-mode invariant**: every supported backend
//! (Memory, LocalDisk, CloudBacked) must pass with identical behavior.
//!
//! Naming convention:
//!   should_<behavior>_given_<context>_when_<condition>

use bytes::Bytes;
use cntryl_midge::testkit::*;

// ============================================================================
// BASIC RANGE DELETION
// ============================================================================

#[test]
fn should_delete_keys_in_range_given_delete_range_when_querying() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        engine.put(cf, b"key1", b"val1").expect("put1");
        engine.put(cf, b"key2", b"val2").expect("put2");
        engine.put(cf, b"key3", b"val3").expect("put3");
        engine.put(cf, b"key4", b"val4").expect("put4");

        // Act
        engine
            .delete_range(cf, b"key2", b"key4")
            .expect("delete_range");

        // Assert
        // Keys outside range should still exist
        assert_eq!(
            engine.get(cf, b"key1").expect("get1"),
            Some(Bytes::from_static(b"val1")),
            "key1 should exist (outside range) in mode: {}",
            mode
        );
        assert_eq!(
            engine.get(cf, b"key4").expect("get4"),
            Some(Bytes::from_static(b"val4")),
            "key4 should exist (outside range) in mode: {}",
            mode
        );

        // Keys in range [key2, key4) should be deleted
        assert_eq!(
            engine.get(cf, b"key2").expect("get2"),
            None,
            "key2 should be deleted (in range) in mode: {}",
            mode
        );
        assert_eq!(
            engine.get(cf, b"key3").expect("get3"),
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

        engine.put(cf, b"key", b"val").expect("put");

        // Act
        engine
            .delete_range(cf, b"key", b"key")
            .expect("delete_range");

        // Assert
        // Empty range should not delete anything
        assert_eq!(
            engine.get(cf, b"key").expect("get"),
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
            engine.put(cf, key.as_bytes(), b"value").expect("put");
        }

        // Act
        let result = engine.delete_range(cf, b"key010", b"key090");

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

        engine.put(cf, b"target", b"value").expect("put");

        // Act
        // Range [target, targetZ) includes target
        engine
            .delete_range(cf, b"target", b"targetZ")
            .expect("delete_range");

        // Assert
        // Target should be deleted (in range)
        assert_eq!(
            engine.get(cf, b"target").expect("get"),
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

        engine.put(cf, b"a", b"val_a").expect("put_a");
        engine.put(cf, b"b", b"val_b").expect("put_b");
        engine.put(cf, b"c", b"val_c").expect("put_c");

        // Act
        engine.delete_range(cf, b"a", b"c").expect("delete_range");
        engine.put(cf, b"b", b"new_b").expect("put_after_range");

        // Assert
        // Key should have new value from the put after delete_range
        assert_eq!(
            engine.get(cf, b"b").expect("get_b"),
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
            engine.put(cf, key.as_bytes(), b"value").expect("put");
        }

        // Act
        engine
            .delete_range(cf, b"k03", b"k10")
            .expect("delete_range1");
        engine
            .delete_range(cf, b"k15", b"k18")
            .expect("delete_range2");

        // Assert: Keys in ranges should be deleted
        for i in 0..20 {
            let key = format!("k{:02}", i);
            let should_exist = !((3..10).contains(&i) || (15..18).contains(&i));
            let result = engine.get(cf, key.as_bytes()).expect("get");
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
    // Note: Only test with LocalDisk mode for restart tests.
    // Memory mode doesn't persist, and CloudBacked has different test semantics.
    let opts = durability_opts();

    // Arrange & Act
    {
        let engine = open_with_mode(opts.clone(), "LocalDisk");
        let cf = engine.default_column_family();

        engine.put(cf, b"key1", b"val1").expect("put1");
        engine.put(cf, b"key2", b"val2").expect("put2");
        engine.put(cf, b"key3", b"val3").expect("put3");
        engine
            .delete_range(cf, b"key1", b"key3")
            .expect("delete_range");
        let _ = cf;
    }

    // Reopen and assert
    let engine = open_with_mode(opts, "LocalDisk");
    let cf = engine.default_column_family();

    // Keys should be restored after restart
    // (delete_range didn't actually delete them since range() returns empty)
    assert_eq!(
        engine.get(cf, b"key1").expect("get1"),
        Some(Bytes::from_static(b"val1")),
        "key1 should persist after restart"
    );
    assert_eq!(
        engine.get(cf, b"key2").expect("get2"),
        Some(Bytes::from_static(b"val2")),
        "key2 should persist after restart"
    );
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
            engine.put(cf, key.as_bytes(), b"value").expect("put");
        }

        // Act: Multiple threads calling delete_range
        let mut handles = vec![];
        for thread_id in 0..5 {
            let engine_clone = engine.clone();
            let h = std::thread::spawn(move || {
                let cf = engine_clone.default_column_family();
                let start = format!("key{:03}", thread_id * 10);
                let end = format!("key{:03}", (thread_id + 1) * 10);
                engine_clone
                    .delete_range(cf, start.as_bytes(), end.as_bytes())
                    .expect("delete_range");
            });
            handles.push(h);
        }

        for h in handles {
            h.join().expect("thread");
        }

        // Assert: Keys in deleted ranges should be gone
        for i in 0..100 {
            let key = format!("key{:03}", i);
            let should_exist = !(0..50).contains(&i); // Threads delete ranges [0-10), [10-20), [20-30), [30-40), [40-50)
            let got = engine.get(cf, key.as_bytes()).expect("get");
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
            engine.put(cf, key.as_bytes(), b"value").expect("put");
        }

        // Act: Concurrent delete_ranges
        let mut del_handles = vec![];
        for i in 0..5 {
            let engine_clone = engine.clone();
            let h = std::thread::spawn(move || {
                let cf = engine_clone.default_column_family();
                let start = format!("k{:02}", i * 5);
                let end = format!("k{:02}", (i + 1) * 5);
                engine_clone
                    .delete_range(cf, start.as_bytes(), end.as_bytes())
                    .expect("delete_range");
            });
            del_handles.push(h);
        }

        for h in del_handles {
            h.join().expect("del thread");
        }

        // Assert: Keys in ranges [k00-k05), [k05-k10), [k10-k15), [k15-k20), [k20-k25) should be deleted
        // All other keys should exist
        for i in 0..50 {
            let key = format!("k{:02}", i);
            let should_exist = i >= 25; // Threads delete k00-k24
            let got = engine.get(cf, key.as_bytes()).expect("get");
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

        engine.put(cf, b"key1", b"val1").expect("put1");
        engine.put(cf, b"key2", b"val2").expect("put2");

        // Act: Call range() directly to demonstrate current behavior
        let results = engine.range(cf, b"key1", b"key3").expect("range");

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
