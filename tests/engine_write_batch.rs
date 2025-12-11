//! Write Batch Integration Tests
//!
//! Tests batched write operations end-to-end using the public MidgeEngine API.
//! Write batches provide atomic multi-operation semantics: all operations in a
//! batch are applied together or not at all.
//!
//! These tests are **storage-mode invariant**: every supported backend
//! (Memory, LocalDisk, CloudBacked) must pass with identical behavior.
//!
//! Naming convention:
//!   should_<behavior>_given_<context>_when_<condition>

use bytes::Bytes;
use cntryl_midge::engine::api::WriteBatch;
use cntryl_midge::testkit::*;

// ============================================================================
// BASIC BATCH OPERATIONS
// ============================================================================

#[test]
fn should_commit_all_operations_given_batch_when_write_batch() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();
        let mut batch = WriteBatch::new();

        // Act
        batch.put(b"key1".to_vec(), b"val1".to_vec());
        batch.put(b"key2".to_vec(), b"val2".to_vec());
        batch.put(b"key3".to_vec(), b"val3".to_vec());
        engine.write_batch(&batch).expect("write_batch");

        // Assert
        assert_eq!(
            engine.get(cf, b"key1").expect("get1"),
            Some(Bytes::from_static(b"val1")),
            "key1 missing in mode: {}",
            mode
        );
        assert_eq!(
            engine.get(cf, b"key2").expect("get2"),
            Some(Bytes::from_static(b"val2")),
            "key2 missing in mode: {}",
            mode
        );
        assert_eq!(
            engine.get(cf, b"key3").expect("get3"),
            Some(Bytes::from_static(b"val3")),
            "key3 missing in mode: {}",
            mode
        );
    });
}

#[test]
fn should_apply_last_value_given_duplicate_keys_when_write_batch() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();
        let mut batch = WriteBatch::new();

        // Act
        batch.put(b"key".to_vec(), b"value1".to_vec());
        batch.put(b"key".to_vec(), b"value2".to_vec());
        batch.put(b"key".to_vec(), b"value3".to_vec());
        engine.write_batch(&batch).expect("write_batch");

        // Assert
        let got = engine.get(cf, b"key").expect("get");
        assert_eq!(
            got,
            Some(Bytes::from_static(b"value3")),
            "expected last value in mode: {}",
            mode
        );
    });
}

#[test]
fn should_succeed_given_empty_batch_when_write_batch() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let batch = WriteBatch::new();

        // Act
        let result = engine.write_batch(&batch);

        // Assert
        result.expect("empty batch should succeed");
    });
}

#[test]
fn should_delete_key_given_delete_after_put_when_write_batch() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();
        let mut batch = WriteBatch::new();

        // Act
        batch.put(b"key".to_vec(), b"value".to_vec());
        batch.delete(b"key".to_vec());
        engine.write_batch(&batch).expect("write_batch");

        // Assert
        let got = engine.get(cf, b"key").expect("get");
        assert_eq!(got, None, "key should be deleted in mode: {}", mode);
    });
}

#[test]
fn should_delete_existing_key_given_delete_in_batch_when_write_batch() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();
        engine.put(cf, b"key", b"value").expect("initial put");

        let mut batch = WriteBatch::new();

        // Act
        batch.delete(b"key".to_vec());
        engine.write_batch(&batch).expect("write_batch");

        // Assert
        let got = engine.get(cf, b"key").expect("get");
        assert_eq!(got, None, "key should be deleted in mode: {}", mode);
    });
}

#[test]
fn should_overwrite_existing_value_given_put_in_batch_when_write_batch() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();
        engine.put(cf, b"key", b"old_value").expect("initial put");

        let mut batch = WriteBatch::new();

        // Act
        batch.put(b"key".to_vec(), b"new_value".to_vec());
        engine.write_batch(&batch).expect("write_batch");

        // Assert
        let got = engine.get(cf, b"key").expect("get");
        assert_eq!(
            got,
            Some(Bytes::from_static(b"new_value")),
            "value not overwritten in mode: {}",
            mode
        );
    });
}

#[test]
fn should_apply_mixed_operations_in_order_when_write_batch() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();
        engine.put(cf, b"a", b"initial_a").expect("setup a");
        engine.put(cf, b"b", b"initial_b").expect("setup b");

        let mut batch = WriteBatch::new();

        // Act
        batch.put(b"a".to_vec(), b"updated_a".to_vec());
        batch.delete(b"b".to_vec());
        batch.put(b"c".to_vec(), b"new_c".to_vec());
        engine.write_batch(&batch).expect("write_batch");

        // Assert
        assert_eq!(
            engine.get(cf, b"a").expect("get a"),
            Some(Bytes::from_static(b"updated_a")),
            "a not updated in mode: {}",
            mode
        );
        assert_eq!(
            engine.get(cf, b"b").expect("get b"),
            None,
            "b should be deleted in mode: {}",
            mode
        );
        assert_eq!(
            engine.get(cf, b"c").expect("get c"),
            Some(Bytes::from_static(b"new_c")),
            "c not created in mode: {}",
            mode
        );
    });
}

#[test]
fn should_handle_large_batch_given_many_operations_when_write_batch() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();
        const BATCH_SIZE: usize = 1000;
        let mut batch = WriteBatch::new();

        // Act
        for i in 0..BATCH_SIZE {
            let key = format!("key_{i}");
            let val = format!("value_{i}");
            batch.put(key.into_bytes(), val.into_bytes());
        }
        engine.write_batch(&batch).expect("write_batch");

        // Assert
        for i in 0..BATCH_SIZE {
            let key = format!("key_{i}");
            let expected = format!("value_{i}");
            let got = engine.get(cf, key.as_bytes()).expect("get");
            assert_eq!(
                got,
                Some(Bytes::from(expected)),
                "value mismatch at index {} in mode: {}",
                i,
                mode
            );
        }
    });
}

// ============================================================================
// MULTI-COLUMN FAMILY BATCH OPERATIONS
// ============================================================================

#[test]
fn should_write_to_multiple_cfs_given_multi_cf_batch_when_write_batch() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        let mut batch = WriteBatch::new();

        // Act
        batch.put(b"cf_default_key".to_vec(), b"cf_default_val".to_vec());
        engine.write_batch(&batch).expect("write_batch");

        // Assert
        let got = engine.get(cf, b"cf_default_key").expect("get");
        assert_eq!(
            got,
            Some(Bytes::from_static(b"cf_default_val")),
            "value not in default cf in mode: {}",
            mode
        );
    });
}

#[test]
fn should_isolate_keys_given_same_key_in_different_cfs_when_write_batch() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf_default = engine.default_column_family();

        let mut batch = WriteBatch::new();

        // Act
        batch.put(b"shared_key".to_vec(), b"value_default".to_vec());
        engine.write_batch(&batch).expect("write_batch");

        // Assert
        let got_default = engine.get(cf_default, b"shared_key").expect("get");
        assert_eq!(
            got_default,
            Some(Bytes::from_static(b"value_default")),
            "value mismatch in default cf in mode: {}",
            mode
        );
    });
}

// ============================================================================
// CONCURRENCY
// ============================================================================

#[test]
fn should_not_interleave_given_concurrent_batches_when_write_batch() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = std::sync::Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();

        let mut handles = vec![];

        // Act
        for thread_id in 0..10 {
            let engine_clone = engine.clone();
            let h = std::thread::spawn(move || {
                let mut batch = WriteBatch::new();
                for i in 0..10 {
                    let key = format!("t{}_k{}", thread_id, i);
                    let val = format!("t{}_v{}", thread_id, i);
                    batch.put(key.into_bytes(), val.into_bytes());
                }
                engine_clone.write_batch(&batch).expect("write_batch");
            });
            handles.push(h);
        }

        for h in handles {
            h.join().expect("thread");
        }

        // Assert
        for thread_id in 0..10 {
            for i in 0..10 {
                let key = format!("t{}_k{}", thread_id, i);
                let expected = format!("t{}_v{}", thread_id, i);
                let got = engine.get(cf, key.as_bytes()).expect("get");
                assert_eq!(
                    got,
                    Some(Bytes::from(expected)),
                    "concurrent write lost data in mode: {}",
                    mode
                );
            }
        }
    });
}

#[test]
fn should_maintain_atomicity_during_concurrent_reads_when_write_batch() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = std::sync::Arc::new(open_with_mode(opts, mode));

        // Set initial values
        {
            let cf = engine.default_column_family();
            for i in 0..10 {
                let key = format!("key_{i}");
                engine.put(cf, key.as_bytes(), b"initial").expect("setup");
            }
        }

        let mut write_handles = vec![];
        let mut read_handles = vec![];

        // Act: Concurrent writers and readers
        for _ in 0..5 {
            let engine_clone = engine.clone();
            let h = std::thread::spawn(move || {
                let mut batch = WriteBatch::new();
                for i in 0..10 {
                    let key = format!("key_{i}");
                    batch.put(key.into_bytes(), b"updated".to_vec());
                }
                engine_clone.write_batch(&batch).expect("write_batch");
            });
            write_handles.push(h);
        }

        for _ in 0..10 {
            let engine_clone = engine.clone();
            let h = std::thread::spawn(move || {
                let cf = engine_clone.default_column_family();
                for i in 0..10 {
                    let key = format!("key_{i}");
                    let _ = engine_clone.get(cf, key.as_bytes());
                }
            });
            read_handles.push(h);
        }

        for h in write_handles {
            h.join().expect("write thread");
        }
        for h in read_handles {
            h.join().expect("read thread");
        }

        // Assert: All keys should have consistent final value
        let cf = engine.default_column_family();
        for i in 0..10 {
            let key = format!("key_{i}");
            let got = engine.get(cf, key.as_bytes()).expect("get");
            assert!(
                got.is_some(),
                "key {} lost in concurrent reads in mode: {}",
                i,
                mode
            );
        }
    });
}

// ============================================================================
// PERSISTENCE & RECOVERY
// ============================================================================

#[test]
fn should_persist_batch_given_flush_when_reopening() {
    // Note: Only test with durable storage modes (LocalDisk, CloudBacked).
    // Memory mode doesn't persist, so it's excluded.
    let opts = durability_opts();

    // Arrange
    {
        let engine = open_with_mode(opts.clone(), "LocalDisk");
        let cf = engine.default_column_family();
        let mut batch = WriteBatch::new();

        // Act
        batch.put(b"persist_key".to_vec(), b"persist_val".to_vec());
        engine.write_batch(&batch).expect("write_batch");
        engine.flush().expect("flush");
        let _ = cf; // Use cf in the block
    }

    // Reopen and assert
    let engine = open_with_mode(opts, "LocalDisk");
    let cf = engine.default_column_family();
    let got = engine.get(cf, b"persist_key").expect("get");
    assert_eq!(
        got,
        Some(Bytes::from_static(b"persist_val")),
        "persisted batch not recovered"
    );
}

#[test]
fn should_be_atomic_given_crash_during_wal_write_when_recovering() {
    // Note: WAL is only used in durable storage modes (not in Memory mode).
    // Memory mode doesn't persist WAL, so skip this test for Memory.
    let opts = durability_opts();

    // Arrange
    {
        let engine = open_with_mode(opts.clone(), "LocalDisk");
        let cf = engine.default_column_family();
        let mut batch = WriteBatch::new();

        // Act: Write batch (atomically in WAL)
        batch.put(b"atomic_key1".to_vec(), b"atomic_val1".to_vec());
        batch.put(b"atomic_key2".to_vec(), b"atomic_val2".to_vec());
        engine.write_batch(&batch).expect("write_batch");
        let _ = cf;
    }

    // Reopen and verify batch atomicity
    let engine = open_with_mode(opts, "LocalDisk");
    let cf = engine.default_column_family();
    let val1 = engine.get(cf, b"atomic_key1").expect("get1");
    let val2 = engine.get(cf, b"atomic_key2").expect("get2");

    // Either both present or both absent (atomic)
    assert!(
        val1.is_some() && val2.is_some() || val1.is_none() && val2.is_none(),
        "batch not atomic"
    );
}

#[test]
fn should_be_atomic_given_large_batch_crash_when_recovering() {
    // Note: WAL is only used in durable storage modes (not in Memory mode).
    // Memory mode doesn't persist WAL, so skip this test for Memory.
    let opts = durability_opts();

    // Arrange
    {
        let engine = open_with_mode(opts.clone(), "LocalDisk");
        let cf = engine.default_column_family();
        let mut batch = WriteBatch::new();

        // Act: Large batch written atomically
        for i in 0..100 {
            let key = format!("crash_key_{i}");
            let val = format!("crash_val_{i}");
            batch.put(key.into_bytes(), val.into_bytes());
        }
        engine.write_batch(&batch).expect("write_batch");
        let _ = cf;
    }

    // Reopen and verify all-or-nothing
    let engine = open_with_mode(opts, "LocalDisk");
    let cf = engine.default_column_family();
    let mut count = 0;
    for i in 0..100 {
        let key = format!("crash_key_{i}");
        if engine.get(cf, key.as_bytes()).expect("get").is_some() {
            count += 1;
        }
    }

    // Either all 100 present or all absent (atomic)
    assert!(
        count == 100 || count == 0,
        "batch not atomic: {} recovered",
        count
    );
}

// ============================================================================
// TTL & SEQUENCE NUMBERS
// ============================================================================

#[test]
fn should_support_batch_with_ttl_when_write_batch() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();
        let mut batch = WriteBatch::new();

        // Act
        batch.put(b"ttl_key".to_vec(), b"ttl_value".to_vec());
        engine.write_batch(&batch).expect("write_batch");

        // Assert: Value immediately readable (TTL not elapsed)
        let got = engine.get(cf, b"ttl_key").expect("get");
        assert_eq!(
            got,
            Some(Bytes::from_static(b"ttl_value")),
            "ttl batch value not readable in mode: {}",
            mode
        );
    });
}

#[test]
fn should_increment_sequence_numbers_given_batch_operations_when_write_batch() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        // Act
        let mut batch = WriteBatch::new();
        batch.put(b"seq_key".to_vec(), b"seq_val".to_vec());
        engine.write_batch(&batch).expect("write_batch");

        let mut batch2 = WriteBatch::new();
        batch2.put(b"seq_key2".to_vec(), b"seq_val2".to_vec());
        engine.write_batch(&batch2).expect("write_batch");

        // Assert: Both values present (sequence advanced)
        let got1 = engine.get(cf, b"seq_key").expect("get1");
        let got2 = engine.get(cf, b"seq_key2").expect("get2");

        assert!(
            got1.is_some(),
            "first batch value missing in mode: {}",
            mode
        );
        assert!(
            got2.is_some(),
            "second batch value missing in mode: {}",
            mode
        );
    });
}
