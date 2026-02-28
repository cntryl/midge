//! Integration tests for TTL (Time-To-Live) support

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use cntryl_midge::testkit::*;
use cntryl_midge::{TransactionMode, WriteOptions};

// ============================================================================
// Basic TTL Behavior
// ============================================================================

#[test]
fn should_return_value_given_ttl_not_elapsed_when_reading() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key1".to_vec(), b"value1".to_vec(), Some(3600))
            .unwrap(); // 1 hour TTL
        engine.commit(tx, WriteOptions::buffered()).unwrap();

        // Act
        let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        let result = read_tx.get(b"key1").unwrap();

        // Assert
        assert_eq!(result, Some(Bytes::from_static(b"value1")));
    });
}

#[test]
fn should_return_none_given_ttl_elapsed_when_reading() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key1".to_vec(), b"value1".to_vec(), Some(1))
            .unwrap(); // 1 second TTL
        engine.commit(tx, WriteOptions::buffered()).unwrap();

        // Act
        thread::sleep(Duration::from_millis(1100)); // Wait for expiration
        let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        let result = read_tx.get(b"key1").unwrap();

        // Assert
        assert_eq!(result, None);
    });
}

#[test]
fn should_not_expire_key_given_zero_ttl_when_zero_means_infinite() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key1".to_vec(), b"value1".to_vec(), Some(0))
            .unwrap(); // 0 = no expiration (infinite)
        engine.commit(tx, WriteOptions::buffered()).unwrap();

        // Act
        thread::sleep(Duration::from_millis(100));
        let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        let result = read_tx.get(b"key1").unwrap();

        // Assert - TTL of 0 means never expires
        assert_eq!(result, Some(Bytes::from_static(b"value1")));
    });
}

// ============================================================================
// Persistence & Recovery
// ============================================================================

#[test]
fn should_persist_ttl_metadata_given_restart_when_reopening() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key1".to_vec(), b"value1".to_vec(), Some(3600))
                .unwrap(); // 1 hour
            engine.commit(tx, WriteOptions::buffered()).unwrap();
            // Engine dropped
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
            let result = read_tx.get(b"key1").unwrap();
            assert_eq!(result, Some(Bytes::from_static(b"value1")));
        }
    });
}

#[test]
fn should_expire_after_restart_given_ttl_elapsed_during_shutdown_when_reopening() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine
                .get_column_family("test")
                .unwrap_or_else(|| engine.create_column_family("test").expect("create cf"));
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key1".to_vec(), b"value1".to_vec(), Some(1))
                .unwrap(); // 1 second
            engine.commit(tx, WriteOptions::buffered()).unwrap();
            thread::sleep(Duration::from_millis(1100)); // Wait for expiration
                                                        // Engine dropped
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine
                .get_column_family("test")
                .unwrap_or_else(|| engine.create_column_family("test").expect("create cf"));
            let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
            let result = read_tx.get(b"key1").unwrap();
            assert_eq!(result, None);
        }
    });
}

// ============================================================================
// Compaction Interaction
// ============================================================================

#[test]
fn should_remove_expired_entries_given_compaction_when_ttl_exceeded() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key1".to_vec(), b"value1".to_vec(), Some(1))
            .unwrap(); // 1 second
        engine.commit(tx, WriteOptions::buffered()).unwrap();
        thread::sleep(Duration::from_millis(1100));

        // Act - trigger flush
        engine.flush_cf(&cf).unwrap();

        // Assert - expired entry should be removed
        let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        let result = read_tx.get(b"key1").unwrap();
        assert_eq!(result, None);
    });
}

#[test]
fn should_preserve_non_expired_entries_given_compaction_when_ttl_not_exceeded() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key1".to_vec(), b"value1".to_vec(), Some(3600))
            .unwrap(); // 1 hour
        engine.commit(tx, WriteOptions::buffered()).unwrap();

        // Act - trigger flush
        engine.flush_cf(&cf).unwrap();

        // Assert - non-expired entry preserved
        let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        let result = read_tx.get(b"key1").unwrap();
        assert_eq!(result, Some(Bytes::from_static(b"value1")));
    });
}

// ============================================================================
// Mixed TTL Keys
// ============================================================================

#[test]
fn should_handle_mixed_ttl_keys_given_some_expire_when_reading() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut tx1 = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx1.put(b"key1".to_vec(), b"value1".to_vec(), Some(1))
            .unwrap(); // Expires
        engine.commit(tx1, WriteOptions::buffered()).unwrap();
        let mut tx2 = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx2.put(b"key2".to_vec(), b"value2".to_vec(), Some(0))
            .unwrap(); // Never expires
        engine.commit(tx2, WriteOptions::buffered()).unwrap();
        let mut tx3 = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx3.put(b"key3".to_vec(), b"value3".to_vec(), Some(3600))
            .unwrap(); // Long TTL
        engine.commit(tx3, WriteOptions::buffered()).unwrap();

        // Act
        thread::sleep(Duration::from_millis(1100));
        let read_tx1 = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        let result1 = read_tx1.get(b"key1").unwrap();
        let read_tx2 = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        let result2 = read_tx2.get(b"key2").unwrap();
        let read_tx3 = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        let result3 = read_tx3.get(b"key3").unwrap();

        // Assert
        assert_eq!(result1, None); // Expired
        assert_eq!(result2, Some(Bytes::from_static(b"value2"))); // Never expires
        assert_eq!(result3, Some(Bytes::from_static(b"value3"))); // Still valid
    });
}

// ============================================================================
// TTL Update
// ============================================================================

#[test]
fn should_update_ttl_given_overwrite_with_new_ttl_when_writing() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut tx1 = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx1.put(b"key1".to_vec(), b"value1".to_vec(), Some(1))
            .unwrap(); // 1 second
        engine.commit(tx1, WriteOptions::buffered()).unwrap();
        thread::sleep(Duration::from_millis(500));

        // Act - overwrite with longer TTL
        let mut tx2 = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx2.put(b"key1".to_vec(), b"value2".to_vec(), Some(3600))
            .unwrap(); // 1 hour
        engine.commit(tx2, WriteOptions::buffered()).unwrap();
        thread::sleep(Duration::from_millis(700)); // Original would have expired

        // Assert - should still be readable with new TTL
        let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        let result = read_tx.get(b"key1").unwrap();
        assert_eq!(result, Some(Bytes::from_static(b"value2")));
    });
}

// ============================================================================
// TTL & Range Tombstone Interactions
// ============================================================================

#[test]
fn should_expire_keys_covered_by_range_tombstone_during_compaction() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        eprintln!(
            "\n=== TTL: Expire Keys Covered by Range Tombstone (mode: {}) ===",
            mode
        );

        // Arrange: Write keys with TTL in range [k3, k8)
        let engine = Arc::new(open_with_mode(opts.clone(), mode));
        let cf = engine.create_column_family("test").expect("create cf");

        // Write keys k1..k10 with 1 second TTL
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        for i in 1..=10 {
            let key = format!("k{}", i);
            tx.put(key.as_bytes().to_vec(), b"ttl_value".to_vec(), Some(1))
                .unwrap();
        }
        engine.commit(tx, WriteOptions::buffered()).unwrap();
        engine.flush_cf(&cf).expect("flush");

        // Write range tombstone [k3, k8) - covers k3-k7
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.delete_range(b"k3".to_vec(), b"k8".to_vec()).unwrap();
        engine.commit(tx, WriteOptions::buffered()).unwrap();
        engine.flush_cf(&cf).expect("flush");

        // Wait for TTL expiry
        thread::sleep(Duration::from_millis(1100));

        // Act: Trigger compaction
        engine.compact_all().ok();

        // Assert: All keys expired and/or tombstoned, compaction cleaned them up
        let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();

        // k1, k2 should be expired (TTL)
        assert_eq!(tx.get(b"k1").unwrap(), None);
        assert_eq!(tx.get(b"k2").unwrap(), None);

        // k3-k7 should be gone (range tombstone)
        assert_eq!(tx.get(b"k3").unwrap(), None);
        assert_eq!(tx.get(b"k5").unwrap(), None);
        assert_eq!(tx.get(b"k7").unwrap(), None);

        // k8-k10 should be expired (TTL)
        assert_eq!(tx.get(b"k8").unwrap(), None);
        assert_eq!(tx.get(b"k10").unwrap(), None);

        eprintln!("✓ TTL and range tombstone both cleaned during compaction");
    });
}

#[test]
fn should_handle_ttl_expiry_during_multi_level_compaction() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        eprintln!(
            "\n=== TTL: Multi-Level Compaction with Expiry (mode: {}) ===",
            mode
        );

        // Arrange: Build multi-level LSM with different TTLs
        let engine = Arc::new(open_with_mode(opts.clone(), mode));
        let cf = engine.create_column_family("test").expect("create cf");

        // L0: Write keys with 1-second TTL
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        for i in 0..50 {
            let key = format!("level0_key_{:04}", i);
            tx.put(key.as_bytes().to_vec(), b"l0_value".to_vec(), Some(1))
                .unwrap();
        }
        engine.commit(tx, WriteOptions::buffered()).unwrap();
        engine.flush_cf(&cf).expect("flush L0");

        // L1: Write keys with longer TTL
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        for i in 50..100 {
            let key = format!("level1_key_{:04}", i);
            tx.put(key.as_bytes().to_vec(), b"l1_value".to_vec(), Some(3600))
                .unwrap();
        }
        engine.commit(tx, WriteOptions::buffered()).unwrap();
        engine.flush_cf(&cf).expect("flush L1");

        // Wait for L0 TTL to expire
        thread::sleep(Duration::from_millis(1100));

        // Act: Trigger compaction (L0→L1)
        engine.compact_all().ok();

        // Assert: L0 expired keys removed; L1 keys unchanged
        let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();

        // L0 keys should be gone (expired)
        let l0_found = (0..50)
            .filter(|i| {
                let key = format!("level0_key_{:04}", i);
                tx.get(key.as_bytes()).ok().flatten().is_some()
            })
            .count();
        assert_eq!(l0_found, 0, "L0 expired keys should be removed");

        // L1 keys should remain (not expired)
        let l1_found = (50..100)
            .filter(|i| {
                let key = format!("level1_key_{:04}", i);
                tx.get(key.as_bytes()).ok().flatten().is_some()
            })
            .count();
        assert!(l1_found >= 40, "L1 keys should remain");

        eprintln!(
            "✓ Multi-level compaction handled TTL correctly; L1 retained: {}",
            l1_found
        );
    });
}

#[test]
fn should_not_expose_ttl_expired_key_covered_by_range_tombstone() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        eprintln!("\n=== TTL: Don't Expose TTL+Tombstone (mode: {}) ===", mode);

        // Arrange: Create scenario with both TTL and tombstone covering same key
        let engine = Arc::new(open_with_mode(opts.clone(), mode));
        let cf = engine.create_column_family("test").expect("create cf");

        // Write k5 with 1-second TTL (not flushed)
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"k5".to_vec(), b"ttl_tombstone_value".to_vec(), Some(1))
            .unwrap();
        engine.commit(tx, WriteOptions::buffered()).unwrap();

        // Immediately write range tombstone [k1, k10)
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.delete_range(b"k1".to_vec(), b"k10".to_vec()).unwrap();
        engine.commit(tx, WriteOptions::buffered()).unwrap();

        // Act: Read k5 after TTL expiry
        thread::sleep(Duration::from_millis(1100));
        let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        let result = tx.get(b"k5").unwrap();

        // Assert: k5 not exposed (TTL expired AND tombstone covers it)
        assert_eq!(result, None);

        eprintln!("✓ TTL-expired + tombstone-covered key not exposed");
    });
}
