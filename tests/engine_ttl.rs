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
        let cf = engine.default_column_family();
        let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite).unwrap();
        tx.put(b"key1".to_vec(), b"value1".to_vec(), Some(3600)).unwrap(); // 1 hour TTL
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
        let cf = engine.default_column_family();
        let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite).unwrap();
        tx.put(b"key1".to_vec(), b"value1".to_vec(), Some(1)).unwrap(); // 1 second TTL
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
        let cf = engine.default_column_family();
        let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite).unwrap();
        tx.put(b"key1".to_vec(), b"value1".to_vec(), Some(0)).unwrap(); // 0 = no expiration (infinite)
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
        // Arrange & Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();
            let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite).unwrap();
            tx.put(b"key1".to_vec(), b"value1".to_vec(), Some(3600)).unwrap(); // 1 hour
            engine.commit(tx, WriteOptions::buffered()).unwrap();
            // Engine dropped
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();
            let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
            let result = read_tx.get(b"key1").unwrap();
            assert_eq!(result, Some(Bytes::from_static(b"value1")));
        }
    });
}

#[test]
fn should_expire_after_restart_given_ttl_elapsed_during_shutdown_when_reopening() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange & Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();
            let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite).unwrap();
            tx.put(b"key1".to_vec(), b"value1".to_vec(), Some(1)).unwrap(); // 1 second
            engine.commit(tx, WriteOptions::buffered()).unwrap();
            thread::sleep(Duration::from_millis(1100)); // Wait for expiration
            // Engine dropped
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();
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
        let cf = engine.default_column_family();
        let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite).unwrap();
        tx.put(b"key1".to_vec(), b"value1".to_vec(), Some(1)).unwrap(); // 1 second
        engine.commit(tx, WriteOptions::buffered()).unwrap();
        thread::sleep(Duration::from_millis(1100));

        // Act - trigger compaction
        engine.compact_all().unwrap();

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
        let cf = engine.default_column_family();
        let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite).unwrap();
        tx.put(b"key1".to_vec(), b"value1".to_vec(), Some(3600)).unwrap(); // 1 hour
        engine.commit(tx, WriteOptions::buffered()).unwrap();

        // Act - trigger compaction
        engine.compact_all().unwrap();

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
        let cf = engine.default_column_family();
        let mut tx1 = engine.begin_tx(cf.id(), TransactionMode::ReadWrite).unwrap();
        tx1.put(b"key1".to_vec(), b"value1".to_vec(), Some(1)).unwrap(); // Expires
        engine.commit(tx1, WriteOptions::buffered()).unwrap();
        let mut tx2 = engine.begin_tx(cf.id(), TransactionMode::ReadWrite).unwrap();
        tx2.put(b"key2".to_vec(), b"value2".to_vec(), Some(0)).unwrap(); // Never expires
        engine.commit(tx2, WriteOptions::buffered()).unwrap();
        let mut tx3 = engine.begin_tx(cf.id(), TransactionMode::ReadWrite).unwrap();
        tx3.put(b"key3".to_vec(), b"value3".to_vec(), Some(3600)).unwrap(); // Long TTL
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
        let cf = engine.default_column_family();
        let mut tx1 = engine.begin_tx(cf.id(), TransactionMode::ReadWrite).unwrap();
        tx1.put(b"key1".to_vec(), b"value1".to_vec(), Some(1)).unwrap(); // 1 second
        engine.commit(tx1, WriteOptions::buffered()).unwrap();
        thread::sleep(Duration::from_millis(500));

        // Act - overwrite with longer TTL
        let mut tx2 = engine.begin_tx(cf.id(), TransactionMode::ReadWrite).unwrap();
        tx2.put(b"key1".to_vec(), b"value2".to_vec(), Some(3600)).unwrap(); // 1 hour
        engine.commit(tx2, WriteOptions::buffered()).unwrap();
        thread::sleep(Duration::from_millis(700)); // Original would have expired

        // Assert - should still be readable with new TTL
        let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        let result = read_tx.get(b"key1").unwrap();
        assert_eq!(result, Some(Bytes::from_static(b"value2")));
    });
}
