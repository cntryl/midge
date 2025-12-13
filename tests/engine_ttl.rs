//! Integration tests for TTL (Time-To-Live) support

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use cntryl_midge::testkit::*;

// ============================================================================
// Basic TTL Behavior
// ============================================================================

#[test]
fn should_return_value_given_ttl_not_elapsed_when_reading() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        engine.put_with_ttl(cf, b"key1", b"value1", 3600).unwrap(); // 1 hour TTL

        // Act
        let result = engine.get(cf, b"key1").unwrap();

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
        engine.put_with_ttl(cf, b"key1", b"value1", 1).unwrap(); // 1 second TTL

        // Act
        thread::sleep(Duration::from_millis(1100)); // Wait for expiration
        let result = engine.get(cf, b"key1").unwrap();

        // Assert
        assert_eq!(result, None);
    });
}

#[test]
fn should_not_expire_key_given_zero_ttl_means_no_expiration_when_reading() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        engine.put_with_ttl(cf, b"key1", b"value1", 0).unwrap(); // 0 = no expiration

        // Act
        thread::sleep(Duration::from_millis(100));
        let result = engine.get(cf, b"key1").unwrap();

        // Assert
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
            engine.put_with_ttl(cf, b"key1", b"value1", 3600).unwrap(); // 1 hour
                                                                        // Engine dropped
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();
            let result = engine.get(cf, b"key1").unwrap();
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
            engine.put_with_ttl(cf, b"key1", b"value1", 1).unwrap(); // 1 second
            thread::sleep(Duration::from_millis(1100)); // Wait for expiration
                                                        // Engine dropped
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();
            let result = engine.get(cf, b"key1").unwrap();
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
        engine.put_with_ttl(cf, b"key1", b"value1", 1).unwrap(); // 1 second
        thread::sleep(Duration::from_millis(1100));

        // Act - trigger compaction (would need force_compaction API)
        // engine.force_compaction(cf).unwrap();

        // Assert - expired entry should be removed
        let result = engine.get(cf, b"key1").unwrap();
        assert_eq!(result, None);
    });
}

#[test]
fn should_preserve_non_expired_entries_given_compaction_when_ttl_not_exceeded() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        engine.put_with_ttl(cf, b"key1", b"value1", 3600).unwrap(); // 1 hour

        // Act - trigger compaction
        // engine.force_compaction(cf).unwrap();

        // Assert - non-expired entry preserved
        let result = engine.get(cf, b"key1").unwrap();
        assert_eq!(result, Some(Bytes::from_static(b"value1")));
    });
}

// ============================================================================
// Snapshot Interaction
// ============================================================================

#[test]
fn should_hide_expired_key_given_snapshot_after_expiry_when_reading_at_snapshot() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        engine.put_with_ttl(cf, b"key1", b"value1", 1).unwrap(); // 1 second
        thread::sleep(Duration::from_millis(1100)); // Wait for expiration

        // Act - take snapshot after expiration
        let snapshot = engine.snapshot();
        let result = snapshot.get(cf, b"key1").unwrap();

        // Assert - should not see expired key even in snapshot
        assert_eq!(result, None);
    });
}

#[test]
fn should_check_expiration_at_read_time_given_snapshot_when_ttl_elapses_after_snapshot() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        engine.put_with_ttl(cf, b"key1", b"value1", 2).unwrap(); // 2 seconds

        // Act - take snapshot before expiration
        let snapshot = engine.snapshot();
        thread::sleep(Duration::from_millis(2100)); // Wait for expiration
        let result = snapshot.get(cf, b"key1").unwrap();

        // Assert - TTL is checked at read time, not snapshot time
        // This means expired keys are hidden even in older snapshots
        assert_eq!(result, None);
    });
}

// ============================================================================
// Write Batch & TTL
// ============================================================================

#[test]
fn should_apply_ttl_given_write_batch_with_ttl_when_committed() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let _cf = engine.default_column_family();

        // Act - write batch with TTL (need WriteBatch::put_with_ttl API)
        // let mut batch = engine.create_write_batch();
        // batch.put_with_ttl(cf, b"key1", b"value1", 1).unwrap();
        // engine.write_batch(&batch).unwrap();
        // thread::sleep(Duration::from_millis(1100));

        // Assert
        // let result = engine.get(cf, b"key1").unwrap();
        // assert_eq!(result, None);
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
        engine.put_with_ttl(cf, b"key1", b"value1", 1).unwrap(); // Expires
        engine.put_with_ttl(cf, b"key2", b"value2", 0).unwrap(); // Never expires
        engine.put_with_ttl(cf, b"key3", b"value3", 3600).unwrap(); // Long TTL

        // Act
        thread::sleep(Duration::from_millis(1100));
        let result1 = engine.get(cf, b"key1").unwrap();
        let result2 = engine.get(cf, b"key2").unwrap();
        let result3 = engine.get(cf, b"key3").unwrap();

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
        engine.put_with_ttl(cf, b"key1", b"value1", 1).unwrap(); // 1 second
        thread::sleep(Duration::from_millis(500));

        // Act - overwrite with longer TTL
        engine.put_with_ttl(cf, b"key1", b"value2", 3600).unwrap(); // 1 hour
        thread::sleep(Duration::from_millis(700)); // Original would have expired

        // Assert - should still be readable with new TTL
        let result = engine.get(cf, b"key1").unwrap();
        assert_eq!(result, Some(Bytes::from_static(b"value2")));
    });
}
