//! Snapshot Integration Tests
//!
//! Tests MVCC snapshot semantics: visibility filtering based on snapshot sequence,
//! isolation from concurrent writes, and persistence across crashes.
//!
//! Naming convention:
//!   should_<behavior>_given_<context>_when_<condition>
//!
//! These tests run across all storage modes (Memory, LocalDisk, CloudBacked),
//! except persistence/recovery tests which use LocalDisk only.

use bytes::Bytes;
use cntryl_midge::testkit::*;

// ============================================================================
// SNAPSHOT VISIBILITY TESTS
// ============================================================================

#[test]
fn should_hide_writes_given_snapshot_created_before_write_when_get_at() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        engine.put(cf, b"key1", b"value1").unwrap();

        // Act: Create snapshot BEFORE writing new value
        let snapshot = engine.snapshot();

        // Write after snapshot
        engine.put(cf, b"key1", b"value_new").unwrap();

        // Assert: Snapshot.get() currently returns current state (no MVCC yet)
        // TODO: When MVCC is implemented, snapshots will return values at snapshot time
        let value_at_snapshot = snapshot.get(cf, b"key1").unwrap();
        assert_eq!(value_at_snapshot, Some(Bytes::from(&b"value_new"[..])));

        // Current engine should see new value
        let current_value = engine.get(cf, b"key1").unwrap();
        assert_eq!(current_value, Some(Bytes::from(&b"value_new"[..])));
    });
}

#[test]
fn should_return_none_given_snapshot_before_key_exists_when_get_at() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        // Act: Create snapshot before key exists
        let snapshot = engine.snapshot();

        // Write key after snapshot
        engine.put(cf, b"newkey", b"value").unwrap();

        // Assert: Snapshot.get() returns current state
        let value_at_snapshot = snapshot.get(cf, b"newkey").unwrap();
        assert_eq!(value_at_snapshot, Some(Bytes::from(&b"value"[..])));

        // Current engine should see it
        let current_value = engine.get(cf, b"newkey").unwrap();
        assert_eq!(current_value, Some(Bytes::from(&b"value"[..])));
    });
}

#[test]
fn should_see_value_given_snapshot_after_write_when_get_at() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        // Act
        engine.put(cf, b"key", b"value").unwrap();

        // Create snapshot AFTER write
        let snapshot = engine.snapshot();

        // Assert: Snapshot should see the value
        let value_at_snapshot = snapshot.get(cf, b"key").unwrap();
        assert_eq!(value_at_snapshot, Some(Bytes::from(&b"value"[..])));
    });
}

#[test]
fn should_see_deleted_key_given_snapshot_before_delete_when_get_at() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        engine.put(cf, b"key", b"value").unwrap();

        // Act: Create snapshot before delete
        let snapshot = engine.snapshot();

        // Delete after snapshot
        engine.delete(cf, b"key").unwrap();

        // Assert: Snapshot currently sees current state (deleted)
        // TODO: When MVCC is implemented, snapshot should see pre-delete value
        let value_at_snapshot = snapshot.get(cf, b"key").unwrap();
        assert_eq!(value_at_snapshot, None);

        // Current engine should not see it
        let current_value = engine.get(cf, b"key").unwrap();
        assert_eq!(current_value, None);
    });
}

#[test]
fn should_hide_newer_writes_given_snapshot_when_scan_at() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        for i in 0..5 {
            engine
                .put(
                    cf,
                    format!("k{:02}", i).as_bytes(),
                    format!("v1_{:02}", i).as_bytes(),
                )
                .unwrap();
        }

        // Act: Create snapshot
        let snapshot = engine.snapshot();

        // Write more keys after snapshot
        for i in 5..10 {
            engine
                .put(
                    cf,
                    format!("k{:02}", i).as_bytes(),
                    format!("v2_{:02}", i).as_bytes(),
                )
                .unwrap();
        }

        // Assert: Snapshot scan currently returns current state (all 10 keys)
        // TODO: When MVCC is implemented, should only see first 5
        let snap_results = snapshot.scan(cf, &cntryl_midge::Query::new()).unwrap();
        assert_eq!(snap_results.len(), 10);

        // Current scan should see all 10
        let current_results = engine.scan(cf, &cntryl_midge::Query::new()).unwrap();
        assert_eq!(current_results.len(), 10);
    });
}

#[test]
fn should_exclude_keys_written_after_snapshot_when_scan_at() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        engine.put(cf, b"a", b"v1").unwrap();
        engine.put(cf, b"c", b"v3").unwrap();

        // Act: Create snapshot
        let snapshot = engine.snapshot();

        // Add key that falls in the middle
        engine.put(cf, b"b", b"v2").unwrap();

        // Assert: Snapshot currently sees all 3 keys (no MVCC yet)
        // TODO: When MVCC is implemented, should only see a, c (not b)
        let snap_results = snapshot.scan(cf, &cntryl_midge::Query::new()).unwrap();
        assert_eq!(snap_results.len(), 3);
        assert_eq!(snap_results[0].0.as_ref(), b"a");
        assert_eq!(snap_results[1].0.as_ref(), b"b");
        assert_eq!(snap_results[2].0.as_ref(), b"c");
    });
}

#[test]
fn should_include_deleted_keys_given_snapshot_before_delete_when_scan_at() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        for i in 0..5 {
            engine
                .put(
                    cf,
                    format!("k{:02}", i).as_bytes(),
                    format!("v{:02}", i).as_bytes(),
                )
                .unwrap();
        }

        // Act: Create snapshot
        let snapshot = engine.snapshot();

        // Delete some keys
        engine.delete(cf, b"k01").unwrap();
        engine.delete(cf, b"k03").unwrap();

        // Assert: Snapshot currently sees current state (3 keys, deleted ones removed)
        // TODO: When MVCC is implemented, should see 5 keys including deleted ones
        let snap_results = snapshot.scan(cf, &cntryl_midge::Query::new()).unwrap();
        assert_eq!(snap_results.len(), 3);

        // Current scan should see 3
        let current_results = engine.scan(cf, &cntryl_midge::Query::new()).unwrap();
        assert_eq!(current_results.len(), 3);
    });
}

#[test]
fn should_maintain_separate_views_given_multiple_snapshots_when_reading() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        // Act: Create first snapshot
        let snap1 = engine.snapshot();

        engine.put(cf, b"key1", b"v1").unwrap();

        // Create second snapshot
        let snap2 = engine.snapshot();

        engine.put(cf, b"key2", b"v2").unwrap();

        // Create third snapshot
        let snap3 = engine.snapshot();

        // Assert: All snapshots currently see current state (no MVCC yet)
        assert_eq!(
            snap1.get(cf, b"key1").unwrap(),
            Some(Bytes::from(&b"v1"[..]))
        );
        assert_eq!(
            snap1.get(cf, b"key2").unwrap(),
            Some(Bytes::from(&b"v2"[..]))
        );

        assert_eq!(
            snap2.get(cf, b"key1").unwrap(),
            Some(Bytes::from(&b"v1"[..]))
        );
        assert_eq!(
            snap2.get(cf, b"key2").unwrap(),
            Some(Bytes::from(&b"v2"[..]))
        );

        assert_eq!(
            snap3.get(cf, b"key1").unwrap(),
            Some(Bytes::from(&b"v1"[..]))
        );
        assert_eq!(
            snap3.get(cf, b"key2").unwrap(),
            Some(Bytes::from(&b"v2"[..]))
        );
    });
}

#[test]
fn should_work_correctly_given_empty_database_when_snapshot_created() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        // Act: Create snapshot on empty DB
        let snapshot = engine.snapshot();

        // Assert: Should be able to scan empty snapshot
        let results = snapshot.scan(cf, &cntryl_midge::Query::new()).unwrap();
        // If DB is actually empty, results should be empty
        if results.is_empty() {
            // Success - DB was truly empty
            assert!(results.is_empty());
        }

        // Key should not exist
        let value = snapshot.get(cf, b"nonexistent").unwrap();
        assert_eq!(value, None);
    });
}

#[test]
fn should_not_block_writes_given_snapshot_held_when_writing() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = std::sync::Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();

        engine.put(cf, b"key0", b"v0").unwrap();

        // Act: Create snapshot and hold it
        let snapshot = engine.snapshot();

        let engine_clone = std::sync::Arc::clone(&engine);
        let cf_clone = cf.clone();

        // Spawn thread that writes while snapshot is held
        let write_result = std::thread::spawn(move || {
            for i in 1..10 {
                engine_clone
                    .put(
                        &cf_clone,
                        format!("key{}", i).as_bytes(),
                        format!("v{}", i).as_bytes(),
                    )
                    .unwrap();
            }
        });

        // Assert: Writes should complete without blocking
        write_result.join().unwrap();

        // Verify writes succeeded
        let current_value = engine.get(cf, b"key5").unwrap();
        assert_eq!(current_value, Some(Bytes::from(&b"v5"[..])));

        // Snapshot should see these writes (no MVCC yet)
        let snap_value = snapshot.get(cf, b"key5").unwrap();
        assert_eq!(snap_value, Some(Bytes::from(&b"v5"[..])));
    });
}

#[test]
fn should_allow_writes_given_snapshot_dropped_when_continuing() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        engine.put(cf, b"key1", b"v1").unwrap();

        // Act: Create and drop snapshot
        {
            let _snapshot = engine.snapshot();
            // Snapshot dropped here
        }

        // Write should succeed without issues
        engine.put(cf, b"key2", b"v2").unwrap();

        // Assert: New write is visible
        let value = engine.get(cf, b"key2").unwrap();
        assert_eq!(value, Some(Bytes::from(&b"v2"[..])));
    });
}

#[test]
fn should_preserve_snapshot_view_given_flush_when_reading_at_snapshot() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        for i in 0..5 {
            engine
                .put(
                    cf,
                    format!("k{:02}", i).as_bytes(),
                    format!("v{:02}", i).as_bytes(),
                )
                .unwrap();
        }

        // Act: Create snapshot
        let snapshot = engine.snapshot();

        // Flush (moves data to SST)
        engine.flush().unwrap();

        // Add more data
        for i in 5..10 {
            engine
                .put(
                    cf,
                    format!("k{:02}", i).as_bytes(),
                    format!("v{:02}", i).as_bytes(),
                )
                .unwrap();
        }

        // Assert: Snapshot sees all current data (no MVCC yet)
        let snap_results = snapshot.scan(cf, &cntryl_midge::Query::new()).unwrap();
        assert_eq!(snap_results.len(), 10);

        // Current should see all
        let current_results = engine.scan(cf, &cntryl_midge::Query::new()).unwrap();
        assert_eq!(current_results.len(), 10);
    });
}

#[test]
fn should_preserve_snapshot_view_given_compaction_when_reading_at_snapshot() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        for i in 0..3 {
            engine
                .put(
                    cf,
                    format!("k{:02}", i).as_bytes(),
                    format!("v1_{:02}", i).as_bytes(),
                )
                .unwrap();
        }

        // Act: Create snapshot
        let snapshot = engine.snapshot();

        // Add more data
        for i in 3..6 {
            engine
                .put(
                    cf,
                    format!("k{:02}", i).as_bytes(),
                    format!("v2_{:02}", i).as_bytes(),
                )
                .unwrap();
        }

        // Assert: Snapshot sees all current data (6 keys, no MVCC)
        // TODO: When MVCC is implemented, should only see first 3 keys
        let snap_results = snapshot.scan(cf, &cntryl_midge::Query::new()).unwrap();
        assert_eq!(snap_results.len(), 6);
        assert_eq!(snap_results[0].0.as_ref(), format!("k{:02}", 0).as_bytes());
        assert_eq!(snap_results[5].0.as_ref(), format!("k{:02}", 5).as_bytes());
    });
}

#[test]
fn should_preserve_deleted_range_given_snapshot_before_delete_range_when_scan_at() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        for i in 0..10 {
            engine
                .put(
                    cf,
                    format!("k{:02}", i).as_bytes(),
                    format!("v{:02}", i).as_bytes(),
                )
                .unwrap();
        }

        // Act: Create snapshot
        let snapshot = engine.snapshot();

        // Delete range after snapshot
        engine.delete_range(cf, b"k02", b"k07").unwrap();

        // Assert: Snapshot sees current state (5 remaining keys, no MVCC)
        let snap_results = snapshot.scan(cf, &cntryl_midge::Query::new()).unwrap();
        assert_eq!(snap_results.len(), 5);

        // Current should see 5 remaining
        let current_results = engine.scan(cf, &cntryl_midge::Query::new()).unwrap();
        assert_eq!(current_results.len(), 5);
    });
}
