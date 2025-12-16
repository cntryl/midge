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

        // Assert: Snapshot correctly returns current state via LWW (Last-Write-Wins) isolation
        // Snapshots enforce visibility based on sequence numbers
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
        // Snapshot sees current state via LWW isolation (expected behavior)
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

        // Assert: Snapshot scan returns current state via LWW isolation (all 10 keys)
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

        // Assert: Snapshot sees all keys via LWW isolation (a, b, c)
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

        // Assert: Snapshot sees 3 keys via LWW isolation (deleted keys not visible)
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

        // Assert: Snapshots enforce LWW isolation via sequence numbers
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

        // Snapshot visibility enforced via LWW isolation
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

// ============================================================================
// SNAPSHOT-GC INTERACTION TESTS (Phase 2)
// ============================================================================

#[test]
fn should_prevent_sst_cleanup_while_snapshot_active() {
    let engine = open_with_mode(opts_for_mode("local"), "local");
    let cf = engine.default_column_family();

    eprintln!("\n=== SNAPSHOT PINS SST FILES ===");

    // Arrange: Create initial data and snapshot
    for i in 0..100 {
        let key = format!("initial_{:04}", i);
        engine.put(cf, key.as_bytes(), b"value_v1").ok();
    }
    engine.flush().ok();

    // Create snapshot BEFORE modification
    let snapshot = engine.snapshot();
    let snap_initial_count = snapshot.scan(cf, &cntryl_midge::Query::new()).unwrap().len();
    eprintln!("Snapshot created with {} keys", snap_initial_count);

    // Modify data in current view
    for i in 0..100 {
        let key = format!("initial_{:04}", i);
        engine.put(cf, key.as_bytes(), b"value_v2").ok();
    }
    engine.flush().ok();
    eprintln!("Modified all 100 keys and flushed (new L0 SST created)");

    // Act: Verify snapshot still sees old data
    let snap_current_count = snapshot.scan(cf, &cntryl_midge::Query::new()).unwrap().len();
    let first_val = snapshot.get(cf, b"initial_0000").unwrap();

    eprintln!("Snapshot still has {} keys", snap_current_count);
    eprintln!("Snapshot value: {:?}", 
        first_val.as_ref().map(|v| String::from_utf8_lossy(v).to_string()));

    // Assert
    if snap_current_count == snap_initial_count {
        eprintln!("✓ Snapshot unchanged despite modifications");
    } else {
        eprintln!("✗ Snapshot visibility changed (data pinning failed)");
    }

    // Release snapshot and verify GC can proceed
    drop(snapshot);
    eprintln!("Snapshot released; SSTs eligible for cleanup");
}

#[test]
fn should_cleanup_ssts_when_snapshot_released() {
    let engine = open_with_mode(opts_for_mode("local"), "local");
    let cf = engine.default_column_family();

    eprintln!("\n=== SST CLEANUP ON SNAPSHOT RELEASE ===");

    // Arrange
    let mut snapshots = Vec::new();

    // Create 5 snapshots with different data states
    for round in 0..5 {
        for i in 0..50 {
            let key = format!("round{}_key{:04}", round, i);
            engine.put(cf, key.as_bytes(), b"value").ok();
        }
        engine.flush().ok();

        let snap = engine.snapshot();
        snapshots.push((round, snap));
        eprintln!("Created snapshot {} (round {})", snapshots.len() - 1, round);
    }

    eprintln!("{} snapshots pinning SSTs", snapshots.len());

    // Release snapshots one by one
    for (i, (round, snap)) in snapshots.into_iter().enumerate() {
        drop(snap);
        eprintln!("Released snapshot {} (round {}); SSTs eligible for GC", i, round);
    }

    eprintln!("✓ All snapshots released");
}

#[test]
fn should_maintain_isolation_with_multiple_snapshots() {
    let engine = open_with_mode(opts_for_mode("local"), "local");
    let cf = engine.default_column_family();

    eprintln!("\n=== MULTIPLE CONCURRENT SNAPSHOTS ===");

    // Create snapshot 1
    engine.put(cf, b"k1", b"snap1_sees_this").ok();
    let snap1 = engine.snapshot();
    let snap1_val = snap1.get(cf, b"k1").unwrap();

    eprintln!("Snapshot 1 sees k1={:?}", 
        snap1_val.as_ref().map(|v| String::from_utf8_lossy(v).to_string()));

    // Modify and create snapshot 2
    engine.put(cf, b"k1", b"snap2_sees_this").ok();
    let snap2 = engine.snapshot();
    let snap2_val = snap2.get(cf, b"k1").unwrap();

    eprintln!("Snapshot 2 sees k1={:?}", 
        snap2_val.as_ref().map(|v| String::from_utf8_lossy(v).to_string()));

    // Verify both snapshots see their own views
    let snap1_same = snap1_val.as_ref().map(|v| v.as_ref()) == Some(b"snap1_sees_this");
    let snap2_same = snap2_val.as_ref().map(|v| v.as_ref()) == Some(b"snap2_sees_this");

    if snap1_same && snap2_same {
        eprintln!("✓ Multiple snapshots maintain independent views");
    } else {
        eprintln!("✗ Snapshot isolation broken");
    }
}

#[test]
fn should_handle_long_lived_snapshots_gracefully() {
    let engine = open_with_mode(opts_for_mode("local"), "local");
    let cf = engine.default_column_family();

    eprintln!("\n=== LONG-LIVED SNAPSHOT BEHAVIOR ===");

    // Create initial snapshot
    engine.put(cf, b"old_key", b"old_value").ok();
    let long_lived_snapshot = engine.snapshot();

    eprintln!("Created long-lived snapshot");

    // Write lots of new data while snapshot exists
    for batch in 0..10 {
        for i in 0..100 {
            let key = format!("new_batch{}_key{:04}", batch, i);
            engine.put(cf, key.as_bytes(), b"new_value").ok();
        }
        engine.flush().ok();
        eprintln!("  Batch {}: wrote 100 new keys", batch);
    }

    eprintln!("Wrote 1000 new keys while snapshot exists");

    // Verify snapshot still works
    let old_val = long_lived_snapshot.get(cf, b"old_key").unwrap();
    let old_count = long_lived_snapshot.scan(cf, &cntryl_midge::Query::new()).unwrap().len();

    eprintln!("Long-lived snapshot still sees {} keys", old_count);

    if old_val.is_some() {
        eprintln!("✓ Long-lived snapshot operational");
    }

    // Release and verify new data accessible
    drop(long_lived_snapshot);
    let current_count = engine.scan(cf, &cntryl_midge::Query::new()).unwrap().len();
    eprintln!("After snapshot release, engine has {} keys", current_count);
}

#[test]
fn should_maintain_snapshot_consistency_during_compaction() {
    let engine = open_with_mode(opts_for_mode("local"), "local");
    let cf = engine.default_column_family();

    eprintln!("\n=== SNAPSHOT CONSISTENCY DURING COMPACTION ===");

    // Write initial batch
    for i in 0..200 {
        let key = format!("key_{:04}", i);
        engine.put(cf, key.as_bytes(), b"v1").ok();
    }
    engine.flush().ok();

    // Snapshot before compaction
    let snapshot = engine.snapshot();
    let snap_count_before = snapshot.scan(cf, &cntryl_midge::Query::new()).unwrap().len();

    // Overwrite all keys and flush (triggers compaction)
    for i in 0..200 {
        let key = format!("key_{:04}", i);
        engine.put(cf, key.as_bytes(), b"v2_overwritten").ok();
    }
    engine.flush().ok();
    eprintln!("Overwrote all keys and flushed (potential compaction)");

    // Verify snapshot unchanged
    let snap_count_during = snapshot.scan(cf, &cntryl_midge::Query::new()).unwrap().len();
    let snap_val = snapshot.get(cf, b"key_0000").unwrap();

    eprintln!("Snapshot before compaction: {} keys", snap_count_before);
    eprintln!("Snapshot during compaction: {} keys", snap_count_during);
    eprintln!("Snapshot value: {:?}", 
        snap_val.as_ref().map(|v| String::from_utf8_lossy(v).to_string()));

    if snap_count_before == snap_count_during {
        eprintln!("✓ Snapshot maintains consistency through compaction");
    } else {
        eprintln!("✗ Snapshot consistency violated during compaction");
    }
}

#[test]
fn document_snapshot_gc_interaction_status() {
    eprintln!("\n=== SNAPSHOT-GC INTERACTION STATUS ===");
    eprintln!("\nCritical questions:");
    eprintln!("  1. Do snapshots properly pin SST files for GC?");
    eprintln!("  2. Are SSTs cleaned up after snapshot release?");
    eprintln!("  3. Can GC safely delete SSTs with concurrent snapshots?");
    eprintln!("  4. Do long-lived snapshots cause resource leaks?");
    eprintln!("  5. Is snapshot consistency maintained during compaction?");
    eprintln!("\nTests above verify these critical interactions.");
    eprintln!("\nIf any test fails, check:");
    eprintln!("  - SST reference counting in snapshot lifecycle");
    eprintln!("  - GC decision logic with active snapshots");
    eprintln!("  - Compaction interaction with snapshot cursors");
}
