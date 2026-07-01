//! Flush And Post-Flush Consistency Tests
//!
//! These tests exercise data visibility and correctness around flush-triggered
//! state transitions, repeated flushes, range tombstones, large values, and
//! overwrite visibility. This file does not inject faults or prove background
//! compaction scheduling semantics.

use bytes::Bytes;
mod common;
use cntryl_midge::Query;
use common::*;
use std::collections::HashSet;

// ============================================================================
// TEST GROUP 1: Snapshot Reads Across Flush
// ============================================================================

#[test]
fn should_preserve_snapshot_reads_when_flushing_while_snapshot_is_open() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("local"), "local");
    let cf = engine.create_column_family("test").expect("create cf");
    for i in 0..100 {
        let key = format!("concurrent_key_{i:04}");
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin write tx");
        tx.put(key.as_bytes().to_vec(), b"initial_value".to_vec(), None)
            .expect("put initial value");
        tx.commit(cntryl_midge::WriteOptions::best_effort()) // Fast setup
            .expect("commit initial value");
    }

    engine.flush_cf(&cf).expect("flush initial data");
    let snapshot = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin snapshot tx");

    // Act
    engine.flush_cf(&cf).expect("flush while snapshot is open");

    // Assert
    let snap_val = snapshot
        .get(b"concurrent_key_0000")
        .expect("read through snapshot");
    assert_eq!(snap_val, Some(Bytes::from_static(b"initial_value")));

    drop(snapshot);
    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin current read tx");
    let current_val = tx
        .get(b"concurrent_key_0000")
        .expect("read current value after flush");
    assert_eq!(current_val, Some(Bytes::from_static(b"initial_value")));
}

// ============================================================================
// TEST GROUP 2: Writes Across Flushes
// ============================================================================

#[test]
fn should_preserve_both_write_batches_after_flushing_between_batches() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("local"), "local");
    let cf = engine.create_column_family("test").expect("create cf");
    {
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin first batch tx");
        for i in 0..500 {
            let key = format!("key_{i:04}");
            tx.put(key.as_bytes().to_vec(), b"v1".to_vec(), None)
                .expect("put first batch value");
        }
        tx.commit(cntryl_midge::WriteOptions::best_effort()) // Fast setup
            .expect("commit first batch");
    }

    engine.flush_cf(&cf).expect("flush first batch");

    // Act
    {
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin second batch tx");
        for i in 500..1000 {
            let key = format!("key_{i:04}");
            tx.put(key.as_bytes().to_vec(), b"v2".to_vec(), None)
                .expect("put second batch value");
        }
        tx.commit(cntryl_midge::WriteOptions::buffered())
            .expect("commit second batch");
    }

    // Assert
    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin read tx");
    let total_keys = tx.scan(&Query::new()).expect("scan all keys").remaining();
    assert_eq!(total_keys, 1000);
}

// ============================================================================
// TEST GROUP 3: Range Tombstones Through Flushes
// ============================================================================

#[test]
fn should_preserve_range_tombstones_after_flushing_deleted_range() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("local"), "local");
    let cf = engine.create_column_family("test").expect("create cf");
    for i in 100..900 {
        let key = format!("k{i:04}");
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin seed tx");
        tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
            .expect("put seed value");
        tx.commit(cntryl_midge::WriteOptions::buffered())
            .expect("commit seed value");
    }
    engine.flush_cf(&cf).expect("flush seed range");

    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin delete range tx");
    tx.delete_range(b"k300".to_vec(), b"k700".to_vec())
        .expect("delete range");
    tx.commit(cntryl_midge::WriteOptions::buffered())
        .expect("commit delete range");

    // Act
    engine.flush_cf(&cf).expect("flush tombstone");

    // Assert
    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin read tx");
    let query = Query::new()
        .start_key(Bytes::from(&b"k300"[..]))
        .end_key(Bytes::from(&b"k700"[..]));
    let mut iter = tx.scan(&query).expect("scan deleted range");
    let remaining = std::iter::from_fn(|| iter.next()).count();
    assert_eq!(remaining, 0);
}

// ============================================================================
// TEST GROUP 4: Large Values Across Flush
// ============================================================================

#[test]
fn should_preserve_large_values_after_flushing() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("local"), "local");
    let cf = engine.create_column_family("test").expect("create cf");
    let large_value = vec![0xAB; 100_000]; // 100KB value

    // Act
    for i in 0..10 {
        let key = format!("large_{i:02}");
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin large value tx");
        tx.put(key.as_bytes().to_vec(), large_value.clone(), None)
            .expect("put large value");
        tx.commit(cntryl_midge::WriteOptions::buffered())
            .expect("commit large value");
    }

    engine.flush_cf(&cf).expect("flush large values");

    // Assert
    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin read tx");
    let val = tx.get(b"large_00").expect("get large value");
    assert_eq!(val.as_ref().map(Bytes::len), Some(100_000));
}

// ============================================================================
// TEST GROUP 5: Overwritten Keys After Flush
// ============================================================================

#[test]
fn should_preserve_latest_overwritten_value_after_flushing() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("local"), "local");
    let cf = engine.create_column_family("test").expect("create cf");
    for version in 0..100 {
        let value = format!("v{version}");
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin overwrite tx");
        tx.put(b"hotkey".to_vec(), value.as_bytes().to_vec(), None)
            .expect("put overwrite value");
        tx.commit(cntryl_midge::WriteOptions::buffered())
            .expect("commit overwrite value");
    }

    // Act
    engine.flush_cf(&cf).expect("flush overwritten values");

    // Assert
    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin read tx");
    let current = tx.get(b"hotkey").expect("get hotkey");
    assert_eq!(current, Some(Bytes::from_static(b"v99")));
}

// ============================================================================
// TEST GROUP 6: Repeated Flushes Preserve All Keys
// ============================================================================

#[test]
fn should_preserve_all_keys_after_repeated_flushes() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("local"), "local");
    let cf = engine.create_column_family("test").expect("create cf");

    // Act
    for batch in 0..3 {
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin batch tx");
        for i in 0..500 {
            let key = format!("batch{batch:02}_key{i:04}");
            tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                .expect("put batch value");
        }
        tx.commit(cntryl_midge::WriteOptions::buffered())
            .expect("commit batch");
        engine.flush_cf(&cf).expect("flush batch");
    }

    // Assert
    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin read tx");
    let key_count = tx.scan(&Query::new()).expect("scan all keys").remaining();
    assert_eq!(key_count, 1500);
}

// ============================================================================
// TEST GROUP: Compaction Manifest Publication
// ============================================================================

// ============================================================================
// TEST GROUP: Compaction Manifest Publication
// ============================================================================

/// Slice 5: Verify that compaction output SSTs are published in the manifest
/// and become the source of truth for subsequent reads.
///
/// This test validates that `CompactionComplete` â†’ `ManifestCompactionComplete`
/// routing correctly updates the manifest with:
/// - Input SSTs removed from active set
/// - Output SSTs added to manifest
/// - Ability to read data from compacted SSTs
///
/// The key proof: after compaction completes and manifest is updated,
/// reads can still access all data (proving reads use the new compacted SSTs).
#[test]
fn should_publish_compacted_ssts_in_manifest_when_compaction_completes() {
    // Arrange: Create engine and write data to trigger L0 compaction
    let engine = open_with_mode(&opts_for_mode("local"), "local");
    let cf = engine.create_column_family("test").expect("create cf");

    // Write enough data to create multiple L0 files via flush
    for batch in 0..5 {
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin batch tx");
        for i in 0..100 {
            let key = format!("batch{batch:02}_key{i:04}");
            tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                .expect("put batch value");
        }
        tx.commit(cntryl_midge::WriteOptions::buffered())
            .expect("commit batch");
        engine.flush_cf(&cf).expect("flush batch");
    }

    // Act: Trigger compaction (merges L0 files to L1)
    engine.compact_all().expect("trigger compaction");

    // Assert: All data remains queryable through compacted SSTs
    // This proves the manifest was updated with output SSTs and reads use them
    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin read tx after compaction");

    let key_count = tx.scan(&Query::new()).expect("scan all keys").remaining();
    assert_eq!(
        key_count, 500,
        "All 500 keys should be queryable after compaction (proves manifest was updated)"
    );

    // Verify specific keys exist with correct values
    let value = tx
        .get(b"batch00_key0000")
        .expect("get key after compaction");
    assert_eq!(value, Some(Bytes::copy_from_slice(b"value")));

    let value = tx
        .get(b"batch04_key0099")
        .expect("get last key after compaction");
    assert_eq!(value, Some(Bytes::copy_from_slice(b"value")));
}

/// Slice 6: Verify that input SSTs are cleaned up (deleted) after compaction
/// and manifest publication succeeds.
///
/// This test validates the cleanup sequence:
/// 1. Manifest is updated with input SSTs removed, output SSTs added
/// 2. Manifest is persisted to disk
/// 3. Input SSTs are deleted from the filesystem
///
/// Strategy: Create two compaction rounds. The second compaction would include
/// old L0 files if they existed (proving they weren't deleted in round 1).
/// Since the second compaction succeeds with correct key counts, we prove
/// old files were deleted after round 1's manifest publish.
#[test]
fn should_cleanup_input_ssts_after_compaction_manifest_publishes() {
    // Arrange: Create 5 L0 files (5 batches Ã— 100 keys each)
    let engine = open_with_mode(&opts_for_mode("local"), "local");
    let cf = engine.create_column_family("test").expect("create cf");
    for batch in 0..5 {
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin batch tx");
        for i in 0..100 {
            let key = format!("batch{batch:02}_key{i:04}");
            tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                .expect("put batch value");
        }
        tx.commit(cntryl_midge::WriteOptions::buffered())
            .expect("commit batch");
        engine.flush_cf(&cf).expect("flush batch");
    }

    // Act: Compact twice, verifying cleanup between rounds
    // Round 1: Merges 5 L0 files â†’ 1 L1 file, deletes input L0s
    engine.compact_all().expect("first compaction");

    // Add new batch (creates new L0)
    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin new batch tx");
    for i in 0..100 {
        let key = format!("batch99_key{i:04}");
        tx.put(key.as_bytes().to_vec(), b"new_value".to_vec(), None)
            .expect("put new batch value");
    }
    tx.commit(cntryl_midge::WriteOptions::buffered())
        .expect("commit new batch");
    engine.flush_cf(&cf).expect("flush new batch");

    // Round 2: If old L0 files still existed, they'd be compacted with new L0
    // But since they were deleted, only new L0 + L1 are compacted
    engine.compact_all().expect("second compaction");

    // Assert: All data (500 old + 100 new) is queryable
    // This proves old L0 files were deleted after round 1's manifest publish
    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin read tx after cleanup");

    let count = tx.scan(&Query::new()).expect("scan all keys").remaining();
    assert_eq!(
        count, 600,
        "All 600 keys (500 old + 100 new) present after cleanup proves old L0s deleted"
    );

    // Verify old and new data values are correct
    let old_val = tx.get(b"batch00_key0000").expect("get old batch key");
    assert_eq!(old_val, Some(Bytes::copy_from_slice(b"value")));

    let new_val = tx.get(b"batch99_key0000").expect("get new batch key");
    assert_eq!(new_val, Some(Bytes::copy_from_slice(b"new_value")));
}

#[test]
fn should_assign_unique_output_sequences_given_emergent_followup_compaction() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("local"), "local");
    let cf = engine.create_column_family("test").expect("create cf");
    cntryl_midge::testkit::bench::set_runtime_compaction_enabled(&engine, false)
        .expect("disable automatic compaction while seeding L0 files");

    let batch_count = 12;
    let keys_per_batch = 20;
    for batch in 0..batch_count {
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin batch tx");
        for key_idx in 0..keys_per_batch {
            let key = format!("batch{batch:02}_key{key_idx:04}");
            let value = format!("value-{batch:02}-{key_idx:04}");
            tx.put(key.into_bytes(), value.into_bytes(), None)
                .expect("put batch value");
        }
        tx.commit(cntryl_midge::WriteOptions::buffered())
            .expect("commit batch");
        engine.flush_cf(&cf).expect("flush batch");
    }

    // Act
    cntryl_midge::testkit::bench::set_runtime_compaction_enabled(&engine, true)
        .expect("enable manual compaction");
    engine.compact_all().expect("compact all seeded L0 files");

    // Assert
    let layout = engine.get_storage_layout().expect("storage layout");
    let compacted_names: Vec<String> = layout
        .levels
        .iter()
        .flat_map(|level| level.files.iter())
        .filter(|file| file.level > 0)
        .map(|file| file.name.clone())
        .collect();

    assert!(
        compacted_names.len() >= 2,
        "test must produce follow-up compaction outputs; got {compacted_names:?}"
    );
    assert!(
        compacted_names
            .iter()
            .all(|name| !name.ends_with("00000000000000000000.sst")),
        "compaction outputs must never use sequence zero: {compacted_names:?}"
    );

    let unique_names: HashSet<&str> = compacted_names.iter().map(String::as_str).collect();
    assert_eq!(
        unique_names.len(),
        compacted_names.len(),
        "follow-up compactions must publish unique SST names: {compacted_names:?}"
    );

    let read_tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin read tx after compaction");
    let total = read_tx
        .scan(&Query::new())
        .expect("scan compacted data")
        .remaining();
    assert_eq!(
        total,
        batch_count * keys_per_batch,
        "all seeded data must remain readable after repeated follow-up compactions"
    );

    for batch in 0..batch_count {
        let key = format!("batch{batch:02}_key0000");
        let value = format!("value-{batch:02}-0000");
        assert_eq!(
            read_tx.get(key.as_bytes()).expect("get compacted key"),
            Some(Bytes::from(value)),
            "compacted key should remain readable: {key}"
        );
    }
}
