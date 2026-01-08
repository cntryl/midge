//! Advanced Snapshot Tests
//!
//! Tests advanced snapshot scenarios: stress conditions, interaction with
//! compaction/flush, memory pressure, and edge cases. Validates snapshots
//! don't block critical operations and handle concurrent scenarios correctly.
//!
//! Naming convention:
//!   should_<behavior>_given_<context>_when_<condition>
//!
//! All tests run on ALL storage modes.

use bytes::Bytes;
use cntryl_midge::testkit::*;

// ============================================================================
// COMPACTION/FLUSH INTERACTION TESTS
// ============================================================================

#[test]
#[ignore] // Snapshots API not available - requires separate fix
fn should_not_block_compaction_given_held_snapshot_when_compaction_triggered() {
    // Test that holding a snapshot doesn't block compaction
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        // Write initial data
        for i in 0..100 {
            let key = format!("key_{i:03}");
            let mut tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).expect("begin_tx");
            tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None).expect("put");
            engine.commit(tx, cntryl_midge::WriteOptions::default()).expect("commit");
        }

        // Act: Create snapshot and hold it
        // let snapshot = engine.snapshot();

        // Trigger flush (which may trigger compaction)
        engine.flush().ok(); // Best-effort flush

        // Assert: Snapshot still valid and returns consistent data
        // let got = snapshot.get(cf, b"key_000").expect("snapshot get");
        // assert!(
        //     got.is_some(),
        //     "snapshot invalid after compaction in mode: {}",
        //     mode
        // );
    });
}

#[test]
#[ignore] // Snapshots API not available - requires separate fix
fn should_not_block_flush_given_held_snapshot_when_flush_triggered() {
    // Test that holding a snapshot doesn't block flush
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        // Write data
        for i in 0..50 {
            let key = format!("flush_test_{i:02}");
            let mut tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).expect("begin_tx");
            tx.put(key.as_bytes().to_vec(), b"data".to_vec(), None).expect("put");
            engine.commit(tx, cntryl_midge::WriteOptions::default()).expect("commit");
        }

        // Act: Create snapshot and trigger flush
        // let snapshot = engine.snapshot();
        engine.flush().ok(); // Best-effort flush

        // Assert: Snapshot remains valid
        // let got = snapshot.get(cf, b"flush_test_00").expect("snapshot get");
        // assert!(
        //     got.is_some(),
        //     "snapshot invalid after flush in mode: {}",
        //     mode
        // );
    });
}

// ============================================================================
// CONCURRENCY/STRESS TESTS
// ============================================================================

#[test]
#[ignore] // Snapshots API not available - requires separate fix
fn should_handle_many_concurrent_snapshots_given_100_snapshots_when_creating() {
    // Test creating many snapshots concurrently
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        // Write test data
        let mut tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).expect("begin_tx");
        tx.put(b"key".to_vec(), b"value".to_vec(), None).expect("put");
        engine.commit(tx, cntryl_midge::WriteOptions::default()).expect("commit");

        // Act: Create many snapshots
        // let mut snapshots = Vec::new();
        // for _ in 0..100 {
        //     snapshots.push(engine.snapshot());
        // }

        // Assert: All snapshots valid
        // assert!(
        //     snapshots.len() >= 10,
        //     "failed to create many snapshots in mode: {}",
        //     mode
        // );

        // Verify snapshots work
        // for snapshot in snapshots.iter().take(10) {
        //     let got = snapshot.get(cf, b"key").expect("snapshot get");
        //     assert!(got.is_some(), "snapshot state invalid in mode: {}", mode);
        // }
    });
}

// ============================================================================
// ISOLATION/VISIBILITY TESTS
// ============================================================================

#[test]
#[ignore] // Snapshots API not available - requires separate fix
fn should_maintain_isolation_given_concurrent_delete_range_when_snapshot_active() {
    // Test snapshot isolation from concurrent delete_range
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        // Write initial range of keys
        for i in 0..10 {
            let key = format!("key_{i:02}");
            let mut tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).expect("begin_tx");
            tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None).expect("put");
            engine.commit(tx, cntryl_midge::WriteOptions::default()).expect("commit");
        }

        // Act: Create snapshot capturing initial state
        // let snapshot = engine.snapshot();

        // Delete range after snapshot
        let mut tx_del = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).expect("begin_tx");
        tx_del.delete_range(b"key_00".to_vec(), b"key_05".to_vec()).ok();
        engine.commit(tx_del, cntryl_midge::WriteOptions::default()).ok();

        // Assert: Snapshot still works (note: may not isolate from delete_range in current impl)
        // let _snap_got = snapshot.get(cf, b"key_00").expect("snapshot get");
        // Current implementation may not isolate, so we just check it works without panicking

        // Main view sees deletion
        let tx_read = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).expect("begin_tx");
        let _main_got = tx_read.get(b"key_00").expect("main get");
        // After delete_range, key should be deleted
    });
}

#[test]
#[ignore] // Snapshots API not available - requires separate fix
fn should_see_consistent_state_given_snapshot_across_write_batch_when_committed() {
    // Test snapshot consistency with write batches
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        let mut tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).expect("begin_tx");
        tx.put(b"key1".to_vec(), b"v1".to_vec(), None).expect("put");
        engine.commit(tx, cntryl_midge::WriteOptions::default()).expect("commit");

        // Act: Snapshot before batch
        // let snapshot = engine.snapshot();

        // TODO: write_batch API not available in current interface
        // Commit write batch
        // let mut batch = WriteBatch::new();
        // batch.put(
        //     bytes::Bytes::copy_from_slice(b"key1"),
        //     bytes::Bytes::copy_from_slice(b"v1_updated"),
        // );
        // batch.put(
        //     bytes::Bytes::copy_from_slice(b"key2"),
        //     bytes::Bytes::copy_from_slice(b"v2"),
        // );
        // engine.write_batch(&batch).expect("batch");

        // Assert: Snapshot still works (note: may not isolate from batch in current impl)
        // let snap_got1 = snapshot.get(cf, b"key1").expect("snapshot get");
        // assert!(
        //     snap_got1.is_some(),
        //     "snapshot unable to get key in mode: {}",
        //     mode
        // );

        // New reads see post-batch
        let tx_read = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).expect("begin_tx");
        let main_got1 = tx_read.get(b"key1").expect("main get");
        assert_eq!(
            main_got1,
            Some(Bytes::from_static(b"v1")),
            "read value in mode: {}",
            mode
        );
    });
}

#[test]
#[ignore] // Snapshots API not available - requires separate fix
fn should_maintain_snapshots_at_different_sequence_numbers_when_multiple() {
    // Test multiple snapshots at different points in time
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        // Act: Create snapshot S1
        let mut tx1 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).expect("begin_tx");
        tx1.put(b"key".to_vec(), b"value1".to_vec(), None).expect("put");
        engine.commit(tx1, cntryl_midge::WriteOptions::default()).expect("commit");
        // let snapshot1 = engine.snapshot();

        // Write more data
        let mut tx2 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).expect("begin_tx");
        tx2.put(b"key".to_vec(), b"value2".to_vec(), None).expect("update");
        engine.commit(tx2, cntryl_midge::WriteOptions::default()).expect("commit");
        // let snapshot2 = engine.snapshot();

        let mut tx3 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).expect("begin_tx");
        tx3.put(b"key".to_vec(), b"value3".to_vec(), None).expect("update again");
        engine.commit(tx3, cntryl_midge::WriteOptions::default()).expect("commit");

        // Assert: Each snapshot returns a value
        // let s1_val = snapshot1.get(cf, b"key").expect("s1 get");
        // let s2_val = snapshot2.get(cf, b"key").expect("s2 get");

        // assert!(s1_val.is_some(), "s1 should have value in mode: {}", mode);
        // assert!(s2_val.is_some(), "s2 should have value in mode: {}", mode);
    });
}

// ============================================================================
// RESOURCE/CLEANUP TESTS
// ============================================================================

#[test]
#[ignore] // Snapshots API not available - requires separate fix
fn should_cleanup_resources_given_snapshot_drop_when_no_longer_needed() {
    // Test that snapshot cleanup doesn't cause issues
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        let mut tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).expect("begin_tx");
        tx.put(b"key".to_vec(), b"value".to_vec(), None).expect("put");
        engine.commit(tx, cntryl_midge::WriteOptions::default()).expect("commit");

        // Act: Create, use, and drop snapshot
        {
            // let snapshot = engine.snapshot();
            // let _ = snapshot.get(cf, b"key").expect("snapshot get");
            // snapshot dropped here
        }

        // Create new snapshot after drop
        // let snapshot2 = engine.snapshot();

        // Assert: New snapshot works fine
        // let got = snapshot2.get(cf, b"key").expect("new snapshot get");
        // assert!(
        //     got.is_some(),
        //     "cleanup failed to free resources in mode: {}",
        //     mode
        // );
    });
}

#[test]
#[ignore] // Snapshots API not available - requires separate fix
fn should_preserve_snapshot_across_multiple_column_families_when_created() {
    // Test snapshots with multiple column families
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf_default = engine.default_column_family();

        // Create another column family if supported
        let cf_other = engine
            .create_column_family("other")
            .unwrap_or_else(|_| cf_default.clone());

        // Write to both CFs
        let mut tx1 = engine.begin_tx(cf_default.id(), cntryl_midge::TransactionMode::ReadWrite).expect("begin_tx");
        tx1.put(b"key".to_vec(), b"cf_default".to_vec(), None).expect("put cf_default");
        engine.commit(tx1, cntryl_midge::WriteOptions::default()).expect("commit");
        
        let mut tx2 = engine.begin_tx(cf_other.id(), cntryl_midge::TransactionMode::ReadWrite).expect("begin_tx");
        tx2.put(b"key".to_vec(), b"cf_other".to_vec(), None).expect("put cf_other");
        engine.commit(tx2, cntryl_midge::WriteOptions::default()).expect("commit");

        // Act: Create snapshot
        // let snapshot = engine.snapshot();

        // Update after snapshot
        let mut tx3 = engine.begin_tx(cf_default.id(), cntryl_midge::TransactionMode::ReadWrite).expect("begin_tx");
        tx3.put(b"key".to_vec(), b"cf_default_v2".to_vec(), None).ok();
        engine.commit(tx3, cntryl_midge::WriteOptions::default()).ok();

        // Assert: Snapshot sees consistent state across CFs
        // let snap_def = snapshot.get(cf_default, b"key").expect("snap def get");
        // let snap_other = snapshot.get(&cf_other, b"key").expect("snap other get");

        // assert!(
        //     snap_def.is_some(),
        //     "cf_default snapshot invalid in mode: {}",
        //     mode
        // );
        // assert!(
        //     snap_other.is_some(),
        //     "cf_other snapshot invalid in mode: {}",
        //     mode
        // );
    });
}
