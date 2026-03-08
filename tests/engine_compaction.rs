//! Flush And Post-Flush Consistency Tests
//!
//! These tests exercise data visibility and correctness around flush-triggered
//! state transitions, repeated flushes, range tombstones, large values, and
//! overwrite visibility. This file does not inject faults or prove background
//! compaction scheduling semantics.

use bytes::Bytes;
use cntryl_midge::testkit::*;
use cntryl_midge::Query;

// ============================================================================
// TEST GROUP 1: Snapshot Reads Across Flush
// ============================================================================

#[test]
fn should_preserve_snapshot_reads_when_flushing_while_snapshot_is_open() {
    // Arrange
    let engine = open_with_mode(opts_for_mode("local"), "local");
    let cf = engine.create_column_family("test").expect("create cf");
    for i in 0..100 {
        let key = format!("concurrent_key_{:04}", i);
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin write tx");
        tx.put(key.as_bytes().to_vec(), b"initial_value".to_vec(), None)
            .expect("put initial value");
        engine
            .commit(tx, cntryl_midge::WriteOptions::best_effort()) // Fast setup
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
    let engine = open_with_mode(opts_for_mode("local"), "local");
    let cf = engine.create_column_family("test").expect("create cf");
    {
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin first batch tx");
        for i in 0..500 {
            let key = format!("key_{:04}", i);
            tx.put(key.as_bytes().to_vec(), b"v1".to_vec(), None)
                .expect("put first batch value");
        }
        engine
            .commit(tx, cntryl_midge::WriteOptions::best_effort()) // Fast setup
            .expect("commit first batch");
    }

    engine.flush_cf(&cf).expect("flush first batch");

    // Act
    {
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin second batch tx");
        for i in 500..1000 {
            let key = format!("key_{:04}", i);
            tx.put(key.as_bytes().to_vec(), b"v2".to_vec(), None)
                .expect("put second batch value");
        }
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
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
    let engine = open_with_mode(opts_for_mode("local"), "local");
    let cf = engine.create_column_family("test").expect("create cf");
    let cf_id = cf.id();
    for i in 100..900 {
        let key = format!("k{:04}", i);
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin seed tx");
        tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
            .expect("put seed value");
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .expect("commit seed value");
    }
    engine.flush_cf(&cf).expect("flush seed range");

    let mut txn = engine
        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin delete range tx");
    txn.delete_range(b"k300".to_vec(), b"k700".to_vec())
        .expect("delete range");
    engine
        .commit(txn, cntryl_midge::WriteOptions::buffered())
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
    let engine = open_with_mode(opts_for_mode("local"), "local");
    let cf = engine.create_column_family("test").expect("create cf");
    let large_value = vec![0xAB; 100_000]; // 100KB value

    // Act
    for i in 0..10 {
        let key = format!("large_{:02}", i);
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin large value tx");
        tx.put(key.as_bytes().to_vec(), large_value.clone(), None)
            .expect("put large value");
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
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
    let engine = open_with_mode(opts_for_mode("local"), "local");
    let cf = engine.create_column_family("test").expect("create cf");
    for version in 0..100 {
        let value = format!("v{}", version);
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin overwrite tx");
        tx.put(b"hotkey".to_vec(), value.as_bytes().to_vec(), None)
            .expect("put overwrite value");
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
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
    let engine = open_with_mode(opts_for_mode("local"), "local");
    let cf = engine.create_column_family("test").expect("create cf");

    // Act
    for batch in 0..3 {
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin batch tx");
        for i in 0..500 {
            let key = format!("batch{:02}_key{:04}", batch, i);
            tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                .expect("put batch value");
        }
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
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
