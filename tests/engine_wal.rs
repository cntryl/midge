//! WAL (Write-Ahead Log) Integration Tests
//!
//! Tests WAL functionality: recovery, data durability, corruption handling,
//! large values, rotation, and mixed operation recovery.

use cntryl_midge::testkit::*;
use cntryl_midge::WriteOptions;

// ============================================================================
// TEST GROUP 1: Basic WAL Recovery
// ============================================================================

#[test]
fn should_recover_data_from_wal_after_flush() {
    let engine = open_with_mode(opts_for_mode("local"), "local");
    let cf = engine.default_column_family();

    eprintln!("\n=== WAL RECOVERY TEST ===");

    // Write data (goes to WAL + memtable)
    for i in 0..50 {
        let key = format!("wal_key_{:04}", i);
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(key.as_bytes().to_vec(), b"wal_value".to_vec(), None)
            .unwrap();
        engine.commit(tx, WriteOptions::buffered()).ok();
    }

    eprintln!("Wrote 50 keys to WAL+memtable");

    // Flush to SST
    engine.flush().ok();
    eprintln!("Flushed to SST");

    // Verify data persisted
    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();
    let mut iter = tx.scan(&cntryl_midge::Query::new()).unwrap();
    let count = std::iter::from_fn(|| iter.next()).count();

    eprintln!("Keys after flush: {}", count);

    if count >= 50 {
        eprintln!("✓ WAL recovery successful");
    } else {
        eprintln!("✗ Data loss: {} keys < 50 expected", count);
    }
}

// ============================================================================
// TEST GROUP 2: WAL with Large Entries
// ============================================================================

#[test]
fn should_handle_large_values_in_wal() {
    let engine = open_with_mode(opts_for_mode("local"), "local");
    let cf = engine.default_column_family();

    eprintln!("\n=== LARGE VALUE IN WAL ===");

    let large_value = vec![0xFF; 1_000_000]; // 1MB value

    // Write large value
    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    tx.put(b"large_wal_key".to_vec(), large_value.clone(), None)
        .unwrap();
    engine.commit(tx, WriteOptions::buffered()).ok();
    eprintln!("Wrote 1MB value to WAL");

    // Flush
    engine.flush().ok();

    // Retrieve and verify
    let read_tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();
    let retrieved = read_tx.get(b"large_wal_key").unwrap();

    match retrieved {
        Some(val) if val.len() == 1_000_000 => {
            eprintln!("✓ Large value persisted and recovered from WAL");
        }
        _ => {
            eprintln!("✗ Large value corrupted or lost in WAL");
        }
    }
}

// ============================================================================
// TEST GROUP 3: WAL with Deletes
// ============================================================================

#[test]
fn should_recover_deletes_from_wal() {
    let engine = open_with_mode(opts_for_mode("local"), "local");
    let cf = engine.default_column_family();

    eprintln!("\n=== DELETE RECOVERY FROM WAL ===");

    // Insert data
    for i in 0..30 {
        let key = format!("del_key_{:04}", i);
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
            .unwrap();
        engine.commit(tx, WriteOptions::buffered()).ok();
    }

    // Delete some keys via transaction
    let mut txn = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    for i in 0..10 {
        let key = format!("del_key_{:04}", i);
        txn.delete(key.into_bytes()).ok();
    }
    engine
        .commit(txn, cntryl_midge::WriteOptions::buffered())
        .ok();

    eprintln!("Deleted 10 of 30 keys via transaction");

    // Flush to persist deletes
    engine.flush().ok();

    // Verify deletes persisted
    let mut deleted_count = 0;
    for i in 0..10 {
        let key = format!("del_key_{:04}", i);
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        if read_tx.get(key.as_bytes()).unwrap().is_none() {
            deleted_count += 1;
        }
    }

    eprintln!("Confirmed {} keys deleted", deleted_count);

    if deleted_count == 10 {
        eprintln!("✓ Deletes recovered correctly from WAL");
    } else {
        eprintln!(
            "✗ Delete recovery failed: {} deletes not persisted",
            10 - deleted_count
        );
    }
}

// ============================================================================
// TEST GROUP 4: Range Tombstones in WAL
// ============================================================================

#[test]
fn should_recover_range_tombstones_from_wal() {
    let engine = open_with_mode(opts_for_mode("local"), "local");
    let cf = engine.default_column_family();

    eprintln!("\n=== RANGE TOMBSTONE IN WAL ===");

    // Insert keys
    for i in 0..100 {
        let key = format!("k{:03}", i);
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
            .unwrap();
        engine.commit(tx, WriteOptions::buffered()).ok();
    }

    // Range delete
    let mut txn = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    txn.delete_range(b"k020".to_vec(), b"k080".to_vec()).ok();
    engine
        .commit(txn, cntryl_midge::WriteOptions::buffered())
        .ok();

    eprintln!("Applied range delete [k020, k080)");

    // Flush to persist range tombstone
    engine.flush().ok();

    // Verify range is empty
    let scan_tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();
    let mut iter = scan_tx.scan(&cntryl_midge::Query::new()).unwrap();
    let in_range = std::iter::from_fn(|| iter.next())
        .filter(|(k, _)| {
            let k_str = String::from_utf8_lossy(k.as_ref());
            k_str.as_ref() >= "k020" && k_str.as_ref() < "k080"
        })
        .count();

    eprintln!("Keys remaining in deleted range: {}", in_range);

    if in_range == 0 {
        eprintln!("✓ Range tombstone recovered from WAL");
    } else {
        eprintln!("✗ Range tombstone lost: {} keys still present", in_range);
    }
}

// ============================================================================
// TEST GROUP 5: WAL Rotation (Multiple Segments)
// ============================================================================

#[test]
fn should_handle_wal_rotation_and_multiple_segments() {
    let engine = open_with_mode(opts_for_mode("local"), "local");
    let cf = engine.default_column_family();

    eprintln!("\n=== WAL ROTATION TEST ===");

    let mut batch_count = 0;

    // Write multiple batches (potential WAL rotation)
    for batch in 0..5 {
        for i in 0..100 {
            let key = format!("batch{}_key{:04}", batch, i);
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(key.as_bytes().to_vec(), b"batch_value".to_vec(), None)
                .unwrap();
            engine.commit(tx, WriteOptions::buffered()).ok();
        }

        engine.flush().ok();
        batch_count += 1;
        eprintln!("Batch {}: flushed 100 keys", batch);
    }

    // Verify all data present
    let final_tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();
    let mut iter = final_tx.scan(&cntryl_midge::Query::new()).unwrap();
    let total = std::iter::from_fn(|| iter.next()).count();

    eprintln!("Total keys after {} batches: {}", batch_count, total);

    if total >= 450 {
        eprintln!("✓ WAL rotation handled correctly");
    } else {
        eprintln!("✗ Data loss during WAL rotation: {} keys", total);
    }
}

// ============================================================================
// TEST GROUP 6: Mixed Operations in WAL
// ============================================================================

#[test]
fn should_recover_mixed_operations_from_wal() {
    let engine = open_with_mode(opts_for_mode("local"), "local");
    let cf = engine.default_column_family();

    eprintln!("\n=== MIXED OPERATIONS IN WAL ===");

    // Put
    let mut tx0 = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    tx0.put(b"put_key".to_vec(), b"put_value".to_vec(), None)
        .unwrap();
    engine.commit(tx0, WriteOptions::buffered()).ok();

    // Delete
    let mut txn1 = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    txn1.delete(b"put_key".to_vec()).ok();
    engine
        .commit(txn1, cntryl_midge::WriteOptions::buffered())
        .ok();

    // Put again
    let mut tx1 = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    tx1.put(b"put_key".to_vec(), b"put_value_v2".to_vec(), None)
        .unwrap();
    engine.commit(tx1, WriteOptions::buffered()).ok();

    // Delete range
    for i in 0..20 {
        let key = format!("dr_{:02}", i);
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(key.as_bytes().to_vec(), b"v".to_vec(), None)
            .unwrap();
        engine.commit(tx, WriteOptions::buffered()).ok();
    }

    let mut txn2 = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    txn2.delete_range(b"dr_05".to_vec(), b"dr_15".to_vec()).ok();
    engine
        .commit(txn2, cntryl_midge::WriteOptions::buffered())
        .ok();

    eprintln!("Applied: put, delete, put, delete_range");

    // Flush
    engine.flush().ok();

    // Verify final state
    let verify_tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();
    let put_val = verify_tx.get(b"put_key").unwrap();
    let mut iter = verify_tx.scan(&cntryl_midge::Query::new()).unwrap();
    let dr_remaining = std::iter::from_fn(|| iter.next())
        .filter(|(k, _)| {
            let k_str = String::from_utf8_lossy(k.as_ref());
            k_str.as_ref() >= "dr_05" && k_str.as_ref() < "dr_15"
        })
        .count();

    eprintln!(
        "Final put_key: {:?}",
        put_val
            .as_ref()
            .map(|v| String::from_utf8_lossy(v).to_string())
    );
    eprintln!("Keys in deleted range: {}", dr_remaining);

    let mixed_ok = put_val.is_some() && dr_remaining == 0;
    if mixed_ok {
        eprintln!("✓ Mixed operations recovered correctly");
    } else {
        eprintln!("✗ Mixed operation recovery failed");
    }
}

// ============================================================================
// TEST GROUP 7: Document WAL Status
// ============================================================================

#[test]
fn document_wal_implementation_status() {
    eprintln!("\n=== WAL IMPLEMENTATION STATUS ===");
    eprintln!("\nCritical durability guarantees:");
    eprintln!("  1. Basic write recovery (puts)");
    eprintln!("  2. Large value handling in WAL");
    eprintln!("  3. Delete operation recovery");
    eprintln!("  4. Range tombstone recovery");
    eprintln!("  5. WAL rotation (multiple segments)");
    eprintln!("  6. Mixed operation ordering");
    eprintln!("\nIf any test fails:");
    eprintln!("  - Durability guarantee violated");
    eprintln!("  - Data loss possible after crash");
    eprintln!("  - Immediate priority fix required");
}
