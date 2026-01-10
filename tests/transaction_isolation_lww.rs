//! Transaction Isolation Level: Authoritative Documentation
//!
//! **OFFICIAL STATEMENT**: Midge implements **Last-Write-Wins (LWW) isolation**
//! with **dirty write prevention**.
//!
//! This file serves as the single source of truth for transaction semantics.
//! All other transaction tests MUST be consistent with this level.

use bytes::Bytes;
use cntryl_midge::testkit::*;
use std::sync::Arc;

#[test]
fn document_transaction_isolation_level_lww() {
    eprintln!("\n╔═══════════════════════════════════════════════════════════════╗");
    eprintln!("║     MIDGE TRANSACTION ISOLATION LEVEL: LAST-WRITE-WINS        ║");
    eprintln!("╚═══════════════════════════════════════════════════════════════╝");
    eprintln!();
    eprintln!("Midge implements: LAST-WRITE-WINS (LWW) isolation");
    eprintln!();
    eprintln!("✅ What IS guaranteed:");
    eprintln!("  1. Dirty write prevention: Uncommitted writes are not visible");
    eprintln!("  2. Last-write-wins: Concurrent writes both succeed, last one wins");
    eprintln!("  3. Atomicity: Transactions commit all-or-nothing");
    eprintln!();
    eprintln!("❌ What is NOT guaranteed:");
    eprintln!("  1. Lost update prevention: Concurrent read-modify-write conflicts");
    eprintln!("     Example: Two txns read counter=0, both increment, final=1 (not 2)");
    eprintln!("  2. Snapshot isolation: Snapshots see new rows inserted after creation");
    eprintln!("  3. Write skew prevention: Concurrent disjoint writes always succeed");
    eprintln!("  4. Repeatable read: Second read in same transaction may see changes");
    eprintln!("  5. Serializable: No global ordering guarantee");
    eprintln!();
    eprintln!("Isolation Hierarchy (weakest to strongest):");
    eprintln!("  1. Read Uncommitted [NOT IMPLEMENTED]");
    eprintln!("  2. Read Committed   [NOT IMPLEMENTED]");
    eprintln!("  3. Repeatable Read  [NOT IMPLEMENTED]");
    eprintln!("  4. Snapshot Iso.    [NOT IMPLEMENTED]");
    eprintln!("  5. Serializable     [NOT IMPLEMENTED]");
    eprintln!("  6. Last-Write-Wins  [✅ IMPLEMENTED] <-- Midge");
    eprintln!();
    eprintln!("Use cases where LWW is appropriate:");
    eprintln!("  ✅ High-throughput distributed systems");
    eprintln!("  ✅ Time-series data (always using latest sensor reading)");
    eprintln!("  ✅ Cache-like usage patterns");
    eprintln!("  ✅ State that converges via last-write (e.g., configuration)");
    eprintln!();
    eprintln!("Use cases where LWW is DANGEROUS:");
    eprintln!("  ❌ Financial transactions (can lose money)");
    eprintln!("  ❌ Inventory management (can oversell)");
    eprintln!("  ❌ Voting/counting (can lose votes)");
    eprintln!("  ❌ Any aggregate that depends on all writes being counted");
    eprintln!();
}

/// Verify: Dirty writes ARE prevented (not LWW quirk, but actual guarantee)
#[test]
fn should_prevent_dirty_writes_when_uncommitted() {
    let engine = open_with_mode(opts_for_mode("memory"), "memory");
    let cf = engine.default_column_family();

    // Create uncomitted transaction
    let mut txn = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
    txn.put(b"key".to_vec(), b"uncommitted".to_vec(), None)
        .unwrap();

    // Other reader should NOT see uncommitted value
    let tx_read = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).unwrap();
    let val = tx_read.get(b"key").unwrap();

    assert!(
        val.is_none(),
        "VIOLATION: Dirty write visible (txn not yet committed)"
    );
}

/// Verify: Concurrent writes both succeed, last one visible
#[test]
fn should_resolve_concurrent_writes_with_lww_when_enabled() {
    let engine = Arc::new(open_with_mode(opts_for_mode("memory"), "memory"));
    let cf = engine.default_column_family();

    let mut txn1 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
    let mut txn2 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();

    txn1.put(b"key".to_vec(), b"from_txn1".to_vec(), None)
        .unwrap();
    txn2.put(b"key".to_vec(), b"from_txn2".to_vec(), None)
        .unwrap();

    let r1 = engine.commit(txn1, cntryl_midge::WriteOptions::buffered());
    let r2 = engine.commit(txn2, cntryl_midge::WriteOptions::buffered());

    // Both should succeed (LWW)
    assert!(r1.is_ok(), "TXN1 should not be rejected");
    assert!(r2.is_ok(), "TXN2 should not be rejected");

    let tx_read = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).unwrap();
    let final_val = tx_read.get(b"key").unwrap();

    // Last commit should be visible
    assert_eq!(
        final_val,
        Some(Bytes::from_static(b"from_txn2")),
        "Last write should be visible in LWW"
    );
}

/// Verify: Lost updates ARE POSSIBLE in LWW (this is expected behavior)
#[test]
fn should_permit_lost_updates_when_not_prevented() {
    let engine = Arc::new(open_with_mode(opts_for_mode("memory"), "memory"));
    let cf = engine.default_column_family();

    // Initialize counter
    let mut tx_init = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
    tx_init.put(b"counter".to_vec(), b"0".to_vec(), None).unwrap();
    engine.commit(tx_init, cntryl_midge::WriteOptions::buffered()).unwrap();

    let mut txn1 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();
    let mut txn2 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();

    // Both read counter
    let val1 = txn1.get(b"counter").unwrap();
    let count1: i32 = String::from_utf8_lossy(&val1.unwrap_or_default())
        .parse()
        .unwrap_or(0);

    let val2 = txn2.get(b"counter").unwrap();
    let count2: i32 = String::from_utf8_lossy(&val2.unwrap_or_default())
        .parse()
        .unwrap_or(0);

    // Both increment and commit
    txn1.put(
        b"counter".to_vec(),
        (count1 + 1).to_string().into_bytes(),
        None,
    )
    .unwrap();

    txn2.put(
        b"counter".to_vec(),
        (count2 + 1).to_string().into_bytes(),
        None,
    )
    .unwrap();

    engine.commit(txn1, cntryl_midge::WriteOptions::buffered()).ok();
    engine.commit(txn2, cntryl_midge::WriteOptions::buffered()).ok();

    let tx_read = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).unwrap();
    let final_val = tx_read.get(b"counter").unwrap();
    let final_count: i32 = String::from_utf8_lossy(&final_val.unwrap_or_default())
        .parse()
        .unwrap_or(0);

    // In LWW, lost updates are expected: final = 1 (last write wins over first)
    assert_eq!(
        final_count, 1,
        "LWW allows lost updates: second write overwrites first"
    );
}

// Note: Snapshot isolation testing removed as the snapshot API is not yet implemented.
// When snapshots are added, snapshot visibility tests should be reintroduced.
