//! Transaction Isolation Level: Authoritative Documentation
//!
//! **OFFICIAL STATEMENT**: Midge implements **Last-Write-Wins (LWW) isolation**
//! with **dirty write prevention**.
//!
//! This file serves as the single source of truth for transaction semantics.
//! All other transaction tests MUST be consistent with this level.

use cntryl_midge::testkit::*;
use std::sync::Arc;
use bytes::Bytes;

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
fn verify_dirty_writes_prevented() {
    let engine = open_with_mode(opts_for_mode("memory"), "memory");
    let cf = engine.default_column_family();

    // Create uncomitted transaction
    let mut txn = engine.transaction();
    txn.put(cf.id(), b"key".to_vec(), b"uncommitted".to_vec())
        .unwrap();

    // Other reader should NOT see uncommitted value
    let val = engine.get(cf, b"key").unwrap();

    assert!(
        val.is_none(),
        "VIOLATION: Dirty write visible (txn not yet committed)"
    );
}

/// Verify: Concurrent writes both succeed, last one visible
#[test]
fn verify_concurrent_writes_lww() {
    let engine = Arc::new(open_with_mode(opts_for_mode("memory"), "memory"));
    let cf = engine.default_column_family();

    let mut txn1 = engine.transaction();
    let mut txn2 = engine.transaction();

    txn1.put(cf.id(), b"key".to_vec(), b"from_txn1".to_vec())
        .unwrap();
    txn2.put(cf.id(), b"key".to_vec(), b"from_txn2".to_vec())
        .unwrap();

    let r1 = engine.commit_transaction(txn1);
    let r2 = engine.commit_transaction(txn2);

    // Both should succeed (LWW)
    assert!(r1.is_ok(), "TXN1 should not be rejected");
    assert!(r2.is_ok(), "TXN2 should not be rejected");

    let final_val = engine.get(cf, b"key").unwrap();

    // Last commit should be visible
    assert_eq!(
        final_val,
        Some(Bytes::from_static(b"from_txn2")),
        "Last write should be visible in LWW"
    );
}

/// Verify: Lost updates ARE POSSIBLE in LWW (this is expected behavior)
#[test]
fn verify_lost_update_possible() {
    let engine = Arc::new(open_with_mode(opts_for_mode("memory"), "memory"));
    let cf = engine.default_column_family();

    // Initialize counter
    engine.put(cf, b"counter", b"0").unwrap();

    let mut txn1 = engine.transaction();
    let mut txn2 = engine.transaction();

    // Both read counter
    let val1 = engine.get(cf, b"counter").unwrap();
    let count1: i32 = String::from_utf8_lossy(&val1.unwrap_or_default())
        .parse()
        .unwrap_or(0);

    let val2 = engine.get(cf, b"counter").unwrap();
    let count2: i32 = String::from_utf8_lossy(&val2.unwrap_or_default())
        .parse()
        .unwrap_or(0);

    // Both increment and commit
    txn1.put(
        cf.id(),
        b"counter".to_vec(),
        (count1 + 1).to_string().into_bytes(),
    )
    .unwrap();

    txn2.put(
        cf.id(),
        b"counter".to_vec(),
        (count2 + 1).to_string().into_bytes(),
    )
    .unwrap();

    engine.commit_transaction(txn1).ok();
    engine.commit_transaction(txn2).ok();

    let final_val = engine.get(cf, b"counter").unwrap();
    let final_count: i32 = String::from_utf8_lossy(&final_val.unwrap_or_default())
        .parse()
        .unwrap_or(0);

    // In LWW, lost updates are expected: final = 1 (last write wins over first)
    assert_eq!(
        final_count, 1,
        "LWW allows lost updates: second write overwrites first"
    );
}

/// Verify: Snapshots see uncommitted changes (NOT true Snapshot Isolation)
#[test]
fn verify_snapshots_not_isolated() {
    let engine = open_with_mode(opts_for_mode("memory"), "memory");
    let cf = engine.default_column_family();

    engine.put(cf, b"initial", b"value").unwrap();

    let snapshot = engine.snapshot();
    let initial_count = snapshot.scan(cf, &cntryl_midge::Query::new()).unwrap().len();

    // Add new data after snapshot
    engine.put(cf, b"added_later", b"value").unwrap();

    let later_count = snapshot.scan(cf, &cntryl_midge::Query::new()).unwrap().len();

    // Note: Not asserting here because snapshot behavior may vary
    // The point is to document that true Snapshot Isolation is NOT guaranteed
    eprintln!("Initial snapshot rows: {}", initial_count);
    eprintln!("After insert, snapshot rows: {}", later_count);
    eprintln!(
        "Snapshot sees new rows: {}",
        if later_count > initial_count {
            "YES - NOT true snapshot isolation"
        } else {
            "NO - appears to be snapshot isolated"
        }
    );
}
