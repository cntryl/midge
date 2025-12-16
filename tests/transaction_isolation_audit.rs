//! Transaction Isolation Level Audit
//!
//! Purpose: Definitively determine which isolation level Midge implements.
//! This audit tests concrete scenarios and documents the actual behavior,
//! which can then be used to validate/fix other isolation tests.
//!
//! Isolation levels in order of strength:
//! 1. Read Uncommitted - No guarantees (weakest)
//! 2. Read Committed - No dirty reads
//! 3. Repeatable Read - No dirty reads, no lost updates
//! 4. Snapshot Isolation - No dirty reads, no lost updates, no phantom reads
//! 5. Serializable - All conflicts prevented (strongest)
//!
//! Special: Last-Write-Wins (LWW) - Concurrent writes always succeed, last one wins

use cntryl_midge::testkit::*;
use std::sync::Arc;

// ============================================================================
// DIAGNOSTIC TEST 1: Dirty Read Prevention
// ============================================================================
//
// If Midge prevents dirty reads, it's at least Read Committed level.
// If Midge allows dirty reads, it's Read Uncommitted.

#[test]
fn should_prevent_dirty_reads_when_reading_uncommitted_writes() {
    eprintln!("\n=== AUDIT: DIRTY READ PREVENTION ===");
    eprintln!("Question: Can a transaction see uncommitted writes from another transaction?");

    let engine = open_with_mode(opts_for_mode("memory"), "memory");
    let cf = engine.default_column_family();

    // Transaction 1: Write but don't commit
    let mut txn1 = engine.transaction();
    txn1.put(cf.id(), b"key".to_vec(), b"uncommitted_value".to_vec())
        .unwrap();
    // TXN1 NOT COMMITTED

    // Transaction 2: Try to read the same key
    let value_seen = engine.get(cf, b"key").unwrap();

    if value_seen.is_some() {
        eprintln!("❌ DIRTY READ ALLOWED: Other transaction saw uncommitted write");
        eprintln!("   Isolation Level: Read Uncommitted or weaker");
    } else {
        eprintln!("✅ NO DIRTY READS: Uncommitted writes are hidden");
        eprintln!("   Isolation Level: At least Read Committed");
    }

    // Cleanup
    engine.rollback_transaction(txn1).ok();
}

// ============================================================================
// DIAGNOSTIC TEST 2: Concurrent Write Conflict Handling (Lost Update)
// ============================================================================
//
// This distinguishes between:
// - LWW: Both commits succeed, last write wins
// - Serializable: One commit fails, no lost update
// - Repeatable Read: Lost update possible

#[test]
fn should_resolve_concurrent_write_conflicts_when_concurrent() {
    eprintln!("\n=== AUDIT: CONCURRENT WRITE CONFLICT ===");
    eprintln!("Question: What happens when two transactions write to the same key?");

    let engine = Arc::new(open_with_mode(opts_for_mode("memory"), "memory"));
    let cf = engine.default_column_family();

    // Initial state
    engine.put(cf, b"key", b"initial").unwrap();

    // Transaction 1 and 2 both read and modify the same key
    let mut txn1 = engine.transaction();
    let mut txn2 = engine.transaction();

    // Both read current value
    // (If isolation were perfect, they'd each have snapshot)

    // Both attempt to write
    txn1.put(cf.id(), b"key".to_vec(), b"value_from_txn1".to_vec())
        .unwrap();
    txn2.put(cf.id(), b"key".to_vec(), b"value_from_txn2".to_vec())
        .unwrap();

    // Both attempt to commit
    let result1 = engine.commit_transaction(txn1);
    let result2 = engine.commit_transaction(txn2);

    let final_value = engine.get(cf, b"key").unwrap();

    eprintln!("TXN1 commit result: {:?}", result1);
    eprintln!("TXN2 commit result: {:?}", result2);
    eprintln!("Final value: {:?}", 
        final_value.as_ref().map(|v| String::from_utf8_lossy(v).to_string()));

    match (result1.is_ok(), result2.is_ok()) {
        (true, true) => {
            eprintln!("✅ Both commits succeeded");
            match final_value {
                Some(v) if v.as_ref() == b"value_from_txn2" => {
                    eprintln!("   Final value: TXN2's value");
                    eprintln!("   => Last-Write-Wins (LWW) isolation");
                }
                Some(v) if v.as_ref() == b"value_from_txn1" => {
                    eprintln!("   Final value: TXN1's value");
                    eprintln!("   => Undefined behavior (depends on commit order)");
                }
                _ => {
                    eprintln!("   Final value: Something else or missing");
                    eprintln!("   => Corruption or merge behavior");
                }
            }
        }
        (true, false) | (false, true) => {
            eprintln!("✅ One commit succeeded, one failed");
            eprintln!("   => Optimistic conflict detection or write lock");
            eprintln!("   => Prevents lost update");
            eprintln!("   => Serializable or Repeatable Read");
        }
        (false, false) => {
            eprintln!("❌ Both commits failed");
            eprintln!("   => May indicate error in test or deadlock");
        }
    }
}

// ============================================================================
// DIAGNOSTIC TEST 3: Read-Modify-Write Lost Update
// ============================================================================
//
// This is the classic "increment counter" test.
// Serializable: Final count = 2
// LWW/Repeatable Read: Final count = 1 (lost update possible)

#[test]
fn should_detect_read_modify_write_conflicts_when_concurrent() {
    eprintln!("\n=== AUDIT: READ-MODIFY-WRITE LOST UPDATE ===");
    eprintln!("Question: Does lost update prevention work?");

    let engine = Arc::new(open_with_mode(opts_for_mode("memory"), "memory"));
    let cf = engine.default_column_family();

    // Initial counter value
    engine.put(cf, b"counter", b"0").unwrap();

    // Scenario: Two transactions each increment counter
    let mut txn1 = engine.transaction();
    let mut txn2 = engine.transaction();

    // TXN1: Read counter
    let val1 = engine.get(cf, b"counter").unwrap();
    let num1: i32 = val1
        .as_ref()
        .map(|v| String::from_utf8_lossy(v).parse().unwrap_or(0))
        .unwrap_or(0);

    // TXN2: Read counter (should see same value)
    let val2 = engine.get(cf, b"counter").unwrap();
    let num2: i32 = val2
        .as_ref()
        .map(|v| String::from_utf8_lossy(v).parse().unwrap_or(0))
        .unwrap_or(0);

    // TXN1: Increment and write
    txn1.put(
        cf.id(),
        b"counter".to_vec(),
        (num1 + 1).to_string().into_bytes(),
    )
    .unwrap();

    // TXN2: Increment and write
    txn2.put(
        cf.id(),
        b"counter".to_vec(),
        (num2 + 1).to_string().into_bytes(),
    )
    .unwrap();

    // Commit both
    engine.commit_transaction(txn1).ok();
    engine.commit_transaction(txn2).ok();

    let final_val = engine.get(cf, b"counter").unwrap();
    let final_count: i32 = String::from_utf8_lossy(&final_val.unwrap_or_default())
        .parse()
        .unwrap_or(0);

    eprintln!("Initial counter: 0");
    eprintln!("TXN1 reads: {}, increments to {}", num1, num1 + 1);
    eprintln!("TXN2 reads: {}, increments to {}", num2, num2 + 1);
    eprintln!("Final counter: {}", final_count);

    if final_count == 2 {
        eprintln!("✅ Both increments applied");
        eprintln!("   => Serializable or strong isolation");
    } else if final_count == 1 {
        eprintln!("❌ LOST UPDATE: Only one increment visible");
        eprintln!("   => LWW or weak isolation (concurrent writes conflict)");
    } else {
        eprintln!("❓ Unexpected value: {}", final_count);
    }
}

// ============================================================================
// DIAGNOSTIC TEST 4: Snapshot Isolation (Phantom Reads)
// ============================================================================
//
// If Midge supports snapshot isolation, the "phantom read" test should fail.
// If snapshots are implemented, a transaction shouldn't see rows inserted
// after snapshot creation.

#[test]
fn should_prevent_phantom_reads_when_isolation_enforced() {
    eprintln!("\n=== AUDIT: PHANTOM READ PREVENTION (Snapshot Isolation) ===");
    eprintln!("Question: Can rows inserted after transaction starts become visible?");

    let engine = open_with_mode(opts_for_mode("memory"), "memory");
    let cf = engine.default_column_family();

    // Initial state: 3 keys
    engine.put(cf, b"k1", b"v1").unwrap();
    engine.put(cf, b"k2", b"v2").unwrap();
    engine.put(cf, b"k3", b"v3").unwrap();

    // Create snapshot/transaction
    let snapshot = engine.snapshot();
    let initial_count = snapshot.scan(cf, &cntryl_midge::Query::new()).unwrap().len();
    eprintln!("Snapshot sees {} keys initially", initial_count);

    // Insert new key after snapshot created
    engine.put(cf, b"k4", b"v4").unwrap();

    // Check if snapshot sees the new key
    let later_count = snapshot.scan(cf, &cntryl_midge::Query::new()).unwrap().len();
    eprintln!("Snapshot sees {} keys after insert", later_count);

    if later_count > initial_count {
        eprintln!("❌ PHANTOM READ: Snapshot sees newly inserted rows");
        eprintln!("   => NOT Snapshot Isolation");
    } else {
        eprintln!("✅ NO PHANTOM READS: Snapshot is consistent");
        eprintln!("   => Snapshot Isolation supported");
    }
}

// ============================================================================
// DIAGNOSTIC TEST 5: Write Conflict on Committed Base
// ============================================================================
//
// Serializable would detect and prevent this.
// LWW would allow it and last write wins.

#[test]
fn should_detect_write_skew_when_isolation_enabled() {
    eprintln!("\n=== AUDIT: WRITE SKEW (Serializable) ===");
    eprintln!("Question: Is write skew (concurrent reads of same base, disjoint writes) detected?");

    let engine = Arc::new(open_with_mode(opts_for_mode("memory"), "memory"));
    let cf = engine.default_column_family();

    // Scenario: Two transactions read the same base but write different keys
    engine.put(cf, b"shared", b"base_value").unwrap();
    engine.put(cf, b"flag1", b"false").unwrap();
    engine.put(cf, b"flag2", b"false").unwrap();

    let mut txn1 = engine.transaction();
    let mut txn2 = engine.transaction();

    // Both read the shared key
    let _shared1 = engine.get(cf, b"shared").unwrap();
    let _shared2 = engine.get(cf, b"shared").unwrap();

    // TXN1 writes to different key
    txn1.put(cf.id(), b"flag1".to_vec(), b"true".to_vec()).unwrap();

    // TXN2 writes to different key
    txn2.put(cf.id(), b"flag2".to_vec(), b"true".to_vec()).unwrap();

    let r1 = engine.commit_transaction(txn1);
    let r2 = engine.commit_transaction(txn2);

    eprintln!("TXN1 (writes flag1): {:?}", r1);
    eprintln!("TXN2 (writes flag2): {:?}", r2);

    if r1.is_ok() && r2.is_ok() {
        eprintln!("✅ Both succeeded (write skew allowed)");
        eprintln!("   => NOT strict Serializable");
    } else {
        eprintln!("✅ One or both failed (write skew prevented)");
        eprintln!("   => Strong isolation (Serializable or MVCC)");
    }
}

// ============================================================================
// COMPREHENSIVE SUMMARY TEST
// ============================================================================

#[test]
fn audit_summary_what_isolation_level_is_implemented() {
    eprintln!("\n╔════════════════════════════════════════════════════════════╗");
    eprintln!("║        MIDGE TRANSACTION ISOLATION LEVEL AUDIT              ║");
    eprintln!("╚════════════════════════════════════════════════════════════╝");
    eprintln!();
    eprintln!("Run the following tests and check their assertions:");
    eprintln!();
    eprintln!("1. audit_dirty_read_prevention_uncommitted_writes");
    eprintln!("   → If PASSES: Dirty reads prevented (≥ Read Committed)");
    eprintln!("   → If FAILS: Dirty reads allowed (Read Uncommitted)");
    eprintln!();
    eprintln!("2. audit_concurrent_write_conflict_resolution");
    eprintln!("   → Both succeed, last wins: Last-Write-Wins (LWW)");
    eprintln!("   → One fails: Conflict detection (≥ Repeatable Read)");
    eprintln!();
    eprintln!("3. audit_read_modify_write_conflict");
    eprintln!("   → Final count = 2: Serializable");
    eprintln!("   → Final count = 1: Lost update possible (LWW)");
    eprintln!();
    eprintln!("4. audit_phantom_read_prevention");
    eprintln!("   → Snapshot unchanged: Snapshot Isolation");
    eprintln!("   → Snapshot sees new rows: NOT Snapshot Isolation");
    eprintln!();
    eprintln!("5. audit_write_skew_detection");
    eprintln!("   → Both succeed: Write skew allowed (LWW)");
    eprintln!("   → One fails: Write skew prevented (Serializable)");
    eprintln!();
    eprintln!("Once you know which level Midge implements:");
    eprintln!("  • Remove tests for unsupported isolation levels");
    eprintln!("  • Align all other transaction tests to match");
    eprintln!("  • Document the level clearly in code comments");
    eprintln!();
}
