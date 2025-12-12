# transaction_advanced.rs - Spec Card

## Philosophy

Tests define the **correct future behavior**, not document current limitations. Always implement tests fully; they may fail until features exist.

- ✅ Write ALL tests (never `#[ignore]`)
- ✅ Tests **MAY FAIL** if features aren't implemented yet
- ✅ Once features are built, failing tests become passing tests
- ✅ Tests act as a specification for what code needs to do
- ❌ Never stub behavior; always assert desired semantics
- ❌ Never skip tests on certain storage modes; use conditional logic instead

---

## PROMPT (Self-Driving Implementation Guide)

**Create a test file that validates transaction crash recovery and durability semantics.**

**Key Requirements**:
- All 10 tests parametrized across durable storage modes ONLY (LocalDisk, CloudBacked)
- Pattern: `for_each_storage_mode(&durable_storage_modes(), |mode, opts| { ... })`
- Storage Modes: FS and Cloud only (WAL durability required)
- Committed transactions persisted and recovered after crash
- Uncommitted transactions NOT persisted (rollback on recovery)
- Delete range in transactions persists correctly
- Large transactions with spill files recover correctly
- Abort idempotency: multiple recovery cycles same state
- Exactly-once semantics: no duplicates after crash
- Incomplete WAL syncs: partial writes detected and recovered
- Mid-spill crashes: spill in progress handles crash correctly

**Testing Approach**:
1. Commit transaction, crash → data recovers
2. No commit, crash → data doesn't persist (rolled back)
3. Delete range in transaction, commit, crash → recovered
4. Large transaction spill, commit, crash → recovered
5. Abort transaction, restart → rollback verified
6. Multiple restart cycles → same final state (idempotency)
7. Exactly-once: no duplicate writes across recoveries
8. Large spill file, crash during spill → recovery completes
9. Incomplete WAL sync detected and handled
10. Multi-operation transaction commits atomically

---

**File Location**: `tests/transaction_advanced.rs`
**Test Count**: 10 tests
**Storage Modes**: FS + Cloud ONLY (WAL durability required)
**Pattern**: `for_each_storage_mode(&durable_storage_modes(), |mode, opts| { ... })`
**Status**: 📋 Not yet created (spec ready)

---

## Purpose

Test transaction crash recovery: committed transactions persist and recover correctly, uncommitted transactions rollback on recovery. Transactions provide ACID guarantees essential for correctness.

---

## Tests

1. **should_persist_atomic_transactions_after_restart**
   - Commit transaction, crash, restart → transaction data persisted

2. **should_not_persist_uncommitted_transaction_after_restart**
   - Create transaction, no commit, crash → rollback on restart

3. **should_recover_after_abort_given_transaction_with_delete_range_when_restart**
   - Transaction with delete_range, commit, crash → recovered

4. **should_recover_committed_spill_given_restart_after_commit**
   - Large transaction with spill, commit, crash → spill recovered

5. **should_rollback_uncommitted_spill_given_restart_before_commit**
   - Large transaction spill, no commit, crash → spill cleaned up, rolled back

6. **should_handle_transaction_abort_idempotency_given_multiple_restart_cycles**
   - Multiple crash/restart cycles → same state (idempotency)

7. **should_maintain_exactly_once_semantics_given_transaction_with_crash**
   - Multiple recoveries → no duplicate writes

8. **should_recover_large_transaction_given_crash_during_spill**
   - Crash during spill → recovery completes

9. **should_not_lose_transaction_writes_given_incomplete_wal_sync**
   - Incomplete WAL sync detected, recovery handles

10. **should_survive_mid_spill_crash_given_transaction_recovery**
    - Mid-spill crash → recovery safe and correct

---

## Key APIs

- `engine.transaction()` → Transaction
- `tx.put(cf, key, value)` → Result
- `tx.delete(cf, key)` → Result
- `tx.delete_range(cf, start, end)` → Result
- `tx.commit()` → Result
- Drop transaction (rollback on drop)

---

## Implementation Notes

✅ Uses `durable_storage_modes()` (FS + Cloud only - WAL required)
✅ Phase 1/Phase 2 structure: transaction ops, crash simulation, restart verification
✅ Committed transactions persisted to WAL and SST, recovered on restart
✅ Uncommitted transactions rolled back during recovery
✅ Large transactions may spill to disk, recovered correctly
✅ Spill files cleaned up if transaction aborted
✅ Exactly-once semantics: no duplicates across multiple recoveries
✅ Idempotency: multiple restart cycles produce same final state
✅ Delete range in transactions persisted and recovered atomically
✅ Incomplete WAL syncs handled with tail recovery or skip

---

## Test Pattern Example - Commit Recovery

```rust
#[test]
fn should_persist_atomic_transactions_after_restart() {
    for_each_storage_mode(&durable_storage_modes(), |mode, opts| {
        // Phase 1: Write and commit
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();
            
            let tx = engine.transaction().expect("txn");
            tx.put(cf, b"tx_key1", b"tx_value1").expect("put");
            tx.put(cf, b"tx_key2", b"tx_value2").expect("put");
            tx.commit().expect("commit");
            // engine dropped, crash simulation
        }
        
        // Phase 2: Recover
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();
            
            let got1 = engine.get(cf, b"tx_key1").expect("get");
            let got2 = engine.get(cf, b"tx_key2").expect("get");
            
            assert_eq!(got1, Some(Bytes::from_static(b"tx_value1")), "key1 not persisted in mode: {}", mode);
            assert_eq!(got2, Some(Bytes::from_static(b"tx_value2")), "key2 not persisted in mode: {}", mode);
        }
    });
}
```

---

## Test Pattern Example - Uncommitted Rollback

```rust
#[test]
fn should_not_persist_uncommitted_transaction_after_restart() {
    for_each_storage_mode(&durable_storage_modes(), |mode, opts| {
        // Phase 1: Write but don't commit, crash
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();
            
            let tx = engine.transaction().expect("txn");
            tx.put(cf, b"uncommitted_key", b"uncommitted_value").expect("put");
            // No commit - engine dropped, crash before commit
        }
        
        // Phase 2: Recover
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();
            
            let got = engine.get(cf, b"uncommitted_key").expect("get");
            assert_eq!(got, None, "uncommitted data persisted in mode: {}", mode);
        }
    });
}
```

---

## Test Pattern Example - Delete Range in Transaction

```rust
#[test]
fn should_recover_after_abort_given_transaction_with_delete_range_when_restart() {
    for_each_storage_mode(&durable_storage_modes(), |mode, opts| {
        // Phase 1: Set up data, delete_range in transaction
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();
            
            // Write initial keys
            for i in 0..10 {
                let key = format!("key_{}", i);
                engine.put(cf, key.as_bytes(), b"value").expect("put");
            }
            
            // Transaction with delete_range
            let tx = engine.transaction().expect("txn");
            tx.delete_range(cf, b"key_2", b"key_7").expect("delete_range");
            tx.commit().expect("commit");
            // engine dropped, crash simulation
        }
        
        // Phase 2: Verify recovery
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();
            
            // Keys outside range exist
            assert!(engine.get(cf, b"key_0").expect("get").is_some());
            assert!(engine.get(cf, b"key_1").expect("get").is_some());
            
            // Keys in range deleted
            assert!(engine.get(cf, b"key_3").expect("get").is_none());
            assert!(engine.get(cf, b"key_5").expect("get").is_none());
            
            // Keys after range exist
            assert!(engine.get(cf, b"key_8").expect("get").is_some());
            assert!(engine.get(cf, b"key_9").expect("get").is_some());
        }
    });
}
```

---

## Status

**Current**: 📋 0/10 not started (spec ready for implementation)
**Notes**: Requires durable storage (FS/Cloud), WAL recovery infrastructure

---

## References
- See INTEGRATION_TESTS_FINAL.md lines ~1150 for full transaction_advanced spec
- Transaction API in `src/engine/api.rs`
- WAL recovery documented in durability_recovery.rs
- Spill file handling in `src/runtime/spill.rs`
