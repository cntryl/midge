# transaction_advanced.rs - Specification Card

**File Location**: `tests/transaction_advanced.rs`
**Test Count**: 10 tests
**Storage Modes**: FS + Cloud ONLY (LocalDisk + CloudBacked)
**Expected Pattern**: `for_each_storage_mode(&durable_storage_modes(), |mode, opts| { ... })`

---

## ✅ Pre-Write Checklist

- [ ] This spec is clear
- [ ] Read similar test: `durability_recovery.rs` (Phase 1/Phase 2 pattern)
- [ ] Understand transaction API usage
- [ ] Have 3-phase structure planned (setup, crash, verify)
- [ ] Identified needed imports

---

## 📝 Test Specifications

### Test 1: should_persist_atomic_transactions_after_restart
**Purpose**: Verify committed transactions survive restart
**Pattern**: Phase 1 (create transaction, write, commit), Phase 2 (reopen, verify all data present)
**Expected**: All data from committed transaction present after restart

### Test 2: should_not_persist_uncommitted_transaction_after_restart
**Purpose**: Verify uncommitted/aborted transactions don't persist
**Pattern**: Phase 1 (create transaction, write, DROP without commit), Phase 2 (reopen, data gone)
**Expected**: No data from rolled-back transaction

### Test 3: should_recover_after_abort_given_transaction_with_delete_range_when_restart
**Purpose**: Verify delete_range rollback works in transactions
**Pattern**: Phase 1 (create data, flush, txn with delete_range, abort), Phase 2 (reopen, delete undone)
**Expected**: Deleted keys are restored after abort

### Test 4: should_recover_committed_spill_given_restart_after_commit
**Purpose**: Verify spilled transaction data persists after commit
**Pattern**: Phase 1 (large txn that spills, commit), Phase 2 (reopen, all data present)
**Expected**: All data recovered (spill files cleaned up after commit)

### Test 5: should_rollback_uncommitted_spill_given_restart_before_commit
**Purpose**: Verify spill files cleaned up when txn rolled back
**Pattern**: Phase 1 (large txn that spills, crash before commit), Phase 2 (reopen, no spill artifacts)
**Expected**: Spill files deleted (data not recovered)

### Test 6: should_handle_transaction_abort_idempotency_given_multiple_restart_cycles
**Purpose**: Verify abort doesn't corrupt data across multiple crashes
**Pattern**: Phase 1a (create txn, abort, crash), Phase 1b (reopen, create new txn, abort, crash), Phase 2 (final reopen, verify consistency)
**Expected**: No data corruption across abort/restart cycles

### Test 7: should_maintain_exactly_once_semantics_given_transaction_with_crash
**Purpose**: Verify idempotent recovery (no duplicate data)
**Pattern**: Phase 1 (write in txn, commit, crash), Phase 2a (reopen, verify count=1), Phase 2b (reopen again, verify count still=1)
**Expected**: Data appears exactly once (not duplicated on multiple recoveries)

### Test 8: should_recover_large_transaction_given_crash_during_spill
**Purpose**: Verify recovery works if crash happens while transaction is spilling
**Pattern**: Phase 1 (start large txn that spills, crash during spill), Phase 2 (reopen, all data recovered)
**Expected**: All data recovered from WAL (spill cleanup is idempotent)

### Test 9: should_not_lose_transaction_writes_given_incomplete_wal_sync
**Purpose**: Verify WAL sync guarantees (durability_opts enables fsync)
**Pattern**: Phase 1 (multiple txn writes with fsync, crash), Phase 2 (reopen, all writes present)
**Expected**: Zero data loss (WAL sync is durable)

### Test 10: should_survive_mid_spill_crash_given_transaction_recovery
**Purpose**: Verify recovery handles partial spill files gracefully
**Pattern**: Phase 1 (large txn spilling, immediate crash), Phase 2 (reopen, no panic, data recovered)
**Expected**: Engine recovers without crashing (spill cleanup is robust)

---

## 🏗️ Code Structure Template

```rust
use bytes::Bytes;
use cntryl_midge::testkit::*;

#[test]
fn should_persist_atomic_transactions_after_restart() {
    for_each_storage_mode(&durable_storage_modes(), |mode, opts| {
        // Arrange & Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            // Create transaction
            let tx = engine.transaction().expect("begin_txn");
            
            // Write data in transaction
            tx.put(cf, b"tx_key", b"tx_value").expect("put");
            
            // Commit
            tx.commit().expect("commit");
            
            // Crash (engine drops here)
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();
            
            // Verify data persisted
            assert_eq!(
                engine.get(cf, b"tx_key").expect("get"),
                Some(Bytes::from_static(b"tx_value")),
                "mode: {}", mode
            );
        }
    });
}
```

---

## ⚠️ Important Notes

1. **Transaction API**: Needs to match what's implemented
   - Likely: `engine.transaction()` → returns `Transaction` struct
   - Methods: `.put()`, `.delete()`, `.commit()`, `.abort()`, or drop = abort
   
2. **Phase structure**: 
   - Phase 1 is IN A SEPARATE SCOPE `{ ... }` to ensure engine drops
   - Phase 2 reopens with SAME opts but NEW engine instance
   
3. **Expected failures**:
   - Spill recovery tests may fail if feature not complete (EXPECTED)
   - Tests document desired behavior, not current state
   
4. **Storage modes**:
   - ONLY use `durable_storage_modes()` (FS + Cloud)
   - Memory mode dropped = all state lost (not useful for recovery tests)
   
5. **Error handling**:
   - Use `.expect()` for APIs that should work
   - Let tests fail if transaction API differs from expectations
   - Document actual errors in code comments

---

## 🔍 Verification Checklist (After Writing)

- [ ] All 10 tests compile without errors
- [ ] Run: `cargo test --test transaction_advanced --quiet`
- [ ] Capture pass/fail counts
- [ ] Fix any API issues discovered during compilation
- [ ] Update INTEGRATION_TESTS_FINAL.md with status (X/10 passing, Y failing)
- [ ] Commit with clear message about what works and what doesn't

---

## 📚 Reference Files

- **Similar patterns**: `durability_recovery.rs` (Phase 1/Phase 2 structure)
- **Transaction API**: Check `src/engine/mod.rs` for actual `transaction()` method
- **Test utilities**: `src/testkit/mod.rs` - verify what's available
- **Full spec**: `INTEGRATION_TESTS_FINAL.md` lines 440-455

---

## Next Actions

1. Write all 10 test function signatures (empty bodies)
2. Review for any missing imports
3. Implement test 1 fully, compile, test
4. Implement remaining tests one-by-one
5. Compile and run after each implementation
6. Document results in INTEGRATION_TESTS_FINAL.md

**Ready to start? Create the file and let's see what errors we get!**

