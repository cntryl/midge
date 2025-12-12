# Test File Creation Plan

This document breaks down each remaining test file with clear specifications before implementation.

---

## ✅ COMPLETED TEST FILES

### durability_wal.rs (10/10 passing)
- Status: ✅ DONE
- Pattern: `for_each_storage_mode(&durable_storage_modes(), |mode, opts| { ... })`
- Storage Modes: LocalDisk + CloudBacked only
- Structure: Phase 1 (crash) / Phase 2 (recovery)

### durability_recovery.rs (13/14 passing, 1 failing)
- Status: ✅ DONE (delete recovery not yet implemented - expected failure)
- Pattern: `for_each_storage_mode(&durable_storage_modes(), |mode, opts| { ... })`
- Storage Modes: LocalDisk + CloudBacked only
- Structure: Phase 1 (crash) / Phase 2 (recovery) with multi-phase for complex scenarios

---

## 🚧 IN PROGRESS

### durability_atomicity.rs (11/11 passing) - ✅ DONE
**Location**: `tests/durability_atomicity.rs`
**Status**: All 11 tests passing
**Details**: Manifest atomicity, SST exposure, WAL precedence, concurrent flush ordering

---

## 📋 READY TO CREATE (Next Priority Order)

### transaction_advanced.rs (10 tests)
**Specification**: Crash recovery for transactions
**Storage Modes**: FS + Cloud ONLY (requires WAL durability)
**Pattern**: `for_each_storage_mode(&durable_storage_modes(), |mode, opts| { ... })`

**Test List**:
```
should_persist_atomic_transactions_after_restart
should_not_persist_uncommitted_transaction_after_restart
should_recover_after_abort_given_transaction_with_delete_range_when_restart
should_recover_committed_spill_given_restart_after_commit
should_rollback_uncommitted_spill_given_restart_before_commit
should_handle_transaction_abort_idempotency_given_multiple_restart_cycles
should_maintain_exactly_once_semantics_given_transaction_with_crash
should_recover_large_transaction_given_crash_during_spill
should_not_lose_transaction_writes_given_incomplete_wal_sync
should_survive_mid_spill_crash_given_transaction_recovery
```

**Key Patterns**:
- Use transaction API (`engine.transaction()`, `tx.put()`, `tx.commit()`)
- Phase 1: Create transaction, write, crash before/during commit
- Phase 2: Reopen engine, verify data persisted or rolled back

**Expected Imports**:
```rust
use bytes::Bytes;
use cntryl_midge::testkit::*;
```

**Critical Details**:
- ⚠️ Use `durable_storage_modes()` (FS + Cloud)
- ⚠️ Each test is 3-phase: setup, crash, verify
- ⚠️ Tests may fail if transaction persistence not fully implemented (EXPECTED)

---

### transaction_spill.rs (13 tests)
**Specification**: Large transaction spill files (memory exceeding configured limit)
**Storage Modes**: FS + Cloud for 12 tests; Memory for 1 test
**Pattern**: Mix of all-modes and durable-only tests

**Test List**:
```
should_commit_large_transaction_given_many_writes_exceeding_memory_limit       [FS, CLOUD]
should_handle_very_large_transaction_given_multiple_spills_when_persisted      [FS, CLOUD]
should_preserve_data_integrity_given_large_transaction_with_specific_values    [FS, CLOUD]
should_preserve_key_order_given_large_transaction_when_iterating               [FS, CLOUD]
should_rollback_spilled_transaction_given_drop_without_commit                  [FS, CLOUD]
should_cleanup_spill_files_given_transaction_rollback_when_finalizing          [FS, CLOUD]
should_rollback_uncommitted_spill_given_restart_before_commit                  [FS, CLOUD]
should_recover_committed_spill_given_restart_after_commit                      [FS, CLOUD]
should_not_starve_foreground_writes_given_background_spill_activity            [FS, CLOUD]
should_handle_concurrent_large_transactions_given_memory_pressure              [FS, CLOUD]
should_handle_transaction_with_tiny_memory_limit_given_forced_spill            [FS, CLOUD]
should_handle_mixed_value_sizes_in_spilled_transaction_when_committed          [FS, CLOUD]
should_not_create_disk_artifacts_given_large_transaction_when_memory_mode      [MEMORY ONLY]
```

**Key Patterns**:
- Tests 1-12: `for_each_storage_mode(&durable_storage_modes(), |mode, opts| { ... })`
- Test 13: `let opts = memory_opts();` (no loop)
- Large write loops (100s-1000s of keys)
- Concurrent transaction spawning for stress tests

**Expected Behavior**:
- Transactions exceeding memory limit should spill to disk
- Commit/rollback should clean up spill files
- Concurrent transactions should not block each other
- Memory-mode should NOT create spill files (test verifies this)

**Critical Details**:
- ⚠️ Need to set small memory budget in opts to force spill
- ⚠️ Use `std::sync::Arc` + `std::thread::spawn` for concurrent tests
- ⚠️ Tests may fail if spill logic not yet implemented (EXPECTED)

---

## 📋 FUTURE PHASES (SST Layer & Beyond)

### sst_reader.rs (7 tests)
### sst_writer.rs (14 tests)
### sst_index_table.rs (20 tests)
### sst_tombstone_index.rs (20 tests)
### sst_fence_pointers.rs (12 tests)
### sst_block_cache.rs (12 tests)
### sst_per_block_bloom.rs (19 tests)

[Phase 5 streaming tests deferred]

---

## CHECKLIST FOR EACH NEW FILE

Before writing any code:
- [ ] Read the test specification above
- [ ] Find a similar test file to copy structure from
- [ ] Verify parametrization pattern (all_storage_modes vs durable_storage_modes)
- [ ] Identify Phase 1/Phase 2 structure (if crash recovery needed)
- [ ] Note any special APIs or patterns
- [ ] Plan out all test names first

While writing:
- [ ] Use correct imports
- [ ] Follow naming convention: `should_<behavior>_given_<context>_when_<condition>`
- [ ] AAA structure with comments: Arrange/Act/Assert
- [ ] Include mode in assertion error messages
- [ ] Handle both successful and expected-failure cases

After writing:
- [ ] Compile: `cargo test --test <filename> --quiet 2>&1 | Select-Object -Last 15`
- [ ] Document pass/fail counts
- [ ] Update INTEGRATION_TESTS_FINAL.md status
- [ ] Move to next file

---

## Current Progress Summary

| File | Tests | Status | Action |
|------|-------|--------|--------|
| durability_wal.rs | 10 | ✅ 10/10 passing | Done |
| durability_recovery.rs | 14 | 🚧 13/14 passing | Done (1 expected fail) |
| durability_atomicity.rs | 11 | ✅ 11/11 passing | Done |
| transaction_advanced.rs | 10 | 📋 Ready to create | Next |
| transaction_spill.rs | 13 | 📋 Ready to create | After transaction_advanced |
| SST layer | 126 | 📋 Planned | Phase 5+ |

