# TODO (Work In Progress)

This document collects and prioritizes outstanding TODOs found across the repository. It is a short, actionable backlog to help triage and assign work.

**Summary**
- Total TODO count: ~35 items (updated Nov 15, 2025 - fresh inventory from codebase)
- Focus areas: transaction conflict detection, deadlock prevention, manifest corruption recovery, comprehensive testing
- **Recent Progress**: 
  - ✅ Implemented detailed health validation with SST continuity and WAL checks (Nov 15, 2025)
  - ✅ Implemented autotuner initialization from config (Nov 15, 2025)
  - ✅ Refactored in-memory WAL to NoOpWal for proper durability semantics (Nov 15, 2025)
  - ✅ Completed transaction conflict detection: write-write conflicts, deadlock detection, lost updates prevention, and isolation levels enforcement (Nov 15, 2025)
  - ✅ Completed manifest corruption recovery: CorruptAfterSave behavior implemented and tested (Nov 15, 2025)
  - ✅ Implemented CF drop during flush concurrency test (Nov 15, 2025)
  - **Overall Progress: 22/52 items complete (42%)** 📈

---

## 🎯 Completed Items (Nov 2025)

### Critical — Durability & Fsync Tests ✅

**`tests/durability_wal.rs`** (7 tests, 4 enhanced):
- ✅ `should_lose_unfsynced_data_given_crash_before_fsync` — Uses `FsyncBehavior::Skip`
- ✅ Enhanced `should_not_acknowledge_commit_given_wal_unsynced_when_crash_occurs` — Instruments fsync calls
- ✅ Enhanced `should_discard_partial_record_given_truncated_wal_segment_when_recovering` — Simulates torn writes

**`tests/durability_recovery.rs`** (3 tests enhanced):
- ✅ Added WAL append count instrumentation  
- ✅ Added fsync verification to sync sequence replay
- ✅ Enhanced crash-during-write with fsync skipping

### Critical — Manifest Durability Tests ✅ (NEW)

**`tests/durability_manifest.rs`** (4 tests enhanced):
- ✅ `should_preserve_consistency_given_crash_between_sst_write_and_manifest_update` — Uses `ManifestBehavior::FailSave` to verify WAL recovery
- ✅ Enhanced `should_fsync_sst_and_update_manifest_before_wal_truncation` — Instruments fsync and manifest counts, verifies ordering
- ✅ Enhanced `should_not_truncate_wal_given_manifest_save_failure` — Verifies WAL is NOT truncated on manifest failure
- ✅ Enhanced `should_fsync_manifest_before_truncating_wal` — Verifies manifest fsync before WAL truncation ordering

### Critical — Compaction Durability Tests ✅ (NEW)

**`tests/durability_compaction.rs`** (6 tests enhanced):
- ✅ `should_commit_new_ssts_and_manifest_together_given_compaction_successful` — Verifies atomic SST+manifest commit
- ✅ `should_cleanup_partial_output_given_compaction_failure` — Uses `CompactionBehavior::FailMidway` to verify cleanup
- ✅ `should_delete_old_sst_files_only_after_manifest_persisted` — Verifies deletion ordering with manifest persistence
- ✅ `should_fsync_new_ssts_before_updating_manifest` — Instruments fsync before manifest update
- ✅ `should_recover_consistent_state_given_crash_mid_compaction_when_restart` — Uses `CompactionBehavior::CrashBeforeFsync`
- ✅ `should_preserve_source_ssts_when_compaction_output_not_fsynced` — Verifies source SST preservation on crash

### Phase 5 — Health & Autotuning ✅ (NEW)

**Health Manager:**
- ✅ `src/health/manager.rs` (line 326) — IMPLEMENTED: Detailed validation with SST continuity checks, missing WAL segments detection, and discrepancy detection

**Autotuner:**
- ✅ `src/core/engine/state/initialization.rs` (line 46) — IMPLEMENTED: Initialize autotuner when config.autotune_enabled() is true

**WAL Improvements:**
- ✅ `src/wal/mem/shared.rs` (line 5) — IMPLEMENTED: Refactored to NoOpWal that explicitly discards writes instead of maintaining useless in-memory buffer

---

## 📋 Remaining TODOs (35 items)

### 🔴 CRITICAL — Transaction Conflict Detection (6 items)

**Write-Write Conflicts:**
- ✅ `tests/txn_write_write_conflicts.rs` (line 138) — IMPLEMENTED: Conflict detection for overlapping ranges prevents concurrent writes to same keys

**Deadlock Detection:**
- ✅ `tests/txn_deadlock_detection.rs` (line 83) — IMPLEMENTED: Conflict detection resolves circular waits by aborting conflicting transactions
- ✅ `tests/txn_deadlock_detection.rs` (line 152) — IMPLEMENTED: Three-way circular conflicts properly resolved with at least one transaction aborted

**Lost Updates Prevention:**
- ✅ `tests/txn_lost_updates.rs` (line 106) — IMPLEMENTED: CAS validation prevents lost updates by checking values at commit time

**Isolation Levels:**
- ✅ `tests/txn_isolation_levels.rs` (line 54) — IMPLEMENTED: Prevents dirty writes through conflict detection (no explicit locking required for optimistic concurrency)

**Transaction Lifecycle:**
- `tests/txn_transaction_lifecycle.rs` (line 31) — TODO: Should timeout if transaction exceeds deadline
- `tests/txn_transaction_lifecycle.rs` (line 55) — TODO: Verify locks released after timeout/abort

### 🟠 HIGH — Manifest Corruption Recovery (1 item) ✅

**Manifest Corruption:**
- ✅ `src/core/manifest/io.rs` (line 187) — IMPLEMENTED: CorruptAfterSave behavior corrupts manifest after fsync to test recovery boundaries

### 🟡 MEDIUM — Multi-CF Support (1 item)

**Transaction Edge Cases:**
- `tests/txn_edge_cases.rs` (line 96) — TODO: Test with multi-CF when CF API is available

### 🟡 MEDIUM — Admin Concurrency Testing (4 items)

**Administrative Operations:**
- ✅ `tests/admin_concurrency.rs` (line 15) — IMPLEMENTED: Attempt backup during compaction - verifies backup succeeds and creates consistent snapshot
- ✅ `tests/admin_concurrency.rs` (line 35) — IMPLEMENTED: Attempt CF drop during flush - verifies drop fails gracefully when flush is in progress
- ✅ `tests/admin_concurrency.rs` (line 56) — IMPLEMENTED: Initiate readonly backup concurrently
- `tests/admin_concurrency.rs` (line 79) — TODO: Reload config during compaction

### 🟡 MEDIUM — Cloud Durability Testing (3 items)

**Cloud Storage:**
- `tests/cloud_durability.rs` (line 48) — TODO: Test cloud mode with mock backend to verify upload retry logic
- `tests/cloud_durability.rs` (line 70) — TODO: Test cloud mode with simulated network failures
- `tests/cloud_durability.rs` (line 98) — TODO: Simulate cloud manifest drift

### 🟡 MEDIUM — Compaction Observability (2 items)

**Compaction Metrics:**
- `tests/compaction_correctness.rs` (line 35) — TODO: Capture compaction output hash/checksum for determinism verification
- `tests/compaction_correctness.rs` (line 139) — TODO: Monitor write amplification metrics

### 🟡 MEDIUM — Sequence Number Validation (2 items)

**Memtable Concurrency:**
- `tests/memtable_concurrency.rs` (line 45) — TODO: Add instrumentation to verify sequence numbers are strictly increasing
- `tests/memtable_concurrency.rs` (line 260) — TODO: Add instrumentation to verify sequence numbers are strictly increasing

### 🟡 MEDIUM — Shutdown Semantics (2 items)

**Cloud Shutdown:**
- `tests/shutdown_semantics.rs` (line 89) — TODO: Test cloud storage mode with long-running uploads
- `tests/shutdown_semantics.rs` (line 163) — TODO: Add instrumentation to verify no WAL replay occurred

### 🟡 MEDIUM — Transaction Isolation (2 items)

**Isolation Verification:**
- `tests/transaction_isolation.rs` (line 97) — TODO: Verify conflict detection behavior based on isolation level
- `tests/transaction_isolation.rs` (line 112) — TODO: Add snapshot.get() API to verify isolation

### 🟡 MEDIUM — Durability Recovery (1 item)

**Manifest Corruption:**
- `tests/durability_recovery.rs` (line 189) — TODO: Corrupt manifest and verify rebuild stops at fsync boundary

---

## 📊 Progress Summary

| Item | Status | Completed |
|------|--------|-----------|
| WAL & Fsync Tests | ✅ Complete | 4 tests |
| Manifest Durability Tests | ✅ Complete | 4 tests |
| Compaction Durability Tests | ✅ Complete | 6 tests |
| Transaction Conflict Detection | 🟡 5/6 Complete | 5/6 |
| Engine Correctness | 🔴 Not Started | 0/5 |
| Instrumentation | 🔴 Not Started | 0/10 |
| **Total** | **32% Complete** | **15/52** |

## 📊 Priority Matrix (Updated)

| Priority | Count | Status | Timeline |
|----------|-------|--------|----------|
| 🔴 CRITICAL | 12 | 11/12 Complete ✅ | COMPLETE (1 remaining) |
| 🟠 HIGH | 15 | 1/15 | Week 1 START |
| 🟡 MEDIUM | 18 | 1/18 | Week 2-3 |
| 🔵 LOW | 7 | 0/7 | Month 2+ |
| **TOTAL** | **52** | **22/52 (42%)** | |

---

## 💡 Testing Pattern Recommendation

**For all new tests, follow this pattern:**

```rust
// 1. Identify failure mode
let hooks = TestHooks::new()
    .with_fsync_behavior(FsyncBehavior::Skip)
    .with_manifest_behavior(ManifestBehavior::FailSave);

// 2. Create options with hooks
let opts = MidgeOptions {
    test_hooks: Some(hooks.clone()),
    ..Default::default()
};

// 3. Execute and verify instrumentation
let result = operation();
assert!(hooks.fsync_count() > 0, "Should have fsynced");

// 4. Verify recovery behavior
let opts_recovery = MidgeOptions { test_hooks: None, ..opts };
let recovered = verify_recovery();
```

---

Generated: November 13, 2025  
Last Updated: Comprehensive TODO extraction and prioritization
