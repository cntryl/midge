# TODO (Work In Progress)

This document collects and prioritizes outstanding TODOs found across the repository. It is a short, actionable backlog to help triage and assign work.

**Summary**
- Total TODO count: ~52 items (extracted Nov 13, 2025)
- Focus areas: transactions & concurrency, engine correctness (merge/write-stalls), instrumentation, manifest/compaction durability
- **Recent Progress**: Implemented critical durability/WAL tests with TestHooks (Nov 2025)

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

---

## 📋 Remaining TODOs (52 items)

### 🔴 CRITICAL — Durability & Manifest (6 items)

**Manifest Atomicity & Recovery:**
- `tests/durability_manifest.rs` (line 22) — TODO: Add test hook to crash between SST write and manifest update
- `tests/durability_manifest.rs` (line 59) — TODO: Add instrumentation to verify manifest fsync before WAL truncation
- `tests/durability_manifest.rs` (line 92) — TODO: Add test hook to fail manifest save and verify WAL not truncated
- `tests/durability_manifest.rs` (line 124) — TODO: Add instrumentation to verify manifest fsync before WAL truncation
- `src/core/manifest/io.rs` (line 159) — TODO: Implement CorruptAfterSave behavior if needed
- `tests/durability_recovery.rs` (line 189) — TODO: Corrupt manifest and verify rebuild stops at fsync boundary

**Compaction Durability:**
- `tests/durability_compaction.rs` (line 29) — TODO: Wait for compaction to complete and verify atomic commit
- `tests/durability_compaction.rs` (line 63) — TODO: Inject compaction failure and verify partial outputs cleaned up
- `tests/durability_compaction.rs` (line 107) — TODO: Verify old SSTs deleted only after manifest persisted
- `tests/durability_compaction.rs` (line 138) — TODO: Add instrumentation to verify new SST fsync before manifest update
- `tests/durability_compaction.rs` (line 172) — TODO: Simulate crash during compaction
- `tests/durability_compaction.rs` (line 206) — TODO: Simulate crash before compaction output fsync completes

---

### 🟠 HIGH — Transactions & Conflict Detection (10 items)

**Write-Write Conflicts:**
- `tests/txn_write_write_conflicts.rs` (line 138) — TODO: implement conflict detection for overlapping ranges
- `src/core/transaction/engine_transaction.rs` (line 88) — TODO: Implement transaction-aware scan in engine
- `src/core/engine/operations/transactions.rs` (line 65) — TODO: Track which CF triggered the WAL rotation and flush that one
- `src/core/engine/operations/transactions.rs` (line 151) — TODO: Implement proper merge semantics with merge operators

**Transaction Lifecycle:**
- `tests/txn_transaction_lifecycle.rs` (line 30) — TODO: Should timeout if transaction exceeds deadline
- `tests/txn_transaction_lifecycle.rs` (line 54) — TODO: Verify locks released after timeout/abort
- `tests/txn_lost_updates.rs` (line 106) — TODO: Should fail if key was modified since snapshot
- `tests/txn_isolation_levels.rs` (line 60) — TODO: Should prevent dirty write when locking is implemented
- `tests/txn_deadlock_detection.rs` (line 83) — TODO: Verify one is aborted with deadlock error
- `tests/txn_deadlock_detection.rs` (line 152) — TODO: At least one should be aborted when deadlock detection is implemented

---

### 🟠 HIGH — Engine Correctness (5 items)

**Merge Semantics:**
- `src/core/memtable/core.rs` (line 232) — TODO: Implement proper merge semantics
- `src/core/engine/operations/transactions.rs` (line 151) — TODO: Implement proper merge semantics with merge operators

**Write Stall Mechanism:**
- `src/core/engine/operations/writes.rs` (line 93) — TODO: Implement proper write stall mechanism
- `src/core/engine/operations/writes.rs` (line 319) — TODO: Track which CF triggered the WAL rotation and flush that one

**WAL Rotation & Flushing:**
- `src/core/engine/operations/maintenance.rs` (line 107) — TODO: After manifest is persisted, we should replace memtable with new empty one

---

### 🟡 MEDIUM — Instrumentation & Observability (10 items)

**Cache & Read Path:**
- `tests/read_path_caching.rs` (line 16) — TODO: Enable paranoid checksum mode
- `tests/read_path_caching.rs` (line 43) — TODO: Monitor cache metrics to verify LRU eviction
- `tests/read_path_caching.rs` (line 77) — TODO: Add instrumentation to verify read amplification metrics

**Transaction Sequencing:**
- `tests/memtable_concurrency.rs` (line 44) — TODO: Add instrumentation to verify sequence numbers are strictly increasing

**Shutdown & Recovery:**
- `tests/shutdown_semantics.rs` (line 88) — TODO: Test cloud storage mode with long-running uploads
- `tests/shutdown_semantics.rs` (line 162) — TODO: Add instrumentation to verify no WAL replay occurred

**Compaction Observability:**
- `tests/compaction_correctness.rs` (line 34) — TODO: Capture compaction output hash/checksum for determinism verification
- `tests/compaction_correctness.rs` (line 138) — TODO: Monitor write amplification metrics
- `tests/engine_compaction.rs` (line 96) — TODO: Background compaction doesn't fully compact in one round. Needs investigation.

**Transaction Isolation:**
- `tests/transaction_isolation.rs` (line 95) — TODO: Verify conflict detection behavior based on isolation level
- `tests/transaction_isolation.rs` (line 110) — TODO: Add snapshot.get() API to verify isolation

---

### 🟡 MEDIUM — Cloud & Configuration (5 items)

**Cloud Storage Durability:**
- `tests/cloud_durability.rs` (line 47) — TODO: Test cloud mode with mock backend to verify upload retry logic
- `tests/cloud_durability.rs` (line 69) — TODO: Test cloud mode with simulated network failures
- `tests/cloud_durability.rs` (line 98) — TODO: Simulate cloud manifest drift

**Configuration Management:**
- `tests/config_validation.rs` (line 44) — TODO: Add API for runtime config updates
- `tests/config_validation.rs` (line 61) — TODO: Add API for config reload
- `tests/config_validation.rs` (line 67) — TODO: Add instrumentation to verify no component restarts occurred

---

### 🟡 MEDIUM — Advanced Concurrency (3 items)

**Administrative Operations:**
- `tests/admin_concurrency.rs` (line 15) — TODO: Attempt backup during compaction
- `tests/admin_concurrency.rs` (line 35) — TODO: Attempt CF drop during flush
- `tests/admin_concurrency.rs` (line 56) — TODO: Initiate readonly backup concurrently
- `tests/admin_concurrency.rs` (line 79) — TODO: Reload config during compaction

**Edge Cases:**
- `tests/txn_edge_cases.rs` (line 96) — TODO: Test with multi-CF when CF API is available

---

### 🔵 LOW — Phase 5 & Polish (4 items)

**Health & Autotuning:**
- `src/health/manager.rs` (line 326) — TODO: Detailed validation (Phase 5)
- `src/core/engine/state/initialization.rs` (line 46) — TODO: Initialize autotuner if enabled

**WAL Improvements:**
- `src/wal/mem/shared.rs` (line 5) — TODO: Refactor to NoOpWal - an in-memory WAL defeats the purpose of durability

---

## 🎯 Recommended Next Steps

### Immediate (Next Sprint)

**1. Manifest Durability Tests** (6 items)
- Use `ManifestBehavior::FailSave` to test atomic commits
- Verify WAL is not truncated before manifest fsync
- Test corruption scenarios during recovery
- **Estimated effort:** 2-3 days
- **Owner:** [Durability Lead]

**2. Compaction Durability** (6 items)
- Use `CompactionBehavior::FailMidway` and `CrashBeforeFsync`
- Verify atomic manifest update after compaction
- Test cleanup of partial outputs on failure
- **Estimated effort:** 2-3 days
- **Owner:** [Durability Lead]

### Short-term (2-3 Weeks)

**3. Transaction Conflict Detection** (10 items)
- Implement write-write conflict detection for overlapping keys/ranges
- Add transaction timeout mechanism
- Implement deadlock detection
- **Estimated effort:** 5-7 days
- **Owner:** [Transaction Lead]

**4. Engine Correctness** (5 items)
- Implement proper merge semantics with merge operators
- Implement write stall mechanism (backpressure when L0 stalls)
- Fix WAL rotation to track per-CF triggering
- **Estimated effort:** 3-5 days
- **Owner:** [Engine Lead]

### Medium-term (1 Month)

**5. Instrumentation & Observability** (10 items)
- Add cache LRU eviction metrics
- Add read/write amplification tracking
- Add compaction determinism verification
- **Estimated effort:** 3-4 days
- **Owner:** [Observability Lead]

### Deferred

**Phase 5 Features** (4 items) — Postpone until after core functionality stabilizes
- Health manager validation
- Autotuner initialization
- In-memory WAL refactor (if needed)

---

## 📊 Priority Matrix

| Priority | Count | Category | Timeline |
|----------|-------|----------|----------|
| 🔴 CRITICAL | 12 | Durability/Manifest | Now-Week 1 |
| 🟠 HIGH | 15 | Transactions/Engine | Week 1-3 |
| 🟡 MEDIUM | 18 | Instrumentation/Cloud | Week 2-4 |
| 🔵 LOW | 7 | Phase 5/Polish | Month 2+ |
| **TOTAL** | **52** | | |

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
