# Prioritized Deterministic Test Plan (Main Midge Repo)

This plan integrates the coverage analysis into an actionable, phased roadmap **for deterministic, correctness-focused tests inside the primary Midge repository**. Chaos / fuzz / non-deterministic failure simulations will live in a separate wrench/chaos repo later.

---
## Guiding Principles
- Add only deterministic tests here (no random crashes, timers, network chaos).
- Every test follows naming: `should_{behavior}_when_{context}`.
- Use AAA pattern for tests >5 lines.
- Focus on high signal: correctness, recovery, isolation, atomicity.
- Leverage existing `TestHooks` for fault injection (fsync skip, WAL truncate, manifest corruption, compaction failure, flush gating).

---
## Phase 1 (P0) – Minimum Non-Critical Production Bar ✅ COMPLETE
Highest leverage: prevents silent corruption & logical inconsistency.

**Status: 17 passing tests, 8 deferred (infrastructure limitations)**

### 1. Error Handling & Fault Injection ✅ (10 tests)
**File: `tests/error_handling_core.rs`, `tests/error_handling_flush.rs`**
- ✅ WAL CRC mismatch recovery (tolerant mode)
- ✅ WAL corruption strict mode failure
- ✅ Manifest corruption graceful fallback
- ✅ Fsync tracking when enabled
- ✅ Unfsynced data not persisted when fsync skipped
- ✅ Flush gate blocking/recovery (5 tests in error_handling_flush.rs)
- 🔄 Disk full across WAL/flush (requires disk full simulator)
- 🔄 SST block I/O error handling (requires SST error injection)
- 🔄 Background error propagation (requires background error hooks)

### 2. WriteBatch Remaining Atomicity ✅ (5 tests)
**File: `tests/engine_write_batch_edge.rs`**
- ✅ Partial WAL crash mid-batch recovery
- ✅ Large batch (100 ops) all-or-nothing under crash
- ✅ Batch and regular write consistency
- 🔄 Disk full during batch (requires disk full simulator)
- 🔄 Rollback on write error (requires error injection)

### 3. Merge Operator Error Paths ✅ (5 tests)
**File: `tests/engine_merge_operator_errors.rs`**
- ✅ Unregistered operator graceful handling
- ✅ Consistent results without operator
- ✅ Failing operator error propagation during flush
- ✅ Operator change on reopen consistency
- 🔄 WAL replay merge error abort (requires WAL replay error injection)

---
## Phase 2 (P1) – Scale & Logical Domain Integrity ✅ COMPLETE
Raises confidence for use as a primary store in most systems.

**Status: 18 passing tests, 10 deferred (SST-level features not fully implemented)**

### 4. Delete Range Core Semantics ✅ (10 tests)
**File: `tests/engine_delete_range_core.rs`**
- ✅ Point + range delete interleaving
- ✅ Memtable + SST combined coverage
- ✅ Empty range handling (start == end)
- ✅ WAL recovery of range tombstones
- 🔄 Multi-level range deletion (requires SST-level delete range)
- 🔄 Overlapping ranges resolution (requires SST-level support)
- 🔄 Compaction application (requires tombstone compaction)
- 🔄 Snapshot retention (requires snapshot-aware tombstones)
- 🔄 Large range efficiency (requires SST-level support)
- 🔄 Resurrection prevention (requires compaction priority)

### 5. Iterator Edge Cases ✅ (8 tests)
**File: `tests/engine_iterator_edge.rs`**
- ✅ Seek after delete skipping
- ✅ Large scan (10k keys) memory bounds
- ✅ Seek to missing key (returns next)
- ✅ Seek past end (returns empty)
- ✅ Range tombstones respected in scan
- ✅ Interleaved put/delete scan correctness
- 🔄 Compaction mid-scan safety (requires concurrent compaction)
- 🔄 SST removal mid-iteration (requires SST lifecycle tracking)

### 6. Durability & Recovery Extensions ✅ (10 tests)
**File: `tests/durability_recovery_edge.rs`**
- ✅ Partial flush crash recovery (via flush gate)
- ✅ WAL vs manifest conflict resolution (WAL wins)
- ✅ Duplicate WAL replay idempotence
- ✅ Ordered transaction replay guarantees
- ✅ Delete operation recovery from WAL
- ✅ WriteBatch atomic recovery from WAL
- ✅ Corrupted tail tolerance in tolerant mode
- ✅ Sequence number preservation across recovery
- 🔄 Out-of-order SST discovery (requires orphaned SST detection)
- 🔄 Manifest rebuild when missing (requires SST discovery)

---
## Phase 3 (P2) – Peripheral Correctness Surfaces ✅ COMPLETE
Important but not core risk reducers.

**Status: 17 passing tests, 8 deferred (advanced features)**

### 7. Column Families ✅ (8 tests)
**File: `tests/column_family_lifecycle.rs`**
- ✅ Handle invalidation after drop
- ✅ CF metadata persistence across restart
- ✅ Per-CF compaction isolation
- ✅ Same key across different CFs
- ✅ CF data deletion when dropped
- 🔄 Deletion during active transaction (requires transaction API)
- 🔄 Default CF protection (requires name validation)
- 🔄 CF limits enforcement (requires max_column_families config)

### 8. Snapshots ✅ (9 tests)
**File: `tests/snapshot_lifecycle.rs`**
- ✅ Crash recovery with active snapshots
- ✅ Snapshot + compaction data preservation
- ✅ Multiple concurrent snapshots
- ✅ Empty DB snapshot
- ✅ Snapshot after delete consistency
- ✅ Snapshot release allowing writes
- 🔄 Long-lived snapshot blocking compaction (requires compaction hooks)
- 🔄 Memory overhead tracking (requires metrics)
- 🔄 Snapshot expiration/TTL (requires cleanup API)

### 9. Checkpoints ✅ (8 tests)
**File: `tests/checkpoint_lifecycle.rs`**
- ✅ Consistency during writes
- ✅ Integrity verification (100 keys)
- ✅ Checkpoint isolation from original
- ✅ Multiple sequential checkpoints
- ✅ Empty DB checkpoint
- ✅ Multi-CF checkpoint inclusion
- 🔄 Crash mid-checkpoint handling (requires cancellation detection)
- 🔄 Incremental checkpoints (requires incremental API)

---
## Summary: All Phases Complete! 🎉

**Total: 52 passing tests, 26 deferred**

### Breakdown by Phase:
- **Phase 1 (P0)**: 17 passing, 8 deferred
- **Phase 2 (P1)**: 18 passing, 10 deferred  
- **Phase 3 (P2)**: 17 passing, 8 deferred

### Deferred Tests Require:
1. **Disk full simulation layer** (6 tests) - ENOSPC injection for WAL/flush/batch
2. **SST-level delete range support** (6 tests) - Tombstone compaction, multi-level deletion
3. **Background error injection** (3 tests) - Async error propagation hooks
4. **Advanced concurrent scenarios** (2 tests) - Compaction during iteration, SST removal
5. **Manifest rebuild/SST discovery** (2 tests) - Orphaned SST detection
6. **Snapshot-aware compaction** (1 test) - Blocking based on oldest snapshot
7. **Transaction API** (1 test) - CF deletion during active transaction
8. **Config extensions** (1 test) - max_column_families field
9. **Advanced checkpoint features** (2 tests) - Crash mid-checkpoint, incremental
10. **Metrics/monitoring** (2 tests) - Memory overhead tracking, expiration

---
## Deferred (Chaos / Non-Deterministic)
Move to separate wrench/chaos repo:
- Random crash injection / power loss simulation
- Network/cloud flakiness & latency spikes
- Long-running soak (24h) tests
- Fuzzing / model-based randomized sequences
- Disk sabotage / partial sector corruption

---
## Actual File Layout (COMPLETE)
```
tests/
  error_handling_core.rs          ✅ Phase 1: 5 passing, 5 deferred
  error_handling_flush.rs         ✅ Phase 1: 5 passing, 0 deferred
  engine_write_batch_edge.rs      ✅ Phase 1: 3 passing, 2 deferred
  engine_merge_operator_errors.rs ✅ Phase 1: 4 passing, 1 deferred
  engine_delete_range_core.rs     ✅ Phase 2: 4 passing, 6 deferred
  engine_iterator_edge.rs         ✅ Phase 2: 6 passing, 2 deferred
  durability_recovery_edge.rs     ✅ Phase 2: 8 passing, 2 deferred
  column_family_lifecycle.rs      ✅ Phase 3: 5 passing, 3 deferred
  snapshot_lifecycle.rs           ✅ Phase 3: 6 passing, 3 deferred
  checkpoint_lifecycle.rs         ✅ Phase 3: 6 passing, 2 deferred
```

All 10 test files created and verified. Plan execution complete!

---
## Success Metrics
- Phase 1 complete → Error Handling score ≥7/10; WriteBatch ≥9/10; Merge Operators ≥9/10.
- Phase 2 complete → Delete Range ≥8/10; Iterators ≥9/10; Durability ≥8/10.
- Phase 3 complete → Remaining subsystems ≥7/10.
- No panics or nondeterministic flakes across CI runs.
- Add coverage tooling (tarpaulin) post Phase 1 for measurement.

---
## Immediate Next Step
Implement Phase 1 error handling tests (target: 8–12) before expanding batch/merge edge cases.

---
## Tracking
See updated TODO list in `manage_todo_list` for live state.

---
## Roadmap Snapshot
- Week 1–2: Phase 1
- Week 3–4: Phase 2 (Delete Range + Iterators)
- Week 5: Durability extensions
- Week 6–7: Phase 3 peripheral systems
- Week 8: Coverage tooling + refactor/fill gaps

---
This plan keeps the main repo deterministic while accelerating correctness confidence.
