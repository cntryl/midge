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
## Phase 1 (P0) – Minimum Non-Critical Production Bar
Highest leverage: prevents silent corruption & logical inconsistency.

### 1. Error Handling & Fault Injection (8–12 tests)
Critical engine failure paths:
- WAL CRC mismatch recovery
- Manifest JSON corruption reaction
- Fsync failure surfacing
- Disk full across WAL / flush / SST
- SST read I/O error
- Partial SST write detection
- Mid-record WAL corruption (strict vs tolerant)
- Skip corrupted SST file and continue
- Background error propagation & write pausing

### 2. WriteBatch Remaining Atomicity (3–5 tests)
Edge atomicity cases not yet covered:
- Partial WAL crash mid-batch
- Rollback on write error
- Large batch all-or-nothing semantics under crash
- Disk full during batch
- Batch and transaction concurrency consistency

### 3. Merge Operator Error Paths (3–4 tests)
Ensure failures do not corrupt logical values:
- Operator returning error
- Unregistered operator usage
- Merge error during flush/compaction aborts safely
- WAL replay encountering merge error

---
## Phase 2 (P1) – Scale & Logical Domain Integrity
Raises confidence for use as a primary store in most systems.

### 4. Delete Range Core Semantics (8–10 tests)
- Multi-level range deletion
- Overlapping ranges resolution
- Point delete plus range delete interactions
- Application during compaction
- Tombstone retention until safe
- Large range performance sanity
- Restart recovery
- Memtable + SST combined coverage
- Snapshot correctness with ranges
- Prevent resurrection post-compaction

### 5. Iterator Edge Cases (6–8 tests)
- Compaction mid-scan
- Seek after delete
- SST removal mid-iteration
- Memory bounds for large scans
- Greater-than seek correctness
- Seek past end behavior
- Respect range tombstones
- Interleaved put/delete scan correctness

### 6. Durability & Recovery Extensions (8–12 tests)
- Partial flush crash recovery
- WAL vs manifest conflict resolution
- Duplicate WAL replay idempotence
- Out-of-order SST discovery
- Orphaned SST detection
- Manifest rebuild when missing
- Ordered transaction replay guarantees

---
## Phase 3 (P2) – Peripheral Correctness Surfaces
Important but not core risk reducers.

### 7. Column Families (6–8 tests)
- Deletion during active transaction
- Handle invalidation after deletion
- Metadata persistence after crash
- Default CF protection
- CF limits (count, name length)
- Per-CF compaction override semantics

### 8. Snapshots (6–8 tests)
- Long-lived snapshot blocking compaction
- Memory overhead validation
- Crash recovery with active snapshot
- Snapshot + compaction coexistence
- Expiration / release semantics

### 9. Checkpoints (6–8 tests)
- Consistency during heavy writes
- Crash mid-checkpoint (partial copy handling)
- Integrity verification on restore
- Incremental checkpoint behavior (if introduced)

---
## Deferred (Chaos / Non-Deterministic)
Move to separate repo later:
- Random crash injection / power loss simulation
- Network/cloud flakiness & latency spikes
- Long-running soak (24h) tests
- Fuzzing / model-based randomized sequences
- Disk sabotage / partial sector corruption

---
## Implementation Mechanics
- Group related tests by feature into new `tests/*_errors.rs`, `tests/*_edge.rs` files.
- Use `TestHooks` for corruption and failure simulation; add new behaviors if needed (e.g., disk full simulation stub layer) via feature gating.
- Avoid `#[ignore]` long-term; temporary stubs may start ignored but should graduate quickly.
- Keep each test focused on ONE behavior.

---
## Initial File Layout Additions
```
tests/
  error_handling_core.rs          # Phase 1 error injection
  write_batch_atomicity_edge.rs   # Remaining batch atomicity tests
  merge_operator_errors.rs        # Merge error path tests
  delete_range_core.rs            # Phase 2 range semantics
  iterator_edge_cases.rs          # Iterator correctness
  durability_recovery_edge.rs     # Extended recovery semantics
  column_family_edge.rs           # Phase 3 CF tests
  snapshot_lifecycle.rs           # Phase 3 snapshots
  checkpoint_consistency.rs       # Phase 3 checkpoints
```
(Only create when starting each phase to reduce code churn.)

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
