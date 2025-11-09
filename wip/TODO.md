# Work-in-progress TODO: Column Family Improvements

Date: 2025-11-09

This document captures the recommended changes and the prioritized test/workplan for the column-family subsystem following a code review and gap analysis vs RocksDB.

NOTE: We will focus on comprehensive unit tests now. Integration tests are deferred to a later, dedicated integration-test phase.

---

## Summary of immediate fixes applied

1. Fix manifest/CF create inconsistency: if persisting the manifest fails after inserting the CF into in-memory maps, the in-memory CF registration is now rolled back (best-effort) to avoid durable vs in-memory mismatch. (See: `src/core/engine/engine.rs`)

2. Fix drop ordering bug: collect SST filenames belonging to a CF before removing them from the manifest, persist the manifest, then delete SST files (best-effort). This prevents orphan SSTs being left on disk. (See: `src/core/engine/engine.rs`)

These are already committed in the current branch.

---

## Priority 1 — Safety & correctness (must do)

1. Add unit tests that verify the two immediate fixes:
   - create rollback test: simulate manifest.save failure (or use a hook) and assert in-memory CF entries are removed and create returns error.
   - drop deletion test: create temporary SST files with manifest entries for CF X, call `drop_column_family()` and assert files are removed and manifest no longer contains the CF.

   Files to add:
   - `tests/cf_create_rollback.rs` (unit)
   - `tests/cf_drop_delete_files.rs` (unit)

   Acceptance criteria:
   - Tests pass locally.
   - Cover edge cases: non-existent files, permission failures (best-effort assertions for cleanup).

2. Prevent unsafe CF drops while in-memory state is active.
   - Short-term: refuse to drop a CF if active memtable is non-empty or if immutable queue is non-empty. Return a clear error message instructing the caller to flush/close the CF first.
   - Long-term: implement safe drop sequence (see Priority 2).

   Where to change:
   - `src/core/engine/engine.rs` — `drop_column_family()` early validation.

   Acceptance criteria:
   - Attempting to drop a CF with unflushed data returns an error (unit test added).

3. Implement unit tests for isolation across column families:
   - Ensure `put_cf` / `get_cf` / `delete_cf` are isolated per CF and don't incorrectly read other CFs' SSTs.
   - Tests: `tests/cf_isolation.rs`.

---

## Priority 2 — Per-CF lifecycle & flush behavior (important)

1. Implement per-CF flush API and coordination.
   - Add `flush_cf(cf: &ColumnFamilyHandle)` that:
     - Freezes active memtable for CF.
     - Flushes all immutable memtables for that CF to SST(s).
     - Updates manifest with new SST file entries referencing the CF id.
     - Waits for flush durability (manifest persisted) before returning.
   - Wire `drop_column_family()` to call `flush_cf()` (or require it) before deletion.

   Files to edit/implement:
   - `src/core/engine/engine.rs` — add `flush_cf` implementation and call site in `drop_column_family`.
   - `src/core/flush_coordinator.rs` (or extend existing flush coordinator) to accept per-CF flush jobs.

   Acceptance criteria:
   - Unit tests that call `flush_cf` and assert manifest updates and SST creation for that CF.

2. Implement write-stall semantics instead of returning an error when immutable queue is full.
   - Design a per-CF condition variable or wait mechanism that blocks/parks writers until immutables are drained below threshold.
   - Add unit test that fills memtable/immutables and asserts writes block and resume after flush.

---

## Priority 3 — API parity & features vs RocksDB (medium-term)

These are enhancements to bring closer to RocksDB parity. Prioritization may vary by user needs.

1. Comparator & Merge Operators
   - Add ability to supply a per-CF comparator and merge operator at CF creation time.
   - Wire comparator into SST creation and memtable ordering.
   - Wire merge operator into merge resolution in compaction/reads.

2. Per-CF table factory & block cache wiring
   - If `ColumnFamilyConfig.block_cache_size` is Some, create a per-CF block cache and ensure SST readers use it.
   - Support different on-disk table formats if needed (block-based, plain, hash table, etc.).

3. Compaction filters and per-CF compaction tuning
   - Add support for per-CF compaction filters (user hooks to drop/modify keys during compaction).
   - Provide more per-CF compaction knobs (compaction priorities, scheduling weight).

4. Per-CF metrics
   - Expose read/write/flush/compaction counters per CF for observability.

---

## Priority 4 — Tests & CI (deferred integration)

1. Integration test plan (phase 2 — large):
   - Multi-CF workloads with concurrent writers and compactions.
   - Crash-recovery tests that create CFs, write data, crash, and on restart validate CFs and SSTs restored.
   - Cloud migration tests where CF SSTs are uploaded/downloaded.

2. For now: focus on comprehensive unit tests and small, fast integration smoke tests later.

## Files changed in this review

- `src/core/engine/engine.rs` — added rollback for CF creation manifest save, fixed drop ordering and file deletion ordering.

(See commit(s) on branch `main` for diffs.)

---

## Implementation notes & assumptions

- We prefer minimal, incremental changes. The immediate fixes are low-risk and already applied.
- For some tests we may need to inject small test hooks (e.g., to force manifest save failure). If injecting test hooks is undesirable, tests should simulate failures using filesystem permissions or tempdir behavior.
- Integration tests (crash recovery, compaction correctness) are deferred to later because they are heavier and will require more infra/time.

---

## Next actions (short-term developer tasks)

1. Add the unit tests listed above (Priority 1) and run them locally. (High priority)
2. Implement `flush_cf` and wire into `drop_column_family` (Priority 2).
3. Implement write-stall mechanism (Priority 2).
4. Add per-CF metrics and per-CF block-cache wiring (Priority 3).

If you want I can start with (1) and add the unit tests now.

---

If anything here should be reordered or clarified, say which part you want me to start with and I will implement it and update this TODO accordingly.
