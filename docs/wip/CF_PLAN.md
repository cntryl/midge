# Column Family (CF) Completion Plan

Last updated: 2025-11-10

This document captures the current state of column-family support in Midge, known gaps, and a prioritized plan to reach full (100%) per-column-family feature parity. It is intended as a short, actionable roadmap for engineers working on CF completion.

## 1. Goal / Success Criteria

We consider column families "100% implemented" when the following hold:
- Per-CF identifiers, handles, and configs are exposed and persisted.
- All I/O layers (WAL, SST, manifest) encode and preserve CF identity.
- Engine APIs allow creating, listing, retrieving (by name) and dropping CFs safely.
- All read/write/merge/flush/compaction code paths correctly honor the CF ID and per-CF configuration (bloom, compression, merge operator, TTL, etc.).
- Transactional semantics and snapshot isolation are correct across CFs (where promised by the API).
- Tests cover the above and there are no TODOs that imply correctness bugs in per-CF behavior.

## 2. Current status (short)

Broadly implemented and usable today. Evidence:
- `ColumnFamilyId`, `ColumnFamilyHandle`, `ColumnFamilyConfig` types exist and are tested (`src/api/column_family.rs`).
- In-memory CF state (`ColumnFamily`, `ColumnFamilySet`) implemented and default CF created at startup (`src/core/engine/column_family.rs`).
- Public engine APIs to create / drop / list / get CFs are implemented (`src/core/engine/cf_manager.rs`) and persist to manifest.
- WAL encodes CF id; replay ignores dropped/nonexistent CFs (`src/wal/*`, WAL tests).
- Flush and SST naming include CF id; manifest stores per-file cf_id and key/seq bounds (`src/core/flush.rs`, `src/core/manifest/*`).
- KvStore adapter and engine read/write/merge APIs are CF-scoped and exercised in tests.

## 3. Known gaps (blocking items)

These are the issues preventing us from marking CF work as 100% complete.

1) Per-CF merge operator resolution
- Location: `src/core/engine/cf_manager.rs` (method `resolve_merges` / `resolve_merges` usage).
- Current behavior: merge resolution during flush/compaction uses the DEFAULT CF id when looking up the merge operator.
- Impact: if different CFs register different merge operators, the wrong operator may be used leading to incorrect merge resolution.
- Severity: correctness (high)

2) Per-CF flush coordination and enqueueing
- Location: TODOs in `src/core/engine/operations/writes.rs`, `src/core/engine/column_family.rs` and related flush coordination logic.
- Current behavior: flush logic works but some comments/TODOs indicate a plan to enqueue per-CF FlushJobs and use per-CF immutable queues; current implementation still uses legacy/global coordination in places.
- Impact: mostly performance and clarity; possible race/ordering complexity for advanced scenarios (medium)

3) Snapshot isolation / multi-CF transactional consistency
- Location: several TODOs in transaction and mutation paths (`operations/*`, `transaction/*`).
- Current behavior: basic transactional flow exists, but comments identify missing snapshot-isolation/consistency refinements across CFs.
- Impact: affects strong transactional guarantees across CFs (high for transactional users)

4) Phase TODOs and coordination polish
- Several Phase 4/5 TODOs: background flush coordination, atomic WAL-to-SST per-CF tracking, metrics per-CF refinements.
- Impact: engineering completeness and performance, not necessarily correctness for simple workloads (low → medium)

## 4. Non-blocking / low priority items
- Performance tweaks (per-CF block_cache sizing, level size tuning) are available in config, and can be further tuned.
- Documentation and examples: more samples showing CF usage, per-CF merge operator examples, and migration notes.

## 5. Recommended fixes and implementation plan (prioritized)

Short-term (1–3 days)
- Fix per-CF merge resolution (high priority, small change)
  - Change `resolve_merges` to use the passed `cf_id` instead of `DEFAULT_CF_ID` when looking up the merge operator.
  - Add unit tests that register distinct merge operators on two CFs and verify resolution behavior per-CF.
  - Files: `src/core/engine/cf_manager.rs`, tests in `src/core/engine/` (or `tests/`).

- Add a focused unit test for merge resolution during flush that exercises the flush path and asserts correct resulting SST contents or memtable contents.

Medium-term (1–2 weeks)
- Implement per-CF flush enqueueing & immutable queue consumption
  - Wire write paths to enqueue CF-specific `FlushJob` objects instead of calling the legacy global `flush()` for all CFs.
  - Ensure `ColumnFamily::pop_immutable()` is used by background flush workers in a safe, lock-free (where possible) manner.
  - Add integration tests that simulate concurrent writes to multiple CFs and verify flush ordering, no stalls when one CF is hot, and correct manifest updates.

- Snapshot isolation across CFs
  - Audit transaction read/write tracking code and ensure snapshot allocations and read-sets are CF-aware.
  - Add tests for multi-CF transactions (conflict, isolation, deadlock detection scenarios).

Longer-term (2–4 weeks)
- Performance and instrumentation
  - Per-CF metrics (flush latency, memtable pressure, SST counts) and dashboards.
  - Per-CF compaction tuning and sublevel placement heuristics.

- Migration tooling & docs
  - Document on-disk format, manifest entries and migration steps for existing deployments.

## 6. Tests to add (minimal set to validate correctness)

- Unit: per-CF merge operator test
  - Register operator A on CF `alpha`, operator B on CF `beta`.
  - Insert merges and flush/resolve; assert resolved values match operator semantics per CF.

- Integration: per-CF flush ordering and WAL-to-SST mapping
  - Drive writes to multiple CFs until memtable rollovers occur.
  - Verify created SSTs contain correct `cf_id` and manifest entries match.

- Transaction: multi-CF snapshot isolation
  - Start transaction, perform reads/writes across two CFs, commit with conflicting concurrent writer and assert proper conflict/no-conflict per isolation guarantees.

## 7. Checklist to mark CF work "100%"

- [ ] Per-CF merge operator resolution implemented and covered by tests
- [ ] Per-CF flush enqueueing and background worker consumption implemented and covered by tests
- [ ] Snapshot isolation behavior across CFs validated with tests
- [ ] No TODOs remaining that describe correctness issues (phase TODOs limited to cosmetic/perf only)
- [ ] Documentation updated: API docs + examples showing per-CF usage, merge operator registration, drop/creation caveats
- [ ] Manifest/migration notes added

## 8. Safety & migration considerations

- Dropping CFs deletes on-disk SST files and removes manifest entries; we already perform best-effort cleanup and persist manifest updates atomically.
- When changing on-disk formats or manifest layout, prefer a manifest version bump and code that supports both old and new formats for a migration window.

## 9. Owner / estimated effort

- Short-term (merge operator fix + tests): 1 engineer, ~4 hours including tests and running suite.
- Medium-term (CF flush + tests): 1–2 engineers, ~1–2 weeks depending on integration complexity.
- Snapshot isolation improvements: 1 engineer, ~3–5 days to implement and test.

## 10. Next steps (recommended immediate action)

1. Implement the per-CF merge operator fix (small, high-impact).
2. Add unit tests to prove correct behavior.
3. Run the full test suite and address failures.

If you want, I can implement item (1) now and open a PR with tests. Say the word and I will prepare the patch and CI run.

---

Notes
- This plan is intentionally pragmatic: prioritize correctness (merge operator) first, then flush coordination, then transactional polish and performance.
- File authoring done by automated code review on 2025-11-10; please review and assign owners/tickets in your issue tracker.
