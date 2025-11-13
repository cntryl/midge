# TODO (Work In Progress)

This document collects and prioritizes outstanding TODOs found across the repository (extracted from `// TODO` comments and existing wip docs). It is a short, actionable backlog to help triage and assign work.

Summary
- Approximate TODO count: ~134 (see `docs/wip/GAP_ANALYSIS.md` findings)
- Focus areas: durability/WAL tests, transaction semantics & concurrency, engine correctness (merge/write-stalls), and test/instrumentation improvements.

Priority Groups

1) Critical — Durability & correctness (must fix before stable release)
- Fix and instrument WAL & fsync semantics in tests and code (tests reference fsync, torn writes, truncated WAL). Examples:
  - `tests/durability_wal.rs` — TODO: Add test that simulates unfsynced data loss; Add instrumentation to verify fsync was called.
  - `tests/durability_recovery.rs` — TODO: Verify sequence numbers and sync boundaries; Simulate crash during writes.
- Deliverable: tests that deterministically simulate failures + code hooks to assert fsync behavior.

2) High — Transactions & concurrency
- Complete transaction semantics and conflict detection (tests and engine behavior). Examples:
  - `tests/txn_write_write_conflicts.rs` — "TODO: implement conflict detection for overlapping ranges"
  - `src/core/transaction/engine_transaction.rs` — "TODO: Implement transaction-aware scan in engine"
- Deliverable: deterministic transaction tests, conflict detection implementation, and deadlock handling tests.

3) High — Engine correctness under concurrency
- Merge semantics, write-stall behavior, and WAL rotation/flush coordination. Examples:
  - `src/core/memtable/core.rs` — "TODO: Implement proper merge semantics"
  - `src/core/engine/operations/writes.rs` — "TODO: Implement proper write stall mechanism"
  - `src/core/engine/operations/writes.rs` & `transactions.rs` — TODO: Track which CF triggered WAL rotation and flush that one
- Deliverable: fix edge cases that lead to incorrect behavior under contention; add focused tests.

4) Medium — Instrumentation & metrics
- Add metrics and instrumentation to validate LRU eviction, read-amplification, fsync calls, compaction progress. Examples:
  - `tests/read_path_caching.rs` — TODO: Monitor cache metrics to verify LRU eviction
  - various durability/compaction tests note "TODO: Add instrumentation"
- Deliverable: new telemetry hooks and test assertions to make behavior observable in CI.

5) Low/Deferred — Phase 5 features and polish
- Autotuner initialization, merge operator polishing, health manager Phase 5 items. Examples:
  - `src/core/engine/state/initialization.rs` — "TODO: Initialize autotuner if enabled"
  - `src/health/manager.rs` — "TODO: Detailed validation (Phase 5)"
- Deliverable: convert to tracked issues if not planned for immediate implementation.

Cross-cutting test backlog
- Many `tests/*` files include TODOs for additional checks or instrumentation. The `docs/wip/GAP_ANALYSIS.md` already documents the density and priority of these test TODOs.

Suggested triage & next steps
- (1) Triage CSV: generate a CSV with columns: path, line, snippet, suggested owner, priority — then create GitHub issues or a project board. (Suggested automation: script to parse `rg "TODO"` output into CSV.)
- (2) Immediate sprint: pick the top 2 Critical items (durability fsync test, torn-WAL simulation) and drive them to green tests.
- (3) Assign owners for High priorities (transactions, concurrency) and create smaller, testable sub-tasks.
- (4) Convert Low/Phase-5 TODOs into issues labeled `deferred` or `phase-5` so they don't clutter the immediate backlog.

Representative TODOs (sample)
- `tests/durability_wal.rs` (line ~25): "TODO: Add test that simulates unfsynced data loss"
- `tests/txn_transaction_lifecycle.rs` (line ~30): "TODO: Should timeout if transaction exceeds deadline"
- `src/core/memtable/core.rs` (line ~232): "TODO: Implement proper merge semantics"
- `src/core/engine/operations/writes.rs` (line ~93): "TODO: Implement proper write stall mechanism"

If you want, I can:
- produce the CSV with every TODO (path, line, comment) and open a PR with `docs/wip/TODO.md` plus the CSV;
- or create GitHub issues from the CSV and assign labels/priorities.

--
Generated automatically from repository TODO markers and `docs/wip/GAP_ANALYSIS.md`.
