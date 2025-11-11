# Gap Analysis — SPEC vs current requirements/tests

Purpose
-------
This document reconciles the authoritative behavioral contract in `docs/wip/SPEC.md` (the canonical spec) with the current requirements/test surface captured in `docs/wip/REQUIREMENTS.md` and related gap analysis. The goal is to provide a prioritized list of missing behaviors (mapped to SPEC sections), suggested test names (where not already present), counts, and a short implementation roadmap.

Quick headline
--------------
SPEC defines ~22 top-level sections with fine-grained acceptance tests. The repository already implements a large portion of those behaviors (many test names exist in `REQUIREMENTS.md`), but mapping SPEC→tests reveals remaining gaps concentrated in: durability/fsync boundaries, compaction end-to-end, exactly-once recovery semantics, iterator lifecycle and stability under compaction, admin API concurrency and shutdown semantics, and several versioning/compatibility items. Estimated missing tests after incorporating SPEC: ~140–160 total (overlap with prior gap counts), with ~40–60 high/critical priority tests to implement first.

Methodology
-----------
- Compare each SPEC acceptance item to the implemented/identified test names in `REQUIREMENTS.md` and the earlier gap analysis.
- Mark as: Covered, Partially covered (stubbed/integration only), or Missing.
- Prioritise by risk (data-loss/corruption = Critical), recurrence (production likelihood), and ease-of-testing.

Summary mapping (top-level SPEC sections)
----------------------------------------
I'll summarize each SPEC section with coverage status, key missing behaviors and example test names to add.

1) WAL & Durability (SPEC §1)
- Coverage status: Partially covered. Many WAL filesystem tests exist (WAL-FS-*) but SPEC adds explicit commit-durability guarantees and fsync-boundary crash tests.
- Key missing behaviors:
  - commit durability: tests that assert "commit only acknowledged after WAL fsync" (SPEC: 1.5)
  - crash between append and fsync behavior
- Example tests to add:
  - `should_not_acknowledge_commit_given_wal_unsynced_when_crash_occurs` (SPEC acceptance already listed)
  - `should_recover_without_loss_given_crash_after_wal_append_before_fsync`
- Priority: Critical

2) Memtable & Indexing (SPEC §2)
- Coverage status: Mostly covered by memtable/skiplist tests; concurrency tests exist but some multi-thread stress tests are missing.
- Missing behaviors: large-scale sequence monotonicity under concurrent load and freeze/handoff boundary tests.
- Example tests:
  - `should_generate_strictly_increasing_sequence_numbers_given_parallel_writes`
  - `should_route_new_writes_to_new_memtable_given_freeze_in_progress_when_full`
- Priority: High

3) SST Files & Compaction (SPEC §3)
- Coverage status: Mixed. Many compaction unit tests implemented, but WSST and COLL integration tests are stubbed and end-to-end compaction + manifest atomicity tests are missing.
- Missing behaviors: deterministic merges across runs, atomic commit of compaction outputs (fsync ordering), cleanup on failure.
- Example tests:
  - `should_commit_new_ssts_and_manifest_together_given_compaction_successful`
  - `should_cleanup_partial_output_given_compaction_failure`
- Priority: Critical

4) Read Path & Caching (SPEC §4)
- Coverage status: Largely covered (checksum, cache), but read-amplification bounds and cache policy under pressure need more integration tests.
- Missing behaviors: amplification measurements, paranoid mode behavior across many blocks.
- Priority: Medium

5) Concurrency & Backpressure (SPEC §5)
- Coverage status: Partial. Some tests for flush/compaction overlap exist; write stall behavior is under-tested.
- Missing behaviors: deterministic backpressure metrics and fairness tests.
- Priority: High

6) Transactions & Isolation (SPEC §6)
- Coverage status: Many transaction API tests exist, but read-your-writes and cross-transaction isolation must be reinforced with crash and concurrency tests.
- Missing behaviors: visibility across restarts, long-running transaction rollback during crashes.
- Priority: Critical

7) Error Handling & Recovery (SPEC §7)
- Coverage status: Many tests, but torn-write detection, partial write recovery and behavior under persistent I/O errors need more integration coverage.
- Missing behaviors: partial block recovery, truncated WAL replay invariants.
- Priority: Critical

8) Cloud Integration (SPEC §8)
- Coverage status: Cloud mock coverage is very good; real-backend integration is missing and local-preservation-until-verified semantics need explicit tests.
- Missing behaviors: preserve local SST until cloud verification persists in manifest.
- Priority: High

9) Multi-Column Families (SPEC §9)
- Coverage status: API tests implemented; compaction and isolation tests across CFs partially missing.
- Missing behaviors: CF lifecycle across drop/recreate and CF-scoped compaction boundaries.
- Priority: Medium

10) Manifest Management (SPEC §10)
- Coverage status: Many manifest tests present. However SPEC demands manifest always reflect durable SSTs; tests verifying atomicity across fsyncs are needed.
- Missing behaviors: manifest vs WAL truncation race tests.
- Priority: Critical

11) Backup & Restore (SPEC §11)
- Coverage status: Backup API tests exist; end-to-end restore and cross-version restore tests are missing.
- Priority: Medium

12) Snapshot Semantics (SPEC §12)
- Coverage status: Snapshot API tests exist; snapshot persistence across restarts and snapshot cleanup tests need more coverage.
- Priority: High

13) Iterator & Range Scan (SPEC §13)
- Coverage status: Many scan tests exist but iterator lifecycle (rewind/close/invalidated by compaction) is under-specified in tests.
- Missing behaviors: `should_continue_iteration_given_compaction_in_progress` and iterator use-after-close errors.
- Priority: High

14) Crash & Shutdown (SPEC §14)
- Coverage status: Partially covered via durability tests, but SPEC requires clean shutdown semantics (fsync all memtables) and crash-resilience quantification.
- Missing behaviors: `should_persist_all_memtables_given_shutdown_signal_when_clean_exit` and abort/completion semantics for uploads/compactions on shutdown.
- Priority: Critical

15) Durability Model (SPEC §15)
- Coverage status: Largely absent as an explicit, configurable model in code/tests; earlier gap analysis recommended adding explicit modes (None, WALStrict, FullSync, CloudReplicated) and tests.
- Missing behaviors: tests asserting invariants per durability mode.
- Priority: Critical

16) Observability & Metrics (SPEC §16)
- Coverage status: Basic metrics exist; tests to bind metrics to operations are missing.
- Example tests: `should_expose_metric_endpoint_given_metrics_enabled_when_server_running`
- Priority: Medium

17) Configuration & Runtime (SPEC §17)
- Coverage status: Validation tests are present; hot reload and idempotence tests are missing.
- Priority: Low–Medium

18) Resource Management (SPEC §18)
- Coverage status: Some coverage; file descriptor exhaustion and memory budget tests needed.
- Priority: High

19) Compatibility & Versioning (SPEC §19)
- Coverage status: Partial; encoding/decoding and manifest versioning tests have many stubs; cross-version compatibility tests are missing.
- Priority: Medium

20) Performance & Scalability (SPEC §20)
- Coverage status: Criterion benchmarks present; required regression tests are missing.
- Priority: Low–Medium

21) Security & Integrity (SPEC §21)
- Coverage status: checksums tested; manifest integrity (signing/hashes) is not implemented.
- Priority: Low (future-facing)

22) System Integration & Admin (SPEC §22)
- Coverage status: Admin API and backup during load semantics need concurrency tests.
- Priority: Medium

Concrete missing test lists (high-impact / critical)
-----------------------------------------------
Below are the highest-impact tests to implement first (grouped and prioritized). Many of these map one-to-one to SPEC acceptance lines.

A. Durability / fsync boundary tests (Critical, implement first)
- `should_not_acknowledge_commit_given_wal_unsynced_when_crash_occurs` (SPEC 1.5)
- `should_recover_without_loss_given_crash_after_wal_append_before_fsync` (SPEC §1 + earlier)
- `should_preserve_consistency_given_crash_between_sst_write_and_manifest_update` (SPEC §3.4 + §10.2)
- `should_fsync_sst_and_update_manifest_before_wal_truncation` (DUR-FLUSH derived)
- `should_delete_old_sst_files_only_after_manifest_persisted` (DUR-COMP derived)

B. Compaction end-to-end / atomicity (Critical)
- `should_commit_new_ssts_and_manifest_together_given_compaction_successful` (SPEC 3.4)
- `should_cleanup_partial_output_given_compaction_failure` (SPEC 3.5)
- `should_recover_consistent_state_given_crash_mid_compaction_when_restart` (SPEC 14.3)

C. Recovery idempotence & exactly-once (Critical)
- `should_detect_and_ignore_already_compacted_wal_entries_given_manifest_sequence` (derived)
- `should_replay_to_last_synced_sequence_given_fullsync_mode_when_recover` (SPEC 15.2)

D. Iterator lifecycle & stability (High)
- `should_return_error_given_iterator_used_after_close` (SPEC 13.1)
- `should_continue_iteration_given_compaction_in_progress_when_scan` (SPEC 13.2)
- `should_rewind_iterator_to_start_given_reset_called` (implicit)

E. Shutdown and admin semantics (High)
- `should_flush_and_fsync_all_memtables_given_shutdown_signal` (SPEC 14.1)
- `should_abort_long_running_uploads_given_shutdown_signal` (implicit/cloud safety)
- `should_block_backup_start_given_active_compaction_when_requested` (SPEC 22.1)

Estimated counts & ordering
---------------------------
- Immediate critical tests (A+C+B): ~30–40 tests (durability, compaction atomicity, recovery idempotence)
- Next-phase (D+E+others): ~30 tests (iterators, admin, metrics, CF lifecycle)
- Remaining medium/low: ~70–90 tests (compatibility, perf, cloud real-backend, signing)

Implementation roadmap (concrete)
---------------------------------
Phase 0: Infra (2–3 days)
- Build a crash injection harness (scripted process termination / fsync toggles) and helper test utilities that run tests in temp dirs and emulate crashes.
- Add a test helper crate/module (e.g., `tests/util/crash_harness.rs`) to centralize patterns.

Phase 1: Durability (Weeks 1–2)
- Implement A (Durability/fsync) tests; add small integration harness tests using the crash helper.
- Make minimal code instrumentation if necessary (hooks to force fsync or to observe fsync callpoints) guarded by test-only features.

Phase 2: Compaction integration (Weeks 3–4)
- Implement WSST + COLL integration tests using temp directories and the SST factory.
- Add tests for manifest atomicity and cleanup on failure.

Phase 3: Recovery & Iterator/Shutdown (Weeks 5–6)
- Implement exactly-once recovery tests and iterator lifecycle tests.
- Add admin concurrency checks (backup vs compaction, CF drop behavior).

Phase 4: Cloud/Compatibility/Performance (Weeks 7–10)
- Real cloud backend integration tests (gated by env vars/CI secrets).
- Cross-version compatibility tests, performance regression harness.

Tactical next step options (pick one)
------------------------------------
A) Implement the crash/fsync harness and a single canonical test: `should_recover_without_loss_given_crash_after_wal_append_before_fsync`. This gives a repeatable pattern to follow.

B) Scaffold WSST + COLL tests and implement one end-to-end compaction atomicity test.

C) Produce a tracked checklist file (`docs/wip/tests-to-implement/`) enumerating each high-priority test as an issue-like entry so contributors can pick tasks.

My recommendation: choose A first (fastest high-value win). I can implement A now: add the test helper, a minimal harness, and one passing test (or the test scaffold if the project needs further locally-run setup). If you want B or C instead, say so and I’ll do that.

---

Files added in this step
- `docs/wip/GAP_ANALYSIS_SPEC.md` (this file)

Next actions
------------
Tell me which tactical next step to run (A/B/C). If A, I will:
- add a test helper in `tests/util/` (Rust test helper module) and a new integration test file `tests/durability/crash_fsync.rs` implementing `should_recover_without_loss_given_crash_after_wal_append_before_fsync` using temp directories and the harness; then run tests locally (if feasible) and report results.

If you prefer B or C, I'll implement that instead.