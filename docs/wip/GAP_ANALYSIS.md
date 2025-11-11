# Gap Analysis — Midge requirements (concise)

This file summarizes missing behaviors, priorities, and recommended next work derived from `docs/wip/REQUIREMENTS.md` and the newly-enumerated implicit/system-bound behaviors (fsync boundaries, iterator lifecycle, versioning, shutdown, metrics, admin concurrency, config idempotence, CF lifecycle, OS limits, security).

## One-line summary

~35–40 new implicit tests were enumerated and added to `REQUIREMENTS.md`. Combined with the previously identified ~115 missing tests, implementing all missing behaviors would add roughly 150 tests across the codebase. Focus first on durability (fsync/crash), compaction integration, and recovery semantics.

## High-level gaps (top priorities)

1. Durability / fsync boundaries (Critical)
   - Missing explicit crash-point tests for WAL, manifest, SST write, compaction output.
   - Representative tests (implement first):
     - `should_recover_without_loss_given_crash_after_wal_append_before_fsync`
     - `should_preserve_consistency_given_crash_between_sst_write_and_manifest_update`
     - `should_recover_partial_compaction_output_given_crash_after_partial_sst_creation`
   - Why priority: These prevent acknowledged-write loss and data corruption.

2. Compaction integration (WSST + COLLECT) (Critical)
   - Missing integration tests exercising file I/O, manifest updates, and compactor atomicity.
   - Representative tests:
     - `WSST-NEW-008`: should_cleanup_partial_output_given_compaction_failure
     - `COLL-NEW-003`: should_merge_entries_given_overlapping_key_ranges
   - Why priority: Compaction bugs cause silent data loss or duplicated keys on recovery.

3. WAL ↔ Manifest coordination and recovery idempotence (High)
   - Ensure WAL truncation, manifest fsync and SST deletion ordering are strictly tested.
   - Representative tests:
     - `DUR-MAN-001`: should_not_truncate_wal_given_manifest_save_failure
     - `DUR-RECOV-001`: should_detect_and_ignore_already_compacted_wal_entries_given_manifest_sequence

4. Iterator & snapshot lifecycle (High)
   - Iterator invalidation, reset/reuse and snapshot checkpoints.
   - Representative tests:
     - `should_return_error_given_iterator_used_after_close`
     - `should_resume_iteration_given_checkpoint_sequence`

## Counts & rough estimates

- Previously identified missing tests: ~115
- Newly enumerated implicit behaviors: ~35–40
- Combined missing tests estimate: ~150 (range 140–160 depending on splitting/combining)

## Recommended short roadmap (first 6 weeks)

Week 1–2 (Blocker remediation)
- Implement 10–15 crash/fsync boundary tests (WAL, manifest, flush, compaction write points).
- Deliverable: crash-injection harness + 8–12 passing durability tests.

Week 3–4 (Compaction + Collect integration)
- Implement WSST and COLL integration tests using temp dirs and the SST factory.
- Deliverable: compaction integration tests covering atomic manifest update and cleanup.

Week 5 (Recovery & manifest hardening)
- Add WAL/manifest coordination tests, exactly-once recovery tests, and manifest pruning tests.
- Deliverable: recovery idempotence and manifest consistency verified.

Week 6 (Iterator, admin, metrics)
- Add iterator lifecycle tests, admin API concurrency tests, basic metrics contract checks.
- Deliverable: iterator and admin API safety tests.

## Quick tactical next steps (pick one)

A. Create crash/fsync test skeletons and one fully implemented test (fast win). This accelerates the harness and provides a template for more tests.
B. Implement the compaction integration skeleton and one end-to-end compaction test (medium work, high impact).
C. Open issues/PRs mapping each missing-test to a small task for parallel contributors.

If you want me to proceed, I can:
- create test skeletons for the durability group and implement `should_recover_without_loss_given_crash_after_wal_append_before_fsync` as a runnable test using temp directories and a simple crash emulator; or
- create per-test issue templates (one file per missing high-priority test) in `docs/wip/tests-to-implement/` to guide contributors; or
- update `docs/wip/REQUIREMENTS.md` traceability tables to include the new counts.

Tell me which of the tactical next steps (A/B/C) to run and I'll do it in this workspace.
