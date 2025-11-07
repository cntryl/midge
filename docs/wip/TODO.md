# TODO — Comprehensive Execution Backlog (annotated)

This file lists the prioritized execution backlog and, where applicable, notes the current implementation status and immediate gaps discovered during a recent code inspection (Nov 2025). Each item keeps its original priority but now includes a short status and recommended next steps.

Legend: P0 (now), P1 (next), P2 (later)

## Storage core

1) P0 — Vectorized WAL I/O (writev/io_uring)
	- Status: Implemented (functional)
	- Evidence: `src/wal/fs/writer.rs` uses `crate::fs::write_vectored()` and the encoder; `src/fs/io.rs` provides a `write_vectored()` entrypoint with an io_uring backend behind `feature = "uring"` and a fallback that uses `write_vectored`.
	- Gaps: Platform-specific optimized gather writes for Windows are not implemented; benchmarks and tuning to reach the stated throughput/fsync p99 targets are missing.
	- Next steps: Add WAL microbench(s) under `benches/` to measure throughput and fsync p99; consider Windows gather-write implementation or document expected fallback behavior.

2) P0 — Group-commit tuning and durability windows
	- Status: Implemented (mechanism present)
	- Evidence: `src/wal/fs/group_commit.rs` implements a lock-free coordinator; `Wal::sync()` uses it when `WalSyncMode::GroupCommit`.
	- Gaps: Tuning and acceptance testing (quantified durability windows, crash-recovery matrix) are not yet performed.
	- Next steps: Create a benchmark + failure-injection harness to measure group-commit latency windows and to validate crash-recovery under different sync modes.

3) P0 — Tiered compaction strategy
	- Status: Partially implemented (leveled strategy present; size-tiered config option exists but dedicated size-tiered algorithm not fully implemented)
	- Evidence: `src/core/compaction/strategy.rs` implements leveled compaction; `CompactionStyle::SizeTiered` exists in CF config.
	- Gaps: A dedicated size-tiered selection algorithm (grouping similarly sized SSTs) is not found; acceptance WA/SA measurements are missing.
	- Next steps: Implement size-tiered selection policy or wire CF `CompactionStyle` to strategy implementations; add WA/SA benchmarks and a tuning job.

4) P1 — mmap-backed SST reader + checksum-verified mapping
	- Status: Missing (design references present)
	- Evidence: `src/sst/reader_common.rs` and `src/sst/fs/reader.rs` parse SSTs from in-memory bytes; `src/fs/io.rs` mentions mmap in comments; docs reference mmap options.
	- Gaps: No mmap-backed reader (zero-copy parsing) or verified mapping with quarantining of corrupted blocks.
	- Next steps: Implement a feature-gated `MmapSstReader` or `FsSstFactory` variant that mmaps SST files, validates footers/checksums, and exposes slices to `reader_common`. Add tests that detect and quarantine corrupted mappings.

5) P1 — Parallel compression workers for SST flush
	- Status: Not implemented (no explicit parallel compression worker pool in SST writer examined)
	- Evidence: `src/sst/writer_common.rs` and related modules handle SST writing, but I did not find a parallel compression worker pipeline.
	- Gaps: Parallelized per-block compression for flush path is not present.
	- Next steps: Add a feature-gated worker pool to compress data blocks in parallel when flushing large memtables; bench to reach target throughput.

6) P2 — SIMD key-compare fast paths
	- Status: Design-ready, implementation missing
	- Evidence: Bloom filter and data structures are SIMD-friendly (`src/sst/bloom.rs` and docs mention SIMD). No explicit SIMD intrinsics or feature-gated accelerated key-compare code was found.
	- Gaps: No platform intrinsics / `std::simd`/target_feature implementations for key comparisons or skiplist hot paths.
	- Next steps: Implement optional, feature-gated SIMD hot-paths for key compares (with runtime detection or build features) and add microbenchmarks to validate improvements.

## Correctness & concurrency

7) P0 — Concurrency safety suite (writes/reads/flush/compaction overlap)
	- Status: Partially implemented (many tests exist)
	- Evidence: Numerous tests (e.g., `tests/compaction_concurrent.rs`, `tests/concurrent_writes.rs`, `tests/transaction_acid.rs`) exercise overlapping operations.
	- Gaps: `wip/REQUIREMENTS.md` lists several missing acceptance tests (some marked ❌). Coverage for specific edge cases (write stalls, fairness, certain multi-CF interactions) needs expansion.
	- Next steps: Convert missing items from `wip/REQUIREMENTS.md` into concrete tests and add them to CI; add stress harnesses for long-running concurrency fuzzing.

8) P0 — Error handling & recovery harness
	- Status: Partially implemented
	- Evidence: WAL replay supports tolerant modes and CRC checks (`replay_wal_file_with_mode`); structured errors exist throughout the codebase.
	- Gaps: No global IO fault-injection harness or systematic torn-write injection suite was identified.
	- Next steps: Implement an IO-fault-injection wrapper for tests and add tests for torn writes, quota enforcement, and manifest atomicity.

9) P1 — Memory pressure behavior
	- Status: Partially implemented
	- Evidence: Memtable and write-stall related config and hooks exist; CF config contains memtable limits.
	- Gaps: Acceptance tests proving deterministic stalling/resume and eviction policies need to be added/expanded.
	- Next steps: Add integration tests for memory pressure scenarios and verify eviction/stall semantics.

10) P1 — Multi-CF integration tests
	- Status: Partially implemented
	- Evidence: Column-family support is present and compaction/manifest track `cf_id`.
	- Gaps: Explicit acceptance tests for isolation, independent compaction, and shared budgets remain to be completed.
	- Next steps: Add targeted multi-CF tests and CI jobs exercising per-CF compaction policies.

## Cloud

11) P1 — Idempotent WAL/SST uploads + reconciliation tool
	- Status: Partially implemented
	- Evidence: Cloud `StorageBackend` abstraction (`src/cloud/backend.rs`), `MockCloudBackend` (test harness), `CloudSstManager` upload APIs (`src/sst/cloud/lifecycle.rs`), and hybrid local+cloud path (`src/cloud/hybrid.rs`) exist.
	- Gaps: Higher-level idempotent upload protocol and a reconciliation tool (e.g., `check-cloud`) are not present. `CloudSstManager.upload` currently does unconditional `put_blob`; `MockCloudBackend` supports `put_if_not_exists` for tests but no reconciler/repair tool is found.
	- Next steps: Implement idempotent upload flows (content-hash keys or conditional put semantics), manifest updates with checksums/etags, and a `reconcile`/`check-cloud` command to detect & heal drift. Add fault-injection tests using `MockCloudBackend`.

12) P2 — Cloud overhead analysis gate
	- Status: Not implemented (needs measurement)
	- Next steps: Add cloud performance benchmarks and CI/perf jobs to measure async upload overhead p99.

## Startup & recovery

13) P1 — Parallel WAL replay and checkpoints
	- Status: Partial scaffolding present
	- Evidence: Rehydration tracking and WAL replay functions exist; `HealthManager` records rehydration progress.
	- Gaps: A fully implemented parallel WAL replay + checkpoint orchestration for the stated recovery-time target is not present.
	- Next steps: Implement checkpoint creation and parallel WAL replay workers; add reproducible benchmarks to validate recovery timing.

## Housekeeping

14) P0 — Health manager: seal WAL segment (Phase 4)
	- Status: In-progress (partial)
	- Evidence: `HealthManager::drain()` previously contained TODOs; updated to set an in-memory cloud checkpoint in the manifest when drain completes.
	- Gaps: Actual WAL segment sealing (on-disk/cloud marking) and atomic manifest persistence/upload still need engine-level context (db path / cloud APIs) and tests.
	- Next steps: Wire HealthManager drain to engine APIs that can persist the manifest (or accept a db_path), implement WAL segment rotate/seal in the WAL layer, and add focused tests using `MockCloudBackend` to assert checkpoint persistence and WAL pruning safety.

15) P0 — CI gates for perf artifacts
	- Status: Not implemented (CI integration needed)
	- Next steps: Add CI jobs to run benchmarks and publish signed artifacts under `infra/proofs/` as required.

Notes
 - Keep `wip/` minimal: `PLAN.md`, `REQUIREMENTS.md`, `TODO.md`.
 - Use the project test rules (AAA, should_*) for all new tests.

Summary
 - Mechanisms for many of the P0 items exist (WAL vectored writes, group-commit, compaction executor, cloud abstraction, health manager scaffold). The primary work remaining is: concrete feature implementations (mmap reader, size-tiered compaction, SIMD hot paths), idempotent cloud flows and reconciliation tooling, targeted benchmarks and tuning (WAL throughput & fsync p99), and completing specific acceptance tests from `wip/REQUIREMENTS.md`.
 - If you'd like, I can implement one targeted change now (suggested: HealthManager WAL sealing + manifest checkpoint or making SST cloud uploads idempotent). Tell me which one and I'll prepare the patch and tests.
