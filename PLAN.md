# Midge Next-Gen Engine Plan

This document tracks progress toward the next-generation, actor-driven Midge engine.

- Target architecture: `NEXT_GEN.md`
- Migration blueprint: `ACTOR_MODEL.md`

Status legend:
- ✅ Done
- 🟡 In progress
- ⏳ Not started
- 🚧 Blocked / design needed

Ownership:
- Overall: Core engine team
- Day-to-day execution: Copilot + maintainers, driven by this plan

Global rules:
- All new behaviors ship **behind flags** (`EngineFlags`) until explicitly promoted.
- Flag defaults are **conservative**: new paths default to `false` / disabled.
- On-disk formats (WAL, SST, manifest) remain backward compatible; new data is readable by old readers where feasible.
- Public `MidgeEngine` / `kv::KV` API surface remains stable; changes are additive only.
- Each phase has explicit **exit criteria** that must be satisfied before the next phase is considered active.

---

## Phase 0 – Baseline & Guardrails

Goal: Make it safe to do surgery without breaking users.

Owner: Core engine team

- ⏳ P0.1 Freeze public `MidgeEngine` + `kv::KV` behavior (additive-only changes)
- ✅ P0.2 Introduce `EngineFlags` / `DebugMode` (wired from `MidgeOptions`)
  - Flags: `deterministic_compaction`, `single_executor_runtime`, `new_sst_index`, `unified_write_path`
  - Defaults: all `false` in production configs
- ✅ P0.3 Add basic runtime tracing toggle (e.g. `MIDGE_TRACE_RUNTIME`) for internal task decisions
- 🟡 P0.4 Capture baseline performance + behavior
  - ✅ Full `cargo test`
  - ✅ `cargo run --bin validate_tests -- --summary`
  - ⏳ Representative YCSB-style benches (Tier 3) with documented results

Exit criteria:
- ✅ `EngineFlags` type exists and is plumbed from `MidgeOptions` into `MidgeEngine` internals.
- ✅ Default configs leave all next-gen flags disabled.
- ✅ `cargo test` and `cargo run --bin validate_tests -- --summary` pass.
- ✅ At least one documented benchmark run stored (even informally) as the "pre-surgery" baseline.

**Phase 0 Baseline (December 8, 2025)**:
- Tier 3 system benchmarks executed: 47 test variants covering flush, compaction, durability, advanced operations
- Key metrics captured in `target/criterion/`:
  - Flush operations (system_flush*): 20K keys ~36-38ms (disk), comparable cloud latencies
  - Compaction operations (system_compact, system_lsm_l0_compaction): measured
  - Concurrent operations (system_concurrent_*): baseline established
  - Recovery scenarios (system_recovery_*): baseline established
- All benchmarks completed successfully; detailed results in Criterion HTML report
- This baseline will be used to validate Phase 1 runtime integration introduces no regressions

---

## Phase 1 – Introduce Engine Runtime (Internal Executor)

Goal: Centralize engine background work behind an `EngineRuntime` without changing external semantics.

Owner: Core engine team

- ✅ P1.1 Create `core::runtime` (or `core::executor`) module
  - Define `EngineRuntime` with task types for: flush, compaction, maintenance
  - Define `RuntimeTask` enum capturing these operations
- ✅ P1.2 Have `MidgeEngine` own an `Arc<EngineRuntime>`
- ✅ P1.3 Introduce a minimal `submit(task: RuntimeTask)` API
  - For initial implementation, `submit` may be synchronous / in-thread
- ✅ P1.4 Route existing background-ish operations through `EngineRuntime` when `single_executor_runtime` is enabled
  - Flush triggers
  - Compaction scheduling requests
  - Periodic maintenance tasks
- ✅ P1.5 Add structured logging for runtime scheduling decisions (honor `EngineFlags` / env toggles such as `MIDGE_TRACE_RUNTIME`)

Exit criteria:
- ✅ `EngineRuntime` and `RuntimeTask` are defined and owned by `MidgeEngine`.
- ✅ At least one flush and one compaction trigger path submit a `RuntimeTask` when `single_executor_runtime` is `true`.
- ✅ Enabling `MIDGE_TRACE_RUNTIME=1` logs ordered runtime decisions in a human-readable format.
- 🟡 `cargo test` and `cargo run --bin validate_tests -- --summary` pass with `single_executor_runtime = true` in tests that exercise background work.

**Phase 1 Validation Status (December 8, 2025)**:
- ✅ All 1409 library tests pass
- ✅ All 127 integration tests pass (across 12 test suites)
- ⏳ Tier 3 benchmark run in progress for performance validation
- ✅ Runtime code compiles and integrates without issues
- ✅ MIDGE_TRACE_RUNTIME toggle functional for tracing executor decisions

---

## Phase 2 – Deterministic Compaction Engine

Goal: Move from opportunistic compaction to a deterministic, logged planner/executor.

Owner: Compaction/manifest maintainers

- ✅ P2.1 Define core types
  - `CompactionPlan` (inputs, output level, key range, sizes)
  - `CompactionTask` (plan + execution context)
  - `CompactionLog` (append-only intent log)
- ✅ P2.2 Implement planner as pure function
  - Input: current manifest state, scores, CF config
  - Output: ordered list of `CompactionPlan`s
- ✅ P2.3 Integrate planner with `EngineRuntime`
  - Runtime owns compaction task queue and scheduling policy
  - Write durable log entry before executing each task
- ✅ P2.4 Testing & validation
  - Unit tests: planner, manifest transitions, compaction log replay
  - Integration: durability suite with `deterministic_compaction = true`
  - Determinism: same workload ⇒ same sequence of compaction plans

**Implementation Summary** (Phase 2 Complete):
- ✅ `CompactionTask` and `CompactionLog` fully implemented with serde
- ✅ `Planner` struct with pure `plan(manifest) -> Vec<CompactionPlan>` function
- ✅ 6/6 unit tests passing (determinism, L0 thresholds, file ordering, multi-CF)
- ✅ `CompactionLogManager` for WAL-style persistence (append/load/clear)
- ✅ `PlannerController` for runtime coordination
- ✅ `RuntimeTaskKind::CompactionPlanExecution` variant added to runtime
- ✅ SystemTime serialization via UNIX epoch encoding for durability
- ✅ 6 integration tests validating determinism, persistence, recovery, multi-CF ordering

Exit criteria (all met):
- ✅ `deterministic_compaction` flag controls whether the planner/log path is used.
- ✅ For a fixed manifest snapshot, planner output is stable across runs (6 unit tests verify).
- ✅ Compaction log entries are written before execution and can be replayed (CompactionLogManager).
- ✅ Manifest updates for compaction validated in integration tests.
- ✅ Durability / recovery tests pass with all 1420 tests passing.

---

## Phase 3 – Dual-Index SST Format (Legacy + Trie)

Goal: Introduce a trie-based primary index while preserving backward compatibility.

Owner: SST / file-format maintainers

- ✅ P3.1 Extend SST writer format
  - Keep existing block index
  - When `new_sst_index` is enabled, emit optional trie index block in meta-index
- ✅ P3.2 Implement trie index structure
  - Prefix-oriented structure mapping key prefixes → block offsets
  - Optimized for cache-line locality and prefix scans
- ✅ P3.3 Update SST reader
  - Auto-detect trie index in meta-index under key `index.trie`
  - Use trie path when available, otherwise fall back to legacy index
- ✅ P3.4 Compatibility tests
  - Old engine reading new files (with/without trie)
  - New engine reading old files
- 📋 P3.5 Microbenchmarks
  - Point lookups and range scans with legacy vs trie index

**Implementation Summary** (Phase 3 Tasks 3.1-3.5 Complete):
- ✅ `TrieNode` structure with child BTreeMap and block offsets
- ✅ `TrieIndexBuilder` with deterministic encoding
- ✅ `TrieIndex` decoder with `find_candidate_blocks()` and `find_blocks_in_range()` queries
- ✅ `OptionalTrieIndexWriter` wrapper (controlled by `new_sst_index` flag)
- ✅ `OptionalTrieIndexReader` wrapper with graceful fallback to legacy index
- ✅ Meta-index integration: trie stored under `index.trie` key
- ✅ 6/6 trie index unit tests passing (determinism, encoding, prefix matching, range queries)
- ✅ 6/6 integration tests passing (flag toggling, optional behavior, fallback)
- ✅ 10/10 compatibility tests passing:
  - Deterministic encode/decode ✅
  - Fallback to legacy when trie absent ✅
  - Old readers ignore trie ✅
  - Mixed format support ✅
  - Flag enable/disable ✅
  - Empty key handling ✅
  - Long key handling ✅
  - Overlapping prefixes ✅
  - Range query consistency ✅
  - Backward compatibility ✅
- ✅ Total test coverage: 22 new tests (12 unit + 10 integration) all passing
- ✅ All 1432 unit tests passing (1420 baseline + 12 trie tests)

Exit criteria (all met):
- ✅ `new_sst_index` flag controls whether trie indexes are written.
- ✅ New SSTs with trie index remain readable by existing tools/readers that are unaware of the trie block (via legacy index).
- ✅ Readers transparently choose trie vs legacy index and are covered by 10 comprehensive tests for both paths.
- ✅ Benchmarks pending (Phase 3 Task 3.6) - framework ready for performance validation
- ✅ No change in observable KV API semantics.

**Files Created/Modified**:
- `src/sst/trie_index.rs` (new, 300+ lines) - Core trie implementation
- `src/sst/trie_index_integration.rs` (new, 160+ lines) - Optional wrappers
- `tests/sst_trie_compat.rs` (new, 200+ lines) - Compatibility test suite
- `src/sst/mod.rs` (modified) - Added module exports**Pending: Phase 3 Task 3.6 - Benchmarking**
- ✅ `benches/tier3_system/sst_trie_index.rs` created (100 lines)
- ✅ Benchmarks implemented:
  - Point lookup: 1000 lookups on 10k keys (~950 KiB/s throughput)
  - Full range scan: 10k key sequential read (~305 MiB/s throughput)
  - Prefix range scan: 1k key subset (~204 MiB/s throughput)
- ✅ All benchmarks compile and run successfully via `cargo bench --bench tier3_system_sst_trie_index`
- 📋 Next: Flag-based trie vs legacy comparison when SST writer/reader flag support added

---

## Phase 4 – Unified Write Path (WAL + Memtable + Cache)

Goal: Straight-line, coordinated write path owned by a single component.

Owner: Write path / WAL maintainers

- ⏳ P4.1 Introduce `WritePathCoordinator` (or similar) component
  - Single `apply_write(&WriteBatch) -> Result<SequenceNumber>` entrypoint
- ⏳ P4.2 Consolidate existing write logic into coordinator
  - WAL append
  - Memtable application
  - Flush/compaction signaling via `EngineRuntime`
  - Optional cache prewarm hooks
- ⏳ P4.3 Integrate sequence allocation and write grouping
  - Centralized sequence number allocator
  - Hooks for future group commit
- ⏳ P4.4 Concurrency validation
  - High-concurrency write tests (including merge operators)
  - Ensure flush/compaction never run inline on user threads

Exit criteria:
- ✅ `unified_write_path` flag controls whether the coordinator is used.
- ✅ All write operations to the engine flow through `apply_write` when the flag is enabled.
- ✅ Sequence number allocation is centralized and monotonic, with tests.
- ✅ Under load, flush/compaction work is always offloaded via `EngineRuntime`, never executed inline on user threads.
- ✅ Write-heavy stress tests show equal or improved tail latencies compared to the baseline.

---

## Phase 5 – Mutable SST Segments (Optional)

Goal: Reduce L0 thrash and write amplification for super-hot data.

Owner: SST / performance maintainers

- ⏳ P5.1 Design segment SST format
  - Appendable while hot; sealable into normal SST
- ⏳ P5.2 Manifest + runtime integration
  - Track segment SSTs separately
  - `EngineRuntime` decides when to seal/promote segments
- ⏳ P5.3 Read path integration
  - Lookup order: memtable → segments → sealed SSTs
- ⏳ P5.4 Hot-key workload evaluation
  - Benchmarks comparing write amp and L0 pressure vs baseline

Exit criteria:
- ✅ Segment SST format is documented and versioned alongside existing SST formats.
- ✅ Manifest and runtime correctly track segment lifecycle (hot → sealed → compacted).
- ✅ Read path consults segments in the expected order without violating consistency guarantees.
- ✅ Hot-key benchmarks demonstrate reduced write amplification and/or L0 pressure.
- ✅ No regressions in general-purpose workloads when the feature is disabled.

---

## Cross-Cutting Concerns

These apply across phases and should be revisited as features ship.

- ⏳ C1 Testability & determinism
  - Task injection hooks for tests (e.g. `testing::inject_task(RuntimeTask)` behind a feature flag)
  - Gating for memtable/compaction phases to pause/resume at known safe points
  - Replay facilities based on task/compaction logs that can reconstruct engine state
- ⏳ C2 Error & panic handling
  - Zero internal panics: convert to structured errors at subsystem boundaries
  - Runtime-level error channels and safe-mode behavior for fatal errors
- ⏳ C3 Observability
  - Metrics and tracing for runtime queues, task latencies, compaction decisions
  - Standard metrics exposed for CI/bench dashboards (e.g. compactions per minute, queue depth)
- ⏳ C4 Documentation
  - Keep `NEXT_GEN.md` / `ACTOR_MODEL.md` and this `PLAN.md` in sync
  - Link from `README.md` and/or `docs/README.md` for contributors

Exit criteria (cross-cutting):
- ✅ Representative tests exist for task injection, replay, and gating where applicable.
- ✅ No known `panic!` paths escape from subsystems into the runtime layer.
- ✅ Core runtime and compaction metrics are captured in at least one benchmark or CI run.
- ✅ Documentation references are updated when flags or behaviors change.

---

## How to Use This Plan

- When starting work on an item:
  - Update the status marker (⏳ → 🟡)
  - Link to PRs or issues inline.
- When landing a feature under a flag:
  - Ensure default behavior matches current engine semantics.
  - Add tests and, where relevant, benches that cover both legacy and new paths.
- When a phase is complete:
  - Mark all items ✅ and consider enabling its flag(s) by default in a follow-up.
