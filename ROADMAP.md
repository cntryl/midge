# Midge Next-Gen Roadmap – Copilot-Led Development Guide

This document complements `PLAN.md`, `ACTOR_MODEL.md`, and `NEXT_GEN.md`. It translates the phased blueprint into an actionable roadmap optimized for AI-assisted development.

## High-Level Goals

- Centralize engine background work under `EngineRuntime` and enable deterministic compaction.
- Ship innovations behind conservative feature flags and avoid breaking public API.
- Maintain compatibility: old readers read new files; old engines read new data when possible.

## Milestones & Timeline

| Phase | Name | Effort | Status |
|-------|------|--------|--------|
| Phase 0 | Baseline & Guardrails | 1–2 days | ✅ Complete |
| Phase 1 | Engine Runtime | 1–2 weeks | 🟡 Tests Passing, Benches In Progress |
| Phase 2 | Deterministic Compaction | 2–4 weeks | ⏳ Task 2.1-2.2 Complete |
| Phase 3 | Trie Index SST Format | 2–3 weeks | 📋 Blocked on Phase 2 |
| Phase 4 | Unified Write Path | 3–6 weeks | 📋 Blocked on Phase 2 |
| Phase 5 | Segment SSTs (Optional) | 2–4 weeks | 📋 Blocked on Phase 4 |

## Core Development Principles

- **Flag-First**: All new paths appear behind `EngineFlags` and default to `false`.
- **Small Steps**: Add types → add runtime plumbing → add tests → update docs/benchmarks.
- **Test-Driven**: Each change has a minimal unit test and an integration test with the flag on/off.
- **Observable**: Use `MIDGE_TRACE_RUNTIME` or metrics to validate behavior.

---

## Phase 1: Engine Runtime

**Status**: ⏳ In Progress  
**Goal**: Route flush and compaction operations through `EngineRuntime` executor when `single_executor_runtime` flag is enabled.

### Task 1.1: Define RuntimeTask Types
**Status**: ✅ Complete  
**Files**: `src/core/runtime.rs` (new)

### Task 1.2: Implement EngineRuntime Executor
**Status**: ✅ Complete  
**Files**: `src/core/runtime.rs`

### Task 1.3: Wire EngineRuntime into MidgeEngine
**Status**: ✅ Complete  
**Files**: `src/core/engine/core.rs`, `src/core/engine/state.rs`, `src/core/engine/factory.rs`

### Task 1.4: Route Flush Through Runtime
**Status**: ✅ Complete  
**Files**: `src/core/engine/flush_manager.rs`

### Task 1.5: Route Compaction Through Runtime
**Status**: ✅ Complete  
**Files**: `src/core/engine/operations/maintenance.rs`

### Task 1.6: Add Runtime Tracing Support
**Status**: ✅ Complete  
**Files**: `src/core/runtime.rs`

### Phase 1 Validation Checklist

**Unit Tests**:
- [ ] `tests/runtime.rs`: Task creation, executor loop single-threaded execution, trace logging
- [ ] `tests/runtime.rs`: `submit()` and `submit_and_wait()` APIs, completion notification

**Integration Tests** (run with and without flag):
- [ ] `tests/engine_runtime.rs`: Flush path with flag enabled matches flush path with flag disabled
- [ ] `tests/engine_runtime.rs`: Manual compaction with flag enabled matches flag disabled
- [ ] `tests/engine_runtime.rs`: Concurrent flush + compaction work correctly with runtime
- [ ] `tests/engine_runtime.rs`: Graceful shutdown: pending tasks drain before executor thread exits

**Bench Verification**:
- [ ] Run `cargo bench --bench tier3_system` with `MIDGE_TRACE_RUNTIME=1` and verify trace logs are produced
- [ ] Capture baseline latency/throughput for flush and compaction (p50/p99)
- [ ] Verify no regressions vs. flag disabled

**Trace Log Verification**:
```bash
MIDGE_TRACE_RUNTIME=1 cargo test -- --nocapture 2>&1 | grep "runtime:"
```

---

## Phase 2: Deterministic Compaction

**Status**: ⏳ In Progress  
**Goal**: Make compaction outcomes deterministic and reproducible.

### Task 2.1: Define CompactionPlan Types
**Status**: ✅ Complete  
**Files**: `src/core/compaction/planner.rs` (new)
- [ ] `CompactionPlan` struct with level, target_level, files_to_compact, output_files
- [ ] `CompactionTask` struct with plan, task_id, created_at
- [ ] `CompactionLog` struct with tasks, next_task_id
- [ ] Serialization support (serde) for replaying logs from disk

### Task 2.2: Implement Deterministic Planner
**Status**: ✅ Complete  
**Files**: `src/core/compaction/planner.rs` (Planner struct)  
**Implemented**: Pure function `planner()` that yields deterministic plans, plans sorted consistently by level/key range, 100% deterministic output

### Task 2.3: Route Compaction Plans Through Runtime
**Status**: 📋 Not Started  
**Files**: `src/core/engine/operations/maintenance.rs` (extend), `src/core/runtime.rs` (extend)  
**Success Criteria**:
- [ ] Runtime accepts `RuntimeTaskKind::CompactionPlan` variant
- [ ] Executor writes plan to compaction log before executing
- [ ] Compaction log persisted to disk (WAL-style) for replay after crash
- [ ] Integration with manifest transition to record completion

### Task 2.4: Add Replay and Validation Tests
**Status**: 📋 Not Started  
**Files**: `tests/compaction_determinism.rs` (new)  
**Success Criteria**:
- [ ] Unit test: Same manifest always yields same plan
- [ ] Integration test: Compaction log replay produces same manifest state
- [ ] Durability test: Crash during compaction, replay log, verify consistency

---

## Phase 3: Trie Index SST Format

**Status**: 📋 Blocked on Phase 2  
**Goal**: Add optional trie-based index to SST files for faster prefix searches.

### Task 3.1: Extend SST Writer
**Status**: 📋 Not Started  
**Files**: `src/sst/writer.rs`  
**Success Criteria**:
- [ ] Add optional `build_trie_index: bool` to writer config
- [ ] When enabled, construct trie from all keys written and append to SST block
- [ ] Trie block serialized and checksummed like other blocks

### Task 3.2: Extend SST Reader
**Status**: 📋 Not Started  
**Files**: `src/sst/reader.rs`  
**Success Criteria**:
- [ ] Detect trie block presence in SST header
- [ ] When present, load trie and use for prefix range queries
- [ ] Fall back to linear scan if trie block absent (backward compatibility)

### Task 3.3: Add Compatibility Tests
**Status**: 📋 Not Started  
**Files**: `tests/sst_trie_compat.rs` (new)  
**Success Criteria**:
- [ ] Old SST files (no trie) read correctly by new reader
- [ ] New SST files (with trie) read correctly by old reader
- [ ] Prefix query performance improvement measured

---

## Phase 4: Unified Write Path

**Status**: 📋 Blocked on Phase 2  
**Goal**: Consolidate WAL, memtable, and cache signaling behind a single coordinator.

### Task 4.1: Define WritePathCoordinator
**Status**: 📋 Not Started  
**Files**: `src/core/write_path/coordinator.rs` (new)  
**Success Criteria**:
- [ ] Single public API: `apply_write(write_batch) -> MidgeResult<()>`
- [ ] Internally orchestrates WAL append → memtable insert → cache update
- [ ] All background signals routed through runtime

### Task 4.2: Refactor Write Paths
**Status**: 📋 Not Started  
**Files**: `src/api/put.rs`, `src/api/delete.rs`, `src/api/merge.rs`  
**Success Criteria**:
- [ ] All write APIs use `WritePathCoordinator::apply_write()`
- [ ] WAL, memtable, cache remain internal to coordinator
- [ ] No breaking changes to public API

---

## Phase 5: Mutable SST Segments

**Status**: 📋 Blocked on Phase 4  
**Goal**: Allow SSTs to be mutable segments that are later sealed and promoted.

### Task 5.1: Segment Design & Manifest Tracking
**Status**: 📋 Not Started  
**Files**: `src/core/manifest/segment.rs` (new)  
**Success Criteria**:
- [ ] Define `Segment` type with unique ID, mutable flag, size, key range
- [ ] Extend manifest to track segments separately from sealed SSTs
- [ ] Segment → SST promotion logic (seal, assign new layer)

### Task 5.2: Read Path for Segments
**Status**: 📋 Not Started  
**Files**: `src/api/get.rs`, `src/api/scan.rs`  
**Success Criteria**:
- [ ] Check memtable → segments (in age order) → sealed SSTs
- [ ] Segments treated as read-only after sealing
- [ ] Performance: segment range query as fast as SST range query

---

## Cross-Cutting Tasks

### Observability & Tracing
- Add `tracing::info!` / `tracing::debug!` to key decision points
- Use `MIDGE_TRACE_RUNTIME` to control verbosity
- Capture latencies and queue depths in metrics where applicable

### Test Coverage
- Each feature has a minimal unit test (types, serialization, logic)
- Integration test with flag on/off produces identical results
- Durability/crash-recovery test for persistent features

### Documentation Updates
- Update `NEXT_GEN.md` as phases complete with code examples
- Add notes to this roadmap as progress is made

---

## Development Workflow

1. **Before starting a phase**: Review the Phase section for task breakdown and success criteria.
2. **During implementation**: Iterate locally, run `cargo test` after each task, verify `cargo clippy`.
3. **After implementation**: Run full suite with feature flag enabled and disabled.
4. **Update this roadmap**: Add notes about decisions, blockers, and design changes.

## Troubleshooting

**Q: How do I know if a task is "done"?**  
A: All items in the Success Criteria checklist are checked off, and integration tests pass both with flag enabled and disabled.

**Q: What if I find a blocker or design issue?**  
A: Document it in this roadmap under the relevant Phase section. Do not work around it; discuss with team and update task breakdown.

**Q: Should I create a new file or extend an existing one?**  
A: The task breakdown specifies "new" or "extend". If unclear, prefer new files for new concerns.

**Q: What if tests fail with the flag enabled but pass with flag disabled?**  
A: This indicates a routing or state management bug. Add tracing logs (`MIDGE_TRACE_RUNTIME=1`) and compare execution traces to identify the divergence.

**Q: How should I handle performance regressions?**  
A: Verify the regression is real (re-run bench to exclude noise). Profile with tracing enabled to identify bottleneck. Document findings and plan mitigation.

---

## Notes

This roadmap is a living document. As we implement each phase, we'll update it with:
- Actual implementation decisions and trade-offs
- Lessons learned and design pivots
- Refined task estimates based on real progress
- Performance baseline results and regressions observed
