# Midge: Path to Completion

**Goal**: Complete the actor-model LSM engine without legacy code or dead paths.

**Current State**: Phases 1-7.1 complete (actor model foundation). Remaining: cloud integration wiring + cleanup.

**Total Effort**: ~12-15 hours of focused work.

---

## Phase A: Eliminate Dead Code & Conditionals (2-3 hours)

Remove all legacy paths and make actor model non-negotiable.

### A1: Remove executor mode conditionals
- **File**: `src/config/options.rs`
- **What**: Delete `single_executor_runtime` flag (default was true, now it's the only way)
- **Why**: Eliminates branching logic, enforces determinism
- **Tests**: All runtime tests pass as-is (they expect this mode)

### A2: Remove spawn_cloud_upload() function entirely
- **File**: `src/core/persistence/flush/process.rs`
- **Current**: ~40 lines of background thread spawning code
- **What**: Delete function, replace all callers with CloudCoordinator::submit_sst_upload_task()
- **Why**: Background threads = non-deterministic; we have CloudCoordinator for this

### A3: Clean up compaction cloud upload path
- **File**: `src/core/compaction_controller.rs`
- **What**: Same as A2 - remove spawn_cloud_upload() calls, use coordinator
- **Why**: Consistency across flush and compaction paths

### A4: Audit and remove old SST format readers (if present)
- **Files**: `src/sst/reader.rs`, `src/sst/writer.rs`
- **What**: Check for legacy block index support that's superseded by trie index
- **Why**: Trie index is the production format; legacy support adds complexity

### A5: Clean test utilities for old code paths
- **Files**: `testutils/`
- **What**: Remove test hooks for `single_executor_runtime=false` mode
- **Why**: No old mode to test anymore

**Success**: Zero dead_code warnings from clippy on core files.

---

## Phase B: Complete Cloud Integration (4-6 hours)

Wire CloudCoordinator into actual cloud operations so they're deterministic.

### B1: Integrate CloudCoordinator into flush uploads
- **File**: `src/core/persistence/flush/process.rs` (line ~187)
- **Current**: 
  ```rust
  spawn_cloud_upload(cloud_manager, sst_id, sst_path, ...);
  ```
- **Target**:
  ```rust
  cloud_coordinator.submit_sst_upload_task(
      &engine.runtime,
      sst_id.clone(),
      move || {
          cloud_manager.upload_sst_async(sst_id, sst_path, ...)
      }
  )?;
  ```
- **Tests to add**: 2-3 tests verifying upload task submission + sequencing

### B2: Integrate CloudCoordinator into compaction uploads
- **File**: `src/core/compaction_controller.rs`
- **What**: After new SST creation, submit upload task (not spawn thread)
- **Tests to add**: 2-3 tests for compaction cloud upload sequencing

### B3: Integrate cache eviction into runtime (Phase 7.3)
- **File**: `src/cloud/hybrid.rs` - HybridStorage::get_or_fetch()
- **Current**: Eviction happens synchronously or in background thread
- **Target**: Submit eviction as CloudCoordinator task
- **Code pattern**:
  ```rust
  if cache_size > max_capacity {
      cloud_coordinator.submit_eviction_task(
          &engine.runtime,
          move || { evict_lru_entry_impl(&mut cache); }
      )?;
  }
  ```
- **Add cache metrics**: `cache_hits`, `cache_misses`, `evictions`, `eviction_latency_us`
- **Tests to add**: 3-4 tests for eviction sequencing + metrics

### B4: Add cloud-backed determinism tests
- **New file**: `tests/cloud_determinism.rs`
- **Tests**:
  - Identical workloads produce identical cloud upload sequence
  - Upload order matches flush order (deterministic)
  - Cache evictions happen in deterministic LRU order
  - Two engines with same workload produce same manifest + cloud metadata
- **Tests to add**: 4-5 comprehensive tests

**Success**: All cloud operations coordinated through EngineRuntime. No spawn_cloud_upload() calls remain.

---

## Phase C: Validation & Consolidated Documentation (3-4 hours)

Replace scattered integration plans with real, current documentation.

### C1: Create single comprehensive ARCHITECTURE.md
- **File**: `docs/ARCHITECTURE.md` (consolidate existing pieces)
- **Sections**:
  - Component overview (EngineRuntime, coordinators, ownership)
  - Write path flow (with diagram)
  - Read path flow (with diagram)
  - Flush lifecycle (memtable → SST → cloud → WAL prune)
  - Compaction lifecycle (plan → execute → cloud delete)
  - Determinism guarantees (what is, what isn't, why)
  - Concurrency model (no shared mutable state)
  - Error handling (background error container)
  - Cloud integration (uploads, downloads, eviction)
  - Performance characteristics (latency, throughput)
- **Length**: ~500 lines (reference, not blueprint)
- **Purpose**: Single source of truth, not implementation guides

### C2: Create OPERATIONS.md (real, not template)
- **File**: `docs/OPERATIONS.md`
- **Sections**:
  - Configuration (what parameters matter, why)
  - Monitoring (metrics to watch)
  - Tuning (performance knobs)
  - Debugging (how to diagnose issues)
  - Troubleshooting (common problems + fixes)
  - Recovery (crash recovery procedures)
- **Purpose**: Practical guide for operators/developers
- **Length**: ~300 lines

### C3: Create E2E determinism validation suite
- **File**: `tests/e2e_determinism.rs`
- **Tests**:
  - Identical workloads → identical manifest
  - Identical workloads → identical SST sequence
  - Identical workloads → identical compaction decisions
  - Crash recovery → identical state
  - Multi-CF isolation + determinism
- **Tests to add**: 6-8 comprehensive tests
- **Purpose**: Prove the big idea works (determinism)

### C4: Document benchmark baselines
- **File**: `docs/PERFORMANCE_BASELINES.md` (new, actual measurements)
- **Sections**:
  - Tier 1: Hotpath (point ops, cached reads)
  - Tier 2: Subsystem (flush, compaction)
  - Tier 3: System (mixed workloads)
  - Tier 4: Cloud (cloud-backed operations)
  - Tier 5: Soak (long-running stability)
- **Content**: Actual benchmark results, not blueprints
- **How to generate**: Run `cargo bench` and capture results

**Success**: Up-to-date documentation reflecting current implementation.

---

## Phase D: Final Cleanup (2 hours)

Remove all deferred/incomplete documentation and dead code markers.

### D1: Delete all integration plan docs
- **Files to delete**:
  - `docs/PHASE_7_2_INTEGRATION_PLAN.md` (now B1-B2)
  - `docs/PHASE_7_3_INTEGRATION_PLAN.md` (now B3)
  - `docs/PHASE_8_2_SPECIFICATION_VALIDATION.md`
  - `docs/PHASE_8_4_BENCHMARKING_GUIDE.md`
  - `docs/PHASE_8_COMPLETION_SUMMARY.md`
  - `docs/PHASE_8_STATUS.md`
  - `docs/QUICK_REFERENCE.md`
  - `docs/SESSION_SUMMARY.md`
- **Why**: Integration plans become code (B1-B3). Summaries become outdated quickly.

### D2: Consolidate operational docs
- **Keep**: ARCHITECTURE.md, OPERATIONS.md, PERFORMANCE_BASELINES.md
- **Keep**: ROADMAP.md (update to current phase only)
- **Keep**: THE_BIG_IDEA.md (vision statement)
- **Delete**: All planning/WIP/completion docs

### D3: Update ROADMAP.md
- **What**: Remove all "Phase X planned", "Phase Y deferred" language
- **Target**: Single section: "Current Implementation Status"
  - ✅ Phase 1-7.1: Complete
  - 🚧 Phase 7.2-7.3: In progress (cloud wiring)
  - ⏳ Phase 8: Production hardening (after 7.2-7.3)

### D4: Final code cleanup
- Run: `cargo clippy --all-targets` (fix all warnings)
- Run: `cargo fmt` (normalize formatting)
- Run: `cargo test --lib` (verify nothing broke)
- Commit: "cleanup: eliminate dead code, consolidate docs, complete cloud integration"

**Success**: Repo is clean, focused, no legacy mess.

---

## Test Increment Targets

| Phase | New Tests | Total Expected | Status |
|-------|-----------|-----------------|--------|
| A (cleanup) | 0 | 1467 lib | 🟢 All pass |
| B1 (flush uploads) | 3 | 1470 | 🟢 All pass |
| B2 (compaction uploads) | 3 | 1473 | 🟢 All pass |
| B3 (cache eviction) | 4 | 1477 | 🟢 All pass |
| B4 (cloud determinism) | 5 | 1482 | 🟢 All pass |
| C1-C4 (validation) | 6 | 1488 | 🟢 All pass |
| **TOTAL** | **21** | **1488** | ✅ Done |

All tests must:
- ✅ Follow naming: `should_{action}_when_{context}`
- ✅ Follow structure: Arrange/Act/Assert
- ✅ One behavior per test (no multi-behavior)
- ✅ Pass `cargo run --bin validate_tests -- --summary`

---

## Commit Strategy

One commit per phase:

1. `A: phase-a-cleanup-dead-code-and-conditionals`
2. `B1: phase-b1-wire-flush-cloud-uploads-through-coordinator`
3. `B2: phase-b2-wire-compaction-cloud-uploads-through-coordinator`
4. `B3: phase-b3-wire-cache-eviction-through-coordinator`
5. `B4: phase-b4-add-cloud-determinism-validation-tests`
6. `C1-C4: phase-c-consolidate-documentation-and-baselines`
7. `D: phase-d-final-cleanup-remove-deferred-docs`

All commits:
- Must pass: `cargo test --lib && cargo clippy --all-targets`
- Must include test count in commit message
- Must reference specific files modified

---

## Definition of Done

✅ **Code**:
- [ ] Zero spawn_cloud_upload() calls remaining
- [ ] All cloud ops through CloudCoordinator
- [ ] All cache evictions through EngineRuntime
- [ ] 1488 tests passing (21 new)
- [ ] 100% test compliance (validate_tests)
- [ ] Zero clippy warnings
- [ ] All code formatted

✅ **Documentation**:
- [ ] ARCHITECTURE.md current and complete
- [ ] OPERATIONS.md comprehensive and practical
- [ ] PERFORMANCE_BASELINES.md with actual numbers
- [ ] All integration plan docs deleted
- [ ] All completion/summary docs deleted
- [ ] ROADMAP.md shows current state only

✅ **Validation**:
- [ ] E2E determinism suite passes
- [ ] Cloud operations deterministic
- [ ] Recovery works correctly
- [ ] All tests named correctly
- [ ] No deferred work lingering

---

## Key Principles

1. **No conditionals on mode**: Actor model is the only way
2. **No background threads**: Everything through EngineRuntime
3. **No dead code**: Delete unused functions, don't mark with #[allow(dead_code)]
4. **No deferred docs**: Planning docs become code or get deleted
5. **Determinism first**: If it can be deterministic, make it so
6. **Tests validate everything**: Every behavior has a test

---

## What You'll Have at the End

A **production-ready actor-model LSM engine** with:

✅ **Clean code**: No legacy paths, no dead code, no conditionals  
✅ **Complete cloud integration**: Uploads, downloads, eviction all coordinated  
✅ **Deterministic guarantees**: Validated by comprehensive test suite  
✅ **Real documentation**: Architecture guide + operations manual  
✅ **Proven performance**: Baselines established and tracked  
✅ **High quality**: All tests pass, zero warnings, fully formatted  

**Ready for**: Deployment, integration with Fitz/Portia, production workloads.

---

## Start Here

1. Review this TODO.md to understand the plan
2. Begin Phase A (cleanest first step, builds confidence)
3. Follow the file/code change specifications
4. Add tests as you complete each section
5. Commit after each phase
6. Use this document as your checklist

**Questions?** Each phase has specific files and line numbers—no ambiguity.

---

*Last Updated: December 9, 2025*  
*Status: Ready to Begin*  
*Branch: actor-model*  
*Estimated Duration: 12-15 hours of focused work*
