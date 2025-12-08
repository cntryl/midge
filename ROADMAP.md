# Midge Next-Gen Roadmap – Path to Done

This roadmap maps the current implementation state to the **NEXT_GEN.md end-state specification** and identifies the critical path to completing the actor-driven LSM engine.

## Vision: End-State Completion (NEXT_GEN.md)

The end state is a **single-coordinated, deterministic, actor-driven engine** where:
- All background work (flush, compaction, WAL upload, eviction) routes through `EngineRuntime`
- The runtime owns all mutable state and sequences all transitions
- Flush and compaction are fully deterministic and replayable
- Hybrid storage seamlessly integrates cloud and local tiers
- Write path is unified and coordinated
- Segment SSTs enable write-amplification reduction

## Critical Path to Done

| Phase | Name | Effort | Status | Blocks |
|-------|------|--------|--------|--------|
| Phase 0 | Baseline & Guardrails | ✅ Done | ✅ Complete | (none) |
| Phase 1 | Engine Runtime Wiring | ✅ Done | ✅ Complete | (none) |
| Phase 2 | Deterministic Compaction | ✅ Done | ✅ Complete | Phase 3 |
| Phase 3 | Trie Index SST Format | ✅ Done | ✅ Complete | Phase 4 |
| Phase 4 | Unified Write Path | ✅ Done | ✅ Complete | Phase 5 |
| **Phase 5** | **Mutable Segment SSTs** | **2–3 weeks** | 🟡 **In Progress** | **None** |
| **Phase 6** | **Runtime Coordinator Unification** | **3–4 weeks** | 📋 **Ready** | Phase 5 |
| **Phase 7** | **Hybrid Storage + Cloud Integration** | **2–3 weeks** | 📋 **Ready** | Phase 6 |
| **Phase 8** | **Production Hardening & Documentation** | **1–2 weeks** | 📋 **Ready** | Phase 7 |

## Phase 5: Mutable Segment SSTs

**Status**: 🟡 In Progress (Tasks 5.1 Complete, 5.2–5.3 Next)  
**Goal**: Enable SSTs to be mutable segments, sealed and promoted to LSM structure. Reduces write-amplification by delaying immutability.

### Task 5.1: Segment Design & Manifest Tracking
**Status**: ✅ Complete  
**Files**: `src/core/manifest/segment.rs`, `src/core/manifest/types.rs`, `src/core/manifest/mod.rs`  
**Implemented**:
- [x] `Segment` struct with state machine (Mutable → Sealing → Sealed → Promoted)
- [x] `SegmentRef` for lightweight read-path references
- [x] `SegmentSequencer` for monotonic per-CF ID allocation
- [x] Manifest extended to track `segments: Vec<Segment>`
- [x] 10 comprehensive unit tests covering lifecycle, overlap detection, containment checks
- [x] 100% test compliance restored (all 12 AAA violations fixed)

**Test Results**: 1452/1452 lib tests passing, 2296/2296 validation tests (100%)

### Task 5.2: Read Path Integration (Segment Lookup)
**Status**: 📋 Next  
**Files**: `src/api/get.rs`, `src/api/scan.rs`, `src/core/engine/operations/reads.rs` (extend)  
**Scope**:
- Integrate segment lookup into point get: Memtable → Active Segments (age order) → SSTs
- Integrate segment lookup into range scan: same ordering, efficient iteration
- Handle segment state transitions during reads (blocking on seal)
- Optional segment block caching (reuse block cache for segment blocks)

**Success Criteria**:
- [ ] Point lookups include active segments in correct age order
- [ ] Range scans include all segments in scope with correct ordering
- [ ] No performance regression vs. SST-only path
- [ ] Reads block gracefully if segment sealing during read

**Estimated Effort**: 3–4 days

### Task 5.3: Segment Flush & Sealing Coordination
**Status**: 📋 Next  
**Files**: `src/core/engine/flush_manager.rs` (extend), `src/core/runtime.rs`  
**Scope**:
- Segments can be sealed on-demand (flush trigger, time-based, size-based)
- Sealed segments become readable SST-like objects
- Segment → Level 0 promotion as runtime task
- Segment cleanup after promotion

**Success Criteria**:
- [ ] Segments transition from mutable to sealed on flush
- [ ] Sealed segments are promoted to Level 0
- [ ] Read path correctly handles mixed mutable + sealed segments

**Estimated Effort**: 3–4 days

---

## Phase 6: Runtime Coordinator Unification

**Status**: 📋 Ready  
**Goal**: Route all flush and compaction workers through `EngineRuntime` exclusively. This enforces the actor model and enables deterministic scheduling.

### Task 6.1: Unify Flush Coordination
**Status**: 📋 Next  
**Files**: `src/core/engine/flush_manager.rs`, `src/core/runtime.rs`  
**Scope**:
- Replace `FlushCoordinator` worker thread with `EngineRuntime` task submission
- All flush work submitted as `RuntimeTaskKind::Flush` tasks
- Flush sequencing deterministic based on memtable age
- Manifest updates happen atomically within flush task

**Success Criteria**:
- [ ] Flushes always routed through `EngineRuntime::submit_task()`
- [ ] Flush ordering is deterministic (oldest memtable first)
- [ ] No direct worker thread access to engine state during flush

**Estimated Effort**: 2–3 days

### Task 6.2: Unify Compaction Coordination
**Status**: 📋 Next  
**Files**: `src/core/compaction/controller.rs`, `src/core/runtime.rs`  
**Scope**:
- Replace `CompactionController` worker threads with `EngineRuntime` task submission
- All compaction plans submitted as `RuntimeTaskKind::Compaction` tasks
- Compaction execution always happens within runtime context
- Determinism: same manifest → same plan sequence (already guaranteed by Phase 2)

**Success Criteria**:
- [ ] Compactions always routed through `EngineRuntime::submit_task()`
- [ ] Compaction executor runs within task context, not independent thread
- [ ] Manifest atomically updated after compaction completion

**Estimated Effort**: 2–3 days

### Task 6.3: Unify WAL Upload Coordination
**Status**: 📋 Next  
**Files**: `src/wal/controller.rs`, `src/core/runtime.rs` (extend `RuntimeTaskKind`)  
**Scope**:
- Add `RuntimeTaskKind::WalUpload` variant
- WAL uploads scheduled as runtime tasks when hybrid storage is enabled
- Cloud durability coordinated with flush/compaction lifecycle
- WAL rotation happens within task context

**Success Criteria**:
- [ ] WAL uploads are runtime tasks
- [ ] No background WAL upload threads independent of runtime
- [ ] Cloud durability is deterministic (same sequence of uploads)

**Estimated Effort**: 2 days

### Task 6.4: State Ownership Transfer to Runtime
**Status**: 📋 Next  
**Files**: `src/core/engine/core.rs`, `src/core/runtime.rs`  
**Scope**:
- Runtime owns mutable state: active memtables, SST set, compaction queues, WAL progress
- MidgeEngine delegates state queries to runtime (not direct struct access)
- Atomicity: all state updates happen within task execution
- Recovery: replay task log to reconstruct exact state

**Success Criteria**:
- [ ] No external direct mutation of engine state
- [ ] All state changes modeled as runtime task side-effects
- [ ] Recovery from task log works for all operation types

**Estimated Effort**: 3–4 days

---

## Phase 7: Hybrid Storage + Cloud Integration

**Status**: 📋 Ready  
**Goal**: Fully integrate cloud storage as the primary durable layer with local NVMe as ephemeral cache.

### Task 7.1: Cloud WAL as Primary Durability
**Status**: 📋 Next  
**Files**: `src/wal/cloud.rs` (extend), `src/wal/controller.rs`  
**Scope**:
- WAL segments uploaded to cloud as runtime tasks
- Local WAL buffer is ephemeral (can be cleared after cloud upload)
- Recovery reads WAL from cloud object store, not local FS
- Local WAL retention configurable (for performance)

**Success Criteria**:
- [ ] Cloud WAL upload is deterministic and ordered
- [ ] Recovery reconstructs state from cloud WAL
- [ ] Local WAL retention doesn't affect durability guarantees

**Estimated Effort**: 2–3 days

### Task 7.2: Cloud SST as Primary Storage
**Status**: 📋 Next  
**Files**: `src/sst/cloud.rs` (extend), `src/sst/file_manager.rs`  
**Scope**:
- SSTs written directly to cloud object store
- Local NVMe holds cached copies (LRU eviction)
- Compaction and flush write to cloud, optionally cache locally
- Block cache operates on cloud-fetched blocks seamlessly

**Success Criteria**:
- [ ] SSTs uploaded to cloud after flush/compaction
- [ ] Local cache is optional (eviction doesn't affect correctness)
- [ ] Block cache works with cloud-fetched blocks

**Estimated Effort**: 2–3 days

### Task 7.3: Hybrid Storage Eviction as Runtime Task
**Status**: 📋 Next  
**Files**: `src/cloud/hybrid.rs`, `src/core/runtime.rs`  
**Scope**:
- Eviction decisions made by runtime (not background thread)
- Eviction submitted as `RuntimeTaskKind::Maintenance` task
- Deterministic eviction policy (LRU, based on access patterns)
- Metrics: cache hit rate, eviction rate, cloud bandwidth utilization

**Success Criteria**:
- [ ] Eviction is runtime-scheduled
- [ ] Cache hit rate is predictable and measurable
- [ ] Eviction doesn't block writes

**Estimated Effort**: 2–3 days

---

## Phase 8: Production Hardening & Documentation

**Status**: 📋 Ready  
**Goal**: Validate end-state specification, document architecture, and prepare for production use.

### Task 8.1: End-to-End Determinism Validation
**Status**: 📋 Next  
**Files**: `tests/determinism.rs` (new comprehensive test suite)  
**Scope**:
- Identical workload on two engines produces identical flush/compaction sequences
- Manifest state is identical after each operation
- Recovery produces identical state to pre-crash state
- Multi-CF workloads are deterministic

**Success Criteria**:
- [ ] Determinism test passes for all operation types
- [ ] Determinism holds across engine restarts
- [ ] No flaky tests

**Estimated Effort**: 2–3 days

### Task 8.2: Specification Validation Against Implementation
**Status**: 📋 Next  
**Files**: `NEXT_GEN.md` (review and validate), `src/` (review implementation)  
**Scope**:
- Verify each NEXT_GEN.md section is implemented:
  - Engine Runtime (central actor) ✓
  - Unified Write Path ✓
  - Memtable + Block Cache Unification (verify)
  - SST Format (dual-index: legacy + trie) ✓
  - Flush Lifecycle (verify determinism)
  - Deterministic Compaction ✓
  - Hybrid Storage (verify cloud integration)
  - Manifest Management (verify atomicity)
  - Concurrency Model (verify no shared mutable state)
- Document any deviations or incomplete sections

**Success Criteria**:
- [ ] All NEXT_GEN.md sections have corresponding implementation
- [ ] Deviations documented with rationale
- [ ] No unimplemented critical features

**Estimated Effort**: 2–3 days

### Task 8.3: Architecture Documentation
**Status**: 📋 Next  
**Files**: `docs/ARCHITECTURE.md` (new), `docs/RUNTIME_MODEL.md` (new)  
**Scope**:
- Architecture overview (actor-driven, deterministic, unified)
- Runtime task model (task types, submission, execution)
- Write path (sequence allocation, WAL, memtable)
- Read path (memtable, segments, SSTs)
- Compaction model (planning, logging, execution)
- Recovery procedure (from task log, WAL, manifest)
- Hybrid storage mode (cloud + local architecture)

**Success Criteria**:
- [ ] New users can understand the architecture from docs
- [ ] All major components documented
- [ ] Operational runbooks created (debugging, tuning, monitoring)

**Estimated Effort**: 2–3 days

### Task 8.4: Performance Benchmarking & Baselines
**Status**: 📋 Next  
**Files**: `benches/` (extend with end-state benchmarks)  
**Scope**:
- Benchmark suite covering:
  - Write throughput (single-threaded, concurrent)
  - Read throughput (point, range, with segments)
  - Flush latency (with segments, cloud upload)
  - Compaction throughput (deterministic scheduling)
  - Hybrid storage eviction impact
- Compare: segments vs. SST-only, local vs. hybrid storage
- Document baseline results and optimization opportunities

**Success Criteria**:
- [ ] Comprehensive benchmark suite
- [ ] Baseline results published
- [ ] No performance regression vs. pre-Phase-5 state
- [ ] Segment benefits demonstrated (reduced write-amplification)

**Estimated Effort**: 3–4 days

---

## Summary: Path to Done

**Current State**:
- ✅ Phase 0–4: Core architecture complete (runtime, deterministic compaction, trie index, unified write path)
- ✅ 100% test compliance and zero clippy warnings
- 🟡 Phase 5.1: Segment types designed and integrated
- 📋 Phases 5.2–8: Implementation path clear

**Estimated Total Effort for Completion**: 12–16 weeks (excluding Phase 5.1 which is complete)

**Critical Path**:
1. **Phase 5 (Segments)** → 2–3 weeks — Unblocked
2. **Phase 6 (Runtime Unification)** → 3–4 weeks — Depends on Phase 5
3. **Phase 7 (Hybrid Storage)** → 2–3 weeks — Depends on Phase 6
4. **Phase 8 (Hardening)** → 1–2 weeks — Final polish

**Definition of Done** (NEXT_GEN.md End-State):
- ✅ Actor-driven engine with centralized `EngineRuntime`
- ✅ Deterministic flush and compaction
- ✅ Unified write path
- ✅ Mutable segment SSTs
- ✅ All workers routed through runtime
- ✅ Hybrid storage with cloud as primary durability
- ✅ Complete specification documentation
- ✅ Comprehensive benchmarking and zero performance regression

---

## Development Workflow

1. **Phase 5.2–5.3**: Integrate segments into read/write paths (2–3 weeks)
2. **Phase 6.1–6.4**: Unify all coordinators into `EngineRuntime` (3–4 weeks)
3. **Phase 7.1–7.3**: Enable full hybrid storage + cloud integration (2–3 weeks)
4. **Phase 8.1–8.4**: Validate, document, benchmark, and ship (1–2 weeks)

Each phase has clear success criteria. Start each phase with:
- Review previous phase completion
- Identify blockers or design changes needed
- Implement incrementally (small PRs, frequent testing)
- Update this roadmap with actual findings and progress

---

## Recent Updates (December 8, 2025)

### Roadmap Rebuilt for Completion
- Restructured roadmap around NEXT_GEN.md end-state specification
- Added Phases 6–8 to complete the actor model vision
- Identified critical path to "done": Phases 5 → 6 → 7 → 8
- Estimated total effort: 12–16 weeks to full completion

### Test Guideline Enforcement
- Implemented automated test guideline validation tool (`validate_tests` binary)
- Fixed 52 AAA structure violations: added proper `// Arrange`, `// Act`, `// Assert` comments to all tests
- Fixed 6 multi-behavior naming violations: renamed tests to follow `should_{action}_when_{context}` pattern
- Fixed 4 additional AAA violations in compatibility tests (segment tests + sst_trie_compat)
- Achieved 100% compliance rate (2296/2296 tests passing)

### Code Quality Improvements
- Resolved all 25 clippy warnings via automated fixes:
  - Range checks converted to idiomatic `RangeInclusive::contains()`
  - Collection length checks simplified to `.is_empty()`
  - Allocations optimized (vec! → array literals, or_insert_with → or_default)
  - Code style issues fixed (empty lines after doc comments, unused parameters)
- Zero clippy warnings on full codebase

### Phase 5.1 Completion
- Implemented `Segment` struct with full lifecycle (Mutable → Sealed → Promoted)
- Implemented `SegmentSequencer` for monotonic per-CF ID allocation
- Implemented `SegmentRef` for lightweight read-path references
- Extended `Manifest` to track segments alongside SSTs
- Created 10 comprehensive unit tests covering all segment operations
- All tests passing, 100% guideline compliance

### Phases 0–4 Status
- ✅ Phase 0: Baseline & Guardrails (complete)
- ✅ Phase 1: Engine Runtime (complete, infrastructure ready)
- ✅ Phase 2: Deterministic Compaction (complete, replayable intent log)
- ✅ Phase 3: Trie Index SST Format (complete, backward compatible)
- ✅ Phase 4: Unified Write Path (complete, coordinated sequence/WAL/memtable)

---

## Notes

This is now the definitive completion roadmap. Each phase has clear scope, success criteria, and effort estimates. The critical path to "done" is clearly marked: **Phases 5 → 6 → 7 → 8**.

When updates happen, record them here in chronological order. Document:
- Phase completion dates and actual effort
- Design decisions and trade-offs
- Lessons learned
- Blockers and how they were resolved
- Performance baseline results
