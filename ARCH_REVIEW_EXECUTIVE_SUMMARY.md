# Executive Summary: Midge Architectural Review

**Conducted**: December 8, 2025  
**Depth**: Full codebase analysis (40+ Rust files, 45,000+ lines)  
**Outcome**: 969-line detailed review with concrete refactoring plan

---

## 🎯 Verdict

Midge is a **well-engineered LSM with strong fundamentals**, but has **implicit architectural patterns** that reduce clarity and increase maintenance overhead. Production-ready with recommended Phase 8-9 refactors.

---

## 💪 Strengths

1. **Unified Write Path** — Single code path (op → seqno → WAL → memtable → cache)
2. **Actor-Model Ready** — Infrastructure for deterministic execution exists
3. **Deterministic Operations** — Flush and compaction can be replayed
4. **Cloud-Native** — WAL and SST abstractions support cloud backends
5. **Test Suite** — 2329 tests at 100% compliance, no unsafe violations
6. **Clean SST Format** — TLV blocks with trie/bloom/sparse indexes
7. **No Panics** — Production code uses Result, not unwrap()

---

## ⚠️ 6 Architectural Gaps

### Gap 1: Runtime Executor is Implicit
- `EngineRuntime` exists but coordinators (flush, compaction) spawn their own threads
- Two scheduling layers: runtime tasks + coordinator threads (confusion)
- **Fix**: Move to true event loop that owns threadpool

### Gap 2: 4 Inconsistent Coordinator APIs
- FlushCoordinator, CompactionController, WalUploadCoordinator, CloudCoordinator
- Different APIs, different locations, different semantics
- **Fix**: Unified `BackgroundCoordinator` trait in single `core/coordinator/` module

### Gap 3: Manifest Cache is Duplicate Source of Truth
- Manifest on disk + ManifestCache in memory + BloomCache + SparseIndexCache
- 3 separate metadata caches with independent invalidation
- Manual update paths (easy to forget)
- **Fix**: Single `Metadata` type with atomic cache invalidation

### Gap 4: Cloud Integration Not Wired
- CloudCoordinator created but never called
- WalUploadCoordinator unused
- SSTs written locally, not submitted for upload
- **Fix**: Wire uploads into flush/compaction hot paths

### Gap 5: Inconsistent Error Handling
- Flush/compaction failures logged but not visible to user code
- No "background error" state on engine
- User can't detect degraded engine health
- **Fix**: Error tracking on engine with visibility API

### Gap 6: Determinism Not Validated
- "Deterministic compaction" claimed but Phase 8 determinism tests not written
- No test that runs same workload twice and compares manifests
- **Fix**: Phase 8 determinism validation suite (already planned)

---

## 📊 Consolidation Opportunities

| Duplication | Lines | Fix |
|-------------|-------|-----|
| Merge resolution (flush + compaction) | ~200 | Create `core/merge/resolver.rs` |
| Entry pipeline (flush + compaction) | ~300 | Create `core/pipeline/entry.rs` |
| Coordinator APIs (4 different types) | N/A | Unified trait |
| Version management (4 types) | ~150 | Reduce to 2: `VersionSet` + `VersionManager` |
| Metadata caching (3 separate caches) | ~500 | Unified `Metadata` struct |

**Total Potential Cleanup**: ~1100 lines of code

---

## 🗑️ Dead Code to Remove

1. **`planner_controller.rs`** (132 lines) — Unused vestigial code
2. **`db_lock` field** (MidgeEngine) — Never used
3. **`mem_mode` field** (MidgeEngine) — Unused, StorageMode is source of truth
4. **`WalUploadCoordinator`** (if cloud uploads go through CloudCoordinator)

**Total Dead Code**: ~200 lines

---

## 📈 Refactoring Effort

| Task | Effort | Priority | Phase | Notes |
|------|--------|----------|-------|-------|
| Consolidate coordinators → `core/coordinator/` | 1 week | HIGH | 8 | Standardize API, improves clarity |
| Extract engine state/deps | 3 days | MEDIUM | 8 | Simplify MidgeEngine struct |
| Wire cloud uploads into flush | 2 days | HIGH | 8 | Cloud integration actually works |
| Merge entry pipeline | 5 days | MEDIUM | 8 | Remove duplication |
| Remove dead code | 1 day | LOW | 8 | Clean code |
| Simplify version management | 3 days | MEDIUM | 8 | 4 types → 2 types |
| **Phase 8 Total** | **~3 weeks** | | | |
| Runtime refactor (V2 parallel) | 3 weeks | HIGH | 9 | Major change, requires parallel impl |
| **Phase 9+ Total** | **~3 weeks** | | | |

---

## ✅ Phase 8 Immediate Actions

**Do in Phase 8** (7-11 days total, as planned):

1. ✅ **Consolidate coordinators** — Move to `core/coordinator/` module
   - Unifies APIs under single `BackgroundCoordinator` trait
   - Reduces cognitive overhead (4 APIs → 1)

2. ✅ **Extract engine state** — New `state.rs` and `deps.rs` files
   - Separates concerns: engine state vs. coordination
   - Simplifies MidgeEngine struct

3. ✅ **Wire cloud into hot paths** — Flush/compaction auto-upload
   - CloudCoordinator actually used
   - SSTs submitted for cloud storage

4. ✅ **Determinism validation** — Phase 8.1 tests
   - Two identical engines, same workload, compare manifests
   - Proves Phase 2 (deterministic compaction) claim

5. ✅ **Audit snapshot isolation** — Invariant checks
   - Every snapshot has a pinned version
   - Test suite validates across refactors

**Not in Phase 8** (too risky):
- Runtime refactor to true event loop (Phase 9)
- Rewrite version management (Phase 9)

---

## 🚀 Phase 9 (Future)

**After Phase 8 consolidation is complete**:

1. **Runtime Refactor** — Implicit → explicit event loop
   - Build `RuntimeV2` in parallel with old runtime
   - Feature-gate both, run tests on both
   - Switch over, remove old runtime

2. **Pipeline Unification** — Shared entry processing for flush/compaction

3. **Version Management Simplification** — 4 types → 2

---

## 📋 Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| Coordinator consolidation breaks compilation | Feature-gate old APIs during transition |
| Cloud upload changes latency | Uploads are async, non-blocking for user code |
| Snapshot isolation breaks | Every test must remain passing; add invariant checks |
| Version management simplification causes issues | Run full test suite after each step |

---

## 🎓 Key Insights

**What's Working Well**:
- Write path is clean and unified
- Background work can be deterministic (infrastructure ready)
- Cloud abstractions are well-designed
- Test suite is comprehensive and compliant

**What Needs Improvement**:
- Implicit runtime execution (threads spawned in multiple places)
- Coordinator APIs are inconsistent
- Cloud integration exists but isn't connected
- Metadata caching is fragmented

**Biggest Impact Quick Win**:
- Move coordinators to single `core/coordinator/` module
- Standardize under one trait
- Clarifies "what background work can the engine do?"
- 1 week of work, huge clarity improvement

---

## 📚 Detailed Review Location

Full architectural review: `ARCHITECTURAL_REVIEW.md`

Covers:
- 6 high-level architectural gaps (with rationale)
- 7 module-by-module refactoring plans (with code examples)
- 4 consolidation opportunities (with line counts)
- 4 pruning recommendations
- Risk mitigation strategies
- Proposed new directory layout
- Phase-by-phase effort estimates

---

## Recommendation

**Proceed with Phase 8** as planned (determinism validation, spec compliance, ops docs, benchmarking).

**During Phase 8**, implement quick wins:
1. Consolidate coordinators (1 week)
2. Wire cloud into flush (2 days)
3. Extract engine state (3 days)

**Defer to Phase 9**:
- Runtime refactor (too risky mid-phase)
- Pipeline unification (depends on entry pipeline refactor)
- Version management simplification (requires careful testing)

**Post-Phase 8 Outcome**: Production-ready LSM with clear architecture, minimal duplication, fully integrated cloud support.
