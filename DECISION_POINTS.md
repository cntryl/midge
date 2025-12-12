# DECISION POINTS - What's Next?

## Overview

Spec tuning is complete. All 24 test specification cards are now accurate and validated against actual test files. Now we need to decide on the next direction.

---

## Critical Decision #1: Filesystem Artifacts Test

**Issue**: engine_basic.rs is missing a test for "memory mode doesn't create filesystem artifacts"

**Options**:

### Option A: Add test #9 to engine_basic.rs
```rust
#[test]
fn should_not_create_filesystem_artifacts_when_memory_mode() {
    let opts = memory_opts();
    let path = opts.path.clone();
    
    // Arrange
    {
        let engine = open_with_mode(opts, StorageMode::Memory);
        let cf = engine.default_column_family();
        engine.put(cf, b"key", b"value").expect("put");
        // engine dropped here
    }
    
    // Assert - path should not exist or be empty
    assert!(!path.exists() || dir_is_empty(&path), "memory mode created artifacts");
}
```

**Pros**:
- ✅ Keeps all basic tests in one file
- ✅ Simple and focused
- ✅ Tests core memory mode isolation property

**Cons**:
- ❌ engine_basic.rs becomes 9 tests instead of 8
- ❌ Breaks the "8 basic operations" pattern

---

### Option B: Create memory_mode_isolation.rs
```
New file with 5-8 tests:
- should_not_create_filesystem_artifacts_when_memory_mode
- should_not_create_wal_files_when_memory_mode
- should_not_create_sst_files_when_memory_mode
- should_cleanup_memory_on_close_when_memory_mode
- should_not_persist_across_restart_when_memory_mode
- should_isolate_multiple_engines_in_memory_when_separate_instances
```

**Pros**:
- ✅ Comprehensive memory mode validation
- ✅ Keeps engine_basic.rs at 8 tests
- ✅ Useful for cloud deployments

**Cons**:
- ❌ New file to maintain
- ❌ Adds to test complexity

---

### RECOMMENDATION
**Option A** - Add test #9 to engine_basic.rs
- Simple, focused, validates critical property
- Can always extract to separate file later if needed
- Minimal disruption

**ACTION REQUIRED**: Approve or choose Option B

---

## Critical Decision #2: New Test Files to Create (Phase 2)

We identified 6-7 recommended new files. Which should we create?

| File | Tests | Priority | Effort | Value |
|------|-------|----------|--------|-------|
| memory_mode_isolation.rs | 5-8 | HIGH | Low | HIGH |
| merge_advanced.rs | 8-10 | MEDIUM | Medium | MEDIUM |
| snapshots_advanced.rs | 6-8 | MEDIUM | Medium | MEDIUM |
| edge_cases.rs | 10-12 | MEDIUM | Medium | MEDIUM |
| cloud_resilience.rs | 8-10 | HIGH | High | HIGH |
| concurrency_stress.rs | 6-8 | HIGH | High | HIGH |
| perf_regression.rs | 8-10 | MEDIUM | High | MEDIUM |

---

### Recommended Priority Order for Phase 2

**Tier 1 (Before Phase 5 SST work)** - ~30-50 tests
1. **memory_mode_isolation.rs** (5-8 tests) - LOW effort, HIGH value
2. **edge_cases.rs** (10-12 tests) - MEDIUM effort, MEDIUM value
3. **merge_advanced.rs** (8-10 tests) - MEDIUM effort, MEDIUM value

**Tier 2 (During Phase 5 SST work)** - ~40-60 tests
4. **snapshots_advanced.rs** (6-8 tests) - MEDIUM effort, MEDIUM value
5. **concurrency_stress.rs** (6-8 tests) - HIGH effort, HIGH value
6. **cloud_resilience.rs** (8-10 tests) - HIGH effort, HIGH value

**Tier 3 (Phase 6+)** - ~20-30 tests
7. **perf_regression.rs** (8-10 tests) - HIGH effort, MEDIUM value

---

### Quick Compatibility Check

Can these new files run in **Phase 2** (before SST work)?

- ✅ memory_mode_isolation.rs - YES (engine-level)
- ✅ merge_advanced.rs - YES (engine_merge already passing)
- ✅ snapshots_advanced.rs - YES (engine_snapshots already passing)
- ✅ edge_cases.rs - YES (engine-level, no SST required)
- ⚠️ cloud_resilience.rs - PARTIAL (need cloud backend mocking)
- ✅ concurrency_stress.rs - YES (uses existing transaction tests)
- ⚠️ perf_regression.rs - PARTIAL (needs criterion setup)

**Recommendation**: Create Tier 1 files in Phase 2, Tier 2+ in Phase 5+

---

## Critical Decision #3: Implementation Sequence

Should we:

### Option A: Implement Phase 2 tests first, THEN implement actual test code
1. Create all 5 Tier 1 spec files (memory_mode_isolation, edge_cases, merge_advanced)
2. Implement engine_basic.rs test #9
3. Fix missing implementations in existing specs (transaction_advanced, transaction_spill)
4. Begin implementing Phase 2 test code

**Pros**:
- ✅ Specs drive implementation (specification-first)
- ✅ Clear roadmap before coding
- ✅ Reduces mistakes

**Cons**:
- ❌ More upfront planning
- ❌ Longer before tests passing

---

### Option B: Implement existing Phase 1 tests, THEN create new specs
1. Begin implementing from existing 24 specs
2. As tests pass, create new spec files (Tier 1)
3. Implement new tests as specs created

**Pros**:
- ✅ Quick wins (24 files ready to implement)
- ✅ New specs informed by implementation learnings
- ✅ Iterative feedback

**Cons**:
- ❌ Less rigorous specification-first approach
- ❌ May miss edge cases

---

### RECOMMENDATION
**Option A** - Spec-driven approach

This aligns with the "specification-first" philosophy. We got the specs right the first time (mostly), so completing Tier 1 specs before implementation will:
1. Catch issues early
2. Provide clear roadmap
3. Enable parallel implementation
4. Reduce rework

**ACTION REQUIRED**: Approve or choose Option B

---

## Critical Decision #4: Test Expansion Scope

Given limited time/resources, should we:

### Option A: Minimal Expansion (Tier 1 only)
- Add filesystem artifacts test (#9) to engine_basic.rs
- Create: memory_mode_isolation.rs (5 tests)
- Skip: edge_cases, merge_advanced, snapshots_advanced
- **New tests**: ~10-15 total
- **Benefit**: Quick win, validates memory mode
- **Risk**: Miss important edge cases

---

### Option B: Moderate Expansion (Tier 1 + some Tier 2)
- Create Tier 1 files: memory_mode_isolation (5), merge_advanced (8), edge_cases (10)
- Partial Tier 2: concurrency_stress (5 instead of 8), skip snapshots_advanced
- **New tests**: ~40-50 total
- **Benefit**: Good coverage, manageable scope
- **Risk**: Still miss some scenarios

---

### Option C: Full Expansion (Tier 1 + Tier 2)
- Create all Tier 1+2 files (memory_mode, merge_advanced, edge_cases, snapshots_advanced, concurrency_stress, cloud_resilience)
- **New tests**: ~80-90 total
- **Benefit**: Comprehensive, production-ready
- **Risk**: Large effort, may slow Phase 2 implementation

---

### RECOMMENDATION
**Option B** - Moderate Expansion (Tier 1 + partial Tier 2)

Rationale:
- ✅ Memory mode isolation is HIGH priority (validates critical property)
- ✅ Merge advanced and edge cases are HIGH value (common failure modes)
- ✅ Concurrency stress is HIGH priority for production readiness
- ✅ ~50 new tests is manageable scope
- ✅ Delivers "specification-first" value without over-commitment

**ACTION REQUIRED**: Approve or choose Option A or C

---

## Summary: Decisions Needed

| Decision | Option | Recommendation | Impact |
|----------|--------|-----------------|--------|
| Filesystem artifacts test | Add to engine_basic (A) vs new file (B) | **Option A** | +1 test |
| New test file priority | Tier 1/2/3 order | **Tier 1 → Tier 2 → Tier 3** | 0-90 tests |
| Implementation sequence | Spec-first (A) vs iterative (B) | **Option A** | Architecture |
| Expansion scope | Minimal (A), Moderate (B), Full (C) | **Option B** | ~50 new tests |

---

## Next Actions (After Approval)

1. **Decision Review** (you)
   - Approve/modify recommendations
   - Confirm resource availability for new test files

2. **Implementation Planning** (you + agent)
   - Create Tier 1 spec files (memory_mode_isolation, edge_cases, merge_advanced)
   - Add engine_basic.rs test #9 spec
   - Update transaction_advanced/spill specs with detailed scenarios
   - Establish test file templates and conventions

3. **Begin Phase 2 Implementation**
   - Start from engine layer (best understood)
   - Use spec cards as implementation guides
   - Track test pass/fail status

4. **Progress Tracking**
   - Update test status in spec cards as tests pass
   - Track coverage percentage (tests passing / total tests)
   - Identify and document blockers

---

## Timeline Estimate

**If Option B approved**:

| Phase | Duration | Deliverables |
|-------|----------|--------------|
| Create Tier 1 specs | 1-2 hours | 4 new spec files |
| Implement Phase 1 tests (engine) | 2-3 days | 117 tests passing |
| Implement Phase 1 tests (other) | 3-5 days | 200+ tests passing |
| Implement Tier 1 tests | 1-2 days | ~50 new tests passing |
| Implement Phase 2 tests (transaction advanced) | 2-3 days | 50+ new tests passing |
| **TOTAL Phase 2** | **1-2 weeks** | **500+ tests passing** |

---

## Files Ready for Review

- ✅ [SPEC_TUNING_COMPLETE.md](SPEC_TUNING_COMPLETE.md) - Summary of tuning work
- ✅ [COVERAGE_ANALYSIS_AND_GAPS.md](COVERAGE_ANALYSIS_AND_GAPS.md) - Detailed gap analysis
- ✅ [ENGINE_LAYER_CORRECTIONS.md](ENGINE_LAYER_CORRECTIONS.md) - Corrections made
- ✅ [test_specs/](test_specs/) - All 24 tuned spec cards

---

## Questions for You

1. **Filesystem artifacts test**: Option A (add to engine_basic) or B (new file)?
2. **Implementation sequence**: Spec-first (Option A) or iterative (Option B)?
3. **Expansion scope**: Minimal (A), Moderate (B), or Full (C)?
4. **Ready to proceed** with Phase 2 after decisions made?

