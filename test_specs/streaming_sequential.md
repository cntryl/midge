# streaming_sequential.rs - Spec Card

## Philosophy

Tests define the **correct future behavior**, not document current limitations. Always implement tests fully; they may fail until features exist.

- ✅ Write ALL tests (never `#[ignore]`)
- ✅ Tests **MAY FAIL** if features aren't implemented yet
- ✅ Once features are built, failing tests become passing tests
- ✅ Tests act as a specification for what code needs to do
- ❌ Never stub behavior; always assert desired semantics
- ❌ Never skip tests on certain storage modes; use conditional logic instead

---

## PROMPT (Self-Driving Implementation Guide)

**Create a test file that validates sequential prefetch optimization for scans.**

**Key Requirements**:
- All 13 tests parametrized across all storage modes (Memory, LocalDisk, CloudBacked)
- Pattern: `for_each_storage_mode(&all_storage_modes_new(), |mode, opts| { ... })`
- Sequential prefetch: preload next blocks during sequential scans
- Cache locality: improve cache hit rates
- Scan performance: measurable latency improvement
- Correctness: no duplicate or missing results
- Adaptive: prefetch when sequential pattern detected
- Concurrent scans: multiple readers prefetch safely

**Testing Approach**:
1. Full table scan → prefetch improves performance
2. Range scan → prefetch optimizes
3. Reverse scan → prefetch adapted
4. Small scans → minimal prefetch
5. Large scans → aggressive prefetch
6. Mixed access pattern → detects sequential
7. Random access → no prefetch
8. Concurrent scans → each prefetches independently
9. Memory pressure → prefetch limited
10. Cache hit improvement measurable
11. Latency improvement measurable
12. Correctness verified (no duplicates/missing)
13. Interaction with other optimizations

---

**File Location**: `tests/streaming_sequential.rs`
**Test Count**: 13 tests
**Storage Modes**: ALL (Memory, LocalDisk, CloudBacked)
**Pattern**: `for_each_storage_mode(&all_storage_modes_new(), |mode, opts| { ... })`
**Status**: 📋 Not yet created (Phase 5 - Streaming layer)

---

## Purpose

Test sequential prefetch: preload upcoming blocks during sequential scans to improve cache locality. Sequential prefetch significantly improves scan performance.

---

## Tests

1. **should_prefetch_blocks_given_sequential_scan_when_iterating**
2. **should_improve_cache_hit_rate_given_prefetch_when_measuring**
3. **should_handle_full_table_scan_given_sequential_access_when_prefetching**
4. **should_handle_range_scan_given_bounded_query_when_prefetching**
5. **should_adapt_to_reverse_scan_given_backward_iteration_when_prefetching**
6. **should_handle_small_scan_given_few_results_when_prefetching**
7. **should_handle_large_scan_given_many_results_when_prefetching**
8. **should_detect_sequential_pattern_given_sequential_access_when_adapting**
9. **should_avoid_prefetch_given_random_access_when_not_sequential**
10. **should_handle_concurrent_scans_given_multiple_readers_when_prefetching**
11. **should_improve_latency_given_prefetch_when_measuring**
12. **should_maintain_correctness_given_prefetch_when_verifying**
13. **should_respect_memory_pressure_given_limited_budget_when_prefetching**

---

## Key APIs

- Sequential prefetch (internal optimization)
- Adaptive prefetch detection
- Block preloader

---

## Implementation Notes

✅ Uses `all_storage_modes_new()` (optimization across all modes)
✅ Detects sequential access pattern
✅ Preloads next blocks during scans
✅ Improves cache hit rate
✅ Adaptive: only when beneficial
✅ Respects memory pressure

---

## Status

**Current**: 📋 Not yet created (Phase 5 priority - streaming)
**Notes**: Sequential scan prefetching optimization tests

---

## References
- See INTEGRATION_TESTS_FINAL.md lines ~1990 for full sequential prefetch spec
- Streaming optimization in `src/streaming/`
