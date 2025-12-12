# streaming_fence_pointer.rs - Spec Card

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

**Create a test file that validates streaming fence pointers: skip unnecessary levels in range queries.**

**Key Requirements**:
- All 15 tests parametrized across all storage modes (Memory, LocalDisk, CloudBacked)
- Pattern: `for_each_storage_mode(&all_storage_modes_new(), |mode, opts| { ... })`
- Streaming fence: pointers indicating which levels contain keys in range
- Range query optimization: skip levels with fence-indicated-absent
- Multi-level skipping: avoid scanning levels
- Correctness: no keys lost from skipping
- Performance: measurable reduction in level scans
- Concurrent access: thread-safe

**Testing Approach**:
1. Range query, level has no overlapping keys → skip
2. Range query, level has overlapping keys → scan
3. Multiple levels skipped
4. Partial overlap handled
5. Edge case: exact boundaries
6. Concurrent queries
7. Update on compaction
8. Performance measurement
9. Large ranges
10. Small ranges
11. Many levels
12. Few levels
13. Hot ranges
14. Cold ranges
15. Interaction with other optimizations

---

**File Location**: `tests/streaming_fence_pointer.rs`
**Test Count**: 15 tests
**Storage Modes**: ALL (Memory, LocalDisk, CloudBacked)
**Pattern**: `for_each_storage_mode(&all_storage_modes_new(), |mode, opts| { ... })`
**Status**: 📋 Not yet created (Phase 5 - Streaming layer)

---

## Purpose

Test streaming fence pointers: skip LSM levels that don't overlap with query range. Fence pointers reduce multi-level traversal for range queries.

---

## Tests

1. **should_skip_level_given_no_overlap_with_range_when_querying**
2. **should_scan_level_given_overlap_with_range_when_querying**
3. **should_skip_multiple_levels_given_range_outside_when_querying**
4. **should_handle_partial_overlap_given_fence_when_querying**
5. **should_handle_exact_boundary_given_range_equals_fence_when_querying**
6. **should_handle_concurrent_queries_given_multiple_readers_when_querying**
7. **should_update_fences_on_compaction_given_level_merge_when_optimizing**
8. **should_provide_performance_improvement_given_fence_pointers_when_measuring**
9. **should_handle_large_ranges_given_wide_query_when_querying**
10. **should_handle_small_ranges_given_narrow_query_when_querying**
11. **should_handle_many_levels_given_deep_lsm_when_querying**
12. **should_handle_few_levels_given_shallow_lsm_when_querying**
13. **should_optimize_hot_ranges_given_frequently_queried_when_measuring**
14. **should_handle_cold_ranges_given_rarely_queried_when_querying**
15. **should_integrate_with_other_optimizations_given_bloom_and_fence_when_querying**

---

## Key APIs

- Streaming fence (internal optimization)
- Multi-level range skip
- Fence update on compaction

---

## Implementation Notes

✅ Uses `all_storage_modes_new()` (optimization across all modes)
✅ Indicates key ranges in each level
✅ Range queries skip levels without overlaps
✅ Updated on compaction/flush
✅ No false negatives (never incorrectly skip)
✅ Measurable performance improvement

---

## Status

**Current**: 📋 Not yet created (Phase 5 priority - streaming)
**Notes**: Multi-level range skipping optimization tests

---

## References
- See INTEGRATION_TESTS_FINAL.md lines ~1920 for full streaming fence pointer spec
- Streaming optimization in `src/streaming/`
