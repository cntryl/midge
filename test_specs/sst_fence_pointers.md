# sst_fence_pointers.rs - Spec Card

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

**Create a test file that validates fence pointers: optimization for skipping unnecessary blocks during range queries.**

**Key Requirements**:
- All 12 tests parametrized across durable storage modes (LocalDisk, CloudBacked)
- Pattern: `for_each_storage_mode(&durable_storage_modes(), |mode, opts| { ... })`
- Fence pointers: mark block boundaries to enable early termination in range queries
- Skip blocks: blocks outside query range skipped without reading
- Range scan performance: fence pointers reduce block I/O
- Index integration: work with block index
- Correctness: no keys lost due to skipping

**Testing Approach**:
1. Create SST with fence pointers
2. Range query entirely before block → skip block
3. Range query entirely after block → skip block
4. Range query contains block → read block
5. Range query partially overlaps → read block
6. Verify skipping doesn't lose keys
7. Performance improvement: measure block I/O reduction
8. Multiple blocks
9. Boundary conditions
10. Integration with index
11. With compression
12. Concurrent access

---

**File Location**: `tests/sst_fence_pointers.rs`
**Test Count**: 12 tests
**Storage Modes**: FS + Cloud ONLY
**Pattern**: `for_each_storage_mode(&durable_storage_modes(), |mode, opts| { ... })`
**Status**: 📋 Not yet created (Phase 5 - SST layer)

---

## Purpose

Test fence pointers: marks on blocks enabling early termination in range queries. Fence pointers reduce unnecessary block I/O for range scans.

---

## Tests

1. **should_skip_blocks_before_query_range_given_fence_pointers_when_scanning**
2. **should_skip_blocks_after_query_range_given_fence_pointers_when_scanning**
3. **should_read_blocks_in_range_given_fence_pointers_when_scanning**
4. **should_not_lose_keys_given_block_skipping_when_verifying**
5. **should_improve_performance_given_fence_pointers_when_measuring_io**
6. **should_work_with_partial_overlap_given_range_query_when_scanning**
7. **should_handle_edge_case_exact_boundaries_given_fence_pointers_when_querying**
8. **should_support_multiple_blocks_given_large_sst_when_scanning**
9. **should_integrate_with_block_index_given_fence_pointers_when_querying**
10. **should_work_with_compressed_blocks_given_fence_pointers_when_reading**
11. **should_handle_concurrent_scans_given_multiple_readers_when_querying**
12. **should_maintain_fence_pointer_correctness_given_variable_block_sizes_when_indexing**

---

## Key APIs

- Fence pointer metadata (internal)
- Range scan with fence pointer optimization
- Block skipper

---

## Implementation Notes

✅ Uses `durable_storage_modes()` (FS + Cloud)
✅ Fence pointers stored in index
✅ Range scans use pointers to skip blocks
✅ No correctness loss from skipping
✅ Performance improvement measurable
✅ Transparent to application logic

---

## Status

**Current**: 📋 Not yet created (Phase 5 priority)
**Notes**: Block skipping optimization tests

---

## References
- See INTEGRATION_TESTS_FINAL.md lines ~1620 for full fence pointer spec
- Optimization in `src/sst/`
