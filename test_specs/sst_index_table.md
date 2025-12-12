# sst_index_table.rs - Spec Card

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

**Create a test file that validates SST block index lookup and performance.**

**Key Requirements**:
- All 20 tests parametrized across durable storage modes (LocalDisk, CloudBacked)
- Pattern: `for_each_storage_mode(&durable_storage_modes(), |mode, opts| { ... })`
- Index lookup: quickly locate blocks containing keys
- Block boundaries: index correctly identifies block boundaries
- Seek performance: O(log n) or better block lookup
- Range queries: find all blocks containing range
- Compressed blocks: work with compression

**Testing Approach**:
1. Create multi-block SST, lookup keys → verify correct block
2. Seek to key → correct block returned
3. Range query → all blocks in range returned
4. Index size reasonable (not O(n))
5. Compressed blocks indexed correctly
6. Empty blocks
7. Single-block SST (edge case)
8. Edge case: seek past end
9. Edge case: seek before start
10. Binary search correctness
11. Large index (1000+ blocks)
12. Index rebuild/verification
13. Index corruption detection (optional)
14. Index memory efficiency
15. Concurrent index access
16. Index layout optimization
17. Variable-sized blocks
18. Skip sparse blocks
19. Index for TTL-aware filtering
20. Index for bloom filter integration

---

**File Location**: `tests/sst_index_table.rs`
**Test Count**: 20 tests
**Storage Modes**: FS + Cloud ONLY
**Pattern**: `for_each_storage_mode(&durable_storage_modes(), |mode, opts| { ... })`
**Status**: 📋 Not yet created (Phase 5 - SST layer)

---

## Purpose

Test SST block index: quickly locate blocks containing keys via binary search or similar. Index is critical for fast key lookup without scanning all blocks.

---

## Tests

1. **should_locate_block_given_key_when_seeking**
2. **should_return_all_blocks_in_range_given_range_query_when_scanning**
3. **should_handle_key_not_in_index_given_missing_key_when_seeking**
4. **should_support_binary_search_given_index_when_querying**
5. **should_index_compressed_blocks_given_compression_when_writing**
6. **should_handle_empty_blocks_given_sparse_sst_when_indexing**
7. **should_handle_single_block_sst_given_small_sst_when_indexing**
8. **should_seek_past_end_given_key_beyond_maximum_when_seeking**
9. **should_seek_before_start_given_key_below_minimum_when_seeking**
10. **should_maintain_binary_search_invariants_given_sorted_index_when_verifying**
11. **should_handle_large_index_given_1000plus_blocks_when_indexing**
12. **should_rebuild_index_given_verification_when_reopening**
13. **should_detect_index_corruption_given_corrupted_index_when_reading**
14. **should_minimize_memory_overhead_given_large_sst_when_loading_index**
15. **should_support_concurrent_index_access_given_multiple_readers_when_querying**
16. **should_optimize_index_layout_for_cache_locality_when_designing**
17. **should_handle_variable_sized_blocks_given_mixed_block_sizes_when_indexing**
18. **should_skip_sparse_blocks_given_range_query_when_optimizing**
19. **should_support_ttl_filtering_with_index_given_expired_entries_when_querying**
20. **should_integrate_with_bloom_filters_given_negative_filters_when_optimizing**

---

## Key APIs

- Index lookup (internal)
- Block locator
- Binary search implementation
- Block boundary information

---

## Implementation Notes

✅ Uses `durable_storage_modes()` (FS + Cloud)
✅ Index structure binary-searched for O(log n) lookup
✅ Block boundaries stored in index
✅ Index loaded into memory for fast access
✅ Range queries use index to find relevant blocks
✅ Compression doesn't affect index accuracy

---

## Status

**Current**: 📋 Not yet created (Phase 5 priority)
**Notes**: Index-based lookup foundation tests

---

## References
- See INTEGRATION_TESTS_FINAL.md lines ~1460 for full SST index spec
- SST index implementation in `src/sst/`
