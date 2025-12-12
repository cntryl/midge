# sst_per_block_bloom.rs - Spec Card

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

**Create a test file that validates per-block bloom filters: probabilistic data structures for negative lookups.**

**Key Requirements**:
- All 19 tests parametrized across durable storage modes (LocalDisk, CloudBacked)
- Pattern: `for_each_storage_mode(&durable_storage_modes(), |mode, opts| { ... })`
- Bloom filters: probabilistic data structure for membership testing
- False positives allowed: may say "might be in block" (forces read)
- False negatives impossible: never says "not in block" if actually present
- Per-block: each block has own bloom filter
- Skip blocks: negative lookup skips blocks with bloom-indicated-absent
- Performance: bloom filters reduce unnecessary block reads
- Correctness: no keys lost

**Testing Approach**:
1. Key in block → bloom says "might be"
2. Key not in block → bloom says "not in" (sometimes says "might be")
3. Negative lookup → skip reading block
4. False positives acceptable
5. False negatives impossible
6. Per-block isolation
7. Configure FP rate
8. Concurrent access
9. Works with compression
10. Large blocks
11. Small blocks
12. Many blocks
13. Empty blocks
14. Integration with range queries
15. Performance improvement measurement
16. Works with TTL
17. Snapshot visibility
18. Rebuild on recovery
19. Space overhead reasonable

---

**File Location**: `tests/sst_per_block_bloom.rs`
**Test Count**: 19 tests
**Storage Modes**: FS + Cloud ONLY
**Pattern**: `for_each_storage_mode(&durable_storage_modes(), |mode, opts| { ... })`
**Status**: 📋 Not yet created (Phase 5 - SST layer)

---

## Purpose

Test per-block bloom filters: probabilistic structures for fast negative lookups. Bloom filters reduce block I/O for non-existent keys.

---

## Tests

1. **should_indicate_key_might_be_present_given_key_in_block_when_querying**
2. **should_indicate_key_not_present_given_key_absent_when_querying**
3. **should_skip_block_given_bloom_negative_lookup_when_scanning**
4. **should_never_miss_key_given_false_negatives_impossible_when_verifying**
5. **should_allow_false_positives_given_bloom_filter_property_when_querying**
6. **should_isolate_blooms_per_block_given_independent_filters_when_comparing**
7. **should_respect_configured_fp_rate_given_tuning_when_building**
8. **should_handle_concurrent_access_given_multiple_readers_when_querying**
9. **should_work_with_compressed_blocks_given_bloom_and_compression_when_reading**
10. **should_handle_large_blocks_given_many_entries_when_filtering**
11. **should_handle_small_blocks_given_few_entries_when_filtering**
12. **should_handle_many_blocks_given_large_sst_when_filtering**
13. **should_handle_empty_blocks_given_no_entries_when_filtering**
14. **should_integrate_with_range_queries_given_bloom_and_range_when_scanning**
15. **should_improve_performance_given_bloom_filters_when_measuring_io**
16. **should_work_with_ttl_given_expired_keys_when_filtering**
17. **should_respect_snapshot_visibility_given_bloom_with_snapshot_when_reading**
18. **should_rebuild_bloom_given_recovery_when_reopening**
19. **should_maintain_reasonable_space_overhead_given_bloom_filters_when_sizing**

---

## Key APIs

- Bloom filter (internal)
- Filter builder
- Membership test

---

## Implementation Notes

✅ Uses `durable_storage_modes()` (FS + Cloud)
✅ Bloom filter is probabilistic: false positives OK, false negatives impossible
✅ Per-block bloom filter stored in block
✅ Reduces I/O by skipping blocks for non-existent keys
✅ False positive rate configurable (space/accuracy tradeoff)
✅ Transparent to application

---

## Status

**Current**: 📋 Not yet created (Phase 5 priority)
**Notes**: Bloom filter optimization tests

---

## References
- See INTEGRATION_TESTS_FINAL.md lines ~1750 for full bloom filter spec
- Bloom filter implementation in `src/sst/`
