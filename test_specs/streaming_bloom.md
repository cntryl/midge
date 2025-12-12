# streaming_bloom.rs - Spec Card

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

**Create a test file that validates streaming bloom filters: fast negative filters for multi-level LSM.**

**Key Requirements**:
- All 16 tests parametrized across all storage modes (Memory, LocalDisk, CloudBacked)
- Pattern: `for_each_storage_mode(&all_storage_modes_new(), |mode, opts| { ... })`
- Streaming bloom: bloom filter that covers multiple levels/files
- Performance: O(1) negative lookups across levels
- Fast: avoid expensive compaction merges
- Correctness: no false negatives
- Levels: cover multiple levels in LSM
- False positives: acceptable for negative filters

**Testing Approach**:
1. Query key not in DB → bloom says no → skip all levels
2. Query key in DB → bloom says maybe → check levels
3. Multi-level coverage
4. Concurrent access
5. Update on flush
6. Rebuild on compaction
7. Performance improvement measurable
8. Large data sets
9. High cardinality keys
10. Low cardinality keys
11. Mixed workload
12. Cache coherency
13. TTL integration
14. Snapshot integration
15. Column family isolation
16. Extreme conditions

---

**File Location**: `tests/streaming_bloom.rs`
**Test Count**: 16 tests
**Storage Modes**: ALL (Memory, LocalDisk, CloudBacked)
**Pattern**: `for_each_storage_mode(&all_storage_modes_new(), |mode, opts| { ... })`
**Status**: 📋 Not yet created (Phase 5 - Streaming layer)

---

## Purpose

Test streaming bloom filters: fast negative filters covering multiple LSM levels. Streaming blooms avoid expensive level traversal for non-existent keys.

---

## Tests

1. **should_indicate_key_absent_given_negative_query_when_checking**
2. **should_indicate_key_present_given_positive_query_when_checking**
3. **should_cover_multiple_levels_given_key_in_level_n_when_checking**
4. **should_provide_fast_lookup_given_bloom_check_when_measuring**
5. **should_handle_concurrent_access_given_multiple_queries_when_checking**
6. **should_update_on_flush_given_new_level_when_rebuilding**
7. **should_rebuild_on_compaction_given_level_merge_when_optimizing**
8. **should_measure_performance_given_streaming_bloom_when_comparing_to_level_scan**
9. **should_handle_large_dataset_given_millions_of_keys_when_querying**
10. **should_handle_high_cardinality_given_unique_keys_when_querying**
11. **should_handle_low_cardinality_given_repeated_keys_when_querying**
12. **should_handle_mixed_workload_given_get_and_scan_when_querying**
13. **should_maintain_cache_coherency_given_flushed_memtable_when_checking**
14. **should_work_with_ttl_given_expired_entries_when_querying**
15. **should_respect_snapshot_visibility_given_snapshot_created_when_querying**
16. **should_isolate_per_column_family_given_multiple_cfs_when_checking**

---

## Key APIs

- Streaming bloom (internal optimization)
- Multi-level filter
- Fast negative test

---

## Implementation Notes

✅ Uses `all_storage_modes_new()` (optimization across all modes)
✅ Covers multiple LSM levels
✅ O(1) negative lookups
✅ False positives acceptable
✅ No false negatives
✅ Updated on flush/compaction

---

## Status

**Current**: 📋 Not yet created (Phase 5 priority - streaming)
**Notes**: Multi-level bloom filter optimization tests

---

## References
- See INTEGRATION_TESTS_FINAL.md lines ~1850 for full streaming bloom spec
- Streaming optimization in `src/streaming/`
