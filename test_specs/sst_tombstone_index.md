# sst_tombstone_index.rs - Spec Card

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

**Create a test file that validates SST range tombstone (delete_range) indexing and application.**

**Key Requirements**:
- All 20 tests parametrized across durable storage modes (LocalDisk, CloudBacked)
- Pattern: `for_each_storage_mode(&durable_storage_modes(), |mode, opts| { ... })`
- Tombstone indexing: efficiently locate tombstones that affect key ranges
- Application: tombstones correctly hide keys in their range
- Ordering: tombstones applied in correct order
- Overlap handling: multiple tombstones in same range
- Efficiency: don't iterate all tombstones for each key

**Testing Approach**:
1. Create SST with tombstones, read keys → verify hidden
2. Multiple overlapping tombstones → all applied
3. Tombstone before key → key hidden
4. Tombstone after key → key visible
5. Partial overlap → correct keys hidden
6. Empty range tombstone
7. Adjacent tombstones
8. Concurrent tombstone access
9. TTL interaction with tombstones
10. Compaction with tombstones
11. Tombstone index size reasonable
12. Index rebuild
13. Corruption detection
14. Large number of tombstones
15. Sparse tombstones
16. Tombstone merge scenarios
17. Scan with tombstones
18. Get with tombstone
19. Range query with tombstones
20. Snapshot with tombstone visibility

---

**File Location**: `tests/sst_tombstone_index.rs`
**Test Count**: 20 tests
**Storage Modes**: FS + Cloud ONLY
**Pattern**: `for_each_storage_mode(&durable_storage_modes(), |mode, opts| { ... })`
**Status**: 📋 Not yet created (Phase 5 - SST layer)

---

## Purpose

Test SST range tombstone (delete_range) indexing: tombstones efficiently stored and indexed, correctly applied to hide key ranges during reads.

---

## Tests

1. **should_hide_keys_in_tombstone_range_given_delete_range_when_reading**
2. **should_apply_multiple_tombstones_given_overlapping_ranges_when_reading**
3. **should_hide_key_given_tombstone_before_key_when_reading**
4. **should_show_key_given_tombstone_after_key_when_reading**
5. **should_handle_partial_overlap_given_tombstone_when_reading**
6. **should_handle_empty_range_tombstone_given_zero_range_when_reading**
7. **should_handle_adjacent_tombstones_given_contiguous_ranges_when_reading**
8. **should_support_concurrent_access_given_multiple_readers_when_querying**
9. **should_interact_correctly_with_ttl_given_expired_and_tombstone_when_reading**
10. **should_survive_compaction_given_tombstones_in_sst_when_compacting**
11. **should_maintain_reasonable_index_size_given_large_number_of_tombstones_when_indexing**
12. **should_rebuild_index_given_verification_when_reopening**
13. **should_detect_corruption_given_invalid_tombstone_when_reading**
14. **should_handle_many_tombstones_given_1000plus_ranges_when_indexing**
15. **should_handle_sparse_tombstones_given_few_ranges_when_querying**
16. **should_merge_adjacent_tombstones_given_optimization_when_compacting**
17. **should_work_with_scan_operations_given_tombstones_in_range_when_iterating**
18. **should_work_with_get_operations_given_tombstone_covering_key_when_reading**
19. **should_handle_large_range_queries_given_many_tombstones_when_scanning**
20. **should_respect_snapshot_visibility_given_tombstone_created_after_snapshot_when_reading_at_snapshot**

---

## Key APIs

- Tombstone storage (internal)
- Tombstone index (internal)
- Range application logic
- Scan with tombstone filtering

---

## Implementation Notes

✅ Uses `durable_storage_modes()` (FS + Cloud)
✅ Tombstones stored in SST alongside regular keys
✅ Efficient index prevents O(n) tombstone checking
✅ Tombstones applied during read/scan operations
✅ Multiple tombstones handled correctly
✅ Snapshot-aware visibility

---

## Status

**Current**: 📋 Not yet created (Phase 5 priority)
**Notes**: Range tombstone index foundation tests

---

## References
- See INTEGRATION_TESTS_FINAL.md lines ~1540 for full tombstone spec
- Tombstone implementation in `src/sst/`
