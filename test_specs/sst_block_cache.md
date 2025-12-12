# sst_block_cache.rs - Spec Card

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

**Create a test file that validates block cache: LRU cache for recently-read blocks.**

**Key Requirements**:
- All 12 tests parametrized across durable storage modes (LocalDisk, CloudBacked)
- Pattern: `for_each_storage_mode(&durable_storage_modes(), |mode, opts| { ... })`
- Block cache: LRU cache keeps hot blocks in memory
- Cache hits: repeated block access hits cache
- Cache misses: new blocks loaded from disk
- Cache eviction: LRU eviction when cache full
- Correctness: no cached stale data
- Performance: cache improves latency

**Testing Approach**:
1. Read block, cache hit on re-read
2. Cache capacity limit
3. LRU eviction
4. No stale data after eviction
5. Concurrent cache access
6. Hit rate measurable
7. Configure cache size
8. Disable cache (size=0)
9. Single block larger than cache
10. Hot blocks stay cached
11. Cold blocks evicted
12. Cache coherency with writes

---

**File Location**: `tests/sst_block_cache.rs`
**Test Count**: 12 tests
**Storage Modes**: FS + Cloud ONLY
**Pattern**: `for_each_storage_mode(&durable_storage_modes(), |mode, opts| { ... })`
**Status**: 📋 Not yet created (Phase 5 - SST layer)

---

## Purpose

Test block cache: LRU cache for frequently-accessed blocks. Block cache significantly improves read performance for hot data.

---

## Tests

1. **should_cache_block_given_first_read_when_accessing**
2. **should_hit_cache_given_second_read_when_accessing_same_block**
3. **should_miss_cache_given_different_block_when_accessing**
4. **should_evict_lru_given_cache_full_when_adding_new_block**
5. **should_not_cache_stale_data_given_block_evicted_when_re_reading**
6. **should_respect_cache_capacity_given_configured_size_when_limiting**
7. **should_handle_concurrent_cache_access_given_multiple_readers_when_querying**
8. **should_measure_cache_hit_rate_given_workload_when_monitoring**
9. **should_disable_cache_given_zero_size_when_configured**
10. **should_handle_block_larger_than_cache_given_oversized_block_when_reading**
11. **should_keep_hot_blocks_cached_given_frequent_access_when_iterating**
12. **should_maintain_coherency_with_writes_given_cache_and_modified_blocks_when_comparing**

---

## Key APIs

- Block cache (internal)
- Cache configuration (size, TTL)
- Cache statistics (hits, misses)
- Cache eviction policy (LRU)

---

## Implementation Notes

✅ Uses `durable_storage_modes()` (FS + Cloud)
✅ LRU eviction: least recently used blocks evicted first
✅ Cache transparent to application
✅ Improves latency for hot blocks
✅ Correctness maintained even with cache
✅ Concurrent access safe

---

## Status

**Current**: 📋 Not yet created (Phase 5 priority)
**Notes**: Block cache foundation tests

---

## References
- See INTEGRATION_TESTS_FINAL.md lines ~1680 for full block cache spec
- Cache implementation in `src/sst/`
