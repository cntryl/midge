# column_families.rs - Spec Card

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

**Create a test file that validates multi-column-family operations and isolation.**

**Key Requirements**:
- All 28 tests parametrized across all storage modes (Memory, LocalDisk, CloudBacked)
- Pattern: `for_each_storage_mode(&all_storage_modes_new(), |mode, opts| { ... })`
- Column families: independent key namespaces within same engine
- Isolation: same key in different CFs are completely independent
- Operations: put/get/delete/scan all work per CF
- Merges: different CFs can have different merge operators
- TTL: per-CF TTL settings independent
- Persistence: CF structure persisted across restart
- Lifecycle: create CF at startup, drop CF (optional)
- Ordering: operations within CF ordered, but operations across CFs not globally ordered
- Snapshots: snapshots capture state of all CFs

**Testing Approach**:
1. Create multiple CFs, put same key in each, verify isolation
2. Delete in CF1, get from CF2, different value
3. Scan across multiple CFs, verify per-CF isolation
4. Different merge operators per CF
5. Different TTL settings per CF
6. Create CF, write data, drop CF, recreate CF (clean slate)
7. Restart with CFs, verify all recovered
8. Concurrent writes across CFs
9. Snapshots across multiple CFs
10. Batches spanning multiple CFs
11. Merge operators with different semantics
12. TTL per CF
13. Column family metadata persisted
14. Get column family by name
15. List all column families
16. Compaction isolation per CF
17. Block cache per CF (if supported)
18. Range queries per CF
19. Delete range per CF
20. Concurrent operations across CFs
21. Stress test many CFs
22. Recovery with many CFs
23. Flush per CF
24. Snapshot visibility across CFs
25. Concurrent CF creation
26. Large values per CF
27. Binary data per CF
28. Mixed operation types per CF

---

**File Location**: `tests/column_families.rs`
**Test Count**: 28 tests
**Storage Modes**: ALL (Memory, LocalDisk, CloudBacked)
**Pattern**: `for_each_storage_mode(&all_storage_modes_new(), |mode, opts| { ... })`
**Status**: 🚧 12/28 passing (16 failing - CF lifecycle and compaction isolation pending)

---

## Purpose

Test column family operations: independent key namespaces within the same engine. Column families enable logical separation of data with different tuning per family.

---

## Tests

1. **should_isolate_same_key_given_different_cfs_when_writing**
   - Same key in CF1 and CF2, different values

2. **should_delete_in_cf_given_different_cf_isolation_when_deleting**
   - Delete in CF1, CF2 unchanged

3. **should_scan_per_cf_given_multiple_cfs_when_iterating**
   - Scan CF1 and CF2, each returns only its data

4. **should_support_different_merge_operators_per_cf_when_merging**
   - CF1 has append, CF2 has sum

5. **should_apply_different_ttl_per_cf_when_expiring**
   - CF1 TTL=1h, CF2 TTL=1min, expire independently

6. **should_recover_all_cfs_given_restart_when_reopening**
   - Restart with multiple CFs, all recovered

7. **should_handle_concurrent_writes_across_cfs_when_multiple_threads**
   - Threads writing to different CFs

8. **should_capture_all_cfs_given_snapshot_when_reading_at_snapshot**
   - Snapshot covers all CFs

9. **should_span_multiple_cfs_given_batch_when_write_batch**
   - Single batch operates across multiple CFs

10. **should_apply_merge_per_cf_given_different_operators_when_merging**
    - Merge semantics per CF

11. **should_isolate_compaction_per_cf_given_independent_levels_when_compacting**
    - Compaction per CF doesn't affect others

12. **should_isolate_block_cache_per_cf_given_separate_caches_when_caching**
    - Block cache per CF (if supported)

13. **should_handle_delete_range_per_cf_when_deleting**
    - Delete range in CF1, CF2 unaffected

14. **should_handle_concurrent_cf_operations_given_multiple_threads_when_stressed**
    - High concurrency across CFs

15. **should_flush_per_cf_independently_given_flush_request_when_flushing**
    - Flush CF1, CF2 data not flushed

16. **should_support_many_cfs_given_large_number_when_creating**
    - Create 50+ CFs

17. **should_recover_data_given_crash_with_multiple_cfs_when_reopening**
    - Crash with multiple active CFs, recover all

18. **should_apply_range_scan_per_cf_when_iterating**
    - Range scan returns only CF-specific data

19. **should_hide_writes_given_snapshot_before_write_per_cf_when_using_snapshot**
    - Snapshot isolation per CF

20. **should_persist_cf_structure_given_restart_when_reopening**
    - CF metadata persisted

21. **should_handle_cf_with_large_values_when_writing**
    - Large values per CF

22. **should_handle_cf_with_binary_data_when_storing**
    - Binary data per CF

23. **should_apply_batch_operations_across_multiple_cfs_when_committed**
    - Batch with mixed CF operations

24. **should_support_get_column_family_by_name_when_querying**
    - Get CF handle by name

25. **should_list_all_column_families_when_enumerating**
    - List all CFs in engine

26. **should_isolate_merge_history_per_cf_given_different_operators_when_changing**
    - Merge history isolated per CF

27. **should_handle_cf_ttl_with_snapshots_when_reading_at_snapshot**
    - TTL respect in CF snapshots

28. **should_maintain_isolation_under_concurrent_stress_given_many_operations_when_heavy_load**
    - Stress test CF isolation

---

## Key APIs

- `engine.create_column_family(name)` → Result<ColumnFamily>
- `engine.get_column_family(name)` → Result<ColumnFamily>
- `engine.list_column_families()` → Vec<ColumnFamily>
- `engine.drop_column_family(cf)` → Result (may not be supported)
- `engine.default_column_family()` → ColumnFamily
- All operations take CF parameter: `put(cf, key, value)`

---

## Implementation Notes

✅ All tests use `all_storage_modes_new()` (CF semantics are mode-invariant)
✅ Each CF is independent namespace for keys
✅ Operations per CF: put, get, delete, scan, merge, TTL all per CF
✅ CF structure persisted across restarts
✅ Concurrent operations across CFs safe
✅ Snapshots capture state of all CFs simultaneously

---

## Test Pattern Example

```rust
#[test]
fn should_isolate_same_key_given_different_cfs_when_writing() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf1 = engine.create_column_family("cf1").unwrap();
        let cf2 = engine.create_column_family("cf2").unwrap();
        
        // Act
        engine.put(cf1, b"key", b"value1").unwrap();
        engine.put(cf2, b"key", b"value2").unwrap();
        
        // Assert
        assert_eq!(engine.get(cf1, b"key").unwrap(), Some(Bytes::from_static(b"value1")), "cf1 in mode: {}", mode);
        assert_eq!(engine.get(cf2, b"key").unwrap(), Some(Bytes::from_static(b"value2")), "cf2 in mode: {}", mode);
    });
}
```

---

## Status

**Current**: 🚧 12/28 passing (16 failing - CF lifecycle, recovery, and advanced features pending)
**Notes**: Basic CF operations working; recovery and compaction isolation pending

---

## References
- See INTEGRATION_TESTS_FINAL.md lines ~800 for full CF spec
- ColumnFamily API in `src/engine/api.rs`
