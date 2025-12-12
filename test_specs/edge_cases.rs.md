# edge_cases.rs - Spec Card

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

**Create a test file that validates engine behavior at the boundaries of normal operation: extreme data sizes, empty states, and stress conditions.**

**Key Requirements**:
- All 12 tests parametrized across all storage modes (Memory, LocalDisk, CloudBacked)
- Pattern: `for_each_storage_mode(&all_storage_modes_new(), |mode, opts| { ... })`
- Tests cover data size extremes: very large keys, very large values
- Tests cover empty/boundary conditions: empty database, single record
- Tests cover stress patterns: thousands of keys, rapid operations
- Focus on correctness, not performance (use perf_regression.rs for benchmarks)

**Testing Approach**:
1. Write very large key (1MB+) → retrieve correctly
2. Write very large value (100MB+) → retrieve correctly
3. Write to empty database → returns None
4. Database with single record → single get works
5. Many keys (10k+) → can handle large sets
6. Scan with empty results → returns empty iterator
7. Mixed sizes: tiny keys, huge values, etc.
8. Delete all keys → database becomes empty
9. Rapid sequence of operations (1k ops) → no loss
10. Tombstones accumulate → compaction handles cleanup
11. Range operations with extremes
12. TTL with very long timeouts

**Critical Details**:
- ✅ Use all_storage_modes_new() (edge cases are mode-invariant)
- ✅ Don't optimize for performance (that's perf_regression.rs)
- ✅ Test correctness, not speed
- ✅ Very large values should work (up to memory/disk limit)
- ✅ Verify data integrity with checksums if available
- ✅ Handle graceful degradation (errors vs panics)

---

**File Location**: `tests/edge_cases.rs`
**Test Count**: 12 tests
**Storage Modes**: ALL (Memory, LocalDisk, CloudBacked)
**Pattern**: `for_each_storage_mode(&all_storage_modes_new(), |mode, opts| { ... })`
**Status**: 🚧 0/12 not started

---

## Purpose

Test engine behavior at boundaries and extreme conditions: very large keys/values, empty states, high operation counts, and stress patterns. Validates correctness under unusual but valid scenarios.

---

## Tests

1. **should_handle_very_large_key_given_1mb_key_when_storing**
   - Write key of 1MB size (filled with repeated pattern)
   - Read back, verify exact match
   - Verify no truncation or corruption

2. **should_handle_very_large_value_given_100mb_value_when_storing**
   - Write value of 100MB (memory mode may skip)
   - Read back, verify integrity (checksum or first/last bytes)
   - Verify partial reads don't corrupt state

3. **should_return_none_given_empty_database_when_get**
   - Fresh engine, no writes
   - Get any key, verify returns None
   - No errors, clean behavior

4. **should_handle_database_with_single_record_when_operations**
   - Write one key→value
   - Get returns value
   - Delete works
   - Get after delete returns None

5. **should_handle_many_keys_given_10k_records_when_writing**
   - Write 10,000 keys with different values
   - Scan all, verify count is 10,000
   - Random get on subset, verify correctness

6. **should_return_empty_iterator_given_empty_database_when_scan**
   - Fresh engine, scan entire keyspace
   - Iterator returns 0 entries
   - No errors on empty result

7. **should_handle_mixed_value_sizes_given_tiny_and_huge_when_storing**
   - Write keys with sizes: 1 byte, 1KB, 1MB, 100MB
   - Retrieve each, verify sizes correct
   - Verify no size confusion

8. **should_become_empty_given_delete_all_keys_when_written**
   - Write 1000 keys, delete each one
   - Verify each delete succeeds
   - Scan returns 0 entries
   - Database recoverable to empty state

9. **should_maintain_correctness_given_rapid_operations_when_1k_ops**
   - 1000 operations rapid-fire: put, get, delete, put, scan
   - No data loss
   - Final state consistent
   - No panics or errors

10. **should_handle_tombstone_accumulation_given_many_deletes_when_cleanup**
    - Write 1000 keys, delete 999 of them
    - Database mostly tombstones
    - Compaction should clean up (if available)
    - Remaining key still accessible

11. **should_handle_range_operation_with_extreme_bounds_when_scanning**
    - Range scan with start key before all keys
    - Range scan with end key after all keys
    - Verify correct results despite bounds
    - No errors with invalid ranges

12. **should_handle_ttl_with_very_long_timeout_given_years_when_expiring**
    - Write key with TTL = 1000 years (won't expire in test)
    - Verify key readable immediately
    - Verify not expired after short time
    - Confirm TTL metadata preserved

---

## Key APIs

- `engine.put(cf, key, value)` → Result
- `engine.get(cf, key)` → Result<Option<Bytes>>
- `engine.delete(cf, key)` → Result
- `engine.scan(cf, start, end)` → Iterator
- `engine.delete_range(cf, start, end)` → Result
- `std::fs::metadata()` → File size info

---

## Implementation Notes

✅ All tests use all_storage_modes_new() (edge cases are semantics, not storage)
✅ Very large values: memory mode may limit to available RAM
✅ Tests focus on CORRECTNESS not performance (no benchmarking)
✅ Verify data integrity through get operations (not checksums)
✅ Rapid operations stress concurrency and ordering
✅ Tombstone accumulation tests compaction efficiency
✅ Range operations verify boundary semantics
✅ TTL with long timeouts validates metadata handling

---

## Test Pattern Example

```rust
#[test]
fn should_handle_very_large_value_given_100mb_value_when_storing() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();
        
        // Create 100MB value (pattern to avoid allocating huge strings)
        let value = vec![42u8; 100 * 1024 * 1024];
        
        // Act
        engine.put(cf, b"large_key", &value).expect("put large");
        
        // Assert
        let got = engine.get(cf, b"large_key").expect("get large");
        assert_eq!(got.map(|v| v.len()), Some(100 * 1024 * 1024), "size mismatch in mode: {}", mode);
        
        // Verify first and last bytes match (don't load all 100MB into memory)
        let retrieved = got.expect("value");
        assert_eq!(retrieved[0], 42, "first byte mismatch");
        assert_eq!(retrieved[retrieved.len() - 1], 42, "last byte mismatch");
    });
}
```

---

## Status

**Current**: 🚧 0/12 not started (spec ready)
**Implementation**: Pending Phase 2

---

## References
- See INTEGRATION_TESTS_FINAL.md for storage mode patterns
- Edge case handling in `src/engine/mod.rs`
- Very large value support documented in architecture

