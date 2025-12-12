# engine_iterators.rs - Spec Card

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

**Create a test file that validates range scans and iterators with proper filtering and ordering.**

**Key Requirements**:
- All 17 tests parametrized across all storage modes (Memory, LocalDisk, CloudBacked)
- Pattern: `for_each_storage_mode(&all_storage_modes_new(), |mode, opts| { ... })`
- Forward iteration in key order, reverse iteration in reverse order
- Limit support: return only first N keys
- Deleted key filtering: tombstones hide deleted keys
- Range tombstone handling: delete_range tombstones hide ranges
- Empty database handling: empty iteration should work
- Seek semantics: seek to key and iterate from that point
- Streaming scan support: same results as regular scan

**Testing Approach**:
1. Iterate all keys → verify sorted order
2. Reverse iteration → verify reverse sorted order
3. Limit results → verify only N keys returned
4. Empty DB → verify empty iterator
5. Seek to key → iterate from that key forward
6. Skip deleted keys → tombstones filter them out
7. Range tombstones → delete_range tombstones hide ranges
8. Interleaved puts/deletes → verify final state
9. Large scan → verify performance and correctness
10. Streaming scan → verify same results as regular scan
11. Concurrent scans → verify no data races

---

**File Location**: `tests/engine_iterators.rs`
**Test Count**: 17 tests
**Storage Modes**: ALL (Memory, LocalDisk, CloudBacked)
**Pattern**: `for_each_storage_mode(&all_storage_modes_new(), |mode, opts| { ... })`
**Status**: ✅ 17/17 passing

---

## Purpose

Test range scans and iterators across all data shapes: forward/reverse, with limits, across tombstones and range tombstones, and with concurrent modifications. Iterators are critical for range queries and full table scans.

---

## Tests

1. **should_iterate_all_keys_in_order_given_populated_db_when_scanning**
   - Populate DB with 10 keys, scan all, verify sorted order

2. **should_iterate_in_reverse_given_reverse_query_when_scanning**
   - Scan reverse, verify reverse sorted order

3. **should_limit_results_given_limit_query_when_scanning**
   - Scan with limit=5, verify only 5 keys returned

4. **should_return_empty_given_empty_db_when_scanning**
   - Scan empty DB, verify empty result

5. **should_return_next_key_given_seek_to_missing_key_when_scanning**
   - Seek to key that doesn't exist, verify returns next key

6. **should_return_empty_given_seek_past_end_when_scanning**
   - Seek past end of range, verify empty

7. **should_return_empty_given_invalid_range_when_start_greater_than_end**
   - Invalid range (start > end), verify empty

8. **should_skip_deleted_keys_given_tombstones_when_scanning**
   - Delete keys, scan, verify deleted keys not in results

9. **should_respect_range_tombstones_given_delete_range_when_scanning**
   - Delete range, scan, verify range hidden

10. **should_return_latest_value_given_interleaved_puts_deletes_when_scanning**
    - Interleaved puts/deletes, scan shows final state

11. **should_match_regular_scan_given_streaming_scan_when_comparing**
    - Streaming scan vs regular scan, verify same results

12. **should_respect_limit_given_streaming_scan_when_limited**
    - Streaming scan with limit, verify limit respected

13. **should_apply_tombstones_given_streaming_scan_when_keys_deleted**
    - Streaming scan with deleted keys, verify tombstones applied

14. **should_handle_large_scan_given_many_keys_when_iterating**
    - Scan 1000+ keys, verify all returned in order

15. **should_handle_large_streaming_scan_given_multiple_ssts_when_spanning**
    - Streaming scan spanning multiple SSTs

16. **should_handle_concurrent_streaming_scans_when_multiple_threads**
    - Multiple threads scanning concurrently

17. **should_produce_identical_results_given_repeated_scans_when_rewinding**
    - Scan same range multiple times, verify identical results

---

## Key APIs

- `engine.scan(cf, start, end)` → Iterator
- `engine.scan_reverse(cf, start, end)` → ReverseIterator
- `iterator.next()` → Option<(Key, Value)>
- `iterator.limit(n)` → Iterator with limit
- `iterator.seek(key)` → Iterator positioned at key
- Streaming scan APIs (if available)

---

## Implementation Notes

✅ All tests use `all_storage_modes_new()` (iteration semantics are mode-invariant)
✅ Verify sorted order in results
✅ Tombstones automatically filter deleted keys
✅ Range tombstones hide key ranges
✅ Streaming scans provide same results as regular scans
✅ Concurrent scans should not block or corrupt state

---

## Test Pattern Example

```rust
#[test]
fn should_iterate_all_keys_in_order_given_populated_db_when_scanning() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();
        for i in 0..10 {
            engine.put(cf, format!("key{:02}", i).as_bytes(), b"v").unwrap();
        }
        
        // Act
        let results: Vec<_> = engine.scan(cf, b"", b"\xff").unwrap().collect();
        
        // Assert
        assert_eq!(results.len(), 10, "mode: {}", mode);
        for (i, (k, _v)) in results.iter().enumerate() {
            assert_eq!(k, format!("key{:02}", i).as_bytes(), "mode: {}", mode);
        }
    });
}
```

---

## Status

**Current**: ✅ 17/17 passing
**Notes**: Iterator and scan operations fully working, all filtering correct

---

## References
- See INTEGRATION_TESTS_FINAL.md lines ~510 for full iterator spec
- Iterator API in `src/engine/api.rs`
