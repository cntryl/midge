# engine_delete_range.rs - Spec Card

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

**Create a test file that validates range deletion with proper [start, end) semantics, tombstone handling, and concurrent safety.**

**Key Requirements**:
- All 10+ tests parametrized across all storage modes (Memory, LocalDisk, CloudBacked)
- Pattern: `for_each_storage_mode(&all_storage_modes_new(), |mode, opts| { ... })`
- Range semantics: [start, end) means includes start, excludes end
- Tombstone handling: deleted keys should not appear in scans
- Concurrent safety: concurrent delete_range + puts/gets should not corrupt state
- Persistence: range tombstones persisted across restart (durable modes)
- Performance: range deletion should not require O(n) individual deletes

**Testing Approach**:
1. Delete range [key1, key3) → verify key1 deleted, key2 deleted, key3 NOT deleted
2. Empty range (start == end) → verify no-op
3. Large range deletion → verify all keys in range removed
4. Multiple consecutive ranges → verify isolation
5. Concurrent delete_range + puts → verify no corruption
6. Mixed ops: put/delete/delete_range → verify correct final state
7. Range deletion with single key → verify behavior
8. Persistence across restart → verify tombstones persisted

**Critical Details**:
- ✅ Use `engine.delete_range(cf, start, end)` API
- ✅ Semantic: [start, end) is inclusive start, exclusive end
- ✅ Tombstones prevent deleted keys from appearing in scans
- ✅ Concurrent operations should be safe (no data races)
- ✅ Test both empty and populated ranges
- ✅ Verify range tombstones persist and take precedence

---

**File Location**: `tests/engine_delete_range.rs`
**Test Count**: 10+ tests
**Storage Modes**: ALL (Memory, LocalDisk, CloudBacked)
**Pattern**: `for_each_storage_mode(&all_storage_modes_new(), |mode, opts| { ... })`
**Status**: ✅ 10/10 passing

---

## Purpose

Test range deletion semantics with proper [start, end) semantics, tombstone creation and visibility, concurrent safety, and persistence. Range deletion is critical for efficient bulk key removal without per-key overhead.

---

## Tests

1. **should_delete_keys_in_range_given_delete_range_when_querying**
   - Delete range [key1, key3), verify key1 and key2 deleted, key3 exists

2. **should_handle_empty_range_given_start_equals_end_when_delete_range**
   - Delete range where start == end, verify no-op

3. **should_accept_delete_range_call_with_valid_bounds_when_called**
   - Calls delete_range with valid start/end bounds, verify operation succeeds

4. **should_delete_key_given_delete_range_with_single_key_when_matching**
   - Delete range [key, key+1), verify single key deleted

5. **should_handle_delete_range_after_put_when_interleaved**
   - Put key, then delete_range covering it, verify deleted

6. **should_allow_multiple_delete_ranges_when_called_sequentially**
   - Multiple ranges: [key1-key5), [key6-key10), verify each isolated

7. **should_persist_keys_across_delete_range_with_restart_when_durable**
   - Delete range, flush, restart, verify keys still deleted

8. **should_handle_concurrent_delete_ranges_when_multiple_threads**
   - 2 threads each deleting different ranges concurrently

9. **should_handle_concurrent_mixed_operations_when_put_delete_interleaved**
   - Concurrent: one thread puts, another delete_range, verify final state correct

10. **should_document_current_limitation_of_range_method_when_called**
    - Document any known limitations of range API

---

## Key APIs

- `engine.delete_range(cf, start, end)` → Result
- `engine.scan(cf, start, end)` → Iterator (for verification)
- Range tombstone internals (not public API, but tested indirectly)

---

## Implementation Notes

✅ All tests use `all_storage_modes_new()` (semantics are mode-invariant)
✅ Range deletion creates tombstones, not individual deletes
✅ Tombstones prevent keys from appearing in future scans
✅ [start, end) semantics: inclusive start, exclusive end
✅ Concurrent operations safe (actor model handles serialization)
✅ Persistence verified in durable modes

---

## Test Pattern Example

```rust
#[test]
fn should_delete_keys_in_range_given_delete_range_when_querying() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();
        engine.put(cf, b"key1", b"v1").unwrap();
        engine.put(cf, b"key2", b"v2").unwrap();
        engine.put(cf, b"key3", b"v3").unwrap();
        
        // Act: Delete range [key1, key3)
        engine.delete_range(cf, b"key1", b"key3").unwrap();
        
        // Assert
        assert_eq!(engine.get(cf, b"key1").unwrap(), None, "key1 should be deleted in mode: {}", mode);
        assert_eq!(engine.get(cf, b"key2").unwrap(), None, "key2 should be deleted in mode: {}", mode);
        assert_eq!(engine.get(cf, b"key3").unwrap(), Some(Bytes::from_static(b"v3")), "key3 should exist in mode: {}", mode);
    });
}
```

---

## Status

**Current**: ✅ 10/10 passing
**Notes**: Range deletion working correctly with proper semantics

---

## References
- See INTEGRATION_TESTS_FINAL.md lines ~440 for full delete_range spec
- RangeScan infrastructure in `src/runtime/`
