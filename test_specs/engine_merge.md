# engine_merge.rs - Spec Card

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

**Create a test file that validates merge operator semantics and behavior.**

**Key Requirements**:
- All 19 tests parametrized across all storage modes (Memory, LocalDisk, CloudBacked)
- Pattern: `for_each_storage_mode(&all_storage_modes_new(), |mode, opts| { ... })`
- Merge operators: custom logic for combining values (e.g., string append, integer add)
- Merge without base: merging when key doesn't exist (use empty/default base)
- Merge with existing: combine new operand with existing value
- Sequential merges: multiple merges apply in order
- Merge after delete: treat deleted key as missing, use default base
- Operator registration: column family can have custom merge operator
- Error handling: merge operator failures should surface errors
- Persistence: merge semantics preserved across restart
- Concurrency: concurrent merges to same key use operator logic

**Testing Approach**:
1. Merge without base value → verify default/empty handling
2. Merge with existing value → verify combination logic
3. Multiple sequential merges → verify accumulation
4. Merge after delete → treat as missing
5. String append operator → concatenate with delimiter
6. Custom operators per column family
7. Default CF merge independently from custom CF
8. Invalid/failing merge operator → surface error
9. Operator change across restart → handle gracefully
10. Concurrent merges to same key → operator handles ordering
11. Merge with binary data → preserve non-UTF8
12. Merge with range tombstones → respect tombstone precedence

---

**File Location**: `tests/engine_merge.rs`
**Test Count**: 19 tests
**Storage Modes**: ALL (Memory, LocalDisk, CloudBacked)
**Pattern**: `for_each_storage_mode(&all_storage_modes_new(), |mode, opts| { ... })`
**Status**: 🚧 11/19 passing (8 failing - advanced features pending)

---

## Purpose

Test merge operators for value combination: custom application-defined logic for combining values. Merge operators enable efficient aggregation and update operations without full value retrieval.

---

## Tests

1. **should_merge_without_base_value_given_no_existing_key_when_merging**
   - Merge on non-existent key, use empty base

2. **should_merge_with_existing_base_value_given_put_when_merging**
   - Put value, then merge, combine them

3. **should_apply_multiple_merges_sequentially_given_repeated_operations_when_reading**
   - Multiple merges on same key, apply in order

4. **should_merge_after_delete_given_tombstone_when_treating_as_missing**
   - Delete key, then merge, treat as missing base

5. **should_handle_merge_with_put_interleaved_given_mixed_ops_when_reading**
   - Merge, put, merge again, verify final state

6. **should_use_string_append_operator_given_delimiter_when_merging**
   - String append merge: concatenate with delimiter

7. **should_string_append_with_base_value_given_initial_put_when_merging**
   - Put initial, string append merge, verify concatenation

8. **should_handle_empty_merge_operand_given_empty_bytes_when_appending**
   - Merge with empty operand, verify handling

9. **should_isolate_merge_operators_across_cfs_given_different_operators_when_merging**
   - CF1 has append operator, CF2 has sum operator, verify isolation

10. **should_handle_default_cf_merge_independently_given_custom_cf_when_merging**
    - Default CF merge independent from custom CF

11. **should_preserve_merge_semantics_across_restart_given_flush_when_recovering**
    - Flush merged data, restart, verify semantics

12. **should_persist_merge_resolutions_given_cf_restart_when_reopening**
    - Persist merge state, reopen, verify

13. **should_error_when_merging_without_registered_operator_when_merging**
    - Merge without operator registered, return error

14. **should_surface_error_given_failing_merge_operator_when_getting**
    - Merge operator fails, error surfaced on get

15. **should_keep_data_readable_given_merge_operator_changed_across_restart_when_reopening**
    - Operator changed between restarts, data still readable

16. **should_not_lose_merge_operands_under_concurrency_given_same_key_when_merging**
    - Concurrent merges to same key, no data loss

17. **should_handle_concurrent_merges_to_same_key_given_integer_add_operator_when_merging**
    - Concurrent integer add merges, verify correctness

18. **should_handle_merge_with_binary_data_given_binary_key_when_merging**
    - Merge with binary data, verify round-trip

19. **should_not_merge_across_delete_range_given_range_tombstone_when_merging**
    - Range tombstone precedence over merge

---

## Key APIs

- `engine.merge(cf, key, operand)` → Result
- `engine.get(cf, key)` → Result<Option<Bytes>> (triggers merge resolution)
- Merge operator registration (in OpenOptions)
- Custom operator trait (application-defined)

---

## Implementation Notes

✅ All tests use `all_storage_modes_new()` (merge semantics are mode-invariant)
✅ Merge operators are column-family specific
✅ String append with delimiter is a common pattern
✅ Integer add is another common pattern (for counters)
✅ Merge semantics preserved across persistence and restart
✅ Concurrent merges to same key accumulate correctly

---

## Test Pattern Example

```rust
#[test]
fn should_merge_without_base_value_given_no_existing_key_when_merging() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();
        
        // Act: Merge without existing key
        engine.merge(cf, b"key", b"operand").unwrap();
        
        // Assert: Should have merged with empty base
        let result = engine.get(cf, b"key").unwrap();
        assert!(result.is_some(), "merge should create entry in mode: {}", mode);
    });
}
```

---

## Status

**Current**: 🚧 11/19 passing (8 failing - advanced merge features pending)
**Notes**: Basic merge working; advanced operators and concurrency pending

---

## References
- See INTEGRATION_TESTS_FINAL.md lines ~650 for full merge spec
- Merge operator API in `src/engine/api.rs`
