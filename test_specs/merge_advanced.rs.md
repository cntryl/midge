# merge_advanced.rs - Spec Card

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

**Create a test file that validates advanced merge operator scenarios: tombstone interactions, operator version changes, error handling, and edge cases.**

**Key Requirements**:
- All 10 tests parametrized across all storage modes (Memory, LocalDisk, CloudBacked)
- Pattern: `for_each_storage_mode(&all_storage_modes_new(), |mode, opts| { ... })`
- Tests cover merge + delete interactions (what happens when merge meets tombstone?)
- Tests cover merge operator failures (what if operator throws error?)
- Tests cover merge over writes and deletes
- Tests cover batch merges (merge in write batch)
- Tests cover multiple merges in sequence
- Tests cover merge with binary data

**Testing Approach**:
1. Merge operation when base value is tombstone (deleted) → merge applies
2. Delete then merge same key → merge operates on deleted state
3. Merge operator returns error → error propagates
4. Multiple merges in single batch → all applied in order
5. Large number of sequential merges (10+) → aggregation correct
6. Merge with binary data (non-UTF8) → roundtrips correctly
7. Merge in transaction → works within txn isolation
8. Merge after delete_range covering key → merge still applies?
9. String append with special characters (empty strings, nulls)
10. Custom operator with state (if supported)

**Critical Details**:
- ✅ Use all_storage_modes_new() (merge semantics are mode-invariant)
- ✅ Test both string append and custom operators
- ✅ Merge + tombstone interaction is critical
- ✅ Error handling must not corrupt state
- ✅ Batch merges must maintain order
- ✅ Binary data support verified

---

**File Location**: `tests/merge_advanced.rs`
**Test Count**: 10 tests
**Storage Modes**: ALL (Memory, LocalDisk, CloudBacked)
**Pattern**: `for_each_storage_mode(&all_storage_modes_new(), |mode, opts| { ... })`
**Status**: 🚧 0/10 not started

---

## Purpose

Test advanced merge operator scenarios beyond basic merge functionality. Validates tombstone interactions, error handling, and complex merge patterns that stress the merge infrastructure.

---

## Tests

1. **should_apply_merge_given_delete_then_merge_when_tombstone_base**
   - Write key, delete it (creates tombstone)
   - Merge operation on deleted key → should apply merge to tombstone
   - Verify final result is merged value (tombstone treated as empty?)

2. **should_delete_after_merge_given_sequence_when_operations**
   - Merge creates value, then delete immediately
   - Verify delete works even after merge
   - Key returns None after delete

3. **should_handle_merge_in_transaction_when_committed**
   - Create transaction, perform merge operation within txn
   - Verify merge applies within transaction
   - Commit, verify persisted

4. **should_propagate_error_given_failing_merge_operator_when_error**
   - Register merge operator that throws error
   - Attempt merge → error propagates
   - Verify state unchanged (error didn't corrupt data)
   - Other keys still accessible

5. **should_apply_multiple_merges_in_batch_given_write_batch_when_committed**
   - Write batch with multiple merge operations on same key
   - Merges apply in order
   - Final value is accumulated result

6. **should_accumulate_values_given_10_sequential_merges_when_applying**
   - Start with no value (or empty)
   - Apply 10 merges in sequence
   - Verify accumulated result (string concatenation)

7. **should_handle_binary_data_in_merge_given_non_utf8_when_merging**
   - Merge with binary keys and values
   - Binary data with null bytes, special chars
   - Round-trip: merge result is valid binary
   - No UTF8 validation needed

8. **should_preserve_merge_with_empty_operand_given_empty_bytes_when_merging**
   - Merge where operand is empty (0 bytes)
   - Should still update (or no-op?) depending on operator
   - Verify well-defined behavior

9. **should_handle_merge_on_key_with_many_tombstones_given_accumulation_when_cleanup**
   - Repeatedly delete and merge same key (creates tombstone stack)
   - Multiple tombstones followed by merge
   - Merge applies correctly despite tombstone history

10. **should_handle_special_characters_in_string_merge_given_delimiters_when_appending**
    - Merge with special delimiter chars: newlines, nulls, UTF-8 sequences
    - String append operator handles all characters
    - No truncation or corruption

---

## Key APIs

- `engine.merge(cf, key, operand)` → Result
- `write_batch.put_merge(cf, key, operand)` → adds merge to batch
- `engine.delete(cf, key)` → creates tombstone
- Merge operators: StringAppend, custom with error handling
- `Transaction::merge()` for transactional merges

---

## Implementation Notes

✅ All tests use all_storage_modes_new() (merge semantics invariant)
✅ Tombstone + merge interaction is critical (should merge apply?)
✅ Error handling must be clean (no state corruption on failure)
✅ Binary data support tested (UTF-8 not required)
✅ Batch merges tested to verify ordering
✅ Special characters in strings tested for delimiters
✅ Transaction merge tested for isolation

---

## Test Pattern Example

```rust
#[test]
fn should_apply_merge_given_delete_then_merge_when_tombstone_base() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();
        
        // Write then delete (tombstone)
        engine.put(cf, b"key", b"initial").expect("put");
        engine.delete(cf, b"key").expect("delete");
        
        // Act - merge on tombstone
        engine.merge(cf, b"key", b"merged_value").expect("merge on tombstone");
        
        // Assert - merge should apply
        let got = engine.get(cf, b"key").expect("get");
        assert!(got.is_some(), "merge didn't apply to tombstone in mode: {}", mode);
    });
}
```

---

## Status

**Current**: 🚧 0/10 not started (spec ready)
**Implementation**: Pending Phase 2

---

## References
- See engine_merge.rs for basic merge tests
- INTEGRATION_TESTS_FINAL.md for merge semantics
- Merge operator API in `src/engine/mod.rs`

