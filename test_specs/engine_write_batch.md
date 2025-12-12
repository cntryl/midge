# engine_write_batch.rs - Spec Card

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

**Create a test file that validates atomic batch semantics for write operations.**

**Key Requirements**:
- All 17 tests parametrized across all storage modes (Memory, LocalDisk, CloudBacked)
- Pattern: `for_each_storage_mode(&all_storage_modes_new(), |mode, opts| { ... })`
- Focus on atomicity: all operations in a batch must succeed or all fail (no partial commits)
- Verify ordering: operations applied in order they were added to batch
- Verify multi-column-family isolation: same key in different CFs are independent
- Test durability: batches persisted across restart (durable modes only)
- Test crash safety: batches atomic even if crash occurs during WAL write

**Testing Approach**:
1. Create batch with multiple operations → verify all applied atomically
2. Apply duplicate keys in same batch → verify last value wins
3. Mix put/delete/merge operations → verify all applied in order
4. Large batches (100s of operations) → verify no limits broken
5. Crash scenarios → verify batch committed entirely or rolled back
6. Concurrent batches → verify no interleaving of partial results
7. Concurrent reads during batch apply → verify atomic visibility
8. TTL in batches → verify TTL metadata preserved

**Critical Details**:
- ✅ Use `engine.write_batch()` API (builder pattern)
- ✅ Single batch operation should be atomic (all-or-nothing)
- ✅ Ordering must be preserved within batch
- ✅ Multi-CF batches should isolate per CF
- ✅ Phase 1/Phase 2 structure for crash tests (batch write, crash during WAL, verify on restart)
- ✅ Concurrent batch tests verify no interleaving

---

**File Location**: `tests/engine_write_batch.rs`
**Test Count**: 17 tests
**Storage Modes**: ALL (Memory, LocalDisk, CloudBacked)
**Pattern**: `for_each_storage_mode(&all_storage_modes_new(), |mode, opts| { ... })`
**Status**: ✅ 17/17 passing

---

## Purpose

Test atomic batch semantics: all-or-nothing commits, ordering, CF isolation, and crash safety. Batches are a critical optimization—they bundle multiple writes into a single atomic unit for efficiency.

---

## Tests

1. **should_commit_all_operations_given_batch_when_write_batch**
   - Create batch with 5 put operations, verify all committed together

2. **should_apply_last_value_given_duplicate_keys_when_write_batch**
   - Batch with same key put twice, verify last value wins

3. **should_succeed_given_empty_batch_when_write_batch**
   - Empty batch should not error

4. **should_delete_key_given_delete_after_put_when_write_batch**
   - Batch: put key, then delete same key, verify key deleted

5. **should_delete_existing_key_given_delete_in_batch_when_write_batch**
   - Batch: delete existing key from DB, verify deleted

6. **should_overwrite_existing_value_given_put_in_batch_when_write_batch**
   - Batch: overwrite existing value, verify new value

7. **should_apply_mixed_operations_in_order_when_write_batch**
   - Batch with put/delete/put/delete mixed, verify order preserved

8. **should_handle_large_batch_given_many_operations_when_write_batch**
   - Batch with 1000 operations, verify all applied

9. **should_persist_batch_given_flush_when_reopening**
   - Batch written, flush, crash, reopen, verify persisted [DURABLE MODES ONLY]

10. **should_write_to_multiple_cfs_given_multi_cf_batch_when_write_batch**
    - Batch operating on multiple column families, verify isolation

11. **should_isolate_keys_given_same_key_in_different_cfs_when_write_batch**
    - Same key in CF1 and CF2, verify independent values

12. **should_not_interleave_given_concurrent_batches_when_write_batch**
    - 2 concurrent batches, verify no partial interleaving

13. **should_be_atomic_given_crash_during_wal_write_when_recovering**
    - Crash during WAL write of batch, verify atomicity on recovery

14. **should_be_atomic_given_large_batch_crash_when_recovering**
    - Large batch crash during WAL write, verify recovery

15. **should_support_batch_with_ttl_when_write_batch**
    - Batch with TTL values, verify TTL preserved

16. **should_maintain_atomicity_during_concurrent_reads_when_write_batch**
    - Concurrent readers during batch apply, verify atomic visibility

17. **should_increment_sequence_numbers_given_batch_operations_when_write_batch**
    - Verify batch operations increment sequence numbers correctly

---

## Key APIs

- `engine.write_batch()` → WriteBatch builder
- `batch.put(cf, key, value)` → WriteBatch
- `batch.delete(cf, key)` → WriteBatch
- `batch.commit()` → Result
- `engine.flush()` → Result (for persistence tests)

---

## Implementation Notes

✅ All tests use `all_storage_modes_new()` (atomicity is mode-invariant logic)
✅ Crash recovery tests use durability_opts() with Phase 1/Phase 2
✅ Verify atomicity by checking all operations applied or none
✅ Concurrent batch tests verify no partial visibility
✅ Large batch tests verify ordering preserved

---

## Test Pattern Example

```rust
#[test]
fn should_commit_all_operations_given_batch_when_write_batch() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();
        
        // Act
        let batch = engine.write_batch();
        batch.put(cf, b"k1", b"v1").unwrap();
        batch.put(cf, b"k2", b"v2").unwrap();
        batch.put(cf, b"k3", b"v3").unwrap();
        batch.commit().unwrap();
        
        // Assert
        assert_eq!(engine.get(cf, b"k1").unwrap(), Some(Bytes::from_static(b"v1")), "mode: {}", mode);
        assert_eq!(engine.get(cf, b"k2").unwrap(), Some(Bytes::from_static(b"v2")), "mode: {}", mode);
        assert_eq!(engine.get(cf, b"k3").unwrap(), Some(Bytes::from_static(b"v3")), "mode: {}", mode);
    });
}
```

---

## Status

**Current**: ✅ 17/17 passing
**Notes**: Atomic batch operations fully working, crash safety verified

---

## References
- See INTEGRATION_TESTS_FINAL.md lines ~380 for full batch spec
- WriteBatch API in `src/engine/api.rs`
