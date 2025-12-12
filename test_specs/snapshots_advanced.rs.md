# snapshots_advanced.rs - Spec Card

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

**Create a test file that validates advanced snapshot scenarios: stress conditions, interaction with compaction/flush, memory pressure, and edge cases.**

**Key Requirements**:
- All 8 tests parametrized across all storage modes (Memory, LocalDisk, CloudBacked)
- Pattern: `for_each_storage_mode(&all_storage_modes_new(), |mode, opts| { ... })`
- Tests cover snapshot held during compaction (should not block)
- Tests cover snapshot held during flush (should not block)
- Tests cover many concurrent snapshots (memory pressure)
- Tests cover snapshot visibility after concurrent operations
- Tests cover snapshot interaction with write batches
- Tests cover long-lived snapshots

**Testing Approach**:
1. Hold snapshot, trigger compaction, verify compaction succeeds
2. Hold snapshot, trigger flush, verify flush succeeds
3. Create 100 snapshots, verify memory doesn't explode
4. Snapshot + concurrent delete_range interaction
5. Snapshot captured mid-write-batch
6. Multiple snapshots at different points in time
7. Snapshot visible across column families
8. Drop and recreate snapshots (resource cleanup)

**Critical Details**:
- ✅ Use all_storage_modes_new() (snapshot semantics invariant)
- ✅ Snapshots should NOT block compaction/flush
- ✅ Many snapshots should be safe (GC handles cleanup)
- ✅ Concurrent operations don't invalidate snapshots
- ✅ Snapshot visibility should be consistent across CFs
- ✅ Snapshots should be droppable anytime

---

**File Location**: `tests/snapshots_advanced.rs`
**Test Count**: 8 tests
**Storage Modes**: ALL (Memory, LocalDisk, CloudBacked)
**Pattern**: `for_each_storage_mode(&all_storage_modes_new(), |mode, opts| { ... })`
**Status**: 🚧 0/8 not started

---

## Purpose

Test advanced snapshot scenarios beyond basic snapshot isolation. Validates snapshots don't block critical operations (compaction, flush), handle stress conditions (many snapshots), and interact correctly with concurrent operations.

---

## Tests

1. **should_not_block_compaction_given_held_snapshot_when_compaction_triggered**
   - Create snapshot, hold it open
   - Trigger compaction (via flush or writes exceeding thresholds)
   - Verify compaction succeeds (doesn't wait for snapshot)
   - Snapshot still returns consistent view

2. **should_not_block_flush_given_held_snapshot_when_flush_triggered**
   - Create snapshot, hold it open
   - Trigger flush (explicit or implicit)
   - Verify flush succeeds immediately (doesn't wait)
   - Snapshot still valid after flush

3. **should_handle_many_concurrent_snapshots_given_100_snapshots_when_creating**
   - Create 100 snapshots concurrently
   - Memory usage should not grow unbounded
   - All snapshots should remain valid
   - Release all, verify cleanup

4. **should_maintain_isolation_given_concurrent_delete_range_when_snapshot_active**
   - Create snapshot, capture state
   - Perform delete_range in different thread
   - Snapshot should still see pre-delete data
   - Range is deleted in main view

5. **should_see_consistent_state_given_snapshot_across_write_batch_when_committed**
   - Snapshot captures state before batch
   - Write batch commits (multi-op atomic)
   - Snapshot sees pre-batch state
   - New reads see post-batch state

6. **should_maintain_snapshots_at_different_sequence_numbers_when_multiple**
   - Create snapshot S1
   - Write key1→value1
   - Create snapshot S2
   - Write key1→value2
   - S1 sees value1, S2 sees value2

7. **should_preserve_snapshot_across_column_families_when_multiple_cfs**
   - Snapshot with multiple column families active
   - Each CF written to independently
   - Snapshot captures consistent state across all CFs
   - Each CF sees its snapshot state

8. **should_cleanup_resources_given_snapshot_drop_when_no_longer_needed**
   - Create, hold, drop snapshot
   - Verify drop doesn't cause issues
   - Recreate new snapshot
   - Memory should return to clean state

---

## Key APIs

- `engine.snapshot()` → Snapshot (RAII handle)
- `snapshot.get(cf, key)` → Result<Option<Bytes>>
- `snapshot.scan(cf, start, end)` → Iterator at snapshot
- `engine.flush(cf)` → force flush
- `engine.compact_range(cf, start, end)` → force compaction

---

## Implementation Notes

✅ All tests use all_storage_modes_new() (snapshot isolation invariant)
✅ Compaction/flush should NOT be blocked by snapshots
✅ Many snapshots should be safe (garbage collection handles refs)
✅ Concurrent delete_range with snapshot tests range semantics
✅ Write batch atomic within snapshot window
✅ Multiple snapshots at different times test versioning
✅ Cross-CF snapshots test isolation model
✅ Snapshot drop and recreation tests resource cleanup

---

## Test Pattern Example

```rust
#[test]
fn should_not_block_compaction_given_held_snapshot_when_compaction_triggered() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        
        // Write initial data
        for i in 0..100 {
            let key = format!("key_{}", i);
            engine.put(cf, key.as_bytes(), b"value").expect("put");
        }
        
        // Act
        let snapshot = engine.snapshot().expect("snapshot");
        
        // Trigger compaction in another thread
        let engine_clone = Arc::clone(&engine);
        let handle = std::thread::spawn(move || {
            // Compact - should not be blocked by snapshot
            engine_clone.compact_range(cf, None, None).expect("compact");
        });
        
        // Let compaction run (should succeed even with snapshot held)
        let compact_result = handle.join();
        assert!(compact_result.is_ok(), "compaction blocked or panicked in mode: {}", mode);
        
        // Assert - snapshot still valid
        let got = snapshot.get(cf, b"key_0").expect("get");
        assert!(got.is_some(), "snapshot invalidated by compaction in mode: {}", mode);
    });
}
```

---

## Status

**Current**: 🚧 0/8 not started (spec ready)
**Implementation**: Pending Phase 2

---

## References
- See engine_snapshots.rs for basic snapshot tests
- INTEGRATION_TESTS_FINAL.md for snapshot semantics
- Snapshot API in `src/engine/mod.rs`

