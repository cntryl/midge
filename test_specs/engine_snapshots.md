# engine_snapshots.rs - Spec Card

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

**Create a test file that validates snapshot isolation and MVCC (Multi-Version Concurrency Control).**

**Key Requirements**:
- All 14+ tests parametrized across all storage modes (Memory, LocalDisk, CloudBacked)
- Pattern: `for_each_storage_mode(&all_storage_modes_new(), |mode, opts| { ... })`
- Snapshot semantics: snapshot captures DB state at sequence number, frozen view
- Isolation: snapshot hides writes that occurred after snapshot creation
- Writes don't block snapshots: writers don't wait for snapshot holders
- Snapshot persistence: snapshots survive across restart (durable modes)
- Compaction safety: snapshots remain valid even if compaction occurs
- Deleted key visibility: deleted keys visible in snapshots taken before deletion

**Testing Approach**:
1. Create snapshot → put data after → snapshot doesn't see new data
2. Snapshot before key exists → get_at returns None
3. Snapshot after write → snapshot sees the write
4. Snapshot before delete → snapshot sees key, current get returns None
5. Multiple snapshots at different points → each has own view
6. Empty DB snapshot → works correctly
7. Concurrent writers don't block snapshots
8. Snapshots survive flush operations
9. Snapshots survive compaction
10. Range scan at snapshot → respects snapshot view
11. Deleted range in snapshot → respects range tombstones from before snapshot
12. Crash with active snapshot → snapshot recoverable

---

**File Location**: `tests/engine_snapshots.rs`
**Test Count**: 14+ tests
**Storage Modes**: ALL (Memory, LocalDisk, CloudBacked)
**Pattern**: `for_each_storage_mode(&all_storage_modes_new(), |mode, opts| { ... })`
**Status**: ✅ 14/14 passing

---

## Purpose

Test snapshot isolation and MVCC: snapshots provide frozen views of the database at a point in time, insulated from concurrent writes. Snapshots are fundamental to consistent reads and transactional semantics.

---

## Tests

1. **should_hide_writes_given_snapshot_created_before_write_when_get_at**
   - Create snapshot, write new key, snapshot.get() returns None

2. **should_return_none_given_snapshot_before_key_exists_when_get_at**
   - Create snapshot before key is written, snapshot.get() returns None

3. **should_see_value_given_snapshot_after_write_when_get_at**
   - Write key, create snapshot, snapshot.get() returns value

4. **should_see_deleted_key_given_snapshot_before_delete_when_get_at**
   - Write key, create snapshot, delete key, snapshot.get() returns value

5. **should_hide_newer_writes_given_snapshot_when_scan_at**
   - Create snapshot, write new keys, scan at snapshot, new keys not visible

6. **should_exclude_keys_written_after_snapshot_when_scan_at**
   - Write keys before snapshot, after snapshot → scan shows only before

7. **should_include_deleted_keys_given_snapshot_before_delete_when_scan_at**
   - Delete key, create snapshot after delete → tombstone visible in snapshot

8. **should_maintain_separate_views_given_multiple_snapshots_when_reading**
   - Create snapshot1, write, create snapshot2, verify each has own view

9. **should_work_correctly_given_empty_database_when_snapshot_created**
   - Snapshot of empty DB works

10. **should_not_block_writes_given_snapshot_held_when_writing**
    - Hold snapshot, concurrent writes proceed without blocking

11. **should_allow_writes_given_snapshot_dropped_when_continuing**
    - Drop snapshot, verify writes continue normally

12. **should_recover_data_given_crash_with_active_snapshot_when_reopening**
    - Active snapshot, crash, reopen, data recoverable

13. **should_preserve_snapshot_view_given_flush_when_reading_at_snapshot**
    - Flush while snapshot active, snapshot view unchanged

14. **should_preserve_snapshot_view_given_compaction_when_reading_at_snapshot**
    - Compaction while snapshot active, snapshot view unchanged

15. **should_preserve_deleted_range_given_snapshot_before_delete_range_when_scan_at**
    - Delete range, create snapshot before delete, scan shows keys

---

## Key APIs

- `engine.snapshot()` → Snapshot
- `snapshot.get(cf, key)` → Result<Option<Bytes>>
- `snapshot.scan(cf, start, end)` → Iterator
- `snapshot.get_at(seq_no)` (internal - not public)
- Snapshot reference counting (automatic drop)

---

## Implementation Notes

✅ All tests use `all_storage_modes_new()` (MVCC semantics are mode-invariant)
✅ Snapshots are immutable frozen views at a specific sequence number
✅ Multiple snapshots can coexist without blocking each other
✅ Writers don't wait for snapshot holders
✅ Compaction and flush safe with active snapshots
✅ Tombstones from before snapshot creation visible in snapshot

---

## Test Pattern Example

```rust
#[test]
fn should_hide_writes_given_snapshot_created_before_write_when_get_at() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();
        
        // Act: Create snapshot, then write
        let snapshot = engine.snapshot().unwrap();
        engine.put(cf, b"key", b"value").unwrap();
        
        // Assert
        assert_eq!(snapshot.get(cf, b"key").unwrap(), None, "snapshot should not see new write in mode: {}", mode);
        assert_eq!(engine.get(cf, b"key").unwrap(), Some(Bytes::from_static(b"value")), "current view should see write in mode: {}", mode);
    });
}
```

---

## Status

**Current**: ✅ 14/14 passing
**Notes**: Snapshot isolation and MVCC fully working

---

## References
- See INTEGRATION_TESTS_FINAL.md lines ~580 for full snapshot spec
- Snapshot API in `src/engine/api.rs`
