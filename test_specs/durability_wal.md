# durability_wal.rs - Spec Card

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

**Create a test file that validates Write-Ahead Log (WAL) behavior: fsync durability, rotation, replay, and corruption handling.**

**Key Requirements**:
- All 10 tests parametrized across durable storage modes ONLY (LocalDisk, CloudBacked)
- Pattern: `for_each_storage_mode(&durable_storage_modes(), |mode, opts| { ... })`
- Storage Modes: FS and Cloud only (Memory doesn't have WAL)
- WAL recovery: unflushed data in memtable recovered from WAL on restart
- Fsync durability: fsync at configured intervals ensures durability
- Rotation: WAL rotates to new segment when current reaches size limit
- Multi-segment replay: recover from multiple WAL segments in order
- Concurrent recovery: multiple threads can recover in parallel
- Corruption tolerance: handle corrupted entries gracefully

**Testing Approach**:
1. Write data, crash without flush, restart → recover from WAL
2. Fsync enabled (strict durability) → all writes durable immediately
3. WAL rotation: fill segment, write more, verify new segment created
4. Replay multiple segments in order
5. Concurrent recovery: multiple threads recovering same data
6. Corrupted WAL entry: skip/skip/warn, don't crash
7. Empty WAL: handle gracefully
8. WAL with large entries: handle correctly
9. Partial write in WAL: detect and recover
10. WAL cleanup after recovery: old segments removed

**Critical Details**:
- ✅ Use `durable_storage_modes()` (FS + Cloud only)
- ✅ Phase 1 (write and crash) in scoped block so engine drops
- ✅ Phase 2 (reopen and verify) verifies recovery
- ✅ FSysync behavior configurable via DurabilityOptions
- ✅ WAL segments are files on disk
- ✅ Replay must preserve order and values

---

**File Location**: `tests/durability_wal.rs`
**Test Count**: 10 tests
**Storage Modes**: FS + Cloud ONLY (requires WAL)
**Pattern**: `for_each_storage_mode(&durable_storage_modes(), |mode, opts| { ... })`
**Status**: ✅ 10/10 passing

---

## Purpose

Test Write-Ahead Log durability: unflushed memtable data is replayed from WAL on restart, WAL rotates correctly, concurrent recovery works, and corruption is handled gracefully.

---

## Tests

1. **should_recover_unflushed_memtable_given_crash_without_flush_when_reopening**
   - Phase 1: Write data without flush. Phase 2: Reopen, data recovered from WAL

2. **should_ensure_durability_given_fsync_enabled_when_strict_durability**
   - Fsync per write: all data immediately durable

3. **should_rotate_wal_given_segment_size_reached_when_writing**
   - Fill WAL segment, write more, new segment created

4. **should_replay_multiple_segments_given_many_writes_when_recovering**
   - Multiple WAL segments replayed in order on restart

5. **should_handle_concurrent_recovery_given_multiple_threads_when_reopening**
   - Multiple threads recovering WAL in parallel

6. **should_tolerate_corruption_given_corrupted_entry_when_replaying**
   - Corrupted WAL entry skipped/warned, recovery continues

7. **should_handle_empty_wal_given_clean_shutdown_when_reopening**
   - No data in WAL after clean shutdown

8. **should_handle_large_entries_given_multi_megabyte_values_when_writing**
   - Large values in WAL handled correctly

9. **should_detect_partial_write_given_incomplete_entry_when_recovering**
   - Partial write detected and handled

10. **should_cleanup_old_segments_given_recovery_complete_when_starting**
    - Old WAL segments removed after successful recovery

---

## Key APIs

- `engine.put(cf, key, value)` → Result (goes to memtable, then WAL)
- `engine.flush()` → Result (memtable → SST)
- Engine restart (drop and recreate with same path)
- WAL files on disk (direct inspection possible)
- DurabilityOptions configuration

---

## Implementation Notes

✅ Uses `durable_storage_modes()` (FS + Cloud)
✅ Phase 1/Phase 2 structure: write/crash, reopen/verify
✅ Unflushed data held in memtable, backed by WAL
✅ Fsync behavior controlled by DurabilityOptions (Strict = per-write, Steady = periodic)
✅ WAL segments are sequential, replayed in order
✅ Corruption handled gracefully (skip, warn, continue)

---

## Test Pattern Example

```rust
#[test]
fn should_recover_unflushed_memtable_given_crash_without_flush_when_reopening() {
    let opts = durability_opts();
    
    // Phase 1: Write data without flush (then crash via scope)
    {
        let engine = open_with_mode(opts.clone(), StorageMode::LocalDisk);
        let cf = engine.default_column_family();
        engine.put(cf, b"key", b"value").unwrap();
        // Engine dropped here, simulating crash
    }
    
    // Phase 2: Reopen and verify recovery
    {
        let engine = open_with_mode(opts, StorageMode::LocalDisk);
        let cf = engine.default_column_family();
        assert_eq!(engine.get(cf, b"key").unwrap(), Some(Bytes::from_static(b"value")));
    }
}
```

---

## Status

**Current**: ✅ 10/10 passing
**Notes**: WAL recovery, rotation, and corruption handling all working

---

## References
- See INTEGRATION_TESTS_FINAL.md lines ~900 for full WAL spec
- WAL implementation in `src/wal/`
