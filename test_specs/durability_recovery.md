# durability_recovery.rs - Spec Card

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

**Create a test file that validates crash recovery after failures: clean shutdown, crashes during flush, crashes during WAL writes, and idempotency.**

**Key Requirements**:
- All 14 tests parametrized across durable storage modes ONLY (LocalDisk, CloudBacked)
- Pattern: `for_each_storage_mode(&durable_storage_modes(), |mode, opts| { ... })`
- Storage Modes: FS and Cloud only (Memory has no persistence)
- Clean shutdown: flush memtable, close cleanly, data recoverable
- Crash after flush: flush complete, then crash → data in SST recoverable
- Crash during flush: flush in progress, crash → WAL recovers unflushed
- WAL precedence: WAL takes precedence over partial SST
- Delete recovery: delete operations persist and recover correctly
- Batch recovery: batch atomicity preserved across crash
- Sequence number continuity: recovered state has correct sequence numbers
- Idempotency: multiple recovery cycles produce same state

**Testing Approach**:
1. Clean shutdown: write, flush, crash safely → recover successfully
2. Crash after flush: flush complete, then crash → SST intact, recover
3. Crash during flush: in middle of flush, crash → WAL recovers, flush redone
4. Crash with deletes: delete operations recovered correctly
5. Delete in batch: batch with deletes recovered atomically
6. Multiple flushes: write, flush, write, flush, crash → all recoverable
7. Idempotency: crash, recover, restart, crash, recover → same state
8. Sequence numbers: seq numbers continuous after recovery
9. Range tombstones: persisted and honored after recovery
10. Concurrent recovery: multiple threads recovering same data
11. Partial manifest write: handle truncated manifest gracefully
12. Large transaction recovery: large batches recovered correctly
13. TTL with recovery: TTL metadata preserved across recovery
14. Column family recovery: multi-CF state recovered correctly

---

**File Location**: `tests/durability_recovery.rs`
**Test Count**: 14 tests
**Storage Modes**: FS + Cloud ONLY (requires persistence)
**Pattern**: `for_each_storage_mode(&durable_storage_modes(), |mode, opts| { ... })`
**Status**: 🚧 13/14 passing (1 failing - delete recovery pending implementation)

---

## Purpose

Test crash recovery semantics: clean shutdown, crashes at various points, WAL/SST consistency, atomicity, and idempotency. Recovery is critical for reliability.

---

## Tests

1. **should_recover_clean_given_flush_complete_when_reopening**
   - Clean shutdown after flush → all data recoverable

2. **should_recover_data_after_flush_given_crash_immediately_when_reopening**
   - Crash immediately after flush complete → SST intact

3. **should_recover_unflushed_given_crash_during_flush_when_reopening**
   - Crash during flush → WAL recovers unflushed data

4. **should_recover_delete_operations_given_deletes_in_log_when_reopening**
   - Delete operations recovered from WAL

5. **should_recover_batch_atomicity_given_batch_in_wal_when_reopening**
   - Batch atomicity preserved across crash

6. **should_handle_multiple_flushes_given_interleaved_writes_when_recovering**
   - Multiple write/flush cycles, all recoverable

7. **should_maintain_idempotency_given_multiple_restarts_when_crashing_repeatedly**
   - Multiple crash/recover cycles produce same state

8. **should_preserve_sequence_numbers_given_recovery_when_reopening**
   - Sequence numbers continuous after recovery

9. **should_respect_range_tombstones_given_delete_range_in_log_when_recovering**
   - Range tombstones persisted and honored

10. **should_handle_concurrent_recovery_given_multiple_readers_when_reopening**
    - Multiple threads can recover in parallel

11. **should_handle_truncated_manifest_given_partial_write_when_recovering**
    - Truncated manifest detected and recovered

12. **should_recover_large_batches_given_multi_megabyte_transaction_when_crashing**
    - Large batch data recovered correctly

13. **should_preserve_ttl_metadata_given_ttl_values_in_log_when_recovering**
    - TTL metadata preserved across recovery

14. **should_recover_multi_cf_state_given_multiple_column_families_when_reopening**
    - Multi-CF state recovered correctly

---

## Key APIs

- `engine.put(cf, key, value)` → Result
- `engine.delete(cf, key)` → Result
- `engine.flush()` → Result
- Engine drop and recreate (simulates restart)
- Phase 1/Phase 2 testing pattern

---

## Implementation Notes

✅ Uses `durable_storage_modes()` (FS + Cloud)
✅ Phase 1/Phase 2 structure: crash in scoped block, verify in reopened engine
✅ Clean shutdown: flush all memtables, close gracefully
✅ Crash handling: WAL and SST consistency verified
✅ Atomicity: batch operations atomic across crash
✅ Idempotency: multiple restarts produce same state
✅ Test 4 (delete recovery) may fail if feature not fully implemented (expected)

---

## Test Pattern Example

```rust
#[test]
fn should_recover_clean_given_flush_complete_when_reopening() {
    let opts = durability_opts();
    
    // Phase 1: Write and flush cleanly
    {
        let engine = open_with_mode(opts.clone(), StorageMode::LocalDisk);
        let cf = engine.default_column_family();
        engine.put(cf, b"key", b"value").unwrap();
        engine.flush().unwrap(); // Clean flush
        // Engine dropped, clean shutdown
    }
    
    // Phase 2: Reopen and verify
    {
        let engine = open_with_mode(opts, StorageMode::LocalDisk);
        let cf = engine.default_column_family();
        assert_eq!(engine.get(cf, b"key").unwrap(), Some(Bytes::from_static(b"value")));
    }
}
```

---

## Status

**Current**: 🚧 13/14 passing (1 failing - delete recovery not yet implemented, documented as expected)
**Notes**: Core recovery working; delete recovery feature pending

---

## References
- See INTEGRATION_TESTS_FINAL.md lines ~970 for full recovery spec
- Recovery logic in `src/engine/`
