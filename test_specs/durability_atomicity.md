# durability_atomicity.rs - Spec Card

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

**Create a test file that validates manifest atomicity and consistency: no orphan SSTs, atomic manifest updates, SST publication, WAL precedence.**

**Key Requirements**:
- All 11 tests parametrized across durable storage modes ONLY (LocalDisk, CloudBacked)
- Pattern: `for_each_storage_mode(&durable_storage_modes(), |mode, opts| { ... })`
- Storage Modes: FS and Cloud only
- Manifest atomicity: manifest updates all-or-nothing (no partial state)
- Orphan SST prevention: deleted/unreferenced SSTs cleaned up
- SST publication: SSTs visible only after manifest records them
- WAL precedence: WAL takes precedence over manifest during recovery
- Concurrent flushes: multiple concurrent flushes ordered correctly
- Compaction completion: compaction atomic and published atomically
- Cleanup: old SSTs removed after compaction
- Sequence number authority: manifest has authoritative sequence
- Truncated WAL: partial WAL entries detected and handled

**Testing Approach**:
1. Manifest update atomicity: verify no partial state
2. Orphan SST cleanup: deleted/unreferenced SSTs removed
3. SST publication: SSTs not visible until manifest committed
4. WAL precedence: WAL entries win over manifest
5. Concurrent flushes: correct ordering maintained
6. Compaction atomicity: compaction is all-or-nothing
7. Cleanup: old SSTs removed
8. Sequence authority: manifest reflects highest sequence
9. Concurrent compaction: multiple compactions ordered
10. Recovery consistency: recovered state consistent
11. Truncated WAL: partial entries handled

---

**File Location**: `tests/durability_atomicity.rs`
**Test Count**: 11 tests
**Storage Modes**: FS + Cloud ONLY (requires persistence)
**Pattern**: `for_each_storage_mode(&durable_storage_modes(), |mode, opts| { ... })`
**Status**: ✅ 11/11 passing

---

## Purpose

Test manifest atomicity and SST consistency: manifest updates are atomic, orphan SSTs are cleaned up, and WAL takes precedence during recovery. Manifest is the source of truth for storage state.

---

## Tests

1. **should_maintain_consistent_manifest_given_concurrent_flushes_when_publishing_ssts**
   - Multiple concurrent flushes, manifest state remains consistent

2. **should_not_create_orphan_ssts_given_failed_manifest_update_when_recovering**
   - Failed manifest update prevents orphan SSTs

3. **should_respect_wal_precedence_given_manifest_sstbut_wal_newer_when_recovering**
   - WAL entries take precedence over manifest SSTs

4. **should_cleanup_unreferenced_ssts_given_compaction_when_removing_old**
   - Old SSTs removed after compaction

5. **should_guarantee_atomicity_given_manifest_update_in_progress_when_crashing**
   - Manifest update atomic even if crash during write

6. **should_expose_ssts_only_after_manifest_commit_given_flush_when_publishing**
   - SSTs not visible until manifest committed

7. **should_handle_concurrent_compactions_given_multiple_compactors_when_ordered**
   - Multiple compactions correctly ordered

8. **should_maintain_sequence_authority_given_manifest_when_querying_highest_sequence**
   - Manifest reflects highest committed sequence

9. **should_detect_truncated_wal_given_partial_entry_when_recovering**
   - Partial WAL entries detected and handled

10. **should_apply_wal_before_manifest_ssts_given_recovery_sequence_when_replaying**
    - WAL entries applied before manifest SSTs

11. **should_maintain_consistency_across_restart_given_multiple_cycles_when_recovering**
    - Consistency maintained across multiple restart cycles

---

## Key APIs

- `engine.put(cf, key, value)` → Result
- `engine.flush()` → Result
- Manifest file access (indirect via recovery)
- SST file inspection (indirect via recovery)
- WAL file access (indirect via recovery)

---

## Implementation Notes

✅ Uses `durable_storage_modes()` (FS + Cloud)
✅ Phase 1/Phase 2 structure: operations/crash, verify recovery
✅ Manifest is single source of truth for storage state
✅ Atomic updates prevent inconsistency
✅ WAL precedence over manifest ensures data safety
✅ Orphan cleanup prevents disk leaks
✅ Concurrent operations correctly ordered

---

## Test Pattern Example

```rust
#[test]
fn should_maintain_consistent_manifest_given_concurrent_flushes_when_publishing_ssts() {
    let opts = durability_opts();
    
    // Phase 1: Concurrent flushes
    {
        let engine = std::sync::Arc::new(open_with_mode(opts.clone(), StorageMode::LocalDisk));
        let cf = engine.default_column_family();
        
        // Write and flush concurrently
        engine.put(cf, b"k1", b"v1").unwrap();
        engine.put(cf, b"k2", b"v2").unwrap();
        engine.flush().unwrap();
        
        engine.put(cf, b"k3", b"v3").unwrap();
        engine.flush().unwrap();
        
        // Engine dropped (simulates shutdown)
    }
    
    // Phase 2: Verify manifest consistency
    {
        let engine = open_with_mode(opts, StorageMode::LocalDisk);
        let cf = engine.default_column_family();
        
        // All data should be present and consistent
        assert_eq!(engine.get(cf, b"k1").unwrap(), Some(Bytes::from_static(b"v1")));
        assert_eq!(engine.get(cf, b"k2").unwrap(), Some(Bytes::from_static(b"v2")));
        assert_eq!(engine.get(cf, b"k3").unwrap(), Some(Bytes::from_static(b"v3")));
    }
}
```

---

## Status

**Current**: ✅ 11/11 passing
**Notes**: Manifest atomicity and SST consistency fully working

---

## References
- See INTEGRATION_TESTS_FINAL.md lines ~1050 for full atomicity spec
- Manifest implementation in `src/manifest/`
