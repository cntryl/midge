# memory_mode_isolation.rs - Spec Card

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

**Create a test file that validates memory mode creates no persistent filesystem artifacts and isolates data between engine instances.**

**Key Requirements**:
- All tests run MEMORY MODE ONLY (not parametrized across modes)
- Pattern: `let opts = memory_opts();` then `open_with_mode(opts, StorageMode::Memory)`
- Memory mode must NOT create any filesystem artifacts (no .db, no WAL, no SST, no manifest)
- Data persisted to memory, not disk
- Multiple memory mode instances are isolated (separate data)
- Engine close/drop cleans up all memory
- No spill files even for large transactions

**Testing Approach**:
1. Open memory engine, write data, close → verify no files created
2. Verify engine path exists or is empty (no database directory)
3. Reopen at same path → get fresh empty engine (no persistence)
4. Create two memory engines at different paths → verify isolation
5. Large transaction in memory mode → no spill files created
6. Close/drop engine → all memory released

**Critical Details**:
- ✅ Memory mode only (not all_storage_modes_new())
- ✅ Use `memory_opts()` function
- ✅ StorageMode::Memory only
- ✅ Verify directory is clean or doesn't exist after close
- ✅ No WAL files, no SST files, no manifest files
- ✅ Multiple instances don't share data
- ✅ No persistence across restarts

---

**File Location**: `tests/memory_mode_isolation.rs`
**Test Count**: 6 tests
**Storage Modes**: MEMORY ONLY
**Pattern**: `let opts = memory_opts(); open_with_mode(opts, StorageMode::Memory)`
**Status**: 🚧 0/6 not started

---

## Purpose

Test that memory mode operates as a purely in-memory engine with zero filesystem artifacts. Validates memory mode isolation: data lives in RAM only, no persistence, no side effects on disk.

---

## Tests

1. **should_not_create_filesystem_artifacts_when_memory_mode**
   - Open engine with memory mode, write data, close engine
   - Verify database directory doesn't exist OR is completely empty
   - Confirm no .db, WAL, SST, manifest, or lock files created

2. **should_not_persist_data_across_restart_given_memory_mode_when_reopening**
   - Open memory engine at path, write key "test"→"value", close
   - Reopen memory engine at SAME path
   - Verify key doesn't exist (data not persisted)

3. **should_isolate_data_given_multiple_memory_engines_when_separate_instances**
   - Create engine1 with memory mode, write key→"engine1_value"
   - Create engine2 with memory mode (different path), write same key→"engine2_value"
   - Verify each engine sees only its own value
   - Confirm engines don't share data

4. **should_not_create_wal_files_given_memory_mode_when_writing**
   - Open memory engine, perform 100 writes, close
   - Verify NO WAL files created in database directory
   - (WAL files typically have .wal or .log extensions)

5. **should_not_create_sst_files_given_memory_mode_when_flushing**
   - Open memory engine, write data, trigger flush
   - Verify NO SST files created (would be .sst or .ldb)
   - Confirm compaction doesn't create disk files

6. **should_not_create_spill_files_given_memory_mode_when_large_transaction**
   - Open memory engine, create transaction with large data (>1GB if possible)
   - Even if transaction must spill, verify NO spill files created on disk
   - Data should stay in memory (may exhaust memory, but not create files)

---

## Key APIs

- `memory_opts()` → OpenOptions configured for memory mode
- `open_with_mode(opts, StorageMode::Memory)` → Open memory-only engine
- `engine.put(cf, key, value)` → Write to memory
- `engine.delete(cf, key)` → Delete from memory
- `engine.flush()` → Flush (should be no-op in memory mode)
- `std::fs::read_dir()` → Verify directory empty

---

## Implementation Notes

✅ Tests run on MEMORY mode only (not all_storage_modes_new())
✅ Use `memory_opts()` and StorageMode::Memory exclusively
✅ Verify filesystem is clean: either dir doesn't exist OR contains no database files
✅ Database files to check for: .wal, .log, .sst, .ldb, MANIFEST, LOCK, etc.
✅ Multiple engine instances should have separate memory spaces
✅ Large transactions should not leak to disk (may fail if OOM, but no files)

---

## Test Pattern Example

```rust
#[test]
fn should_not_create_filesystem_artifacts_when_memory_mode() {
    // Arrange
    let opts = memory_opts();
    let path = opts.path.clone();
    
    {
        // Act: Open, write, close
        let engine = open_with_mode(opts, StorageMode::Memory);
        let cf = engine.default_column_family();
        engine.put(cf, b"test_key", b"test_value").expect("put");
        // Engine dropped here
    }
    
    // Assert: No files created
    if path.exists() {
        let entries = std::fs::read_dir(&path)
            .expect("read_dir")
            .collect::<Result<Vec<_>, _>>()
            .expect("entries");
        assert!(
            entries.is_empty(),
            "memory mode created files: {:?}",
            entries.iter().map(|e| e.path()).collect::<Vec<_>>()
        );
    }
}
```

---

## Status

**Current**: 🚧 0/6 not started (spec ready)
**Implementation**: Pending Phase 2

---

## References
- See INTEGRATION_TESTS_FINAL.md for memory mode patterns
- Test utilities: `memory_opts()`, `open_with_mode()` in testkit
- Verifying clean filesystem: `std::fs::read_dir()`, `std::fs::metadata()`

