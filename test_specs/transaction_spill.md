# transaction_spill.rs - Spec Card

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

**Create a test file that validates large transaction spill behavior and memory management.**

**Key Requirements**:
- 12 tests parametrized across durable modes (LocalDisk, CloudBacked)
- 1 test for memory-only (no spill files created)
- Pattern: 
  - Tests 1-12: `for_each_storage_mode(&durable_storage_modes(), |mode, opts| { ... })`
  - Test 13: `let opts = memory_opts();` (no loop)
- Spill semantics: transactions exceeding memory limit write to temporary disk files
- Commit/rollback: cleanup spill files appropriately
- Data integrity: spill doesn't corrupt values or order
- Concurrent spills: multiple transactions spilling in parallel
- Foreground performance: spills don't starve foreground writes
- Recovery: spilled data recovers correctly
- Memory pressure: handle tiny memory limits gracefully
- Mixed value sizes: various value sizes handled correctly

**Testing Approach**:
1. Small memory limit → force spill, write 100+ keys → all committed
2. Multiple spills → verify spill files created
3. Data integrity → scan large transaction, verify order and values
4. Key ordering → spilled transaction maintains order
5. Rollback → drop without commit, spill files removed
6. Cleanup after rollback → verify no disk artifacts
7. Restart before commit → spill rolled back
8. Restart after commit → spill recovered
9. Concurrent spills → multiple transactions spilling together
10. Foreground writes → doesn't block background spill
11. Tiny memory limit → extreme stress, handle gracefully
12. Mixed value sizes → small, medium, large values mixed
13. Memory mode → no spill files created (test 13)

---

**File Location**: `tests/transaction_spill.rs`
**Test Count**: 13 tests
**Storage Modes**: FS + Cloud (12 tests), Memory (1 test)
**Pattern**: Mostly `for_each_storage_mode(&durable_storage_modes(), |mode, opts| { ... })`
**Status**: 📋 Not yet created (spec ready)

---

## Purpose

Test transaction spill files: when transaction data exceeds memory limit, overflow to disk. Spill enables large transactions without unbounded memory growth.

---

## Tests

1. **should_commit_large_transaction_given_many_writes_exceeding_memory_limit**
   - Write 100+ keys with small memory limit → all committed

2. **should_handle_very_large_transaction_given_multiple_spills_when_persisted**
   - Multiple spill files created and managed correctly

3. **should_preserve_data_integrity_given_large_transaction_with_specific_values**
   - Large transaction with specific values → verify exact match

4. **should_preserve_key_order_given_large_transaction_when_iterating**
   - Large transaction → scan shows keys in order

5. **should_rollback_spilled_transaction_given_drop_without_commit**
   - Drop without commit → no data in DB

6. **should_cleanup_spill_files_given_transaction_rollback_when_finalizing**
   - Rollback → spill files removed from disk

7. **should_rollback_uncommitted_spill_given_restart_before_commit**
   - Restart before commit → spill rolled back, not recovered

8. **should_recover_committed_spill_given_restart_after_commit**
   - Restart after commit → spilled data recovered

9. **should_not_starve_foreground_writes_given_background_spill_activity**
   - Concurrent foreground writes not blocked by spill

10. **should_handle_concurrent_large_transactions_given_memory_pressure**
    - Multiple transactions spilling concurrently

11. **should_handle_transaction_with_tiny_memory_limit_given_forced_spill**
    - Extreme memory pressure (1KB limit), handle gracefully

12. **should_handle_mixed_value_sizes_in_spilled_transaction_when_committed**
    - Mix of small, medium, large values handled correctly

13. **should_not_create_disk_artifacts_given_large_transaction_when_memory_mode**
    - Memory mode with large transaction → no spill files on disk

---

## Key APIs

- `engine.transaction()` → Transaction
- `tx.put(cf, key, value)` → Result (may trigger spill if memory exceeded)
- `tx.commit()` → Result (flushes spill)
- Drop transaction (cleanup on drop)
- OpenOptions memory_budget setting (for forcing spill)

---

## Implementation Notes

✅ Tests 1-12: Use `durable_storage_modes()` (FS + Cloud with spill)
✅ Test 13: Use `memory_opts()` with StorageMode::Memory (no spill)
✅ Small memory budget forces spill (set to 1MB for 100+ keys)
✅ Spill files are temporary, stored in temp directory near DB
✅ Spill files cleaned up after commit or rollback
✅ Spill doesn't block foreground writes (concurrent safety)
✅ Data integrity through spill: values unchanged, order preserved
✅ Concurrent spills handled correctly (parallel transactions)
✅ Restart before commit: spill cleaned, transaction rolled back
✅ Restart after commit: spill data recovered from SST/WAL
✅ Memory mode never creates spill files (enforced)
✅ Extreme memory limits handled gracefully (error or proceed with spill)

---

## Test Pattern Example - Forced Spill

```rust
#[test]
fn should_commit_large_transaction_given_many_writes_exceeding_memory_limit() {
    for_each_storage_mode(&durable_storage_modes(), |mode, opts| {
        // Arrange: Force spill with tiny memory budget
        let mut opts = opts;
        opts = opts.memory_budget(1024 * 1024); // 1MB - force spill on 100 keys
        
        // Act
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();
        
        let tx = engine.transaction().expect("txn");
        for i in 0..1000 {
            let key = format!("key{:04}", i);
            let value = format!("value_{:04}", i);
            tx.put(cf, key.as_bytes(), value.as_bytes()).expect("put");
        }
        tx.commit().expect("commit");
        
        // Assert: All committed despite spill
        for i in 0..1000 {
            let key = format!("key{:04}", i);
            let expected = format!("value_{:04}", i);
            let got = engine.get(cf, key.as_bytes()).expect("get");
            assert_eq!(got.map(|b| String::from_utf8_lossy(&b).to_string()),
                      Some(expected),
                      "mismatch for {} in mode: {}", key, mode);
        }
    });
}
```

---

## Test Pattern Example - Rollback Cleanup

```rust
#[test]
fn should_cleanup_spill_files_given_transaction_rollback_when_finalizing() {
    for_each_storage_mode(&durable_storage_modes(), |mode, opts| {
        // Arrange: Setup to track spill directory
        let mut opts = opts;
        opts = opts.memory_budget(1024 * 1024); // Force spill
        let spill_dir = opts.path.join("spill"); // Typical spill location
        
        // Act: Create transaction with spill, then rollback
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();
            
            let tx = engine.transaction().expect("txn");
            for i in 0..1000 {
                let key = format!("key{:04}", i);
                tx.put(cf, key.as_bytes(), b"value").expect("put");
            }
            // Drop without commit = rollback
        }
        
        // Assert: Spill files cleaned up
        if spill_dir.exists() {
            let entries = std::fs::read_dir(&spill_dir)
                .expect("read_dir")
                .collect::<Result<Vec<_>, _>>()
                .expect("entries");
            assert!(entries.is_empty(), "spill files not cleaned in mode: {}", mode);
        }
    });
}
```

---

## Test Pattern Example - Memory Mode No Spill

```rust
#[test]
fn should_not_create_disk_artifacts_given_large_transaction_when_memory_mode() {
    // Memory mode only - no loop
    let opts = memory_opts();
    let path = opts.path.clone();
    let spill_dir = path.join("spill");
    
    // Act
    let engine = open_with_mode(opts, StorageMode::Memory);
    let cf = engine.default_column_family();
    
    let tx = engine.transaction().expect("txn");
    for i in 0..10000 {
        let key = format!("key{:05}", i);
        tx.put(cf, key.as_bytes(), b"v").expect("put");
    }
    tx.commit().expect("commit");
    // engine dropped
    
    // Assert: No spill files created on disk
    if spill_dir.exists() {
        let entries = std::fs::read_dir(&spill_dir)
            .expect("read_dir")
            .collect::<Result<Vec<_>, _>>()
            .expect("entries");
        assert!(entries.is_empty(), "memory mode created spill artifacts");
    }
}
```

---

## Status

**Current**: 📋 0/13 not started (spec ready for implementation)
**Notes**: Requires spill API and memory budget enforcement in transaction runtime

---

## References
- See INTEGRATION_TESTS_FINAL.md lines ~1220 for full transaction_spill spec
- Spill implementation in `src/engine/` or `src/runtime/`
- Temporary file handling for spill files
- Memory budget enforcement in transaction builder
