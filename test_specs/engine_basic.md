# engine_basic.rs - Spec Card

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

**Create a test file that validates fundamental KV store operations (get, put, delete) with storage-mode invariant semantics.**

**Key Requirements**:
- All 8 tests parametrized across all storage modes (Memory, LocalDisk, CloudBacked)
- Pattern: `for_each_storage_mode(&all_storage_modes_new(), |mode, opts| { ... })`
- These are the most basic tests ensuring core functionality works identically across all backends
- Each test focuses on ONE basic operation, not combinations
- Verify that put/get/delete work in all modes with identical semantics

**Testing Approach**:
1. Test basic put → get (write then read)
2. Test get on nonexistent key → returns None
3. Test overwrite (put same key twice, verify latest)
4. Test edge cases: empty values, binary data (non-UTF8)
5. Test delete → get (delete then verify returns None)
6. Test delete on nonexistent key (should succeed, not error)
7. Test that memory mode doesn't leak filesystem artifacts

**Critical Details**:
- ✅ Use all_storage_modes_new() (logic/semantics tests, not persistence)
- ✅ No WAL/recovery/crash testing needed
- ✅ Focus on happy path + key edge cases
- ✅ Each test should be simple and self-contained (<10 lines per test)
- ✅ Verify consistency across modes with mode parameter in assertions
- ✅ Last test specifically checks that memory mode doesn't create files

---

**File Location**: `tests/engine_basic.rs`
**Test Count**: 9 tests
**Storage Modes**: ALL (Memory, LocalDisk, CloudBacked)
**Pattern**: `for_each_storage_mode(&all_storage_modes_new(), |mode, opts| { ... })`
**Status**: 🚧 8/9 passing (1 new test pending)

---

## Purpose
Test fundamental KV store operations (get, put, delete) with storage-mode invariant semantics. These are the most basic tests ensuring core functionality works across all backends.

---

## Tests

1. **should_get_value_given_existing_key_when_put**
   - Write a key, then read it back, verify value matches

2. **should_return_none_given_nonexistent_key_when_get**
   - Try to read a key that was never written, verify returns None

3. **should_overwrite_value_given_existing_key_when_put**
   - Write key→value1, then overwrite with value2, verify latest value returned

4. **should_handle_empty_value_when_put**
   - Write empty value (0 bytes), verify it's stored and retrievable

5. **should_handle_binary_data_when_put**
   - Write binary data (non-UTF8, special bytes), verify round-trip

6. **should_return_none_given_deleted_key_when_get**
   - Write key, delete it, verify get returns None

7. **should_succeed_given_nonexistent_key_when_delete**
   - Delete a key that never existed, verify no error

8. **should_handle_many_operations_when_sequential**
   - Performs 100 sequential put/get operations in a loop, verifies all values correct

9. **should_not_create_filesystem_artifacts_when_memory_mode**
   - In memory mode, verify no database files created on disk after engine close
   - Path should not exist or be completely empty (no .wal, .sst, MANIFEST, etc.)

---

## Key APIs
- `engine.put(cf, key, value)` → Result
- `engine.get(cf, key)` → Result<Option<Bytes>>
- `engine.delete(cf, key)` → Result
- `engine.default_column_family()` → ColumnFamily

---

## Implementation Notes

✅ All tests use `all_storage_modes_new()` (mode-invariant logic)
✅ Each test focused on single operation (no combined behaviors)
✅ Use `expect()` for operations that should succeed
✅ Assertion messages include mode for debugging
✅ Last test (no filesystem artifacts) verifies memory mode isolation

---

## Test Pattern Example

```rust
#[test]
fn should_get_value_given_existing_key_when_put() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        // Act
        engine.put(cf, b"key", b"value").expect("put");

        // Assert
        let got = engine.get(cf, b"key").expect("get");
        assert_eq!(
            got,
            Some(Bytes::from_static(b"value")),
            "unexpected value in mode: {}",
            mode
        );
    });
}
```

---

## Status

**Current**: ✅ 8/8 passing
**Notes**: Foundation tests, all basic operations working

---

## References
- See INTEGRATION_TESTS_FINAL.md lines 253-275 for engine_basic spec
- MidgeEngine public API in `src/engine/mod.rs`
