# sst_reader.rs - Spec Card

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

**Create a test file that validates SST (Sorted String Table) read operations and format correctness.**

**Key Requirements**:
- All 7 tests parametrized across durable storage modes (LocalDisk, CloudBacked)
- Pattern: `for_each_storage_mode(&durable_storage_modes(), |mode, opts| { ... })`
- Storage Modes: FS and Cloud only (SSTs are persisted storage format)
- SST format: binary format with metadata, blocks, index, footer
- Read correctness: keys and values read correctly
- Key ordering: SST keys strictly ordered
- Binary compatibility: SST format stable across versions
- Index lookup: index correctly locates blocks
- Partial reads: reading subset of keys works correctly

**Testing Approach**:
1. Create SST with known values → read directly → verify correctness
2. Scan SST → verify keys in order
3. Seek to key → verify correct position
4. Verify index structure
5. Read with compression
6. Handle empty SST
7. Multi-block SST

---

**File Location**: `tests/sst_reader.rs`
**Test Count**: 7 tests
**Storage Modes**: FS + Cloud ONLY
**Pattern**: `for_each_storage_mode(&durable_storage_modes(), |mode, opts| { ... })`
**Status**: 📋 Not yet created (Phase 5 - SST layer)

---

## Purpose

Test SST read operations: keys and values read correctly, index lookup works, binary format is stable. SSTs are the persistent storage format for sorted key-value data.

---

## Tests

1. **should_read_values_correctly_given_written_sst_when_reading**
   - Write SST with known values, read directly, verify

2. **should_maintain_key_order_given_sst_when_scanning**
   - SST keys strictly sorted, scan verifies order

3. **should_locate_keys_via_index_given_sst_with_index_when_seeking**
   - Index correctly locates blocks containing keys

4. **should_handle_partial_reads_given_subset_of_keys_when_iterating**
   - Reading subset of keys works correctly

5. **should_support_compressed_blocks_given_compression_when_reading**
   - Read compressed SST blocks

6. **should_handle_empty_sst_given_zero_entries_when_reading**
   - Empty SST handled gracefully

7. **should_read_multiblocksst_given_large_sst_when_scanning**
   - Multi-block SST (data spanning multiple blocks) read correctly

---

## Key APIs

- SST file format (binary, implementation-dependent)
- SST reader (internal, not public API for now)
- Block iteration
- Index lookup

---

## Implementation Notes

✅ Uses `durable_storage_modes()` (FS + Cloud)
✅ SST format is binary, not human-readable
✅ Index speeds up key lookup
✅ Compression reduces storage footprint
✅ Tests verify internal format correctness

---

## Status

**Current**: 📋 Not yet created (Phase 5 priority)
**Notes**: SST layer foundation tests

---

## References
- See INTEGRATION_TESTS_FINAL.md lines ~1350 for full SST reader spec
- SST implementation in `src/sst/`
