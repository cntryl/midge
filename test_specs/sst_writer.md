# sst_writer.rs - Spec Card

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

**Create a test file that validates SST (Sorted String Table) write operations and format generation.**

**Key Requirements**:
- All 14 tests parametrized across durable storage modes (LocalDisk, CloudBacked)
- Pattern: `for_each_storage_mode(&durable_storage_modes(), |mode, opts| { ... })`
- Storage Modes: FS and Cloud only
- SST writing: keys written in sorted order, values stored
- Block boundaries: blocks created at configured size limits
- Compression: data optionally compressed
- Metadata: footer, index, bloom filters written
- Format stability: SST format deterministic (same input → same output)
- Large keys/values: handle edge cases

**Testing Approach**:
1. Write SST with sorted keys → verify file format
2. Compression enabled → verify compression applied
3. Block boundaries → verify blocks sized correctly
4. Metadata integrity → footer, index present and valid
5. Large keys → handle multi-KB keys
6. Large values → handle multi-MB values
7. Edge cases: empty, single key, exactly block boundary

---

**File Location**: `tests/sst_writer.rs`
**Test Count**: 14 tests
**Storage Modes**: FS + Cloud ONLY
**Pattern**: `for_each_storage_mode(&durable_storage_modes(), |mode, opts| { ... })`
**Status**: 📋 Not yet created (Phase 5 - SST layer)

---

## Purpose

Test SST write operations: keys written in sorted order, blocks created correctly, compression applied, metadata written. SST writer is the core of data persistence.

---

## Tests

1. **should_write_sorted_sst_given_sorted_input_when_writing**
   - Write SST with sorted keys, verify file format

2. **should_create_blocks_at_size_boundaries_given_large_data_when_writing**
   - Blocks created at configured size limits

3. **should_apply_compression_given_enabled_when_writing**
   - Compression applied to blocks

4. **should_write_index_structure_given_multi_block_sst_when_writing**
   - Index written for block location

5. **should_write_footer_metadata_given_sst_when_finalizing**
   - Footer with metadata written

6. **should_handle_large_keys_given_multi_kb_keys_when_writing**
   - Large keys (>10KB) handled

7. **should_handle_large_values_given_multi_mb_values_when_writing**
   - Large values handled

8. **should_produce_deterministic_output_given_same_input_when_writing**
   - Same input → same SST output

9. **should_write_empty_sst_given_no_entries_when_writing**
   - Empty SST handled

10. **should_write_single_key_sst_given_one_entry_when_writing**
    - Single-key SST works

11. **should_respect_block_size_configuration_given_custom_block_size_when_writing**
    - Custom block size applied

12. **should_include_bloom_filters_given_enabled_when_writing**
    - Bloom filters included in SST

13. **should_handle_binary_data_given_non_utf8_when_writing**
    - Binary data preserved

14. **should_achieve_expected_compression_ratio_given_highly_compressible_data_when_writing**
    - Compression ratio reasonable

---

## Key APIs

- SST writer (internal API)
- Block builder
- Compression options
- Metadata writer

---

## Implementation Notes

✅ Uses `durable_storage_modes()` (FS + Cloud)
✅ Keys must be pre-sorted before writing
✅ Blocks sized according to configuration
✅ Compression reduces file size
✅ Deterministic output for consistency
✅ Metadata essential for reading

---

## Status

**Current**: 📋 Not yet created (Phase 5 priority)
**Notes**: SST writer foundation tests

---

## References
- See INTEGRATION_TESTS_FINAL.md lines ~1400 for full SST writer spec
- SST implementation in `src/sst/`
