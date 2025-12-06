# Phase 1.1: Writer Integration — Getting Started

## Quick Start

**Objective:** Wire BlockBloom into SST writer to build per-block blooms during SST creation.

**Status:** Ready to start (all Phase 1 core tests passing)

**Estimated Time:** 2-3 hours

## Immediate Next Steps

### Step 1: Locate Writer Code (15 min)

**Search for:** Files that write SST metadata/index

```powershell
# From repo root
cd d:\repos\cntryl\midge

# Search for index writing or footer writing
rg "index.*write|metaindex|footer" src/sst --type rust -l
rg "BlockHandle|write.*block" src/sst --type rust -l
```

**Look for:**
- File containing SST write path
- Function/struct that builds index or footer
- Where BlockIndexEntry or index metadata is currently written
- Where SstFooter is currently written

**Likely locations:**
- `src/sst/fs/writer.rs` (most likely)
- `src/sst/fs/writer_common.rs`
- `src/sst/api/writer.rs`

### Step 2: Understand Current Writer Flow (30 min)

Once you find the writer file(s):

1. **Read the writer code:**
   - How are data blocks written?
   - Where is metadata/index built?
   - Where is footer written?
   - Current flow: block → offset/length → index entry → footer

2. **Find these key functions/types:**
   - Something like `write_data_block()` or `add_entry()`
   - Something like `build_index()` or `finalize()`
   - Something like `write_footer()`

3. **Create a quick ASCII diagram** of current flow

### Step 3: Write First Test (TDD) (30 min)

**Location:** Create `tests/sst_writer_bloom_tests.rs`

**First test:**
```rust
#[test]
fn should_build_per_block_blooms_during_sst_write() {
    // Arrange: Create test data with known keys
    let mut records = vec![
        ("key1", "value1"),
        ("key2", "value2"),
        ("key3", "value3"),
    ];
    
    // Act: Write SST with bloom enabled
    let sst_data = write_test_sst_with_blooms(&records);
    
    // Assert: Verify blooms were written
    assert_eq!(sst_data.footer.has_per_block_blooms, true);
    for entry in &sst_data.index_entries {
        assert!(entry.bloom_offset.is_some(), "Bloom offset must be set");
    }
    
    // Assert: Load and verify blooms work
    let blooms = load_blooms_from_sst(&sst_data);
    for (i, bloom) in blooms.iter().enumerate() {
        for (key, _val) in &records {
            assert!(bloom.maybe_contains(key.as_bytes()));
        }
    }
}
```

### Step 4: Implement Writer Changes (1-1.5 hours)

**Modifications needed:**

1. **In data block writing:**
   ```rust
   let mut bloom = BlockBloom::new(BLOOM_CAPACITY_BYTES); // e.g., 4096
   
   // As you iterate over records in the block:
   for (key, _val) in &block_records {
       bloom.add(key.as_bytes());
   }
   
   // After writing data block, write bloom:
   let bloom_offset = writer.write_bloom(&bloom)?;
   
   // Store offset in entry:
   entry.bloom_offset = Some(bloom_offset);
   ```

2. **In index entry creation:**
   - Update BlockIndexEntry to include bloom_offset

3. **In footer finalization:**
   ```rust
   footer.has_per_block_blooms = true;  // Enable format flag
   ```

4. **Constants to add:**
   ```rust
   const BLOOM_CAPACITY_BYTES: usize = 4096;  // Tunable, roughly 4KB per data block
   ```

### Step 5: Run Tests & Iterate (30 min)

```powershell
cd d:\repos\cntryl\midge

# Build
cargo build --lib

# Run new writer tests
cargo test --test sst_writer_bloom_tests

# Run all Phase 1 tests to verify no regressions
cargo test --test per_block_bloom_tests
cargo test --lib sst::block_meta
cargo test --test sst_invariants

# Check for clippy warnings
cargo clippy --all-targets
```

**Expected results:**
- New test passes (or fails with clear error message to guide implementation)
- All existing tests still pass
- No clippy warnings

## Resources & References

**Core Types (ready to use):**
- `BlockBloom` — `src/sst/block_meta.rs:12-88`
- `BlockIndexEntry` — `src/sst/block_meta.rs:100-106`
- `SstFooter` — `src/sst/block_meta.rs:109-113`

**Example Code:**
- Encoding bloom: `BlockBloom::encode() -> Bytes`
- Creating bloom: `BlockBloom::new(capacity_bytes)`
- Adding keys: `bloom.add(&key)`

**Test References:**
- Integration tests: `tests/per_block_bloom_tests.rs`
- Inline tests: `src/sst/block_meta.rs` (scroll to bottom)

**Documentation:**
- `docs/sst/PHASE1_PROGRESS.md` — Full progress report
- `docs/sst/PHASE1_INTEGRATION_CHECKLIST.md` — All Phase 1 tasks
- `docs/sst/PHASE1_IMPLEMENTATION_SUMMARY.md` — What's implemented

## Key Points to Remember

1. **TDD First:** Write the test before implementing the feature
2. **Conservative Bloom Capacity:** Start with 4KB per block, can tune later
3. **Lazy-Load Design:** Blooms are stored by offset, loaded on-demand in reader (Phase 1.2)
4. **Backward Compat:** Old SSTs without blooms should still work (flag = false)
5. **No External Crates:** Use only what's already in project (bytes::Bytes, etc.)

## Done Checklist for Phase 1.1

- [ ] Writer code located and understood
- [ ] First test written (TDD)
- [ ] Bloom creation integrated into writer
- [ ] Bloom written to SST file
- [ ] Bloom offset stored in BlockIndexEntry
- [ ] Format flag set (has_per_block_blooms = true)
- [ ] All new tests passing
- [ ] All existing tests still passing (no regressions)
- [ ] No clippy warnings
- [ ] Documentation updated (optional, but recommended)

## Next Phase After 1.1

Once Phase 1.1 is complete:
→ **Phase 1.2: Reader Integration** — Load and query blooms from reader

---

**Ready to start?** Follow Steps 1-5 above.

**Questions?** Check the references section above.

**Blockers?** Ensure all Phase 1 core tests pass before starting: `cargo test --test per_block_bloom_tests`

**Session Context:** Phase 1 core implementation complete, 40/40 tests passing. Ready for writer wiring.
