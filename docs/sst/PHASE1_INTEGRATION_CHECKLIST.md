# Phase 1 Integration Checklist

**Objective:** Wire BlockBloom into the SST writer and reader to complete Phase 1

**Current Status:** Core BlockBloom type fully implemented and tested (40/40 tests passing)

## Remaining Phase 1 Tasks

### Task 1: Update Writer to Build Per-Block Blooms ⏳

**File:** `src/sst/fs/writer.rs` or new `index_writer.rs`

**Subtasks:**
- [ ] Find or create writer code that builds index/metadata
- [ ] During SST write, create BlockBloom for each data block
- [ ] Add keys to bloom as block is written
- [ ] Write bloom to SST file (right after data block)
- [ ] Store bloom offset in BlockIndexEntry
- [ ] Set `SstFooter.has_per_block_blooms = true` in footer
- [ ] Write tests first (TDD): test_should_build_per_block_blooms_during_write

**Expected Changes:**
- ~50-100 lines: bloom creation and offset tracking
- ~20-30 lines: tests (create test SST, verify blooms written)

---

### Task 2: Update Reader to Load and Query Per-Block Blooms ⏳

**File:** `src/sst/fs/reader.rs` or new `index_reader.rs`

**Subtasks:**
- [ ] Find or create reader code that loads index/metadata
- [ ] On SST open, check `has_per_block_blooms` flag
- [ ] Lazy-load per-block blooms when first queried
- [ ] Integrate bloom query into read path (before block I/O)
- [ ] Handle old SST format (no blooms) gracefully
- [ ] Write tests first (TDD): test_should_load_and_query_per_block_blooms

**Expected Changes:**
- ~80-120 lines: bloom loading, read-path integration
- ~30-50 lines: tests (open old/new SSTs, verify queries)

---

### Task 3: Integration & Backward Compatibility Tests ⏳

**File:** `tests/per_block_bloom_integration.rs` (new or extend existing)

**Test Cases:**
- [ ] Create new SST with per-block blooms, read back, verify blooms present
- [ ] Read old SST without per-block blooms, verify queries work (conservative default)
- [ ] Bloom queries return correct results (no false negatives)
- [ ] Negative lookups skip I/O when bloom says "definitely absent"
- [ ] Format version flag correctly set in both cases

**Expected:**
- ~15-20 tests, ~300-400 lines

---

### Task 4: Benchmarking ⏳

**File:** `benches/tier1_hotpath/per_block_bloom.rs` (new)

**Benchmarks:**
- [ ] Bloom creation cost per block (nanoseconds)
- [ ] Bloom query cost per lookup (nanoseconds)
- [ ] Negative lookup microbench (with/without bloom)
- [ ] L0 read-amp reduction (scanning many SSTs with per-block blooms)
- [ ] SST creation overhead (bloom build cost as % of write time)

**Expected:**
- 4-6 criterion benchmarks
- ~200-300 lines with setup code

---

## Implementation Strategy

**Phase 1.1: Writer Wiring** (Prerequisite for reader)
1. Locate writer code (search for "index" or "footer" in fs/ directory)
2. Understand current metadata flow
3. TDD: Write test that expects writer to create blooms
4. Implement bloom creation + offset storage
5. All tests pass ✅

**Phase 1.2: Reader Wiring** (Builds on Phase 1.1)
1. Locate reader code
2. Understand current read path
3. TDD: Write test that expects blooms to be loaded + queried
4. Implement lazy-loading + read-path integration
5. All tests pass ✅

**Phase 1.3: Integration Testing**
1. Create test SSTs with both old and new formats
2. Verify backward compatibility
3. Test negative lookup optimization
4. All tests pass ✅

**Phase 1.4: Benchmarking**
1. Measure bloom creation overhead
2. Measure query latency
3. Measure I/O reduction in negative lookup case
4. Document results

---

## Testing Checklist

**Before Proceeding to Phase 2:**
- [ ] cargo build --lib (no warnings)
- [ ] cargo test (all tests pass)
- [ ] cargo test --test per_block_bloom_tests (19 tests pass)
- [ ] cargo test --test sst_invariants (10 tests pass)
- [ ] cargo test --test per_block_bloom_integration (new tests pass)
- [ ] cargo bench per_block_bloom (runs without panics)
- [ ] cargo clippy --all-targets (no warnings)

---

## Key Integration Points

### Writer Path
```rust
// Pseudo-code for writer
for each data_block in blocks {
    // Write data
    let offset = writer.write_block(&data_block);
    
    // NEW: Build bloom
    let mut bloom = BlockBloom::new(BLOOM_CAPACITY);
    for key in &data_block.keys {
        bloom.add(key);
    }
    
    // NEW: Write bloom
    let bloom_offset = writer.write_bloom(&bloom);
    
    // Store metadata
    let entry = BlockIndexEntry {
        min_key, max_key, block_offset: offset, block_len: len,
        bloom_offset: Some(bloom_offset),  // NEW
    };
}

// NEW: Set format flag
footer.has_per_block_blooms = true;
```

### Reader Path
```rust
// Pseudo-code for reader (in read_key function)
fn read_key(&mut self, key: &[u8]) -> Option<Value> {
    for block_meta in self.index.find_blocks_for_key(key) {
        // NEW: Check bloom first
        if !block_meta.bloom_maybe_contains(key) {
            continue;  // Skip this block (no false negatives!)
        }
        
        // Load block (possibly from cache)
        let block = self.read_block(&block_meta);
        
        // Binary search
        if let Some(val) = block.search(key) {
            return Some(val);
        }
    }
    None
}
```

---

## Estimated Effort

| Task | Estimate | Status |
|------|----------|--------|
| Writer wiring | 2-3 hours | ⏳ Not started |
| Reader wiring | 2-3 hours | ⏳ Not started |
| Integration tests | 1-2 hours | ⏳ Not started |
| Benchmarking | 1-2 hours | ⏳ Not started |
| **Total** | **6-10 hours** | ⏳ To do |

---

## Success Criteria

✅ **Phase 1 Complete When:**
1. Writer builds per-block blooms during SST creation
2. Reader loads and queries per-block blooms
3. All 40+ tests passing (including new integration tests)
4. Backward compatibility verified (old SSTs still work)
5. Benchmarks show measurable I/O reduction for negative lookups
6. No clippy warnings
7. Build time <30s

---

## References

- Core implementation: `src/sst/block_meta.rs` (BlockBloom, BlockIndexEntry, SstFooter)
- Test suite: `tests/per_block_bloom_tests.rs` (19 integration tests)
- Baseline: `tests/sst_invariants.rs` (10 invariant tests)
- Format spec: `docs/sst/INDEX_SPEC.md` (13 locked invariants)
- Progress: `docs/sst/PHASE1_PROGRESS.md`

---

**Created after:** Phase 1 core implementation complete (40/40 tests passing)
**Ready for:** Phase 1.1 - Writer integration
