# Phase 1: Per-Block Bloom Filters — Progress Report

**Status:** Core implementation complete (19/19 integration tests + 11/11 inline tests passing)

## Summary

Phase 1 core implementation is feature-complete and fully tested. All data structures, serialization, and integration points with BlockMeta are in place. The next step is wiring these structures into the actual SST writer/reader.

## What's Done ✅

### 1. Core BlockBloom Type (`src/sst/block_meta.rs`)

```rust
pub struct BlockBloom {
    bits: Vec<u8>,
    capacity_bytes: usize,
}

impl BlockBloom {
    pub fn new(capacity_bytes: usize) -> Self
    pub fn add(&mut self, key: &[u8])
    pub fn maybe_contains(&self, key: &[u8]) -> bool
    pub fn encode(&self) -> Bytes
    pub fn decode(data: &[u8]) -> MidgeResult<Self>
    fn hash(key: &[u8]) -> u64  // Simple multiplicative hash
}
```

**Invariants:**
- No false negatives: if `add(key)` was called, `maybe_contains(key)` returns `true`
- False positive rate acceptable: ~8-10% with current hash function and typical bloom sizes
- Encode/decode round-trip preserves all state
- Simple hash function suitable for Phase 1 (can be upgraded to double-hashing in optimization phase)

### 2. Format Support (`src/sst/block_meta.rs`)

```rust
pub struct BlockIndexEntry {
    pub min_key: Bytes,
    pub max_key: Bytes,
    pub block_offset: u64,
    pub block_len: u32,
    pub bloom_offset: Option<u64>,  // NEW: Offset to per-block bloom
}

pub struct SstFooter {
    pub metaindex_handle: BlockHandle,
    pub index_handle: BlockHandle,
    pub has_per_block_blooms: bool,  // NEW: Format version flag
}
```

**Benefits:**
- Backward compatible: old SSTs (has_per_block_blooms = false) still readable
- Forward compatible: new code can handle both formats
- Bloom location stored in BlockIndexEntry for efficient loading

### 3. BlockMeta Integration (`src/sst/block_meta.rs`)

```rust
pub struct BlockMeta {
    // ... existing fields ...
    pub bloom_offset: Option<u64>,
    bloom: Option<BlockBloom>,  // Cached bloom
}

impl BlockMeta {
    pub fn with_bloom(mut self, bloom: BlockBloom) -> Self
    pub fn bloom_maybe_contains(&self, key: &[u8]) -> bool
    pub fn has_loaded_bloom(&self) -> bool
    pub fn bloom(&self) -> Option<&BlockBloom>
}
```

**Design:**
- `bloom_offset` stored for lazy-loading during read path
- `bloom` field is Option for memory efficiency (only load when needed)
- Conservative default: if no bloom loaded, assume key might be present (true)
- Integrates seamlessly with existing read path

## Test Coverage ✅

### Integration Tests: `tests/per_block_bloom_tests.rs` (19 tests)

**Test Categories:**
1. **Creation & Capacity:** BlockBloom construction with various sizes
2. **Add/Contains Operations:** Add keys, query for presence/absence
3. **Encoding/Decoding:** Serialization round-trip, format detection
4. **False Positive Rate:** <10% fp rate with typical configurations
5. **Format Versioning:** SstFooter flag, old format still readable
6. **Batch Operations:** Add multiple keys efficiently
7. **Edge Cases:** Empty blooms, large keys, small bloom sizes
8. **BlockMeta Integration:** Query bloom through BlockMeta

**Coverage:**
```
test result: ok. 19 passed; 0 failed; 0 ignored; 0 measured
```

### Inline Tests: `src/sst/block_meta.rs` (11 tests)

**Test Categories:**
1. **No False Negatives:** Core invariant—added keys must be found
2. **Query Through BlockMeta:** Integration point for read path
3. **Conservative Default:** Without bloom, assume key might exist
4. **Encode/Decode Preservation:** State preserved through serialization
5. **Bloom Offset Tracking:** Metadata properly stored in BlockMeta

**Coverage:**
```
test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured
```

### Phase 0 Baseline Tests: `tests/sst_invariants.rs` (10 tests)

All baseline tests still passing—foundation is stable.

## Performance Expectations

| Scenario | Impact |
|----------|--------|
| Negative lookups (key not in SST) | ~80-90% I/O saved (bloom hit) |
| SST creation overhead | ~2-5% (bloom building cost) |
| Memory per 10MB SST | ~40KB (4KB bloom per 1MB data) |
| Read-path latency | No impact (bloom query is negligible) |

## Next Steps: Phase 1 Integration (Writer/Reader Wiring)

### Task 1: Update `src/sst/fs/writer.rs` or `index_writer.rs`
- Build per-block bloom during SST write (one bloom per data block)
- Store bloom offset in BlockIndexEntry
- Set `SstFooter.has_per_block_blooms = true`
- TDD: Write tests for writer integration first

### Task 2: Update `src/sst/fs/reader.rs` or `index_reader.rs`
- Load per-block blooms on SST open (lazy-load from index)
- Query bloom before block I/O in read path
- Maintain backward compatibility with old SSTs
- TDD: Write tests for reader integration first

### Task 3: Integration Testing
- Old SST format still readable (backward compat)
- New SSTs created with per-block blooms
- Format versioning detected correctly
- Bloom queries work in actual read path

### Task 4: Benchmarking
- Negative lookup microbench (with/without block blooms)
- L0 read-amp reduction (many SSTs with per-block blooms)
- SST creation overhead (bloom build cost)

## Architecture Diagram

```
┌─────────────────────────────────────┐
│ Read Path                            │
├─────────────────────────────────────┤
│ 1. Load BlockMeta (includes bloom_offset) │
│ 2. IF bloom not loaded:              │
│    - Load bloom from SST file        │
│    - Set BlockMeta.bloom             │
│ 3. Query bloom: bloom_maybe_contains │
│ 4. IF bloom says "maybe present":    │
│    - Read block from cache or disk   │
│    - Binary search block             │
│ 5. IF bloom says "definitely absent":│
│    - Skip block I/O (FAST PATH!)     │
└─────────────────────────────────────┘

┌─────────────────────────────────────┐
│ Write Path                           │
├─────────────────────────────────────┤
│ 1. Write data blocks                │
│ 2. Build per-block bloom for each   │
│ 3. Write blooms to SST file         │
│ 4. Store bloom_offset in index      │
│ 5. Set has_per_block_blooms flag    │
└─────────────────────────────────────┘
```

## Code Quality

- ✅ All tests passing (40/40 total: 19 integration + 11 inline + 10 baseline)
- ✅ No clippy warnings
- ✅ Compile time: ~22s (includes full test build)
- ✅ No unsafe code in BlockBloom
- ✅ Comprehensive error handling (MidgeResult/MidgeError)

## Key Design Decisions

1. **Simple Hash Function:** Single hash function (multiplicative) for Phase 1
   - Reason: Simpler code, easier to verify correctness
   - Trade-off: ~8-10% false positive rate (acceptable)
   - Future: Can upgrade to double-hashing if needed

2. **Lazy-Loading:** Bloom loaded on-demand in read path
   - Reason: Not all blocks are read; save I/O and memory
   - Trade-off: First read to block pays bloom-load cost
   - Benefit: Keeps BlockMeta small in memory

3. **Optional Bloom in BlockMeta:** `bloom: Option<BlockBloom>`
   - Reason: Memory efficiency; only cache if actively used
   - Trade-off: Option lookup overhead (negligible)
   - Benefit: No bloom bloat for rarely-read blocks

4. **Format Flag in SstFooter:** `has_per_block_blooms: bool`
   - Reason: Clean backward compatibility
   - Trade-off: Adds 1 byte to footer
   - Benefit: Old readers ignore blooms; new readers see them

## Dependencies

- `bytes::Bytes` — Key storage
- `crate::sst::format::BlockHandle` — Physical location
- `crate::error::{MidgeError, MidgeResult}` — Error handling
- No external crates added

## Files Modified

- `src/sst/block_meta.rs` — Core implementation (11 inline tests)
- `tests/per_block_bloom_tests.rs` — Integration test suite (19 tests) [NEW]
- `tests/sst_invariants.rs` — Baseline tests (unchanged, all passing)

## Session History

| Step | Task | Status |
|------|------|--------|
| 1 | Design SST indexing architecture | ✅ Complete (docs/sst/SST_INDEX_DESIGN.md) |
| 2 | Create 6-phase roadmap | ✅ Complete (docs/sst/SST_INDEX_TODO.md) |
| 3 | Phase 0: Lock baseline invariants | ✅ Complete (10 tests, INDEX_SPEC.md) |
| 4 | Phase 1: Core BlockBloom + format | ✅ Complete (this report) |
| 5 | Phase 1: Writer/Reader wiring | ⏳ Next |
| 6 | Phase 2: Fence pointers integration | ⏳ Future |
| 7 | Phase 3-5: Sparse index, tombstones, zone maps | ⏳ Future |

---

**Last Updated:** After Phase 1 core implementation complete (40/40 tests passing)
**Next Action:** Wire BlockBloom into index_writer.rs and index_reader.rs using TDD
