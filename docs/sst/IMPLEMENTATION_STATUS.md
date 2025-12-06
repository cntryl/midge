# SST Index Implementation Status

**Status**: 🟢 Phase 0 Complete | Phase 2 Foundation In Progress

## Phase 0 ✅ Complete

### What Was Done

1. **INDEX_SPEC.md** (`docs/sst/INDEX_SPEC.md`)
   - Locked the current SST format as an immutable contract
   - Documented file layout, block types, footer format
   - Specified invariants for format, index, data blocks, and bloom filters
   - Defined recovery and consistency guarantees
   - Added versioning strategy for future extensions

2. **BlockMeta & IndexTable** (`src/sst/block_meta.rs`)
   - `BlockMeta`: Struct encapsulating all block-level metadata
     - `min_key`, `max_key` (fence pointers)
     - `handle` (physical location)
     - `has_tombstones`, `tombstone_min`, `tombstone_max`
     - `bloom_offset` (for Phase 1)
     - Helper methods: `contains_key()`, `range_intersects()`, `might_be_fully_covered()`
   - `IndexTable`: Compact in-memory index representation
     - Separates search keys from metadata for cache efficiency
     - Binary search on min-keys
     - Range query support

3. **Baseline Invariant Test Suite** (`tests/sst_invariants.rs`)
   - 10 comprehensive tests covering:
     - BlockMeta creation and fence pointer logic
     - Key containment checks
     - Range intersection detection
     - IndexTable construction and queries
     - Tombstone metadata handling
     - Bloom offset support
     - Empty index edge cases
   - All tests passing ✅

### Deliverables

- ✅ `docs/sst/INDEX_SPEC.md` (locked format specification)
- ✅ `src/sst/block_meta.rs` (BlockMeta + IndexTable)
- ✅ `tests/sst_invariants.rs` (10 passing baseline tests)
- ✅ `src/sst/mod.rs` updated with exports

### Invariants Locked

These invariants are now the locked contract:

1. **Format Invariants**
   - Magic number at EOF: `0xdb4775248b80fb57`
   - Footer: exactly 48 bytes, fixed location
   - CRC32C checksums on all blocks
   - Offset ordering: `metaindex < index < footer`

2. **Index Invariants**
   - Sparse index entries sorted by key
   - Non-overlapping blocks: `index[i].key < index[i+1].key`
   - One index entry per data block
   - All block handles within file bounds

3. **Data Block Invariants**
   - Keys strictly increasing within block
   - Fence pointers: `min_key` (first), `max_key` (last)
   - Non-overlapping: `block[i].max_key < block[i+1].min_key`

4. **Bloom Filter Invariants**
   - Complete key coverage (all keys included)
   - No false negatives
   - False positive rate within design bounds

---

## Phase 1 ⏳ Pending

### Per-Block Bloom Filters (Biggest Win)

**Goal**: Add optional per-block blooms to reduce false positives when many SSTs present

**Design**:
- Keep SST-level bloom for fast file skipping
- Add per-block bloom in metadata region
- `BlockIndexEntry` extended with `bloom_offset`
- Lookup: Check SST bloom → binary search index → check per-block bloom → I/O

**Deliverables**:
- [ ] `BlockIndexEntry` struct with `bloom_offset`
- [ ] Footer versioning (indicator for block blooms)
- [ ] `index_writer.rs` updates (build per-block blooms)
- [ ] `index_reader.rs` updates (load & query per-block blooms)
- [ ] Format upgrade/downgrade tests
- [ ] Benches: negative lookup, L0 read-amp reduction

**Acceptance**: Measurable false-positive reduction, all tests passing

---

## Phase 2 🟡 In Progress

### Tight Fence Pointers + Tombstone Awareness

**Goal**: Thread BlockMeta through read path, enabling fast tombstone coverage checks

**What's Done**:
- ✅ `BlockMeta` struct with tombstone fields
- ✅ `IndexTable` for compact in-memory representation
- ✅ Tests for tombstone logic

**What's Remaining**:
- [ ] Update `SstFile` reader to use `IndexTable`
- [ ] Thread `BlockMeta` into iterators
- [ ] Fast-path in compaction (skip reads if fully covered)
- [ ] Integration tests with compaction

**Deliverables**:
- [ ] `SstFile::index_table()` returns `&IndexTable`
- [ ] Iterator uses `BlockMeta` for range skipping
- [ ] Compaction can skip fully-covered blocks without reading

**Acceptance**: Compaction benches show I/O reduction for tombstone-heavy workloads

---

## Phase 3 ⏳ Pending

### Compact Sparse Index (In-Memory Layout)

**Goal**: Separate search keys from metadata for minimal memory footprint

**Design**:
- `IndexTable` with `Vec<PrefixKey>` for search
- `Vec<BlockMeta>` for metadata
- Binary search on search keys only
- All block info still available for compaction/iteration

**Deliverables**:
- [ ] Prefix-compression for sampled keys
- [ ] Memory footprint benchmarks
- [ ] Regression tests for index stability

**Acceptance**: Memory savings for large SSTs, no lookup latency regression

---

## Phase 4 ⏳ Pending

### Range Tombstone Indexing

**Goal**: Separate tombstones into dedicated blocks for faster compaction decisions

**Design**:
- Tombstone blocks: sorted by start key
- `TombstoneIndex`: keyed by start key
- Fast path: skip reads if range fully covered

**Deliverables**:
- [ ] Tombstone block format
- [ ] `TombstoneIndexEntry` struct
- [ ] SST writer builds tombstone index
- [ ] SST reader loads & queries tombstone index
- [ ] Compaction fast-path

**Acceptance**: Compaction I/O reduction, snapshot isolation preserved

---

## Phase 5 ⏳ Pending

### Zone Maps (Optional, Analytics-Focused)

**Goal**: Optional per-block statistics for analytical workloads

**Design**:
- Separate optional metadata block
- Per-block min/max for time/sequence keys
- Fast skipping in wide range scans

**Deliverables**:
- [ ] Zone map metadata format
- [ ] Feature flag / config gate
- [ ] Build & load zone maps (optional)
- [ ] Use in compaction/iteration

**Acceptance**: Measurable improvement for analytical workloads

---

## Next Immediate Steps (Phase 2 Continuation)

1. **Integrate BlockMeta with SstFile**
   - Modify `src/sst/fs/reader.rs` to build `IndexTable` from sparse index
   - Cache `IndexTable` in `SstFile` struct
   - Update block lookup methods to use `IndexTable`

2. **Thread BlockMeta through iterators**
   - `SstRangeIter` uses fence pointers for block skipping
   - Range scan: skip blocks where `block.max_key < range.start` or `block.min_key > range.end`

3. **Add compaction fast-path**
   - Check `might_be_fully_covered()` before reading block
   - Skip tombstone-covered blocks in L0→L1 compaction

4. **Expand test coverage**
   - Integration tests: SST write → read → compaction
   - Tombstone coverage tests
   - Range scan correctness

---

## Test & Bench Strategy

### Unit Tests (in progress)
- `tests/sst_invariants.rs` — 10 baseline tests ✅
- New: iterator range skipping tests
- New: tombstone coverage tests

### Integration Tests (pending)
- SST write → read → compaction lifecycle
- Range scans with block skipping
- Tombstone edge cases

### Benches (pending)
- `benches/sst_index.rs`
  - Index binary search latency
  - Bloom query (per-SST vs. per-block)
  - Range scan with block skipping
  - Compaction I/O reduction (tombstones)

---

## Completion Criteria

All phases complete when:
- ✅ Phase 0: Baseline locked + invariant tests passing
- ⏳ Phase 1-5: All deliverables complete + tests passing + benches show expected wins
- ✅ No regressions in core KV latency or throughput
- ✅ Code organized in `src/sst/` with clear module boundaries
- ✅ Documentation updated with examples and rationale

---

## Architecture Notes

### Module Organization

```
src/sst/
├── block_meta.rs          (BlockMeta, IndexTable) [Phase 0 ✅]
├── format.rs              (Footer, Block, BlockHandle)
├── sparse_index.rs        (SparseIndex, IndexEntry)
├── fs/
│   └── reader.rs          (SstFile — to be updated Phase 2)
├── block_cache/           (LRU cache for blocks)
├── bloom.rs               (Bloom filter)
└── ...
```

### Key Design Decisions

1. **BlockMeta as the source of truth**: All block-level reasoning (skipping, tombstone coverage, etc.) goes through `BlockMeta`
2. **IndexTable for efficiency**: Separates search keys from metadata; minimal memory overhead
3. **Lazy tombstone indexing**: Tombstone index built on SST creation (Phase 4); optional upfront
4. **Versioned format**: Footer version allows safe extension (per-block blooms, zone maps, etc.)
5. **No pluggable indexes**: Focused on single-level, high-performance design for Midge's LSM

---

## Risk Mitigation

- **Backward compatibility**: All new features gated behind footer version or config flags
- **Testing**: Invariant test suite acts as canary for any breaking changes
- **Incremental**: Each phase can be tested independently before moving to next
- **Benchmarking**: Performance wins validated before integrating each phase

---

## Related Documentation

- `docs/sst/INDEX_SPEC.md` — Format specification (locked)
- `docs/SST_INDEX_DESIGN.md` — Design rationale
- `docs/SST_INDEX_TODO.md` — Detailed task breakdown
- `tests/sst_invariants.rs` — Baseline invariants (executable spec)
