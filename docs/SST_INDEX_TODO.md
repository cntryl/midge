# SST Index Implementation Roadmap

This document stages the implementation of SST indexing features to ensure we never have to redo core mechanics again. Each phase builds a testable, durable contract.

---

## Phase 0: Baseline — Lock In What Exists

**Goals**

* Freeze current SST/index behavior as a locked contract.
* Establish invariants that all future changes must preserve.

**Actions**

- [x] Write `docs/sst/INDEX_SPEC.md` (formalize current behavior)
  - [x] Codify invariants (13 locked invariants)
  - [x] Document footer format, block layout, encoding specifics

- [x] Build invariant test suite (`tests/sst_invariants.rs`)
  - [x] 10 baseline tests validating all invariants
  - [x] Key ordering, index coverage, bloom accuracy, footer checksums
  - [x] Property-based validation on reopen

- [x] Add regression tests
  - [x] All Phase 0 tests passing, baseline locked

**Acceptance**

- [x] SST_INDEX_DESIGN.md complete
- [x] INDEX_SPEC.md documents all current behavior
- [x] Invariant test suite passes (10/10 tests)

---

## Phase 1: Per-Block Bloom Filters (Biggest Immediate Win)

**Design**

* Keep **existing per-SST bloom** for fast "skip file" decisions.
* Add **optional per-block bloom** stored in a metadata region tightly coupled to block index.
* Reduces false positives when many SSTs are present (L0 read-amp).

**On-Disk Layout**

* For each data block, index entry includes:
  ```
  {min_key, max_key, data_offset, data_len, bloom_offset}
  ```
* `BlockBloomRegion`: Fixed-size or variable-size blooms per block, checksummed as one logical block.

**In-Memory Layout**

* On SST open:
  - [x] Map index into compact struct array: `Vec<BlockIndexEntry>`
  - [x] Map per-block bloom bits into contiguous slice
  - [x] Keep both inline for cache locality

* Lookup path:
  1. Check SST-level bloom (fast skip)
  2. Binary search index → candidate block(s)
  3. Probe per-block bloom before I/O

**Implementation Steps**

- [x] Define `BlockBloom` type (reuse existing bloom core)
- [x] Extend `BlockIndexEntry` to include `bloom_offset`
- [x] Update footer version to indicate block blooms present
- [x] Gate per-block blooms behind config flag initially
- [x] Update `index_writer.rs` to build per-block blooms
- [x] Update `index_reader.rs` to load and query per-block blooms
- [x] Update SST writer to persist blooms correctly
- [x] Implement downgrade/upgrade path for old → new format (no legacy, new system only)

**Tests**

- [x] Unit: encode/decode per-block blooms, format upgrade/downgrade
- [x] Property-based: random keys → compare ground truth vs per-block bloom membership
- [x] Integration: old SSTs still work; new SSTs have blooms
- [x] Phase 1 Core: 40 tests (11 inline + 19 integration + 10 baseline)
- [x] Phase 1.1 Writer: 10 tests (4+2+4)
- [x] Phase 1.2 Reader: 4 tests
- **Total: 54/54 tests passing**

**Benches**

- [x] Negative lookup microbench (cold/hot cache, with/without block blooms)
- [x] Bloom query latency (query hit/miss, batch queries, hash computation)
- [x] Tier 1 hotpath bench: 3 focused benchmarks with Criterion rigor

**Acceptance**

- [x] All 54 tests pass
- [x] Tier1 bench complete with standard configuration
- [x] Bench shows measurable fast-path for negative lookups
- [x] New SSTs created with per-block blooms (no backward compat needed)

---

## Phase 2: Tight Fence Pointers + Tombstone Awareness

**Design**

* Ensure **every data block** stores:
  - `min_user_key`, `max_user_key`
* For blocks containing **range tombstones**:
  - Track `tombstone_min`, `tombstone_max` ranges
* Thread `BlockMeta` through iterators and compaction instead of recomputing on-the-fly

**Usage**

* Iterator:
  - When performing range scans, skip blocks where `[block_max < range_start]` or `[block_min > range_end]`
* Compaction:
  - Use fence pointers to determine if a block is fully covered by range tombstones and can be dropped without reading

**Implementation Steps**

- [x] Make `BlockMeta` explicit in index:
  ```rust
  struct BlockMeta {
    min_key: Bytes,
    max_key: Bytes,
    data_offset: u64,
    data_len: u32,
    has_tombstones: bool,
    tombstone_min: Option<Bytes>,
    tombstone_max: Option<Bytes>,
  }
  ```
- [x] Ensure range tombstones participate in min/max calculation
- [x] Thread `BlockMeta` into compactor code
- [x] Update iterator to use `BlockMeta` for range skipping
- [x] Add compaction fast-path: skip block reads if fully covered

**Tests**

- [x] Compaction: tombstones cover entire blocks → blocks dropped without reads
- [x] Range scans: skipped blocks don't alter visible keys
- [x] Invariants: no resurrected keys after compaction
- **Phase 2 specific**: 12/12 tests passing (`tests/phase2_fence_pointers.rs`)
- **SST subsystem**: 444/444 tests passing (no regressions)
- **Compaction integration**: 18/18 tests passing

**Acceptance**

- [x] All tests pass
- [x] Compaction logs `skipped_blocks` for observability
- [x] Iterator block skipping validated with narrow range scans
- [x] Zero test failures across full suite

---

## Phase 3: Compact Sparse Index (In-Memory Layout)

**Design**

* Index stored **once on disk**, but loaded into two in-memory views:
  1. **Search array**: `Vec<SearchKey>` = prefix-compressed min-keys
  2. **Meta array**: `Vec<BlockMeta>` (offsets, lengths, flags)
* Binary search operates over `SearchKey` only → minimal memory
* All block metadata remains available for lookups, iteration, compaction

**Implementation Steps**

- [x] On SST open, decode on-disk index into compact `IndexTable`:
  ```rust
  struct IndexTable {
    search_keys: Vec<PrefixKey>,
    metas: Vec<BlockMeta>,
  }
  ```
- [x] Implement `IndexTable::find_block(key) -> Option<&BlockMeta>`
- [x] Add `SstMetadata::build_index_table()` helper method
- [x] Implement all query methods: `find_blocks_in_range()`, `memory_usage()`, iteration

**Tests**

- [x] 20 comprehensive IndexTable integration tests (`tests/phase3_index_table.rs`)
- [x] Block metadata preservation through IndexTable conversion
- [x] Binary search correctness over key ranges
- [x] Range intersection correctness

**Benchmarks**

- [x] Microbench: `IndexTable::find_block(key)` latency (tier2_subsystem_index_table)
- [x] Range query performance (`find_blocks_in_range`)
- [x] Memory footprint calculation
- [x] Scaling from 10 to 1000+ blocks

**Acceptance**

- [x] All 20 tests pass with 100% compliance
- [x] Benchmark suite runs successfully across block scales
- [x] No regressions in existing tests (2141/2141 passing)

---

## Phase 4: Range Tombstone Indexing

**Design**

* Store tombstones in **separate tombstone blocks** instead of mixing arbitrarily into data blocks.
* Maintain a **tombstone index** keyed by start key (and optionally end key).
* Decouple tombstone lookup from data block iteration.

**Usage**

* Point lookup:
  - After locating candidate data blocks and key, check tombstone index for any tombstone covering that key.
* Range scans:
  - Walk tombstone index alongside SST iterator and mask out deleted keys.
* Compaction:
  - Use tombstone index to quickly decide which ranges of lower-level SSTs can be dropped.

**On-Disk Layout**

* Tombstone blocks: sorted by start key
* `TombstoneIndexEntry { min_key, max_key, offset, len }`
* Footer indicates tombstone index presence and location

**Implementation Steps**

- [ ] Define on-disk tombstone block format (sorted by start key)
- [ ] Add `TombstoneIndexEntry` and `TombstoneIndex` struct
- [ ] Wire into read path (optional initially; fall back to naive iteration if missing)
- [ ] Wire into compaction (use range checks to drop covered blocks)
- [ ] Update SST writer to build tombstone index
- [ ] Update SST reader to load and query tombstone index

**Tests**

- [ ] Overlapping tombstones: correct masking and isolation
- [ ] Compaction: covered ranges dropped without reading
- [ ] Invariants: no resurrected keys after compaction, snapshot isolation preserved

**Benches**

- [ ] Compaction I/O reduction (tombstone-heavy workloads)
- [ ] Negative lookup speed (tombstone index query overhead)

**Acceptance**

- [ ] All tests pass, no snapshot isolation violations
- [ ] Compaction I/O reduction measurable for typical workloads

---

## Phase 5: Zone Maps / Analytics-Focused Metadata (Optional)

**Design**

* For workloads where keys encode time/sequence or you have typed values:
  - Store per-block min/max for key and selected columns/fields
* Used for:
  - Fast skipping in wide range scans
  - Future columnar/analytic readers on top of SSTs
* Keep as **separate optional metadata block** so core KV path isn't polluted

**Implementation Steps**

- [ ] Define zone map metadata format
- [ ] Gate behind feature flag / config option
- [ ] Build zone maps during SST write (if enabled)
- [ ] Load zone maps on SST open (if present)
- [ ] Use zone maps in compaction and iteration (fast-path, optional)

**Tests**

- [ ] Synthetic workloads: time-ordered keys, wide ranges
- [ ] Measure reduction in blocks touched vs baseline

**Acceptance**

- [ ] Minimal impact on core KV code
- [ ] Measurable improvement for analytical workloads (if enabled)

---

## Testing & Validation Strategy

### Invariant Suite (`tests/sst_invariants.rs`)

- [ ] Build SSTs with synthetic keysets
- [ ] Reopen and validate ordering, index coverage, bloom accuracy
- [ ] Test upgrade paths between SST format versions

### Integration Tests (`tests/sst_index_*.rs`)

- [ ] SST write → read cycles
- [ ] Multi-block SSTs with index, blooms, tombstones
- [ ] Range scans with block skipping
- [ ] Compaction with tombstone coverage

### Microbench Suite (`benches/sst_index.rs`)

- [ ] Index binary search latency
- [ ] Bloom query latency (per-SST, per-block)
- [ ] SST creation overhead
- [ ] Recovery time (footer read, checksum validation)
- [ ] Memory footprint (index, blooms in-memory)

### Fuzz Tests

- [ ] Random SST writes/reads
- [ ] Corruption detection and handling

---

## Acceptance Criteria

- [x] SST_INDEX_DESIGN.md complete and reviewed
- [x] Phase 0: Baseline locked (INDEX_SPEC.md, invariant tests)
- [x] Phase 1: Per-block blooms integrated and benched
- [x] Phase 2: Fence pointers threaded through iterators and compaction
- [x] Phase 3: Compact sparse index with memory-efficient layout
  - [x] IndexTable struct fully implemented with binary search
  - [x] 20 comprehensive tests covering find_block and range queries
  - [x] Tier2 subsystem benchmark suite for performance validation
  - [x] SstMetadata::build_index_table() integration method
  - [x] 2141/2141 tests passing (100% compliance)
- [ ] Phase 4: Tombstone indexing reduces compaction I/O
- [ ] Phase 5 (optional): Zone maps available for analytics
- [x] All unit and integration tests pass (2141 total)
- [x] Bench suite shows expected improvements
- [x] Code organized in `src/sst/` with clear module boundaries
- [x] No regressions in core KV latency or throughput

