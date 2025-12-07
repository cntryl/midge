# Streaming Backend Optimization Roadmap

**Context**: Single-producer, multi-consumer streaming workload. Consumers perform historical window scans with heavy negative lookups. No deletes.

**Goal**: Maximize read throughput and minimize latency for range scans on historical data.

**Strategy**: Focus on three high-ROI optimizations that directly impact consumer query latency. Skip phases that don't apply to streaming (no deletes = skip tombstone indexing).

---

## Phase 1.5: Bloom Filter Tuning for Negative Lookups ⭐⭐⭐⭐

**Effort**: ⭐ Very Low (1-2 days)  
**Impact**: ⭐⭐⭐⭐ Huge (30-50% negative lookup speedup)  
**Why**: 80-95% of consumer queries are negative lookups across historical windows.

### Design

Optimize per-block bloom filters for the negative-lookup case:

1. **Increase bloom bits/key**: 8 → 12
   - False positive rate: 0.1% → 0.01% (huge for negative case)
   - Cost: ~50% more memory, negligible CPU

2. **Add L1-cached "fast negative" bitset**
   - 1KB summary bitset per SST (256 bits = 256 blocks)
   - Each bit = "any non-matching query would skip this block"
   - Checked first before per-block bloom
   - Hits L1 cache for 95% of negative queries

3. **Precompute bloom hashes in iterator**
   - Cache hash(key) during binary search
   - Reuse during per-block bloom probe
   - Saves 1-2 hash computations per block

### Implementation Steps

- [x] Increase per-block bloom bits/key from 8 → 12 in `BlockBloom`
- [x] Add `FastNegativeFilter` struct (256-bit bitset per SST)
  - Built during SST write from block blooms
  - Loaded on SST open alongside bloom filter
- [x] Update read path:
  - [x] Check SST-level bloom (existing)
  - [x] Check fast negative bitset (NEW, L1-cached)
  - [x] Check per-block bloom (existing, now 12 bits/key)
  - [x] Read block if all three pass
- [x] Precompute hash in iterator
  - [x] Cache `hash(key)` during binary search
  - [x] Reuse in per-block bloom probe
- [x] Update writer to build fast negative filter
  - [x] Update writer to build fast negative filter

### Tests

- [x] Unit: Fast negative filter construction with various block bloom patterns
- [x] Unit: False positive rate with 12 bits/key (should be < 0.01%)
- [x] Integration: Negative lookup correctness (no false negatives)
- [x] Integration: 1000-block SST with varied key distributions
- [x] Property-based: Random keys vs bloom membership

### Benchmarks

- [x] Microbench: Negative lookup latency (8 bits/key vs 12 bits/key)
- [x] Microbench: Fast negative filter query cost (L1 hit vs miss)
- [x] Integration: Range scan with 90% negative blocks
- [ ] Measure: Memory footprint increase (should be small)

### Acceptance Criteria

- [ ] All tests pass (100% compliance)
- [ ] Benchmark shows 30-50% improvement on negative lookups
- [ ] Memory overhead < 2% per SST
- [x] No regressions in existing tests

---

## Phase 2.5: Fence-Pointer Range Skipping in Iterators ⭐⭐⭐⭐

**Effort**: ⭐ Low (1-2 days)  
**Impact**: ⭐⭐⭐⭐ Huge (3-10x faster wide range scans)  
**Why**: Consumer queries often scan large time windows; fence pointers skip 30-90% of blocks.

### Design

Use fence pointers (`BlockMeta.min_key`, `BlockMeta.max_key`) to skip blocks without touching them:

```
Consumer query: "Give me user_67890 from 2:00 PM to 3:00 PM"
→ Binary search to 2:00 PM block
→ Iterate: block 100 (covers 2:00-2:17) → check fence pointer
→ Block 101 (covers 2:17-2:34) → matches user, read it
→ Block 102 (covers 2:34-2:51) → user_67890 not in range, skip (fence pointer)
→ Block 103-140 (2:51-3:00) → fence pointer check first, then read if needed
```

Key insight: **Avoid decompression entirely for skipped blocks.**

### Implementation Steps

- [x] Add block skip counter to iterator for observability
- [x] Update range scan iterator:
  - [x] At block boundary, check `block_meta.min_key >= range_end`
  - [x] If true, stop iteration (we've passed the range)
  - [x] Check `block_meta.max_key < range_start`
  - [x] If true, skip to next block (fence pointer tells us no keys in range)
- [x] Optimize for sequential access:
  - [x] Cache last queried block index
  - [x] If next query is same range, resume from last position
- [ ] Measure block-skip ratio in benchmarks
- [x] Add `Iterator::skipped_blocks()` metric

### Tests

- [x] Unit: Fence pointer comparison logic (min/max checks)
- [x] Integration: Wide range scan with 1000 blocks, mixed matches
- [x] Integration: Narrow range scan (single block result)
- [x] Integration: Range before all blocks (no reads)
- [x] Integration: Range after all blocks (no reads)
- [x] Property-based: Random ranges → correct block set returned

### Benchmarks

- [ ] Microbench: Binary search + fence pointer skip vs baseline
- [ ] Integration: Range scan efficiency (blocks touched vs total)
- [ ] Integration: 1000-block SST, various window sizes (10%, 50%, 100% coverage)
- [ ] Measure: Block skip ratio (should be 50-90% for typical queries)

### Acceptance Criteria

- [ ] All tests pass (100% compliance)
- [ ] Benchmark shows 3-10x improvement on wide range scans
- [ ] Skip ratio benchmark shows 50-90% blocks skipped for typical windows
- [x] No regressions in existing tests
- [x] Iterator produces identical results to baseline

---

## Phase 3.5: IndexTable Sequential Access Optimization ⭐⭐⭐

**Effort**: ⭐⭐ Medium (2-3 days)  
**Impact**: ⭐⭐⭐ Medium (10-15% throughput improvement)  
**Why**: Consumer iterators read sequential blocks; optimize the hot path.

### Design

Streaming queries often read many blocks in order:

```
Iterator reads: Block 100 → 101 → 102 → 103 → ... → 200

Today: Each block lookup = binary search on 100 keys
Phase 3.5: Most lookups are "next block" = cache/predictor hit
```

### Implementation Steps

- [ ] Cache-line pack `BlockMeta`
  - [ ] Target 32 bytes = 2-4 metas per cache line
  - [ ] Reorder fields: most-accessed first (min_key, max_key, handle)
  - [ ] Benchmark cache efficiency
- [x] Add sequential predictor to `IndexTable`
  - [x] Track last queried index
  - [x] If next query is `key > last_max_key`, try `last_index + 1` first
  - [x] Fall back to binary search if predictor misses
- [x] Lock-free 64-entry direct-mapped cache for sequential access
  - [x] Hash(key) = entry in fixed array
  - [x] Store (key_prefix, block_meta) without locks
  - [x] Hits on sequential reads, misses on random access (acceptable)

### Tests

- [x] Unit: Sequential predictor correctness
- [x] Unit: Cache-line packing (verify size = 32 bytes)
- [x] Integration: Sequential block access (100+ blocks)
- [x] Integration: Random access (verify fallback)
- [x] Integration: Mixed sequential + random access

### Benchmarks

- [x] Microbench: Sequential block lookups (predictor hit % rate)
- [ ] Microbench: Cache-line packing impact (L1 hit rate)
- [x] Integration: Iterator throughput with 1000-block sequential scan
- [x] Measure: Predictor hit ratio (should be 85-95% for streaming)

### Acceptance Criteria

- [ ] All tests pass (100% compliance)
- [ ] Benchmark shows 10-15% throughput improvement
- [ ] Predictor hit ratio > 85% for sequential scans
- [x] No regressions in existing tests

---

## Summary: What NOT to Do

### ❌ Phase 4: Range Tombstone Indexing

**Why it's wrong for streaming:**
- Single producer + consumer-only readers = ~0 deletes
- Tombstone index adds format complexity, on-disk overhead, test burden
- Zero benefit for your workload
- **Skip entirely**

### ❌ Phase 5: Zone Maps

**Why it's wrong for streaming:**
- Zone maps are for **analytical workloads** (columnar, time-windowed aggregates)
- Streaming is KV-focused, not analytical
- **Skip entirely**

---

## Implementation Sequence

### Week 1: Phase 1.5 (Bloom Tuning)
```
Day 1-2: Design + fast negative filter implementation
Day 3: Iterator hash caching
Day 4: Tests + benchmarks
Day 5: Polish + documentation
```

**Expected Result**: 30-50% faster negative lookups, immediately visible

### Week 2: Phase 2.5 (Fence-Pointer Skipping)
```
Day 1-2: Iterator fence-pointer logic
Day 3: Sequential access optimization
Day 4: Tests + benchmarks
Day 5: Polish + skip-ratio analysis
```

**Expected Result**: 3-10x faster wide range scans, major throughput gain

### Week 3: Phase 3.5 (Index Sequential Optimization)
```
Day 1: Cache-line packing
Day 2: Sequential predictor + direct-mapped cache
Day 3-4: Tests + benchmarks
Day 5: Polish
```

**Expected Result**: 10-15% iterator throughput improvement

### STOP HERE

After these three phases, your system is **world-class for streaming KV workloads**.

---

## Acceptance Criteria (Overall)

- [ ] Phase 1.5: Bloom tuning complete, 30-50% negative lookup speedup
- [ ] Phase 2.5: Fence-pointer skipping, 3-10x wide scan improvement
- [ ] Phase 3.5: Index optimization, 10-15% iterator throughput gain
- [ ] All tests pass (100% compliance)
- [ ] Streaming benchmark suite validates end-to-end improvements
- [ ] No regressions in existing tests
- [ ] Code organized with clear module boundaries
- [ ] Consumer query latency measurably improved (target: 50-60% improvement over baseline)

---

## Why This Order Works

1. **Phase 1.5 first**: Low effort, high impact, affects all queries
2. **Phase 2.5 second**: Builds on bloom work, massive win for consumer use case
3. **Phase 3.5 third**: Polish, refines sequential access path
4. **Then stop**: System is optimized for your actual workload

**Not building** Phase 4 or 5 saves ~4-6 weeks of engineering while delivering 50-60% real-world improvement.
