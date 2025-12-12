# Tier-2 Subsystem Coverage Analysis

## Existing Tier-2 Benchmarks (7)

### ✅ Well-Covered Subsystems

**SST Read Path** (3 benchmarks):
- `sst_point_read_bloom.rs` - Bloom → Sparse Index → Cache
- `range_scan_cache.rs` - Range scans with warm/cold cache
- `iterator_multi_sst.rs` - Multi-SST merge iterator

**Cache** (1 benchmark):
- `block_cache.rs` - Eviction, LRU, access patterns

**Bloom Filters** (2 benchmarks):
- `bloom_build.rs` - Filter construction (10k, 100k, 1M keys)
- `bloom_false_positive_rate.rs` - FPR measurement

**Memtable** (1 benchmark):
- `memtable_rotate.rs` - Fill + drain cycle

## Missing Tier-2 Coverage

### 🔴 Critical Gaps (High Value)

**1. WAL Subsystem** (`src/wal/` - 10 files)
- ❌ **WAL append throughput** - Sequential write performance
- ❌ **WAL recovery** - Replay performance for crash recovery
- ❌ **WAL segment rollover** - File rotation overhead
- **Priority**: HIGH - WAL is write path critical

**2. SST Write Path** (`src/sst/`)
- ❌ **SST builder throughput** - How fast can we build SSTs?
- ❌ **Compression impact** - None/LZ4/Zstd overhead comparison
- ❌ **Sparse index build** - Index construction cost
- ❌ **Trie index build** - Alternative index construction
- **Priority**: HIGH - Write path not benchmarked

**3. Compaction** (`src/compaction/` - 5 files)
- ❌ **Merge iterator performance** - Core compaction primitive
- ❌ **Compaction task execution** - End-to-end compaction time
- ❌ **Level selection overhead** - Planner performance
- **Priority**: MEDIUM - Complex subsystem, critical for LSM

**4. Metadata/Manifest** (`src/metadata/` - 5 files)
- ❌ **Manifest apply** - Version edit application
- ❌ **Version set lookup** - File lookup performance
- ❌ **Manifest persistence** - Write/read overhead
- **Priority**: MEDIUM - Metadata on hot path

**5. Trie Index** (`src/sst/trie/`)
- ❌ **Trie lookup** - Key→block mapping (alternative to sparse index)
- ❌ **Trie prefix scan** - Range query performance
- **Priority**: LOW - Already have sparse_index tier-1 bench

**6. Iterator Subsystem** (`src/iterators/` - 3 files)
- ❌ **Merge iterator** - Heap-based merge performance
- ❌ **SkipList iterator** - In-memory traversal
- **Priority**: LOW - Partially covered by iterator_multi_sst

### 🟡 Nice-to-Have

**7. Compression** (`src/sst/compression/`)
- ❌ **Compression throughput** - LZ4/Zstd/Zlib comparison
- ❌ **Adaptive compression** - Policy selection overhead
- **Priority**: LOW - Use case specific

**8. Index Tuner** (`src/sst/index/tuner.rs`)
- ❌ **Index profiling** - Adaptive index selection
- **Priority**: LOW - Advanced feature

## Recommended Backfill Priority

### Phase 1: Write Path (HIGH) - 3 benchmarks
1. **`wal_append_throughput.rs`**
   - Tests: Sequential writes with NoSync/Sync modes
   - Metrics: Throughput MB/s, latency p50/p99
   - Validates: WAL write performance critical for durability

2. **`sst_builder_throughput.rs`**
   - Tests: Build SST from 10k, 100k, 1M keys with/without compression
   - Metrics: Build time, compression ratio, throughput
   - Validates: Flush performance

3. **`compaction_merge_performance.rs`**
   - Tests: Merge 2-5 SSTs with overlap patterns
   - Metrics: Merge throughput, key comparisons
   - Validates: Core compaction primitive

### Phase 2: Recovery & Metadata (MEDIUM) - 2 benchmarks
4. **`wal_recovery.rs`**
   - Tests: Replay 100k, 1M WAL entries
   - Metrics: Recovery time, throughput
   - Validates: Crash recovery performance

5. **`metadata_version_apply.rs`**
   - Tests: Apply 100, 1000 version edits
   - Metrics: Apply latency, lookup time
   - Validates: Metadata hot path

### Phase 3: Advanced (LOW) - Optional
6. Compression comparison
7. Trie index lookup
8. Index tuner profiling

## Summary

**Current Coverage**: 7 benchmarks
- ✅ Read path (SST, cache, bloom)
- ✅ Memtable
- ❌ Write path (WAL, SST builder)
- ❌ Compaction
- ❌ Recovery
- ❌ Metadata

**Recommended**: Add 5 benchmarks (3 high priority, 2 medium priority) to achieve comprehensive tier-2 coverage.
