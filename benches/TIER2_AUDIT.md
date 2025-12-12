# Tier-2 Subsystem Benchmarks — Audit & Backfill Plan

**Date:** 2024-12-12  
**Branch:** actor-model

## Tier-2 Design Criteria

**Purpose:** Answer "Is this subsystem worth it?" and "Does this design choice improve real engine behavior?"

**Should include:**
- Single major subsystem end-to-end in real execution context
- Realistic interactions with adjacent components (e.g., Bloom inside SST reads)
- Realistic access patterns (mixed hit/miss, skewed distributions)
- Controlled background behavior (flush, limited compaction, cache eviction)
- System-level metrics (cache hit rate, blocks avoided, false positive cost, read amplification)
- Fixed seeds for deterministic randomness
- Wall-clock latency, throughput, secondary counters

**Must NOT include:**
- Full end-to-end production scenarios
- Multi-node or distributed behavior
- Long-running stress/soak tests
- Chaos testing (corruption, kill -9)
- Unbounded randomness or nondeterministic timing

---

## Current State Assessment

### ✅ Compiling & Tier-2 Compliant

**None currently compile** due to actor-model migration breaking imports.

### ❌ Broken (Import Errors)

1. **block_cache.rs** - Import errors (`BlockCacheOptions`, types moved)
2. **bloom_build.rs** - Import error (`BloomFilterBuilder` moved)
3. **bloom_false_positive_rate.rs** - Import error (`BloomFilterBuilder`)
4. **flush.rs** - Import errors (`SstMemWriter`, types moved)
5. **sst.rs** - Import errors (TLV types moved)
6. **streaming_iterators.rs** - Import errors
7. **streaming_range_scan.rs** - Import errors
8. **wal_io.rs** - Import errors
9. **wal_replay.rs** - Import errors
10. **wal_segment_rollover.rs** - Import errors
11. **memtable_full.rs** - Import errors
12. **memtable_rotate.rs** - Import errors
13. **manifest_apply.rs** - Import errors
14. **manifest_large_history.rs** - Import errors
15. **manifest_parse.rs** - Import errors
16. **index_table.rs** - Import errors
17. **tombstone_index.rs** - Import errors
18. **core_primitives.rs** - Import errors
19. **streaming_iterator_throughput.rs** - Import errors

---

## Tier-2 Gaps (Missing Benchmarks)

Based on Tier-2 criteria, we're missing these critical subsystem benchmarks:

### 🔴 **Priority 1: Core Read Path**

1. **SST Point Read with Bloom Filter On/Off**
   - Measures: Blocks avoided, false positive cost
   - Config: Bloom bits/key (8, 10, 12)
   - Access: 50% hit, 50% miss
   - **Why:** Validates bloom filter effectiveness in real SST reads

2. **Range Scan with Block Cache Warm/Cold**
   - Measures: Cache hit rate, blocks read, scan latency
   - Scenarios: Cold cache, warm cache, mixed working set
   - **Why:** Shows cache value for range queries

3. **Iterator Traversal Across Multiple SSTs**
   - Measures: Merge cost, block reads, cache pressure
   - Config: 2-5 SSTs with overlapping ranges
   - **Why:** Tests MergeIterator in realistic LSM context

### 🟡 **Priority 2: Write Path & Background Work**

4. **Memtable Flush with Bloom Building**
   - Measures: Flush time, bloom build cost, SST size
   - Config: Different bloom configurations
   - **Why:** Shows flush path cost with realistic SST building

5. **Read Amplification Under Mixed Workload**
   - Measures: Blocks read per query, cache behavior
   - Workload: Zipfian key distribution, mixed get/scan
   - **Why:** Reveals read amplification in real access patterns

6. **Compaction Impact on Foreground Reads**
   - Measures: P50/P99 read latency during compaction
   - Scenario: Background compaction while serving reads
   - **Why:** Shows interference between background and foreground

### 🟢 **Priority 3: Index Structures**

7. **Sparse Index vs Trie for Block Lookup**
   - Measures: Lookup latency, memory overhead
   - Config: Different key distributions (sequential, random, hierarchical)
   - **Why:** Validates index design choice

8. **Block Cache Eviction Under Real Access Pattern**
   - Measures: Hit rate, eviction churn
   - Access: Zipfian with working set rotation
   - **Why:** Tests cache policy effectiveness

---

## Backfill Strategy

### Phase 1: Fix Existing Benchmarks (Broken Imports)

**Goal:** Get existing tier2 benchmarks compiling and validate against Tier-2 criteria.

**Action Items:**
1. Fix import paths for actor-model migration
2. Audit each benchmark against Tier-2 criteria
3. Remove benchmarks that don't meet criteria (move to tier3 if appropriate)
4. Update benchmarks to use realistic access patterns if currently using simple sequential

**Estimated:** 19 benchmarks to fix/audit

### Phase 2: Create Priority 1 Benchmarks (Core Read Path)

**Focus:** SST reads with bloom, block cache, and iterators - the most critical subsystems.

**New Benchmarks:**
1. `sst_point_read_bloom.rs` - SST point reads with bloom on/off
2. `range_scan_cache.rs` - Range scans with warm/cold cache
3. `iterator_multi_sst.rs` - Iterator across multiple SSTs

**Estimated:** 3 new benchmarks

### Phase 3: Create Priority 2 Benchmarks (Write Path)

**Focus:** Flush, compaction interaction, read amplification.

**New Benchmarks:**
4. `flush_with_bloom.rs` - Memtable flush including bloom building
5. `read_amplification.rs` - Mixed workload read amplification
6. `compaction_interference.rs` - Foreground read latency during compaction

**Estimated:** 3 new benchmarks

### Phase 4: Create Priority 3 Benchmarks (Index Structures)

**Focus:** Index comparison and cache policies.

**New Benchmarks:**
7. `index_comparison.rs` - Sparse index vs trie
8. `cache_eviction_patterns.rs` - Cache behavior under realistic access

**Estimated:** 2 new benchmarks

---

## Benchmark Template

```rust
//! Tier 2 — [Subsystem Name] Benchmark
//!
//! **Target Runtime:** 2-8 seconds
//! **Run Frequency:** Local development / Periodic CI
//!
//! **Subsystem Goal:** [What design question does this answer?]
//!
//! **Realistic Context:**
//! - [Adjacent component interaction]
//! - [Access pattern description]
//! - [Background behavior included]
//!
//! **Metrics:**
//! - [Primary metric 1]
//! - [Secondary metric 2]
//! - [System impact metric 3]

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;

// Fixed seed for deterministic randomness
const BENCH_SEED: u64 = 0xDEADBEEFCAFEBABE;

fn bench_subsystem_scenario(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_[name]_[scenario]");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(/* count */));

    // Setup: Create realistic execution context
    // - Pre-populate data
    // - Configure subsystem
    // - Set up adjacent components

    group.bench_function("[scenario_name]", |b| {
        b.iter(|| {
            // Execute subsystem operation in realistic context
            // - Measure wall-clock latency
            // - Capture secondary metrics
            black_box(/* result */)
        })
    });

    // Report system-level metrics after benchmark
    println!("[Subsystem]: [metric] = [value]");

    group.finish();
}

criterion_group! {
    name = tier2_subsystem_[name];
    config = criterion_config_for_tier(BenchTier::Tier2Subsystem);
    targets = bench_subsystem_scenario
}
criterion_main!(tier2_subsystem_[name]);
```

---

## Success Criteria

**Phase 1 Complete:**
- All 19 existing tier2 benchmarks compile
- Each validated against Tier-2 criteria
- Removed or moved benchmarks that don't fit

**Phase 2 Complete:**
- 3 core read path benchmarks created
- Each answers specific design question
- Realistic access patterns implemented
- System-level metrics reported

**Phases 3-4 Complete:**
- 5 additional subsystem benchmarks created
- Full coverage of major subsystems
- Design trade-offs quantified

**Final State:**
- ~15-20 tier2 benchmarks (after cleanup)
- + 8 new high-value benchmarks
- = ~23-28 total tier2 benchmarks
- All measuring realistic subsystem behavior with system-level impact metrics
