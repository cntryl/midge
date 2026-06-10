//! Tier 2 — Range Scan with Cache Warm/Cold
//!
//! **Target Runtime:** 3-6 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! **Purpose**: Quantifies block cache value for range scans by comparing warm vs cold cache.
//! Validates that caching provides significant speedup for sequential access patterns.
//!
//! **Tier-2 Compliance**:
//! - Subsystem interaction: Iterator → Block cache → Block reads
//! - System metrics: Cache hit rate, blocks read, scan throughput
//! - Realistic patterns: Sequential scans (10, 100, 1000 blocks)

#[path = "./criterion_config.rs"]
mod criterion_config;

use cntryl_midge::sst::cache::{BlockCache, CacheKey, CachePolicyType};
use cntryl_midge::Bytes;
use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_config::criterion_config_for_tier2;
use std::hint::black_box;

// ─── Test Configuration ──────────────────────────────────────────────────────

const BLOCK_SIZE: usize = 4096;
const _KEYS_PER_BLOCK: usize = 100;
const SST_ID: u64 = 1;

/// Represents a range scan over consecutive blocks
struct RangeScan {
    start_block: usize,
    num_blocks: usize,
}

impl RangeScan {
    fn new(start_block: usize, num_blocks: usize) -> Self {
        Self {
            start_block,
            num_blocks,
        }
    }

    /// Execute scan with cache, returning (blocks_read, cache_hits)
    fn execute(&self, cache: &BlockCache, sst_id: u64, miss_block_data: &Bytes) -> (u32, u32) {
        let mut blocks_read = 0u32;
        let mut cache_hits = 0u32;

        for block_idx in self.start_block..(self.start_block + self.num_blocks) {
            let key = CacheKey::for_data(sst_id, (block_idx * BLOCK_SIZE) as u64);

            if cache.get(&key).is_some() {
                cache_hits += 1;
            } else {
                // Simulate block read + cache insert
                blocks_read += 1;
                cache.put(key, miss_block_data.clone());
            }
        }

        (blocks_read, cache_hits)
    }
}

/// Pre-generate block data (deterministic, no allocations in benchmark)
fn precompute_block_data() -> Bytes {
    Bytes::from_static(&[0xCD; BLOCK_SIZE])
}

/// Populate cache with specified block range
fn populate_cache(cache: &BlockCache, sst_id: u64, start_block: usize, num_blocks: usize) {
    let block_data = precompute_block_data();
    for block_idx in start_block..(start_block + num_blocks) {
        let key = CacheKey::for_data(sst_id, (block_idx * BLOCK_SIZE) as u64);
        cache.put(key, block_data.clone());
    }
}

// ─── Warm Cache Benchmarks ───────────────────────────────────────────────────

/// Benchmark range scan with warm cache (all blocks cached)
fn bench_range_scan_warm_cache(c: &mut Criterion) {
    let miss_block_data = precompute_block_data();

    for &num_blocks in &[10, 100, 1000] {
        let mut group = c.benchmark_group(format!("range_scan_warm_cache_{}_blocks", num_blocks));
        group.sampling_mode(SamplingMode::Flat);
        group.throughput(Throughput::Elements(num_blocks as u64));

        group.bench_function("sequential_scan", |b| {
            // Pre-populate cache
            let cache = BlockCache::new(10 * 1024 * 1024, 16, CachePolicyType::Lru);
            populate_cache(&cache, SST_ID, 0, num_blocks);

            let scan = RangeScan::new(0, num_blocks);

            b.iter(|| {
                let (blocks_read, cache_hits) = scan.execute(&cache, SST_ID, &miss_block_data);

                black_box((blocks_read, cache_hits))
            })
        });

        group.finish();
    }
}

// ─── Cold Cache Benchmarks ───────────────────────────────────────────────────

/// Benchmark range scan with cold cache (no blocks cached, must read all)
fn bench_range_scan_cold_cache(c: &mut Criterion) {
    let miss_block_data = precompute_block_data();

    for &num_blocks in &[10, 100, 1000] {
        let mut group = c.benchmark_group(format!("range_scan_cold_cache_{}_blocks", num_blocks));
        group.sampling_mode(SamplingMode::Flat);
        group.throughput(Throughput::Elements(num_blocks as u64));

        group.bench_function("sequential_scan", |b| {
            b.iter_batched(
                || {
                    // Create fresh cache for each iteration
                    let cache = BlockCache::new(10 * 1024 * 1024, 16, CachePolicyType::Lru);
                    (cache, RangeScan::new(0, num_blocks))
                },
                |(cache, scan)| {
                    let (blocks_read, cache_hits) = scan.execute(&cache, SST_ID, &miss_block_data);

                    black_box((blocks_read, cache_hits))
                },
                criterion::BatchSize::SmallInput,
            )
        });

        group.finish();
    }
}

// ─── Partially Warm Cache ────────────────────────────────────────────────────

// ─── Strided Access Pattern ──────────────────────────────────────────────────

/// Benchmark non-sequential access (every 10th block) with warm/cold cache
fn bench_range_scan_strided_access(c: &mut Criterion) {
    let block_data = precompute_block_data();
    let stride = 10;
    let num_accesses = 100; // Access 100 blocks with stride=10 (covers 1000 blocks)

    let mut group = c.benchmark_group("range_scan_strided_access");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(num_accesses as u64));

    for &mode in &["warm", "cold"] {
        group.bench_with_input(BenchmarkId::from_parameter(mode), &mode, |b, &mode| {
            if mode == "warm" {
                let cache = BlockCache::new(10 * 1024 * 1024, 16, CachePolicyType::Lru);
                // Pre-populate strided blocks
                for i in 0..num_accesses {
                    let block_idx = i * stride;
                    let key = CacheKey::for_data(SST_ID, (block_idx * BLOCK_SIZE) as u64);
                    cache.put(key, block_data.clone());
                }

                b.iter(|| {
                    let mut cache_hits = 0u32;
                    for i in 0..num_accesses {
                        let block_idx = i * stride;
                        let key = CacheKey::for_data(SST_ID, (block_idx * BLOCK_SIZE) as u64);
                        if cache.get(&key).is_some() {
                            cache_hits += 1;
                        }
                    }
                    black_box(cache_hits)
                })
            } else {
                b.iter_batched(
                    || BlockCache::new(10 * 1024 * 1024, 16, CachePolicyType::Lru),
                    |cache| {
                        let mut blocks_read = 0u32;
                        for i in 0..num_accesses {
                            let block_idx = i * stride;
                            let key = CacheKey::for_data(SST_ID, (block_idx * BLOCK_SIZE) as u64);
                            if cache.get(&key).is_none() {
                                blocks_read += 1;
                                cache.put(key, block_data.clone());
                            }
                        }
                        black_box(blocks_read)
                    },
                    criterion::BatchSize::SmallInput,
                )
            }
        });
    }

    group.finish();
}

// ─── Criterion Setup ─────────────────────────────────────────────────────────

criterion_group! {
    name = tier2_subsystem_range_scan_cache;
    config = criterion_config_for_tier2();
    targets =
        bench_range_scan_warm_cache,
        bench_range_scan_cold_cache,
        bench_range_scan_strided_access
}
criterion_main!(tier2_subsystem_range_scan_cache);
