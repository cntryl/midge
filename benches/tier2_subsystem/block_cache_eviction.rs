//! Tier 2 — Block Cache Eviction Benchmarks
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers block cache LRU eviction behavior under pressure

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;
use std::sync::Arc;

use cntryl_midge::sst::block_cache::{
    BlockCache, BlockCacheOptions, BlockData, BlockKey, BlockKind, ShardedBlockCache,
};

/// Pre-computed block keys to avoid allocation in benchmarks.
struct PrecomputedKeys {
    keys: Vec<BlockKey>,
}

impl PrecomputedKeys {
    fn new(count: usize) -> Self {
        let keys = (0..count)
            .map(|i| BlockKey::new(0, (i * 4096) as u64, BlockKind::Data, 0))
            .collect();
        Self { keys }
    }

    #[inline]
    fn get(&self, idx: usize) -> &BlockKey {
        &self.keys[idx]
    }
}

/// Pre-allocated block data to avoid allocation in benchmarks.
fn make_block_data_static() -> BlockData {
    static BLOCK_DATA: [u8; 4096] = [0xAB; 4096];
    let data: Arc<[u8]> = Arc::from(&BLOCK_DATA[..]);
    BlockData::uncompressed(data, BlockKind::Data)
}

fn create_cache(capacity: usize) -> ShardedBlockCache {
    ShardedBlockCache::new(BlockCacheOptions::with_capacity(capacity))
}

/// Benchmark LRU eviction with 1k entries
/// Fills cache to capacity, then continues inserting to trigger evictions
fn bench_block_cache_lru_eviction_1k(c: &mut Criterion) {
    // Pre-compute all keys needed (1125 total: 125 initial + 1000 evictions)
    let keys = PrecomputedKeys::new(1125);
    let block = make_block_data_static();

    let mut group = c.benchmark_group("subsystem_block_cache_lru_eviction_1k");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1000)); // Measuring the 1k eviction inserts

    group.bench_function("evict_1k", |b| {
        b.iter_batched(
            || {
                // Setup: create cache and fill to capacity (125 blocks in 512KB)
                let cache = create_cache(512 * 1024);
                for i in 0..125 {
                    cache.insert(keys.get(i).clone(), block.clone());
                }
                cache
            },
            |cache| {
                // Measure: insert 1k more blocks, triggering evictions
                for i in 125..1125 {
                    cache.insert(keys.get(i).clone(), block.clone());
                }
                black_box(cache)
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

/// Benchmark LRU eviction with 10k entries
/// Larger-scale eviction stress test
fn bench_block_cache_lru_eviction_10k(c: &mut Criterion) {
    // Pre-compute all keys needed (10500 total: 500 initial + 10000 evictions)
    let keys = PrecomputedKeys::new(10_500);
    let block = make_block_data_static();

    let mut group = c.benchmark_group("subsystem_block_cache_lru_eviction_10k");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(10_000)); // Measuring the 10k eviction inserts

    group.bench_function("evict_10k", |b| {
        b.iter_batched(
            || {
                // Setup: create cache and fill to capacity (500 blocks in 2MB)
                let cache = create_cache(2 * 1024 * 1024);
                for i in 0..500 {
                    cache.insert(keys.get(i).clone(), block.clone());
                }
                cache
            },
            |cache| {
                // Measure: insert 10k more blocks, heavy eviction pressure
                for i in 500..10_500 {
                    cache.insert(keys.get(i).clone(), block.clone());
                }
                black_box(cache)
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group! {
    name = tier2_subsystem_block_cache_eviction;
    config = criterion_config_for_tier(BenchTier::Tier2Subsystem);
    targets = bench_block_cache_lru_eviction_1k, bench_block_cache_lru_eviction_10k
}
criterion_main!(tier2_subsystem_block_cache_eviction);
