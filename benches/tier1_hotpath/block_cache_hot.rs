//! Tier 1 — Hot Path Block Cache Benchmarks
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers critical block cache hot paths:
//! - Hot cache lookups and insertions
//! - Hit ratio calculations

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use cntryl_midge::sst::block_cache::{
    BlockCache, BlockCacheOptions, BlockData, BlockKey, BlockKind, ShardedBlockCache,
};
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;
use std::sync::Arc;

/// Pre-allocated block data to avoid allocation in benchmark setup.
fn make_block_data(size: usize) -> BlockData {
    let data: Arc<[u8]> = vec![0xAB; size].into();
    BlockData::uncompressed(data, BlockKind::Data)
}

/// Pre-create a block key.
#[inline]
fn make_block_key(offset: u64) -> BlockKey {
    BlockKey::new(1, offset, BlockKind::Data, 0)
}

fn create_cache(capacity: usize) -> ShardedBlockCache {
    ShardedBlockCache::new(BlockCacheOptions::with_capacity(capacity))
}

/// Benchmark block cache get operations on hot data
fn bench_block_cache_get_hot(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_block_cache_get_hot");
    group.measurement_time(std::time::Duration::from_millis(200));
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let cache = create_cache(10 * 1024 * 1024); // 10MB cache

    // Pre-populate with hot data (setup outside benchmark loop)
    for i in 0..1000 {
        cache.insert(make_block_key(i * 4096), make_block_data(4096));
    }

    // Precompute hot key (no allocation in hot path)
    let hot_key = make_block_key(42 * 4096);

    group.bench_function("get_hot_4k_block", |b| {
        b.iter(|| black_box(cache.get(black_box(&hot_key))))
    });

    group.finish();
}

/// Benchmark block cache insert operations.
///
/// This measures single-block insertion into a warm cache, not cache creation.
fn bench_block_cache_insert_hot(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_block_cache_insert_hot");
    group.measurement_time(std::time::Duration::from_millis(200));
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    // Create cache once outside benchmark - large enough to not trigger eviction
    let cache = create_cache(100 * 1024 * 1024); // 100MB cache

    // Pre-populate to simulate realistic warm cache state
    for i in 0..100 {
        cache.insert(make_block_key(i * 4096), make_block_data(4096));
    }

    // Pre-create block data outside hot loop (avoid allocation in measurement)
    let block_data = make_block_data(4096);

    // Use a counter to insert unique keys (avoiding overwrites)
    let mut offset_counter = 1000u64;

    group.bench_function("insert_4k_block", |b| {
        b.iter(|| {
            let key = make_block_key(offset_counter * 4096);
            offset_counter = offset_counter.wrapping_add(1);
            cache.insert(black_box(key), black_box(block_data.clone()));
        })
    });

    group.finish();
}

/// Benchmark hit ratio calculation (batch of 100 lookups).
///
/// Measures the amortized cost of cache lookups when computing hit ratio
/// across a batch of accesses. All keys are hits for stable measurement.
fn bench_block_cache_hit_ratio_fast(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_block_cache_hit_ratio_fast");
    group.measurement_time(std::time::Duration::from_millis(200));
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(100)); // 100 lookups per iteration

    let cache = create_cache(10 * 1024 * 1024);

    // Pre-populate cache
    for i in 0..100 {
        cache.insert(make_block_key(i * 4096), make_block_data(1024));
    }

    // Precompute keys for accesses (all hits)
    let access_keys: Vec<BlockKey> = (0..100).map(|i| make_block_key(i * 4096)).collect();

    group.bench_function("hit_ratio_calc_100_accesses", |b| {
        b.iter(|| {
            let mut hits = 0u32;
            for key in &access_keys {
                if cache.get(black_box(key)).is_some() {
                    hits += 1;
                }
            }
            let ratio = hits as f64 / access_keys.len() as f64;
            black_box(ratio)
        })
    });

    group.finish();
}

criterion_group! {
    name = tier1_hotpath_block_cache_hot;
    config = criterion_config_for_tier(BenchTier::Tier1Hot);
    targets = bench_block_cache_get_hot, bench_block_cache_insert_hot, bench_block_cache_hit_ratio_fast
}
criterion_main!(tier1_hotpath_block_cache_hot);
