//! Tier 1 — Hot Path Cache Benchmarks
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers block cache operations (critical read path)

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use cntryl_midge::sst::{create_basic_cache, BlockKey, CacheBlockType, CachedBlock};
use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;

fn make_block_key(file_num: usize, offset: u64) -> BlockKey {
    BlockKey {
        file_name: format!("sst_{:06}.sst", file_num),
        block_type: CacheBlockType::Data,
        offset,
    }
}

fn make_cached_block(size: usize) -> CachedBlock {
    let data = vec![0u8; size];
    CachedBlock {
        data: Bytes::from(data),
    }
}

fn precompute_keys_and_blocks(
    num_blocks: usize,
    block_size: usize,
) -> (Vec<BlockKey>, Vec<CachedBlock>) {
    let mut keys = Vec::with_capacity(num_blocks);
    let mut blocks = Vec::with_capacity(num_blocks);
    for i in 0..num_blocks {
        let key = make_block_key(i / 100, (i % 100) as u64 * block_size as u64);
        let block = make_cached_block(block_size);
        keys.push(key);
        blocks.push(block);
    }
    (keys, blocks)
}

/// Benchmark cache insert operations
fn bench_cache_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_cache_insert");
    group.sampling_mode(SamplingMode::Flat);

    let cache_size = 10 * 1024 * 1024; // 10 MB
    let block_size = 4 * 1024; // 4 KB

    for &num_blocks in &[100, 1_000] {
        // Precompute keys and blocks outside the loop
        let (keys, blocks) = precompute_keys_and_blocks(num_blocks, block_size);
        group.throughput(Throughput::Elements(num_blocks as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(num_blocks),
            &num_blocks,
            |b, &n| {
                b.iter_batched(
                    || create_basic_cache(cache_size),
                    |cache| {
                        for i in 0..n {
                            cache.insert(keys[i].clone(), blocks[i].clone());
                        }
                        black_box(());
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Benchmark cache get operations (hot path for every read)
fn bench_cache_get_hit(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_cache_get_hit");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1000));

    let cache_size = 10 * 1024 * 1024;
    let block_size = 4 * 1024;
    let num_blocks = 1000;

    // Precompute keys and blocks
    let (keys, blocks) = precompute_keys_and_blocks(num_blocks, block_size);

    // Pre-populate cache
    let cache = create_basic_cache(cache_size);
    for i in 0..num_blocks {
        cache.insert(keys[i].clone(), blocks[i].clone());
    }

    group.bench_function("get_hit", |b| {
        b.iter(|| {
            let mut count = 0;
            for key in keys.iter().take(num_blocks) {
                if cache.get(key).is_some() {
                    count += 1;
                }
            }
            black_box(count);
        })
    });

    group.finish();
}

/// Benchmark cache get operations on missing keys (cache misses)
fn bench_cache_get_miss(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_cache_get_miss");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1000));

    let cache_size = 10 * 1024 * 1024;
    let block_size = 4 * 1024;
    let num_blocks = 1000;

    // Precompute keys and blocks for population (file 0-9)
    let (keys, blocks) = precompute_keys_and_blocks(num_blocks, block_size);

    // Pre-populate cache with keys 0..1000
    let cache = create_basic_cache(cache_size);
    for i in 0..num_blocks {
        cache.insert(keys[i].clone(), blocks[i].clone());
    }

    // Create miss keys that DON'T exist in the cache (different file range: 100-199)
    let miss_keys: Vec<BlockKey> = (0..num_blocks)
        .map(|i| make_block_key(100 + i / 100, (i % 100) as u64 * block_size as u64))
        .collect();

    group.bench_function("get_miss", |b| {
        b.iter(|| {
            let mut count = 0;
            for key in &miss_keys {
                if cache.get(black_box(key)).is_some() {
                    count += 1;
                }
            }
            black_box(count)
        })
    });

    group.finish();
}

/// Benchmark cache eviction under memory pressure
fn bench_cache_eviction(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_cache_eviction");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1000));

    // Small cache to trigger eviction (2 MB, holds ~512 4KB blocks)
    let cache_size = 2 * 1024 * 1024;
    let block_size = 4 * 1024;
    let num_blocks = 1000;

    // Precompute keys and blocks
    let (keys, blocks) = precompute_keys_and_blocks(num_blocks, block_size);

    // Insert more blocks than cache can hold
    group.bench_function("evict_under_pressure", |b| {
        b.iter(|| {
            let cache = create_basic_cache(cache_size);
            // Try to insert 1000 blocks when only ~512 fit
            for i in 0..num_blocks {
                cache.insert(keys[i].clone(), blocks[i].clone());
            }
            black_box(cache);
        })
    });

    group.finish();
}

// Note: Concurrent cache benchmark moved to tier2 (thread spawning too expensive for tier1)

criterion_group! {
    name = tier1_hotpath_cache;
    config = criterion_config_for_tier(BenchTier::Tier1Hot);
    targets = bench_cache_insert, bench_cache_get_hit, bench_cache_get_miss, bench_cache_eviction
}
criterion_main!(tier1_hotpath_cache);
