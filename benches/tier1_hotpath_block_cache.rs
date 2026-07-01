//! Tier 1 — Hot Path Block Cache Benchmarks
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers critical block cache hot paths:
//! - Single hot key lookups (`get_hot`)
//! - Batch lookups for hits and misses
//! - Insert operations (single and batch)
//! - Eviction under memory pressure

#[path = "./criterion_config.rs"]
mod criterion_config;

use cntryl_midge::sst::cache::{BlockCache, CacheKey, CachePolicyType};
use cntryl_midge::Bytes;
use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_config::criterion_config_for_tier1;
use std::hint::black_box;

const INSERT_BATCH_ROUNDS: usize = 4;

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Pre-create a cache key with SST file 1.
#[inline]
fn make_cache_key(block_offset: u64) -> CacheKey {
    CacheKey::for_data(1, block_offset)
}

/// Pre-create a cache key with specified SST file.
#[inline]
fn make_cache_key_with_sst(sst_id: u64, block_offset: u64) -> CacheKey {
    CacheKey::for_data(sst_id, block_offset)
}

/// Pre-allocated block data to avoid allocation in benchmark hot path.
fn make_block_data(size: usize) -> Bytes {
    Bytes::from(vec![0xAB; size])
}

fn create_cache(capacity: u64) -> BlockCache {
    BlockCache::new(capacity, 16, CachePolicyType::Lru)
}

fn precompute_keys_and_blocks(num_blocks: usize, block_size: usize) -> (Vec<CacheKey>, Vec<Bytes>) {
    let mut keys = Vec::with_capacity(num_blocks);
    let mut blocks = Vec::with_capacity(num_blocks);
    for i in 0..num_blocks {
        let key = make_cache_key_with_sst((i / 100) as u64, (i % 100) as u64 * block_size as u64);
        let block = make_block_data(block_size);
        keys.push(key);
        blocks.push(block);
    }
    (keys, blocks)
}

// ─── Single-Key Benchmarks (Hot Path) ────────────────────────────────────────

/// Benchmark single hot key lookup (critical read path).
fn bench_get_hot_single(c: &mut Criterion) {
    let mut group = c.benchmark_group("block_cache/get_hot_single");
    group.measurement_time(std::time::Duration::from_millis(200));
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let cache = create_cache(10 * 1024 * 1024); // 10MB cache

    // Pre-populate with hot data
    for i in 0..1000 {
        let key = make_cache_key(i * 4096);
        let block = make_block_data(4096);
        cache.put(key, &block);
    }

    // Precompute hot key (no allocation in hot path)
    let hot_key = make_cache_key(42 * 4096);

    group.bench_function("4k_block", |b| {
        b.iter(|| black_box(cache.get(black_box(&hot_key))));
    });

    group.finish();
}

/// Benchmark single-block insert into warm cache.
fn bench_insert_single(c: &mut Criterion) {
    let mut group = c.benchmark_group("block_cache/insert_single");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let cache = create_cache(100 * 1024 * 1024); // 100MB cache

    // Pre-populate to simulate realistic warm cache state.
    for i in 0..100 {
        let key = make_cache_key(i * 4096);
        let block = make_block_data(4096);
        cache.put(key, &block);
    }

    // Rotate through a bounded keyset so the benchmark stays below cache
    // capacity and does not drift into eviction-heavy behavior mid-run.
    let insert_keys: Vec<CacheKey> = (0u64..4096)
        .map(|i| make_cache_key((1000 + i) * 4096))
        .collect();
    let block_data = make_block_data(4096);
    let mut key_index = 0usize;

    group.bench_function("4k_block", |b| {
        b.iter(|| {
            let key = insert_keys[key_index % insert_keys.len()];
            key_index = key_index.wrapping_add(1);
            cache.put(black_box(key), black_box(&block_data));
        });
    });

    group.finish();
}

// ─── Batch Benchmarks ────────────────────────────────────────────────────────

/// Benchmark batch cache get operations (all hits).
fn bench_get_batch_hit(c: &mut Criterion) {
    let mut group = c.benchmark_group("block_cache/get_batch_hit");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1000));

    let cache_size = 10 * 1024 * 1024;
    let block_size = 4 * 1024;
    let num_blocks = 1000;

    let (keys, blocks) = precompute_keys_and_blocks(num_blocks, block_size);

    // Pre-populate cache
    let cache = create_cache(cache_size);
    for i in 0..num_blocks {
        cache.put(keys[i], &blocks[i]);
    }

    group.bench_function("1000_lookups", |b| {
        b.iter(|| {
            let mut count = 0;
            for key in keys.iter().take(num_blocks) {
                if cache.get(key).is_some() {
                    count += 1;
                }
            }
            black_box(count)
        });
    });

    group.finish();
}

/// Benchmark batch cache get operations (all misses).
fn bench_get_batch_miss(c: &mut Criterion) {
    let mut group = c.benchmark_group("block_cache/get_batch_miss");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1000));

    let cache_size = 10 * 1024 * 1024;
    let block_size = 4 * 1024;
    let num_blocks = 1000;

    let (keys, blocks) = precompute_keys_and_blocks(num_blocks, block_size);

    // Pre-populate cache with keys in SST range 0-9
    let cache = create_cache(cache_size);
    for i in 0..num_blocks {
        cache.put(keys[i], &blocks[i]);
    }

    // Create miss keys in different SST range (100-199)
    let miss_keys: Vec<CacheKey> = (0..num_blocks)
        .map(|i| {
            make_cache_key_with_sst((100 + i / 100) as u64, (i % 100) as u64 * block_size as u64)
        })
        .collect();

    group.bench_function("1000_lookups", |b| {
        b.iter(|| {
            let mut count = 0;
            for key in &miss_keys {
                if cache.get(black_box(key)).is_some() {
                    count += 1;
                }
            }
            black_box(count)
        });
    });

    group.finish();
}

/// Benchmark batch insert with varying counts.
fn bench_insert_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("block_cache/insert_batch");
    group.sampling_mode(SamplingMode::Flat);

    let cache_size = 10 * 1024 * 1024; // 10 MB
    let block_size = 4 * 1024; // 4 KB

    let num_blocks = 100;
    let (keys, blocks) = precompute_keys_and_blocks(num_blocks, block_size);
    group.throughput(Throughput::Elements(
        (num_blocks * INSERT_BATCH_ROUNDS) as u64,
    ));

    group.bench_with_input(
        BenchmarkId::from_parameter(num_blocks),
        &num_blocks,
        |b, &n| {
            b.iter_batched(
                || create_cache(cache_size),
                |cache| {
                    for round in 0..INSERT_BATCH_ROUNDS {
                        for i in 0..n {
                            let idx = (i + round) % n;
                            cache.put(keys[idx], &blocks[idx]);
                        }
                    }
                    black_box(());
                },
                BatchSize::SmallInput,
            );
        },
    );

    group.finish();
}

// ─── Criterion Setup ─────────────────────────────────────────────────────────

criterion_group! {
    name = tier1_hotpath_block_cache;
    config = criterion_config_for_tier1();
    targets =
        bench_get_hot_single,
        bench_insert_single,
        bench_get_batch_hit,
        bench_get_batch_miss,
        bench_insert_batch
}
criterion_main!(tier1_hotpath_block_cache);
