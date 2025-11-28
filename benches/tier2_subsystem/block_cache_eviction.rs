//! Tier 2 — Block Cache Eviction Benchmarks
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers block cache LRU eviction behavior under pressure

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::criterion_config;
use std::hint::black_box;

use cntryl_midge::sst::block_cache::BlockType;
use cntryl_midge::sst::{create_basic_cache, BlockKey, CachedBlock};

fn make_block_key(file_idx: usize, block_idx: usize) -> BlockKey {
    BlockKey {
        file_name: format!("file_{}.sst", file_idx),
        block_type: BlockType::Data,
        offset: (block_idx * 4096) as u64,
    }
}

fn make_cached_block(size: usize) -> CachedBlock {
    CachedBlock {
        data: bytes::Bytes::from(vec![0xAB; size]),
    }
}

/// Benchmark LRU eviction with 1k entries
/// Fills cache to capacity, then continues inserting to trigger evictions
fn bench_block_cache_lru_eviction_1k(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_block_cache_lru_eviction_1k");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1000));

    group.bench_function("evict_1k", |b| {
        b.iter(|| {
            // 512KB cache = ~125 blocks of 4KB each
            let cache = create_basic_cache(512 * 1024);

            // Fill cache to capacity (125 blocks)
            for i in 0..125 {
                let key = make_block_key(0, i);
                let block = make_cached_block(4096);
                cache.insert(key, block);
            }

            // Insert 1k more blocks, triggering evictions
            for i in 125..1125 {
                let key = make_block_key(0, i);
                let block = make_cached_block(4096);
                cache.insert(key, block);
            }

            black_box(cache);
        })
    });

    group.finish();
}

/// Benchmark LRU eviction with 10k entries
/// Larger-scale eviction stress test
fn bench_block_cache_lru_eviction_10k(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_block_cache_lru_eviction_10k");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(10_000));

    group.bench_function("evict_10k", |b| {
        b.iter(|| {
            // 2MB cache = ~500 blocks of 4KB each
            let cache = create_basic_cache(2 * 1024 * 1024);

            // Fill cache to capacity
            for i in 0..500 {
                let key = make_block_key(0, i);
                let block = make_cached_block(4096);
                cache.insert(key, block);
            }

            // Insert 10k more blocks, heavy eviction pressure
            for i in 500..10_500 {
                let key = make_block_key(0, i);
                let block = make_cached_block(4096);
                cache.insert(key, block);
            }

            black_box(cache);
        })
    });

    group.finish();
}

criterion_group! {
    name = tier2_subsystem_block_cache_eviction;
    config = criterion_config();
    targets = bench_block_cache_lru_eviction_1k, bench_block_cache_lru_eviction_10k
}
criterion_main!(tier2_subsystem_block_cache_eviction);
