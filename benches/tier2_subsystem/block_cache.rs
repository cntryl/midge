//! Tier 2 — Block Cache Subsystem Benchmarks
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers block cache subsystem operations:
//! - Eviction scanning and filling
//! - Hit ratio calculations

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::criterion_config;
use std::hint::black_box;

use cntryl_midge::sst::{create_basic_cache, BlockKey, CachedBlock};
use cntryl_midge::sst::block_cache::BlockType;

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

/// Benchmark block cache eviction scanning
fn bench_block_cache_eviction_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_block_cache_eviction_scan");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1000));

    group.bench_function("scan_1k_entries", |b| {
        b.iter_batched(
            || {
                let cache = create_basic_cache(1024 * 1024); // 1MB cache
                // Fill cache
                for i in 0..1000 {
                    let key = make_block_key(0, i);
                    let block = make_cached_block(4096);
                    cache.insert(key, block);
                }
                cache
            },
            |cache| {
                // Scan all entries (simulating eviction scan)
                let mut count = 0;
                for i in 0..1000 {
                    let key = make_block_key(0, i);
                    if cache.get(&key).is_some() {
                        count += 1;
                    }
                }
                black_box(count);
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

/// Benchmark filling cache then hitting
fn bench_block_cache_fill_then_hit(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_block_cache_fill_then_hit");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1000));

    group.bench_function("fill_100_then_hit_1000", |b| {
        b.iter_batched(
            || {
                let cache = create_basic_cache(1024 * 1024); // 1MB cache
                // Fill with initial data
                for i in 0..100 {
                    let key = make_block_key(0, i);
                    let block = make_cached_block(4096);
                    cache.insert(key, block);
                }
                cache
            },
            |cache| {
                // Hit existing entries and add new ones (causing evictions)
                let mut hits = 0;
                for i in 0..1000 {
                    let key = make_block_key(0, i % 150); // Mix of hits and new entries
                    if cache.get(&key).is_some() {
                        hits += 1;
                    } else {
                        let new_key = make_block_key(1, i);
                        let block = make_cached_block(4096);
                        cache.insert(new_key, block);
                    }
                }
                black_box(hits);
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

/// Benchmark hot set rotation
fn bench_block_cache_hotset_rotation(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_block_cache_hotset_rotation");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(500));

    group.bench_function("rotate_hotset_50_entries", |b| {
        b.iter_batched(
            || {
                let cache = create_basic_cache(1024 * 1024); // 1MB cache
                // Establish hot set
                for i in 0..50 {
                    let key = make_block_key(0, i);
                    let block = make_cached_block(4096);
                    cache.insert(key, block);
                }
                cache
            },
            |cache| {
                // Rotate hot set - access some, evict others
                for round in 0..10 {
                    for i in 0..50 {
                        let key = make_block_key(0, (i + round) % 75); // Rotate through 75 possible keys
                        if cache.get(&key).is_none() {
                            let block = make_cached_block(4096);
                            cache.insert(key, block);
                        }
                    }
                }
                black_box(cache.stats().entry_count);
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group! {
    name = block_cache_group;
    config = criterion_config();
    targets = bench_block_cache_eviction_scan, bench_block_cache_fill_then_hit, bench_block_cache_hotset_rotation
}
criterion_main!(block_cache_group);