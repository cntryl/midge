//! Tier 2 — Block Cache Subsystem Benchmarks
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers block cache subsystem operations:
//! - Eviction scanning and filling
//! - Hit ratio calculations
//! - Hot set rotation patterns

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::criterion_config;
use std::hint::black_box;

use cntryl_midge::sst::block_cache::BlockType;
use cntryl_midge::sst::{create_basic_cache, BlockKey, CachedBlock};

/// Pre-computed block keys to avoid allocation in benchmarks.
struct PrecomputedKeys {
    keys: Vec<BlockKey>,
}

impl PrecomputedKeys {
    fn new(file_count: usize, blocks_per_file: usize) -> Self {
        let mut keys = Vec::with_capacity(file_count * blocks_per_file);
        for file_idx in 0..file_count {
            let file_name = format!("file_{}.sst", file_idx);
            for block_idx in 0..blocks_per_file {
                keys.push(BlockKey {
                    file_name: file_name.clone(),
                    block_type: BlockType::Data,
                    offset: (block_idx * 4096) as u64,
                });
            }
        }
        Self { keys }
    }

    #[inline]
    fn get(&self, file_idx: usize, block_idx: usize, blocks_per_file: usize) -> &BlockKey {
        &self.keys[file_idx * blocks_per_file + block_idx]
    }

    #[inline]
    fn get_linear(&self, idx: usize) -> &BlockKey {
        &self.keys[idx]
    }
}

/// Pre-allocated block data to avoid allocation in benchmarks.
fn make_cached_block_static() -> CachedBlock {
    // Use static data - Bytes::from_static is zero-copy
    static BLOCK_DATA: [u8; 4096] = [0xAB; 4096];
    CachedBlock {
        data: bytes::Bytes::from_static(&BLOCK_DATA),
    }
}

/// Benchmark block cache eviction scanning
fn bench_block_cache_eviction_scan(c: &mut Criterion) {
    // Pre-compute keys outside the benchmark
    let keys = PrecomputedKeys::new(1, 1000);
    let block = make_cached_block_static();

    let mut group = c.benchmark_group("subsystem_block_cache_eviction_scan");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1000));

    group.bench_function("scan_1k_entries", |b| {
        b.iter_batched(
            || {
                let cache = create_basic_cache(5 * 1024 * 1024); // 5MB to hold all 1000 x 4KB blocks
                                                                 // Fill cache with pre-computed keys
                for i in 0..1000 {
                    cache.insert(keys.get_linear(i).clone(), block.clone());
                }
                cache
            },
            |cache| {
                // Scan all entries (simulating eviction scan)
                let mut count = 0u32;
                for i in 0..1000 {
                    if cache.get(keys.get_linear(i)).is_some() {
                        count += 1;
                    }
                }
                black_box(count)
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

/// Benchmark filling cache then hitting
fn bench_block_cache_fill_then_hit(c: &mut Criterion) {
    // Pre-compute keys for 2 files, 1000 blocks each
    let keys = PrecomputedKeys::new(2, 1000);
    let block = make_cached_block_static();

    let mut group = c.benchmark_group("subsystem_block_cache_fill_then_hit");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1000));

    group.bench_function("fill_100_then_hit_1000", |b| {
        b.iter_batched(
            || {
                let cache = create_basic_cache(1024 * 1024); // 1MB cache
                                                             // Fill with initial 100 blocks from file 0
                for i in 0..100 {
                    cache.insert(keys.get(0, i, 1000).clone(), block.clone());
                }
                cache
            },
            |cache| {
                // Hit existing entries and add new ones (causing evictions)
                let mut hits = 0u32;
                for i in 0..1000 {
                    let key = keys.get(0, i % 150, 1000);
                    if cache.get(key).is_some() {
                        hits += 1;
                    } else {
                        // Insert from file 1 to avoid key collision
                        cache.insert(keys.get(1, i, 1000).clone(), block.clone());
                    }
                }
                black_box(hits)
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

/// Benchmark hot set rotation
fn bench_block_cache_hotset_rotation(c: &mut Criterion) {
    // Pre-compute keys: need indices 0..74 for the rotation pattern
    let keys = PrecomputedKeys::new(1, 100);
    let block = make_cached_block_static();

    let mut group = c.benchmark_group("subsystem_block_cache_hotset_rotation");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(500)); // 10 rounds * 50 ops

    group.bench_function("rotate_hotset_50_entries", |b| {
        b.iter_batched(
            || {
                let cache = create_basic_cache(1024 * 1024); // 1MB cache
                                                             // Establish initial hot set (indices 0..49)
                for i in 0..50 {
                    cache.insert(keys.get_linear(i).clone(), block.clone());
                }
                cache
            },
            |cache| {
                // Rotate hot set - access some, evict others
                for round in 0..10 {
                    for i in 0..50 {
                        let key = keys.get_linear((i + round) % 75);
                        if cache.get(key).is_none() {
                            cache.insert(key.clone(), block.clone());
                        }
                    }
                }
                black_box(cache.stats().entry_count)
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group! {
    name = tier2_subsystem_block_cache;
    config = criterion_config();
    targets = bench_block_cache_eviction_scan, bench_block_cache_fill_then_hit, bench_block_cache_hotset_rotation
}
criterion_main!(tier2_subsystem_block_cache);
