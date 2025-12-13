//! Tier 2 — Block Cache Subsystem Benchmarks
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers block cache subsystem operations:
//! - Eviction scanning and filling
//! - Hit ratio calculations
//! - Hot set rotation patterns
//! - LRU eviction under pressure (1k, 10k entries)

#[path = "./criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;

use bytes::Bytes;
use cntryl_midge::sst::cache::{BlockCache, CacheKey, CachePolicyType};

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Pre-computed block keys to avoid allocation in benchmarks.
struct PrecomputedKeys {
    keys: Vec<CacheKey>,
}

impl PrecomputedKeys {
    fn new(file_count: usize, blocks_per_file: usize) -> Self {
        let mut keys = Vec::with_capacity(file_count * blocks_per_file);
        for file_idx in 0..file_count {
            for block_idx in 0..blocks_per_file {
                keys.push(CacheKey::new(file_idx as u64, (block_idx * 4096) as u64));
            }
        }
        Self { keys }
    }

    fn linear(count: usize) -> Self {
        let keys = (0..count)
            .map(|i| CacheKey::new(0, (i * 4096) as u64))
            .collect();
        Self { keys }
    }

    #[inline]
    fn get(&self, file_idx: usize, block_idx: usize, blocks_per_file: usize) -> CacheKey {
        self.keys[file_idx * blocks_per_file + block_idx]
    }

    #[inline]
    fn get_linear(&self, idx: usize) -> CacheKey {
        self.keys[idx]
    }
}

/// Pre-allocated block data to avoid allocation in benchmarks.
fn make_block_data_static() -> Bytes {
    Bytes::from_static(&[0xAB; 4096])
}

fn create_cache(capacity: u64) -> BlockCache {
    BlockCache::new(capacity, 16, CachePolicyType::Lru)
}

// ─── Eviction Scan Benchmarks ────────────────────────────────────────────────

/// Benchmark block cache eviction scanning
fn bench_eviction_scan(c: &mut Criterion) {
    let keys = PrecomputedKeys::linear(1000);
    let block = make_block_data_static();

    let mut group = c.benchmark_group("block_cache/eviction_scan");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1000));

    group.bench_function("1k_entries", |b| {
        b.iter_batched(
            || {
                let cache = create_cache(5 * 1024 * 1024); // 5MB to hold all 1000 x 4KB blocks
                for i in 0..1000 {
                    cache.put(keys.get_linear(i), block.clone());
                }
                cache
            },
            |cache| {
                let mut count = 0u32;
                for i in 0..1000 {
                    if cache.get(&keys.get_linear(i)).is_some() {
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

// ─── Fill Then Hit Benchmarks ────────────────────────────────────────────────

/// Benchmark filling cache then hitting
fn bench_fill_then_hit(c: &mut Criterion) {
    let keys = PrecomputedKeys::new(2, 1000);
    let block = make_block_data_static();

    let mut group = c.benchmark_group("block_cache/fill_then_hit");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1000));

    group.bench_function("fill_100_hit_1000", |b| {
        b.iter_batched(
            || {
                let cache = create_cache(1024 * 1024); // 1MB cache
                for i in 0..100 {
                    cache.put(keys.get(0, i, 1000), block.clone());
                }
                cache
            },
            |cache| {
                let mut hits = 0u32;
                for i in 0..1000 {
                    let key = keys.get(0, i % 150, 1000);
                    if cache.get(&key).is_some() {
                        hits += 1;
                    } else {
                        cache.put(keys.get(1, i, 1000), block.clone());
                    }
                }
                black_box(hits)
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

// ─── Hot Set Rotation Benchmarks ─────────────────────────────────────────────

/// Benchmark hot set rotation
fn bench_hotset_rotation(c: &mut Criterion) {
    let keys = PrecomputedKeys::linear(100);
    let block = make_block_data_static();

    let mut group = c.benchmark_group("block_cache/hotset_rotation");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(500)); // 10 rounds * 50 ops

    group.bench_function("rotate_50_entries", |b| {
        b.iter_batched(
            || {
                let cache = create_cache(1024 * 1024); // 1MB cache
                for i in 0..50 {
                    cache.put(keys.get_linear(i), block.clone());
                }
                cache
            },
            |cache| {
                for round in 0..10 {
                    for i in 0..50 {
                        let key = keys.get_linear((i + round) % 75);
                        if cache.get(&key).is_none() {
                            cache.put(key, block.clone());
                        }
                    }
                }
                black_box(())
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

// ─── LRU Eviction Benchmarks ─────────────────────────────────────────────────

/// Benchmark LRU eviction with 1k entries
fn bench_lru_eviction_1k(c: &mut Criterion) {
    let keys = PrecomputedKeys::linear(1125);
    let block = make_block_data_static();

    let mut group = c.benchmark_group("block_cache/lru_eviction");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1000));

    group.bench_function("evict_1k", |b| {
        b.iter_batched(
            || {
                let cache = create_cache(512 * 1024); // 512KB holds ~125 blocks
                for i in 0..125 {
                    cache.put(keys.get_linear(i), block.clone());
                }
                cache
            },
            |cache| {
                for i in 125..1125 {
                    cache.put(keys.get_linear(i), block.clone());
                }
                black_box(cache)
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

/// Benchmark LRU eviction with 10k entries
fn bench_lru_eviction_10k(c: &mut Criterion) {
    let keys = PrecomputedKeys::linear(10_500);
    let block = make_block_data_static();

    let mut group = c.benchmark_group("block_cache/lru_eviction");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(10_000));

    group.bench_function("evict_10k", |b| {
        b.iter_batched(
            || {
                let cache = create_cache(2 * 1024 * 1024); // 2MB holds ~500 blocks
                for i in 0..500 {
                    cache.put(keys.get_linear(i), block.clone());
                }
                cache
            },
            |cache| {
                for i in 500..10_500 {
                    cache.put(keys.get_linear(i), block.clone());
                }
                black_box(cache)
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

// ─── Criterion Setup ─────────────────────────────────────────────────────────

criterion_group! {
    name = tier2_subsystem_block_cache;
    config = criterion_config_for_tier(BenchTier::Tier2Subsystem);
    targets =
        bench_eviction_scan,
        bench_fill_then_hit,
        bench_hotset_rotation,
        bench_lru_eviction_1k,
        bench_lru_eviction_10k
}
criterion_main!(tier2_subsystem_block_cache);
