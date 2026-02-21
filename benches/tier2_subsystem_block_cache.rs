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

#[path = "./criterion_config.rs"]
mod criterion_config;

use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_config::criterion_config_for_tier2;
use std::hint::black_box;

use cntryl_midge::sst::cache::{BlockCache, CacheKey, CachePolicyType};
use cntryl_midge::Bytes;

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Pre-computed block keys to avoid allocation in benchmarks.
struct PrecomputedKeys {
    keys: Vec<CacheKey>,
}

impl PrecomputedKeys {
    fn linear(count: usize) -> Self {
        let keys = (0..count)
            .map(|i| CacheKey::for_data(0, (i * 4096) as u64))
            .collect();
        Self { keys }
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
    config = criterion_config_for_tier2();
    targets =
        bench_hotset_rotation,
        bench_lru_eviction_10k
}
criterion_main!(tier2_subsystem_block_cache);
