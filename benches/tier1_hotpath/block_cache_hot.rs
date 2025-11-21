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

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

// Mock block cache for hot path testing
struct MockBlockCache {
    data: std::collections::HashMap<u64, Bytes>,
}

impl MockBlockCache {
    fn new() -> Self {
        Self {
            data: std::collections::HashMap::new(),
        }
    }

    fn get(&self, key: u64) -> Option<&Bytes> {
        self.data.get(&key)
    }

    fn insert(&mut self, key: u64, value: Bytes) {
        self.data.insert(key, value);
    }
}

fn make_block_data(size: usize) -> Bytes {
    Bytes::from(vec![0xAB; size])
}

/// Benchmark block cache get operations on hot data
fn bench_block_cache_get_hot(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_block_cache_get_hot");
    group.measurement_time(std::time::Duration::from_millis(200));

    let mut cache = MockBlockCache::new();
    // Pre-populate with hot data
    for i in 0..1000 {
        cache.insert(i, make_block_data(4096));
    }

    group.bench_function("get_hot_4k_block", |b| {
        b.iter(|| {
            let result = cache.get(42);
            black_box(result);
        })
    });

    group.finish();
}

/// Benchmark block cache insert operations
fn bench_block_cache_insert_hot(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_block_cache_insert_hot");
    group.measurement_time(std::time::Duration::from_millis(200));

    group.bench_function("insert_4k_block", |b| {
        b.iter_batched(
            || {
                let mut cache = MockBlockCache::new();
                // Pre-populate to simulate hot cache
                for i in 0..100 {
                    cache.insert(i, make_block_data(4096));
                }
                cache
            },
            |mut cache| {
                let key = 1000 + (cache.data.len() as u64);
                cache.insert(key, make_block_data(4096));
                black_box(&cache);
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

/// Benchmark hit ratio calculation (simplified)
fn bench_block_cache_hit_ratio_fast(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_block_cache_hit_ratio_fast");
    group.measurement_time(std::time::Duration::from_millis(200));

    let mut cache = MockBlockCache::new();
    // Pre-populate
    for i in 0..100 {
        cache.insert(i, make_block_data(1024));
    }

    let mut hits = 0;
    let mut total = 0;

    group.bench_function("hit_ratio_calc_100_accesses", |b| {
        b.iter(|| {
            for i in 0..100 {
                total += 1;
                if cache.get(i % 100).is_some() {
                    hits += 1;
                }
            }
            let ratio = hits as f64 / total as f64;
            black_box(ratio);
        })
    });

    group.finish();
}

criterion_group! {
    name = block_cache_hot_group;
    config = criterion_config();
    targets = bench_block_cache_get_hot, bench_block_cache_insert_hot, bench_block_cache_hit_ratio_fast
}
criterion_main!(block_cache_hot_group);