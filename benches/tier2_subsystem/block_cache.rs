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

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::collections::HashMap;
use std::hint::black_box;

// Mock block cache for subsystem testing
struct MockBlockCache {
    data: HashMap<u64, Bytes>,
    capacity: usize,
    access_count: u64,
}

impl MockBlockCache {
    fn new(capacity: usize) -> Self {
        Self {
            data: HashMap::new(),
            capacity,
            access_count: 0,
        }
    }

    fn get(&mut self, key: u64) -> Option<&Bytes> {
        self.access_count += 1;
        self.data.get(&key)
    }

    fn insert(&mut self, key: u64, value: Bytes) -> bool {
        if self.data.len() >= self.capacity && !self.data.contains_key(&key) {
            // Simulate eviction - remove oldest entry
            if let Some(oldest_key) = self.data.keys().next().cloned() {
                self.data.remove(&oldest_key);
            }
        }
        self.data.insert(key, value);
        true
    }

    fn len(&self) -> usize {
        self.data.len()
    }
}

fn make_block_data(size: usize) -> Bytes {
    Bytes::from(vec![0xAB; size])
}

/// Benchmark block cache eviction scanning
fn bench_block_cache_eviction_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_block_cache_eviction_scan");
    group.measurement_time(std::time::Duration::from_millis(500));

    group.bench_function("scan_1k_entries", |b| {
        b.iter_batched(
            || {
                let mut cache = MockBlockCache::new(1000);
                // Fill cache
                for i in 0..1000 {
                    cache.insert(i, make_block_data(4096));
                }
                cache
            },
            |mut cache| {
                // Scan all entries (simulating eviction scan)
                let mut count = 0;
                for i in 0..1000 {
                    if cache.get(i).is_some() {
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
    group.measurement_time(std::time::Duration::from_millis(500));

    group.bench_function("fill_100_then_hit_1000", |b| {
        b.iter_batched(
            || {
                let mut cache = MockBlockCache::new(200);
                // Fill with initial data
                for i in 0..100 {
                    cache.insert(i, make_block_data(4096));
                }
                cache
            },
            |mut cache| {
                // Hit existing entries and add new ones (causing evictions)
                let mut hits = 0;
                for i in 0..1000 {
                    let key = i % 150; // Mix of hits and new entries
                    if cache.get(key).is_some() {
                        hits += 1;
                    } else {
                        cache.insert(key + 1000, make_block_data(4096));
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
    group.measurement_time(std::time::Duration::from_millis(500));

    group.bench_function("rotate_hotset_50_entries", |b| {
        b.iter_batched(
            || {
                let mut cache = MockBlockCache::new(100);
                // Establish hot set
                for i in 0..50 {
                    cache.insert(i, make_block_data(4096));
                }
                cache
            },
            |mut cache| {
                // Rotate hot set - access some, evict others
                for round in 0..10 {
                    for i in 0..50 {
                        let key = (i + round) % 75; // Rotate through 75 possible keys
                        if cache.get(key).is_none() {
                            cache.insert(key, make_block_data(4096));
                        }
                    }
                }
                black_box(cache.len());
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