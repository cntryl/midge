//! Tier 1 — Bloom filter hot path benchmarks
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers bloom filter hot paths:
//! - Hash computation and containment checks

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

// Simple mock bloom filter for hot path testing
struct MockBloomFilter {
    bits: Vec<bool>,
    hash_count: usize,
}

impl MockBloomFilter {
    fn new(size: usize, hash_count: usize) -> Self {
        Self {
            bits: vec![false; size],
            hash_count,
        }
    }

    fn add(&mut self, key: &[u8]) {
        for i in 0..self.hash_count {
            let hash = self.hash(key, i);
            let index = hash % self.bits.len();
            self.bits[index] = true;
        }
    }

    fn maybe_contains(&self, key: &[u8]) -> bool {
        for i in 0..self.hash_count {
            let hash = self.hash(key, i);
            let index = hash % self.bits.len();
            if !self.bits[index] {
                return false;
            }
        }
        true
    }

    fn hash(&self, key: &[u8], seed: usize) -> usize {
        // Simple hash for benchmarking
        let mut h = seed as u64;
        for &b in key {
            h = h.wrapping_mul(31).wrapping_add(b as u64);
        }
        h as usize
    }
}

fn make_test_key(i: usize) -> Bytes {
    Bytes::from(format!("key_{:010}", i))
}

/// Benchmark bloom filter containment check
fn bench_bloom_maybe_contains(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_bloom_maybe_contains");
    group.measurement_time(std::time::Duration::from_millis(200));

    let mut filter = MockBloomFilter::new(1024, 3);
    // Pre-populate with some keys
    for i in 0..100 {
        filter.add(&make_test_key(i));
    }

    group.bench_function("maybe_contains_hit", |b| {
        b.iter(|| {
            let result = filter.maybe_contains(&make_test_key(42));
            black_box(result);
        })
    });

    group.bench_function("maybe_contains_miss", |b| {
        b.iter(|| {
            let result = filter.maybe_contains(&make_test_key(1000)); // Not in filter
            black_box(result);
        })
    });

    group.finish();
}

/// Benchmark hash computation
fn bench_bloom_compute_hashes(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_bloom_compute_hashes");
    group.measurement_time(std::time::Duration::from_millis(200));

    let filter = MockBloomFilter::new(1024, 3);
    let key = make_test_key(42);

    group.bench_function("compute_3_hashes", |b| {
        b.iter(|| {
            let mut hashes = Vec::with_capacity(3);
            for i in 0..3 {
                hashes.push(filter.hash(&key, i));
            }
            black_box(hashes);
        })
    });

    group.finish();
}

/// Benchmark hot filter check (pre-computed hashes)
fn bench_bloom_filter_hot_check(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_bloom_filter_hot_check");
    group.measurement_time(std::time::Duration::from_millis(200));

    let mut filter = MockBloomFilter::new(1024, 3);
    filter.add(&make_test_key(42));

    // Pre-compute hashes for hot path
    let key = make_test_key(42);
    let hashes: Vec<usize> = (0..3).map(|i| filter.hash(&key, i)).collect();

    group.bench_function("hot_check_precomputed", |b| {
        b.iter(|| {
            let mut result = true;
            for &hash in &hashes {
                let index = hash % filter.bits.len();
                if !filter.bits[index] {
                    result = false;
                    break;
                }
            }
            black_box(result);
        })
    });

    group.finish();
}

criterion_group! {
    name = bloom_group;
    config = criterion_config();
    targets = bench_bloom_maybe_contains, bench_bloom_compute_hashes, bench_bloom_filter_hot_check
}
criterion_main!(bloom_group);