//! Tier 2 — Bloom Build Benchmarks
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers bloom filter building operations:
//! - Building filters with different key counts

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use criterion_helper::criterion_config;
use std::hint::black_box;

// Mock bloom filter for subsystem testing
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

    fn hash(&self, key: &[u8], seed: usize) -> usize {
        let mut h = seed as u64;
        for &b in key {
            h = h.wrapping_mul(31).wrapping_add(b as u64);
        }
        h as usize
    }

    fn build_from_keys(keys: &[Bytes], bits_per_key: f64) -> Self {
        let optimal_size = ((keys.len() as f64) * bits_per_key / std::f64::consts::LN_2) as usize;
        let hash_count = ((bits_per_key / std::f64::consts::LN_2) as usize).max(1);

        let mut filter = Self::new(optimal_size, hash_count);
        for key in keys {
            filter.add(key);
        }
        filter
    }
}

fn make_test_key(i: usize) -> Bytes {
    Bytes::from(format!("key_{:010}", i))
}

/// Benchmark building bloom filter with 10k keys
fn bench_bloom_build_10k_keys(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_bloom_build_10k_keys");
    group.measurement_time(std::time::Duration::from_millis(500));

    let keys: Vec<Bytes> = (0..10_000).map(make_test_key).collect();
    group.throughput(Throughput::Elements(10_000));

    group.bench_function("build_10k_keys", |b| {
        b.iter(|| {
            let filter = MockBloomFilter::build_from_keys(&keys, 10.0);
            black_box(filter);
        })
    });

    group.finish();
}

/// Benchmark building bloom filter with 100k keys
fn bench_bloom_build_100k_keys(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_bloom_build_100k_keys");
    group.measurement_time(std::time::Duration::from_millis(500));

    let keys: Vec<Bytes> = (0..100_000).map(make_test_key).collect();
    group.throughput(Throughput::Elements(100_000));

    group.bench_function("build_100k_keys", |b| {
        b.iter(|| {
            let filter = MockBloomFilter::build_from_keys(&keys, 10.0);
            black_box(filter);
        })
    });

    group.finish();
}

criterion_group! {
    name = bloom_build_group;
    config = criterion_config();
    targets = bench_bloom_build_10k_keys, bench_bloom_build_100k_keys
}
criterion_main!(bloom_build_group);