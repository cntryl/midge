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
use cntryl_midge::sst::bloom::BloomFilterBuilder;
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::criterion_config;
use std::hint::black_box;

fn make_test_key(i: usize) -> Bytes {
    Bytes::from(format!("key_{:010}", i))
}

/// Benchmark building bloom filter with 10k keys
fn bench_bloom_build_10k_keys(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_bloom_build_10k_keys");
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_millis(500));

    let keys: Vec<Bytes> = (0..10_000).map(make_test_key).collect();
    group.throughput(Throughput::Elements(10_000));

    group.bench_function("build_10k_keys", |b| {
        b.iter(|| {
            // Use real BloomFilterBuilder with 10 bits per key (~1% FPR)
            let mut builder = BloomFilterBuilder::with_expected_keys(10_000, 10);
            for key in &keys {
                builder.add_key(key);
            }
            let filter = builder.finish();
            black_box(filter);
        })
    });

    group.finish();
}

/// Benchmark building bloom filter with 100k keys
fn bench_bloom_build_100k_keys(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_bloom_build_100k_keys");
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_millis(500));

    let keys: Vec<Bytes> = (0..100_000).map(make_test_key).collect();
    group.throughput(Throughput::Elements(100_000));

    group.bench_function("build_100k_keys", |b| {
        b.iter(|| {
            // Use real BloomFilterBuilder with 10 bits per key (~1% FPR)
            let mut builder = BloomFilterBuilder::with_expected_keys(100_000, 10);
            for key in &keys {
                builder.add_key(key);
            }
            let filter = builder.finish();
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
