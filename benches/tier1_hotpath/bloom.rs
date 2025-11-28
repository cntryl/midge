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
use cntryl_midge::sst::bloom::BloomFilterBuilder;
use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

fn make_test_key(i: usize) -> Bytes {
    Bytes::from(format!("key_{:010}", i))
}

/// Benchmark bloom filter containment check
fn bench_bloom_maybe_contains(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_bloom_maybe_contains");
    group.measurement_time(std::time::Duration::from_millis(200));

    let mut builder = BloomFilterBuilder::with_bits_per_key(10);
    // Pre-populate with some keys
    for i in 0..100 {
        builder.add_key(&make_test_key(i));
    }
    let filter = builder.finish();

    // Precompute keys
    let hit_key = make_test_key(42);
    let miss_key = make_test_key(1000);

    group.bench_function("maybe_contains_hit", |b| {
        b.iter(|| {
            let result = filter.may_contain(&hit_key);
            black_box(result);
        })
    });

    group.bench_function("maybe_contains_miss", |b| {
        b.iter(|| {
            let result = filter.may_contain(&miss_key);
            black_box(result);
        })
    });

    group.finish();
}

/// Benchmark hash computation (via may_contain on miss)
fn bench_bloom_compute_hashes(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_bloom_compute_hashes");
    group.measurement_time(std::time::Duration::from_millis(200));

    let mut builder = BloomFilterBuilder::with_bits_per_key(10);
    for i in 0..100 {
        builder.add_key(&make_test_key(i));
    }
    let filter = builder.finish();

    // Precompute miss key
    let miss_key = make_test_key(1000);

    group.bench_function("compute_hashes_via_miss", |b| {
        b.iter(|| {
            let result = filter.may_contain(&miss_key); // Miss, involves hashing
            black_box(result);
        })
    });

    group.finish();
}

/// Benchmark hot filter check (may_contain on hit)
fn bench_bloom_filter_hot_check(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_bloom_filter_hot_check");
    group.measurement_time(std::time::Duration::from_millis(200));

    let mut builder = BloomFilterBuilder::with_bits_per_key(10);
    builder.add_key(&make_test_key(42));
    let filter = builder.finish();

    // Precompute hit key
    let hit_key = make_test_key(42);

    group.bench_function("hot_check_hit", |b| {
        b.iter(|| {
            let result = filter.may_contain(&hit_key);
            black_box(result);
        })
    });

    group.finish();
}

criterion_group! {
    name = tier1_hotpath_bloom;
    config = criterion_config();
    targets = bench_bloom_maybe_contains, bench_bloom_compute_hashes, bench_bloom_filter_hot_check
}
criterion_main!(tier1_hotpath_bloom);
