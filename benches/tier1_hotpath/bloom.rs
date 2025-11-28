//! Tier 1 — Bloom filter hot path benchmarks
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers bloom filter hot paths:
//! - Hash computation and containment checks
//! - Single key lookups (hit/miss)

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use cntryl_midge::sst::bloom::BloomFilterBuilder;
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::criterion_config;
use std::hint::black_box;

/// Benchmark bloom filter containment check (hit vs miss)
fn bench_bloom_maybe_contains(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_bloom_maybe_contains");
    group.measurement_time(std::time::Duration::from_millis(200));
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    // Build filter once outside benchmark loop
    let mut builder = BloomFilterBuilder::with_bits_per_key(10);
    let keys: Vec<Bytes> = (0..100)
        .map(|i| Bytes::from(format!("key_{:010}", i)))
        .collect();
    for key in &keys {
        builder.add_key(key);
    }
    let filter = builder.finish();

    // Precompute keys (avoid allocation in hot path)
    let hit_key = keys[42].clone();
    let miss_key = Bytes::from_static(b"key_00001000");

    group.bench_function("maybe_contains_hit", |b| {
        b.iter(|| black_box(filter.may_contain(black_box(&hit_key))))
    });

    group.bench_function("maybe_contains_miss", |b| {
        b.iter(|| black_box(filter.may_contain(black_box(&miss_key))))
    });

    group.finish();
}

/// Benchmark bloom filter batch lookups (realistic access pattern)
fn bench_bloom_batch_lookups(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_bloom_batch_lookups");
    group.measurement_time(std::time::Duration::from_millis(200));
    group.sampling_mode(SamplingMode::Flat);

    // Build a larger filter
    let mut builder = BloomFilterBuilder::with_bits_per_key(10);
    let keys: Vec<Bytes> = (0..1000)
        .map(|i| Bytes::from(format!("key_{:010}", i)))
        .collect();
    for key in &keys {
        builder.add_key(key);
    }
    let filter = builder.finish();

    // Precompute lookup keys (mix of hits and misses)
    let lookup_keys: Vec<Bytes> = (0..100)
        .map(|i| {
            if i % 2 == 0 {
                keys[i * 5].clone() // hit
            } else {
                Bytes::from(format!("miss_{:010}", i)) // miss
            }
        })
        .collect();

    group.throughput(Throughput::Elements(100));

    group.bench_function("batch_100_lookups_mixed", |b| {
        b.iter(|| {
            let mut count = 0u32;
            for key in &lookup_keys {
                if filter.may_contain(black_box(key)) {
                    count += 1;
                }
            }
            black_box(count)
        })
    });

    group.finish();
}

/// Benchmark hash computation isolated (via may_contain on miss)
fn bench_bloom_compute_hashes(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_bloom_compute_hashes");
    group.measurement_time(std::time::Duration::from_millis(200));
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let mut builder = BloomFilterBuilder::with_bits_per_key(10);
    let keys: Vec<Bytes> = (0..100)
        .map(|i| Bytes::from(format!("key_{:010}", i)))
        .collect();
    for key in &keys {
        builder.add_key(key);
    }
    let filter = builder.finish();

    // Precompute miss key
    let miss_key = Bytes::from_static(b"key_00001000");

    group.bench_function("compute_hashes_via_miss", |b| {
        b.iter(|| black_box(filter.may_contain(black_box(&miss_key))))
    });

    group.finish();
}

criterion_group! {
    name = tier1_hotpath_bloom;
    config = criterion_config();
    targets = bench_bloom_maybe_contains, bench_bloom_batch_lookups, bench_bloom_compute_hashes
}
criterion_main!(tier1_hotpath_bloom);
