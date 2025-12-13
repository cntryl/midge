//! Tier 1 — Bloom filter hot path benchmarks
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers bloom filter hot paths:
//! - Hash computation and containment checks
//! - Single key lookups (hit/miss)

#[path = "./criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use cntryl_midge::sst::bloom::BloomWriter;
use cntryl_midge::sst::bloom::writer::BloomFilterOps;
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;

/// Benchmark bloom filter containment check (hit vs miss)
fn bench_bloom_maybe_contains(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_bloom_maybe_contains");
    group.measurement_time(std::time::Duration::from_millis(200));
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    // Build filter once outside benchmark loop
    let mut builder = BloomWriter::with_defaults(100);
    let keys: Vec<Bytes> = (0..100)
        .map(|i| Bytes::from(format!("key_{:010}", i)))
        .collect();
    for key in &keys {
        builder.insert(key);
    }
    let filter = builder.finish();

    // Precompute keys (avoid allocation in hot path)
    let hit_key = &keys[42];
    let miss_key = b"key_00001000";

    group.bench_function("maybe_contains_hit", |b| {
        b.iter(|| {
            let result = filter.contains(black_box(hit_key));
            black_box(result.might_be_present())
        })
    });

    group.bench_function("maybe_contains_miss", |b| {
        b.iter(|| {
            let result = filter.contains(black_box(miss_key));
            black_box(result.definitely_not_present())
        })
    });

    group.finish();
}

/// Benchmark bloom filter batch lookups (realistic access pattern)
fn bench_bloom_batch_lookups(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_bloom_batch_lookups");
    group.measurement_time(std::time::Duration::from_millis(200));
    group.sampling_mode(SamplingMode::Flat);

    // Build a larger filter
    let mut builder = BloomWriter::with_defaults(1000);
    let keys: Vec<Bytes> = (0..1000)
        .map(|i| Bytes::from(format!("key_{:010}", i)))
        .collect();
    for key in &keys {
        builder.insert(key);
    }
    let filter = builder.finish();

    // Precompute lookup keys (mix of hits and misses)
    let lookup_keys: Vec<(bool, Vec<u8>)> = (0..100)
        .map(|i| {
            if i % 2 == 0 {
                (true, keys[i * 5].to_vec()) // hit
            } else {
                (false, format!("miss_{:010}", i).into_bytes()) // miss
            }
        })
        .collect();

    group.throughput(Throughput::Elements(100));

    group.bench_function("batch_100_lookups_mixed", |b| {
        b.iter(|| {
            let mut count = 0u32;
            for (_is_hit, key) in &lookup_keys {
                if filter.contains(black_box(key)).might_be_present() {
                    count += 1;
                }
            }
            black_box(count)
        })
    });

    group.finish();
}

/// Benchmark hash computation isolated (via contains on miss)
fn bench_bloom_compute_hashes(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_bloom_compute_hashes");
    group.measurement_time(std::time::Duration::from_millis(200));
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let mut builder = BloomWriter::with_defaults(100);
    let keys: Vec<Bytes> = (0..100)
        .map(|i| Bytes::from(format!("key_{:010}", i)))
        .collect();
    for key in &keys {
        builder.insert(key);
    }
    let filter = builder.finish();

    // Precompute miss key
    let miss_key = b"key_00001000";

    group.bench_function("compute_hashes_via_miss", |b| {
        b.iter(|| {
            let result = filter.contains(black_box(miss_key));
            black_box(result.definitely_not_present())
        })
    });

    group.finish();
}

criterion_group! {
    name = tier1_hotpath_bloom;
    config = criterion_config_for_tier(BenchTier::Tier1Hot);
    targets = bench_bloom_maybe_contains, bench_bloom_batch_lookups, bench_bloom_compute_hashes
}
criterion_main!(tier1_hotpath_bloom);
