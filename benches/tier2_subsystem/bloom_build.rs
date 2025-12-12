//! Tier 2 — Bloom Build Benchmarks
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers bloom filter building operations:
//! - Building filters with different key counts
//! - Measures key insertion throughput (hashing + bit-setting)

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use cntryl_midge::sst::bloom::BloomWriter;
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;

/// Pre-generate keys as raw bytes (no Bytes wrapper overhead in benchmark)
fn make_test_keys(count: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|i| format!("key_{:010}", i).into_bytes())
        .collect()
}

/// Benchmark building bloom filter with 10k keys
fn bench_bloom_build_10k_keys(c: &mut Criterion) {
    let keys = make_test_keys(10_000);

    let mut group = c.benchmark_group("subsystem_bloom_build_10k_keys");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(10_000));

    group.bench_function("build_10k_keys", |b| {
        b.iter(|| {
            let mut builder = BloomWriter::with_defaults(10_000);
            for key in &keys {
                builder.insert(key);
            }
            black_box(builder.finish())
        })
    });

    group.finish();
}

/// Benchmark building bloom filter with 100k keys
fn bench_bloom_build_100k_keys(c: &mut Criterion) {
    let keys = make_test_keys(100_000);

    let mut group = c.benchmark_group("subsystem_bloom_build_100k_keys");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(100_000));

    group.bench_function("build_100k_keys", |b| {
        b.iter(|| {
            let mut builder = BloomWriter::with_defaults(100_000);
            for key in &keys {
                builder.insert(key);
            }
            black_box(builder.finish())
        })
    });

    group.finish();
}

/// Benchmark building bloom filter with 1M keys (stress test)
fn bench_bloom_build_1m_keys(c: &mut Criterion) {
    let keys = make_test_keys(1_000_000);

    let mut group = c.benchmark_group("subsystem_bloom_build_1m_keys");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1_000_000));
    group.sample_size(10); // Fewer samples for long-running benchmark

    group.bench_function("build_1m_keys", |b| {
        b.iter(|| {
            let mut builder = BloomWriter::with_defaults(1_000_000);
            for key in &keys {
                builder.insert(key);
            }
            black_box(builder.finish())
        })
    });

    group.finish();
}

criterion_group! {
    name = tier2_subsystem_bloom_build;
    config = criterion_config_for_tier(BenchTier::Tier2Subsystem);
    targets = bench_bloom_build_10k_keys, bench_bloom_build_100k_keys, bench_bloom_build_1m_keys
}
criterion_main!(tier2_subsystem_bloom_build);
