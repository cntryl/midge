//! Tier 2 — Flush Benchmarks
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers memtable flush operations (SST building, encoding)
//! Note: These benchmarks focus on the flush path itself, not engine setup.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use cntryl_midge::common::codec::CompressionType;
use cntryl_midge::sst::mem::SstMemWriter;
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;

/// Pre-generate keys and values as Bytes (required by SstMemWriter API)
fn make_entries(count: usize, value_size: usize) -> Vec<(Bytes, Bytes)> {
    // Pre-allocate value to share across all entries (Bytes is ref-counted)
    let value = Bytes::from(vec![b'v'; value_size]);
    (0..count)
        .map(|i| {
            let key = Bytes::from(format!("key_{:010}", i));
            (key, value.clone())
        })
        .collect()
}

/// Benchmark SST building (1k entries)
fn bench_flush_sst_build_small(c: &mut Criterion) {
    let entries = make_entries(1_000, 128);

    let mut group = c.benchmark_group("subsystem_flush_sst_build_small");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1_000));

    group.bench_function("sst_build_1k", |b| {
        b.iter(|| {
            let mut writer = SstMemWriter::new(CompressionType::None, 4096);
            for (key, value) in &entries {
                writer.add(key, value).unwrap();
            }
            black_box(writer.finish().unwrap())
        })
    });

    group.finish();
}

/// Benchmark SST building (10k entries)
fn bench_flush_sst_build_medium(c: &mut Criterion) {
    let entries = make_entries(10_000, 128);

    let mut group = c.benchmark_group("subsystem_flush_sst_build_medium");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(10_000));

    group.bench_function("sst_build_10k", |b| {
        b.iter(|| {
            let mut writer = SstMemWriter::new(CompressionType::None, 4096);
            for (key, value) in &entries {
                writer.add(key, value).unwrap();
            }
            black_box(writer.finish().unwrap())
        })
    });

    group.finish();
}

/// Benchmark SST building with large values (1k entries × 1KB values)
fn bench_flush_sst_build_large_values(c: &mut Criterion) {
    let entries = make_entries(1_000, 1024);

    let mut group = c.benchmark_group("subsystem_flush_sst_build_large_values");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Bytes(1_000 * 1024)); // 1MB total

    group.bench_function("sst_build_large_values", |b| {
        b.iter(|| {
            let mut writer = SstMemWriter::new(CompressionType::None, 4096);
            for (key, value) in &entries {
                writer.add(key, value).unwrap();
            }
            black_box(writer.finish().unwrap())
        })
    });

    group.finish();
}

criterion_group! {
    name = tier2_subsystem_flush;
    config = criterion_config_for_tier(BenchTier::Tier2Subsystem);
    targets = bench_flush_sst_build_small, bench_flush_sst_build_medium, bench_flush_sst_build_large_values
}
criterion_main!(tier2_subsystem_flush);
