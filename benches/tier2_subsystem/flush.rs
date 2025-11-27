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
use criterion_helper::criterion_config;
use std::hint::black_box;

fn make_key(i: usize) -> Bytes {
    Bytes::from(format!("key_{:010}", i))
}

fn make_value(size: usize) -> Bytes {
    Bytes::from(vec![b'v'; size])
}

/// Benchmark SST building (1k entries)
fn bench_flush_sst_build_small(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_flush_sst_build_small");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1_000));

    // Pre-generate keys and values
    let entries: Vec<_> = (0..1_000)
        .map(|i| (make_key(i), make_value(128)))
        .collect();

    group.bench_function("sst_build_1k", |b| {
        b.iter(|| {
            let mut writer = SstMemWriter::new(CompressionType::None, 4096);
            for (key, value) in &entries {
                writer.add(key, value).unwrap();
            }
            let reader = writer.finish().unwrap();
            black_box(reader);
        })
    });

    group.finish();
}

/// Benchmark SST building (10k entries)
fn bench_flush_sst_build_medium(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_flush_sst_build_medium");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(10_000));

    // Pre-generate keys and values
    let entries: Vec<_> = (0..10_000)
        .map(|i| (make_key(i), make_value(128)))
        .collect();

    group.bench_function("sst_build_10k", |b| {
        b.iter(|| {
            let mut writer = SstMemWriter::new(CompressionType::None, 4096);
            for (key, value) in &entries {
                writer.add(key, value).unwrap();
            }
            let reader = writer.finish().unwrap();
            black_box(reader);
        })
    });

    group.finish();
}

/// Benchmark SST building with large values (1k entries × 1KB values)
fn bench_flush_sst_build_large_values(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_flush_sst_build_large_values");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Bytes(1_000 * 1024)); // 1MB total

    // Pre-generate keys and large values
    let entries: Vec<_> = (0..1_000)
        .map(|i| (make_key(i), make_value(1024)))
        .collect();

    group.bench_function("sst_build_large_values", |b| {
        b.iter(|| {
            let mut writer = SstMemWriter::new(CompressionType::None, 4096);
            for (key, value) in &entries {
                writer.add(key, value).unwrap();
            }
            let reader = writer.finish().unwrap();
            black_box(reader);
        })
    });

    group.finish();
}

criterion_group! {
    name = flush_group;
    config = criterion_config();
    targets = bench_flush_sst_build_small, bench_flush_sst_build_medium, bench_flush_sst_build_large_values
}
criterion_main!(flush_group);
