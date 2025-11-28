//! Tier 1 — Memtable seek hot path
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers memtable seek/lookup hot paths:
//! - Point lookups and version finding
//! - Forward and reverse iteration (using get_all_keys)

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use cntryl_midge::core::memtable::MemTable;
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::criterion_config;
use std::hint::black_box;

fn make_key(i: usize) -> Bytes {
    Bytes::from(format!("key_{:010}", i))
}

fn make_value(i: usize) -> Bytes {
    Bytes::from(format!("value_{}", i))
}

/// Benchmark point lookup
fn bench_memtable_get_point_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_memtable_get_point_lookup");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));
    group.measurement_time(std::time::Duration::from_millis(200));

    let memtable = MemTable::new();
    // Pre-populate
    for i in 0..1000 {
        memtable.put(make_key(i).as_ref(), make_value(i).as_ref());
    }

    group.bench_function("point_lookup_hit", |b| {
        b.iter(|| {
            let result = memtable.get(make_key(500).as_ref());
            black_box(result);
        })
    });

    group.bench_function("point_lookup_miss", |b| {
        b.iter(|| {
            let result = memtable.get(make_key(2000).as_ref()); // Not present
            black_box(result);
        })
    });

    group.finish();
}

/// Benchmark getting latest version
fn bench_memtable_get_latest_version(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_memtable_get_latest_version");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));
    group.measurement_time(std::time::Duration::from_millis(200));

    let memtable = MemTable::new();
    let key = make_key(42);
    // Add multiple versions (using sequence numbers)
    for i in 0..5 {
        memtable.put_with_seq(key.as_ref(), make_value(i).as_ref(), i as u64);
    }

    group.bench_function("get_latest_version", |b| {
        b.iter(|| {
            let result = memtable.get(key.as_ref());
            black_box(result);
        })
    });

    group.finish();
}

/// Benchmark forward seek using get_all_keys
/// Simulates forward iteration from a start key
fn bench_memtable_seek_forward_32steps(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_memtable_seek_forward_32steps");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(32));
    group.measurement_time(std::time::Duration::from_millis(200));

    let memtable = MemTable::new();
    // Pre-populate sequential keys
    for i in 0..100 {
        memtable.put(make_key(i).as_ref(), make_value(i).as_ref());
    }

    group.bench_function("seek_forward_32", |b| {
        b.iter(|| {
            let start_key = make_key(10);
            let all_keys = memtable.get_all_keys();

            // Filter keys >= start_key and take 32
            let results: Vec<_> = all_keys
                .iter()
                .filter(|k| k.as_ref() >= start_key.as_ref())
                .take(32)
                .cloned()
                .collect();

            black_box(results);
        })
    });

    group.finish();
}

/// Benchmark reverse seek using get_all_keys
/// Simulates reverse iteration from a start key
fn bench_memtable_seek_reverse_32steps(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_memtable_seek_reverse_32steps");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(32));
    group.measurement_time(std::time::Duration::from_millis(200));

    let memtable = MemTable::new();
    // Pre-populate sequential keys
    for i in 0..100 {
        memtable.put(make_key(i).as_ref(), make_value(i).as_ref());
    }

    group.bench_function("seek_reverse_32", |b| {
        b.iter(|| {
            let start_key = make_key(50);
            let all_keys = memtable.get_all_keys();

            // Filter keys <= start_key, reverse, and take 32
            let mut results: Vec<_> = all_keys
                .iter()
                .filter(|k| k.as_ref() <= start_key.as_ref())
                .cloned()
                .collect();
            results.reverse();
            results.truncate(32);

            black_box(results);
        })
    });

    group.finish();
}

criterion_group! {
    name = tier1_hotpath_memtable_seek;
    config = criterion_config();
    targets = bench_memtable_get_point_lookup, bench_memtable_get_latest_version, bench_memtable_seek_forward_32steps, bench_memtable_seek_reverse_32steps
}
criterion_main!(tier1_hotpath_memtable_seek);
