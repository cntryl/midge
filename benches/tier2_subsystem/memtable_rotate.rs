//! Tier 2 — Memtable rotate benchmark
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers memtable rotation behavior

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::criterion_config;
use std::hint::black_box;

use cntryl_midge::core::memtable::MemTable;

/// Benchmark memtable rotate small
fn bench_memtable_rotate_small(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_memtable_rotate_small");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(100));

    group.bench_function("rotate_small", |b| {
        b.iter(|| {
            // Create and fill memtable with 100 entries
            let memtable = MemTable::new();
            for i in 0..100 {
                let key = format!("key_{:03}", i);
                let value = format!("value_{:03}", i);
                memtable.put(key.as_bytes(), value.as_bytes());
            }
            // Drain (simulate rotation)
            let drained = memtable.drain_with_meta_internal();
            black_box(drained);
        })
    });

    group.finish();
}

/// Benchmark memtable rotate large
fn bench_memtable_rotate_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_memtable_rotate_large");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(10_000));

    group.bench_function("rotate_large", |b| {
        b.iter(|| {
            // Create and fill memtable with 10k entries
            let memtable = MemTable::new();
            for i in 0..10_000 {
                let key = format!("key_{:05}", i);
                let value = format!("value_{:05}", i);
                memtable.put(key.as_bytes(), value.as_bytes());
            }
            // Drain (simulate rotation)
            let drained = memtable.drain_with_meta_internal();
            black_box(drained);
        })
    });

    group.finish();
}

criterion_group! {
    name = memtable_rotate_group;
    config = criterion_config();
    targets = bench_memtable_rotate_small, bench_memtable_rotate_large
}
criterion_main!(memtable_rotate_group);