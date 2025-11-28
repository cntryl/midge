//! Tier 2 — Memtable rotate benchmark
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers memtable rotation behavior (fill + drain cycle)

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;

use cntryl_midge::core::memtable::MemTable;

/// Pre-generate keys and values as raw bytes
fn make_kv_pairs(count: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..count)
        .map(|i| {
            (
                format!("key_{:010}", i).into_bytes(),
                format!("value_{:010}", i).into_bytes(),
            )
        })
        .collect()
}

/// Benchmark memtable rotate small (100 entries)
fn bench_memtable_rotate_small(c: &mut Criterion) {
    let kv_pairs = make_kv_pairs(100);

    let mut group = c.benchmark_group("subsystem_memtable_rotate_small");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(100));

    group.bench_function("rotate_small", |b| {
        b.iter(|| {
            let memtable = MemTable::new();
            for (key, value) in &kv_pairs {
                memtable.put(key, value);
            }
            // Drain (simulate rotation)
            black_box(memtable.drain_with_meta_internal())
        })
    });

    group.finish();
}

/// Benchmark memtable rotate large (10k entries)
fn bench_memtable_rotate_large(c: &mut Criterion) {
    let kv_pairs = make_kv_pairs(10_000);

    let mut group = c.benchmark_group("subsystem_memtable_rotate_large");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(10_000));

    group.bench_function("rotate_large", |b| {
        b.iter(|| {
            let memtable = MemTable::new();
            for (key, value) in &kv_pairs {
                memtable.put(key, value);
            }
            // Drain (simulate rotation)
            black_box(memtable.drain_with_meta_internal())
        })
    });

    group.finish();
}

criterion_group! {
    name = tier2_subsystem_memtable_rotate;
    config = criterion_config_for_tier(BenchTier::Tier2Subsystem);
    targets = bench_memtable_rotate_small, bench_memtable_rotate_large
}
criterion_main!(tier2_subsystem_memtable_rotate);
