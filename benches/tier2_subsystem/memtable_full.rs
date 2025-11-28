//! Tier 2 — Memtable Full Benchmark
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers memtable full behavior and eviction triggers

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

/// Benchmark memtable full scan
fn bench_memtable_full_scan(c: &mut Criterion) {
    // Pre-generate KV pairs
    let kv_pairs = make_kv_pairs(10_000);

    // Pre-fill memtable with 10k entries
    let memtable = MemTable::new();
    for (key, value) in &kv_pairs {
        memtable.put(key, value);
    }

    let mut group = c.benchmark_group("subsystem_memtable_full_scan");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(10_000));

    group.bench_function("scan_full", |b| {
        b.iter(|| {
            let keys = memtable.get_all_keys();
            let mut count = 0u32;
            for key in &keys {
                if memtable.get(key).is_some() {
                    count += 1;
                }
            }
            black_box(count)
        })
    });

    group.finish();
}

/// Benchmark memtable fill to capacity (measures put + is_full check)
fn bench_memtable_fill_to_capacity(c: &mut Criterion) {
    // Pre-generate KV pairs
    let kv_pairs = make_kv_pairs(10_000);

    let mut group = c.benchmark_group("subsystem_memtable_fill_to_capacity");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(10_000));

    group.bench_function("fill_10k", |b| {
        b.iter(|| {
            let memtable = MemTable::new();
            let size_limit = 10 * 1024 * 1024; // 10MB limit (won't trigger early)

            for (key, value) in &kv_pairs {
                memtable.put(key, value);
            }
            // Check fullness at end
            black_box(memtable.is_full(size_limit))
        })
    });

    group.finish();
}

criterion_group! {
    name = tier2_subsystem_memtable_full;
    config = criterion_config_for_tier(BenchTier::Tier2Subsystem);
    targets = bench_memtable_full_scan, bench_memtable_fill_to_capacity
}
criterion_main!(tier2_subsystem_memtable_full);
