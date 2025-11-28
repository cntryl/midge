//! Tier 2 — Memtable Full Benchmark
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers memtable full behavior and eviction triggers

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::criterion_config;
use std::hint::black_box;

use cntryl_midge::core::memtable::MemTable;

/// Benchmark memtable full scan
fn bench_memtable_full_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_memtable_full_scan");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(10_000));

    // Pre-fill memtable with 10k entries
    let memtable = MemTable::new();
    for i in 0..10_000 {
        let key = format!("key_{:05}", i);
        let value = format!("value_{:05}", i);
        memtable.put(key.as_bytes(), value.as_bytes());
    }

    group.bench_function("scan_full", |b| {
        b.iter(|| {
            let keys = memtable.get_all_keys();
            let mut result = Vec::with_capacity(keys.len());
            for key in &keys {
                if let Some(value) = memtable.get(key) {
                    result.push((key.clone(), value));
                }
            }
            black_box(result);
        })
    });

    group.finish();
}

/// Benchmark memtable full eviction trigger
fn bench_memtable_full_eviction_trigger(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_memtable_full_eviction_trigger");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(10_000));

    group.bench_function("trigger_eviction", |b| {
        b.iter(|| {
            let memtable = MemTable::new();
            let size_limit = 1024 * 1024; // 1MB limit

            // Add entries until full, checking is_full each time
            for i in 0..10_000 {
                let key = format!("key_{:05}", i);
                let value = format!("value_{:05}", i);
                memtable.put(key.as_bytes(), value.as_bytes());

                // Check if full (this is the eviction trigger check)
                let _is_full = memtable.is_full(size_limit);
                black_box(_is_full);
            }
        })
    });

    group.finish();
}

criterion_group! {
    name = tier2_subsystem_memtable_full;
    config = criterion_config();
    targets = bench_memtable_full_scan, bench_memtable_full_eviction_trigger
}
criterion_main!(tier2_subsystem_memtable_full);
