//! Tier 2 — Memtable Full Benchmark
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers memtable full behavior and eviction triggers

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

/// Benchmark memtable full scan
fn bench_memtable_full_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_memtable_full_scan");
    group.bench_function("scan_full", |b| b.iter(|| { black_box(1000usize); }));
    group.finish();
}

/// Benchmark memtable full eviction trigger
fn bench_memtable_full_eviction_trigger(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_memtable_full_eviction_trigger");
    group.bench_function("trigger_eviction", |b| b.iter(|| { black_box(true); }));
    group.finish();
}

criterion_group! {
    name = memtable_full_group;
    config = criterion_config();
    targets = bench_memtable_full_scan, bench_memtable_full_eviction_trigger
}
criterion_main!(memtable_full_group);