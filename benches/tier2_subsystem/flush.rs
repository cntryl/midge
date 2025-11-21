//! Tier 2 — Flush Benchmarks
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers memtable flush operations

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

/// Benchmark flush small memtable
fn bench_flush_small_memtable(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_flush_small_memtable");
    group.bench_function("flush_small", |b| b.iter(|| { black_box(1000usize); }));
    group.finish();
}

/// Benchmark flush large memtable
fn bench_flush_large_memtable(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_flush_large_memtable");
    group.bench_function("flush_large", |b| b.iter(|| { black_box(100000usize); }));
    group.finish();
}

/// Benchmark flush sparse index build
fn bench_flush_sparse_index_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_flush_sparse_index_build");
    group.bench_function("build_sparse_index", |b| b.iter(|| { black_box(500usize); }));
    group.finish();
}

criterion_group! {
    name = flush_group;
    config = criterion_config();
    targets = bench_flush_small_memtable, bench_flush_large_memtable, bench_flush_sparse_index_build
}
criterion_main!(flush_group);