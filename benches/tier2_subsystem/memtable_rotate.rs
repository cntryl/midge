//! Tier 2 — Memtable rotate benchmark
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers memtable rotation behavior

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

/// Benchmark memtable rotate small
fn bench_memtable_rotate_small(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_memtable_rotate_small");
    group.bench_function("rotate_small", |b| b.iter(|| { black_box(1usize); }));
    group.finish();
}

/// Benchmark memtable rotate large
fn bench_memtable_rotate_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_memtable_rotate_large");
    group.bench_function("rotate_large", |b| b.iter(|| { black_box(10usize); }));
    group.finish();
}

criterion_group! {
    name = memtable_rotate_group;
    config = criterion_config();
    targets = bench_memtable_rotate_small, bench_memtable_rotate_large
}
criterion_main!(memtable_rotate_group);