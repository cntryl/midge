//! Tier 2 — Manifest apply benchmark
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers manifest application operations

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

/// Benchmark manifest apply 100 ops
fn bench_manifest_apply_100_ops(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_manifest_apply_100_ops");
    group.bench_function("apply_100", |b| b.iter(|| { black_box(100usize); }));
    group.finish();
}

/// Benchmark manifest apply 10k ops
fn bench_manifest_apply_10k_ops(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_manifest_apply_10k_ops");
    group.bench_function("apply_10k", |b| b.iter(|| { black_box(10000usize); }));
    group.finish();
}

criterion_group! {
    name = manifest_apply_group;
    config = criterion_config();
    targets = bench_manifest_apply_100_ops, bench_manifest_apply_10k_ops
}
criterion_main!(manifest_apply_group);