//! Tier 2 — Manifest large history (stub)
//!
//! Placeholder bench to verify test harness; real workloads should be added later.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

fn bench_manifest_large_history(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_manifest_large_history");
    group.bench_function("noop", |b| b.iter(|| { black_box(34usize); }));
    group.finish();
}

criterion_group! {
    name = manifest_large_history_group;
    config = criterion_config();
    targets = bench_manifest_large_history
}
criterion_main!(manifest_large_history_group);
