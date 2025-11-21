//! Tier 2 — Manifest apply benchmark (stub)
//!
//! Minimal placeholder bench to compile and run a no-op workload.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

fn bench_manifest_apply(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_manifest_apply");
    group.bench_function("noop", |b| b.iter(|| { black_box(21usize); }));
    group.finish();
}

criterion_group! {
    name = manifest_apply_group;
    config = criterion_config();
    targets = bench_manifest_apply
}
criterion_main!(manifest_apply_group);
