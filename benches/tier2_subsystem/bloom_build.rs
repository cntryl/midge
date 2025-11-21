//! Tier 2 — Bloom Build Benchmarks (stub)
//!
//! Minimal placeholder bench for building bloom filters.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

fn bench_bloom_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_bloom_build");
    group.bench_function("noop", |b| b.iter(|| { black_box(3); }));
    group.finish();
}

criterion_group! {
    name = bloom_build_group;
    config = criterion_config();
    targets = bench_bloom_build
}
criterion_main!(bloom_build_group);
