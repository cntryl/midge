//! Tier 1 — Bloom filter hot path benchmarks (stub)
//!
//! This is a minimal placeholder so benches compile; real bench logic should be added by engineers.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

fn bench_bloom(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_bloom");
    group.bench_function("noop", |b| b.iter(|| { black_box(1 + 1); }));
    group.finish();
}

criterion_group! {
    name = bloom_group;
    config = criterion_config();
    targets = bench_bloom
}
criterion_main!(bloom_group);
