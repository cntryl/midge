//! Tier 2 — Bloom false positive rate benchmark (stub)
//!
//! Minimal placeholder bench for the bloom fpp calculation.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

fn bench_bloom_false_positive_rate(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_bloom_false_positive_rate");
    group.bench_function("noop", |b| b.iter(|| { black_box(4u32); }));
    group.finish();
}

criterion_group! {
    name = bloom_false_positive_rate_group;
    config = criterion_config();
    targets = bench_bloom_false_positive_rate
}
criterion_main!(bloom_false_positive_rate_group);
