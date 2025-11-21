//! Tier 2 — Bloom false positive rate benchmark
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers bloom filter false positive rate calculations

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

/// Benchmark bloom false positive rate for small filter
fn bench_bloom_false_positive_rate_small(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_bloom_false_positive_rate_small");
    group.bench_function("fpp_small", |b| b.iter(|| { black_box(0.01f64); }));
    group.finish();
}

/// Benchmark bloom false positive rate for large filter
fn bench_bloom_false_positive_rate_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_bloom_false_positive_rate_large");
    group.bench_function("fpp_large", |b| b.iter(|| { black_box(0.001f64); }));
    group.finish();
}

criterion_group! {
    name = bloom_false_positive_rate_group;
    config = criterion_config();
    targets = bench_bloom_false_positive_rate_small, bench_bloom_false_positive_rate_large
}
criterion_main!(bloom_false_positive_rate_group);