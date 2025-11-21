//! Tier 3 — Contention-heavy benchmark (stub)
//!
//! Placeholder bench to simulate heavy contention cases.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

fn bench_contention_heavy(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_contention_heavy");
    group.bench_function("noop", |b| b.iter(|| { black_box(99usize); }));
    group.finish();
}

criterion_group! {
    name = contention_heavy_group;
    config = criterion_config();
    targets = bench_contention_heavy
}
criterion_main!(contention_heavy_group);
