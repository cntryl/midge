//! Tier 5 — Soak/Space Amplification Bench (stub)
//!
//! Placeholder bench for space amplification tests.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

fn bench_space_amplification(c: &mut Criterion) {
    let mut group = c.benchmark_group("soak_space_amplification");
    group.bench_function("noop", |b| b.iter(|| { black_box(1u64); }));
    group.finish();
}

criterion_group! {
    name = space_amplification_group;
    config = criterion_config();
    targets = bench_space_amplification
}
criterion_main!(space_amplification_group);
