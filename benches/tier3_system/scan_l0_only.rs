//! Tier 3 — Scan L0-only bench (stub)
//!
//! Placeholder for L0-only scan benchmark.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

fn bench_scan_l0_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_scan_l0_only");
    group.bench_function("noop", |b| b.iter(|| { black_box(0u8); }));
    group.finish();
}

criterion_group! {
    name = scan_l0_only_group;
    config = criterion_config();
    targets = bench_scan_l0_only
}
criterion_main!(scan_l0_only_group);
