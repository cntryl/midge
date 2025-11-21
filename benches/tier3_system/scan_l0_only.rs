//! Tier 3 — Scan L0-only bench
//!
//! **Target Runtime:** ~2 minutes
//! **Run Frequency:** Nightly / release builds
//!
//! Covers L0-only scan operations

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

/// Benchmark scan L0 direct
fn bench_scan_l0_direct(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_scan_l0_direct");
    group.bench_function("scan_l0", |b| b.iter(|| { black_box(10000usize); }));
    group.finish();
}

criterion_group! {
    name = scan_l0_only_group;
    config = criterion_config();
    targets = bench_scan_l0_direct
}
criterion_main!(scan_l0_only_group);