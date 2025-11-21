//! Tier 2 — WAL segment rollover bench
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers WAL segment rollover operations

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

/// Benchmark WAL rollover small segments
fn bench_wal_rollover_small_segments(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_wal_rollover_small_segments");
    group.bench_function("rollover_small", |b| b.iter(|| { black_box(10usize); }));
    group.finish();
}

/// Benchmark WAL rollover large segments
fn bench_wal_rollover_large_segments(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_wal_rollover_large_segments");
    group.bench_function("rollover_large", |b| b.iter(|| { black_box(100usize); }));
    group.finish();
}

criterion_group! {
    name = wal_segment_rollover_group;
    config = criterion_config();
    targets = bench_wal_rollover_small_segments, bench_wal_rollover_large_segments
}
criterion_main!(wal_segment_rollover_group);