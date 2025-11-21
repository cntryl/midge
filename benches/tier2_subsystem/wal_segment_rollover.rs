//! Tier 2 — WAL segment rollover bench (stub)
//!
//! Placeholder bench to check rollover logic; no real workload included.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

fn bench_wal_segment_rollover(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_wal_segment_rollover");
    group.bench_function("noop", |b| b.iter(|| { black_box(256usize); }));
    group.finish();
}

criterion_group! {
    name = wal_segment_rollover_group;
    config = criterion_config();
    targets = bench_wal_segment_rollover
}
criterion_main!(wal_segment_rollover_group);
