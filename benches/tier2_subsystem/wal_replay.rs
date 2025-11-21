//! Tier 2 — WAL replay bench (stub)
//!
//! Minimal bench to validate compile and harness for WAL replay tests.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

fn bench_wal_replay(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_wal_replay");
    group.bench_function("noop", |b| b.iter(|| { black_box(99usize); }));
    group.finish();
}

criterion_group! {
    name = wal_replay_group;
    config = criterion_config();
    targets = bench_wal_replay
}
criterion_main!(wal_replay_group);
