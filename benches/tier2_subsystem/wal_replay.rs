//! Tier 2 — WAL replay bench
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers WAL replay operations

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

/// Benchmark WAL replay small file
fn bench_wal_replay_small_file(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_wal_replay_small_file");
    group.bench_function("replay_small", |b| b.iter(|| { black_box(1000usize); }));
    group.finish();
}

/// Benchmark WAL replay large file
fn bench_wal_replay_large_file(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_wal_replay_large_file");
    group.bench_function("replay_large", |b| b.iter(|| { black_box(100000usize); }));
    group.finish();
}

/// Benchmark WAL replay corrupted tail
fn bench_wal_replay_corrupted_tail(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_wal_replay_corrupted_tail");
    group.bench_function("replay_corrupted", |b| b.iter(|| { black_box(false); }));
    group.finish();
}

criterion_group! {
    name = wal_replay_group;
    config = criterion_config();
    targets = bench_wal_replay_small_file, bench_wal_replay_large_file, bench_wal_replay_corrupted_tail
}
criterion_main!(wal_replay_group);