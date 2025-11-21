//! Tier 1 — WAL frame parsing hot path (stub)
//!
//! Placeholder bench for WAL frame parsing; add real logic when available.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

fn bench_wal_frame_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_wal_frame_parse");
    group.bench_function("noop", |b| b.iter(|| { black_box(0u64); }));
    group.finish();
}

criterion_group! {
    name = wal_frame_parse_group;
    config = criterion_config();
    targets = bench_wal_frame_parse
}
criterion_main!(wal_frame_parse_group);
