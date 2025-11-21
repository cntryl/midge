//! Tier 5 — Soak/Compaction backlog growth (stub)
//!
//! Minimal placeholder bench for backlog growth soak tests.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

fn bench_compaction_backlog_growth(c: &mut Criterion) {
    let mut group = c.benchmark_group("soak_compaction_backlog_growth");
    group.bench_function("noop", |b| b.iter(|| { black_box(123u32); }));
    group.finish();
}

criterion_group! {
    name = compaction_backlog_growth_group;
    config = criterion_config();
    targets = bench_compaction_backlog_growth
}
criterion_main!(compaction_backlog_growth_group);
