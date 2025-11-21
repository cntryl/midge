//! Tier 2 — WAL segment rollover bench
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers WAL segment rollover operations

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::criterion_config;
use std::hint::black_box;
use tempfile::TempDir;

use cntryl_midge::wal::{FsWalFactory, WalController, WalFactory};
use std::sync::Arc;

/// Benchmark WAL rollover small segments
fn bench_wal_rollover_small_segments(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_wal_rollover_small_segments");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(10));

    group.bench_function("rollover_small", |b| {
        b.iter(|| {
            let temp_dir = TempDir::new().unwrap();
            let factory = FsWalFactory::new();
            let writer = factory.create_writer(temp_dir.path()).unwrap();
            let factory_arc: Arc<dyn WalFactory> = Arc::new(factory);
            let controller = WalController::new(writer, factory_arc);

            // Perform 10 rollovers
            for seq in 1..=10 {
                controller.rotate(temp_dir.path(), seq).unwrap();
            }
            black_box(controller);
        })
    });

    group.finish();
}

/// Benchmark WAL rollover large segments
fn bench_wal_rollover_large_segments(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_wal_rollover_large_segments");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(100));

    group.bench_function("rollover_large", |b| {
        b.iter(|| {
            let temp_dir = TempDir::new().unwrap();
            let factory = FsWalFactory::new();
            let writer = factory.create_writer(temp_dir.path()).unwrap();
            let factory_arc: Arc<dyn WalFactory> = Arc::new(factory);
            let controller = WalController::new(writer, factory_arc);

            // Perform 100 rollovers
            for seq in 1..=100 {
                controller.rotate(temp_dir.path(), seq).unwrap();
            }
            black_box(controller);
        })
    });

    group.finish();
}

criterion_group! {
    name = wal_segment_rollover_group;
    config = criterion_config();
    targets = bench_wal_rollover_small_segments, bench_wal_rollover_large_segments
}
criterion_main!(wal_segment_rollover_group);