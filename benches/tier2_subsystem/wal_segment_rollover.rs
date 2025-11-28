//! Tier 2 — WAL segment rollover bench
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers WAL segment rollover operations

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, SamplingMode, Throughput};
use criterion_helper::criterion_config;
use std::hint::black_box;
use std::path::PathBuf;
use tempfile::TempDir;

use cntryl_midge::wal::{FsWalFactory, WalController, WalFactory};
use std::sync::Arc;

/// Setup struct to hold tempdir and controller for benchmarks.
/// TempDir must be kept alive to prevent directory cleanup.
struct RolloverSetup {
    #[allow(dead_code)] // Kept alive for drop behavior
    temp_dir: TempDir,
    dir_path: PathBuf,
    controller: WalController,
}

fn create_rollover_setup() -> RolloverSetup {
    let temp_dir = TempDir::new().unwrap();
    let dir_path = temp_dir.path().to_path_buf();
    let factory = FsWalFactory::new();
    let writer = factory.create_writer(&dir_path).unwrap();
    let factory_arc: Arc<dyn WalFactory> = Arc::new(factory);
    let controller = WalController::new(writer, factory_arc);
    RolloverSetup {
        temp_dir,
        dir_path,
        controller,
    }
}

/// Benchmark WAL rollover small segments
fn bench_wal_rollover_small_segments(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_wal_rollover_small_segments");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(10));

    group.bench_function("rollover_small", |b| {
        b.iter_batched(
            create_rollover_setup,
            |setup| {
                // Perform 10 rollovers
                for seq in 1..=10 {
                    setup.controller.rotate(&setup.dir_path, seq).unwrap();
                }
                black_box(setup.controller);
                // setup.temp_dir dropped here, cleaning up files
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

/// Benchmark WAL rollover large segments
fn bench_wal_rollover_large_segments(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_wal_rollover_large_segments");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(100));

    group.bench_function("rollover_large", |b| {
        b.iter_batched(
            create_rollover_setup,
            |setup| {
                // Perform 100 rollovers
                for seq in 1..=100 {
                    setup.controller.rotate(&setup.dir_path, seq).unwrap();
                }
                black_box(setup.controller);
                // setup.temp_dir dropped here, cleaning up files
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group! {
    name = tier2_subsystem_wal_segment_rollover;
    config = criterion_config();
    targets = bench_wal_rollover_small_segments, bench_wal_rollover_large_segments
}
criterion_main!(tier2_subsystem_wal_segment_rollover);
