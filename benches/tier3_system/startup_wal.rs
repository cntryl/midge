//! Tier 3 — Startup WAL replay bench
//!
//! **Target Runtime:** ~30-60 seconds
//! **Run Frequency:** Nightly CI / Perf Baselines
//!
//! Covers engine startup with WAL replay
//!
//! ## Design Notes
//!
//! - Uses DURABLE_STORAGE_MODES since WAL replay requires persistence

#[path = "../criterion_helper.rs"]
mod criterion_helper;

mod bench_common;

use bench_common::{
    precompute_kv, unique_bench_path, BenchStorageMode, DURABLE_STORAGE_MODES, KEY_SIZE, VALUE_SIZE,
};

use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

/// Bytes per operation
const BYTES_PER_OP: u64 = (KEY_SIZE + VALUE_SIZE) as u64;

/// Setup engine at specific path for WAL replay tests
fn setup_engine_at_path(path: &std::path::Path, mode: BenchStorageMode) -> MidgeEngine {
    use cntryl_midge::cloud::mock::MockCloudBackend;

    match mode {
        BenchStorageMode::Memory => panic!("WAL replay benchmarks require persistent storage"),
        BenchStorageMode::LocalDisk => {
            let opts = MidgeOptions {
                storage_mode: StorageMode::LocalDisk {
                    db_path: path.to_path_buf(),
                },
                memtable_size: 100 * 1024 * 1024, // Large memtable = no auto flush
                enable_compaction: false,
                wal_sync: false,
                ..Default::default()
            };
            MidgeEngine::open(opts).expect("failed to open")
        }
        BenchStorageMode::CloudBacked => {
            let backend = Arc::new(MockCloudBackend::new().with_latency(Duration::from_millis(1)));
            let opts = MidgeOptions {
                storage_mode: StorageMode::CloudBacked {
                    local_cache_path: path.to_path_buf(),
                    cloud_backend: backend,
                    storage_context: Default::default(),
                    local_wal_sync: false,
                    wal_batch_size: 1024 * 1024,
                    sst_cache_capacity: 10,
                },
                memtable_size: 100 * 1024 * 1024,
                enable_compaction: false,
                wal_sync: false,
                ..Default::default()
            };
            MidgeEngine::open(opts).expect("failed to open")
        }
    }
}

/// Benchmark engine startup with WAL replay (50k operations)
fn bench_engine_startup_from_wal(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_engine_startup_from_wal");
    group.sampling_mode(SamplingMode::Flat);

    let num_ops = 50_000usize;
    let (keys, vals) = precompute_kv(num_ops, VALUE_SIZE);
    let bytes_total = (num_ops as u64) * BYTES_PER_OP;

    group.throughput(Throughput::Bytes(bytes_total));

    for mode in DURABLE_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("replay_50k_wal_ops", mode.as_str()),
            &mode,
            |b, &mode| {
                let keys_ref = &keys;
                let vals_ref = &vals;

                b.iter_batched(
                    || {
                        let path = unique_bench_path("wal_replay");
                        let _ = std::fs::remove_dir_all(&path);

                        // Create WAL with 50k operations WITHOUT flushing
                        {
                            let engine = setup_engine_at_path(&path, mode);
                            let cf = engine.default_column_family();

                            // Write ops to WAL without flushing
                            for i in 0..num_ops {
                                engine.put(&cf, &keys_ref[i], &vals_ref[i]).unwrap();
                            }
                            // DO NOT flush - keep data only in WAL
                        }

                        (path, mode)
                    },
                    |(path, mode)| {
                        // Measure startup time (WAL replay into memtable)
                        let engine = setup_engine_at_path(&path, mode);

                        // Verify data was recovered from WAL
                        let cf = engine.default_column_family();
                        black_box(engine.get(&cf, &keys_ref[25_000]).unwrap());

                        engine // prevent Drop during timing
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = tier3_system_startup_wal;
    config = criterion_config_for_tier(BenchTier::Tier3System);
    targets = bench_engine_startup_from_wal
}
criterion_main!(tier3_system_startup_wal);
