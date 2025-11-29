//! Tier 3 — Startup large dataset bench
//!
//! **Target Runtime:** ~30-60 seconds
//! **Run Frequency:** Nightly CI / Perf Baselines
//!
//! Covers engine startup with large manifest (many SST files)
//!
//! ## Design Notes
//!
//! - Uses DURABLE_STORAGE_MODES since startup with SSTs requires persistence

#[path = "../criterion_helper.rs"]
mod criterion_helper;

mod bench_common;

use bench_common::{
    precompute_kv, unique_bench_path, BenchStorageMode, BYTES_PER_OP, DURABLE_STORAGE_MODES,
    VALUE_SIZE,
};

use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::sync::Arc;
use std::time::Duration;

/// Setup engine at specific path for reopen tests
fn setup_engine_at_path(path: &std::path::Path, mode: BenchStorageMode) -> MidgeEngine {
    use cntryl_midge::cloud::mock::MockCloudBackend;

    match mode {
        BenchStorageMode::Memory => panic!("Startup benchmarks require persistent storage"),
        BenchStorageMode::LocalDisk => {
            let opts = MidgeOptions {
                storage_mode: StorageMode::LocalDisk {
                    db_path: path.to_path_buf(),
                },
                memtable_size: 64 * 1024, // Small memtable = more SSTs
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
                memtable_size: 64 * 1024,
                enable_compaction: false,
                wal_sync: false,
                ..Default::default()
            };
            MidgeEngine::open(opts).expect("failed to open")
        }
    }
}

/// Benchmark engine startup with large manifest (simulated via many flushes)
fn bench_engine_startup_100k_sst_files(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_engine_startup_large_manifest");
    group.sampling_mode(SamplingMode::Flat);

    let num_keys = 5_000usize;
    let (keys, vals) = precompute_kv(num_keys, VALUE_SIZE);
    let bytes_total = (num_keys as u64) * BYTES_PER_OP;

    group.throughput(Throughput::Bytes(bytes_total));

    for mode in DURABLE_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("startup_with_many_ssts", mode.as_str()),
            &mode,
            |b, &mode| {
                let keys_ref = &keys;
                let vals_ref = &vals;

                b.iter_batched(
                    || {
                        let path = unique_bench_path("large_manifest");
                        let _ = std::fs::remove_dir_all(&path);

                        // Create database and populate with many small flushes
                        {
                            let engine = setup_engine_at_path(&path, mode);
                            let cf = engine.default_column_family();

                            // Write keys with periodic flushes to create ~50 SST files
                            for i in 0..num_keys {
                                engine.put(&cf, &keys_ref[i], &vals_ref[i]).unwrap();

                                if i % 100 == 99 {
                                    engine.flush().unwrap();
                                }
                            }
                            engine.flush().unwrap();
                        }

                        (path, mode)
                    },
                    |(path, mode)| {
                        // Measure startup time (manifest loading + recovery)
                        setup_engine_at_path(&path, mode) // prevent Drop during timing
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = tier3_system_startup_large;
    config = criterion_config_for_tier(BenchTier::Tier3System);
    targets = bench_engine_startup_100k_sst_files
}
criterion_main!(tier3_system_startup_large);
