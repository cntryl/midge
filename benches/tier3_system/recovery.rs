//! Tier 3 — System Benchmarks: Crash Recovery
//!
//! **Target Runtime:** ~30-60 seconds
//! **Run Frequency:** Nightly CI / Perf Baselines
//!
//! Measures WAL replay performance and recovery correctness:
//! - Time to replay WAL after crash/restart
//! - Replay throughput (bytes/sec)
//! - Data integrity validation
//! - Different dataset sizes (10K, 100K, 500K records)
//!
//! ## Design Notes
//!
//! - Returns engine from timed closures to exclude teardown from timing
//! - Precomputes all keys/values outside hot loops
//! - Uses unique paths to avoid cross-iteration interference
//! - Throughput measured in bytes
//! - Uses DURABLE_STORAGE_MODES since recovery requires persistence

#[path = "../criterion_helper.rs"]
mod criterion_helper;

mod bench_common;

use bench_common::{
    precompute_kv, unique_bench_path, BenchStorageMode, BYTES_PER_OP, DURABLE_STORAGE_MODES,
    KEY_SIZE, VALUE_SIZE,
};

use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;
use std::sync::Arc;

/// Setup a db at a specific path for recovery tests.
/// Returns the path so we can reopen the same database.
fn setup_db_at_path_with_mode(
    path: &std::path::Path,
    mode: BenchStorageMode,
    wal_sync: bool,
) -> MidgeEngine {
    use cntryl_midge::cloud::mock::MockCloudBackend;
    use std::time::Duration;

    match mode {
        BenchStorageMode::Memory => {
            panic!("Recovery benchmarks require durable storage")
        }
        BenchStorageMode::LocalDisk => {
            let opts = MidgeOptions {
                storage_mode: StorageMode::LocalDisk {
                    db_path: path.to_path_buf(),
                },
                memtable_size: 4 * 1024 * 1024,
                enable_compaction: false,
                wal_sync,
                ..Default::default()
            };
            MidgeEngine::open(opts).unwrap()
        }
        BenchStorageMode::CloudBacked => {
            let backend = Arc::new(MockCloudBackend::new().with_latency(Duration::from_millis(1)));
            let opts = MidgeOptions {
                storage_mode: StorageMode::CloudBacked {
                    local_cache_path: path.to_path_buf(),
                    cloud_backend: backend,
                    storage_context: Default::default(),
                    local_wal_sync: wal_sync,
                    wal_batch_size: 1024 * 1024,
                    sst_cache_capacity: 10,
                },
                memtable_size: 4 * 1024 * 1024,
                enable_compaction: false,
                wal_sync,
                ..Default::default()
            };
            MidgeEngine::open(opts).unwrap()
        }
    }
}

// ============================================================================
// WAL Recovery Performance
// ============================================================================

/// Benchmark WAL replay performance after crash
///
/// Simulates crash by:
/// 1. Writing N records to database
/// 2. Dropping engine (simulating crash, leaving WAL on disk)
/// 3. Reopening database (triggers WAL replay)
/// 4. Measuring replay throughput
fn bench_recovery_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_recovery_throughput");
    group.sampling_mode(SamplingMode::Flat);

    for &op_count in &[10_000usize, 100_000] {
        let (keys, vals) = precompute_kv(op_count, VALUE_SIZE);
        let bytes_total = (op_count as u64) * BYTES_PER_OP;

        group.throughput(Throughput::Bytes(bytes_total));

        for mode in DURABLE_STORAGE_MODES {
            let bench_name = format!("{}/{}", op_count, mode.as_str());
            group.bench_with_input(
                BenchmarkId::new("replay", &bench_name),
                &(op_count, mode),
                |b, &(num_ops, mode)| {
                    let keys_ref = &keys;
                    let vals_ref = &vals;

                    b.iter_batched(
                        || {
                            // Setup and prefill database
                            let db_path = unique_bench_path("replay");
                            let _ = std::fs::remove_dir_all(&db_path);

                            let engine = setup_db_at_path_with_mode(&db_path, mode, false);
                            let cf = engine.default_column_family();

                            // Write records to create WAL entries
                            for i in 0..num_ops {
                                engine.put(&cf, &keys_ref[i], &vals_ref[i]).unwrap();
                            }

                            // Simulate crash (drop engine, don't clean up DB)
                            drop(engine);
                            (db_path, mode)
                        },
                        |(db_path, mode)| {
                            // Measure recovery time (WAL replay)
                            let engine = setup_db_at_path_with_mode(&db_path, mode, false);

                            // Validate some data integrity
                            let cf = engine.default_column_family();
                            for i in (0..num_ops).step_by(1_000) {
                                let val = engine.get(&cf, &keys_ref[i]).unwrap();
                                assert!(val.is_some(), "key not recovered: {}", i);
                            }

                            engine // prevent Drop during timing
                        },
                        BatchSize::LargeInput,
                    )
                },
            );
        }
    }

    group.finish();
}

// ============================================================================
// Recovery with WAL Sync Enabled
// ============================================================================

/// Benchmark recovery performance with synchronous WAL
fn bench_recovery_with_wal_sync(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_recovery_wal_sync");
    group.sampling_mode(SamplingMode::Flat);

    for &op_count in &[10_000usize, 50_000] {
        let (keys, vals) = precompute_kv(op_count, VALUE_SIZE);
        let bytes_total = (op_count as u64) * BYTES_PER_OP;

        group.throughput(Throughput::Bytes(bytes_total));

        for mode in DURABLE_STORAGE_MODES {
            let bench_name = format!("{}/{}", op_count, mode.as_str());
            group.bench_with_input(
                BenchmarkId::new("replay_sync", &bench_name),
                &(op_count, mode),
                |b, &(num_ops, mode)| {
                    let keys_ref = &keys;
                    let vals_ref = &vals;

                    b.iter_batched(
                        || {
                            let db_path = unique_bench_path("sync");
                            let _ = std::fs::remove_dir_all(&db_path);

                            let engine = setup_db_at_path_with_mode(&db_path, mode, true);
                            let cf = engine.default_column_family();

                            for i in 0..num_ops {
                                engine.put(&cf, &keys_ref[i], &vals_ref[i]).unwrap();
                            }

                            drop(engine);
                            (db_path, mode)
                        },
                        |(db_path, mode)| {
                            let engine = setup_db_at_path_with_mode(&db_path, mode, true);

                            // Quick validation
                            let cf = engine.default_column_family();
                            for i in (0..num_ops).step_by(10_000) {
                                black_box(engine.get(&cf, &keys_ref[i]).unwrap());
                            }

                            engine // prevent Drop during timing
                        },
                        BatchSize::LargeInput,
                    )
                },
            );
        }
    }

    group.finish();
}

// ============================================================================
// Partial Recovery (Some Data in L0)
// ============================================================================

/// Benchmark recovery when some writes reached L0 and some are in WAL
fn bench_recovery_with_l0_data(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_recovery_with_l0");
    group.sampling_mode(SamplingMode::Flat);

    for &op_count in &[50_000usize, 100_000] {
        let (keys, vals) = precompute_kv(op_count, VALUE_SIZE);
        let bytes_total = (op_count as u64) * BYTES_PER_OP;

        group.throughput(Throughput::Bytes(bytes_total));

        for mode in DURABLE_STORAGE_MODES {
            let bench_name = format!("{}/{}", op_count, mode.as_str());
            group.bench_with_input(
                BenchmarkId::new("replay_l0", &bench_name),
                &(op_count, mode),
                |b, &(num_ops, mode)| {
                    let keys_ref = &keys;
                    let vals_ref = &vals;

                    b.iter_batched(
                        || {
                            let db_path = unique_bench_path("l0");
                            let _ = std::fs::remove_dir_all(&db_path);

                            let engine = setup_db_at_path_with_mode(&db_path, mode, false);
                            let cf = engine.default_column_family();

                            // Write half the data
                            for i in 0..(num_ops / 2) {
                                engine.put(&cf, &keys_ref[i], &vals_ref[i]).unwrap();
                            }

                            // Flush memtable to L0
                            engine.flush().unwrap();

                            // Write remaining data (stays in WAL)
                            for i in (num_ops / 2)..num_ops {
                                engine.put(&cf, &keys_ref[i], &vals_ref[i]).unwrap();
                            }

                            drop(engine);
                            (db_path, mode)
                        },
                        |(db_path, mode)| {
                            let engine = setup_db_at_path_with_mode(&db_path, mode, false);

                            // Validate all data recovered
                            let cf = engine.default_column_family();
                            for i in (0..num_ops).step_by(5_000) {
                                let val = engine.get(&cf, &keys_ref[i]).unwrap();
                                assert!(val.is_some(), "key {} not recovered", i);
                            }

                            engine // prevent Drop during timing
                        },
                        BatchSize::LargeInput,
                    )
                },
            );
        }
    }

    group.finish();
}

// ============================================================================
// Recovery Speed Comparison: Small vs Large Values
// ============================================================================

/// Compare recovery speed when starting with small vs large values
fn bench_recovery_speed_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_recovery_comparison");
    group.sampling_mode(SamplingMode::Flat);

    let op_count = 100_000usize;

    // Small values (128 bytes)
    let small_value_size = 128usize;
    let (keys_small, vals_small) = precompute_kv(op_count, small_value_size);
    let small_bytes = (op_count as u64) * (KEY_SIZE + small_value_size) as u64;

    group.throughput(Throughput::Bytes(small_bytes));

    for mode in DURABLE_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("recovery_small_values_100k", mode.as_str()),
            &mode,
            |b, &mode| {
                let keys_ref = &keys_small;
                let vals_ref = &vals_small;

                b.iter_batched(
                    || {
                        let db_path = unique_bench_path("small_vals");
                        let _ = std::fs::remove_dir_all(&db_path);

                        let engine = setup_db_at_path_with_mode(&db_path, mode, false);
                        let cf = engine.default_column_family();

                        for i in 0..op_count {
                            engine.put(&cf, &keys_ref[i], &vals_ref[i]).unwrap();
                        }

                        drop(engine);
                        (db_path, mode)
                    },
                    |(db_path, mode)| {
                        let engine = setup_db_at_path_with_mode(&db_path, mode, false);

                        let cf = engine.default_column_family();
                        black_box(engine.get(&cf, &keys_ref[50_000]).unwrap());

                        engine // prevent Drop during timing
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }

    // Large values (1KB)
    let large_value_size = 1024usize;
    let (keys_large, vals_large) = precompute_kv(op_count, large_value_size);
    let large_bytes = (op_count as u64) * (KEY_SIZE + large_value_size) as u64;

    group.throughput(Throughput::Bytes(large_bytes));

    for mode in DURABLE_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("recovery_large_values_100k", mode.as_str()),
            &mode,
            |b, &mode| {
                let keys_ref = &keys_large;
                let vals_ref = &vals_large;

                b.iter_batched(
                    || {
                        let db_path = unique_bench_path("large_vals");
                        let _ = std::fs::remove_dir_all(&db_path);

                        let engine = setup_db_at_path_with_mode(&db_path, mode, false);
                        let cf = engine.default_column_family();

                        for i in 0..op_count {
                            engine.put(&cf, &keys_ref[i], &vals_ref[i]).unwrap();
                        }

                        drop(engine);
                        (db_path, mode)
                    },
                    |(db_path, mode)| {
                        let engine = setup_db_at_path_with_mode(&db_path, mode, false);

                        let cf = engine.default_column_family();
                        black_box(engine.get(&cf, &keys_ref[50_000]).unwrap());

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
    name = tier3_system_recovery;
    config = criterion_config_for_tier(BenchTier::Tier3System);
    targets =
        bench_recovery_throughput,
        bench_recovery_with_wal_sync,
        bench_recovery_with_l0_data,
        bench_recovery_speed_comparison
}
criterion_main!(tier3_system_recovery);
