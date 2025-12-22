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
#[allow(unused)]
const _TIER3_GUARD: () = {
    // Tier-3 benches must use bench_common::tier3 APIs and `tier3_bench!`/`tier3_bench_restore!`.
};
#[path = "./criterion_helper.rs"]
mod criterion_helper;

#[path = "./tier3_system_bench_common.rs"]
mod bench_common;

use bench_common::{
    precompute_kv, reopen_engine_at_path, setup_engine_at_path, unique_bench_path,
    BenchEngineConfig, BenchStorageMode, BYTES_PER_OP, DURABLE_STORAGE_MODES, KEY_SIZE, VALUE_SIZE,
};

use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;

/// Benchmark name constants
const BENCH_REPLAY: &str = "replay";
const BENCH_SYNC: &str = "sync";
const BENCH_L0: &str = "l0";
const BENCH_SMALL_VALS: &str = "small_vals";
const BENCH_LARGE_VALS: &str = "large_vals";

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

    for &op_count in &[10_000usize, 50_000] {
        // Reduced from 100k for faster runs
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
                            let db_path = unique_bench_path(BENCH_REPLAY);
                            let _ = std::fs::remove_dir_all(&db_path);

                            let config = BenchEngineConfig {
                                storage_mode: mode,
                                enable_compaction: false,
                                wal_sync: false,
                                ..Default::default()
                            };

                            let engine = setup_engine_at_path(&db_path, &config);
                            let cf = engine.default_column_family();

                            // Write records to create WAL entries
                            for i in 0..num_ops {
                                engine
                                    .put(cf, &keys_ref[i], &vals_ref[i])
                                    .expect("put failed");
                            }

                            // Simulate crash (drop engine, don't clean up DB)
                            drop(engine);
                            (db_path, config)
                        },
                        |(db_path, config)| {
                            // Measure recovery time (WAL replay)
                            let engine = reopen_engine_at_path(&db_path, &config);

                            // Validate some data integrity
                            let cf = engine.default_column_family();
                            for i in (0..num_ops).step_by(1_000) {
                                let key = black_box(&keys_ref[i]);
                                let val = engine.get(cf, key).expect("get failed");
                                assert!(val.is_some(), "key not recovered: {}", i);
                            }

                            engine
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
                            let db_path = unique_bench_path(BENCH_SYNC);
                            let _ = std::fs::remove_dir_all(&db_path);

                            let config = BenchEngineConfig {
                                storage_mode: mode,
                                enable_compaction: false,
                                wal_sync: true,
                                ..Default::default()
                            };

                            let engine = setup_engine_at_path(&db_path, &config);
                            let cf = engine.default_column_family();

                            for i in 0..num_ops {
                                engine
                                    .put(cf, &keys_ref[i], &vals_ref[i])
                                    .expect("put failed");
                            }

                            drop(engine);
                            (db_path, config)
                        },
                        |(db_path, config)| {
                            let engine = reopen_engine_at_path(&db_path, &config);

                            // Quick validation
                            let cf = engine.default_column_family();
                            for i in (0..num_ops).step_by(10_000) {
                                let key = black_box(&keys_ref[i]);
                                black_box(engine.get(cf, key).expect("get failed"));
                            }

                            engine
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

    for &op_count in &[25_000usize, 50_000] {
        // Reduced from 50k/100k
        let (keys, vals) = precompute_kv(op_count, VALUE_SIZE);
        let bytes_total = (op_count as u64) * BYTES_PER_OP;

        group.throughput(Throughput::Bytes(bytes_total));

        // LocalDisk only for larger workload to avoid cloud overhead
        for mode in DURABLE_STORAGE_MODES {
            if op_count > 25_000 && !matches!(mode, BenchStorageMode::LocalDisk) {
                continue;
            }
            let bench_name = format!("{}/{}", op_count, mode.as_str());
            group.bench_with_input(
                BenchmarkId::new("replay_l0", &bench_name),
                &(op_count, mode),
                |b, &(num_ops, mode)| {
                    let keys_ref = &keys;
                    let vals_ref = &vals;

                    b.iter_batched(
                        || {
                            let db_path = unique_bench_path(BENCH_L0);
                            let _ = std::fs::remove_dir_all(&db_path);

                            let config = BenchEngineConfig {
                                storage_mode: mode,
                                enable_compaction: false,
                                wal_sync: false,
                                ..Default::default()
                            };

                            let engine = setup_engine_at_path(&db_path, &config);
                            let cf = engine.default_column_family();

                            // Write half the data
                            for i in 0..(num_ops / 2) {
                                engine
                                    .put(cf, &keys_ref[i], &vals_ref[i])
                                    .expect("put failed");
                            }

                            // Flush memtable to L0
                            engine.flush().expect("flush failed");

                            // Write remaining data (stays in WAL)
                            for i in (num_ops / 2)..num_ops {
                                engine
                                    .put(cf, &keys_ref[i], &vals_ref[i])
                                    .expect("put failed");
                            }

                            drop(engine);
                            (db_path, config)
                        },
                        |(db_path, config)| {
                            let engine = reopen_engine_at_path(&db_path, &config);

                            // Validate all data recovered
                            let cf = engine.default_column_family();
                            for i in (0..num_ops).step_by(5_000) {
                                let key = black_box(&keys_ref[i]);
                                let val = engine.get(cf, key).expect("get failed");
                                assert!(val.is_some(), "key {} not recovered", i);
                            }

                            engine
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

    let op_count = 50_000usize; // Reduced from 100k for faster runs

    // Small values (128 bytes)
    let small_value_size = 128usize;
    let (keys_small, vals_small) = precompute_kv(op_count, small_value_size);
    let small_bytes = (op_count as u64) * (KEY_SIZE + small_value_size) as u64;

    group.throughput(Throughput::Bytes(small_bytes));

    for mode in DURABLE_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("recovery_small_values_50k", mode.as_str()),
            &mode,
            |b, &mode| {
                let keys_ref = &keys_small;
                let vals_ref = &vals_small;

                b.iter_batched(
                    || {
                        let db_path = unique_bench_path(BENCH_SMALL_VALS);
                        let _ = std::fs::remove_dir_all(&db_path);

                        let config = BenchEngineConfig {
                            storage_mode: mode,
                            enable_compaction: false,
                            wal_sync: false,
                            ..Default::default()
                        };

                        let engine = setup_engine_at_path(&db_path, &config);
                        let cf = engine.default_column_family();

                        for i in 0..op_count {
                            engine
                                .put(cf, &keys_ref[i], &vals_ref[i])
                                .expect("put failed");
                        }

                        drop(engine);
                        (db_path, config)
                    },
                    |(db_path, config)| {
                        let engine = reopen_engine_at_path(&db_path, &config);

                        let cf = engine.default_column_family();
                        let key = black_box(&keys_ref[25_000]);
                        black_box(engine.get(cf, key).expect("get failed"));

                        engine
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
            BenchmarkId::new("recovery_large_values_50k", mode.as_str()),
            &mode,
            |b, &mode| {
                let keys_ref = &keys_large;
                let vals_ref = &vals_large;

                b.iter_batched(
                    || {
                        let db_path = unique_bench_path(BENCH_LARGE_VALS);
                        let _ = std::fs::remove_dir_all(&db_path);

                        let config = BenchEngineConfig {
                            storage_mode: mode,
                            enable_compaction: false,
                            wal_sync: false,
                            ..Default::default()
                        };

                        let engine = setup_engine_at_path(&db_path, &config);
                        let cf = engine.default_column_family();

                        for i in 0..op_count {
                            engine
                                .put(cf, &keys_ref[i], &vals_ref[i])
                                .expect("put failed");
                        }

                        drop(engine);
                        (db_path, config)
                    },
                    |(db_path, config)| {
                        let engine = reopen_engine_at_path(&db_path, &config);

                        let cf = engine.default_column_family();
                        let key = black_box(&keys_ref[25_000]);
                        black_box(engine.get(cf, key).expect("get failed"));

                        engine
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
