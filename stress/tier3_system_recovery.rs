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

#[path = "./common/tier3_harness.rs"]
mod tier3;

use bench_common::{
    create_seed_dir, precompute_kv, setup_engine_at_path, BenchEngineConfig, BenchStorageMode,
    BYTES_PER_OP, DURABLE_STORAGE_MODES, KEY_SIZE, VALUE_SIZE,
};

use cntryl_midge::Durability;
use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
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

                    let config = BenchEngineConfig {
                        storage_mode: mode,
                        enable_compaction: false,
                        durability: Durability::Steady,
                        ..Default::default()
                    };

                    let keys_seed = keys_ref;
                    let vals_seed = vals_ref;
                    let seed_prefix = format!("{}_seed_replay_{}_{}", BENCH_REPLAY, num_ops, mode);
                    let config_seed = config.clone();
                    let seed_path = create_seed_dir(seed_prefix.as_str(), move |p| {
                        let engine = setup_engine_at_path(p, &config_seed);
                        let cf = engine.default_column_family();

                        // Write records to create WAL entries.
                        for i in 0..num_ops {
                            engine
                                .put(cf, &keys_seed[i], &vals_seed[i])
                                .expect("put failed");
                        }

                        // Simulate crash (drop engine, leave WAL on disk).
                        drop(engine);
                    });

                    let case = tier3::Tier3OpenCase::from_seed(seed_path, config.clone());
                    tier3_bench_open!(b, case, move |engine| {
                        // Validate some data integrity.
                        let cf = engine.default_column_family();
                        for i in (0..num_ops).step_by(1_000) {
                            let key = black_box(&keys_ref[i]);
                            let val = engine.get(cf, key).expect("get failed");
                            assert!(val.is_some(), "key not recovered: {}", i);
                        }
                    });
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

                    let config = BenchEngineConfig {
                        storage_mode: mode,
                        enable_compaction: false,
                        durability: Durability::Strict,
                        ..Default::default()
                    };

                    let keys_seed = keys_ref;
                    let vals_seed = vals_ref;
                    let seed_prefix = format!("{}_seed_sync_{}_{}", BENCH_SYNC, num_ops, mode);
                    let config_seed = config.clone();
                    let seed_path = create_seed_dir(seed_prefix.as_str(), move |p| {
                        let engine = setup_engine_at_path(p, &config_seed);
                        let cf = engine.default_column_family();

                        for i in 0..num_ops {
                            engine
                                .put(cf, &keys_seed[i], &vals_seed[i])
                                .expect("put failed");
                        }

                        drop(engine);
                    });

                    let case = tier3::Tier3OpenCase::from_seed(seed_path, config.clone());
                    tier3_bench_open!(b, case, move |engine| {
                        // Quick validation.
                        let cf = engine.default_column_family();
                        for i in (0..num_ops).step_by(10_000) {
                            let key = black_box(&keys_ref[i]);
                            black_box(engine.get(cf, key).expect("get failed"));
                        }
                    });
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

                    let config = BenchEngineConfig {
                        storage_mode: mode,
                        enable_compaction: false,
                        durability: Durability::Steady,
                        ..Default::default()
                    };

                    let keys_seed = keys_ref;
                    let vals_seed = vals_ref;
                    let seed_prefix = format!("{}_seed_l0_{}_{}", BENCH_L0, num_ops, mode);
                    let config_seed = config.clone();
                    let seed_path = create_seed_dir(seed_prefix.as_str(), move |p| {
                        let engine = setup_engine_at_path(p, &config_seed);
                        let cf = engine.default_column_family();

                        // Write half the data.
                        for i in 0..(num_ops / 2) {
                            engine
                                .put(cf, &keys_seed[i], &vals_seed[i])
                                .expect("put failed");
                        }

                        // Flush memtable to L0.
                        engine.flush().expect("flush failed");

                        // Write remaining data (stays in WAL).
                        for i in (num_ops / 2)..num_ops {
                            engine
                                .put(cf, &keys_seed[i], &vals_seed[i])
                                .expect("put failed");
                        }

                        drop(engine);
                    });

                    let case = tier3::Tier3OpenCase::from_seed(seed_path, config.clone());
                    tier3_bench_open!(b, case, move |engine| {
                        // Validate all data recovered.
                        let cf = engine.default_column_family();
                        for i in (0..num_ops).step_by(5_000) {
                            let key = black_box(&keys_ref[i]);
                            let val = engine.get(cf, key).expect("get failed");
                            assert!(val.is_some(), "key {} not recovered", i);
                        }
                    });
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

                let config = BenchEngineConfig {
                    storage_mode: mode,
                    enable_compaction: false,
                    durability: Durability::Steady,
                    ..Default::default()
                };

                let keys_seed = keys_ref;
                let vals_seed = vals_ref;
                let seed_prefix = format!("{}_seed_small_vals_{}", BENCH_SMALL_VALS, mode);
                let config_seed = config.clone();
                let seed_path = create_seed_dir(seed_prefix.as_str(), move |p| {
                    let engine = setup_engine_at_path(p, &config_seed);
                    let cf = engine.default_column_family();

                    for i in 0..op_count {
                        engine
                            .put(cf, &keys_seed[i], &vals_seed[i])
                            .expect("put failed");
                    }

                    drop(engine);
                });

                let case = tier3::Tier3OpenCase::from_seed(seed_path, config.clone());
                tier3_bench_open!(b, case, move |engine| {
                    let cf = engine.default_column_family();
                    let key = black_box(&keys_ref[25_000]);
                    black_box(engine.get(cf, key).expect("get failed"));
                });
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

                let config = BenchEngineConfig {
                    storage_mode: mode,
                    enable_compaction: false,
                    durability: Durability::Steady,
                    ..Default::default()
                };

                let keys_seed = keys_ref;
                let vals_seed = vals_ref;
                let seed_prefix = format!("{}_seed_large_vals_{}", BENCH_LARGE_VALS, mode);
                let config_seed = config.clone();
                let seed_path = create_seed_dir(seed_prefix.as_str(), move |p| {
                    let engine = setup_engine_at_path(p, &config_seed);
                    let cf = engine.default_column_family();

                    for i in 0..op_count {
                        engine
                            .put(cf, &keys_seed[i], &vals_seed[i])
                            .expect("put failed");
                    }

                    drop(engine);
                });

                let case = tier3::Tier3OpenCase::from_seed(seed_path, config.clone());
                tier3_bench_open!(b, case, move |engine| {
                    let cf = engine.default_column_family();
                    let key = black_box(&keys_ref[25_000]);
                    black_box(engine.get(cf, key).expect("get failed"));
                });
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
