//! Tier 3 — System Benchmarks: Crash Recovery
//!
//! **Target Runtime:** 2-5 minutes
//! **Run Frequency:** Nightly / release builds
//!
//! Measures WAL replay performance and recovery correctness:
//! - Time to replay WAL after crash/restart
//! - Replay throughput (ops/sec)
//! - Data integrity validation
//! - Different dataset sizes (10K, 100K, 500K records)

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;
use std::path::Path;
use std::time::Instant;

fn setup_db_at_path(path: &Path, enable_wal_sync: bool) -> MidgeEngine {
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: path.to_path_buf(),
        },
        memtable_size: 4 * 1024 * 1024,
        enable_compaction: false,
        wal_sync: enable_wal_sync,
        ..Default::default()
    };
    MidgeEngine::open(opts).unwrap()
}

fn make_key(i: usize) -> Bytes {
    Bytes::from(format!("key_{:010}", i))
}

fn make_value(i: usize) -> Bytes {
    Bytes::from(vec![b'x'; 128 + (i % 100)])
}

// ============================================================================
// WAL Recovery Performance
// ============================================================================

/// Benchmark WAL replay performance after crash
///
/// Simulates crash by:
/// 1. Writing N records to database (with some to L0)
/// 2. Dropping engine (simulating crash, leaving WAL on disk)
/// 3. Reopening database (triggers WAL replay)
/// 4. Measuring replay throughput
fn bench_recovery_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_recovery_throughput");
    group.sample_size(10); // Recovery is slow; fewer samples needed

    for &op_count in &[10_000, 100_000, 500_000] {
        group.bench_with_input(
            BenchmarkId::new("replay", op_count),
            &op_count,
            |b, &num_ops| {
                b.iter_batched(
                    || {
                        // Step 1: Setup and prefill database
                        let db_path =
                            std::env::temp_dir().join(format!("midge_recovery_replay_{}", num_ops));
                        let _ = std::fs::remove_dir_all(&db_path);

                        let engine = setup_db_at_path(&db_path, false); // async WAL
                        let cf = engine.default_column_family();

                        // Write records to create WAL entries
                        for i in 0..num_ops {
                            engine.put(&cf, &make_key(i), &make_value(i)).unwrap();
                        }

                        // Step 2: Simulate crash (drop engine, don't clean up DB)
                        drop(engine);

                        db_path
                    },
                    |db_path| {
                        // Step 3: Measure recovery time (WAL replay)
                        let start = Instant::now();
                        let engine = setup_db_at_path(&db_path, false); // Triggers WAL replay
                        let elapsed = start.elapsed();

                        // Step 4: Validate some data integrity
                        let cf = engine.default_column_family();
                        for i in (0..num_ops).step_by(1_000) {
                            let val = engine
                                .get(&cf, &make_key(i))
                                .expect("recovery data missing");
                            assert!(val.is_some(), "key not recovered: {}", i);
                        }

                        black_box((elapsed, engine));
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }

    group.finish();
}

// ============================================================================
// Recovery with WAL Sync Enabled
// ============================================================================

/// Benchmark recovery performance with synchronous WAL
/// This should be faster since all writes are flushed to disk
fn bench_recovery_with_wal_sync(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_recovery_wal_sync");
    group.sample_size(10);

    for &op_count in &[10_000, 100_000] {
        group.bench_with_input(
            BenchmarkId::new("replay_sync", op_count),
            &op_count,
            |b, &num_ops| {
                b.iter_batched(
                    || {
                        let db_path =
                            std::env::temp_dir().join(format!("midge_recovery_sync_{}", num_ops));
                        let _ = std::fs::remove_dir_all(&db_path);

                        let engine = setup_db_at_path(&db_path, true); // sync WAL
                        let cf = engine.default_column_family();

                        for i in 0..num_ops {
                            engine.put(&cf, &make_key(i), &make_value(i)).unwrap();
                        }

                        drop(engine);
                        db_path
                    },
                    |db_path| {
                        let start = Instant::now();
                        let engine = setup_db_at_path(&db_path, true);
                        let elapsed = start.elapsed();

                        // Quick validation
                        let cf = engine.default_column_family();
                        for i in (0..num_ops).step_by(10_000) {
                            let _ = engine.get(&cf, &make_key(i)).unwrap();
                        }

                        black_box((elapsed, engine));
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }

    group.finish();
}

// ============================================================================
// Partial Recovery (Some Data in L0)
// ============================================================================

/// Benchmark recovery when some writes reached L0 and some are in WAL
/// More complex case: requires merging L0 + WAL data
fn bench_recovery_with_l0_data(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_recovery_with_l0");
    group.sample_size(10);

    for &op_count in &[50_000, 250_000] {
        group.bench_with_input(
            BenchmarkId::new("replay_l0", op_count),
            &op_count,
            |b, &num_ops| {
                b.iter_batched(
                    || {
                        let db_path =
                            std::env::temp_dir().join(format!("midge_recovery_l0_{}", num_ops));
                        let _ = std::fs::remove_dir_all(&db_path);

                        let engine = setup_db_at_path(&db_path, false);
                        let cf = engine.default_column_family();

                        // Write half the data
                        for i in 0..(num_ops / 2) {
                            engine.put(&cf, &make_key(i), &make_value(i)).unwrap();
                        }

                        // Flush memtable to L0
                        engine.flush().unwrap();

                        // Write remaining data (stays in WAL)
                        for i in (num_ops / 2)..num_ops {
                            engine.put(&cf, &make_key(i), &make_value(i)).unwrap();
                        }

                        drop(engine);
                        db_path
                    },
                    |db_path| {
                        let start = Instant::now();
                        let engine = setup_db_at_path(&db_path, false);
                        let elapsed = start.elapsed();

                        // Validate all data recovered
                        let cf = engine.default_column_family();
                        for i in (0..num_ops).step_by(5_000) {
                            let val = engine.get(&cf, &make_key(i)).unwrap();
                            assert!(val.is_some(), "key {} not recovered", i);
                        }

                        black_box((elapsed, engine));
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }

    group.finish();
}

// ============================================================================
// Recovery Speed Comparison: Empty vs Full WAL
// ============================================================================

/// Compare recovery speed when starting with empty WAL vs large WAL
/// Shows impact of WAL size on recovery time
fn bench_recovery_speed_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_recovery_comparison");
    group.sample_size(10);

    // Test with 100K records written as small writes (tests WAL parsing)
    group.bench_function("recovery_small_writes_100k", |b| {
        b.iter_batched(
            || {
                let db_path = std::env::temp_dir().join("midge_recovery_small_writes");
                let _ = std::fs::remove_dir_all(&db_path);

                let engine = setup_db_at_path(&db_path, false);
                let cf = engine.default_column_family();

                // Many small writes = large WAL
                for i in 0..100_000 {
                    let small_val = make_value(i % 10); // Small values, many writes
                    engine.put(&cf, &make_key(i), &small_val).unwrap();
                }

                drop(engine);
                db_path
            },
            |db_path| {
                let start = Instant::now();
                let engine = setup_db_at_path(&db_path, false);
                let elapsed = start.elapsed();

                let cf = engine.default_column_family();
                let val = engine.get(&cf, &make_key(50_000)).unwrap();
                assert!(val.is_some());

                black_box((elapsed, engine));
            },
            BatchSize::LargeInput,
        )
    });

    // Test with 100K records in large values (tests WAL throughput)
    group.bench_function("recovery_large_writes_100k", |b| {
        b.iter_batched(
            || {
                let db_path = std::env::temp_dir().join("midge_recovery_large_writes");
                let _ = std::fs::remove_dir_all(&db_path);

                let engine = setup_db_at_path(&db_path, false);
                let cf = engine.default_column_family();

                // Many large writes = large WAL with big payloads
                for i in 0..100_000 {
                    let large_val = Bytes::from(vec![b'x'; 1024]); // Large values
                    engine.put(&cf, &make_key(i), &large_val).unwrap();
                }

                drop(engine);
                db_path
            },
            |db_path| {
                let start = Instant::now();
                let engine = setup_db_at_path(&db_path, false);
                let elapsed = start.elapsed();

                let cf = engine.default_column_family();
                let val = engine.get(&cf, &make_key(50_000)).unwrap();
                assert!(val.is_some());

                black_box((elapsed, engine));
            },
            BatchSize::LargeInput,
        )
    });

    group.finish();
}

criterion_group! {
    name = system_recovery;
    config = criterion_config();
    targets =
        bench_recovery_throughput,
        bench_recovery_with_wal_sync,
        bench_recovery_with_l0_data,
        bench_recovery_speed_comparison
}
criterion_main!(system_recovery);
