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

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

/// Global counter for unique benchmark directory names
static BENCH_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Key size in bytes
const KEY_SIZE: usize = 14;
/// Default value size
const VALUE_SIZE: usize = 128;
/// Bytes per operation
const BYTES_PER_OP: u64 = (KEY_SIZE + VALUE_SIZE) as u64;

/// Generate unique path for benchmark database
fn unique_bench_path(prefix: &str) -> PathBuf {
    let counter = BENCH_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("midge_bench_recovery_{}_{}_{}", prefix, pid, counter))
}

#[inline]
fn make_key(i: usize) -> Bytes {
    let mut key = vec![0u8; KEY_SIZE];
    key[..4].copy_from_slice(b"key_");
    let mut n = i;
    for j in (4..KEY_SIZE).rev() {
        key[j] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    Bytes::from(key)
}

#[inline]
fn make_value_fixed(size: usize) -> Bytes {
    Bytes::from(vec![b'x'; size])
}

fn precompute_kv(n: usize, value_size: usize) -> (Vec<Bytes>, Vec<Bytes>) {
    let mut keys = Vec::with_capacity(n);
    let mut vals = Vec::with_capacity(n);
    for i in 0..n {
        keys.push(make_key(i));
        vals.push(make_value_fixed(value_size));
    }
    (keys, vals)
}

fn setup_db_at_path(path: &std::path::Path, enable_wal_sync: bool) -> MidgeEngine {
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
        group.bench_with_input(
            BenchmarkId::new("replay", op_count),
            &op_count,
            |b, &num_ops| {
                b.iter_batched(
                    || {
                        // Setup and prefill database
                        let db_path = unique_bench_path("replay");
                        let _ = std::fs::remove_dir_all(&db_path);

                        let engine = setup_db_at_path(&db_path, false);
                        let cf = engine.default_column_family();

                        // Write records to create WAL entries
                        for i in 0..num_ops {
                            engine.put(&cf, &keys[i], &vals[i]).unwrap();
                        }

                        // Simulate crash (drop engine, don't clean up DB)
                        drop(engine);
                        db_path
                    },
                    |db_path| {
                        // Measure recovery time (WAL replay)
                        let engine = setup_db_at_path(&db_path, false);

                        // Validate some data integrity
                        let cf = engine.default_column_family();
                        for i in (0..num_ops).step_by(1_000) {
                            let val = engine.get(&cf, &keys[i]).unwrap();
                            assert!(val.is_some(), "key not recovered: {}", i);
                        }

                        engine // prevent Drop during timing
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
fn bench_recovery_with_wal_sync(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_recovery_wal_sync");
    group.sampling_mode(SamplingMode::Flat);

    for &op_count in &[10_000usize, 50_000] {
        let (keys, vals) = precompute_kv(op_count, VALUE_SIZE);
        let bytes_total = (op_count as u64) * BYTES_PER_OP;

        group.throughput(Throughput::Bytes(bytes_total));
        group.bench_with_input(
            BenchmarkId::new("replay_sync", op_count),
            &op_count,
            |b, &num_ops| {
                b.iter_batched(
                    || {
                        let db_path = unique_bench_path("sync");
                        let _ = std::fs::remove_dir_all(&db_path);

                        let engine = setup_db_at_path(&db_path, true);
                        let cf = engine.default_column_family();

                        for i in 0..num_ops {
                            engine.put(&cf, &keys[i], &vals[i]).unwrap();
                        }

                        drop(engine);
                        db_path
                    },
                    |db_path| {
                        let engine = setup_db_at_path(&db_path, true);

                        // Quick validation
                        let cf = engine.default_column_family();
                        for i in (0..num_ops).step_by(10_000) {
                            black_box(engine.get(&cf, &keys[i]).unwrap());
                        }

                        engine // prevent Drop during timing
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
fn bench_recovery_with_l0_data(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_recovery_with_l0");
    group.sampling_mode(SamplingMode::Flat);

    for &op_count in &[50_000usize, 100_000] {
        let (keys, vals) = precompute_kv(op_count, VALUE_SIZE);
        let bytes_total = (op_count as u64) * BYTES_PER_OP;

        group.throughput(Throughput::Bytes(bytes_total));
        group.bench_with_input(
            BenchmarkId::new("replay_l0", op_count),
            &op_count,
            |b, &num_ops| {
                b.iter_batched(
                    || {
                        let db_path = unique_bench_path("l0");
                        let _ = std::fs::remove_dir_all(&db_path);

                        let engine = setup_db_at_path(&db_path, false);
                        let cf = engine.default_column_family();

                        // Write half the data
                        for i in 0..(num_ops / 2) {
                            engine.put(&cf, &keys[i], &vals[i]).unwrap();
                        }

                        // Flush memtable to L0
                        engine.flush().unwrap();

                        // Write remaining data (stays in WAL)
                        for i in (num_ops / 2)..num_ops {
                            engine.put(&cf, &keys[i], &vals[i]).unwrap();
                        }

                        drop(engine);
                        db_path
                    },
                    |db_path| {
                        let engine = setup_db_at_path(&db_path, false);

                        // Validate all data recovered
                        let cf = engine.default_column_family();
                        for i in (0..num_ops).step_by(5_000) {
                            let val = engine.get(&cf, &keys[i]).unwrap();
                            assert!(val.is_some(), "key {} not recovered", i);
                        }

                        engine // prevent Drop during timing
                    },
                    BatchSize::LargeInput,
                )
            },
        );
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
    group.bench_function("recovery_small_values_100k", |b| {
        b.iter_batched(
            || {
                let db_path = unique_bench_path("small_vals");
                let _ = std::fs::remove_dir_all(&db_path);

                let engine = setup_db_at_path(&db_path, false);
                let cf = engine.default_column_family();

                for i in 0..op_count {
                    engine.put(&cf, &keys_small[i], &vals_small[i]).unwrap();
                }

                drop(engine);
                db_path
            },
            |db_path| {
                let engine = setup_db_at_path(&db_path, false);

                let cf = engine.default_column_family();
                black_box(engine.get(&cf, &keys_small[50_000]).unwrap());

                engine // prevent Drop during timing
            },
            BatchSize::LargeInput,
        )
    });

    // Large values (1KB)
    let large_value_size = 1024usize;
    let (keys_large, vals_large) = precompute_kv(op_count, large_value_size);
    let large_bytes = (op_count as u64) * (KEY_SIZE + large_value_size) as u64;

    group.throughput(Throughput::Bytes(large_bytes));
    group.bench_function("recovery_large_values_100k", |b| {
        b.iter_batched(
            || {
                let db_path = unique_bench_path("large_vals");
                let _ = std::fs::remove_dir_all(&db_path);

                let engine = setup_db_at_path(&db_path, false);
                let cf = engine.default_column_family();

                for i in 0..op_count {
                    engine.put(&cf, &keys_large[i], &vals_large[i]).unwrap();
                }

                drop(engine);
                db_path
            },
            |db_path| {
                let engine = setup_db_at_path(&db_path, false);

                let cf = engine.default_column_family();
                black_box(engine.get(&cf, &keys_large[50_000]).unwrap());

                engine // prevent Drop during timing
            },
            BatchSize::LargeInput,
        )
    });

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
