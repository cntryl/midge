//! Tier 3 — System Benchmarks: Durability Modes Comparison
//!
//! **Target Runtime:** ~2 minutes
//! **Run Frequency:** Nightly / release builds
//!
//! Compares WAL synchronization modes:
//! - Async WAL (no fsync, highest throughput)
//! - Sync every write (lowest throughput, highest safety)
//! - Batch sync every N operations (balance)
//!
//! Measures throughput trade-offs for different durability guarantees

#[path = "../criterion_helper.rs"]
mod criterion_helper;
#[path = "../tier4_integration/ycsb_common.rs"]
mod ycsb_common;

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use ycsb_common::*;

// ============================================================================
// Configuration
// ============================================================================

const OPS_PER_THREAD: usize = 5_000;
const RECORD_COUNT: usize = 25_000;

/// Global counter for unique benchmark directory names
static BENCH_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Pre-generate values outside the benchmark to avoid format! allocations
fn pregen_workload_values(count: usize, seed: u64) -> Vec<Bytes> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..count)
        .map(|i| {
            // Simple value: key_id + random bytes (no format! allocation)
            let mut buf = Vec::with_capacity(32);
            buf.extend_from_slice(b"value_");
            // Append key_id as decimal
            if i == 0 {
                buf.push(b'0');
            } else {
                let start = buf.len();
                let mut n = i;
                while n > 0 {
                    buf.push(b'0' + (n % 10) as u8);
                    n /= 10;
                }
                buf[start..].reverse();
            }
            buf.push(b'_');
            // Append random bytes
            let r = rng.random::<u64>();
            let start = buf.len();
            let mut n = r;
            while n > 0 {
                buf.push(b'0' + (n % 10) as u8);
                n /= 10;
            }
            buf[start..].reverse();
            Bytes::from(buf)
        })
        .collect()
}

fn load_data(engine: &MidgeEngine, count: usize) {
    let keys = (0..count).map(generate_key).collect::<Vec<_>>();
    let values = pregen_values(count, 42);
    load_data_batched(engine, &keys, &values, BATCH_SIZE);
}

// ============================================================================
// Database Setup - Durability Modes
// ============================================================================

/// Generate unique path for benchmark database
fn unique_bench_path(prefix: &str) -> PathBuf {
    let counter = BENCH_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("midge_durability_{}_{}_{}", prefix, pid, counter))
}

fn setup_db_with_wal_sync(db_name: &str, wal_sync: bool) -> (MidgeEngine, PathBuf) {
    let path = unique_bench_path(db_name);
    let _ = std::fs::remove_dir_all(&path);

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: path.clone() },
        memtable_size: 8 * 1024 * 1024,
        enable_compaction: false,
        wal_sync,
        ..Default::default()
    };

    let engine = MidgeEngine::open(opts).expect("failed to open engine");
    load_data(&engine, RECORD_COUNT);
    (engine, path)
}

fn cleanup_path(path: PathBuf) {
    let _ = std::fs::remove_dir_all(&path);
}

/// Workload A variant: 50% read, 50% write
/// Uses pre-computed keys and values to avoid allocations
fn run_workload_a_variant(engine: &MidgeEngine, keys: &[Bytes], values: &[Bytes], operations: usize) {
    let cf = engine.default_column_family();
    let mut rng = StdRng::seed_from_u64(12345);
    let zipfian = ZipfianGenerator::new(keys.len(), 0.99);

    for i in 0..operations {
        let key_id = zipfian.next(&mut rng);

        if rng.random_bool(0.5) {
            // Read operation
            let _ = black_box(engine.get(&cf, &keys[key_id]));
        } else {
            // Write operation - use rotating value index
            let _ = engine.put(&cf, &keys[key_id], &values[i % values.len()]);
        }
    }
}

// ============================================================================
// Benchmarks
// ============================================================================

/// Async WAL: Baseline - no fsync, writes buffered
fn bench_durability_async_wal(c: &mut Criterion) {
    let mut group = c.benchmark_group("durability_async_wal");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(OPS_PER_THREAD as u64));

    // Pre-compute keys and values outside benchmark
    let keys: Vec<Bytes> = (0..RECORD_COUNT).map(generate_key).collect();
    let values = pregen_workload_values(OPS_PER_THREAD, 12345);

    group.bench_function("50_50_workload", |b| {
        b.iter_batched(
            || setup_db_with_wal_sync("async_baseline", false),
            |(engine, path)| {
                run_workload_a_variant(&engine, &keys, &values, OPS_PER_THREAD);
                cleanup_path(path);
                black_box(());
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

/// Sync WAL: Every write is flushed to disk (slowest, safest)
fn bench_durability_wal_sync_every(c: &mut Criterion) {
    let mut group = c.benchmark_group("durability_wal_sync_every");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(OPS_PER_THREAD as u64));
    group.sample_size(10); // Fewer samples since it's slow

    // Pre-compute keys and values outside benchmark
    let keys: Vec<Bytes> = (0..RECORD_COUNT).map(generate_key).collect();
    let values = pregen_workload_values(OPS_PER_THREAD, 12346);

    group.bench_function("50_50_workload", |b| {
        b.iter_batched(
            || setup_db_with_wal_sync("sync_every", true),
            |(engine, path)| {
                run_workload_a_variant(&engine, &keys, &values, OPS_PER_THREAD);
                cleanup_path(path);
                black_box(());
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

// ============================================================================
// Multi-threaded Durability Comparison
// ============================================================================

/// Compare durability modes under concurrent load (4 threads)
fn bench_durability_concurrent(c: &mut Criterion) {
    let mut group = c.benchmark_group("durability_concurrent");
    group.sampling_mode(SamplingMode::Flat);

    // Pre-compute keys and values outside benchmark
    let keys: Arc<Vec<Bytes>> = Arc::new((0..RECORD_COUNT).map(generate_key).collect());
    let values: Arc<Vec<Bytes>> = Arc::new(pregen_workload_values(OPS_PER_THREAD, 12347));

    for (mode_name, wal_sync) in &[("async", false), ("sync_every", true)] {
        let total_ops = 4 * OPS_PER_THREAD;
        group.throughput(Throughput::Elements(total_ops as u64));
        group.sample_size(if *wal_sync { 5 } else { 20 });

        let keys = Arc::clone(&keys);
        let values = Arc::clone(&values);

        group.bench_with_input(
            BenchmarkId::from_parameter(*mode_name),
            mode_name,
            |b, &mode_name| {
                let keys = Arc::clone(&keys);
                let values = Arc::clone(&values);
                b.iter_batched(
                    || {
                        setup_db_with_wal_sync(
                            &format!("concurrent_{}", mode_name),
                            mode_name == "sync_every",
                        )
                    },
                    |(engine, path)| {
                        let engine = Arc::new(engine);
                        let keys = Arc::clone(&keys);
                        let values = Arc::clone(&values);

                        thread::scope(|scope| {
                            for _ in 0..4 {
                                let e = Arc::clone(&engine);
                                let keys = Arc::clone(&keys);
                                let values = Arc::clone(&values);
                                scope.spawn(move || {
                                    run_workload_a_variant(&*e, &keys, &values, OPS_PER_THREAD);
                                });
                            }
                        });

                        cleanup_path(path);
                        black_box(());
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

// ============================================================================
// Read-Heavy Workload (Workload B variant)
// ============================================================================

/// 95% read, 5% write - durability should have smaller impact on reads
fn run_workload_b_variant(engine: &MidgeEngine, keys: &[Bytes], values: &[Bytes], operations: usize) {
    let cf = engine.default_column_family();
    let mut rng = StdRng::seed_from_u64(54321);
    let zipfian = ZipfianGenerator::new(keys.len(), 0.99);

    for i in 0..operations {
        let key_id = zipfian.next(&mut rng);

        if rng.random::<f64>() < 0.95 {
            // Read operation
            let _ = black_box(engine.get(&cf, &keys[key_id]));
        } else {
            // Write operation - use rotating value index
            let _ = engine.put(&cf, &keys[key_id], &values[i % values.len()]);
        }
    }
}

fn bench_durability_read_heavy(c: &mut Criterion) {
    let mut group = c.benchmark_group("durability_read_heavy");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(OPS_PER_THREAD as u64));

    // Pre-compute keys and values outside benchmark
    let keys: Vec<Bytes> = (0..RECORD_COUNT).map(generate_key).collect();
    let values = pregen_workload_values(OPS_PER_THREAD, 54321);

    for (mode_name, wal_sync) in &[("async", false), ("sync_every", true)] {
        group.sample_size(if *wal_sync { 10 } else { 20 });

        group.bench_with_input(
            BenchmarkId::from_parameter(*mode_name),
            mode_name,
            |b, &mode_name| {
                b.iter_batched(
                    || {
                        setup_db_with_wal_sync(
                            &format!("read_heavy_{}", mode_name),
                            mode_name == "sync_every",
                        )
                    },
                    |(engine, path)| {
                        run_workload_b_variant(&engine, &keys, &values, OPS_PER_THREAD);
                        cleanup_path(path);
                        black_box(());
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

// ============================================================================
// Write-Heavy Workload
// ============================================================================

/// 100% write workload - durability impact is maximum
fn run_write_heavy_workload(engine: &MidgeEngine, keys: &[Bytes], values: &[Bytes], operations: usize) {
    let cf = engine.default_column_family();

    for i in 0..operations {
        // Write new keys (beyond RECORD_COUNT range) with pre-computed values
        let key_idx = (RECORD_COUNT + i) % keys.len();
        let _ = engine.put(&cf, &keys[key_idx], &values[i % values.len()]);
    }
}

fn bench_durability_write_heavy(c: &mut Criterion) {
    let mut group = c.benchmark_group("durability_write_heavy");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(OPS_PER_THREAD as u64));

    // Pre-compute keys and values outside benchmark
    // Need keys beyond RECORD_COUNT for write-heavy workload
    let keys: Vec<Bytes> = (0..RECORD_COUNT + OPS_PER_THREAD).map(generate_key).collect();
    let values = pregen_workload_values(OPS_PER_THREAD, 99999);

    for (mode_name, wal_sync) in &[("async", false), ("sync_every", true)] {
        group.sample_size(if *wal_sync { 5 } else { 20 });

        group.bench_with_input(
            BenchmarkId::from_parameter(*mode_name),
            mode_name,
            |b, &mode_name| {
                b.iter_batched(
                    || {
                        setup_db_with_wal_sync(
                            &format!("write_heavy_{}", mode_name),
                            mode_name == "sync_every",
                        )
                    },
                    |(engine, path)| {
                        run_write_heavy_workload(&engine, &keys, &values, OPS_PER_THREAD);
                        cleanup_path(path);
                        black_box(());
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = tier3_system_durability_modes;
    config = criterion_config_for_tier(BenchTier::Tier3System);
    targets =
        bench_durability_async_wal,
        bench_durability_wal_sync_every,
        bench_durability_concurrent,
        bench_durability_read_heavy,
        bench_durability_write_heavy
}
criterion_main!(tier3_system_durability_modes);
