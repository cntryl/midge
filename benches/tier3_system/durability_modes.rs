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
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use criterion_helper::criterion_config;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;
use std::thread;
use ycsb_common::*;

// ============================================================================
// Configuration
// ============================================================================

const OPS_PER_THREAD: usize = 5_000;
const RECORD_COUNT: usize = 25_000;

// ============================================================================
// Missing functions
// ============================================================================

fn load_data(engine: &MidgeEngine, count: usize) {
    let keys = (0..count).map(generate_key).collect::<Vec<_>>();
    let values = pregen_values(count, 42);
    load_data_batched(engine, &keys, &values, BATCH_SIZE);
}

fn generate_value(key_id: usize, random: u64) -> Bytes {
    // Simple value generation: key_id + random
    Bytes::from(format!("value_{}_{}", key_id, random))
}

// ============================================================================
// Database Setup - Durability Modes
// ============================================================================

fn setup_db_with_wal_sync(db_name: &str, wal_sync: bool) -> MidgeEngine {
    let path = std::env::temp_dir().join(format!("midge_durability_{}", db_name));
    let _ = std::fs::remove_dir_all(&path);

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: path },
        memtable_size: 8 * 1024 * 1024,
        enable_compaction: false,
        wal_sync,
        ..Default::default()
    };

    let engine = MidgeEngine::open(opts).unwrap();
    load_data(&engine, RECORD_COUNT);
    engine
}

/// Workload A variant: 50% read, 50% write
fn run_workload_a_variant(engine: &MidgeEngine, operations: usize) {
    let cf = engine.default_column_family();
    let mut rng = StdRng::seed_from_u64(12345);
    let zipfian = ZipfianGenerator::new(RECORD_COUNT, 0.99);

    for _ in 0..operations {
        let key_id = zipfian.next(&mut rng);
        let key = generate_key(key_id);

        if rng.random_bool(0.5) {
            // Read operation
            let _ = black_box(engine.get(&cf, &key));
        } else {
            // Write operation
            let value = generate_value(key_id, rng.random());
            let _ = engine.put(&cf, &key, &value);
        }
    }
}

// ============================================================================
// Benchmarks
// ============================================================================

/// Async WAL: Baseline - no fsync, writes buffered
fn bench_durability_async_wal(c: &mut Criterion) {
    let mut group = c.benchmark_group("durability_async_wal");
    group.throughput(Throughput::Elements(OPS_PER_THREAD as u64));

    group.bench_function("50_50_workload", |b| {
        b.iter_batched(
            || setup_db_with_wal_sync("async_baseline", false),
            |engine| {
                run_workload_a_variant(&engine, OPS_PER_THREAD);
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
    group.throughput(Throughput::Elements(OPS_PER_THREAD as u64));
    group.sample_size(10); // Fewer samples since it's slow

    group.bench_function("50_50_workload", |b| {
        b.iter_batched(
            || setup_db_with_wal_sync("sync_every", true),
            |engine| {
                run_workload_a_variant(&engine, OPS_PER_THREAD);
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

    for (mode_name, wal_sync) in &[("async", false), ("sync_every", true)] {
        let total_ops = 4 * OPS_PER_THREAD;
        group.throughput(Throughput::Elements(total_ops as u64));
        group.sample_size(if *wal_sync { 5 } else { 20 });

        group.bench_with_input(
            BenchmarkId::from_parameter(*mode_name),
            mode_name,
            |b, &mode_name| {
                b.iter_batched(
                    || {
                        setup_db_with_wal_sync(
                            &format!("concurrent_{}", mode_name),
                            mode_name == "sync_every",
                        )
                    },
                    |engine| {
                        let engine = Arc::new(engine);

                        thread::scope(|scope| {
                            for _ in 0..4 {
                                let e = Arc::clone(&engine);
                                scope.spawn(move || {
                                    run_workload_a_variant(&e, OPS_PER_THREAD);
                                });
                            }
                        });

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
fn run_workload_b_variant(engine: &MidgeEngine, operations: usize) {
    let cf = engine.default_column_family();
    let mut rng = StdRng::seed_from_u64(54321);
    let zipfian = ZipfianGenerator::new(RECORD_COUNT, 0.99);

    for _ in 0..operations {
        let key_id = zipfian.next(&mut rng);
        let key = generate_key(key_id);

        if rng.random::<f64>() < 0.95 {
            // Read operation
            let _ = black_box(engine.get(&cf, &key));
        } else {
            // Write operation
            let value = generate_value(key_id, rng.random());
            let _ = engine.put(&cf, &key, &value);
        }
    }
}

fn bench_durability_read_heavy(c: &mut Criterion) {
    let mut group = c.benchmark_group("durability_read_heavy");
    group.throughput(Throughput::Elements(OPS_PER_THREAD as u64));

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
                    |engine| {
                        run_workload_b_variant(&engine, OPS_PER_THREAD);
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
fn run_write_heavy_workload(engine: &MidgeEngine, operations: usize) {
    let cf = engine.default_column_family();
    let mut rng = StdRng::seed_from_u64(99999);

    for i in 0..operations {
        let key = generate_key(RECORD_COUNT + i);
        let value = generate_value(i, rng.random());
        let _ = engine.put(&cf, &key, &value);
    }
}

fn bench_durability_write_heavy(c: &mut Criterion) {
    let mut group = c.benchmark_group("durability_write_heavy");
    group.throughput(Throughput::Elements(OPS_PER_THREAD as u64));

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
                    |engine| {
                        run_write_heavy_workload(&engine, OPS_PER_THREAD);
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
    name = durability_modes;
    config = criterion_config();
    targets =
        bench_durability_async_wal,
        bench_durability_wal_sync_every,
        bench_durability_concurrent,
        bench_durability_read_heavy,
        bench_durability_write_heavy
}
criterion_main!(durability_modes);
