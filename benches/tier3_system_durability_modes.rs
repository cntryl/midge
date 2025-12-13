//! Tier 3 — System Benchmarks: Durability Modes Comparison
//!
//! **Target Runtime:** ~2 minutes
//! **Run Frequency:** Nightly / release builds
//!
//! Compares WAL synchronization modes:
//! - Async WAL (no fsync, highest throughput)
//! - Sync every write (lowest throughput, highest safety)
//!
//! Measures throughput trade-offs for different durability guarantees
//! across storage modes (LocalDisk and CloudBacked).
//!
//! ## Design Notes
//!
//! - Uses DURABLE_STORAGE_MODES since durability requires persistence
//! - Tests both wal_sync=false (async) and wal_sync=true (sync)
//! - Heavy scenarios are restricted to LocalDisk to keep runtime bounded

#[path = "./criterion_helper.rs"]
mod criterion_helper;

#[path = "./tier3_system_bench_common.rs"]
mod bench_common;

use bench_common::{
    make_key, make_value_fixed, unique_bench_path, BenchStorageMode, DURABLE_STORAGE_MODES,
    VALUE_SIZE,
};

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;
use std::sync::Arc;
use std::thread;

// ============================================================================
// Configuration
// ============================================================================

// Trimmed to keep runtime reasonable while still exercising durability paths.
const OPS_PER_THREAD: usize = 2_000;
const RECORD_COUNT: usize = 10_000;
const BATCH_SIZE: usize = 100;

// ============================================================================
// Database Setup - Durability Modes
// ============================================================================

fn setup_db_with_options(db_name: &str, mode: BenchStorageMode, wal_sync: bool) -> MidgeEngine {
    let path = unique_bench_path(db_name);
    let _ = std::fs::remove_dir_all(&path);

    let storage_mode = match mode {
        BenchStorageMode::Memory => panic!("Durability benchmarks require persistent storage"),
        BenchStorageMode::LocalDisk => StorageMode::LocalDisk { db_path: path },
        BenchStorageMode::CloudBacked => panic!("CloudBacked mode not yet supported in benchmarks"),
    };

    let opts = MidgeOptions {
        storage_mode,
        memtable_size: 8 * 1024 * 1024,
        enable_compaction: false,
        wal_sync,
        ..Default::default()
    };

    MidgeEngine::open(opts).unwrap()
}

fn load_data_batched(engine: &MidgeEngine, keys: &[Bytes], values: &[Bytes]) {
    let cf = engine.default_column_family();
    for chunk in keys.chunks(BATCH_SIZE) {
        for (i, key) in chunk.iter().enumerate() {
            let val_idx = i % values.len();
            engine.put(&cf, key, &values[val_idx]).unwrap();
        }
    }
}

/// 50% read, 50% write workload
fn run_mixed_workload(engine: &MidgeEngine, keys: &[Bytes], values: &[Bytes], operations: usize) {
    let cf = engine.default_column_family();
    // Simple deterministic pattern: even iterations read, odd iterations write
    for i in 0..operations {
        let key_idx = i % keys.len();
        if i % 2 == 0 {
            // Read
            let _ = black_box(engine.get(&cf, &keys[key_idx]));
        } else {
            // Write
            let val_idx = i % values.len();
            let _ = engine.put(&cf, &keys[key_idx], &values[val_idx]);
        }
    }
}

// ============================================================================
// Async WAL Benchmark
// ============================================================================

fn bench_durability_async_wal(c: &mut Criterion) {
    let mut group = c.benchmark_group("durability/async_wal");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(OPS_PER_THREAD as u64));
    group.sample_size(20);

    // Pre-compute keys and values outside benchmark
    let keys: Vec<Bytes> = (0..RECORD_COUNT).map(make_key).collect();
    let values: Vec<_> = (0..OPS_PER_THREAD)
        .map(|_| make_value_fixed(VALUE_SIZE))
        .collect();

    for mode in DURABLE_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("50_50_workload", mode.as_str()),
            &mode,
            |b, &mode| {
                let keys_ref = &keys;
                let values_ref = &values;

                b.iter_batched(
                    || {
                        let engine = setup_db_with_options("async_baseline", mode, false);
                        load_data_batched(&engine, keys_ref, values_ref);
                        engine
                    },
                    |engine| {
                        run_mixed_workload(&engine, keys_ref, values_ref, OPS_PER_THREAD);
                        engine
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }

    group.finish();
}

// ============================================================================
// Sync WAL Benchmark (Every write flushed)
// ============================================================================

fn bench_durability_wal_sync_every(c: &mut Criterion) {
    let mut group = c.benchmark_group("durability/sync_wal");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(OPS_PER_THREAD as u64));
    group.sample_size(10); // Fewer samples since it's slow

    // Pre-compute keys and values outside benchmark
    let keys: Vec<Bytes> = (0..RECORD_COUNT).map(make_key).collect();
    let values: Vec<_> = (0..OPS_PER_THREAD)
        .map(|_| make_value_fixed(VALUE_SIZE))
        .collect();

    for mode in DURABLE_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("50_50_workload", mode.as_str()),
            &mode,
            |b, &mode| {
                let keys_ref = &keys;
                let values_ref = &values;

                b.iter_batched(
                    || {
                        let engine = setup_db_with_options("sync_every", mode, true);
                        load_data_batched(&engine, keys_ref, values_ref);
                        engine
                    },
                    |engine| {
                        run_mixed_workload(&engine, keys_ref, values_ref, OPS_PER_THREAD);
                        engine
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }

    group.finish();
}

// ============================================================================
// Multi-threaded Durability Comparison
// ============================================================================

fn bench_durability_concurrent(c: &mut Criterion) {
    let mut group = c.benchmark_group("durability/concurrent");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(15);

    let total_ops = 4 * OPS_PER_THREAD;
    group.throughput(Throughput::Elements(total_ops as u64));

    // Pre-compute keys and values outside benchmark
    let keys: Arc<Vec<Bytes>> = Arc::new((0..RECORD_COUNT).map(make_key).collect());
    let values: Arc<Vec<Bytes>> = Arc::new(
        (0..OPS_PER_THREAD)
            .map(|_| make_value_fixed(VALUE_SIZE))
            .collect(),
    );

    for mode in DURABLE_STORAGE_MODES {
        // Heavy scenario: skip cloud-backed to keep bench fast
        if !matches!(mode, BenchStorageMode::LocalDisk) {
            continue;
        }

        for &wal_sync in &[false, true] {
            let sync_name = if wal_sync { "sync" } else { "async" };
            let bench_name = format!("{}/{}", mode.as_str(), sync_name);

            let keys = Arc::clone(&keys);
            let values = Arc::clone(&values);

            group.bench_with_input(
                BenchmarkId::new("4threads", &bench_name),
                &(mode, wal_sync),
                |b, &(mode, wal_sync)| {
                    let keys = Arc::clone(&keys);
                    let values = Arc::clone(&values);

                    b.iter_batched(
                        || {
                            let engine = setup_db_with_options("concurrent", mode, wal_sync);
                            // Load data outside timed section
                            let cf = engine.default_column_family();
                            for (i, key) in keys.iter().take(RECORD_COUNT).enumerate() {
                                engine.put(&cf, key, &values[i % values.len()]).unwrap();
                            }
                            Arc::new(engine)
                        },
                        |engine| {
                            let keys = Arc::clone(&keys);
                            let values = Arc::clone(&values);

                            thread::scope(|scope| {
                                for _ in 0..4 {
                                    let e = Arc::clone(&engine);
                                    let keys = Arc::clone(&keys);
                                    let values = Arc::clone(&values);
                                    scope.spawn(move || {
                                        run_mixed_workload(&e, &keys, &values, OPS_PER_THREAD);
                                    });
                                }
                            });

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
// Write-Heavy Workload
// ============================================================================

fn run_write_heavy_workload(
    engine: &MidgeEngine,
    keys: &[Bytes],
    values: &[Bytes],
    operations: usize,
) {
    let cf = engine.default_column_family();
    for i in 0..operations {
        let key_idx = i % keys.len();
        let val_idx = i % values.len();
        let _ = engine.put(&cf, &keys[key_idx], &values[val_idx]);
    }
}

fn bench_durability_write_heavy(c: &mut Criterion) {
    let mut group = c.benchmark_group("durability/write_heavy");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(OPS_PER_THREAD as u64));

    // Pre-compute keys and values outside benchmark
    let keys: Vec<Bytes> = (0..RECORD_COUNT + OPS_PER_THREAD).map(make_key).collect();
    let values: Vec<_> = (0..OPS_PER_THREAD)
        .map(|_| make_value_fixed(VALUE_SIZE))
        .collect();

    for mode in DURABLE_STORAGE_MODES {
        // Heavy scenario: skip cloud-backed to keep bench fast
        if !matches!(mode, BenchStorageMode::LocalDisk) {
            continue;
        }

        for &wal_sync in &[false, true] {
            let sync_name = if wal_sync { "sync" } else { "async" };
            let bench_name = format!("{}/{}", mode.as_str(), sync_name);

            group.sample_size(if wal_sync { 10 } else { 15 });

            group.bench_with_input(
                BenchmarkId::new("100pct_writes", &bench_name),
                &(mode, wal_sync),
                |b, &(mode, wal_sync)| {
                    let keys_ref = &keys;
                    let values_ref = &values;

                    b.iter_batched(
                        || {
                            let engine = setup_db_with_options("write_heavy", mode, wal_sync);
                            load_data_batched(&engine, keys_ref, values_ref);
                            engine
                        },
                        |engine| {
                            run_write_heavy_workload(&engine, keys_ref, values_ref, OPS_PER_THREAD);
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

criterion_group! {
    name = tier3_system_durability_modes;
    config = criterion_config_for_tier(BenchTier::Tier3System);
    targets =
        bench_durability_async_wal,
        bench_durability_wal_sync_every,
        bench_durability_concurrent,
        bench_durability_write_heavy
}
criterion_main!(tier3_system_durability_modes);
