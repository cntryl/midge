// This file was moved to `stress/pruned/tier3_system_durability_modes.rs`.
// It contained both single-shot comparisons and heavier stress scenarios. The heavy
// concurrent durability workloads have been moved to the stress harness; lightweight
// single-shot durability comparisons can be re-added to Tier-3 as isolated benches.

// Original content preserved at `stress/pruned/tier3_system_durability_modes.rs` for migration.

#[allow(unused)]
const _TIER3_GUARD: () = {
    // Tier-3 benches must use bench_common::tier3 APIs and `tier3_bench!`/`tier3_bench_restore!`.
};

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

#[allow(dead_code)]
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
        let _ = engine.put(cf, &keys[key_idx], &values[val_idx]);
    }
}

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
            engine.put(cf, key, &values[val_idx]).unwrap();
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
            let _ = black_box(engine.get(cf, &keys[key_idx]));
        } else {
            // Write
            let val_idx = i % values.len();
            let _ = engine.put(cf, &keys[key_idx], &values[val_idx]);
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

// bench_durability_concurrent was pruned — moved to `stress/pruned/tier3_system_durability_modes.rs`.
// See stress/pruned file for the full multi-threaded durability scenario.
// ============================================================================
// Write-Heavy Workload (pruned)
// ============================================================================

// The heavy write-heavy workload has been moved to `stress/pruned/tier3_system_durability_modes.rs`.
// The helper implementation is preserved in the pruned version; this file keeps a stub comment
// to avoid duplicate definitions while preserving history.

// bench_durability_write_heavy was pruned — moved to `stress/pruned/tier3_system_durability_modes.rs`.

criterion_group! {
    name = tier3_system_durability_modes;
    config = criterion_config_for_tier(BenchTier::Tier3System);
    targets =
        bench_durability_async_wal,
        bench_durability_wal_sync_every
}
criterion_main!(tier3_system_durability_modes);
