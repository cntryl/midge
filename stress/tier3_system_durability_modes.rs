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

#[path = "./common/tier3_harness.rs"]
mod tier3;

use bench_common::{
    create_seed_dir, make_key, make_value_fixed, setup_engine_at_path, BenchEngineConfig,
    BenchStorageMode, DURABLE_STORAGE_MODES, VALUE_SIZE,
};

use bytes::Bytes;
use cntryl_midge::{Durability, MidgeEngine, WriteBatch};
use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
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

/// Return bench parameters (ops_per_thread, record_count, batch_size, value_size).
/// Set `MIDGE_BENCH_QUICK` in the environment to run a much smaller, faster bench
/// suitable for local iteration.
fn bench_params() -> (usize, usize, usize, usize) {
    if std::env::var("MIDGE_BENCH_QUICK").is_ok() {
        // Quick mode: keep everything small for fast iteration
        (500, 1_000, 50, 64)
    } else {
        (OPS_PER_THREAD, RECORD_COUNT, BATCH_SIZE, VALUE_SIZE)
    }
}

// ============================================================================
// Database Setup - Durability Modes
// ============================================================================

fn config_for_mode(mode: BenchStorageMode, wal_sync: bool) -> BenchEngineConfig {
    let durability = if wal_sync {
        Durability::Strict
    } else {
        Durability::Steady
    };

    BenchEngineConfig {
        storage_mode: mode,
        durability,
        enable_compaction: false,
        memtable_size: Some(8 * 1024 * 1024),
        ..Default::default()
    }
}

fn load_data_batched(engine: &MidgeEngine, keys: &[Bytes], values: &[Bytes], batch_size: usize) {
    // Important: using `put()` here can turn into "sludge" under AckPolicy::AfterLocalDurable
    // because each call will wait for local durability. Batching keeps this setup phase
    // from dominating wall-clock time while preserving the same dataset.
    let cf = engine.default_column_family();
    for chunk in keys.chunks(batch_size) {
        let mut batch = WriteBatch::new();
        for (i, key) in chunk.iter().enumerate() {
            let val_idx = i % values.len();
            batch.put_cf(cf.id(), key.clone(), values[val_idx].clone());
        }
        engine.write_batch(&batch).unwrap();
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
    let (ops, records, batch, value_size) = bench_params();

    let mut group = c.benchmark_group("durability/async_wal");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(ops as u64));

    // Pre-compute keys and values outside benchmark
    let keys: Vec<Bytes> = (0..records).map(make_key).collect();
    let values: Vec<_> = (0..ops).map(|_| make_value_fixed(value_size)).collect();

    for mode in DURABLE_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("50_50_workload", mode.as_str()),
            &mode,
            |b, &mode| {
                let keys_ref = &keys;
                let values_ref = &values;

                // Exclude expensive DB creation + dataset load from timing.
                let config = config_for_mode(mode, false);
                let seed_prefix = format!("durability_async_seed_{}", mode.as_str());
                let seed_path = create_seed_dir(seed_prefix.as_str(), |p| {
                    let engine = setup_engine_at_path(p, &config);
                    load_data_batched(&engine, keys_ref, values_ref, batch);
                    drop(engine);
                });

                let case = tier3::Tier3Case::from_seed(seed_path, config);

                tier3_bench!(b, case, move |engine| {
                    run_mixed_workload(&engine, keys_ref, values_ref, ops);
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Sync WAL Benchmark (Every write flushed)
// ============================================================================

fn bench_durability_wal_sync_every(c: &mut Criterion) {
    let (ops, records, batch, value_size) = bench_params();

    let mut group = c.benchmark_group("durability/sync_wal");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(ops as u64));

    // Pre-compute keys and values outside benchmark
    let keys: Vec<Bytes> = (0..records).map(make_key).collect();
    let values: Vec<_> = (0..ops).map(|_| make_value_fixed(value_size)).collect();

    for mode in DURABLE_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("50_50_workload", mode.as_str()),
            &mode,
            |b, &mode| {
                let keys_ref = &keys;
                let values_ref = &values;

                // Exclude expensive DB creation + dataset load from timing.
                let config = config_for_mode(mode, true);
                let seed_prefix = format!("durability_sync_seed_{}", mode.as_str());
                let seed_path = create_seed_dir(seed_prefix.as_str(), |p| {
                    let engine = setup_engine_at_path(p, &config);
                    load_data_batched(&engine, keys_ref, values_ref, batch);
                    drop(engine);
                });

                let case = tier3::Tier3Case::from_seed(seed_path, config);

                tier3_bench!(b, case, move |engine| {
                    run_mixed_workload(&engine, keys_ref, values_ref, ops);
                });
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
        bench_durability_wal_sync_every
}
criterion_main!(tier3_system_durability_modes);
