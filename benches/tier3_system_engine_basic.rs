// This file was moved to `stress/pruned/tier3_system_engine_basic.rs`.
// The concurrent variants in this file are better suited to the stress harness.
// Single-op baselines should be placed in Tier-1 or Tier-2 where they are stable.

// Original content preserved at `stress/pruned/tier3_system_engine_basic.rs` for migration.

#[path = "./criterion_helper.rs"]
mod criterion_helper;

#[path = "./tier3_system_bench_common.rs"]
mod bench_common;

use bench_common::{
    make_key, make_value_fixed, precompute_kv, setup_engine_with_mode, ALL_STORAGE_MODES,
    BYTES_PER_OP, KEY_SIZE, VALUE_SIZE,
};

use bytes::Bytes;
use cntryl_midge::WriteBatch;
use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;

// ============================================================================
// Single Put Benchmark
// ============================================================================

fn bench_single_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_basic/single_put");
    group.sampling_mode(SamplingMode::Flat);

    let num_ops = 1_000usize;
    let (keys, vals) = precompute_kv(num_ops, VALUE_SIZE);
    let bytes_total = (num_ops as u64) * BYTES_PER_OP;

    group.throughput(Throughput::Bytes(bytes_total));

    for mode in ALL_STORAGE_MODES {
        group.bench_with_input(BenchmarkId::new("put", mode.as_str()), &mode, |b, &mode| {
            b.iter_batched(
                || setup_engine_with_mode("single_put", mode),
                |engine| {
                    let cf = engine.default_column_family();
                    for i in 0..num_ops {
                        engine.put(cf, &keys[i], &vals[i]).unwrap();
                    }
                    engine // prevent Drop during timing
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

// ============================================================================
// Single Get Benchmark
// ============================================================================

fn bench_single_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_basic/single_get");
    group.sampling_mode(SamplingMode::Flat);

    let num_keys = 10_000usize;
    let num_reads = 1_000usize;
    let (keys, vals) = precompute_kv(num_keys, VALUE_SIZE);

    // Precompute read indices so the inner loop is just the engine call
    let read_indices: Vec<usize> = (0..num_reads).map(|i| i % num_keys).collect();

    let bytes_total = (num_reads as u64) * BYTES_PER_OP;
    group.throughput(Throughput::Bytes(bytes_total));

    for mode in ALL_STORAGE_MODES {
        group.bench_with_input(BenchmarkId::new("get", mode.as_str()), &mode, |b, &mode| {
            b.iter_batched(
                || {
                    let engine = setup_engine_with_mode("single_get", mode);
                    let cf = engine.default_column_family();
                    for i in 0..num_keys {
                        engine.put(cf, &keys[i], &vals[i]).unwrap();
                    }
                    engine
                },
                |engine| {
                    let cf = engine.default_column_family();
                    for &idx in &read_indices {
                        black_box(engine.get(cf, &keys[idx]).unwrap());
                    }
                    engine // prevent Drop during timing
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

// ============================================================================
// Single Delete Benchmark
// ============================================================================

fn bench_single_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_basic/single_delete");
    group.sampling_mode(SamplingMode::Flat);

    let num_keys = 5_000usize;
    let (keys, vals) = precompute_kv(num_keys, VALUE_SIZE);
    let bytes_total = (num_keys as u64) * KEY_SIZE as u64; // delete only uses key

    group.throughput(Throughput::Bytes(bytes_total));

    for mode in ALL_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("delete", mode.as_str()),
            &mode,
            |b, &mode| {
                b.iter_batched(
                    || {
                        let engine = setup_engine_with_mode("single_delete", mode);
                        let cf = engine.default_column_family();
                        for (key, val) in keys.iter().zip(vals.iter()) {
                            engine.put(cf, key, val).unwrap();
                        }
                        engine
                    },
                    |engine| {
                        let cf = engine.default_column_family();
                        for key in &keys {
                            engine.delete(cf, key).unwrap();
                        }
                        engine // prevent Drop during timing
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

// ============================================================================
// Batch Put Benchmark
// ============================================================================

fn bench_batch_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_basic/batch_put");
    group.sampling_mode(SamplingMode::Flat);

    let batch_size = 100usize;
    let num_batches = 50usize;
    let total_keys = batch_size * num_batches;
    let (keys, vals) = precompute_kv(total_keys, VALUE_SIZE);
    let bytes_total = (total_keys as u64) * BYTES_PER_OP;

    group.throughput(Throughput::Bytes(bytes_total));

    for mode in ALL_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("batch_100", mode.as_str()),
            &mode,
            |b, &mode| {
                b.iter_batched(
                    || setup_engine_with_mode("batch_put", mode),
                    |engine| {
                        let _cf = engine.default_column_family();
                        for batch_idx in 0..num_batches {
                            let mut batch = WriteBatch::new();
                            let start = batch_idx * batch_size;
                            let end = start + batch_size;

                            for (k, v) in keys[start..end].iter().zip(&vals[start..end]) {
                                batch.put(k.clone(), v.clone());
                            }

                            engine.write_batch(&batch).unwrap();
                        }
                        engine // prevent Drop during timing
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

// bench_mixed_crud was pruned — moved to `stress/pruned/tier3_system_engine_basic.rs`.
// See stress/pruned file for the full scenario implementation.
// bench_concurrent_reads was pruned — moved to `stress/pruned/tier3_system_engine_basic.rs`.
// bench_concurrent_writes was pruned — moved to `stress/pruned/tier3_system_engine_basic.rs`.

// Update criterion targets to remove pruned concurrent benches.// ============================================================================
// Point Lookup Miss Benchmark
// ============================================================================

fn bench_point_lookup_miss(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_basic/point_lookup_miss");
    group.sampling_mode(SamplingMode::Flat);

    let num_keys = 5_000usize;
    let num_lookups = 1_000usize;

    // Insert even keys, lookup odd keys (guaranteed misses)
    let even_keys: Vec<Bytes> = (0..num_keys).map(|i| make_key(i * 2)).collect();
    let odd_keys: Vec<Bytes> = (0..num_lookups).map(|i| make_key(i * 2 + 1)).collect();
    let vals: Vec<Bytes> = (0..num_keys)
        .map(|_| make_value_fixed(VALUE_SIZE))
        .collect();

    let bytes_total = (num_lookups as u64) * KEY_SIZE as u64; // only key for miss
    group.throughput(Throughput::Bytes(bytes_total));

    for mode in ALL_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("miss", mode.as_str()),
            &mode,
            |b, &mode| {
                b.iter_batched(
                    || {
                        let engine = setup_engine_with_mode("point_miss", mode);
                        let cf = engine.default_column_family();
                        for i in 0..num_keys {
                            engine.put(cf, &even_keys[i], &vals[i]).unwrap();
                        }
                        engine
                    },
                    |engine| {
                        let cf = engine.default_column_family();
                        for key in &odd_keys {
                            black_box(engine.get(cf, key).unwrap());
                        }
                        engine // prevent Drop during timing
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = tier3_system_engine_basic;
    config = criterion_config_for_tier(BenchTier::Tier3System);
    targets =
        bench_single_put,
        bench_single_get,
        bench_single_delete,
        bench_batch_put,
        bench_point_lookup_miss
}
criterion_main!(tier3_system_engine_basic);
