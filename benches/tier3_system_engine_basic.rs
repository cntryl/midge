//! Tier-3 System Benchmarks: Engine Basic Operations
//!
//! Tests core engine operations (put/get/delete) under realistic conditions.
//! These represent the fundamental building blocks of all database operations.
//!
//! ## Design Notes
//!
//! - Returns engine from timed closure to prevent Drop during measurement
//! - Throughput measured in bytes (key + value sizes)
//! - Uses SamplingMode::Flat for system benchmarks
//! - Tests all storage modes: Memory, LocalDisk, and CloudBacked

#[path = "./criterion_helper.rs"]
mod criterion_helper;

#[path = "./tier3_system_bench_common.rs"]
mod bench_common;

use bench_common::{
    make_key, make_value_fixed, precompute_kv, setup_engine_arc, setup_engine_with_mode,
    ALL_STORAGE_MODES, BYTES_PER_OP, KEY_SIZE, VALUE_SIZE,
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
                        engine.put(&cf, &keys[i], &vals[i]).unwrap();
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
                        engine.put(&cf, &keys[i], &vals[i]).unwrap();
                    }
                    engine
                },
                |engine| {
                    let cf = engine.default_column_family();
                    for &idx in &read_indices {
                        black_box(engine.get(&cf, &keys[idx]).unwrap());
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
                            engine.put(&cf, key, val).unwrap();
                        }
                        engine
                    },
                    |engine| {
                        let cf = engine.default_column_family();
                        for key in &keys {
                            engine.delete(&cf, key).unwrap();
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
                        let cf = engine.default_column_family();
                        for batch_idx in 0..num_batches {
                            let mut batch = WriteBatch::new();
                            let start = batch_idx * batch_size;
                            let end = start + batch_size;

                            for (k, v) in keys[start..end].iter().zip(&vals[start..end]) {
                                batch.put(cf.id(), k.clone(), v.clone());
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

// ============================================================================
// Mixed CRUD Benchmark
// ============================================================================

fn bench_mixed_crud(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_basic/mixed_crud");
    group.sampling_mode(SamplingMode::Flat);

    let num_keys = 2_000usize;
    let (keys, vals) = precompute_kv(num_keys, VALUE_SIZE);

    // Approximate: 25% put, 50% get, 25% delete
    // Precompute operations so the inner loop is branch-only
    #[derive(Clone, Copy)]
    enum Op {
        Put(usize),
        Get(usize),
        Delete(usize),
    }

    let ops: Vec<Op> = (0..num_keys)
        .map(|i| {
            let key_idx = i % num_keys;
            match i % 4 {
                0 => Op::Put(key_idx),
                1 | 2 => Op::Get(key_idx),
                _ => Op::Delete(key_idx),
            }
        })
        .collect();

    let bytes_total = (num_keys as u64) * BYTES_PER_OP;
    group.throughput(Throughput::Bytes(bytes_total));

    for mode in ALL_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("crud_cycle", mode.as_str()),
            &mode,
            |b, &mode| {
                b.iter_batched(
                    || {
                        let engine = setup_engine_with_mode("mixed_crud", mode);
                        let cf = engine.default_column_family();
                        // Preload half of keys
                        for i in 0..(num_keys / 2) {
                            engine.put(&cf, &keys[i], &vals[i]).unwrap();
                        }
                        engine
                    },
                    |engine| {
                        let cf = engine.default_column_family();
                        for op in &ops {
                            match *op {
                                Op::Put(idx) => {
                                    engine.put(&cf, &keys[idx], &vals[idx]).unwrap();
                                }
                                Op::Get(idx) => {
                                    let _ = engine.get(&cf, &keys[idx]);
                                }
                                Op::Delete(idx) => {
                                    let _ = engine.delete(&cf, &keys[idx]);
                                }
                            }
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
// Concurrent Reads Benchmark
// ============================================================================

fn bench_concurrent_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_basic/concurrent_reads");
    group.sampling_mode(SamplingMode::Flat);

    let num_keys = 10_000usize;
    let ops_per_thread = 500usize;
    let num_threads = 4usize;
    let (keys, vals) = precompute_kv(num_keys, VALUE_SIZE);

    // Precompute per-thread index sequences
    let thread_indices: Vec<Vec<usize>> = (0..num_threads)
        .map(|t| {
            (0..ops_per_thread)
                .map(|i| (t * ops_per_thread + i) % num_keys)
                .collect()
        })
        .collect();

    let bytes_total = (num_threads * ops_per_thread) as u64 * BYTES_PER_OP;
    group.throughput(Throughput::Bytes(bytes_total));

    for mode in ALL_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("4_threads", mode.as_str()),
            &mode,
            |b, &mode| {
                b.iter_batched(
                    || {
                        let engine = setup_engine_arc("concurrent_reads", mode);
                        let cf = engine.default_column_family();
                        for i in 0..num_keys {
                            engine.put(&cf, &keys[i], &vals[i]).unwrap();
                        }
                        engine
                    },
                    |engine| {
                        let cf = engine.default_column_family();
                        std::thread::scope(|s| {
                            for indices in &thread_indices {
                                let engine_ref = &engine;
                                let cf_ref = &cf;
                                let keys_ref = &keys;

                                s.spawn(move || {
                                    for &idx in indices {
                                        let _ = engine_ref.get(cf_ref, &keys_ref[idx]);
                                    }
                                });
                            }
                        });
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
// Concurrent Writes Benchmark
// ============================================================================

fn bench_concurrent_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_basic/concurrent_writes");
    group.sampling_mode(SamplingMode::Flat);

    let ops_per_thread = 250usize;
    let num_threads = 4usize;
    let total_ops = num_threads * ops_per_thread;

    // Precompute keys/values for each thread (non-overlapping key ranges)
    let thread_data: Vec<(Vec<Bytes>, Vec<Bytes>)> = (0..num_threads)
        .map(|t| {
            let start = t * ops_per_thread;
            let keys: Vec<Bytes> = (start..(start + ops_per_thread)).map(make_key).collect();
            let vals: Vec<Bytes> = (0..ops_per_thread)
                .map(|_| make_value_fixed(VALUE_SIZE))
                .collect();
            (keys, vals)
        })
        .collect();

    let bytes_total = (total_ops as u64) * BYTES_PER_OP;
    group.throughput(Throughput::Bytes(bytes_total));

    for mode in ALL_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("4_threads", mode.as_str()),
            &mode,
            |b, &mode| {
                b.iter_batched(
                    || setup_engine_arc("concurrent_writes", mode),
                    |engine| {
                        let cf = engine.default_column_family();
                        std::thread::scope(|s| {
                            for (keys, vals) in &thread_data {
                                let engine_ref = &engine;
                                let cf_ref = &cf;

                                s.spawn(move || {
                                    for (k, v) in keys.iter().zip(vals.iter()) {
                                        engine_ref.put(cf_ref, k, v).unwrap();
                                    }
                                });
                            }
                        });
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
                            engine.put(&cf, &even_keys[i], &vals[i]).unwrap();
                        }
                        engine
                    },
                    |engine| {
                        let cf = engine.default_column_family();
                        for key in &odd_keys {
                            black_box(engine.get(&cf, key).unwrap());
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
        bench_mixed_crud,
        bench_concurrent_reads,
        bench_concurrent_writes,
        bench_point_lookup_miss
}
criterion_main!(tier3_system_engine_basic);
