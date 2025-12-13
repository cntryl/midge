//! Tier 3 — MVCC & Transaction Isolation Benchmarks
//!
//! **Target Runtime:** ~30-60 seconds
//! **Run Frequency:** Nightly CI / Perf Baselines
//!
//! Focus areas:
//! - Snapshot creation and consistency under concurrent writes
//! - Transaction isolation and MVCC overhead
//! - Single-threaded baseline for scaling analysis
//! - Contention breakdown (readers vs writers)
//!
//! ## Design Notes
//!
//! - Returns engine from timed closures to exclude teardown from timing
//! - Precomputes all keys/values outside hot loops
//! - Uses unique paths to avoid cross-iteration interference
//! - Throughput measured in bytes where applicable
//! - Tests all storage modes: Memory, LocalDisk, and CloudBacked

#[path = "./criterion_helper.rs"]
mod criterion_helper;

#[path = "./tier3_system_bench_common.rs"]
mod bench_common;

use bench_common::{
    make_key, make_value_fixed, precompute_kv, setup_engine_arc, setup_engine_with_mode,
    ALL_STORAGE_MODES, BYTES_PER_OP, VALUE_SIZE,
};

use bytes::Bytes;
use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

// ============================================================================
// 1. Single-Threaded Baseline
// ============================================================================

fn bench_single_thread_baseline(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_baseline_single_thread");
    group.sampling_mode(SamplingMode::Flat);

    let num_ops = 1_000usize;
    let (keys, vals) = precompute_kv(num_ops, VALUE_SIZE);
    let bytes_total = (num_ops as u64) * BYTES_PER_OP;

    group.throughput(Throughput::Bytes(bytes_total));

    for mode in ALL_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("baseline_seq_puts", mode.as_str()),
            &mode,
            |b, &mode| {
                b.iter_batched(
                    || setup_engine_with_mode("baseline_seq", mode),
                    |engine| {
                        let cf = engine.default_column_family();
                        for i in 0..num_ops {
                            engine.put(cf, &keys[i], &vals[i]).unwrap();
                        }
                        engine // prevent Drop during timing
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    // Get benchmark - reads step_by(5) = 200 reads
    let read_count = num_ops / 5;
    group.throughput(Throughput::Bytes((read_count as u64) * BYTES_PER_OP));

    for mode in ALL_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("baseline_random_gets_hit", mode.as_str()),
            &mode,
            |b, &mode| {
                b.iter_batched(
                    || {
                        let engine = setup_engine_with_mode("baseline_get", mode);
                        let cf = engine.default_column_family();
                        for i in 0..num_ops {
                            engine.put(cf, &keys[i], &vals[i]).unwrap();
                        }
                        engine
                    },
                    |engine| {
                        let cf = engine.default_column_family();
                        for i in (0..num_ops).step_by(5) {
                            black_box(engine.get(cf, &keys[i]).unwrap());
                        }
                        engine
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

// ============================================================================
// 2. Contention Breakdown
// ============================================================================

fn bench_contention_breakdown(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_contention_breakdown");
    group.sampling_mode(SamplingMode::Flat);

    let ops_per_thread = 2_500usize;
    let num_threads = 4usize;
    let total_ops = ops_per_thread * num_threads;

    // Precompute all KV pairs for writers
    let all_kv: Vec<Vec<(Bytes, Bytes)>> = (0..num_threads)
        .map(|tid| {
            (0..ops_per_thread)
                .map(|i| {
                    let idx = tid * ops_per_thread + i;
                    (make_key(idx), make_value_fixed(VALUE_SIZE))
                })
                .collect()
        })
        .collect();

    let bytes_total = (total_ops as u64) * BYTES_PER_OP;

    group.throughput(Throughput::Bytes(bytes_total));

    for mode in ALL_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("writers_only_4threads", mode.as_str()),
            &mode,
            |b, &mode| {
                b.iter_batched(
                    || setup_engine_arc("writers_only", mode),
                    |engine| {
                        let cf = engine.default_column_family();

                        thread::scope(|scope| {
                            for thread_kv in &all_kv {
                                let e = Arc::clone(&engine);
                                let cf = cf.clone();
                                scope.spawn(move || {
                                    for (k, v) in thread_kv.iter() {
                                        e.put(&cf, k, v).unwrap();
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

    // Readers benchmark - preload data once
    let read_keys: Vec<Bytes> = (0..10_000).map(make_key).collect();
    let read_count = 10_000 / 4; // step_by(4)

    group.throughput(Throughput::Bytes((read_count as u64) * BYTES_PER_OP));

    for mode in ALL_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("readers_only_4threads", mode.as_str()),
            &mode,
            |b, &mode| {
                b.iter_batched(
                    || {
                        let engine = setup_engine_arc("readers_only", mode);
                        let cf = engine.default_column_family();
                        let vals: Vec<Bytes> =
                            (0..10_000).map(|_| make_value_fixed(VALUE_SIZE)).collect();
                        for i in 0..10_000 {
                            engine.put(cf, &read_keys[i], &vals[i]).unwrap();
                        }
                        engine
                    },
                    |engine| {
                        let cf = engine.default_column_family();

                        thread::scope(|scope| {
                            for _ in 0..4 {
                                let e = Arc::clone(&engine);
                                let cf = cf.clone();
                                let keys_ref = &read_keys;
                                scope.spawn(move || {
                                    for i in (0..10_000).step_by(4) {
                                        black_box(e.get(&cf, &keys_ref[i]).unwrap());
                                    }
                                });
                            }
                        });

                        engine
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    // Mixed workload
    let prefill_count = 5_000usize;
    let (prefill_keys, prefill_vals) = precompute_kv(prefill_count, VALUE_SIZE);

    // Writers will write to keys 10_000+
    let writer_kv: Vec<Vec<(Bytes, Bytes)>> = (0..num_threads)
        .map(|tid| {
            (0..ops_per_thread)
                .map(|i| {
                    let idx = 10_000 + tid * ops_per_thread + i;
                    (make_key(idx), make_value_fixed(VALUE_SIZE))
                })
                .collect()
        })
        .collect();

    // Total: 4 writers * 2500 ops + 4 readers * 1000 ops
    let mixed_write_ops = num_threads * ops_per_thread;
    let mixed_read_ops = prefill_count / 5; // step_by(5) per reader * 4 readers
    let mixed_bytes = ((mixed_write_ops + mixed_read_ops) as u64) * BYTES_PER_OP;

    group.throughput(Throughput::Bytes(mixed_bytes));

    for mode in ALL_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("mixed_4w4r", mode.as_str()),
            &mode,
            |b, &mode| {
                b.iter_batched(
                    || {
                        let engine = setup_engine_arc("mixed_contention", mode);
                        let cf = engine.default_column_family();
                        for i in 0..prefill_count {
                            engine.put(cf, &prefill_keys[i], &prefill_vals[i]).unwrap();
                        }
                        engine
                    },
                    |engine| {
                        let cf = engine.default_column_family();
                        let keys_ref = &prefill_keys;

                        thread::scope(|scope| {
                            // Writers
                            for thread_kv in &writer_kv {
                                let e = Arc::clone(&engine);
                                let cf = cf.clone();
                                scope.spawn(move || {
                                    for (k, v) in thread_kv.iter() {
                                        e.put(&cf, k, v).unwrap();
                                    }
                                });
                            }

                            // Readers
                            for _ in 0..4 {
                                let e = Arc::clone(&engine);
                                let cf = cf.clone();
                                scope.spawn(move || {
                                    for i in (0..prefill_count).step_by(5) {
                                        black_box(e.get(&cf, &keys_ref[i]).unwrap());
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
// 3. MVCC & Snapshot Consistency
// ============================================================================

fn bench_snapshot_stress(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_snapshot_stress");
    group.sampling_mode(SamplingMode::Flat);

    let prefill_count = 1_000usize;
    let (prefill_keys, prefill_vals) = precompute_kv(prefill_count, VALUE_SIZE);

    // Writer KV pairs
    let writer_ops = 500usize;
    let writer_kv: Vec<Vec<(Bytes, Bytes)>> = (0..2)
        .map(|tid| {
            (0..writer_ops)
                .map(|i| {
                    let idx = prefill_count + tid * writer_ops + i;
                    (make_key(idx), make_value_fixed(VALUE_SIZE))
                })
                .collect()
        })
        .collect();

    // Snapshot read keys
    let snap_read_indices: Vec<usize> = (0..500).step_by(10).collect();

    let total_write_ops = 2 * writer_ops;
    let total_read_ops = snap_read_indices.len() * 10 * 2; // 10 snapshots per reader, 2 readers
    let bytes_total = ((total_write_ops + total_read_ops) as u64) * BYTES_PER_OP;

    group.throughput(Throughput::Bytes(bytes_total));

    for mode in ALL_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("concurrent_snapshots_with_writes", mode.as_str()),
            &mode,
            |b, &mode| {
                b.iter_batched(
                    || {
                        let engine = setup_engine_arc("snapshot_stress", mode);
                        let cf = engine.default_column_family();
                        for i in 0..prefill_count {
                            engine.put(cf, &prefill_keys[i], &prefill_vals[i]).unwrap();
                        }
                        engine
                    },
                    |engine| {
                        let cf = engine.default_column_family();
                        let snapshot_count = Arc::new(AtomicUsize::new(0));
                        let keys_ref = &prefill_keys;
                        let indices_ref = &snap_read_indices;

                        thread::scope(|scope| {
                            // Writer threads
                            for thread_kv in &writer_kv {
                                let e = Arc::clone(&engine);
                                let cf_clone = cf.clone();
                                scope.spawn(move || {
                                    for (k, v) in thread_kv.iter() {
                                        e.put(&cf_clone, k, v).unwrap();
                                    }
                                });
                            }

                            // Snapshot readers
                            for _ in 0..2 {
                                let e = Arc::clone(&engine);
                                let cf_clone = cf.clone();
                                let sc = Arc::clone(&snapshot_count);
                                scope.spawn(move || {
                                    for _ in 0..10 {
                                        let snap = e.snapshot();
                                        for &i in indices_ref {
                                            black_box(
                                                e.get_at(&cf_clone, &keys_ref[i], &snap).ok(),
                                            );
                                        }
                                        sc.fetch_add(1, Ordering::Relaxed);
                                    }
                                });
                            }
                        });

                        black_box(snapshot_count);
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
// 4. Transaction Isolation
// ============================================================================

fn bench_transaction_isolation(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_transaction_isolation");
    group.sampling_mode(SamplingMode::Flat);

    let ops_per_thread = 100usize;
    let num_threads = 4usize;
    let total_ops = ops_per_thread * num_threads;

    // Precompute all KV pairs
    let all_kv: Vec<Vec<(Bytes, Bytes)>> = (0..num_threads)
        .map(|tid| {
            (0..ops_per_thread)
                .map(|i| {
                    let idx = tid * 1_000 + i;
                    (make_key(idx), make_value_fixed(VALUE_SIZE))
                })
                .collect()
        })
        .collect();

    let bytes_total = (total_ops as u64) * BYTES_PER_OP;

    group.throughput(Throughput::Bytes(bytes_total));

    for mode in ALL_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("concurrent_tx_isolation", mode.as_str()),
            &mode,
            |b, &mode| {
                b.iter_batched(
                    || setup_engine_arc("tx_isolation", mode),
                    |engine| {
                        let cf = engine.default_column_family();
                        let tx_success = Arc::new(AtomicUsize::new(0));

                        thread::scope(|scope| {
                            for thread_kv in &all_kv {
                                let e = Arc::clone(&engine);
                                let cf_clone = cf.clone();
                                let counter = Arc::clone(&tx_success);

                                scope.spawn(move || {
                                    for (k, v) in thread_kv.iter() {
                                        if e.put(&cf_clone, k, v).is_ok() {
                                            counter.fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                });
                            }
                        });

                        black_box(tx_success);
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
    name = tier3_system_isolation_mvcc;
    config = criterion_config_for_tier(BenchTier::Tier3System);
    targets =
        bench_single_thread_baseline,
        bench_contention_breakdown,
        bench_snapshot_stress,
        bench_transaction_isolation
}
criterion_main!(tier3_system_isolation_mvcc);
