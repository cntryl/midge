//! Tier 3 — MVCC & Transaction Isolation Benchmarks
//!
//! **Target Runtime:** ~15 seconds
//! **Run Frequency:** Nightly CI / Perf Baselines
//!
//! Focus areas:
//! - Snapshot creation and consistency under concurrent writes
//! - Transaction isolation and MVCC overhead
//! - Snapshot behavior during compaction (old versions preserved)
//! - Single-threaded baseline for scaling analysis
//! - Latency distribution under concurrent reader/writer load
//! - Compaction amplification measurement

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

fn make_key(i: usize) -> Bytes {
    Bytes::from(format!("key_{:010}", i))
}
fn make_value(size: usize) -> Bytes {
    Bytes::from(vec![b'x'; size])
}

fn setup_db(name: &str, compaction: bool) -> MidgeEngine {
    let path = std::env::temp_dir().join(format!("midge_bench_t3_mvcc_{}", name));
    let _ = std::fs::remove_dir_all(&path);
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: path },
        memtable_size: 4 * 1024 * 1024,
        enable_compaction: compaction,
        wal_sync: true,
        ..Default::default()
    };
    MidgeEngine::open(opts).unwrap()
}

// ============================================================================
// 1. Single-Threaded Baseline
// ============================================================================

fn bench_single_thread_baseline(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_baseline_single_thread");

    group.bench_function("baseline_seq_puts_128b", |b| {
        b.iter_batched(
            || Arc::new(setup_db("baseline_seq", false)),
            |engine| {
                let cf = engine.default_column_family();
                for i in 0..1_000 {
                    engine.put(&cf, &make_key(i), &make_value(128)).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("baseline_random_gets_hit", |b| {
        let engine = Arc::new(setup_db("baseline_get", false));
        let cf = engine.default_column_family();
        for i in 0..1_000 {
            engine.put(&cf, &make_key(i), &make_value(128)).unwrap();
        }

        b.iter(|| {
            for i in (0..1_000).step_by(5) {
                let _ = engine.get(&cf, &make_key(i)).unwrap();
            }
        })
    });

    group.finish();
}

// ============================================================================
// 2. Concurrent Puts with Latency Distribution
// ============================================================================

fn bench_concurrent_puts_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_concurrent_puts_latency");
    group.sample_size(50);

    for &threads in &[1, 2, 4, 8] {
        for latency_pct in &["p50", "p99"] {
            group.bench_with_input(
                BenchmarkId::new(&format!("put_{}_latency_us", latency_pct), threads),
                &threads,
                |b, &tcount| {
                    b.iter_batched(
                        || Arc::new(setup_db(&format!("latency_{}", tcount), false)),
                        |engine| {
                            let cf = engine.default_column_family();
                            let latencies = Arc::new(std::sync::Mutex::new(Vec::new()));

                            thread::scope(|scope| {
                                for tid in 0..tcount {
                                    let engine = Arc::clone(&engine);
                                    let cf = cf.clone();
                                    let lats = Arc::clone(&latencies);

                                    scope.spawn(move || {
                                        for i in 0..1_000 {
                                            let start = Instant::now();
                                            engine
                                                .put(
                                                    &cf,
                                                    &make_key(tid * 1_000 + i),
                                                    &make_value(128),
                                                )
                                                .unwrap();
                                            let elapsed_us = start.elapsed().as_micros() as u64;
                                            lats.lock().unwrap().push(elapsed_us);
                                        }
                                    });
                                }
                            });

                            black_box(Arc::try_unwrap(latencies).unwrap().into_inner().unwrap());
                        },
                        BatchSize::SmallInput,
                    )
                },
            );
        }
    }

    group.finish();
}

// ============================================================================
// 3. Contention Breakdown
// ============================================================================

fn bench_contention_breakdown(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_contention_breakdown");

    group.bench_function("writers_only_4threads", |b| {
        b.iter_batched(
            || Arc::new(setup_db("writers_only", false)),
            |engine| {
                let cf = engine.default_column_family();
                let start = Instant::now();

                thread::scope(|scope| {
                    for tid in 0..4 {
                        let e = Arc::clone(&engine);
                        let cf = cf.clone();
                        scope.spawn(move || {
                            for i in 0..2_500 {
                                e.put(&cf, &make_key(tid * 2_500 + i), &make_value(128))
                                    .unwrap();
                            }
                        });
                    }
                });

                black_box(start.elapsed());
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("readers_only_4threads", |b| {
        let engine = Arc::new(setup_db("readers_only", false));
        let cf = engine.default_column_family();
        for i in 0..10_000 {
            engine.put(&cf, &make_key(i), &make_value(128)).unwrap();
        }

        b.iter(|| {
            let start = Instant::now();

            thread::scope(|scope| {
                for _ in 0..4 {
                    let e = Arc::clone(&engine);
                    let cf = cf.clone();
                    scope.spawn(move || {
                        for i in (0..10_000).step_by(4) {
                            let _ = e.get(&cf, &make_key(i)).unwrap();
                        }
                    });
                }
            });

            black_box(start.elapsed());
        })
    });

    group.bench_function("mixed_4w4r", |b| {
        b.iter_batched(
            || {
                let engine = Arc::new(setup_db("mixed_contention", false));
                let cf = engine.default_column_family();
                for i in 0..5_000 {
                    engine.put(&cf, &make_key(i), &make_value(128)).unwrap();
                }
                engine
            },
            |engine| {
                let cf = engine.default_column_family();
                let start = Instant::now();

                thread::scope(|scope| {
                    // Writers
                    for tid in 0..4 {
                        let e = Arc::clone(&engine);
                        let cf = cf.clone();
                        scope.spawn(move || {
                            for i in 0..2_500 {
                                e.put(&cf, &make_key(10_000 + tid * 2_500 + i), &make_value(128))
                                    .unwrap();
                            }
                        });
                    }

                    // Readers
                    for _ in 0..4 {
                        let e = Arc::clone(&engine);
                        let cf = cf.clone();
                        scope.spawn(move || {
                            for i in (0..5_000).step_by(5) {
                                let _ = e.get(&cf, &make_key(i)).unwrap();
                            }
                        });
                    }
                });

                black_box(start.elapsed());
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

// ============================================================================
// 4. Compaction Amplification
// ============================================================================

fn bench_compaction_amplification(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_compaction_amplification");

    group.bench_function("write_amplification_ratio", |b| {
        b.iter_batched(
            || setup_db("compaction_amp", true),
            |engine| {
                let cf = engine.default_column_family();

                // Multiple rounds to accumulate SSTs
                for round in 0..3 {
                    for i in 0..10_000 {
                        engine
                            .put(&cf, &make_key(round * 10_000 + i), &make_value(256))
                            .unwrap();
                    }
                    thread::sleep(Duration::from_millis(100));
                }

                black_box(());
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

// ============================================================================
// 5. Read Interference During Compaction
// ============================================================================

fn bench_reads_during_compaction(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_compaction_interference");

    group.bench_function("read_latency_during_compaction", |b| {
        b.iter_batched(
            || {
                let engine = Arc::new(setup_db("compaction_interference", true));
                let cf = engine.default_column_family();

                // Prefill
                for i in 0..10_000 {
                    engine.put(&cf, &make_key(i), &make_value(256)).unwrap();
                }

                (engine, cf)
            },
            |(engine, cf)| {
                let read_count = Arc::new(AtomicUsize::new(0));

                thread::scope(|scope| {
                    // Writer thread triggering compaction
                    scope.spawn({
                        let engine = Arc::clone(&engine);
                        let cf_h = cf.clone();

                        move || {
                            for round in 0..3 {
                                for i in 0..5_000 {
                                    engine
                                        .put(
                                            &cf_h,
                                            &make_key(10_000 + round * 5_000 + i),
                                            &make_value(256),
                                        )
                                        .unwrap();
                                }
                                thread::sleep(Duration::from_millis(50));
                            }
                        }
                    });

                    // Reader threads: measure latency during writes
                    for _ in 0..2 {
                        scope.spawn({
                            let engine = Arc::clone(&engine);
                            let cf_h = cf.clone();
                            let rc = Arc::clone(&read_count);

                            move || {
                                for _ in 0..1_000 {
                                    let _ = engine.get(&cf_h, &make_key(1_000)).unwrap();
                                    rc.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        });
                    }
                });

                black_box(read_count);
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

// ============================================================================
// 6. MVCC & Snapshot Consistency Stress
// ============================================================================

/// Benchmark snapshot creation and isolation under concurrent writes
fn bench_snapshot_stress(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_snapshot_stress");

    group.bench_function("concurrent_snapshots_with_writes", |b| {
        b.iter_batched(
            || {
                let engine = Arc::new(setup_db("snapshot_stress", false));
                let cf = engine.default_column_family();
                // Prefill with some initial data
                for i in 0..1_000 {
                    engine.put(&cf, &make_key(i), &make_value(128)).unwrap();
                }
                engine
            },
            |engine| {
                let cf = engine.default_column_family();
                let snapshot_count = Arc::new(AtomicUsize::new(0));

                thread::scope(|scope| {
                    // Writer threads
                    for tid in 0..2 {
                        let e = Arc::clone(&engine);
                        let cf_clone = cf.clone();
                        scope.spawn(move || {
                            for i in 0..500 {
                                e.put(
                                    &cf_clone,
                                    &make_key(1_000 + tid * 500 + i),
                                    &make_value(128),
                                )
                                .unwrap();
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
                                // Try to read through snapshot
                                for i in (0..500).step_by(10) {
                                    let _ = e.get_at(&cf_clone, &make_key(i), &snap).ok();
                                }
                                sc.fetch_add(1, Ordering::Relaxed);
                            }
                        });
                    }
                });

                black_box(snapshot_count);
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

// ============================================================================
// 7. Transaction Isolation Stress
// ============================================================================

/// Benchmark transaction isolation across concurrent writers
fn bench_transaction_isolation(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_transaction_isolation");

    group.bench_function("concurrent_tx_isolation", |b| {
        b.iter_batched(
            || Arc::new(setup_db("tx_isolation", false)),
            |engine| {
                let cf = engine.default_column_family();
                let tx_success = Arc::new(AtomicUsize::new(0));

                thread::scope(|scope| {
                    for tid in 0..4 {
                        let e = Arc::clone(&engine);
                        let cf_clone = cf.clone();
                        let counter = Arc::clone(&tx_success);

                        scope.spawn(move || {
                            for i in 0..100 {
                                let key = make_key(tid * 1_000 + i);
                                let value = make_value(128);

                                if e.put(&cf_clone, &key, &value).is_ok() {
                                    counter.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        });
                    }
                });

                black_box(tx_success);
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

// ============================================================================
// 8. Snapshot + Compaction Interaction
// ============================================================================

/// Benchmark snapshot behavior during active compaction
fn bench_snapshots_during_compaction(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_snapshots_compaction");

    group.bench_function("snapshot_reads_during_compaction", |b| {
        b.iter_batched(
            || Arc::new(setup_db("snap_compact", true)), // compaction enabled
            |engine| {
                let cf = engine.default_column_family();

                // Prefill
                for i in 0..5_000 {
                    engine.put(&cf, &make_key(i), &make_value(256)).unwrap();
                }

                let read_ops = Arc::new(AtomicUsize::new(0));
                let snapshot_count = Arc::new(AtomicUsize::new(0));

                thread::scope(|scope| {
                    // Continuous writes to trigger compaction
                    {
                        let e = Arc::clone(&engine);
                        let cf_clone = cf.clone();
                        scope.spawn(move || {
                            for i in 0..2_000 {
                                e.put(&cf_clone, &make_key(10_000 + i), &make_value(256))
                                    .ok();
                            }
                        });
                    }

                    // Snapshot readers checking consistency
                    for _ in 0..2 {
                        let e = Arc::clone(&engine);
                        let cf_clone = cf.clone();
                        let ro = Arc::clone(&read_ops);
                        let sc = Arc::clone(&snapshot_count);

                        scope.spawn(move || {
                            thread::sleep(Duration::from_millis(10));
                            for _ in 0..5 {
                                let snap = e.snapshot();
                                for i in (0..5_000).step_by(50) {
                                    if e.get_at(&cf_clone, &make_key(i), &snap).is_ok() {
                                        ro.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                                sc.fetch_add(1, Ordering::Relaxed);
                                thread::sleep(Duration::from_millis(50));
                            }
                        });
                    }
                });

                black_box((read_ops, snapshot_count));
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group! {
    name = subsystem_isolation_mvcc;
    config = criterion_config();
    targets =
        bench_single_thread_baseline,
        bench_concurrent_puts_latency,
        bench_contention_breakdown,
        bench_compaction_amplification,
        bench_reads_during_compaction,
        bench_snapshot_stress,
        bench_transaction_isolation,
        bench_snapshots_during_compaction
}
criterion_main!(subsystem_isolation_mvcc);
