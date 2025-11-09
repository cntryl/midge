//! Tier 3 — Concurrency & Compaction Benchmarks
//!
//! **Target Runtime:** ~10 seconds
//! **Run Frequency:** Nightly CI / Perf Baselines
//!
//! Focus areas:
//! - Concurrent writers (thread scaling, WAL batching contention)
//! - Read/write contention
//! - Compaction interference under sustained write load
//!
//! This assumes group commit and background compaction are enabled.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;
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
    let path = std::env::temp_dir().join(format!("midge_bench_t3_{}", name));
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
// Concurrent Writers
// ============================================================================

fn bench_concurrent_puts(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_concurrent_puts");

    for &threads in &[1, 2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |b, &tcount| {
                b.iter_batched(
                    || Arc::new(setup_db(&format!("concurrent_{}", tcount), false)),
                    |engine| {
                        let cf = engine.default_column_family();
                        let n_ops = 5_000;
                        let start = Instant::now();
                        thread::scope(|scope| {
                            for tid in 0..tcount {
                                let engine = Arc::clone(&engine);
                                let cf = cf.clone();
                                scope.spawn(move || {
                                    let offset = tid * n_ops;
                                    for i in 0..n_ops {
                                        engine
                                            .put(&cf, &make_key(offset + i), &make_value(128))
                                            .expect("put failed");
                                    }
                                });
                            }
                        });
                        black_box(start.elapsed());
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

// ============================================================================
// Read/Write Contention
// ============================================================================

fn bench_mixed_read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_read_write_contention");

    group.bench_function("4w4r_threads", |b| {
        b.iter_batched(
            || Arc::new(setup_db("mixed", false)),
            |engine| {
                let cf = engine.default_column_family();
                // prefill
                for i in 0..10_000 {
                    engine.put(&cf, &make_key(i), &make_value(64)).unwrap();
                }

                let engine_r = Arc::clone(&engine);
                thread::scope(|scope| {
                    // writers
                    for t in 0..4 {
                        let e = Arc::clone(&engine);
                        let cf = cf.clone();
                        scope.spawn(move || {
                            for i in (t * 1_000)..(t * 1_000 + 1_000) {
                                e.put(&cf, &make_key(i + 20_000), &make_value(128)).unwrap();
                            }
                        });
                    }
                    // readers
                    for _ in 0..4 {
                        let e = Arc::clone(&engine_r);
                        let cf = cf.clone();
                        scope.spawn(move || {
                            for i in (0..10_000).step_by(3) {
                                let _ = e.get(&cf, &make_key(i)).unwrap();
                            }
                        });
                    }
                });
                black_box(());
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

// ============================================================================
// Compaction Stress
// ============================================================================

fn bench_compaction_pressure(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_compaction_pressure");

    group.bench_function("steady_write_with_compaction", |b| {
        b.iter_batched(
            || setup_db("compacting", true),
            |engine| {
                let cf = engine.default_column_family();
                for round in 0..5 {
                    for i in 0..5_000 {
                        engine
                            .put(&cf, &make_key(round * 5_000 + i), &make_value(256))
                            .unwrap();
                    }
                    // brief pause to let background compaction catch up
                    thread::sleep(Duration::from_millis(50));
                }
                // Verify a few reads during/after compaction
                for i in (0..1_000).step_by(50) {
                    let _ = engine.get(&cf, &make_key(i)).unwrap();
                }
                black_box(());
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group! {
    name = subsystem_concurrency_compaction;
    config = criterion_config();
    targets =
        bench_concurrent_puts,
        bench_mixed_read_write,
        bench_compaction_pressure
}
criterion_main!(subsystem_concurrency_compaction);
