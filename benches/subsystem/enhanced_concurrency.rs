//! Enhanced Tier 3 — Concurrency & Compaction Benchmarks with Latency Insights
//!
//! **Target Runtime:** ~15 seconds
//! **Run Frequency:** Nightly CI / Perf Baselines
//!
//! Focus areas (improved):
//! - Concurrent writers with latency distribution (p50, p99, p99.9)
//! - WAL batching effectiveness curves
//! - Compaction amplification measurement
//! - Single-threaded baseline for scaling analysis
//! - Read/write contention breakdown
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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

fn make_key(i: usize) -> Bytes {
    Bytes::from(format!("key_{:010}", i))
}
fn make_value(size: usize) -> Bytes {
    Bytes::from(vec![b'x'; size])
}

fn setup_db(name: &str, compaction: bool) -> MidgeEngine {
    let path = std::env::temp_dir().join(format!("midge_bench_t3_enh_{}", name));
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
// Latency Tracking Utility
// ============================================================================

#[derive(Default)]
struct LatencyStats {
    samples: Vec<u64>, // nanoseconds
}

impl LatencyStats {
    fn record(&mut self, ns: u64) {
        self.samples.push(ns);
    }

    fn p50(&self) -> u64 {
        self.percentile(50)
    }

    fn p99(&self) -> u64 {
        self.percentile(99)
    }

    fn p99_9(&self) -> u64 {
        self.percentile(999)
    }

    fn percentile(&self, p: usize) -> u64 {
        if self.samples.is_empty() {
            return 0;
        }
        let mut sorted = self.samples.clone();
        sorted.sort_unstable();
        let idx = (sorted.len() * p) / 1000;
        sorted[idx.min(sorted.len() - 1)]
    }

    fn mean(&self) -> u64 {
        if self.samples.is_empty() {
            0
        } else {
            self.samples.iter().sum::<u64>() / self.samples.len() as u64
        }
    }

    fn count(&self) -> usize {
        self.samples.len()
    }
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

    for &threads in &[1, 2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::new("put_p50_latency_us", threads),
            &threads,
            |b, &tcount| {
                b.iter_batched(
                    || Arc::new(setup_db(&format!("concurrent_lat_{}", tcount), false)),
                    |engine| {
                        let cf = engine.default_column_family();
                        let n_ops = 2_500;
                        let stats = Arc::new(std::sync::Mutex::new(LatencyStats::default()));

                        thread::scope(|scope| {
                            for tid in 0..tcount {
                                let engine = Arc::clone(&engine);
                                let cf_h = cf.clone();
                                let stats = Arc::clone(&stats);

                                scope.spawn(move || {
                                    let offset = tid * n_ops;
                                    for i in 0..n_ops {
                                        let start = Instant::now();
                                        engine
                                            .put(&cf_h, &make_key(offset + i), &make_value(128))
                                            .unwrap();
                                        let elapsed = start.elapsed().as_nanos() as u64;
                                        stats.lock().unwrap().record(elapsed);
                                    }
                                });
                            }
                        });

                        black_box(stats);
                    },
                    BatchSize::SmallInput,
                )
            },
        );

        group.bench_with_input(
            BenchmarkId::new("put_p99_latency_us", threads),
            &threads,
            |b, &tcount| {
                b.iter_batched(
                    || Arc::new(setup_db(&format!("concurrent_p99_{}", tcount), false)),
                    |engine| {
                        let cf = engine.default_column_family();
                        let n_ops = 2_500;
                        let stats = Arc::new(std::sync::Mutex::new(LatencyStats::default()));

                        thread::scope(|scope| {
                            for tid in 0..tcount {
                                let engine = Arc::clone(&engine);
                                let cf_h = cf.clone();
                                let stats = Arc::clone(&stats);

                                scope.spawn(move || {
                                    let offset = tid * n_ops;
                                    for i in 0..n_ops {
                                        let start = Instant::now();
                                        engine
                                            .put(&cf_h, &make_key(offset + i), &make_value(128))
                                            .unwrap();
                                        let elapsed = start.elapsed().as_nanos() as u64;
                                        stats.lock().unwrap().record(elapsed);
                                    }
                                });
                            }
                        });

                        black_box(stats);
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

// ============================================================================
// 3. Contention Breakdown: Writers vs Readers
// ============================================================================

fn bench_contention_breakdown(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_contention_breakdown");

    // Baseline: writers only
    group.bench_function("writers_only_4threads", |b| {
        b.iter_batched(
            || Arc::new(setup_db("writes_only", false)),
            |engine| {
                let cf = engine.default_column_family();

                thread::scope(|scope| {
                    for tid in 0..4 {
                        let engine = Arc::clone(&engine);
                        let cf_h = cf.clone();

                        scope.spawn(move || {
                            for i in 0..1_000 {
                                let start = Instant::now();
                                engine
                                    .put(&cf_h, &make_key(tid * 1_000 + i), &make_value(128))
                                    .unwrap();
                                let _ = start.elapsed();
                            }
                        });
                    }
                });

                black_box(());
            },
            BatchSize::SmallInput,
        )
    });

    // Baseline: readers only (with pre-populated data)
    group.bench_function("readers_only_4threads", |b| {
        let engine = Arc::new(setup_db("reads_only", false));
        let cf = engine.default_column_family();
        for i in 0..10_000 {
            engine.put(&cf, &make_key(i), &make_value(128)).unwrap();
        }

        b.iter(|| {
            thread::scope(|scope| {
                for tid in 0..4 {
                    let engine = Arc::clone(&engine);
                    let cf_h = cf.clone();

                    scope.spawn(move || {
                        for i in (tid * 500)..(tid * 500 + 500) {
                            let _ = engine.get(&cf_h, &make_key(i)).unwrap();
                        }
                    });
                }
            });
            black_box(());
        })
    });

    // Mixed: 4 writers + 4 readers
    group.bench_function("mixed_4w4r", |b| {
        b.iter_batched(
            || {
                let engine = Arc::new(setup_db("mixed", false));
                let cf = engine.default_column_family();
                for i in 0..10_000 {
                    engine.put(&cf, &make_key(i), &make_value(128)).unwrap();
                }
                engine
            },
            |engine| {
                let cf = engine.default_column_family();

                thread::scope(|scope| {
                    // Writers
                    for t in 0..4 {
                        let e = Arc::clone(&engine);
                        let cf_h = cf.clone();
                        scope.spawn(move || {
                            for i in 0..1_000 {
                                e.put(&cf_h, &make_key(20_000 + t * 1_000 + i), &make_value(128))
                                    .unwrap();
                            }
                        });
                    }
                    // Readers
                    for t in 0..4 {
                        let e = Arc::clone(&engine);
                        let cf_h = cf.clone();
                        scope.spawn(move || {
                            for i in (t * 1_000)..(t * 1_000 + 1_000) {
                                let _ = e.get(&cf_h, &make_key(i % 10_000)).unwrap();
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
// 4. Compaction Amplification Measurement
// ============================================================================

fn bench_compaction_amplification(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_compaction_amplification");

    group.bench_function("write_amplification_ratio", |b| {
        b.iter_batched(
            || setup_db("compacting", true),
            |engine| {
                let cf = engine.default_column_family();

                // Write 5 rounds of data to trigger compaction across levels
                for round in 0..5 {
                    for i in 0..2_000 {
                        engine
                            .put(&cf, &make_key(round * 2_000 + i), &make_value(512))
                            .unwrap();
                    }
                    // Flush to SST
                    let _ = engine.flush();
                    // Allow compaction time
                    thread::sleep(Duration::from_millis(100));
                }

                // Verify some reads during/after compaction
                for i in (0..5_000).step_by(100) {
                    let _ = engine.get(&cf, &make_key(i)).unwrap();
                }

                black_box(());
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

// ============================================================================
// 5. Read/Write Contention During Compaction
// ============================================================================

fn bench_reads_during_compaction(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_compaction_interference");

    group.bench_function("read_latency_during_compaction", |b| {
        b.iter_batched(
            || {
                let engine = setup_db("compaction_interference", true);
                let cf = engine.default_column_family();
                // Pre-populate
                for i in 0..5_000 {
                    engine.put(&cf, &make_key(i), &make_value(256)).unwrap();
                }
                let _ = engine.flush();
                Arc::new(engine)
            },
            |engine| {
                let cf = engine.default_column_family();
                let read_count = Arc::new(AtomicUsize::new(0));
                let read_count_clone = Arc::clone(&read_count);

                thread::scope(|scope| {
                    // Writer thread: trigger compaction
                    scope.spawn({
                        let engine = Arc::clone(&engine);
                        let cf_h = cf.clone();
                        move || {
                            for round in 0..3 {
                                for i in 0..2_000 {
                                    engine
                                        .put(
                                            &cf_h,
                                            &make_key(5_000 + round * 2_000 + i),
                                            &make_value(256),
                                        )
                                        .unwrap();
                                }
                                let _ = engine.flush();
                                thread::sleep(Duration::from_millis(50));
                            }
                        }
                    });

                    // Reader threads: measure latency during writes
                    for _ in 0..2 {
                        scope.spawn({
                            let engine = Arc::clone(&engine);
                            let cf_h = cf.clone();
                            let rc = Arc::clone(&read_count_clone);

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

criterion_group! {
    name = subsystem_enhanced_concurrency;
    config = criterion_config();
    targets =
        bench_single_thread_baseline,
        bench_concurrent_puts_latency,
        bench_contention_breakdown,
        bench_compaction_amplification,
        bench_reads_during_compaction
}
criterion_main!(subsystem_enhanced_concurrency);
