//! Tier 1-2 — Basic Engine Benchmarks
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers fundamental engine operations:
//! - CRUD operations (put/get/delete, random vs sequential)
//! - Write strategies (sync modes, batch sizes)
//! - Memory mode performance

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use criterion_helper::criterion_config;
use hdrhistogram::Histogram;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use cntryl_midge::api::column_family::ColumnFamilyConfig;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
use std::hint::black_box;

fn setup_db(name: &str, enable_wal_sync: bool) -> MidgeEngine {
    let path = std::env::temp_dir().join(format!("midge_bench_subsystem_basic_{}", name));
    let _ = std::fs::remove_dir_all(&path);
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: path },
        memtable_size: 4 * 1024 * 1024,
        enable_compaction: false,
        wal_sync: enable_wal_sync,
        ..Default::default()
    };
    MidgeEngine::open(opts).unwrap()
}

fn make_key(i: usize) -> Bytes {
    Bytes::from(format!("key_{:010}", i))
}
fn make_value(i: usize, base: usize) -> Bytes {
    // introduce slight variance in size
    let size = base + (i % 50);
    Bytes::from(vec![b'x'; size])
}

// ============================================================================
// CRUD Operations
// ============================================================================

fn bench_put_variants(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_put_variants");
    // Treat each benchmark invocation as N element operations so Criterion
    // reports ns/op and ops/sec. Use a shorter measurement window for
    // quick CI microbenchmarks; switch to CRITERION_FULL=1 for full mode.
    group.measurement_time(Duration::from_millis(200));
    group.sample_size(30);

    for &op_count in &[100, 1000] {
        group.throughput(Throughput::Elements(op_count as u64));
        group.bench_with_input(
            BenchmarkId::new("sequential", op_count),
            &op_count,
            |b, &n| {
                b.iter_batched(
                    || setup_db("sequential", false),
                    |engine| {
                        let cf = engine.default_column_family();
                        for i in 0..n {
                            engine.put(&cf, &make_key(i), &make_value(i, 80)).unwrap();
                        }
                    },
                    BatchSize::SmallInput,
                )
            },
        );

        group.throughput(Throughput::Elements(op_count as u64));
        group.bench_with_input(BenchmarkId::new("random", op_count), &op_count, |b, &n| {
            b.iter_batched(
                || {
                    let mut rng = StdRng::seed_from_u64(42);
                    let mut indices: Vec<usize> = (0..n).collect();
                    indices.shuffle(&mut rng);
                    (setup_db("random", false), indices)
                },
                |(engine, indices)| {
                    let cf = engine.default_column_family();
                    for i in indices {
                        engine.put(&cf, &make_key(i), &make_value(i, 80)).unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

// ============================================================================
// Concurrent CF scaling (2-8 threads)
// Captures 99th-percentile latencies and measures write amplification.
// ============================================================================

fn bench_concurrent_cf_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_concurrent_cf_scaling");

    // thread counts to test
    let thread_counts = [2usize, 4, 8usize];

    // per-thread operations per iteration (keeps runs short but measurable)
    let ops_per_thread = 200usize;

    for &threads in &thread_counts {
        group.throughput(Throughput::Elements((threads * ops_per_thread) as u64));

        group.bench_with_input(BenchmarkId::from_parameter(threads), &threads, |b, &threads| {
            b.iter_custom(|iters| {
                // We'll run `iters` internal iterations; each iteration spawns `threads`
                // threads and each thread performs `ops_per_thread` writes.
                let mut total_elapsed = Duration::ZERO;

                for _ in 0..iters {
                    // fresh engine per iteration for isolation
                    let engine = Arc::new(setup_db(&format!("concurrent_{}", threads), true));

                    // create column families (one per thread)
                    let mut cfs = vec![engine.default_column_family()];
                    for i in 1..threads {
                        let name = format!("bench_cf_{}", i);
                        let cf = engine.create_column_family(&name, ColumnFamilyConfig::default()).unwrap();
                        cfs.push(cf);
                    }

                    // prepare keys for each thread to avoid shared allocations during timing
                    let keys: Vec<Vec<Bytes>> = (0..threads)
                        .map(|t| (0..ops_per_thread).map(|i| make_key(i + t * ops_per_thread)).collect())
                        .collect();

                    // capture baseline write amplification
                    let wa_before = engine.write_amplification();

                    let start = Instant::now();

                    // per-thread histograms
                    let mut thread_handles = Vec::with_capacity(threads);
                    for t in 0..threads {
                        let engine = Arc::clone(&engine);
                        let cf = cfs[t].clone();
                        let thread_keys = keys[t].clone();

                        thread_handles.push(thread::spawn(move || {
                            let mut hist = Histogram::<u64>::new_with_bounds(1, 10_000_000, 3).unwrap();
                            for k in thread_keys.iter() {
                                let before = Instant::now();
                                engine.put(&cf, k, &make_value(0, 128)).unwrap();
                                let us = before.elapsed().as_micros() as u64;
                                let _ = hist.record(us);
                            }
                            hist
                        }));
                    }

                    // collect and merge histograms
                    let mut merged = Histogram::<u64>::new_with_bounds(1, 10_000_000, 3).unwrap();
                    for handle in thread_handles {
                        let h = handle.join().expect("thread join");
                        merged.add(&h).unwrap();
                    }

                    let elapsed = start.elapsed();
                    total_elapsed += elapsed;

                    // compute 99th percentile (microseconds)
                    let p99 = merged.value_at_percentile(99.0);

                    // measure write amplification after workload
                    let wa_after = engine.write_amplification();

                    // Print a brief summary for this iteration so CI logs capture it.
                    println!(
                        "concurrent_cf scaling: threads={} ops={} elapsed_ms={:.3} p99_us={} wa_before={:.2}x wa_after={:.2}x",
                        threads,
                        threads * ops_per_thread,
                        elapsed.as_secs_f64() * 1000.0,
                        p99,
                        wa_before,
                        wa_after
                    );
                }

                total_elapsed
            });
        });
    }

    group.finish();
}

// ============================================================================
// GET Operations
// ============================================================================

fn bench_get_hit_miss(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_get");
    group.measurement_time(Duration::from_millis(200));
    group.sample_size(30);

    group.throughput(Throughput::Elements(250));
    group.bench_function("hit_mixed", |b| {
        let engine = setup_db("get_hit", false);
        let cf = engine.default_column_family();
        for i in 0..1000 {
            engine.put(&cf, &make_key(i), &make_value(i, 100)).unwrap();
        }

        b.iter(|| {
            for i in (0..1000).step_by(4) {
                let _ = engine.get(&cf, &make_key(i)).unwrap();
            }
        })
    });

    group.throughput(Throughput::Elements(100));
    group.bench_function("miss_random", |b| {
        let engine = setup_db("get_miss", false);
        let cf = engine.default_column_family();

        b.iter(|| {
            for i in 1000..1100 {
                let _ = engine.get(&cf, &make_key(i)).unwrap();
            }
        })
    });

    group.finish();
}

// ============================================================================
// DELETE Operations
// ============================================================================

fn bench_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_delete");
    group.measurement_time(Duration::from_millis(200));
    group.sample_size(30);

    group.throughput(Throughput::Elements(1000));
    group.bench_function("delete_existing", |b| {
        b.iter_batched(
            || {
                let engine = setup_db("delete", false);
                let cf = engine.default_column_family();
                for i in 0..1000 {
                    engine.put(&cf, &make_key(i), &make_value(i, 100)).unwrap();
                }
                engine
            },
            |engine| {
                let cf = engine.default_column_family();
                for i in 0..1000 {
                    engine.delete(&cf, &make_key(i)).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

// ============================================================================
// Write Modes
// ============================================================================

fn bench_write_modes(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_write_modes");
    group.measurement_time(Duration::from_millis(200));
    group.sample_size(30);

    group.throughput(Throughput::Elements(500));
    group.bench_function("nosync_batched", |b| {
        b.iter_batched(
            || setup_db("nosync", false),
            |engine| {
                let cf = engine.default_column_family();
                for i in 0..500 {
                    engine.put(&cf, &make_key(i), &make_value(i, 100)).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.throughput(Throughput::Elements(500));
    group.bench_function("sync_every_write", |b| {
        b.iter_batched(
            || setup_db("sync", true),
            |engine| {
                let cf = engine.default_column_family();
                for i in 0..500 {
                    engine.put(&cf, &make_key(i), &make_value(i, 100)).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.throughput(Throughput::Elements(500));
    group.bench_function("small_batch_write", |b| {
        b.iter_batched(
            || setup_db("batch", false),
            |engine| {
                let cf = engine.default_column_family();
                for i in 0..500 {
                    engine.put(&cf, &make_key(i), &make_value(i, 100)).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

// ============================================================================
// Memory Mode
// ============================================================================

fn bench_memory_mode(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_memory_mode");
    group.measurement_time(Duration::from_millis(200));
    group.sample_size(30);

    // 100 writes + 50 reads per iteration = 150 element operations
    group.throughput(Throughput::Elements(150));
    group.bench_function("read_write_mix", |b| {
        b.iter_batched(
            || setup_db("memory_mode", false),
            |engine| {
                let cf = engine.default_column_family();

                // writes
                for i in 0..100 {
                    engine.put(&cf, &make_key(i), &make_value(i, 200)).unwrap();
                }

                // reads
                for i in (0..100).step_by(2) {
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
// Full-stack end-to-end throughput
// ============================================================================
fn bench_full_stack_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_end_to_end");
    let cf_counts = [1usize, 4, 8, 16];
    let n_ops = 10_000usize;

    for &n_cfs in &cf_counts {
        group.throughput(Throughput::Elements(n_ops as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n_cfs), &n_cfs, |b, &n_cfs| {
            b.iter_batched(
                || {
                    let engine = setup_db(&format!("end_to_end_{}", n_cfs), false);
                    // create CF handles (default + extras)
                    let mut cfs = vec![engine.default_column_family()];
                    for i in 1..n_cfs {
                        let name = format!("bench_cf_{}", i);
                        let cf = engine
                            .create_column_family(&name, ColumnFamilyConfig::default())
                            .unwrap();
                        cfs.push(cf);
                    }

                    let keys: Vec<Bytes> = (0..n_ops).map(make_key).collect();
                    (engine, cfs, keys)
                },
                |(engine, cfs, keys)| {
                    // 1) Writes spread across CFs
                    for (i, k) in keys.iter().enumerate() {
                        let cf = &cfs[i % n_cfs];
                        engine.put(cf, k, &make_value(i, 100)).unwrap();
                    }

                    // 2) Mixed reads (hits then misses)
                    for k in keys.iter().take(9_000) {
                        black_box(engine.get(&cfs[0], k).unwrap());
                    }
                    for i in 0..1_000 {
                        black_box(engine.get(&cfs[0], &make_key(n_ops + i)).unwrap());
                    }

                    // 3) Deletes
                    for (i, k) in keys.iter().take(2_000).enumerate() {
                        let cf = &cfs[i % n_cfs];
                        engine.delete(cf, k).unwrap();
                    }

                    // 4) Flush all CFs
                    for cf in &cfs {
                        engine.flush_cf(cf).unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

criterion_group! {
    name = subsystem_engine_basic;
    config = criterion_config();
    targets =
        bench_put_variants,
        bench_get_hit_miss,
        bench_delete,
        bench_write_modes,
        bench_memory_mode,
        bench_full_stack_throughput
        ,
        bench_concurrent_cf_scaling
}
criterion_main!(subsystem_engine_basic);
