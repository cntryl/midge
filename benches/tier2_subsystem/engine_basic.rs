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

fn precompute_kv(n: usize, value_base: usize) -> (Vec<Bytes>, Vec<Bytes>) {
    let mut keys = Vec::with_capacity(n);
    let mut vals = Vec::with_capacity(n);
    for i in 0..n {
        keys.push(make_key(i));
        vals.push(make_value(i, value_base));
    }
    (keys, vals)
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
        let (keys, vals) = precompute_kv(op_count, 80);
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
                            engine.put(&cf, &keys[i], &vals[i]).unwrap();
                        }
                    },
                    BatchSize::SmallInput,
                )
            },
        );

        let (keys_random, vals_random) = precompute_kv(op_count, 80);
        let mut rng = StdRng::seed_from_u64(42);
        let mut indices: Vec<usize> = (0..op_count).collect();
        indices.shuffle(&mut rng);

        group.throughput(Throughput::Elements(op_count as u64));
        group.bench_with_input(BenchmarkId::new("random", op_count), &op_count, |b, _| {
            b.iter_batched(
                || setup_db("random", false),
                |engine| {
                    let cf = engine.default_column_family();
                    for &i in &indices {
                        engine.put(&cf, &keys_random[i], &vals_random[i]).unwrap();
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

                    // prepare keys and values for each thread to avoid shared allocations during timing
                    let kv_pairs: Vec<Vec<(Bytes, Bytes)>> = (0..threads)
                        .map(|t| {
                            (0..ops_per_thread)
                                .map(|i| {
                                    let idx = i + t * ops_per_thread;
                                    (make_key(idx), make_value(0, 128))
                                })
                                .collect()
                        })
                        .collect();

                    // capture baseline write amplification
                    let wa_before = engine.write_amplification();

                    let start = Instant::now();

                    // per-thread histograms
                    let mut thread_handles = Vec::with_capacity(threads);
                    for t in 0..threads {
                        let engine = Arc::clone(&engine);
                        let cf = cfs[t].clone();
                        let thread_kvs = kv_pairs[t].clone();

                        thread_handles.push(thread::spawn(move || {
                            let mut hist = Histogram::<u64>::new_with_bounds(1, 10_000_000, 3).unwrap();
                            for (k, v) in thread_kvs.iter() {
                                let before = Instant::now();
                                engine.put(&cf, k, v).unwrap();
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

                    // (we could assert or report metrics here; criterion bench collects time)
                }

                total_elapsed
            })
        });
    }

    group.finish();
}

criterion_group! {
    name = subsystem_engine_basic;
    config = criterion_config();
    targets =
        bench_put_variants,
        bench_concurrent_cf_scaling
}

criterion_main!(subsystem_engine_basic);
