//! YCSB Workload C — 100% Read (Read-Only)
//!
//! - Uses Zipfian read distribution
//! - Exercises CF scaling (1, 2, 4, 8, 16)
//! - Exercises concurrency scaling (1, 2, 8 threads)
//! - Measures p50 / p99 / p99.9 tail latency
//!
//! Hot path: only `engine.get()` + Zipf + CF index math.
//! No heap allocs, no string formatting, no RNG beyond fast next_u64().

#[path = "../criterion_helper.rs"]
mod criterion_helper;

#[path = "ycsb_common.rs"]
mod ycsb_common;

use cntryl_midge::MidgeEngine;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use criterion_helper::criterion_config;
use hdrhistogram::Histogram;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use std::hint::black_box;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use ycsb_common::*;

const CF_COUNTS: &[usize] = &[1, 2, 4, 8, 16];

// ============================================================================
// Latency stats
// ============================================================================

#[derive(Clone)]
struct LatencyStats {
    p50: u64,
    p99: u64,
    p99_9: u64,
}

// ============================================================================
// Core workload logic
// ============================================================================

fn run_workload_c(
    engine: &MidgeEngine,
    operations: usize,
    cf_count: usize,
    seed: u64,
) -> LatencyStats {
    let cf_list = engine.list_column_families();

    // Precomputed globals
    let keys = PREGEN_KEYS.get().unwrap();
    let zipf = ZIPF_DEFAULT.get().unwrap();

    let mut rng = StdRng::seed_from_u64(seed);
    let mut hist = Histogram::<u64>::new(3).unwrap();

    for _ in 0..operations {
        // Zipf key
        let key_id = zipf.next(&mut rng);
        let key = &keys[key_id];

        // Pick CF
        let cf = &cf_list[rng.gen_range(0..cf_count)];

        let start = Instant::now();
        let _ = black_box(engine.get(cf, key));
        let elapsed_us = start.elapsed().as_micros() as u64;

        let _ = hist.record(elapsed_us.max(1));
    }

    LatencyStats {
        p50: hist.value_at_percentile(50.0),
        p99: hist.value_at_percentile(99.0),
        p99_9: hist.value_at_percentile(99.9),
    }
}

fn run_workload_c_concurrent(
    engine: Arc<MidgeEngine>,
    ops_per_thread: usize,
    thread_id: usize,
    cf_count: usize,
) -> LatencyStats {
    let cf_list = engine.list_column_families();

    let keys = PREGEN_KEYS.get().unwrap();
    let zipf = ZIPF_DEFAULT.get().unwrap();

    let mut rng = make_thread_rng(thread_id, 0xC0FFEE);
    let mut hist = Histogram::<u64>::new(3).unwrap();

    for _ in 0..ops_per_thread {
        let key_id = zipf.next(&mut rng);
        let key = &keys[key_id];

        let cf = &cf_list[rng.gen_range(0..cf_count)];

        let start = Instant::now();
        let _ = black_box(engine.get(cf, key));
        let elapsed_us = start.elapsed().as_micros() as u64;

        let _ = hist.record(elapsed_us.max(1));
    }

    LatencyStats {
        p50: hist.value_at_percentile(50.0),
        p99: hist.value_at_percentile(99.0),
        p99_9: hist.value_at_percentile(99.9),
    }
}

// ============================================================================
// Benchmark driver
// ============================================================================

fn bench_workload_c(c: &mut Criterion) {
    // Ensure OnceLock globals exist
    init_ycsb_globals();

    let mut group = c.benchmark_group("ycsb_workload_c_cf_variants");
    group.throughput(Throughput::Elements(OPS_PER_ITER as u64));

    // We only benchmark no-sync variants here — read workload does not care about WAL
    let scenarios = ["fs_nosync", "cloud_nosync"];

    for &cf_count in CF_COUNTS {
        // ----------------------------------------------------
        // Storage variants
        // ----------------------------------------------------
        for &scenario in &scenarios {
            let (engine, _tmp) = match scenario {
                "fs_nosync" => setup_engine_fs_nosync(),
                "cloud_nosync" => setup_engine_cloud_nosync(),
                _ => unreachable!(),
            };

            // Create CFs up front (zero allocations later)
            for i in 1..cf_count {
                let _ =
                    engine.create_column_family(&format!("cf{cf_count}_{i}"), Default::default());
            }

            // Pre-load dataset
            load_full_dataset(&engine);

            group.bench_with_input(
                BenchmarkId::new(format!("{scenario}_cf{cf_count}"), "100r"),
                &cf_count,
                |b, &_cf| {
                    b.iter(|| {
                        let stats = run_workload_c(&engine, OPS_PER_ITER, cf_count, 0xC0FFEE00);
                        black_box(stats)
                    })
                },
            );
        }

        // ----------------------------------------------------
        // Concurrency scaling (fs_nosync only)
        // ----------------------------------------------------
        for &threads in &THREAD_COUNTS {
            let (engine, _tmp) = setup_engine_fs_nosync();

            for i in 1..cf_count {
                let _ =
                    engine.create_column_family(&format!("cf{cf_count}_{i}"), Default::default());
            }

            load_full_dataset(&engine);

            let engine = Arc::new(engine);
            let ops_per_thread = OPS_PER_ITER / threads;

            group.bench_with_input(
                BenchmarkId::new(format!("concurrent_cf{cf_count}"), threads),
                &threads,
                |b, &_t| {
                    b.iter(|| {
                        let handles: Vec<_> = (0..threads)
                            .map(|tid| {
                                let engine = Arc::clone(&engine);
                                thread::spawn(move || {
                                    run_workload_c_concurrent(engine, ops_per_thread, tid, cf_count)
                                })
                            })
                            .collect();

                        let _stats: Vec<_> =
                            handles.into_iter().map(|h| h.join().unwrap()).collect();

                        black_box(_stats)
                    })
                },
            );
        }
    }

    group.finish();
}

criterion_group! {
    name = ycsb_workload_c;
    config = criterion_config();
    targets = bench_workload_c
}
criterion_main!(ycsb_workload_c);
