//! YCSB Workload F — Read-Modify-Write (RMW)
//!
//! Models workloads where each operation:
//!   1. Reads a record
//!   2. Modifies it
//!   3. Writes it back
//!
//! This is a classic YCSB-style RMW workload.
//!
//! Features:
//! - Uses pre-generated keys/values (no allocations in hot loop)
//! - Zipfian access pattern via global ZIPF_DEFAULT
//! - p50 / p99 / p99.9 latency for full RMW operation
//! - Scales by column families and thread counts

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

fn run_workload_f(
    engine: &MidgeEngine,
    operations: usize,
    _record_count: usize,
    cf_count: usize,
    seed: u64,
) -> LatencyStats {
    let cf_list = engine.list_column_families();

    // Pre-generated key/value pools
    let keys = PREGEN_KEYS.get().expect("call init_ycsb_globals()");
    let values = PREGEN_VALUES.get().expect("call init_ycsb_globals()");
    let zipf = ZIPF_DEFAULT.get().expect("call init_ycsb_globals()");

    let mut rng = StdRng::seed_from_u64(seed);
    let mut hist = Histogram::<u64>::new(3).unwrap();

    for _ in 0..operations {
        // Choose key via Zipfian distribution
        let key_idx = zipf.next(&mut rng);
        let key = &keys[key_idx];

        // Choose CF
        let cf = &cf_list[rng.gen_range(0..cf_count)];

        // Choose a new value (RMW doesn't care about value derivation)
        let new_val_idx = rng.gen_range(0..values.len());
        let new_val = &values[new_val_idx];

        let start = Instant::now();

        // Read
        let _existing = engine.get(cf, key).unwrap();
        black_box(&_existing);

        // Modify + Write (update)
        engine.put(cf, key, new_val).unwrap();

        let elapsed_us = start.elapsed().as_micros() as u64;
        let _ = hist.record(elapsed_us.max(1));
    }

    LatencyStats {
        p50: hist.value_at_percentile(50.0),
        p99: hist.value_at_percentile(99.0),
        p99_9: hist.value_at_percentile(99.9),
    }
}

fn run_workload_f_concurrent(
    engine: Arc<MidgeEngine>,
    operations_per_thread: usize,
    _record_count: usize,
    thread_id: usize,
    cf_count: usize,
) -> LatencyStats {
    let cf_list = engine.list_column_families();

    let keys = PREGEN_KEYS.get().expect("call init_ycsb_globals()");
    let values = PREGEN_VALUES.get().expect("call init_ycsb_globals()");
    let zipf = ZIPF_DEFAULT.get().expect("call init_ycsb_globals()");

    let mut rng = make_thread_rng(thread_id, 0xF0F0_F0F0);
    let mut hist = Histogram::<u64>::new(3).unwrap();

    for _ in 0..operations_per_thread {
        let key_idx = zipf.next(&mut rng);
        let key = &keys[key_idx];

        let cf = &cf_list[rng.gen_range(0..cf_count)];

        let new_val_idx = rng.gen_range(0..values.len());
        let new_val = &values[new_val_idx];

        let start = Instant::now();

        let _existing = engine.get(cf, key).unwrap();
        black_box(&_existing);

        engine.put(cf, key, new_val).unwrap();

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

fn bench_workload_f(c: &mut Criterion) {
    // Initialize OnceLock globals for keys/values/zipf
    init_ycsb_globals();

    let mut group = c.benchmark_group("ycsb_workload_f_cf_variants");
    group.throughput(Throughput::Elements(OPS_PER_ITER as u64));

    let scenarios = ["fs_nosync", "fs_sync", "cloud_nosync", "cloud_sync"];

    for &cf_count in CF_COUNTS {
        // Storage variations
        for &scenario in &scenarios {
            let engine = match scenario {
                "fs_nosync" => {
                    let (e, _t) = setup_engine_fs_nosync();
                    e
                }
                "fs_sync" => {
                    let (e, _t) = setup_engine_fs_sync();
                    e
                }
                "cloud_nosync" => {
                    let (e, _b) = setup_engine_cloud_nosync_with_latency(1);
                    e
                }
                "cloud_sync" => {
                    let (e, _b) = setup_engine_cloud_sync_with_latency(1);
                    e
                }
                _ => unreachable!(),
            };

            // Create additional CFs
            for i in 1..cf_count {
                let _ = engine.create_column_family(&format!("cf{cf_count}_{i}"), Default::default());
            }

            // Pre-load full dataset once per engine
            load_full_dataset(&engine);

            group.bench_with_input(
                BenchmarkId::new(format!("{scenario}_cf{cf_count}"), "rmw"),
                &cf_count,
                |b, &_cf| {
                    b.iter(|| {
                        let stats = run_workload_f(
                            &engine,
                            OPS_PER_ITER,
                            RECORD_COUNT,
                            cf_count,
                            0xDEAD_BEEF,
                        );
                        black_box(stats)
                    })
                },
            );
        }

        // Concurrent: focus on fs_nosync for throughput scaling
        for &threads in &THREAD_COUNTS {
            let (engine, _t) = setup_engine_fs_nosync();

            for i in 1..cf_count {
                let _ = engine.create_column_family(&format!("cf{cf_count}_{i}"), Default::default());
            }
            load_full_dataset(&engine);

            let engine = Arc::new(engine);
            let total_ops = OPS_PER_ITER;
            let ops_per_thread = total_ops / threads;

            group.bench_with_input(
                BenchmarkId::new(format!("concurrent_cf{cf_count}"), threads),
                &threads,
                |b, &_threads| {
                    b.iter(|| {
                        let handles: Vec<_> = (0..threads)
                            .map(|tid| {
                                let engine = Arc::clone(&engine);
                                thread::spawn(move || {
                                    run_workload_f_concurrent(
                                        engine,
                                        ops_per_thread,
                                        RECORD_COUNT,
                                        tid,
                                        cf_count,
                                    )
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
    name = ycsb_workload_f;
    config = criterion_config();
    targets = bench_workload_f
}
criterion_main!(ycsb_workload_f);
