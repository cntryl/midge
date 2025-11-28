//! YCSB Workload A — 50% Read / 50% Update (Update-Heavy)
//!
//! Behavior Profile:
//! - Random point reads (Zipfian)
//! - Random updates (Zipfian)
//! - Batched writes (BATCH_SIZE)
//! - Varying CF counts (1, 2, 4, 8, 16)
//! - Varying thread counts (1, 2, 8)
//!
//! Latency Tracking:
//! - p50, p99, p99.9 read/update latencies
//!
//! All hot loops: NO allocations, NO formatting, NO RNG except u64 mixing.

#[path = "../criterion_helper.rs"]
mod criterion_helper;
#[path = "ycsb_common.rs"]
mod ycsb_common;

use cntryl_midge::{MidgeEngine, WriteBatch};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use hdrhistogram::Histogram;

use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};

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
#[allow(dead_code)]
struct LatencyStats {
    p50: u64,
    p99: u64,
    p99_9: u64,
}

// ============================================================================
// Core workload logic
// ============================================================================

fn run_workload_a(
    engine: &MidgeEngine,
    operations: usize,
    _record_count: usize,
    cf_count: usize,
    rng_seed: u64,
) -> LatencyStats {
    let cf_list = engine.list_column_families();
    let mut rng = StdRng::seed_from_u64(rng_seed);

    // Precomputed Zipfian
    let zipf = ZIPF_DEFAULT.get().unwrap();

    // Latency histogram
    let mut hist = Histogram::<u64>::new(3).unwrap();

    // Pre-allocated WriteBatch
    let mut batch = WriteBatch::new();

    for _ in 0..operations {
        // Pick key
        let key_id = zipf.next(&mut rng);
        let key = PREGEN_KEYS.get().unwrap()[key_id].clone();

        // Pick CF
        let cf = &cf_list[rng.gen_range(0..cf_count)];
        let cf_id = cf.id();

        let start = Instant::now();

        if rng.next_u32() & 1 == 0 {
            // ----- READ -----
            let _ = black_box(engine.get(cf, &key));
        } else {
            // ----- WRITE -----
            let value = PREGEN_VALUES.get().unwrap()[key_id].clone();
            batch.put(cf_id, key, value);

            if batch.len() >= BATCH_SIZE {
                engine.write_batch(&batch).unwrap();
                batch.clear();
            }
        }

        let elapsed = start.elapsed().as_micros() as u64;
        let _ = hist.record(elapsed.max(1));
    }

    if !batch.is_empty() {
        engine.write_batch(&batch).unwrap();
    }

    LatencyStats {
        p50: hist.value_at_percentile(50.0),
        p99: hist.value_at_percentile(99.0),
        p99_9: hist.value_at_percentile(99.9),
    }
}

// ----- Concurrent Version ----------------------------------------------------

fn run_workload_a_concurrent(
    engine: Arc<MidgeEngine>,
    ops_per_thread: usize,
    _record_count: usize,
    thread_id: usize,
    cf_count: usize,
) -> LatencyStats {
    let cf_list = engine.list_column_families();
    let mut rng = make_thread_rng(thread_id, 0xCAFEBABE);

    let zipf = ZIPF_DEFAULT.get().unwrap();

    let mut hist = Histogram::<u64>::new(3).unwrap();
    let mut batch = WriteBatch::new();

    for _ in 0..ops_per_thread {
        let key_id = zipf.next(&mut rng);
        let key = PREGEN_KEYS.get().unwrap()[key_id].clone();

        let cf = &cf_list[rng.gen_range(0..cf_count)];
        let cf_id = cf.id();

        let start = Instant::now();

        if rng.next_u32() & 1 == 0 {
            let _ = black_box(engine.get(cf, &key));
        } else {
            let value = PREGEN_VALUES.get().unwrap()[key_id].clone();
            batch.put(cf_id, key, value);

            if batch.len() >= BATCH_SIZE {
                engine.write_batch(&batch).unwrap();
                batch.clear();
            }
        }

        let elapsed = start.elapsed().as_micros() as u64;
        let _ = hist.record(elapsed.max(1));
    }

    if !batch.is_empty() {
        engine.write_batch(&batch).unwrap();
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

fn bench_workload_a(c: &mut Criterion) {
    init_ycsb_globals();

    let mut group = c.benchmark_group("ycsb_workload_a_cf_variants");
    group.throughput(Throughput::Elements(OPS_PER_ITER as u64));

    let scenarios = ["fs_nosync", "fs_sync", "cloud_nosync", "cloud_sync"];

    for &cf_count in CF_COUNTS {
        for &scenario in &scenarios {
            let (engine, _tmp) = match scenario {
                "fs_nosync" => setup_engine_fs_nosync(),
                "fs_sync" => setup_engine_fs_sync(),
                "cloud_nosync" => setup_engine_cloud_nosync(),
                "cloud_sync" => setup_engine_cloud_sync(),
                _ => unreachable!(),
            };

            // Create CFs
            for i in 1..cf_count {
                let _ =
                    engine.create_column_family(&format!("cf{cf_count}_{i}"), Default::default());
            }

            // Load all data
            load_full_dataset(&engine);

            group.bench_with_input(
                BenchmarkId::new(format!("{scenario}_cf{cf_count}"), "50r_50w"),
                &cf_count,
                |b, &_cf| {
                    b.iter(|| {
                        let stats =
                            run_workload_a(&engine, OPS_PER_ITER, RECORD_COUNT, cf_count, 0xABCDEF);
                        black_box(stats)
                    })
                },
            );
        }

        // ----- CONCURRENT CF × THREADS -------------------------------------

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
                                    run_workload_a_concurrent(
                                        engine,
                                        ops_per_thread,
                                        RECORD_COUNT,
                                        tid,
                                        cf_count,
                                    )
                                })
                            })
                            .collect();

                        let _results: Vec<_> =
                            handles.into_iter().map(|h| h.join().unwrap()).collect();

                        black_box(_results)
                    })
                },
            );
        }
    }

    group.finish();
}

criterion_group! {
    name = tier4_integration_ycsb_workload_a;
    config = criterion_config_for_tier(BenchTier::Tier4Integration);
    targets = bench_workload_a
}
criterion_main!(tier4_integration_ycsb_workload_a);
