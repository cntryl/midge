//! YCSB Workload D — Read-Latest (95% Read / 5% Update)
//!
//! - Biases reads toward the "latest" keys (tail of keyspace)
//! - 95% reads, 5% updates
//! - Scales over CF counts (1, 2, 4, 8, 16)
//! - Scales concurrency (1, 2, 8 threads)
//!
//! Hot path rules:
//! - No heap allocations or string formatting
//! - Uses pre-generated keys/values (OnceLock)
//! - Uses global Zipf generator for skew
//! - Measures p50 / p99 / p99.9 latency

#[path = "./criterion_helper.rs"]
mod criterion_helper;

#[path = "./tier4_integration_ycsb_common.rs"]
mod ycsb_common;

use cntryl_midge::MidgeEngine;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use hdrhistogram::Histogram;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use std::collections::VecDeque;
use std::hint::black_box;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use ycsb_common::*;

// Per-worker recent-update set size — reads in Workload D preferentially target
// keys that were written recently during RUN. This buffer is per-thread to avoid
// cross-thread coordination and preserve independent workers.
const RECENT_WINDOW: usize = 1024;
const RECENT_READ_PREFERENCE: f64 = 0.8; // 80% of reads (when buffer non-empty) choose recent keys

const CF_COUNTS: &[usize] = &[1, 4, 16]; // Reduced from [1,2,4,8,16] - cloud doesn't need full sweep
const READ_RATIO: f64 = 0.95;

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

// Helper: map a Zipf sample into "latest" region — tail of keyspace.
#[inline]
fn latest_index_from_zipf(zipf_sample: usize) -> usize {
    // zipf_sample is biased to small numbers; invert to hit tail
    let base = zipf_sample.min(RECORD_COUNT - 1);

    RECORD_COUNT - 1 - base
}

// ============================================================================
// Core workload logic
// ============================================================================

fn run_workload_d(
    engine: &MidgeEngine,
    operations: usize,
    cf_count: usize,
    seed: u64,
) -> LatencyStats {
    let cf_list = engine
        .list_column_families()
        .expect("failed to list column families");

    let keys = PREGEN_KEYS.get().unwrap();
    let values = PREGEN_VALUES.get().unwrap();
    let zipf = ZIPF_DEFAULT.get().unwrap();

    let mut rng = StdRng::seed_from_u64(seed);
    let mut hist = Histogram::<u64>::new(3).unwrap();

    // Per-worker recent-update buffer for read-latest behavior. Kept small and
    // local to the worker to avoid cross-thread coordination.
    let mut recent: VecDeque<usize> = VecDeque::with_capacity(RECENT_WINDOW);

    for _ in 0..operations {
        // Decide key: for reads prefer recently-updated keys in this worker
        let mut key_id = latest_index_from_zipf(zipf.next(&mut rng));

        // Prefer recent keys when available
        if !recent.is_empty() && rng.gen_bool(RECENT_READ_PREFERENCE) {
            let idx = rng.gen_range(0..recent.len());
            key_id = *recent.get(idx).unwrap();
        }

        let key = &keys[key_id];
        let val = &values[key_id];

        let cf = &cf_list[rng.gen_range(0..cf_count)];

        let start = Instant::now();
        if rng.gen_bool(READ_RATIO) {
            // 95% reads
            let _ = black_box(engine.get(cf, key));
        } else {
            // 5% update — perform the write synchronously (no batching) to preserve
            // causal visibility and avoid cross-thread coordination.
            engine.put(cf, key.as_ref(), val.as_ref()).unwrap();

            // Track this write as "recent" for future reads in this worker.
            if recent.len() >= RECENT_WINDOW {
                recent.pop_front();
            }
            recent.push_back(key_id);
        }
        let elapsed_us = start.elapsed().as_micros() as u64;
        let _ = hist.record(elapsed_us.max(1));
    }

    LatencyStats {
        p50: hist.value_at_percentile(50.0),
        p99: hist.value_at_percentile(99.0),
        p99_9: hist.value_at_percentile(99.9),
    }
}

fn run_workload_d_concurrent(
    engine: Arc<MidgeEngine>,
    ops_per_thread: usize,
    thread_id: usize,
    cf_count: usize,
) -> LatencyStats {
    let cf_list = engine
        .list_column_families()
        .expect("failed to list column families");

    let keys = PREGEN_KEYS.get().unwrap();
    let values = PREGEN_VALUES.get().unwrap();
    let zipf = ZIPF_DEFAULT.get().unwrap();

    let mut rng = make_thread_rng(thread_id, 0xD00D_D00D);
    let mut hist = Histogram::<u64>::new(3).unwrap();
    let mut recent: VecDeque<usize> = VecDeque::with_capacity(RECENT_WINDOW);

    for _ in 0..ops_per_thread {
        // Prefer recently-updated keys (local buffer) for reads
        let mut key_id = latest_index_from_zipf(zipf.next(&mut rng));
        if !recent.is_empty() && rng.gen_bool(RECENT_READ_PREFERENCE) {
            let idx = rng.gen_range(0..recent.len());
            key_id = *recent.get(idx).unwrap();
        }

        let key = &keys[key_id];
        let val = &values[key_id];

        let cf = &cf_list[rng.gen_range(0..cf_count)];

        let start = Instant::now();
        if rng.gen_bool(READ_RATIO) {
            let _ = black_box(engine.get(cf, key));
        } else {
            engine.put(cf, key.as_ref(), val.as_ref()).unwrap();

            if recent.len() >= RECENT_WINDOW {
                recent.pop_front();
            }
            recent.push_back(key_id);
        }
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

fn bench_workload_d(c: &mut Criterion) {
    // Ensure OnceLock globals exist
    init_ycsb_globals();

    let mut group = c.benchmark_group("ycsb_workload_d_cf_variants");
    group.throughput(Throughput::Elements(OPS_PER_ITER as u64));

    let scenarios = ["fs_nosync", "fs_sync", "cloud_nosync", "cloud_sync"];

    for &cf_count in CF_COUNTS {
        // --------------------------------------------------------------------
        // Storage scenarios
        // --------------------------------------------------------------------
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
                    let (e, _b) = setup_engine_cloud_nosync();
                    e
                }
                "cloud_sync" => {
                    let (e, _b) = setup_engine_cloud_sync();
                    e
                }
                _ => unreachable!(),
            };

            // CFs up front, no metadata changes during benchmark
            for i in 1..cf_count {
                let _ = engine.create_column_family(&format!("cf{cf_count}_{i}"));
            }

            // Load full dataset once per scenario
            load_full_dataset(&engine);

            // Do a single deterministic RUN to record and print RUN stats (separate from LOAD).
            let start = Instant::now();
            let run_stats = run_workload_d(&engine, OPS_PER_ITER, cf_count, 0xFACE_FEED);
            let dur = start.elapsed();
            let throughput = (OPS_PER_ITER as f64) / dur.as_secs_f64();
            eprintln!("RUN STATS (single-run): scenario={} cf={} ops={} duration_s={:.3} throughput_op_s={:.0} p50={} p99={} p99_9={}",
                      scenario, cf_count, OPS_PER_ITER, dur.as_secs_f64(), throughput, run_stats.p50, run_stats.p99, run_stats.p99_9);

            group.bench_with_input(
                BenchmarkId::new(format!("{scenario}_cf{cf_count}"), "read_latest_95r_5u"),
                &cf_count,
                |b, &_cf| {
                    b.iter(|| {
                        let stats = run_workload_d(&engine, OPS_PER_ITER, cf_count, 0xFACE_FEED);
                        black_box(stats)
                    })
                },
            );
        }

        // --------------------------------------------------------------------
        // Concurrency scaling (fs_nosync only)
        // --------------------------------------------------------------------
        for &threads in &THREAD_COUNTS {
            let (engine, _t) = setup_engine_fs_nosync();

            for i in 1..cf_count {
                let _ = engine.create_column_family(&format!("cf{cf_count}_{i}"));
            }
            load_full_dataset(&engine);

            let engine = Arc::new(engine);
            let ops_per_thread = OPS_PER_ITER / threads;

            // Single concurrent RUN to capture wall-clock throughput and per-thread latency summaries
            let start = Instant::now();
            let handles: Vec<_> = (0..threads)
                .map(|tid| {
                    let engine = Arc::clone(&engine);
                    thread::spawn(move || {
                        run_workload_d_concurrent(engine, ops_per_thread, tid, cf_count)
                    })
                })
                .collect();

            let thread_stats: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
            let dur = start.elapsed();
            let total_ops = ops_per_thread * threads;
            let throughput = (total_ops as f64) / dur.as_secs_f64();
            let mean_p50: f64 =
                thread_stats.iter().map(|s| s.p50 as f64).sum::<f64>() / (threads as f64);
            let mean_p99: f64 =
                thread_stats.iter().map(|s| s.p99 as f64).sum::<f64>() / (threads as f64);
            let mean_p999: f64 =
                thread_stats.iter().map(|s| s.p99_9 as f64).sum::<f64>() / (threads as f64);
            eprintln!("RUN STATS (concurrent single-run): cf={} threads={} ops={} duration_s={:.3} throughput_op_s={:.0} avg_p50={} avg_p99={} avg_p99_9={}",
                      cf_count, threads, total_ops, dur.as_secs_f64(), throughput, mean_p50, mean_p99, mean_p999);

            group.bench_with_input(
                BenchmarkId::new(format!("concurrent_cf{cf_count}"), threads),
                &threads,
                |b, &_t| {
                    b.iter(|| {
                        let handles: Vec<_> = (0..threads)
                            .map(|tid| {
                                let engine = Arc::clone(&engine);
                                thread::spawn(move || {
                                    run_workload_d_concurrent(engine, ops_per_thread, tid, cf_count)
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
    name = tier4_integration_ycsb_workload_d;
    config = criterion_config_for_tier(BenchTier::Tier4Integration);
    targets = bench_workload_d
}
criterion_main!(tier4_integration_ycsb_workload_d);
