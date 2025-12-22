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

#[path = "./criterion_helper.rs"]
mod criterion_helper;

#[path = "./tier4_integration_ycsb_common.rs"]
mod ycsb_common;

use cntryl_midge::{MidgeEngine, WriteBatch};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use hdrhistogram::Histogram;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use std::hint::black_box;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use ycsb_common::*;

const CF_COUNTS: &[usize] = &[1, 4, 16]; // Reduced from [1,2,4,8,16] - cloud doesn't need full sweep

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

fn run_workload_f(
    engine: &MidgeEngine,
    operations: usize,
    _record_count: usize,
    cf_count: usize,
    seed: u64,
) -> LatencyStats {
    let cf_list = engine.list_column_families().unwrap_or_default();

    // Pre-generated key/value pools
    let keys = PREGEN_KEYS.get().expect("call init_ycsb_globals()");
    let values = PREGEN_VALUES.get().expect("call init_ycsb_globals()");
    let zipf = ZIPF_DEFAULT.get().expect("call init_ycsb_globals()");

    let mut rng = StdRng::seed_from_u64(seed);
    let mut hist = Histogram::<u64>::new(3).unwrap();
    let mut batch = WriteBatch::new();

    for _ in 0..operations {
        // Choose key via Zipfian distribution
        let key_idx = zipf.next(&mut rng);
        let key = &keys[key_idx];

        // Choose CF
        let cf = &cf_list[rng.gen_range(0..cf_count)];
        let cf_id = cf.id();

        // Choose a new value (RMW doesn't care about value derivation)
        let new_val_idx = rng.gen_range(0..values.len());
        let new_val = &values[new_val_idx];

        let start = Instant::now();

        // Read
        let _existing = engine.get(cf, key).unwrap();
        black_box(&_existing);

        // Modify + Write (update)
        batch.put_cf(cf_id, key.clone(), new_val.clone());

        if batch.len() >= BATCH_SIZE {
            engine.write_batch(&batch).unwrap();
            batch.clear();
        }

        let elapsed_us = start.elapsed().as_micros() as u64;
        let _ = hist.record(elapsed_us.max(1));
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

fn run_workload_f_concurrent(
    engine: Arc<MidgeEngine>,
    operations_per_thread: usize,
    _record_count: usize,
    thread_id: usize,
    cf_count: usize,
) -> LatencyStats {
    let cf_list = engine.list_column_families().unwrap_or_default();

    let keys = PREGEN_KEYS.get().expect("call init_ycsb_globals()");
    let values = PREGEN_VALUES.get().expect("call init_ycsb_globals()");
    let zipf = ZIPF_DEFAULT.get().expect("call init_ycsb_globals()");

    let mut rng = make_thread_rng(thread_id, 0xF0F0_F0F0);
    let mut hist = Histogram::<u64>::new(3).unwrap();

    for _ in 0..operations_per_thread {
        let key_idx = zipf.next(&mut rng);
        let key = &keys[key_idx];

        let cf = &cf_list[rng.gen_range(0..cf_count)];

        let start = Instant::now();

        let existing = engine.get(cf, key).unwrap();
        black_box(&existing);

        let new_val = if let Some(v) = existing {
            let mut buf = v.to_vec();
            if !buf.is_empty() {
                buf[0] = buf[0].wrapping_add(1);
            }
            bytes::Bytes::from(buf)
        } else {
            values[key_idx % values.len()].clone()
        };

        engine.put(cf, key.as_ref(), new_val.as_ref()).unwrap();

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
                    let (e, _b) = setup_engine_cloud_nosync();
                    e
                }
                "cloud_sync" => {
                    let (e, _b) = setup_engine_cloud_sync();
                    e
                }
                _ => unreachable!(),
            };

            // Create additional CFs
            for i in 1..cf_count {
                let _ = engine.create_column_family(&format!("cf{cf_count}_{i}"));
            }

            // Pre-load full dataset once per engine
            load_full_dataset(&engine);

            // Single deterministic RUN to get RMW RUN stats (separate from LOAD)
            let start = Instant::now();
            let run_stats = run_workload_f(&engine, OPS_PER_ITER, RECORD_COUNT, cf_count, 0xDEAD_BEEF);
            let dur = start.elapsed();
            let throughput = (OPS_PER_ITER as f64) / dur.as_secs_f64();
            eprintln!("RUN STATS (single-run): scenario={} cf={} ops={} duration_s={:.3} throughput_op_s={:.0} p50={} p99={} p99_9={}",
                      scenario, cf_count, OPS_PER_ITER, dur.as_secs_f64(), throughput, run_stats.p50, run_stats.p99, run_stats.p99_9);

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
                let _ = engine.create_column_family(&format!("cf{cf_count}_{i}"));
            }
            load_full_dataset(&engine);

            let engine = Arc::new(engine);
            let total_ops = OPS_PER_ITER;
            let ops_per_thread = total_ops / threads;

            // Single concurrent-run to capture throughput and per-thread latency summaries
            let start = Instant::now();
            let handles: Vec<_> = (0..threads)
                .map(|tid| {
                    let engine = Arc::clone(&engine);
                    thread::spawn(move || {
                        run_workload_f_concurrent(engine, ops_per_thread, RECORD_COUNT, tid, cf_count)
                    })
                })
                .collect();

            let thread_stats: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
            let dur = start.elapsed();
            let throughput = (total_ops as f64) / dur.as_secs_f64();
            let mean_p50: f64 = thread_stats.iter().map(|s| s.p50 as f64).sum::<f64>() / (threads as f64);
            let mean_p99: f64 = thread_stats.iter().map(|s| s.p99 as f64).sum::<f64>() / (threads as f64);
            let mean_p999: f64 = thread_stats.iter().map(|s| s.p99_9 as f64).sum::<f64>() / (threads as f64);
            eprintln!("RUN STATS (concurrent single-run): cf={} threads={} ops={} duration_s={:.3} throughput_op_s={:.0} avg_p50={} avg_p99={} avg_p99_9={}",
                      cf_count, threads, total_ops, dur.as_secs_f64(), throughput, mean_p50, mean_p99, mean_p999);

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
    name = tier4_integration_ycsb_workload_f;
    config = criterion_config_for_tier(BenchTier::Tier4Integration);
    targets = bench_workload_f
}
criterion_main!(tier4_integration_ycsb_workload_f);
