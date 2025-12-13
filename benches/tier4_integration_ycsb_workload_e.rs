//! YCSB Workload E — Short Range Scan (Read-Only)
//!
//! Models real systems that perform:
//! - short forward scans (10–50 keys)
//! - hot-key distribution (Zipfian)
//!
//! Features:
//! - 100% read-only
//! - realistic range iteration
//! - p50 / p99 / p99.9 latency
//!
//! Zero-allocation rules:
//! - No string formatting in hot loop
//! - Uses PREGEN_KEYS and pre-generated scan ranges
//! - No heap allocations

#[path = "./criterion_helper.rs"]
mod criterion_helper;

#[path = "./tier4_integration_ycsb_common.rs"]
mod ycsb_common;

use bytes::Bytes;
use cntryl_midge::MidgeEngine;
use cntryl_midge::Query;
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
const SCAN_LENGTH: usize = 50;
const RANGE_COUNT: usize = OPS_PER_ITER;

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

fn run_workload_e(
    engine: &MidgeEngine,
    ranges: &[(Bytes, Bytes)],
    cf_count: usize,
    seed: u64,
) -> LatencyStats {
    let cf_list = engine.list_column_families();
    let mut rng = StdRng::seed_from_u64(seed);
    let mut hist = Histogram::<u64>::new(3).unwrap();

    // Use actual CF list length to avoid index out of bounds
    let cf_list_unwrapped = cf_list.as_ref().unwrap();
    let actual_cf_count = cf_list_unwrapped.len().min(cf_count);

    for (start_key, end_key) in ranges.iter() {
        let cf = &cf_list_unwrapped[rng.gen_range(0..actual_cf_count)];

        let start = Instant::now();

        // Execute the scan
        let iter = engine
            .scan(
                cf,
                Query::new()
                    .start_key(start_key.clone())
                    .end_key(end_key.clone()),
            )
            .unwrap();
        for item in &iter {
            black_box(item);
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

fn run_workload_e_concurrent(
    engine: Arc<MidgeEngine>,
    ranges: Arc<Vec<(Bytes, Bytes)>>,
    ops_per_thread: usize,
    thread_id: usize,
    cf_count: usize,
) -> LatencyStats {
    let cf_list = engine.list_column_families();
    let mut rng = make_thread_rng(thread_id, 0xABCDEF01);

    let mut hist = Histogram::<u64>::new(3).unwrap();

    // Use actual CF list length to avoid index out of bounds
    let actual_cf_count = cf_list.len().min(cf_count);

    for i in 0..ops_per_thread.min(ranges.len()) {
        let (start_key, end_key) = &ranges[i];
        let cf = &cf_list[rng.gen_range(0..actual_cf_count)];

        let start = Instant::now();

        let iter = engine
            .scan(
                cf,
                Query::new()
                    .start_key(start_key.clone())
                    .end_key(end_key.clone()),
            )
            .unwrap();
        for item in &iter {
            black_box(item);
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

fn bench_workload_e(c: &mut Criterion) {
    // Init OnceLock globals
    init_ycsb_globals();

    let scan_ranges = pregen_scan_ranges(RANGE_COUNT, SCAN_LENGTH);
    let scan_ranges = Arc::new(scan_ranges);

    let mut group = c.benchmark_group("ycsb_workload_e_cf_variants");
    group.throughput(Throughput::Elements(OPS_PER_ITER as u64));

    let scenarios = ["fs_nosync", "cloud_nosync"];

    for &cf_count in CF_COUNTS {
        // Storage variations
        for &scenario in &scenarios {
            let engine = match scenario {
                "fs_nosync" => {
                    let (e, _t) = setup_engine_fs_nosync();
                    e
                }
                "cloud_nosync" => {
                    let (e, _b) = setup_engine_cloud_nosync();
                    e
                }
                _ => unreachable!(),
            };

            for i in 1..cf_count {
                let _ =
                    engine.create_column_family(&format!("cf{cf_count}_{i}"));
            }

            load_full_dataset(&engine);

            let ranges_ref = scan_ranges.clone();

            group.bench_with_input(
                BenchmarkId::new(format!("{scenario}_cf{cf_count}"), "range_scan"),
                &cf_count,
                |b, &_cf| {
                    b.iter(|| {
                        let stats =
                            run_workload_e(&engine, ranges_ref.as_slice(), cf_count, 0xFEED_CAFE);
                        black_box(stats)
                    })
                },
            );
        }

        // Concurrent: fs_nosync only
        for &threads in &THREAD_COUNTS {
            let (engine, _t) = setup_engine_fs_nosync();
            for i in 1..cf_count {
                let _ =
                    engine.create_column_family(&format!("cf{cf_count}_{i}"));
            }
            load_full_dataset(&engine);

            let engine = Arc::new(engine);
            let ranges = scan_ranges.clone();
            let ops_per_thread = OPS_PER_ITER / threads;

            group.bench_with_input(
                BenchmarkId::new(format!("concurrent_cf{cf_count}"), threads),
                &threads,
                |b, &_t| {
                    b.iter(|| {
                        let handles: Vec<_> = (0..threads)
                            .map(|tid| {
                                let engine = Arc::clone(&engine);
                                let ranges = ranges.clone();

                                thread::spawn(move || {
                                    run_workload_e_concurrent(
                                        engine,
                                        ranges,
                                        ops_per_thread,
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
    name = tier4_integration_ycsb_workload_e;
    config = criterion_config_for_tier(BenchTier::Tier4Integration);
    targets = bench_workload_e
}
criterion_main!(tier4_integration_ycsb_workload_e);
