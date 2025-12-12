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

#[path = "../criterion_helper.rs"]
mod criterion_helper;

#[path = "ycsb_common.rs"]
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
    let cf_list = engine.list_column_families();

    let keys = PREGEN_KEYS.get().unwrap();
    let values = PREGEN_VALUES.get().unwrap();
    let zipf = ZIPF_DEFAULT.get().unwrap();

    let mut rng = StdRng::seed_from_u64(seed);
    let mut hist = Histogram::<u64>::new(3).unwrap();

    // Reuse a small WriteBatch for updates; keeps API realistic but
    // ensures no internal allocations in the hot loop.
    let mut batch = WriteBatch::new();

    for _ in 0..operations {
        let key_id = latest_index_from_zipf(zipf.next(&mut rng));
        let key = &keys[key_id];
        let val = &values[key_id];

        let cf = &cf_list[rng.gen_range(0..cf_count)];
        let cf_id = cf.id();

        let start = Instant::now();
        if rng.gen_bool(READ_RATIO) {
            // 95% reads
            let _ = black_box(engine.get(cf, key));
        } else {
            // 5% updates, grouped into small batches
            batch.put_cf(cf_id, key.clone(), val.clone());

            if batch.len() >= BATCH_SIZE {
                engine.write_batch(&batch).unwrap();
                batch.clear();
            }
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

fn run_workload_d_concurrent(
    engine: Arc<MidgeEngine>,
    ops_per_thread: usize,
    thread_id: usize,
    cf_count: usize,
) -> LatencyStats {
    let cf_list = engine.list_column_families();

    let keys = PREGEN_KEYS.get().unwrap();
    let values = PREGEN_VALUES.get().unwrap();
    let zipf = ZIPF_DEFAULT.get().unwrap();

    let mut rng = make_thread_rng(thread_id, 0xD00D_D00D);
    let mut hist = Histogram::<u64>::new(3).unwrap();
    let mut batch = WriteBatch::new();

    for _ in 0..ops_per_thread {
        let key_id = latest_index_from_zipf(zipf.next(&mut rng));
        let key = &keys[key_id];
        let val = &values[key_id];

        let cf = &cf_list[rng.gen_range(0..cf_count)];
        let cf_id = cf.id();

        let start = Instant::now();
        if rng.gen_bool(READ_RATIO) {
            let _ = black_box(engine.get(cf, key));
        } else {
            batch.put_cf(cf_id, key.clone(), val.clone());
            if batch.len() >= BATCH_SIZE {
                engine.write_batch(&batch).unwrap();
                batch.clear();
            }
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
                let _ =
                    engine.create_column_family(&format!("cf{cf_count}_{i}"), Default::default());
            }

            // Load full dataset once per scenario
            load_full_dataset(&engine);

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
