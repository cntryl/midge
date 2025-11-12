//! YCSB Workload C: 100% Read (Read-Only)
//!
//! Benchmarks read-heavy workload across column families (1, 2, 4, 8, 16)
//! and scales concurrency (1, 2, 8 threads).
//!
//! **Enhanced with Latency Tracking:**
//! - Measures p50, p99, p99.9 read latencies
//! - Reports cache efficiency and read-path performance

#[path = "../criterion_helper.rs"]
mod criterion_helper;
#[path = "ycsb_common.rs"]
mod ycsb_common;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use criterion_helper::criterion_config;
use cntryl_midge::MidgeEngine;
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
// Latency tracking structures
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

fn run_workload_c(engine: &MidgeEngine, operations: usize, record_count: usize, cf_count: usize) -> LatencyStats {
    let cf_list = engine.list_column_families();
    let mut rng = StdRng::seed_from_u64(12345);
    let zipfian = ZipfianGenerator::new(record_count, 0.99);
    let mut histogram = Histogram::<u64>::new(3).unwrap();

    for _ in 0..operations {
        let key_id = zipfian.next(&mut rng);
        let key = generate_key(key_id);
        let cf_index = rng.gen_range(0..cf_count);
        let cf = &cf_list[cf_index];
        
        let start = Instant::now();
        let _ = black_box(engine.get(&cf, &key));
        let elapsed_us = start.elapsed().as_micros() as u64;
        let _ = histogram.record(elapsed_us.max(1));
    }

    LatencyStats {
        p50: histogram.value_at_percentile(50.0),
        p99: histogram.value_at_percentile(99.0),
        p99_9: histogram.value_at_percentile(99.9),
    }
}

fn run_workload_c_concurrent(
    engine: Arc<MidgeEngine>,
    operations_per_thread: usize,
    record_count: usize,
    thread_id: usize,
    cf_count: usize,
) -> LatencyStats {
    let cf_list = engine.list_column_families();
    let mut rng = StdRng::seed_from_u64(12345 + thread_id as u64);
    let zipfian = ZipfianGenerator::new(record_count, 0.99);
    let mut histogram = Histogram::<u64>::new(3).unwrap();

    for _ in 0..operations_per_thread {
        let key_id = zipfian.next(&mut rng);
        let key = generate_key(key_id);
        let cf_index = rng.gen_range(0..cf_count);
        let cf = &cf_list[cf_index];
        
        let start = Instant::now();
        let _ = black_box(engine.get(&cf, &key));
        let elapsed_us = start.elapsed().as_micros() as u64;
        let _ = histogram.record(elapsed_us.max(1));
    }

    LatencyStats {
        p50: histogram.value_at_percentile(50.0),
        p99: histogram.value_at_percentile(99.0),
        p99_9: histogram.value_at_percentile(99.9),
    }
}

// ============================================================================
// Benchmark driver
// ============================================================================

fn bench_workload_c(c: &mut Criterion) {
    let mut group = c.benchmark_group("ycsb_workload_c_cf_variants");
    group.throughput(Throughput::Elements(OPS_PER_ITER as u64));

    // For this read-only benchmark we exercise two scenarios:
    // - fs_nosync (local disk, no WAL sync)
    // - cloud_nosync (cloud-backed, local cache, no WAL sync)
    let scenarios = ["fs_nosync", "cloud_nosync"];

    for &cf_count in CF_COUNTS {
        for &scenario in &scenarios {
            let (engine, _backend) = match scenario {
                "fs_nosync" => {
                    let (e, _t) = setup_engine_fs_nosync();
                    (e, None)
                }
                "cloud_nosync" => {
                    let (e, b) = setup_engine_cloud_nosync();
                    (e, Some(b))
                }
                _ => unreachable!(),
            };

            // Create additional column families
            for i in 1..cf_count {
                let _ = engine.create_column_family(
                    &format!("cf{}", i),
                    Default::default(),
                );
            }

            load_data(&engine, RECORD_COUNT);

            group.bench_with_input(
                BenchmarkId::new(format!("{scenario}_cf{cf_count}"), "100r"),
                &cf_count,
                |b, &_cf_count| {
                    b.iter(|| {
                        let stats = run_workload_c(&engine, OPS_PER_ITER, RECORD_COUNT, cf_count);
                        black_box(stats)
                    })
                },
            );
        }

        // Concurrent (threads × cf_count)
        for &threads in &THREAD_COUNTS {
            let (engine, _temp_dir) = setup_engine_fs_nosync();
            
            // Create additional column families
            for i in 1..cf_count {
                let _ = engine.create_column_family(
                    &format!("cf{}", i),
                    Default::default(),
                );
            }
            
            load_data(&engine, RECORD_COUNT);
            let engine = Arc::new(engine);
            let total_ops = OPS_PER_ITER;
            let ops_per_thread = total_ops / threads;

            group.bench_with_input(
                BenchmarkId::new(format!("concurrent_cf{cf_count}"), threads),
                &threads,
                |b, &_threads| {
                    b.iter(|| {
                        let handles: Vec<_> = (0..threads)
                            .map(|thread_id| {
                                let engine = Arc::clone(&engine);
                                thread::spawn(move || {
                                    run_workload_c_concurrent(
                                        engine,
                                        ops_per_thread,
                                        RECORD_COUNT,
                                        thread_id,
                                        cf_count,
                                    )
                                })
                            })
                            .collect();
                        let _stats: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
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
