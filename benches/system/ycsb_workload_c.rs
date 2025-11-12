//! YCSB Workload C: 100% Read (Read-Only)
//!
//! Benchmarks read-heavy workload across column families (1, 2, 4, 8, 16)
//! and scales concurrency (1, 2, 8 threads).

#[path = "../criterion_helper.rs"]
mod criterion_helper;
#[path = "ycsb_common.rs"]
mod ycsb_common;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use criterion_helper::criterion_config;

use cntryl_midge::MidgeEngine;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;
use std::thread;
use ycsb_common::*;

const CF_COUNTS: &[usize] = &[1, 2, 4, 8, 16];

// ============================================================================
// Core workload logic
// ============================================================================

fn run_workload_c(engine: &MidgeEngine, operations: usize, record_count: usize, cf_count: usize) {
    let cf_list = engine.list_column_families();
    let mut rng = StdRng::seed_from_u64(12345);
    let zipfian = ZipfianGenerator::new(record_count, 0.99);

    for _ in 0..operations {
        let key_id = zipfian.next(&mut rng);
        let key = generate_key(key_id);
        let cf_index = rng.gen_range(0..cf_count);
        let cf = &cf_list[cf_index];
        let _ = black_box(engine.get(&cf, &key));
    }
}

fn run_workload_c_concurrent(
    engine: Arc<MidgeEngine>,
    operations_per_thread: usize,
    record_count: usize,
    thread_id: usize,
    cf_count: usize,
) {
    let cf_list = engine.list_column_families();
    let mut rng = StdRng::seed_from_u64(12345 + thread_id as u64);
    let zipfian = ZipfianGenerator::new(record_count, 0.99);

    for _ in 0..operations_per_thread {
        let key_id = zipfian.next(&mut rng);
        let key = generate_key(key_id);
        let cf_index = rng.gen_range(0..cf_count);
        let cf = &cf_list[cf_index];
        let _ = black_box(engine.get(&cf, &key));
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
                    b.iter(|| run_workload_c(&engine, OPS_PER_ITER, RECORD_COUNT, cf_count))
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
                        for h in handles {
                            h.join().unwrap();
                        }
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
