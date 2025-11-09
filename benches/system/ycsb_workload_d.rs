//! YCSB Workload D: 95% Read Latest / 5% Insert - consolidated scenarios

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

fn run_workload_d(engine: &MidgeEngine, operations: usize, record_count: usize) {
    let cf = engine.default_column_family();
    let mut rng = StdRng::seed_from_u64(12345);
    let zipfian = ZipfianGenerator::new(record_count, 0.99);
    let mut insert_key_id = record_count;

    for _ in 0..operations {
        if rng.random_bool(0.95) {
            // Read latest - skewed towards recently inserted keys
            let key_id = zipfian.next(&mut rng);
            let key = generate_key(key_id);
            let _ = black_box(engine.get(&cf, &key));
        } else {
            // Insert new record
            let key = generate_key(insert_key_id);
            let value = generate_value(insert_key_id, rng.random());
            engine.put(&cf, &key, &value).unwrap();
            insert_key_id += 1;
        }
    }
}

fn bench_workload_d(c: &mut Criterion) {
    let mut group = c.benchmark_group("ycsb_workload_d");
    group.throughput(Throughput::Elements(OPS_PER_ITER as u64));

    // Non-concurrent scenarios
    let scenarios = ["fs_nosync", "fs_sync", "cloud_nosync", "cloud_sync"];

    for &scenario in &scenarios {
        match scenario {
            "fs_nosync" => {
                let (engine, _temp_dir) = setup_engine_fs_nosync();
                load_data(&engine, RECORD_COUNT);
                group.bench_with_input(BenchmarkId::new("95r_5i", scenario), &engine, |b, e| {
                    b.iter(|| run_workload_d(e, OPS_PER_ITER, RECORD_COUNT))
                });
            }
            "fs_sync" => {
                let (engine, _temp_dir) = setup_engine_fs_sync();
                load_data(&engine, RECORD_COUNT);
                group.bench_with_input(BenchmarkId::new("95r_5i", scenario), &engine, |b, e| {
                    b.iter(|| run_workload_d(e, OPS_PER_ITER, RECORD_COUNT))
                });
            }
            "cloud_nosync" => {
                let (engine, _backend) = setup_engine_cloud_nosync();
                load_data(&engine, RECORD_COUNT);
                group.bench_with_input(BenchmarkId::new("95r_5i", scenario), &engine, |b, e| {
                    b.iter(|| run_workload_d(e, OPS_PER_ITER, RECORD_COUNT))
                });
            }
            "cloud_sync" => {
                let (engine, _backend) = setup_engine_cloud_sync();
                load_data(&engine, RECORD_COUNT);
                group.bench_with_input(BenchmarkId::new("95r_5i", scenario), &engine, |b, e| {
                    b.iter(|| run_workload_d(e, OPS_PER_ITER, RECORD_COUNT))
                });
            }
            _ => unreachable!(),
        }
    }

    // Concurrent scenarios (different thread counts) - centralized in `ycsb_common`
    for &threads in &THREAD_COUNTS {
        let total_ops = OPS_PER_ITER;
        let ops_per_thread = total_ops / threads;

        // For concurrent tests we use the fs_nosync setup, matching prior behavior
        let (engine, _temp_dir) = setup_engine_fs_nosync();
        load_data(&engine, RECORD_COUNT);
        let engine = Arc::new(engine);

        group.bench_with_input(
            BenchmarkId::new("concurrent_95r_5i", threads),
            &threads,
            |b, &_threads| {
                b.iter(|| {
                    let cf = engine.default_column_family();
                    let handles: Vec<_> = (0..threads)
                        .map(|thread_id| {
                            let engine = Arc::clone(&engine);
                            let cf = cf.clone();
                            thread::spawn(move || {
                                let mut rng = StdRng::seed_from_u64(12345 + thread_id as u64);
                                let zipfian = ZipfianGenerator::new(RECORD_COUNT, 0.99);
                                // Each thread uses an insert key offset to avoid collisions
                                let mut insert_key_id = RECORD_COUNT + thread_id * ops_per_thread;

                                for _ in 0..ops_per_thread {
                                    if rng.random_bool(0.95) {
                                        // Read latest - skewed towards recently inserted keys
                                        let key_id = zipfian.next(&mut rng);
                                        let key = generate_key(key_id);
                                        let _ = black_box(engine.get(&cf, &key));
                                    } else {
                                        // Insert new record
                                        let key = generate_key(insert_key_id);
                                        let value = generate_value(insert_key_id, rng.random());
                                        engine.put(&cf, &key, &value).unwrap();
                                        insert_key_id += 1;
                                    }
                                }
                            })
                        })
                        .collect();

                    for handle in handles {
                        handle.join().unwrap();
                    }
                })
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = ycsb_workload_d;
    config = criterion_config();
    targets = bench_workload_d
}
criterion_main!(ycsb_workload_d);
