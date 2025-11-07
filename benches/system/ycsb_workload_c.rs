//! YCSB Workload C: 100% Read (Read-Only) - FS NoSync

#[path = "../criterion_helper.rs"]
mod criterion_helper;
#[path = "ycsb_common.rs"]
mod ycsb_common;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use criterion_helper::criterion_config;

use midge::MidgeEngine;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::hint::black_box;
use std::sync::Arc;
use std::thread;
use ycsb_common::*;

fn run_workload_c(engine: &MidgeEngine, operations: usize, record_count: usize) {
    let mut rng = StdRng::seed_from_u64(12345);
    let zipfian = ZipfianGenerator::new(record_count, 0.99);

    for _ in 0..operations {
        let key_id = zipfian.next(&mut rng);
        let key = generate_key(key_id);
        let _ = black_box(engine.get(&key));
    }
}

fn bench_workload_c(c: &mut Criterion) {
    let mut group = c.benchmark_group("ycsb_workload_c");
    group.throughput(Throughput::Elements(OPS_PER_ITER as u64));

    // For this read-only benchmark we only exercise two scenarios:
    // - fs_nosync (local disk, no WAL sync)
    // - cloud_nosync (cloud-backed, local cache, no WAL sync)
    let scenarios = ["fs_nosync", "cloud_nosync"];

    for &scenario in &scenarios {
        match scenario {
            "fs_nosync" => {
                let (engine, _temp_dir) = setup_engine_fs_nosync();
                load_data(&engine, RECORD_COUNT);
                group.bench_with_input(BenchmarkId::new("100r", scenario), &engine, |b, e| {
                    b.iter(|| run_workload_c(e, OPS_PER_ITER, RECORD_COUNT))
                });
            }
            "cloud_nosync" => {
                let (engine, _backend) = setup_engine_cloud_nosync();
                load_data(&engine, RECORD_COUNT);
                group.bench_with_input(BenchmarkId::new("100r", scenario), &engine, |b, e| {
                    b.iter(|| run_workload_c(e, OPS_PER_ITER, RECORD_COUNT))
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
            BenchmarkId::new("concurrent_100r", threads),
            &threads,
            |b, &_threads| {
                b.iter(|| {
                    let handles: Vec<_> = (0..threads)
                        .map(|thread_id| {
                            let engine = Arc::clone(&engine);
                            thread::spawn(move || {
                                let mut rng = StdRng::seed_from_u64(12345 + thread_id as u64);
                                let zipfian = ZipfianGenerator::new(RECORD_COUNT, 0.99);

                                for _ in 0..ops_per_thread {
                                    let key_id = zipfian.next(&mut rng);
                                    let key = generate_key(key_id);
                                    let _ = black_box(engine.get(&key));
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
    name = ycsb_workload_c;
    config = criterion_config();
    targets = bench_workload_c
}
criterion_main!(ycsb_workload_c);
