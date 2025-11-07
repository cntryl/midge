//! YCSB Workload A: 50% Read / 50% Write (Update-Heavy)

#[path = "../criterion_helper.rs"]
mod criterion_helper;
#[path = "ycsb_common.rs"]
mod ycsb_common;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use criterion_helper::criterion_config;

use midge::{MidgeEngine, WriteBatch};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;
use std::thread;
use ycsb_common::*;

// ============================================================================
// Workload A: 50% Read / 50% Write (Update-Heavy) - consolidated scenarios
// Scenarios supported: fs_nosync, fs_sync, cloud_nosync, cloud_sync, concurrent
// ============================================================================

fn run_workload_a(engine: &MidgeEngine, operations: usize, record_count: usize) {
    let mut rng = StdRng::seed_from_u64(12345);
    let zipfian = ZipfianGenerator::new(record_count, 0.99);
    let mut batch = WriteBatch::new();

    for _ in 0..operations {
        let key_id = zipfian.next(&mut rng);
        let key = generate_key(key_id);

        if rng.random_bool(0.5) {
            // Read operation
            let _ = black_box(engine.get(&key));
        } else {
            // Write operation - add to batch
            let value = generate_value(key_id, rng.random());
            batch.put(key, value);

            // Flush batch every BATCH_SIZE writes for realistic throughput
            if batch.len() >= BATCH_SIZE {
                engine.write_batch(&batch).unwrap();
                batch.clear();
            }
        }
    }

    // Flush any remaining writes
    if !batch.is_empty() {
        engine.write_batch(&batch).unwrap();
    }
}

fn run_workload_a_concurrent(
    engine: Arc<MidgeEngine>,
    operations_per_thread: usize,
    record_count: usize,
    thread_id: usize,
) {
    let mut rng = StdRng::seed_from_u64(12345 + thread_id as u64);
    let zipfian = ZipfianGenerator::new(record_count, 0.99);
    let mut batch = WriteBatch::new();

    for _ in 0..operations_per_thread {
        let key_id = zipfian.next(&mut rng);
        let key = generate_key(key_id);

        if rng.random_bool(0.5) {
            // Read operation
            let _ = black_box(engine.get(&key));
        } else {
            // Write operation - add to batch
            let value = generate_value(key_id, rng.random());
            batch.put(key, value);

            // Flush batch every BATCH_SIZE writes for realistic throughput
            if batch.len() >= BATCH_SIZE {
                engine.write_batch(&batch).unwrap();
                batch.clear();
            }
        }
    }

    // Flush any remaining writes
    if !batch.is_empty() {
        engine.write_batch(&batch).unwrap();
    }
}

fn bench_workload_a(c: &mut Criterion) {
    // Single Criterion group used for all scenarios; throughput is OPS_PER_ITER
    let mut group = c.benchmark_group("ycsb_workload_a");
    group.throughput(Throughput::Elements(OPS_PER_ITER as u64));

    // Non-concurrent scenarios
    let scenarios = ["fs_nosync", "fs_sync", "cloud_nosync", "cloud_sync"];

    for &scenario in &scenarios {
        match scenario {
            "fs_nosync" => {
                let (engine, _temp_dir) = setup_engine_fs_nosync();
                load_data(&engine, RECORD_COUNT);
                group.bench_with_input(BenchmarkId::new("50r_50w", scenario), &engine, |b, e| {
                    b.iter(|| run_workload_a(e, OPS_PER_ITER, RECORD_COUNT))
                });
            }
            "fs_sync" => {
                let (engine, _temp_dir) = setup_engine_fs_sync();
                load_data(&engine, RECORD_COUNT);
                group.bench_with_input(BenchmarkId::new("50r_50w", scenario), &engine, |b, e| {
                    b.iter(|| run_workload_a(e, OPS_PER_ITER, RECORD_COUNT))
                });
            }
            "cloud_nosync" => {
                let (engine, _backend) = setup_engine_cloud_nosync();
                load_data(&engine, RECORD_COUNT);
                group.bench_with_input(BenchmarkId::new("50r_50w", scenario), &engine, |b, e| {
                    b.iter(|| run_workload_a(e, OPS_PER_ITER, RECORD_COUNT))
                });
            }
            "cloud_sync" => {
                let (engine, _backend) = setup_engine_cloud_sync();
                load_data(&engine, RECORD_COUNT);
                group.bench_with_input(BenchmarkId::new("50r_50w", scenario), &engine, |b, e| {
                    b.iter(|| run_workload_a(e, OPS_PER_ITER, RECORD_COUNT))
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
            BenchmarkId::new("concurrent_50r_50w", threads),
            &threads,
            |b, &_threads| {
                b.iter(|| {
                    let handles: Vec<_> = (0..threads)
                        .map(|thread_id| {
                            let engine = Arc::clone(&engine);
                            thread::spawn(move || {
                                run_workload_a_concurrent(
                                    engine,
                                    ops_per_thread,
                                    RECORD_COUNT,
                                    thread_id,
                                )
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
    name = ycsb_workload_a;
    config = criterion_config();
    targets = bench_workload_a
}
criterion_main!(ycsb_workload_a);
