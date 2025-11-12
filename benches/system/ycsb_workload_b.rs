//! YCSB Workload B: 95% Read / 5% Write (Read-Heavy)
//!
//! Benchmarks both storage backends (fs/cloud, sync/nosync) and
//! scales by number of column families (1, 2, 4, 8, 16) and threads.

#[path = "../criterion_helper.rs"]
mod criterion_helper;
#[path = "ycsb_common.rs"]
mod ycsb_common;

use cntryl_midge::{MidgeEngine, WriteBatch};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use criterion_helper::criterion_config;
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

fn run_workload_b(engine: &MidgeEngine, operations: usize, record_count: usize, cf_count: usize) {
    let cf_list = engine.list_column_families();
    let mut rng = StdRng::seed_from_u64(12345);
    let zipfian = ZipfianGenerator::new(record_count, 0.99);
    let mut batch = WriteBatch::new();

    for _ in 0..operations {
        let key_id = zipfian.next(&mut rng);
        let key = generate_key(key_id);
        let cf_index = rng.gen_range(0..cf_count);
        let cf = &cf_list[cf_index];
        let cf_id = cf.id();

        if rng.random_bool(0.95) {
            // Read operation
            let _ = black_box(engine.get(&cf, &key));
        } else {
            // Write operation - add to batch
            let value = generate_value(key_id, rng.random());
            batch.put(cf_id, key, value);

            // Flush batch every BATCH_SIZE writes
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

fn run_workload_b_concurrent(
    engine: Arc<MidgeEngine>,
    operations_per_thread: usize,
    record_count: usize,
    thread_id: usize,
    cf_count: usize,
) {
    let cf_list = engine.list_column_families();
    let mut rng = StdRng::seed_from_u64(12345 + thread_id as u64);
    let zipfian = ZipfianGenerator::new(record_count, 0.99);
    let mut batch = WriteBatch::new();

    for _ in 0..operations_per_thread {
        let key_id = zipfian.next(&mut rng);
        let key = generate_key(key_id);
        let cf_index = rng.gen_range(0..cf_count);
        let cf = &cf_list[cf_index];
        let cf_id = cf.id();

        if rng.random_bool(0.95) {
            // Read operation
            let _ = black_box(engine.get(&cf, &key));
        } else {
            // Write operation - add to batch
            let value = generate_value(key_id, rng.random());
            batch.put(cf_id, key, value);

            // Flush batch every BATCH_SIZE writes
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

// ============================================================================
// Benchmark driver
// ============================================================================

fn bench_workload_b(c: &mut Criterion) {
    let mut group = c.benchmark_group("ycsb_workload_b_cf_variants");
    group.throughput(Throughput::Elements(OPS_PER_ITER as u64));

    let scenarios = ["fs_nosync", "fs_sync", "cloud_nosync", "cloud_sync"];

    for &cf_count in CF_COUNTS {
        for &scenario in &scenarios {
            let (engine, _backend) = match scenario {
                "fs_nosync" => {
                    let (e, _t) = setup_engine_fs_nosync();
                    (e, None)
                }
                "fs_sync" => {
                    let (e, _t) = setup_engine_fs_sync();
                    (e, None)
                }
                "cloud_nosync" => {
                    let (e, b) = setup_engine_cloud_nosync();
                    (e, Some(b))
                }
                "cloud_sync" => {
                    let (e, b) = setup_engine_cloud_sync();
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
                BenchmarkId::new(format!("{scenario}_cf{cf_count}"), "95r_5w"),
                &cf_count,
                |b, &_cf_count| {
                    b.iter(|| run_workload_b(&engine, OPS_PER_ITER, RECORD_COUNT, cf_count))
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
                                    run_workload_b_concurrent(
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
    name = ycsb_workload_b;
    config = criterion_config();
    targets = bench_workload_b
}
criterion_main!(ycsb_workload_b);
