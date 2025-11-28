//! Tier 3 — Contention-heavy benchmark
//!
//! **Target Runtime:** ~2 minutes
//! **Run Frequency:** Nightly / release builds
//!
//! Covers heavy contention scenarios with multiple threads

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::criterion_config;
use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::TempDir;

fn setup_db(name: &str) -> (MidgeEngine, TempDir) {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join(name);
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: path },
        memtable_size: 8 * 1024 * 1024,
        enable_compaction: false,
        ..Default::default()
    };
    (MidgeEngine::open(opts).unwrap(), tmp)
}

/// Benchmark heavy write contention (16 threads, 1000 ops each)
fn bench_engine_heavy_write_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_engine_heavy_write_contention");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(16_000));
    group.measurement_time(std::time::Duration::from_secs(3));
    group.sample_size(10);

    group.bench_function("write_16_threads", |b| {
        b.iter(|| {
            let (engine, _tmp) = setup_db("write_contention");
            let engine = Arc::new(engine);
            let cf = engine.default_column_family();
            let barrier = Arc::new(Barrier::new(17)); // 16 workers + 1 main

            let mut handles = vec![];
            for tid in 0..16 {
                let engine_clone = Arc::clone(&engine);
                let cf_clone = cf.clone();
                let barrier_clone = Arc::clone(&barrier);

                handles.push(thread::spawn(move || {
                    barrier_clone.wait(); // Sync start
                    for i in 0..1000 {
                        let key = format!("t{:02}_key_{:06}", tid, i);
                        let val = format!("value_{}", i);
                        engine_clone
                            .put(&cf_clone, key.as_bytes(), val.as_bytes())
                            .unwrap();
                    }
                }));
            }

            barrier.wait(); // Start all threads
            for h in handles {
                h.join().unwrap();
            }
            black_box(engine);
        })
    });

    group.finish();
}

/// Benchmark heavy read contention (16 threads reading same keys)
fn bench_engine_heavy_read_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_engine_heavy_read_contention");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(32_000));
    group.measurement_time(std::time::Duration::from_secs(3));
    group.sample_size(10);

    group.bench_function("read_16_threads", |b| {
        b.iter(|| {
            let (engine, _tmp) = setup_db("read_contention");
            let cf = engine.default_column_family();

            // Pre-populate with data
            for i in 0..2000 {
                let key = format!("key_{:06}", i);
                let val = format!("value_{}", i);
                engine.put(&cf, key.as_bytes(), val.as_bytes()).unwrap();
            }
            engine.flush().unwrap();

            let engine = Arc::new(engine);
            let barrier = Arc::new(Barrier::new(17));

            let mut handles = vec![];
            for _tid in 0..16 {
                let engine_clone = Arc::clone(&engine);
                let cf_clone = cf.clone();
                let barrier_clone = Arc::clone(&barrier);

                handles.push(thread::spawn(move || {
                    barrier_clone.wait();
                    for i in 0..2000 {
                        let key = format!("key_{:06}", i);
                        let _ = engine_clone.get(&cf_clone, key.as_bytes());
                    }
                }));
            }

            barrier.wait();
            for h in handles {
                h.join().unwrap();
            }
            black_box(engine);
        })
    });

    group.finish();
}

/// Benchmark mixed read/write contention
fn bench_engine_mixed_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_engine_mixed_contention");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(24_000));
    group.measurement_time(std::time::Duration::from_secs(3));
    group.sample_size(10);

    group.bench_function("mixed_16_threads", |b| {
        b.iter(|| {
            let (engine, _tmp) = setup_db("mixed_contention");
            let cf = engine.default_column_family();

            // Pre-populate
            for i in 0..1500 {
                let key = format!("key_{:06}", i);
                engine.put(&cf, key.as_bytes(), b"init_value").unwrap();
            }

            let engine = Arc::new(engine);
            let barrier = Arc::new(Barrier::new(17));

            let mut handles = vec![];
            for tid in 0..16 {
                let engine_clone = Arc::clone(&engine);
                let cf_clone = cf.clone();
                let barrier_clone = Arc::clone(&barrier);

                handles.push(thread::spawn(move || {
                    barrier_clone.wait();
                    for i in 0..1500 {
                        if (tid + i) % 2 == 0 {
                            // Write
                            let key = format!("key_{:06}", i);
                            let val = format!("t{}_v{}", tid, i);
                            engine_clone
                                .put(&cf_clone, key.as_bytes(), val.as_bytes())
                                .unwrap();
                        } else {
                            // Read
                            let key = format!("key_{:06}", i);
                            let _ = engine_clone.get(&cf_clone, key.as_bytes());
                        }
                    }
                }));
            }

            barrier.wait();
            for h in handles {
                h.join().unwrap();
            }
            black_box(engine);
        })
    });

    group.finish();
}

criterion_group! {
    name = tier3_system_contention_heavy;
    config = criterion_config();
    targets = bench_engine_heavy_write_contention, bench_engine_heavy_read_contention, bench_engine_mixed_contention
}
criterion_main!(tier3_system_contention_heavy);
