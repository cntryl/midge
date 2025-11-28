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

/// Pre-generate keys with format "key_{:06}"
fn make_key(i: usize) -> Vec<u8> {
    let mut key = vec![0u8; 10];
    key[..4].copy_from_slice(b"key_");
    let mut n = i;
    for j in (4..10).rev() {
        key[j] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    key
}

/// Pre-generate keys with format "t{:02}_key_{:06}"
fn make_thread_key(tid: usize, i: usize) -> Vec<u8> {
    let mut key = vec![0u8; 15];
    key[0] = b't';
    key[1] = b'0' + (tid / 10) as u8;
    key[2] = b'0' + (tid % 10) as u8;
    key[3] = b'_';
    key[4..8].copy_from_slice(b"key_");
    let mut n = i;
    for j in (8..14).rev() {
        key[j] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    key[14] = 0; // null terminator not needed but keeping length consistent
    key.truncate(14);
    key
}

/// Pre-generate value with format "value_{}"
fn make_value(i: usize) -> Vec<u8> {
    let mut val = Vec::with_capacity(16);
    val.extend_from_slice(b"value_");
    // Append i as decimal string
    if i == 0 {
        val.push(b'0');
    } else {
        let mut n = i;
        let start = val.len();
        while n > 0 {
            val.push(b'0' + (n % 10) as u8);
            n /= 10;
        }
        val[start..].reverse();
    }
    val
}

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

    // Pre-compute all keys and values outside the benchmark loop
    // 16 threads * 1000 ops = 16000 key/value pairs
    let keys: Vec<Vec<Vec<u8>>> = (0..16)
        .map(|tid| (0..1000).map(|i| make_thread_key(tid, i)).collect())
        .collect();
    let values: Vec<Vec<u8>> = (0..1000).map(make_value).collect();
    let keys = Arc::new(keys);
    let values = Arc::new(values);

    group.bench_function("write_16_threads", |b| {
        let keys = Arc::clone(&keys);
        let values = Arc::clone(&values);
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
                let keys = Arc::clone(&keys);
                let values = Arc::clone(&values);

                handles.push(thread::spawn(move || {
                    barrier_clone.wait(); // Sync start
                    for i in 0..1000 {
                        engine_clone
                            .put(&cf_clone, &keys[tid][i], &values[i])
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

    // Pre-compute keys and values outside the benchmark loop
    let keys: Vec<Vec<u8>> = (0..2000).map(make_key).collect();
    let values: Vec<Vec<u8>> = (0..2000).map(make_value).collect();
    let keys = Arc::new(keys);
    let values = Arc::new(values);

    group.bench_function("read_16_threads", |b| {
        let keys = Arc::clone(&keys);
        let values = Arc::clone(&values);
        b.iter(|| {
            let (engine, _tmp) = setup_db("read_contention");
            let cf = engine.default_column_family();

            // Pre-populate with data (using precomputed keys/values)
            for i in 0..2000 {
                engine.put(&cf, &keys[i], &values[i]).unwrap();
            }
            engine.flush().unwrap();

            let engine = Arc::new(engine);
            let barrier = Arc::new(Barrier::new(17));

            let mut handles = vec![];
            for _tid in 0..16 {
                let engine_clone = Arc::clone(&engine);
                let cf_clone = cf.clone();
                let barrier_clone = Arc::clone(&barrier);
                let keys = Arc::clone(&keys);

                handles.push(thread::spawn(move || {
                    barrier_clone.wait();
                    for i in 0..2000 {
                        let _ = engine_clone.get(&cf_clone, &keys[i]);
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

    // Pre-compute keys and values outside the benchmark loop
    let keys: Vec<Vec<u8>> = (0..1500).map(make_key).collect();
    // Pre-compute thread-specific values "t{}_v{}"
    let thread_values: Vec<Vec<Vec<u8>>> = (0..16)
        .map(|tid| {
            (0..1500)
                .map(|i| {
                    let mut val = Vec::with_capacity(16);
                    val.push(b't');
                    if tid == 0 {
                        val.push(b'0');
                    } else {
                        let mut n = tid;
                        let start = val.len();
                        while n > 0 {
                            val.push(b'0' + (n % 10) as u8);
                            n /= 10;
                        }
                        val[start..].reverse();
                    }
                    val.extend_from_slice(b"_v");
                    if i == 0 {
                        val.push(b'0');
                    } else {
                        let mut n = i;
                        let start = val.len();
                        while n > 0 {
                            val.push(b'0' + (n % 10) as u8);
                            n /= 10;
                        }
                        val[start..].reverse();
                    }
                    val
                })
                .collect()
        })
        .collect();
    let keys = Arc::new(keys);
    let thread_values = Arc::new(thread_values);

    group.bench_function("mixed_16_threads", |b| {
        let keys = Arc::clone(&keys);
        let thread_values = Arc::clone(&thread_values);
        b.iter(|| {
            let (engine, _tmp) = setup_db("mixed_contention");
            let cf = engine.default_column_family();

            // Pre-populate (using precomputed keys)
            for i in 0..1500 {
                engine.put(&cf, &keys[i], b"init_value").unwrap();
            }

            let engine = Arc::new(engine);
            let barrier = Arc::new(Barrier::new(17));

            let mut handles = vec![];
            for tid in 0..16 {
                let engine_clone = Arc::clone(&engine);
                let cf_clone = cf.clone();
                let barrier_clone = Arc::clone(&barrier);
                let keys = Arc::clone(&keys);
                let thread_values = Arc::clone(&thread_values);

                handles.push(thread::spawn(move || {
                    barrier_clone.wait();
                    for i in 0..1500 {
                        if (tid + i) % 2 == 0 {
                            // Write
                            engine_clone
                                .put(&cf_clone, &keys[i], &thread_values[tid][i])
                                .unwrap();
                        } else {
                            // Read
                            let _ = engine_clone.get(&cf_clone, &keys[i]);
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
