//! Tier 3 — Concurrency Stress & Compaction Benchmarks
//!
//! **Target Runtime:** ~10 seconds
//! **Run Frequency:** Nightly CI / Perf Baselines
//!
//! Focus areas:
//! - Concurrent writer scaling (1-16 threads)
//! - Read/write contention patterns
//! - Compaction interference under sustained load
//! - Delete-heavy concurrent operations
//! - Column family scalability under concurrent access

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::criterion_config;
use std::hint::black_box;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

fn make_key(i: usize) -> Bytes {
    // Fixed-size key using direct byte manipulation (no format! allocations)
    let mut key = vec![0u8; 14];
    key[..4].copy_from_slice(b"key_");
    // Write i as 10-digit decimal directly
    let mut n = i;
    for j in (4..14).rev() {
        key[j] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    Bytes::from(key)
}
fn make_value(size: usize) -> Bytes {
    Bytes::from(vec![b'x'; size])
}

fn precompute_kv(n: usize, value_size: usize) -> (Vec<Bytes>, Vec<Bytes>) {
    let mut keys = Vec::with_capacity(n);
    let mut vals = Vec::with_capacity(n);
    for i in 0..n {
        keys.push(make_key(i));
        vals.push(make_value(value_size));
    }
    (keys, vals)
}

fn setup_db(name: &str, compaction: bool) -> MidgeEngine {
    let path = std::env::temp_dir().join(format!("midge_bench_t3_stress_{}", name));
    let _ = std::fs::remove_dir_all(&path);
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: path },
        memtable_size: 4 * 1024 * 1024,
        enable_compaction: compaction,
        wal_sync: true,
        ..Default::default()
    };
    MidgeEngine::open(opts).unwrap()
}

// ============================================================================
// Concurrent Writer Scaling
// ============================================================================

fn bench_concurrent_puts(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_concurrent_puts");
    group.sampling_mode(SamplingMode::Flat);

    let max_threads = 16;
    let n_ops = 5_000;
    let total_ops = max_threads * n_ops;
    let (keys, vals) = precompute_kv(total_ops, 128);
    let keys = Arc::new(keys);
    let vals = Arc::new(vals);

    for &threads in &[1, 2, 4, 8, 16] {
        let ops_per_iter = threads * n_ops;
        group.throughput(Throughput::Elements(ops_per_iter as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |b, &tcount| {
                b.iter_batched(
                    || Arc::new(setup_db(&format!("concurrent_{}", tcount), false)),
                    |engine| {
                        let cf = engine.default_column_family();
                        let keys = Arc::clone(&keys);
                        let vals = Arc::clone(&vals);
                        let start = Instant::now();
                        thread::scope(|scope| {
                            for tid in 0..tcount {
                                let engine = Arc::clone(&engine);
                                let cf = cf.clone();
                                let keys = Arc::clone(&keys);
                                let vals = Arc::clone(&vals);
                                scope.spawn(move || {
                                    let offset = tid * n_ops;
                                    for i in 0..n_ops {
                                        let idx = offset + i;
                                        engine
                                            .put(&cf, &keys[idx], &vals[idx])
                                            .expect("put failed");
                                    }
                                });
                            }
                        });
                        black_box(start.elapsed());
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

// ============================================================================
// Read/Write Contention
// ============================================================================

fn bench_mixed_read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_read_write_contention");
    group.sampling_mode(SamplingMode::Flat);

    // Precompute data to avoid allocations in hot path
    let prefill_keys: Vec<_> = (0..10_000).map(make_key).collect();
    let prefill_vals: Vec<_> = (0..10_000).map(|_| make_value(64)).collect();
    let writer_keys: Vec<_> = (0..4_000).map(|i| make_key(i + 20_000)).collect();
    let writer_vals: Vec<_> = (0..4_000).map(|_| make_value(128)).collect();
    let writer_keys = Arc::new(writer_keys);
    let writer_vals = Arc::new(writer_vals);
    let reader_keys: Vec<_> = (0..10_000).step_by(3).map(make_key).collect();
    let reader_keys = Arc::new(reader_keys);

    group.bench_function("4w4r_threads", |b| {
        b.iter_batched(
            || Arc::new(setup_db("mixed", false)),
            |engine| {
                let cf = engine.default_column_family();
                let writer_keys = Arc::clone(&writer_keys);
                let writer_vals = Arc::clone(&writer_vals);
                let reader_keys = Arc::clone(&reader_keys);
                // prefill
                for i in 0..10_000 {
                    engine.put(&cf, &prefill_keys[i], &prefill_vals[i]).unwrap();
                }

                let engine_r = Arc::clone(&engine);
                thread::scope(|scope| {
                    // writers
                    for t in 0..4 {
                        let e = Arc::clone(&engine);
                        let cf = cf.clone();
                        let writer_keys = Arc::clone(&writer_keys);
                        let writer_vals = Arc::clone(&writer_vals);
                        scope.spawn(move || {
                            for i in 0..1_000 {
                                let idx = t * 1_000 + i;
                                e.put(&cf, &writer_keys[idx], &writer_vals[idx]).unwrap();
                            }
                        });
                    }
                    // readers
                    for _ in 0..4 {
                        let e = Arc::clone(&engine_r);
                        let cf = cf.clone();
                        let reader_keys = Arc::clone(&reader_keys);
                        scope.spawn(move || {
                            for j in 0..reader_keys.len() {
                                let _ = e.get(&cf, &reader_keys[j]).unwrap();
                            }
                        });
                    }
                });
                black_box(());
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

// ============================================================================
// Compaction Stress
// ============================================================================

fn bench_compaction_pressure(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_compaction_pressure");
    group.sampling_mode(SamplingMode::Flat);

    // Precompute data
    let compaction_keys: Vec<_> = (0..25_000).map(make_key).collect();
    let compaction_vals: Vec<_> = (0..25_000).map(|_| make_value(256)).collect();
    let verify_keys: Vec<_> = (0..1_000).step_by(50).map(make_key).collect();

    group.bench_function("steady_write_with_compaction", |b| {
        b.iter_batched(
            || setup_db("compacting", true),
            |engine| {
                let cf = engine.default_column_family();
                for round in 0..5 {
                    for i in 0..5_000 {
                        let idx = round * 5_000 + i;
                        engine
                            .put(&cf, &compaction_keys[idx], &compaction_vals[idx])
                            .unwrap();
                    }
                    // brief pause to let background compaction catch up
                    thread::sleep(Duration::from_millis(50));
                }
                // Verify a few reads during/after compaction
                for key in &verify_keys {
                    let _ = engine.get(&cf, key).unwrap();
                }
                black_box(());
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

// ============================================================================
// Delete-Heavy Concurrent Operations
// ============================================================================

fn bench_concurrent_deletes(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_concurrent_deletes");
    group.sampling_mode(SamplingMode::Flat);

    // Precompute data
    let prefill_keys: Vec<_> = (0..10_000).map(make_key).collect();
    let prefill_vals: Vec<_> = (0..10_000).map(|_| make_value(100)).collect();
    let delete_keys: Vec<_> = (0..10_000).map(make_key).collect();
    let delete_keys = Arc::new(delete_keys);

    for &threads in &[2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |b, &tcount| {
                b.iter_batched(
                    || {
                        let engine =
                            Arc::new(setup_db(&format!("delete_concurrent_{}", tcount), false));
                        let cf = engine.default_column_family();
                        // Prefill with 10k keys
                        for i in 0..10_000 {
                            engine.put(&cf, &prefill_keys[i], &prefill_vals[i]).unwrap();
                        }
                        engine
                    },
                    |engine| {
                        let cf = engine.default_column_family();
                        let delete_keys = Arc::clone(&delete_keys);
                        let start = Instant::now();
                        thread::scope(|scope| {
                            for tid in 0..tcount {
                                let engine = Arc::clone(&engine);
                                let cf = cf.clone();
                                let delete_keys = Arc::clone(&delete_keys);
                                scope.spawn(move || {
                                    let offset = tid * (10_000 / tcount);
                                    let count = 10_000 / tcount;
                                    for i in 0..count {
                                        engine.delete(&cf, &delete_keys[offset + i]).ok();
                                    }
                                });
                            }
                        });
                        black_box(start.elapsed());
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

// ============================================================================
// Column Family Concurrent Operations
// ============================================================================

/// Benchmark concurrent writes across multiple column families
fn bench_concurrent_multi_cf(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_concurrent_multi_cf");
    group.sampling_mode(SamplingMode::Flat);

    // Precompute for max pairs=8, 8*2*2500=40000
    let multi_cf_keys: Vec<_> = (0..40_000).map(make_key).collect();
    let multi_cf_vals: Vec<_> = (0..40_000).map(|_| make_value(150)).collect();
    let multi_cf_keys = Arc::new(multi_cf_keys);
    let multi_cf_vals = Arc::new(multi_cf_vals);

    for &thread_pairs in &[2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::from_parameter(thread_pairs),
            &thread_pairs,
            |b, &pairs| {
                b.iter_batched(
                    || {
                        let engine = Arc::new(setup_db(&format!("multi_cf_{}", pairs), false));
                        // Create N column families
                        for i in 1..pairs {
                            engine
                                .create_column_family(&format!("cf{}", i), Default::default())
                                .ok();
                        }
                        engine
                    },
                    |engine| {
                        let cf_list = engine.list_column_families();
                        let multi_cf_keys = Arc::clone(&multi_cf_keys);
                        let multi_cf_vals = Arc::clone(&multi_cf_vals);
                        let start = Instant::now();
                        thread::scope(|scope| {
                            // 2 threads per CF
                            for (cf_idx, cf) in cf_list.iter().enumerate().take(pairs) {
                                for tid in 0..2 {
                                    let engine = Arc::clone(&engine);
                                    let cf = cf.clone();
                                    let multi_cf_keys = Arc::clone(&multi_cf_keys);
                                    let multi_cf_vals = Arc::clone(&multi_cf_vals);
                                    scope.spawn(move || {
                                        let base = cf_idx * 2 * 2_500 + tid * 2_500;
                                        for i in 0..2_500 {
                                            engine
                                                .put(
                                                    &cf,
                                                    &multi_cf_keys[base + i],
                                                    &multi_cf_vals[base + i],
                                                )
                                                .unwrap();
                                        }
                                    });
                                }
                            }
                        });
                        black_box(start.elapsed());
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = tier3_system_concurrency_stress;
    config = criterion_config();
    targets =
        bench_concurrent_puts,
        bench_mixed_read_write,
        bench_compaction_pressure,
        bench_concurrent_deletes,
        bench_concurrent_multi_cf
}
criterion_main!(tier3_system_concurrency_stress);
