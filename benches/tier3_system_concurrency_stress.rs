//! Tier 3 — Concurrency Stress & Compaction Benchmarks
//!
//! **Target Runtime:** ~10 seconds per benchmark
//! **Run Frequency:** Nightly CI / Perf Baselines
//!
//! Focus areas:
//! - Concurrent writer scaling (1-16 threads)
//! - Read/write contention patterns
//! - Compaction interference under sustained load
//! - Delete-heavy concurrent operations
//! - Column family scalability under concurrent access
//!
//! ## Design Notes
//!
//! - Returns engine from timed closures to exclude teardown from timing
//! - Precomputes all keys/values outside hot loops
//! - Uses unique paths to avoid cross-iteration interference
//! - Throughput measured in bytes where applicable
//! - Tests all storage modes: Memory, LocalDisk, and CloudBacked

#[path = "./criterion_helper.rs"]
mod criterion_helper;

#[path = "./tier3_system_bench_common.rs"]
mod bench_common;

use bench_common::{
    make_key, make_value_fixed, precompute_kv, setup_engine, setup_engine_arc, BenchEngineConfig,
    BenchStorageMode, ALL_STORAGE_MODES, DURABLE_STORAGE_MODES, KEY_SIZE,
};

use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::sync::Arc;
use std::thread;

// ============================================================================
// Concurrent Writer Scaling
// ============================================================================

fn bench_concurrent_puts(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_concurrent_puts");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(15);

    let max_threads = 16;
    let n_ops = 3_000; // Reduced from 5000 for faster runs
    let value_size = 128;
    let total_ops = max_threads * n_ops;
    let (keys, vals) = precompute_kv(total_ops, value_size);
    let keys = Arc::new(keys);
    let vals = Arc::new(vals);

    for &threads in &[1, 2, 4, 8, 16] {
        let ops_per_iter = threads * n_ops;
        let bytes_per_iter = ops_per_iter * (KEY_SIZE + value_size);
        group.throughput(Throughput::Bytes(bytes_per_iter as u64));

        for mode in ALL_STORAGE_MODES {
            let bench_name = format!("{}threads/{}", threads, mode.as_str());
            group.bench_with_input(
                BenchmarkId::new("scaling", &bench_name),
                &(threads, mode),
                |b, &(tcount, mode)| {
                    let keys = Arc::clone(&keys);
                    let vals = Arc::clone(&vals);
                    b.iter_batched(
                        || setup_engine_arc("concurrent", mode),
                        |engine| {
                            let cf = engine.default_column_family();
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
                            engine // prevent Drop during timing
                        },
                        BatchSize::SmallInput,
                    )
                },
            );
        }
    }

    group.finish();
}

// ============================================================================
// Read/Write Contention
// ============================================================================

fn bench_mixed_read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_read_write_contention");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(15);

    // Precompute data to avoid allocations in hot path
    let prefill_keys: Vec<_> = (0..5_000).map(make_key).collect(); // Reduced from 10000
    let prefill_vals: Vec<_> = (0..5_000).map(|_| make_value_fixed(64)).collect();
    let writer_keys: Vec<_> = (0..2_000).map(|i| make_key(i + 20_000)).collect(); // Reduced from 4000
    let writer_vals: Vec<_> = (0..2_000).map(|_| make_value_fixed(128)).collect();
    let writer_keys = Arc::new(writer_keys);
    let writer_vals = Arc::new(writer_vals);
    let reader_keys: Vec<_> = (0..5_000).step_by(3).map(make_key).collect();
    let reader_keys = Arc::new(reader_keys);

    // Calculate throughput: 4 writers * 500 ops + 4 readers * ~1666 ops
    let total_ops = 4 * 500 + 4 * reader_keys.len();
    group.throughput(Throughput::Elements(total_ops as u64));

    for mode in ALL_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("4w4r_threads", mode.as_str()),
            &mode,
            |b, &mode| {
                let writer_keys = Arc::clone(&writer_keys);
                let writer_vals = Arc::clone(&writer_vals);
                let reader_keys = Arc::clone(&reader_keys);
                let prefill_keys_ref = &prefill_keys;
                let prefill_vals_ref = &prefill_vals;

                b.iter_batched(
                    || {
                        let engine = setup_engine_arc("mixed", mode);
                        let cf = engine.default_column_family();
                        // Prefill in setup
                        for i in 0..5_000 {
                            engine
                                .put(cf, &prefill_keys_ref[i], &prefill_vals_ref[i])
                                .expect("prefill failed");
                        }
                        engine
                    },
                    |engine| {
                        let cf = engine.default_column_family();
                        let writer_keys = Arc::clone(&writer_keys);
                        let writer_vals = Arc::clone(&writer_vals);
                        let reader_keys = Arc::clone(&reader_keys);

                        thread::scope(|scope| {
                            // Writers
                            for t in 0..4 {
                                let e = Arc::clone(&engine);
                                let cf = cf.clone();
                                let writer_keys = Arc::clone(&writer_keys);
                                let writer_vals = Arc::clone(&writer_vals);
                                scope.spawn(move || {
                                    for i in 0..500 {
                                        let idx = t * 500 + i;
                                        e.put(&cf, &writer_keys[idx], &writer_vals[idx])
                                            .expect("write failed");
                                    }
                                });
                            }
                            // Readers
                            for _ in 0..4 {
                                let e = Arc::clone(&engine);
                                let cf = cf.clone();
                                let reader_keys = Arc::clone(&reader_keys);
                                scope.spawn(move || {
                                    for j in 0..reader_keys.len() {
                                        let _ = e.get(&cf, &reader_keys[j]);
                                    }
                                });
                            }
                        });
                        engine // prevent Drop during timing
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

// ============================================================================
// Compaction Stress
// ============================================================================

fn bench_compaction_pressure(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_compaction_pressure");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);

    // Precompute data - reduced from 25k for faster runs
    let compaction_keys: Vec<_> = (0..15_000).map(make_key).collect();
    let compaction_vals: Vec<_> = (0..15_000).map(|_| make_value_fixed(256)).collect();
    let verify_keys: Vec<_> = (0..1_000).step_by(50).map(make_key).collect();

    let total_bytes = 15_000 * (KEY_SIZE + 256);
    group.throughput(Throughput::Bytes(total_bytes as u64));

    // Heavy scenario: LocalDisk only to avoid cloud latency overhead
    for mode in DURABLE_STORAGE_MODES {
        if !matches!(mode, BenchStorageMode::LocalDisk) {
            continue;
        }
        group.bench_with_input(
            BenchmarkId::new("steady_write_with_compaction", mode.as_str()),
            &mode,
            |b, &mode| {
                let keys_ref = &compaction_keys;
                let vals_ref = &compaction_vals;
                let verify_ref = &verify_keys;

                b.iter_batched(
                    || {
                        setup_engine(
                            "compacting",
                            &BenchEngineConfig {
                                storage_mode: mode,
                                enable_compaction: true,
                                ..Default::default()
                            },
                        )
                    },
                    |engine| {
                        let cf = engine.default_column_family();
                        for round in 0..5 {
                            for i in 0..3_000 {
                                let idx = round * 3_000 + i;
                                engine
                                    .put(cf, &keys_ref[idx], &vals_ref[idx])
                                    .expect("write failed");
                            }
                            // Small yield to allow compaction progress
                            std::thread::yield_now();
                        }
                        // Verify a few reads during/after compaction
                        for key in verify_ref {
                            let _ = engine.get(cf, key);
                        }
                        engine // prevent Drop during timing
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

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
    let prefill_vals: Vec<_> = (0..10_000).map(|_| make_value_fixed(100)).collect();
    let delete_keys: Vec<_> = (0..10_000).map(make_key).collect();
    let delete_keys = Arc::new(delete_keys);

    for &threads in &[2, 4, 8] {
        group.throughput(Throughput::Elements(10_000));

        for mode in ALL_STORAGE_MODES {
            let bench_name = format!("{}threads/{}", threads, mode.as_str());
            group.bench_with_input(
                BenchmarkId::new("delete_scaling", &bench_name),
                &(threads, mode),
                |b, &(tcount, mode)| {
                    let delete_keys = Arc::clone(&delete_keys);
                    let prefill_keys_ref = &prefill_keys;
                    let prefill_vals_ref = &prefill_vals;

                    b.iter_batched(
                        || {
                            let engine = setup_engine_arc("delete_concurrent", mode);
                            let cf = engine.default_column_family();
                            // Prefill with 10k keys
                            for i in 0..10_000 {
                                engine
                                    .put(cf, &prefill_keys_ref[i], &prefill_vals_ref[i])
                                    .expect("prefill failed");
                            }
                            engine
                        },
                        |engine| {
                            let cf = engine.default_column_family();
                            let delete_keys = Arc::clone(&delete_keys);
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
                            engine // prevent Drop during timing
                        },
                        BatchSize::SmallInput,
                    )
                },
            );
        }
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
    let value_size = 150;
    let multi_cf_keys: Vec<_> = (0..40_000).map(make_key).collect();
    let multi_cf_vals: Vec<_> = (0..40_000).map(|_| make_value_fixed(value_size)).collect();
    let multi_cf_keys = Arc::new(multi_cf_keys);
    let multi_cf_vals = Arc::new(multi_cf_vals);

    for &thread_pairs in &[2, 4, 8] {
        let ops_per_iter = thread_pairs * 2 * 2_500; // pairs * threads_per_cf * ops_per_thread
        let bytes_per_iter = ops_per_iter * (KEY_SIZE + value_size);
        group.throughput(Throughput::Bytes(bytes_per_iter as u64));

        for mode in ALL_STORAGE_MODES {
            let bench_name = format!("{}cfs/{}", thread_pairs, mode.as_str());
            group.bench_with_input(
                BenchmarkId::new("cf_scaling", &bench_name),
                &(thread_pairs, mode),
                |b, &(pairs, mode)| {
                    let multi_cf_keys = Arc::clone(&multi_cf_keys);
                    let multi_cf_vals = Arc::clone(&multi_cf_vals);
                    b.iter_batched(
                        || {
                            let engine = setup_engine_arc("multi_cf", mode);
                            // Create N column families
                            for i in 1..pairs {
                                engine
                                    .create_column_family(&format!("cf{}", i))
                                    .ok();
                            }
                            engine
                        },
                        |engine| {
                            let cf_list = engine.list_column_families().unwrap_or_default();
                            let multi_cf_keys = Arc::clone(&multi_cf_keys);
                            let multi_cf_vals = Arc::clone(&multi_cf_vals);
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
                                                    .expect("write failed");
                                            }
                                        });
                                    }
                                }
                            });
                            engine // prevent Drop during timing
                        },
                        BatchSize::SmallInput,
                    )
                },
            );
        }
    }

    group.finish();
}

criterion_group! {
    name = tier3_system_concurrency_stress;
    config = criterion_config_for_tier(BenchTier::Tier3System);
    targets =
        bench_concurrent_puts,
        bench_mixed_read_write,
        bench_compaction_pressure,
        bench_concurrent_deletes,
        bench_concurrent_multi_cf
}
criterion_main!(tier3_system_concurrency_stress);
