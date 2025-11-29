//! Tier 3 — Contention-heavy benchmark
//!
//! **Target Runtime:** ~2 minutes
//! **Run Frequency:** Nightly / release builds
//!
//! Covers heavy contention scenarios with multiple threads competing for
//! concurrent access to the storage engine.
//!
//! ## Benchmarks
//!
//! - `system_engine_heavy_write_contention`: 16 threads writing concurrently
//! - `system_engine_heavy_read_contention`: 16 threads reading same keys
//! - `system_engine_mixed_contention`: Mixed read/write workload
//!
//! ## Design Notes
//!
//! - Returns engine from timed closures to exclude teardown from timing
//! - Precomputes all keys/values outside hot loops
//! - Uses unique paths to avoid cross-iteration interference
//! - Tests all storage modes: Memory, LocalDisk, and CloudBacked

#[path = "../criterion_helper.rs"]
mod criterion_helper;

mod bench_common;

use bench_common::{make_key, make_value_fixed, setup_engine_arc, ALL_STORAGE_MODES, KEY_SIZE};

use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;
use std::sync::Arc;
use std::thread;

/// Key sizes for consistent throughput measurement
const THREAD_KEY_SIZE: usize = 14; // "t00_key_" + 6 digits
const VALUE_SIZE: usize = 64; // Fixed value size for accurate throughput

/// Pre-generate keys with format "t{:02}_key_{:06}"
#[inline]
fn make_thread_key(tid: usize, i: usize) -> Vec<u8> {
    let mut key = vec![0u8; THREAD_KEY_SIZE];
    key[0] = b't';
    key[1] = b'0' + (tid / 10) as u8;
    key[2] = b'0' + (tid % 10) as u8;
    key[3] = b'_';
    key[4..8].copy_from_slice(b"key_");
    let mut n = i;
    for j in (8..THREAD_KEY_SIZE).rev() {
        key[j] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    key
}

/// Pre-generate fixed-size value with pattern
#[inline]
fn make_value_pattern(i: usize) -> Vec<u8> {
    let mut val = vec![0u8; VALUE_SIZE];
    // Store index in first 8 bytes for verification
    if VALUE_SIZE >= 8 {
        val[..8].copy_from_slice(&(i as u64).to_be_bytes());
    }
    // Fill rest with pattern
    let pattern = (i % 256) as u8;
    for byte in val.iter_mut().skip(8) {
        *byte = pattern;
    }
    val
}

/// Benchmark heavy write contention (16 threads, 1000 ops each)
fn bench_engine_heavy_write_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_engine_heavy_write_contention");
    group.sampling_mode(SamplingMode::Flat);

    let num_threads = 16;
    let ops_per_thread = 1_000;
    let total_ops = num_threads * ops_per_thread;
    let total_bytes = total_ops * (THREAD_KEY_SIZE + VALUE_SIZE);
    group.throughput(Throughput::Bytes(total_bytes as u64));
    group.measurement_time(std::time::Duration::from_secs(3));
    group.sample_size(10);

    // Pre-compute all keys and values outside the benchmark loop
    let keys: Vec<Vec<Vec<u8>>> = (0..num_threads)
        .map(|tid| (0..ops_per_thread).map(|i| make_thread_key(tid, i)).collect())
        .collect();
    let values: Vec<Vec<u8>> = (0..ops_per_thread).map(make_value_pattern).collect();
    let keys = Arc::new(keys);
    let values = Arc::new(values);

    for mode in ALL_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("write_16_threads", mode.as_str()),
            &mode,
            |b, &mode| {
                let keys = Arc::clone(&keys);
                let values = Arc::clone(&values);

                b.iter_batched(
                    || setup_engine_arc("write_contention", mode),
                    |engine| {
                        let cf = engine.default_column_family();
                        let keys = Arc::clone(&keys);
                        let values = Arc::clone(&values);

                        thread::scope(|scope| {
                            for tid in 0..num_threads {
                                let engine = Arc::clone(&engine);
                                let cf = cf.clone();
                                let keys = Arc::clone(&keys);
                                let values = Arc::clone(&values);

                                scope.spawn(move || {
                                    for i in 0..ops_per_thread {
                                        engine
                                            .put(&cf, &keys[tid][i], &values[i])
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

    group.finish();
}

/// Benchmark heavy read contention (16 threads reading same keys)
fn bench_engine_heavy_read_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_engine_heavy_read_contention");
    group.sampling_mode(SamplingMode::Flat);

    let num_threads = 16;
    let num_keys = 2_000;
    let reads_per_thread = num_keys;
    let total_reads = num_threads * reads_per_thread;
    // Throughput in reads (elements) since read data varies
    group.throughput(Throughput::Elements(total_reads as u64));
    group.measurement_time(std::time::Duration::from_secs(3));
    group.sample_size(10);

    // Pre-compute keys and values outside the benchmark loop
    let keys: Vec<_> = (0..num_keys).map(make_key).collect();
    let values: Vec<_> = (0..num_keys).map(|_| make_value_fixed(VALUE_SIZE)).collect();
    let keys_arc = Arc::new(keys.clone());

    for mode in ALL_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("read_16_threads", mode.as_str()),
            &mode,
            |b, &mode| {
                let keys_ref = &keys;
                let vals_ref = &values;
                let keys_arc = Arc::clone(&keys_arc);

                b.iter_batched(
                    || {
                        let engine = setup_engine_arc("read_contention", mode);
                        let cf = engine.default_column_family();

                        // Pre-populate with data
                        for i in 0..num_keys {
                            engine.put(&cf, &keys_ref[i], &vals_ref[i]).expect("put failed");
                        }
                        engine.flush().expect("flush failed");
                        engine
                    },
                    |engine| {
                        let cf = engine.default_column_family();
                        let keys_arc = Arc::clone(&keys_arc);

                        thread::scope(|scope| {
                            for _ in 0..num_threads {
                                let engine = Arc::clone(&engine);
                                let cf = cf.clone();
                                let keys = Arc::clone(&keys_arc);

                                scope.spawn(move || {
                                    for i in 0..num_keys {
                                        let _ = black_box(engine.get(&cf, &keys[i]));
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

/// Benchmark mixed read/write contention
fn bench_engine_mixed_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_engine_mixed_contention");
    group.sampling_mode(SamplingMode::Flat);

    let num_threads = 16;
    let ops_per_thread = 1_500;
    let total_ops = num_threads * ops_per_thread;
    // Approximately half reads, half writes
    let total_bytes = (total_ops / 2) * (KEY_SIZE + VALUE_SIZE);
    group.throughput(Throughput::Bytes(total_bytes as u64));
    group.measurement_time(std::time::Duration::from_secs(3));
    group.sample_size(10);

    // Pre-compute keys outside the benchmark loop
    let keys: Vec<_> = (0..ops_per_thread).map(make_key).collect();
    // Pre-compute thread-specific values with fixed size
    let thread_values: Vec<Vec<Vec<u8>>> = (0..num_threads)
        .map(|tid| {
            (0..ops_per_thread)
                .map(|i| make_value_pattern(tid * ops_per_thread + i))
                .collect()
        })
        .collect();
    let keys_arc = Arc::new(keys.clone());
    let thread_values = Arc::new(thread_values);
    let init_value = make_value_pattern(0);

    for mode in ALL_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("mixed_16_threads", mode.as_str()),
            &mode,
            |b, &mode| {
                let keys_ref = &keys;
                let init_ref = &init_value;
                let keys_arc = Arc::clone(&keys_arc);
                let thread_values = Arc::clone(&thread_values);

                b.iter_batched(
                    || {
                        let engine = setup_engine_arc("mixed_contention", mode);
                        let cf = engine.default_column_family();

                        // Pre-populate with init values
                        for key in keys_ref.iter().take(ops_per_thread) {
                            engine.put(&cf, key, init_ref).expect("put failed");
                        }
                        engine
                    },
                    |engine| {
                        let cf = engine.default_column_family();
                        let keys_arc = Arc::clone(&keys_arc);
                        let thread_values = Arc::clone(&thread_values);

                        thread::scope(|scope| {
                            for tid in 0..num_threads {
                                let engine = Arc::clone(&engine);
                                let cf = cf.clone();
                                let keys = Arc::clone(&keys_arc);
                                let thread_values = Arc::clone(&thread_values);

                                scope.spawn(move || {
                                    for i in 0..ops_per_thread {
                                        if (tid + i) % 2 == 0 {
                                            // Write
                                            engine
                                                .put(&cf, &keys[i], &thread_values[tid][i])
                                                .expect("put failed");
                                        } else {
                                            // Read
                                            let _ = black_box(engine.get(&cf, &keys[i]));
                                        }
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

criterion_group! {
    name = tier3_system_contention_heavy;
    config = criterion_config_for_tier(BenchTier::Tier3System);
    targets = bench_engine_heavy_write_contention, bench_engine_heavy_read_contention, bench_engine_mixed_contention
}
criterion_main!(tier3_system_contention_heavy);
