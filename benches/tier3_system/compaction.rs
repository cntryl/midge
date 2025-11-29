//! Tier 3 — System Benchmarks: Compaction
//!
//! **Target Runtime:** 1-5 minutes
//! **Run Frequency:** Nightly / release builds
//!
//! Covers full compaction workflows (flush, merge, write amplification).
//!
//! ## Benchmarks
//!
//! - `system_flush`: Measures memtable-to-SST flush latency
//! - `system_compact`: Measures full compaction (compact_all) latency
//! - `system_flush_throughput`: Measures flush bytes/sec with varying value sizes
//! - `system_incremental_compact`: Multiple L0 files compaction
//!
//! ## Design Notes
//!
//! - Uses DURABLE_STORAGE_MODES since compaction requires persistence

#[path = "../criterion_helper.rs"]
mod criterion_helper;

mod bench_common;

use bench_common::{
    setup_engine, unique_bench_path, BenchEngineConfig, BenchStorageMode, DURABLE_STORAGE_MODES,
};

use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

/// Key size in bytes (fixed for consistent measurements)
const KEY_SIZE: usize = 16;
/// Default value size in bytes
const DEFAULT_VALUE_SIZE: usize = 100;

/// Pre-generate keys and values with configurable value size.
fn generate_kv(num_keys: usize, value_size: usize) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let mut keys = Vec::with_capacity(num_keys);
    let mut values = Vec::with_capacity(num_keys);

    for i in 0..num_keys {
        // Fixed-size keys: "k" + 15-digit zero-padded number
        let mut key = vec![b'k'; KEY_SIZE];
        let mut n = i;
        for j in (1..KEY_SIZE).rev() {
            key[j] = b'0' + (n % 10) as u8;
            n /= 10;
        }
        keys.push(key);

        // Fixed-size value
        let mut value = vec![0u8; value_size];
        if value_size >= 8 {
            value[..8].copy_from_slice(&(i as u64).to_be_bytes());
        }
        let pattern = (i % 256) as u8;
        for byte in value.iter_mut().skip(8) {
            *byte = pattern;
        }
        values.push(value);
    }

    (keys, values)
}

/// Setup engine at specific path for reopen
fn setup_engine_at_path(path: &std::path::Path, mode: BenchStorageMode) -> MidgeEngine {
    use cntryl_midge::cloud::mock::MockCloudBackend;

    match mode {
        BenchStorageMode::Memory => panic!("Compaction benchmarks require persistent storage"),
        BenchStorageMode::LocalDisk => {
            let opts = MidgeOptions {
                storage_mode: StorageMode::LocalDisk {
                    db_path: path.to_path_buf(),
                },
                memtable_size: 4 * 1024 * 1024,
                enable_compaction: false,
                wal_sync: false,
                ..Default::default()
            };
            MidgeEngine::open(opts).expect("failed to open")
        }
        BenchStorageMode::CloudBacked => {
            let backend = Arc::new(MockCloudBackend::new().with_latency(Duration::from_millis(1)));
            let opts = MidgeOptions {
                storage_mode: StorageMode::CloudBacked {
                    local_cache_path: path.to_path_buf(),
                    cloud_backend: backend,
                    storage_context: Default::default(),
                    local_wal_sync: false,
                    wal_batch_size: 1024 * 1024,
                    sst_cache_capacity: 10,
                },
                memtable_size: 4 * 1024 * 1024,
                enable_compaction: false,
                wal_sync: false,
                ..Default::default()
            };
            MidgeEngine::open(opts).expect("failed to open")
        }
    }
}

fn bench_flush(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_flush");
    group.sampling_mode(SamplingMode::Flat);

    for &num_keys in &[10_000, 50_000] {
        let (keys, values) = generate_kv(num_keys, DEFAULT_VALUE_SIZE);
        let total_bytes = num_keys * (KEY_SIZE + DEFAULT_VALUE_SIZE);

        group.throughput(Throughput::Bytes(total_bytes as u64));

        for mode in DURABLE_STORAGE_MODES {
            let bench_name = format!("{}keys/{}", num_keys, mode.as_str());
            group.bench_with_input(
                BenchmarkId::new("flush", &bench_name),
                &(num_keys, mode),
                |b, &(_size, mode)| {
                    let keys_ref = &keys;
                    let vals_ref = &values;

                    b.iter_batched(
                        || {
                            let engine = setup_engine(
                                "flush",
                                &BenchEngineConfig {
                                    storage_mode: mode,
                                    enable_compaction: false,
                                    ..Default::default()
                                },
                            );
                            let cf = engine.default_column_family();
                            for (k, v) in keys_ref.iter().zip(vals_ref.iter()) {
                                engine.put(&cf, k, v).unwrap();
                            }
                            engine
                        },
                        |engine| {
                            engine.flush().expect("flush failed");
                            engine // prevent Drop during timing
                        },
                        BatchSize::LargeInput,
                    )
                },
            );
        }
    }

    group.finish();
}

fn bench_compact_all(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_compact");
    group.sampling_mode(SamplingMode::Flat);

    for &num_keys in &[50_000, 100_000] {
        let (keys, values) = generate_kv(num_keys, DEFAULT_VALUE_SIZE);
        let total_bytes = num_keys * (KEY_SIZE + DEFAULT_VALUE_SIZE);

        group.throughput(Throughput::Bytes(total_bytes as u64));

        for mode in DURABLE_STORAGE_MODES {
            let bench_name = format!("{}keys/{}", num_keys, mode.as_str());
            group.bench_with_input(
                BenchmarkId::new("compact_all", &bench_name),
                &(num_keys, mode),
                |b, &(_size, mode)| {
                    let keys_ref = &keys;
                    let vals_ref = &values;

                    b.iter_batched(
                        || {
                            let engine = setup_engine(
                                "compact",
                                &BenchEngineConfig {
                                    storage_mode: mode,
                                    enable_compaction: true,
                                    ..Default::default()
                                },
                            );
                            let cf = engine.default_column_family();
                            for (k, v) in keys_ref.iter().zip(vals_ref.iter()) {
                                engine.put(&cf, k, v).unwrap();
                            }
                            engine.flush().unwrap();
                            engine
                        },
                        |engine| {
                            engine.compact_all().expect("compact_all failed");
                            engine // prevent Drop during timing
                        },
                        BatchSize::LargeInput,
                    )
                },
            );
        }
    }

    group.finish();
}

fn bench_flush_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_flush_throughput");
    group.sampling_mode(SamplingMode::Flat);

    let num_keys = 20_000;

    for &value_size in &[64, 256, 1024, 4096] {
        let (keys, values) = generate_kv(num_keys, value_size);
        let total_bytes = num_keys * (KEY_SIZE + value_size);

        group.throughput(Throughput::Bytes(total_bytes as u64));

        for mode in DURABLE_STORAGE_MODES {
            let bench_name = format!("{}B_values/{}", value_size, mode.as_str());
            group.bench_with_input(
                BenchmarkId::new("flush_tp", &bench_name),
                &(value_size, mode),
                |b, &(_vs, mode)| {
                    let keys_ref = &keys;
                    let vals_ref = &values;

                    b.iter_batched(
                        || {
                            let engine = setup_engine(
                                "flush_tp",
                                &BenchEngineConfig {
                                    storage_mode: mode,
                                    enable_compaction: false,
                                    ..Default::default()
                                },
                            );
                            let cf = engine.default_column_family();
                            for (k, v) in keys_ref.iter().zip(vals_ref.iter()) {
                                engine.put(&cf, k, v).unwrap();
                            }
                            engine
                        },
                        |engine| {
                            engine.flush().expect("flush failed");
                            engine // prevent Drop during timing
                        },
                        BatchSize::LargeInput,
                    )
                },
            );
        }
    }

    group.finish();
}

fn bench_incremental_compact(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_incremental_compact");
    group.sampling_mode(SamplingMode::Flat);

    let num_keys_per_batch = 10_000;
    let num_batches = 5;

    // Generate multiple batches of overlapping keys
    let mut all_keys = Vec::new();
    let mut all_values = Vec::new();

    for batch in 0..num_batches {
        let (keys, values) = generate_kv(num_keys_per_batch, DEFAULT_VALUE_SIZE);
        for (mut k, v) in keys.into_iter().zip(values.into_iter()) {
            k[0] = b'a' + (batch as u8);
            all_keys.push(k);
            all_values.push(v);
        }
    }

    let total_keys = num_keys_per_batch * num_batches;
    let total_bytes = total_keys * (KEY_SIZE + DEFAULT_VALUE_SIZE);

    group.throughput(Throughput::Bytes(total_bytes as u64));

    for mode in DURABLE_STORAGE_MODES {
        let bench_name = format!("{}batches_x_{}keys/{}", num_batches, num_keys_per_batch, mode.as_str());
        group.bench_with_input(
            BenchmarkId::new("incremental", &bench_name),
            &mode,
            |b, &mode| {
                let keys_ref = &all_keys;
                let vals_ref = &all_values;

                b.iter_batched(
                    || {
                        let path = unique_bench_path("incr_compact");
                        let _ = std::fs::remove_dir_all(&path);

                        let engine = setup_engine_at_path(&path, mode);
                        let cf = engine.default_column_family();

                        // Write and flush in batches to create multiple L0 files
                        for batch_idx in 0..num_batches {
                            let start = batch_idx * num_keys_per_batch;
                            let end = start + num_keys_per_batch;
                            for idx in start..end {
                                engine.put(&cf, &keys_ref[idx], &vals_ref[idx]).unwrap();
                            }
                            engine.flush().unwrap();
                        }

                        engine
                    },
                    |engine| {
                        engine.compact_all().expect("compact_all failed");
                        engine // prevent Drop during timing
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = tier3_system_compaction;
    config = criterion_config_for_tier(BenchTier::Tier3System);
    targets = bench_flush, bench_compact_all, bench_flush_throughput, bench_incremental_compact
}
criterion_main!(tier3_system_compaction);
