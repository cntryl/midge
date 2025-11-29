//! Tier 3 — System LSM Benchmarks
//!
//! **Target Runtime:** ~30-60 seconds
//! **Run Frequency:** Nightly CI / Perf Baselines
//!
//! Covers:
//! - WAL append throughput
//! - Memtable insert throughput
//! - Flush to SST
//! - Reopen and point reads
//! - L0 → L1 compaction
//! - Mixed read/write workloads
//!
//! ## Design Notes
//!
//! - Returns engine from timed closures to exclude teardown from timing
//! - Precomputes all keys/values outside hot loops
//! - Uses unique paths to avoid cross-iteration interference
//! - Throughput measured in bytes
//! - Uses DURABLE_STORAGE_MODES since LSM operations require persistence

#[path = "../criterion_helper.rs"]
mod criterion_helper;

mod bench_common;

use bench_common::{
    precompute_read_indices, setup_engine, unique_bench_path, BenchEngineConfig, BenchStorageMode,
    DURABLE_STORAGE_MODES,
};

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

/// Key size in bytes
const KEY_SIZE: usize = 21; // "user:" + 8 bytes BE + ":profile"
/// Default value size
const VALUE_SIZE: usize = 40;
/// Bytes per operation
const BYTES_PER_OP: u64 = (KEY_SIZE + VALUE_SIZE) as u64;

#[inline]
fn make_key(i: usize) -> Bytes {
    let mut key = Vec::with_capacity(KEY_SIZE);
    key.extend_from_slice(b"user:");
    key.extend_from_slice(&(i as u64).to_be_bytes());
    key.extend_from_slice(b":profile");
    Bytes::from(key)
}

#[inline]
fn make_value(i: usize) -> Bytes {
    // Build value without format! allocation
    let mut value = Vec::with_capacity(VALUE_SIZE);
    value.extend_from_slice(b"{\"id\":");
    // Append i as decimal
    if i == 0 {
        value.push(b'0');
    } else {
        let start = value.len();
        let mut n = i;
        while n > 0 {
            value.push(b'0' + (n % 10) as u8);
            n /= 10;
        }
        value[start..].reverse();
    }
    value.extend_from_slice(b",\"name\":\"User");
    // Append i again
    if i == 0 {
        value.push(b'0');
    } else {
        let start = value.len();
        let mut n = i;
        while n > 0 {
            value.push(b'0' + (n % 10) as u8);
            n /= 10;
        }
        value[start..].reverse();
    }
    value.extend_from_slice(b"\"}");
    // Pad to VALUE_SIZE for consistent throughput
    while value.len() < VALUE_SIZE {
        value.push(b' ');
    }
    Bytes::from(value)
}

fn precompute_kv(n: usize) -> (Vec<Bytes>, Vec<Bytes>) {
    let mut keys = Vec::with_capacity(n);
    let mut vals = Vec::with_capacity(n);
    for i in 0..n {
        keys.push(make_key(i));
        vals.push(make_value(i));
    }
    (keys, vals)
}

/// Setup engine at a specific path for reopen tests
fn setup_engine_at_path(path: &std::path::Path, mode: BenchStorageMode) -> MidgeEngine {
    use cntryl_midge::cloud::mock::MockCloudBackend;

    match mode {
        BenchStorageMode::Memory => panic!("LSM benchmarks require persistent storage"),
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
            MidgeEngine::open(opts).unwrap()
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
            MidgeEngine::open(opts).unwrap()
        }
    }
}

// ---------------------------------------------------------------------------
// 1. WAL + Memtable Writes
// ---------------------------------------------------------------------------

fn bench_system_wal_write(c: &mut Criterion) {
    let mut g = c.benchmark_group("system_wal_write");
    g.sampling_mode(SamplingMode::Flat);

    for &entries in &[1_000usize, 10_000, 100_000] {
        let (keys, vals) = precompute_kv(entries);
        let bytes_total = (entries as u64) * BYTES_PER_OP;

        g.throughput(Throughput::Bytes(bytes_total));

        for mode in DURABLE_STORAGE_MODES {
            let bench_name = format!("{}/{}", entries, mode.as_str());
            g.bench_with_input(
                BenchmarkId::new("writes", &bench_name),
                &(entries, mode),
                |b, &(n, mode)| {
                    let keys_ref = &keys;
                    let vals_ref = &vals;

                    b.iter_batched(
                        || {
                            setup_engine(
                                "wal_write",
                                &BenchEngineConfig {
                                    storage_mode: mode,
                                    enable_compaction: false,
                                    ..Default::default()
                                },
                            )
                        },
                        |engine| {
                            let cf = engine.default_column_family();
                            for i in 0..n {
                                engine.put(&cf, &keys_ref[i], &vals_ref[i]).unwrap();
                            }
                            engine // prevent Drop during timing
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }

    g.finish();
}

// ---------------------------------------------------------------------------
// 2. Flush + Reopen + Point Reads
// ---------------------------------------------------------------------------

fn bench_system_flush_reopen_read(c: &mut Criterion) {
    let mut g = c.benchmark_group("system_flush_reopen_read");
    g.sampling_mode(SamplingMode::Flat);

    for &entries in &[10_000usize, 50_000] {
        let (keys, vals) = precompute_kv(entries);
        let read_count = 1_000usize;
        let read_indices = precompute_read_indices(entries, read_count, 42);
        let bytes_total = (read_count as u64) * BYTES_PER_OP;

        g.throughput(Throughput::Bytes(bytes_total));

        for mode in DURABLE_STORAGE_MODES {
            let bench_name = format!("{}/{}", entries, mode.as_str());
            g.bench_with_input(
                BenchmarkId::new("reads", &bench_name),
                &(entries, mode),
                |b, &(n, mode)| {
                    let keys_ref = &keys;
                    let vals_ref = &vals;
                    let read_indices_ref = &read_indices;

                    b.iter_batched(
                        || {
                            let path = unique_bench_path("flush_reopen");
                            let _ = std::fs::remove_dir_all(&path);

                            let engine = setup_engine_at_path(&path, mode);
                            let cf = engine.default_column_family();
                            for i in 0..n {
                                engine.put(&cf, &keys_ref[i], &vals_ref[i]).unwrap();
                            }
                            engine.flush().unwrap();
                            drop(engine);
                            (path, mode)
                        },
                        |(path, mode)| {
                            let engine = setup_engine_at_path(&path, mode);
                            let cf = engine.default_column_family();

                            for &idx in read_indices_ref {
                                black_box(engine.get(&cf, &keys_ref[idx]).unwrap());
                            }

                            engine // prevent Drop during timing
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }

    g.finish();
}

// ---------------------------------------------------------------------------
// 3. L0 → L1 Compaction
// ---------------------------------------------------------------------------

fn bench_system_l0_compaction(c: &mut Criterion) {
    let mut g = c.benchmark_group("system_l0_compaction");
    g.sampling_mode(SamplingMode::Flat);

    for &entries in &[50_000usize, 100_000] {
        let (keys, vals) = precompute_kv(entries);
        let bytes_total = (entries as u64) * BYTES_PER_OP;

        g.throughput(Throughput::Bytes(bytes_total));

        for mode in DURABLE_STORAGE_MODES {
            let bench_name = format!("{}/{}", entries, mode.as_str());
            g.bench_with_input(
                BenchmarkId::new("compact", &bench_name),
                &(entries, mode),
                |b, &(n, mode)| {
                    let keys_ref = &keys;
                    let vals_ref = &vals;

                    b.iter_batched(
                        || {
                            let engine = setup_engine(
                                "l0_compact",
                                &BenchEngineConfig {
                                    storage_mode: mode,
                                    enable_compaction: true,
                                    ..Default::default()
                                },
                            );
                            let cf = engine.default_column_family();
                            for i in 0..n {
                                engine.put(&cf, &keys_ref[i], &vals_ref[i]).unwrap();
                            }
                            engine.flush().unwrap();
                            (engine, cf)
                        },
                        |(engine, cf)| {
                            engine.compact_level(&cf, 0).unwrap();
                            engine // prevent Drop during timing
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }

    g.finish();
}

// ---------------------------------------------------------------------------
// 4. Mixed Read/Write Hotspot Workload
// ---------------------------------------------------------------------------

fn bench_system_mixed_workload(c: &mut Criterion) {
    let mut g = c.benchmark_group("system_mixed_workload");
    g.sampling_mode(SamplingMode::Flat);

    let hot_set_size = 10_000usize;
    let total_ops = 50_000usize;
    let (keys, vals) = precompute_kv(hot_set_size);

    // Precompute operation indices and types (80% read, 20% write)
    // Deterministic sequence
    let mut ops: Vec<(usize, bool)> = Vec::with_capacity(total_ops);
    let mut state = 12345u64;
    for _ in 0..total_ops {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let idx = (state as usize) % hot_set_size;
        // 80% read (is_read = true), 20% write
        let is_read = (state >> 32) % 100 < 80;
        ops.push((idx, is_read));
    }

    let bytes_total = (total_ops as u64) * BYTES_PER_OP;
    g.throughput(Throughput::Bytes(bytes_total));

    for mode in DURABLE_STORAGE_MODES {
        g.bench_with_input(
            BenchmarkId::new("mixed_80r_20w_hotset", mode.as_str()),
            &mode,
            |b, &mode| {
                let keys_ref = &keys;
                let vals_ref = &vals;
                let ops_ref = &ops;

                b.iter_batched(
                    || {
                        let engine = setup_engine(
                            "mixed_workload",
                            &BenchEngineConfig {
                                storage_mode: mode,
                                enable_compaction: false,
                                ..Default::default()
                            },
                        );
                        let cf = engine.default_column_family();
                        // Prefill hot set
                        for i in 0..hot_set_size {
                            engine.put(&cf, &keys_ref[i], &vals_ref[i]).unwrap();
                        }
                        engine.flush().unwrap();
                        engine
                    },
                    |engine| {
                        let cf = engine.default_column_family();

                        for &(idx, is_read) in ops_ref {
                            if is_read {
                                black_box(engine.get(&cf, &keys_ref[idx]).unwrap());
                            } else {
                                engine.put(&cf, &keys_ref[idx], &vals_ref[idx]).unwrap();
                            }
                        }

                        engine // prevent Drop during timing
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    g.finish();
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

criterion_group! {
    name = tier3_system_lsm;
    config = criterion_config_for_tier(BenchTier::Tier3System);
    targets =
        bench_system_wal_write,
        bench_system_flush_reopen_read,
        bench_system_l0_compaction,
        bench_system_mixed_workload
}

criterion_main!(tier3_system_lsm);
