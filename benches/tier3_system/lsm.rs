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

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

/// Global counter for unique benchmark directory names
static BENCH_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Key size in bytes
const KEY_SIZE: usize = 21; // "user:" + 8 bytes BE + ":profile"
/// Default value size
const VALUE_SIZE: usize = 40;
/// Bytes per operation
const BYTES_PER_OP: u64 = (KEY_SIZE + VALUE_SIZE) as u64;

/// Generate unique path for benchmark database
fn unique_bench_path(prefix: &str) -> PathBuf {
    let counter = BENCH_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("midge_bench_lsm_{}_{}_{}", prefix, pid, counter))
}

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

/// Precompute deterministic indices for reads
fn precompute_read_indices(n: usize, count: usize, seed: u64) -> Vec<usize> {
    // Simple deterministic pseudo-random using seed
    let mut indices = Vec::with_capacity(count);
    let mut state = seed;
    for _ in 0..count {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        indices.push((state as usize) % n);
    }
    indices
}

fn setup_db(name: &str, compaction: bool) -> MidgeEngine {
    let path = unique_bench_path(name);
    let _ = std::fs::remove_dir_all(&path);
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: path },
        memtable_size: 4 * 1024 * 1024,
        enable_compaction: compaction,
        wal_sync: false, // Disable sync for benchmark speed
        wal_buffer_size: 1024 * 1024,
        ..Default::default()
    };
    MidgeEngine::open(opts).unwrap()
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

        g.bench_with_input(BenchmarkId::from_parameter(entries), &entries, |b, &n| {
            b.iter_batched(
                || setup_db("wal_write", false),
                |engine| {
                    let cf = engine.default_column_family();
                    for i in 0..n {
                        engine.put(&cf, &keys[i], &vals[i]).unwrap();
                    }
                    engine // prevent Drop during timing
                },
                BatchSize::SmallInput,
            );
        });
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

        g.bench_with_input(BenchmarkId::from_parameter(entries), &entries, |b, &n| {
            b.iter_batched(
                || {
                    let path = unique_bench_path("flush_reopen");
                    let _ = std::fs::remove_dir_all(&path);
                    let opts = MidgeOptions {
                        storage_mode: StorageMode::LocalDisk {
                            db_path: path.clone(),
                        },
                        memtable_size: 4 * 1024 * 1024,
                        enable_compaction: false,
                        wal_sync: false,
                        ..Default::default()
                    };
                    let engine = MidgeEngine::open(opts).unwrap();
                    let cf = engine.default_column_family();
                    for i in 0..n {
                        engine.put(&cf, &keys[i], &vals[i]).unwrap();
                    }
                    engine.flush().unwrap();
                    drop(engine);
                    path
                },
                |path| {
                    let opts = MidgeOptions {
                        storage_mode: StorageMode::LocalDisk { db_path: path },
                        memtable_size: 4 * 1024 * 1024,
                        enable_compaction: false,
                        wal_sync: false,
                        ..Default::default()
                    };
                    let engine = MidgeEngine::open(opts).unwrap();
                    let cf = engine.default_column_family();

                    for &idx in &read_indices {
                        black_box(engine.get(&cf, &keys[idx]).unwrap());
                    }

                    engine // prevent Drop during timing
                },
                BatchSize::SmallInput,
            );
        });
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

        g.bench_with_input(BenchmarkId::from_parameter(entries), &entries, |b, &n| {
            b.iter_batched(
                || {
                    let engine = setup_db("l0_compact", true);
                    let cf = engine.default_column_family();
                    for i in 0..n {
                        engine.put(&cf, &keys[i], &vals[i]).unwrap();
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
        });
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

    g.bench_function("mixed_80r_20w_hotset", |b| {
        b.iter_batched(
            || {
                let engine = setup_db("mixed_workload", false);
                let cf = engine.default_column_family();
                // Prefill hot set
                for i in 0..hot_set_size {
                    engine.put(&cf, &keys[i], &vals[i]).unwrap();
                }
                engine.flush().unwrap();
                engine
            },
            |engine| {
                let cf = engine.default_column_family();

                for &(idx, is_read) in &ops {
                    if is_read {
                        black_box(engine.get(&cf, &keys[idx]).unwrap());
                    } else {
                        engine.put(&cf, &keys[idx], &vals[idx]).unwrap();
                    }
                }

                engine // prevent Drop during timing
            },
            BatchSize::SmallInput,
        );
    });

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
