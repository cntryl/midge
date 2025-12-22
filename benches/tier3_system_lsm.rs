//! Tier 3 — System LSM Benchmarks
//!
//! **Target Runtime:** ~30–60 seconds
//! **Run Frequency:** Nightly CI / Perf Baselines
//!
//! Covers full LSM-engine behaviors:
//! - WAL append throughput
//! - Memtable inserts
//! - Flush → Reopen → Point reads
//! - L0 → L1 compaction
//! - Mixed 80/20 read/write hotspot workload
//!
//! ## Design Notes
//!
//! - Returns engine from timed closures to exclude teardown
//! - Precomputes all keys/values outside hot loops
//! - Uses unique paths to avoid cross-test interference
//! - Throughput measured in total bytes
//! - Uses DURABLE_STORAGE_MODES since LSM ops require persistence
#[allow(unused)]
const _TIER3_GUARD: () = {
    // Tier-3 benches must use bench_common::tier3 APIs and `tier3_bench!`/`tier3_bench_restore!`.
};
#[path = "./criterion_helper.rs"]
mod criterion_helper;

#[path = "./tier3_system_bench_common.rs"]
mod bench_common;

use bench_common::{
    precompute_read_indices, setup_engine, setup_engine_at_path, unique_bench_path,
    BenchEngineConfig, BenchStorageMode, DURABLE_STORAGE_MODES,
};

use bytes::Bytes;
use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;

// ===========================================================================
// Shared Constants
// ===========================================================================

const KEY_SIZE: usize = 21; // "user:{u64_be}:profile"
const VALUE_SIZE: usize = 40;
const BYTES_PER_OP: u64 = (KEY_SIZE + VALUE_SIZE) as u64;

const BENCH_WAL_WRITE: &str = "wal_write";
const BENCH_FLUSH_REOPEN: &str = "flush_reopen";
const BENCH_L0_COMPACT: &str = "l0_compact";
const BENCH_MIXED: &str = "mixed_workload";

// ===========================================================================
// Fast deterministic key/value generators
// ===========================================================================

#[inline]
fn make_key(i: usize) -> Bytes {
    let mut buf = Vec::with_capacity(KEY_SIZE);
    buf.extend_from_slice(b"user:");
    buf.extend_from_slice(&(i as u64).to_be_bytes());
    buf.extend_from_slice(b":profile");
    Bytes::from(buf)
}

#[inline]
fn make_value(i: usize) -> Bytes {
    let mut buf = vec![0u8; VALUE_SIZE];
    buf[..8].copy_from_slice(&(i as u64).to_be_bytes());
    let pattern = (i % 256) as u8;
    for b in buf.iter_mut().skip(8) {
        *b = pattern;
    }
    Bytes::from(buf)
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

// ===========================================================================
// 1. WAL + Memtable Writes
// ===========================================================================

fn bench_system_wal_write(c: &mut Criterion) {
    let mut g = c.benchmark_group("system_lsm/wal_write");
    g.sampling_mode(SamplingMode::Flat);

    for &entries in &[1_000, 10_000, 50_000] {
        let (keys, vals) = precompute_kv(entries);
        let bytes_total = (entries as u64) * BYTES_PER_OP;

        g.throughput(Throughput::Bytes(bytes_total));

        for mode in DURABLE_STORAGE_MODES {
            g.bench_with_input(
                BenchmarkId::new(format!("{}_entries", entries), mode.as_str()),
                &(entries, mode),
                |b, &(n, mode)| {
                    let keys_ref = &keys;
                    let vals_ref = &vals;

                    b.iter_batched(
                        || {
                            setup_engine(
                                BENCH_WAL_WRITE,
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
                                engine.put(cf, &keys_ref[i], &vals_ref[i]).unwrap();
                            }
                            engine
                        },
                        BatchSize::LargeInput,
                    );
                },
            );
        }
    }

    g.finish();
}

// ===========================================================================
// 2. Flush → Reopen → Point Reads
// ===========================================================================

fn bench_system_flush_reopen_read(c: &mut Criterion) {
    let mut g = c.benchmark_group("system_lsm/flush_reopen_read");
    g.sampling_mode(SamplingMode::Flat);

    for &entries in &[10_000, 50_000] {
        let (keys, vals) = precompute_kv(entries);
        let read_count = 1_000;
        let read_indices = precompute_read_indices(entries, read_count, 42);
        let bytes_total = (read_count as u64) * BYTES_PER_OP;

        g.throughput(Throughput::Bytes(bytes_total));

        for mode in DURABLE_STORAGE_MODES {
            g.bench_with_input(
                BenchmarkId::new(format!("{}_entries", entries), mode.as_str()),
                &(entries, mode),
                |b, &(n, mode)| {
                    let keys_ref = &keys;
                    let vals_ref = &vals;
                    let read_indices_ref = &read_indices;

                    b.iter_batched(
                        || {
                            let path = unique_bench_path(BENCH_FLUSH_REOPEN);
                            let _ = std::fs::remove_dir_all(&path);

                            let cfg = BenchEngineConfig {
                                storage_mode: mode,
                                enable_compaction: false,
                                ..Default::default()
                            };

                            let engine = setup_engine_at_path(&path, &cfg);
                            let cf = engine.default_column_family();

                            for i in 0..n {
                                engine.put(cf, &keys_ref[i], &vals_ref[i]).unwrap();
                            }
                            engine.flush().unwrap();
                            drop(engine);

                            (path, cfg)
                        },
                        |(path, cfg)| {
                            let engine = setup_engine_at_path(&path, &cfg);
                            let cf = engine.default_column_family();

                            for &idx in read_indices_ref {
                                let key = black_box(&keys_ref[idx]);
                                black_box(engine.get(cf, key).unwrap());
                            }

                            engine
                        },
                        BatchSize::LargeInput,
                    );
                },
            );
        }
    }

    g.finish();
}

// ===========================================================================
// 3. L0 → L1 Compaction
// ===========================================================================

fn bench_system_l0_compaction(c: &mut Criterion) {
    let mut g = c.benchmark_group("system_lsm/l0_compaction");
    g.sampling_mode(SamplingMode::Flat);

    for &entries in &[25_000, 50_000] {
        let (keys, vals) = precompute_kv(entries);
        let bytes_total = (entries as u64) * BYTES_PER_OP;

        g.throughput(Throughput::Bytes(bytes_total));

        for mode in DURABLE_STORAGE_MODES {
            if entries > 25_000 && !matches!(mode, BenchStorageMode::LocalDisk) {
                continue; // Avoid huge cloud costs
            }

            g.bench_with_input(
                BenchmarkId::new(format!("{}_entries", entries), mode.as_str()),
                &(entries, mode),
                |b, &(n, mode)| {
                    let keys_ref = &keys;
                    let vals_ref = &vals;

                    b.iter_batched(
                        || {
                            let engine = setup_engine(
                                BENCH_L0_COMPACT,
                                &BenchEngineConfig {
                                    storage_mode: mode,
                                    enable_compaction: true,
                                    ..Default::default()
                                },
                            );

                            let cf = engine.default_column_family();
                            for i in 0..n {
                                engine.put(cf, &keys_ref[i], &vals_ref[i]).unwrap();
                            }
                            engine.flush().unwrap(); // creates L0

                            engine
                        },
                        |engine| {
                            engine.compact_all().unwrap();
                            engine
                        },
                        BatchSize::LargeInput,
                    );
                },
            );
        }
    }

    g.finish();
}

// ===========================================================================
// 4. Mixed Hotspot Read/Write Workload (80/20)
// ===========================================================================

fn bench_system_mixed_workload(c: &mut Criterion) {
    let mut g = c.benchmark_group("system_lsm/mixed_workload");
    g.sampling_mode(SamplingMode::Flat);

    let hot_set_size = 10_000;
    let total_ops = 50_000;

    let (keys, vals) = precompute_kv(hot_set_size);

    // Precompute deterministic 80% read / 20% write sequence
    let mut ops = Vec::with_capacity(total_ops);
    let mut state = 12345u64;

    for _ in 0..total_ops {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);

        let idx = (state as usize) % hot_set_size;
        let is_read = ((state >> 32) % 100) < 80;

        ops.push((idx, is_read));
    }

    let bytes_total = (total_ops as u64) * BYTES_PER_OP;
    g.throughput(Throughput::Bytes(bytes_total));

    for mode in DURABLE_STORAGE_MODES {
        g.bench_with_input(
            BenchmarkId::new("80r_20w_hotset", mode.as_str()),
            &mode,
            |b, &mode| {
                let keys_ref = &keys;
                let vals_ref = &vals;
                let ops_ref = &ops;

                b.iter_batched(
                    || {
                        let engine = setup_engine(
                            BENCH_MIXED,
                            &BenchEngineConfig {
                                storage_mode: mode,
                                enable_compaction: false,
                                ..Default::default()
                            },
                        );

                        let cf = engine.default_column_family();
                        for i in 0..hot_set_size {
                            engine.put(cf, &keys_ref[i], &vals_ref[i]).unwrap();
                        }
                        engine.flush().unwrap(); // baseline L0

                        engine
                    },
                    |engine| {
                        let cf = engine.default_column_family();

                        for &(idx, is_read) in ops_ref {
                            let key = black_box(&keys_ref[idx]);
                            if is_read {
                                black_box(engine.get(cf, key).unwrap());
                            } else {
                                let val = black_box(&vals_ref[idx]);
                                engine.put(cf, key, val).unwrap();
                            }
                        }

                        engine
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }

    g.finish();
}

// ===========================================================================
// Registration
// ===========================================================================

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
