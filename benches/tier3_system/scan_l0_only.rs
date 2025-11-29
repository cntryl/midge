//! Tier 3 — Scan L0-only bench
//!
//! **Target Runtime:** ~30-60 seconds
//! **Run Frequency:** Nightly CI / Perf Baselines
//!
//! Covers L0-only scan operations (memtable + L0 SSTs)
//!
//! ## Design Notes
//!
//! - Uses DURABLE_STORAGE_MODES since L0 scans require persistence

#[path = "../criterion_helper.rs"]
mod criterion_helper;

mod bench_common;

use bench_common::{
    precompute_kv, setup_engine, BenchEngineConfig, BYTES_PER_OP, DURABLE_STORAGE_MODES,
    VALUE_SIZE,
};

use bytes::Bytes;
use cntryl_midge::Query;
use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;

/// Benchmark scanning L0 SSTs (10k keys spread across multiple L0 files)
fn bench_scan_l0_direct(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_scan_l0_direct");
    group.sampling_mode(SamplingMode::Flat);

    let num_keys = 10_000usize;
    let (keys, vals) = precompute_kv(num_keys, VALUE_SIZE);
    let bytes_total = (num_keys as u64) * BYTES_PER_OP;

    // Precompute query bounds
    let start_key: Bytes = Bytes::from_static(b"key_0000000000");
    let end_key: Bytes = Bytes::from_static(b"key_9999999999");

    group.throughput(Throughput::Bytes(bytes_total));

    for mode in DURABLE_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("scan_l0_10k_keys", mode.as_str()),
            &mode,
            |b, &mode| {
                let keys_ref = &keys;
                let vals_ref = &vals;
                let start = &start_key;
                let end = &end_key;

                b.iter_batched(
                    || {
                        let engine = setup_engine(
                            "scan_l0",
                            &BenchEngineConfig {
                                storage_mode: mode,
                                enable_compaction: false,
                                memtable_size: 2 * 1024 * 1024,
                                ..Default::default()
                            },
                        );
                        let cf = engine.default_column_family();

                        // Write in chunks and flush each
                        for (chunk_idx, chunk) in keys_ref.chunks(2_500).enumerate() {
                            let base_idx = chunk_idx * 2_500;
                            for (i, key) in chunk.iter().enumerate() {
                                engine.put(&cf, key, &vals_ref[base_idx + i]).unwrap();
                            }
                            engine.flush().unwrap();
                        }

                        engine
                    },
                    |engine| {
                        let cf = engine.default_column_family();
                        let query = Query::new().start_key(start.clone()).end_key(end.clone());
                        let results = engine.scan(&cf, query).unwrap();
                        for kv in results {
                            black_box(kv);
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

/// Benchmark scanning with prefix filter in L0
fn bench_scan_l0_prefix(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_scan_l0_prefix");
    group.sampling_mode(SamplingMode::Flat);

    let num_keys = 10_000usize;
    let (keys, vals) = precompute_kv(num_keys, VALUE_SIZE);
    // Prefix scan returns ~10% of keys
    let expected_matches = num_keys / 10;
    let bytes_total = (expected_matches as u64) * BYTES_PER_OP;

    let prefix: Bytes = Bytes::from_static(b"key_0000001"); // Matches key_0000001000 to key_0000001999

    group.throughput(Throughput::Bytes(bytes_total));

    for mode in DURABLE_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("prefix_scan_1k_match", mode.as_str()),
            &mode,
            |b, &mode| {
                let keys_ref = &keys;
                let vals_ref = &vals;
                let prefix_ref = &prefix;

                b.iter_batched(
                    || {
                        let engine = setup_engine(
                            "scan_l0_prefix",
                            &BenchEngineConfig {
                                storage_mode: mode,
                                enable_compaction: false,
                                memtable_size: 2 * 1024 * 1024,
                                ..Default::default()
                            },
                        );
                        let cf = engine.default_column_family();

                        for (chunk_idx, chunk) in keys_ref.chunks(2_500).enumerate() {
                            let base_idx = chunk_idx * 2_500;
                            for (i, key) in chunk.iter().enumerate() {
                                engine.put(&cf, key, &vals_ref[base_idx + i]).unwrap();
                            }
                            engine.flush().unwrap();
                        }

                        engine
                    },
                    |engine| {
                        let cf = engine.default_column_family();
                        let query = Query::new().prefix(prefix_ref.clone());
                        let results = engine.scan(&cf, query).unwrap();
                        for kv in results {
                            black_box(kv);
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

criterion_group! {
    name = tier3_system_scan_l0;
    config = criterion_config_for_tier(BenchTier::Tier3System);
    targets =
        bench_scan_l0_direct,
        bench_scan_l0_prefix
}
criterion_main!(tier3_system_scan_l0);
