//! Tier 3 — Scan multi-level benchmark
//!
//! **Target Runtime:** ~30-60 seconds
//! **Run Frequency:** Nightly CI / Perf Baselines
//!
//! Covers LSM scans across multiple levels
//!
//! ## Design Notes
//!
//! - Uses DURABLE_STORAGE_MODES since multi-level scans require persistence

#[path = "./criterion_helper.rs"]
mod criterion_helper;

#[path = "./tier3_system_bench_common.rs"]
mod bench_common;

use bench_common::{
    precompute_kv, setup_engine, BenchEngineConfig, BYTES_PER_OP, DURABLE_STORAGE_MODES, VALUE_SIZE,
};

use bytes::Bytes;
use cntryl_midge::Query;
use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;

/// Benchmark scanning across multiple LSM levels (50k keys)
fn bench_scan_multi_level_range(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_scan_multi_level_range");
    group.sampling_mode(SamplingMode::Flat);

    let num_keys = 50_000usize;
    let (keys, vals) = precompute_kv(num_keys, VALUE_SIZE);
    let bytes_total = (num_keys as u64) * BYTES_PER_OP;

    // Precompute query bounds
    let start_key: Bytes = Bytes::from_static(b"key_0000000000");
    let end_key: Bytes = Bytes::from_static(b"key_0000049999");

    group.throughput(Throughput::Bytes(bytes_total));

    for mode in DURABLE_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("scan_50k_keys", mode.as_str()),
            &mode,
            |b, &mode| {
                let keys_ref = &keys;
                let vals_ref = &vals;
                let start = &start_key;
                let end = &end_key;

                b.iter_batched(
                    || {
                        let engine = setup_engine(
                            "scan_multi",
                            &BenchEngineConfig {
                                storage_mode: mode,
                                enable_compaction: true,
                                memtable_size: 1024 * 1024,
                                ..Default::default()
                            },
                        );
                        let cf = engine.default_column_family();

                        // Populate with 50k keys to trigger multiple flushes and compactions
                        for i in 0..num_keys {
                            engine
                                .put(&cf, &keys_ref[i], &vals_ref[i])
                                .expect("put failed");

                            // Flush periodically to create multiple files
                            if i % 5000 == 4999 {
                                engine.flush().expect("flush failed");
                            }
                        }

                        // Trigger compactions to spread data across levels
                        engine.flush().expect("final flush failed");
                        let _ = engine.compact_level(&cf, 0);

                        engine
                    },
                    |engine| {
                        // Scan a large range across all levels
                        let cf = engine.default_column_family();
                        let query = Query::new().start_key(start.clone()).end_key(end.clone());

                        let results = engine.scan(&cf, query).expect("scan failed");
                        black_box(results.len());

                        engine
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = tier3_system_scan_multi_level;
    config = criterion_config_for_tier(BenchTier::Tier3System);
    targets = bench_scan_multi_level_range
}
criterion_main!(tier3_system_scan_multi_level);
