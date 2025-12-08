//! Tier-3 System Benchmarks: SST Trie Index Performance
//!
//! Benchmarks core SST operations for baseline measurement.
//! Note: Detailed trie vs legacy index comparison requires integration 
//! with SST writer/reader flags (phase 3.6).
//!
//! This benchmark focuses on:
//! - Point lookups (single key retrieval)
//! - Range scans (multiple keys in range)
//! - Full scans (complete database iteration)
//!
//! ## Design Notes
//!
//! - Uses standard LocalDisk storage mode
//! - Precomputes all SST data outside timed sections
//! - Uses black_box to prevent compiler optimizations
//! - Measures lookup latency and throughput

#[path = "../criterion_helper.rs"]
mod criterion_helper;

mod bench_common;

use bench_common::{
    make_key, precompute_kv, BenchEngineConfig, setup_engine,
    BYTES_PER_OP, VALUE_SIZE,
};

use cntryl_midge::Query;
use criterion::{
    criterion_group, criterion_main, BatchSize, Criterion, SamplingMode, Throughput,
};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;

// ============================================================================
// Point Lookup Benchmark
// ============================================================================

fn bench_point_lookups(c: &mut Criterion) {
    let mut group = c.benchmark_group("sst_trie_index/point_lookups");
    group.sampling_mode(SamplingMode::Flat);

    let num_keys = 10_000usize;
    let num_lookups = 1_000usize;
    let (keys, vals) = precompute_kv(num_keys, VALUE_SIZE);

    // Precompute lookup indices for deterministic access pattern
    let lookup_indices: Vec<usize> = (0..num_lookups).map(|i| i % num_keys).collect();

    let bytes_total = (num_lookups as u64) * BYTES_PER_OP;
    group.throughput(Throughput::Bytes(bytes_total));

    group.bench_function("dense_10k_keys_lookup", |b| {
        b.iter_batched(
            || {
                let config = BenchEngineConfig::local_disk();
                let engine = setup_engine("sst_point_lookup", &config);
                let cf = engine.default_column_family();
                for (k, v) in keys.iter().zip(vals.iter()) {
                    engine.put(&cf, k, v).unwrap();
                }
                engine.flush().unwrap();
                engine
            },
            |engine| {
                let cf = engine.default_column_family();
                for &idx in &lookup_indices {
                    let result = engine.get(&cf, &keys[idx]).unwrap();
                    black_box(result);
                }
                engine
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

// ============================================================================
// Full Range Scan Benchmark
// ============================================================================

fn bench_full_scans(c: &mut Criterion) {
    let mut group = c.benchmark_group("sst_trie_index/full_scans");
    group.sampling_mode(SamplingMode::Flat);

    let num_keys = 10_000usize;
    let (keys, vals) = precompute_kv(num_keys, VALUE_SIZE);

    let bytes_total = (num_keys as u64) * BYTES_PER_OP;
    group.throughput(Throughput::Bytes(bytes_total));

    group.bench_function("full_range_scan_10k", |b| {
        b.iter_batched(
            || {
                let config = BenchEngineConfig::local_disk();
                let engine = setup_engine("sst_full_scan", &config);
                let cf = engine.default_column_family();
                for (k, v) in keys.iter().zip(vals.iter()) {
                    engine.put(&cf, k, v).unwrap();
                }
                engine.flush().unwrap();
                engine
            },
            |engine| {
                let cf = engine.default_column_family();
                let query = Query::new();
                let results = engine.scan(&cf, query).expect("scan failed");
                black_box(results.len());
                engine
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

// ============================================================================
// Prefix Range Scan Benchmark (where Trie excels)
// ============================================================================

fn bench_prefix_scans(c: &mut Criterion) {
    let mut group = c.benchmark_group("sst_trie_index/prefix_scans");
    group.sampling_mode(SamplingMode::Flat);

    let num_keys = 10_000usize;
    let (keys, vals) = precompute_kv(num_keys, VALUE_SIZE);

    // Scan 10% of keys (1000 keys)
    let scan_keys = 1_000usize;
    let bytes_total = (scan_keys as u64) * BYTES_PER_OP;
    group.throughput(Throughput::Bytes(bytes_total));

    group.bench_function("prefix_range_1k_of_10k", |b| {
        b.iter_batched(
            || {
                let config = BenchEngineConfig::local_disk();
                let engine = setup_engine("sst_prefix_scan", &config);
                let cf = engine.default_column_family();
                for (k, v) in keys.iter().zip(vals.iter()) {
                    engine.put(&cf, k, v).unwrap();
                }
                engine.flush().unwrap();
                engine
            },
            |engine| {
                let cf = engine.default_column_family();
                // Scan range: key_0000000001 to key_0000001000
                let start = make_key(1);
                let end = make_key(1000);
                let query = Query::new().start_key(start).end_key(end);
                let results = engine.scan(&cf, query).expect("scan failed");
                black_box(results.len());
                engine
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

// ============================================================================
// Criterion Setup
// ============================================================================

criterion_group!(
    name = sst_trie_benches;
    config = criterion_config_for_tier(BenchTier::Tier3System);
    targets = bench_point_lookups, bench_full_scans, bench_prefix_scans
);

criterion_main!(sst_trie_benches);
