//! Tier 2 — Bloom false positive rate benchmark
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Measures query throughput and actual false positive rates for bloom filters

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;

use cntryl_midge::sst::bloom::{writer::BloomFilterOps, BloomWriter};

/// Pre-generate keys as raw bytes
fn make_test_keys(start: usize, count: usize) -> Vec<Vec<u8>> {
    (start..start + count)
        .map(|i| format!("key_{:010}", i).into_bytes())
        .collect()
}

/// Benchmark bloom false positive rate for small filter (1k keys)
/// Measures FPR by querying non-existent keys
fn bench_bloom_false_positive_rate_small(c: &mut Criterion) {
    // Pre-generate all keys
    let insert_keys = make_test_keys(0, 1_000);
    let query_keys = make_test_keys(100_000, 10_000);

    // Build the filter once (outside benchmark)
    let mut builder = BloomWriter::with_defaults(1_000); // 1% FPR
    for key in &insert_keys {
        builder.insert(key);
    }
    let filter = builder.finish();

    let mut group = c.benchmark_group("subsystem_bloom_false_positive_rate_small");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(10_000));

    group.bench_function("query_10k_absent", |b| {
        b.iter(|| {
            // Query 10k non-existent keys
            let mut false_positives = 0u32;
            for key in &query_keys {
                if filter.contains(key).might_be_present() {
                    false_positives += 1;
                }
            }
            black_box(false_positives)
        })
    });

    group.finish();

    // Report actual FPR after benchmark (informational)
    let mut fps = 0;
    for key in &query_keys {
        if filter.contains(key).might_be_present() {
            fps += 1;
        }
    }
    println!(
        "Small filter FPR: {:.4}% ({} / {})",
        fps as f64 / query_keys.len() as f64 * 100.0,
        fps,
        query_keys.len()
    );
}

/// Benchmark bloom false positive rate for large filter (100k keys)
/// Larger scale FPR measurement
fn bench_bloom_false_positive_rate_large(c: &mut Criterion) {
    // Pre-generate all keys
    let insert_keys = make_test_keys(0, 100_000);
    let query_keys = make_test_keys(1_000_000, 50_000);

    // Build the filter once
    let mut builder = BloomWriter::with_defaults(100_000); // 1% FPR
    for key in &insert_keys {
        builder.insert(key);
    }
    let filter = builder.finish();

    let mut group = c.benchmark_group("subsystem_bloom_false_positive_rate_large");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(50_000));

    group.bench_function("query_50k_absent", |b| {
        b.iter(|| {
            // Query 50k non-existent keys
            let mut false_positives = 0u32;
            for key in &query_keys {
                if filter.contains(key).might_be_present() {
                    false_positives += 1;
                }
            }
            black_box(false_positives)
        })
    });

    group.finish();

    // Report actual FPR
    let mut fps = 0;
    for key in &query_keys {
        if filter.contains(key).might_be_present() {
            fps += 1;
        }
    }
    println!(
        "Large filter FPR: {:.4}% ({} / {})",
        fps as f64 / query_keys.len() as f64 * 100.0,
        fps,
        query_keys.len()
    );
}

criterion_group! {
    name = tier2_subsystem_bloom_false_positive_rate;
    config = criterion_config_for_tier(BenchTier::Tier2Subsystem);
    targets = bench_bloom_false_positive_rate_small, bench_bloom_false_positive_rate_large
}
criterion_main!(tier2_subsystem_bloom_false_positive_rate);
