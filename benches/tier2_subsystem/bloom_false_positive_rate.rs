//! Tier 2 — Bloom false positive rate benchmark
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Measures actual false positive rates for bloom filters

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::criterion_config;
use std::hint::black_box;

use cntryl_midge::sst::bloom::BloomFilterBuilder;

/// Benchmark bloom false positive rate for small filter (1k keys)
/// Measures FPR by querying non-existent keys
fn bench_bloom_false_positive_rate_small(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_bloom_false_positive_rate_small");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(10_000));

    group.bench_function("fpp_small", |b| {
        b.iter(|| {
            // Build bloom filter with 1k keys, target FPR ~1%
            let mut builder = BloomFilterBuilder::with_expected_keys(1_000, 100);
            
            // Insert 1k keys
            for i in 0..1_000 {
                let key = format!("key_{:06}", i);
                builder.add_key(key.as_bytes());
            }
            
            let filter = builder.finish();
            
            // Query 10k non-existent keys (offset range to avoid true positives)
            let mut false_positives = 0;
            for i in 100_000..110_000 {
                let key = format!("key_{:06}", i);
                if filter.may_contain(key.as_bytes()) {
                    false_positives += 1;
                }
            }
            
            // Calculate FPR
            let fpr = false_positives as f64 / 10_000.0;
            black_box(fpr);
        })
    });

    group.finish();
}

/// Benchmark bloom false positive rate for large filter (100k keys)
/// Larger scale FPR measurement with lower target rate
fn bench_bloom_false_positive_rate_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_bloom_false_positive_rate_large");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(50_000));

    group.bench_function("fpp_large", |b| {
        b.iter(|| {
            // Build bloom filter with 100k keys, target FPR ~0.1%
            let mut builder = BloomFilterBuilder::with_expected_keys(100_000, 1000);
            
            // Insert 100k keys
            for i in 0..100_000 {
                let key = format!("key_{:06}", i);
                builder.add_key(key.as_bytes());
            }
            
            let filter = builder.finish();
            
            // Query 50k non-existent keys
            let mut false_positives = 0;
            for i in 1_000_000..1_050_000 {
                let key = format!("key_{:06}", i);
                if filter.may_contain(key.as_bytes()) {
                    false_positives += 1;
                }
            }
            
            // Calculate FPR
            let fpr = false_positives as f64 / 50_000.0;
            black_box(fpr);
        })
    });

    group.finish();
}

criterion_group! {
    name = bloom_false_positive_rate_group;
    config = criterion_config();
    targets = bench_bloom_false_positive_rate_small, bench_bloom_false_positive_rate_large
}
criterion_main!(bloom_false_positive_rate_group);