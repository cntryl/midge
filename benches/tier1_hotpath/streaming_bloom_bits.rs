//! Tier 1 — Bloom bits/key comparison for negative lookups
//!
//! Compares 8 vs 12 bits/key for single negative lookup latency.
//! Target runtime: <1s, Flat sampling, precomputed data.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use cntryl_midge::sst::bloom::BloomFilterBuilder;
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::{criterion_config_for_tier, BenchTier};

fn bench_negative_lookup_bits(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_bloom_bits_negative_lookup");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    // Prebuild two filters with the same keys
    let keys: Vec<Bytes> = (0..512)
        .map(|i| Bytes::from(format!("key_{:06}", i)))
        .collect();

    let mut builder8 = BloomFilterBuilder::with_bits_per_key(8);
    let mut builder12 = BloomFilterBuilder::with_bits_per_key(12);
    for k in &keys {
        builder8.add_key(k);
        builder12.add_key(k);
    }
    let bloom8 = builder8.finish();
    let bloom12 = builder12.finish();

    // Miss key
    let miss_key = Bytes::from_static(b"key_999999");

    group.bench_function("bits8_negative_lookup", |b| {
        b.iter(|| black_box(bloom8.may_contain(black_box(&miss_key))))
    });

    group.bench_function("bits12_negative_lookup", |b| {
        b.iter(|| black_box(bloom12.may_contain(black_box(&miss_key))))
    });

    group.finish();
}

criterion_group!(
    name = streaming_bloom_bits_hotpath;
    config = criterion_config_for_tier(BenchTier::Tier1Hot);
    targets = bench_negative_lookup_bits
);
criterion_main!(streaming_bloom_bits_hotpath);
