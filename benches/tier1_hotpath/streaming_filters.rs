//! Tier 1 — Streaming filter hot path benchmarks
//!
//! Covers:
//! - Fast negative filter bit checks (L1-cached bitset)
//! - Per-block bloom (12 bits/key) probe hit/miss
//!
//! Target runtime: <1s total; no allocations in hot path.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use cntryl_midge::sst::{bloom::BloomFilterBuilder, fast_negative_filter::FastNegativeFilter};
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::{criterion_config_for_tier, BenchTier};

fn bench_fast_negative_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_fast_negative_filter");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    // Build filter once: 256 bits for 256 blocks
    let mut filter = FastNegativeFilter::new();
    // Mark blocks [10, 20, 30] as containing data
    for &idx in &[10usize, 20, 30] {
        filter.set_block(idx);
    }

    let hit = 20usize;
    let miss = 99usize;

    group.bench_function("fast_negative_hit", |b| {
        b.iter(|| black_box(filter.might_contain_block(black_box(hit))))
    });

    group.bench_function("fast_negative_miss", |b| {
        b.iter(|| black_box(filter.might_contain_block(black_box(miss))))
    });

    group.finish();
}

fn bench_block_bloom_12_bits(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_block_bloom_12_bits");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    // Prebuild bloom filter with 12 bits per key
    let mut builder = BloomFilterBuilder::with_bits_per_key(12);
    let keys: Vec<Bytes> = (0..256)
        .map(|i| Bytes::from(format!("key_{:06}", i)))
        .collect();
    for k in &keys {
        builder.add_key(k);
    }
    let bloom = builder.finish();

    let hit_key = keys[123].clone();
    let miss_key = Bytes::from_static(b"key_999999");

    group.bench_function("block_bloom_hit", |b| {
        b.iter(|| black_box(bloom.may_contain(black_box(&hit_key))))
    });

    group.bench_function("block_bloom_miss", |b| {
        b.iter(|| black_box(bloom.may_contain(black_box(&miss_key))))
    });

    group.finish();
}

criterion_group!(
    name = streaming_filters_hotpath;
    config = criterion_config_for_tier(BenchTier::Tier1Hot);
    targets = bench_fast_negative_filter, bench_block_bloom_12_bits
);
criterion_main!(streaming_filters_hotpath);
