//! Tier 1 — Per-block bloom filter hot path benchmarks
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers per-block bloom filter hot paths:
//! - Bloom query (hit vs miss — no I/O cost)
//! - Batch queries (realistic read path)
//! - Hash computation cost (via miss probes)

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use cntryl_midge::sst::block_meta::BlockBloom;
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;

/// Benchmark per-block bloom query (hit vs miss)
///
/// Measures the fast path: bloom says "maybe present" vs "definitely absent"
/// This is the critical path for negative lookups (skip block I/O on miss).
fn bench_per_block_bloom_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_per_block_bloom_query");
    group.measurement_time(std::time::Duration::from_millis(200));
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    // Build bloom once (outside hot loop)
    let mut bloom = BlockBloom::new(4096);
    let keys: Vec<Vec<u8>> = (0..100)
        .map(|i| format!("key_{:010}", i).into_bytes())
        .collect();
    for key in &keys {
        bloom.add(key);
    }

    // Precompute hit/miss keys (avoid allocation in hot path)
    let hit_key = keys[42].clone();
    let miss_key = b"key_00001000".to_vec();

    group.bench_function("query_hit", |b| {
        b.iter(|| black_box(bloom.maybe_contains(black_box(&hit_key))))
    });

    group.bench_function("query_miss", |b| {
        b.iter(|| black_box(bloom.maybe_contains(black_box(&miss_key))))
    });

    group.finish();
}

/// Benchmark per-block bloom batch queries (realistic read path)
///
/// Simulates the read path: iterate blocks, check bloom, skip if "definitely absent".
/// Realistic mix of hits (would read block) and misses (skip I/O).
fn bench_per_block_bloom_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_per_block_bloom_batch");
    group.measurement_time(std::time::Duration::from_millis(200));
    group.sampling_mode(SamplingMode::Flat);

    // Build a larger bloom (more realistic)
    let mut bloom = BlockBloom::new(4096);
    let keys: Vec<Vec<u8>> = (0..1000)
        .map(|i| format!("key_{:010}", i).into_bytes())
        .collect();
    for key in &keys {
        bloom.add(key);
    }

    // Precompute lookup keys (mix of hits and misses, precomputed outside hot loop)
    let lookup_keys: Vec<Vec<u8>> = (0..100)
        .map(|i| {
            if i % 2 == 0 {
                keys[i * 5].clone() // hit
            } else {
                format!("miss_{:010}", i).into_bytes() // miss
            }
        })
        .collect();

    group.throughput(Throughput::Elements(100));

    group.bench_function("batch_100_queries_mixed", |b| {
        b.iter(|| {
            let mut skipped = 0u32;
            for key in &lookup_keys {
                if !bloom.maybe_contains(black_box(key)) {
                    skipped += 1; // Bloom says definitely absent → skip block I/O
                }
            }
            black_box(skipped)
        })
    });

    group.finish();
}

/// Benchmark hash computation isolated (via bloom query on miss)
///
/// Isolates the hash function cost. Measured via miss queries (all misses = pure hash cost).
fn bench_per_block_bloom_hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_per_block_bloom_hash");
    group.measurement_time(std::time::Duration::from_millis(200));
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    // Build bloom (precomputed outside hot loop)
    let mut bloom = BlockBloom::new(4096);
    let keys: Vec<Vec<u8>> = (0..100)
        .map(|i| format!("key_{:010}", i).into_bytes())
        .collect();
    for key in &keys {
        bloom.add(key);
    }

    // Precompute miss key
    let miss_key = b"key_00001000".to_vec();

    group.bench_function("hash_via_miss_query", |b| {
        b.iter(|| black_box(bloom.maybe_contains(black_box(&miss_key))))
    });

    group.finish();
}

criterion_group! {
    name = tier1_hotpath_per_block_bloom;
    config = criterion_config_for_tier(BenchTier::Tier1Hot);
    targets = bench_per_block_bloom_query, bench_per_block_bloom_batch, bench_per_block_bloom_hash
}
criterion_main!(tier1_hotpath_per_block_bloom);
