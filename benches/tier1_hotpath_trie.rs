//! Tier 1 — Trie Index Hot Path Benchmarks
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers trie index hot paths:
//! - Exact key lookup (find_block)
//! - Prefix range lookup (find_prefix_range)
//! - Hit/miss scenarios at different trie depths

#[path = "./criterion_helper.rs"]
mod criterion_helper;

use cntryl_midge::sst::trie::{TrieBuilder, TrieReader};
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;

/// Benchmark trie exact key lookup (hot path for point reads)
fn bench_trie_find_block(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_trie_find_block");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    // Build trie with 100 keys (simulates small SST)
    let mut builder = TrieBuilder::new();
    for i in 0..100 {
        let key = format!("user:{}:profile", i);
        builder.add_key(key.as_bytes(), i as u32).unwrap();
    }
    let encoded = builder.finish();
    let reader = TrieReader::new(&encoded).unwrap();

    // Precompute keys for different scenarios
    let key_hit = b"user:50:profile"; // Exists in trie
    let key_miss = b"user:999:profile"; // Doesn't exist
    let key_partial = b"user:50:prof"; // Partial match

    group.bench_function("find_hit", |b| {
        b.iter(|| {
            let block_id = reader.find_block(black_box(key_hit));
            black_box(block_id);
        })
    });

    group.bench_function("find_miss", |b| {
        b.iter(|| {
            let block_id = reader.find_block(black_box(key_miss));
            black_box(block_id);
        })
    });

    group.bench_function("find_partial_match", |b| {
        b.iter(|| {
            let block_id = reader.find_block(black_box(key_partial));
            black_box(block_id);
        })
    });

    group.finish();
}

/// Benchmark trie prefix range lookup
fn bench_trie_prefix_range(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_trie_prefix_range");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    // Build trie with hierarchical keys
    let mut builder = TrieBuilder::new();
    for user_id in 0..10 {
        for resource in &["profile", "settings", "prefs"] {
            let key = format!("user:{}:{}", user_id, resource);
            builder.add_key(key.as_bytes(), user_id as u32).unwrap();
        }
    }
    let encoded = builder.finish();
    let reader = TrieReader::new(&encoded).unwrap();

    // Precompute prefixes
    let prefix_single_user = b"user:5:"; // Should match 3 keys
    let prefix_all_users = b"user:"; // Should match many keys
    let prefix_no_match = b"admin:"; // No matches

    group.bench_function("prefix_single_user", |b| {
        b.iter(|| {
            let blocks = reader.find_prefix_range(black_box(prefix_single_user));
            black_box(blocks);
        })
    });

    group.bench_function("prefix_all_users", |b| {
        b.iter(|| {
            let blocks = reader.find_prefix_range(black_box(prefix_all_users));
            black_box(blocks);
        })
    });

    group.bench_function("prefix_no_match", |b| {
        b.iter(|| {
            let blocks = reader.find_prefix_range(black_box(prefix_no_match));
            black_box(blocks);
        })
    });

    group.finish();
}

/// Benchmark trie with different key patterns
fn bench_trie_key_patterns(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_trie_key_patterns");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    // Short keys with high branching
    let mut builder_short = TrieBuilder::new();
    for i in 0..100 {
        let key = format!("k{}", i);
        builder_short.add_key(key.as_bytes(), i as u32).unwrap();
    }
    let encoded_short = builder_short.finish();
    let reader_short = TrieReader::new(&encoded_short).unwrap();

    // Long keys with shared prefix
    let mut builder_long = TrieBuilder::new();
    for i in 0..100 {
        let key = format!("very_long_shared_prefix_key_{:010}", i);
        builder_long.add_key(key.as_bytes(), i as u32).unwrap();
    }
    let encoded_long = builder_long.finish();
    let reader_long = TrieReader::new(&encoded_long).unwrap();

    group.bench_function("short_keys_high_branch", |b| {
        b.iter(|| {
            let block_id = reader_short.find_block(black_box(b"k50"));
            black_box(block_id);
        })
    });

    group.bench_function("long_keys_shared_prefix", |b| {
        b.iter(|| {
            let block_id =
                reader_long.find_block(black_box(b"very_long_shared_prefix_key_0000000050"));
            black_box(block_id);
        })
    });

    group.finish();
}

criterion_group! {
    name = tier1_hotpath_trie;
    config = criterion_config_for_tier(BenchTier::Tier1Hot);
    targets =
        bench_trie_find_block,
        bench_trie_prefix_range,
        bench_trie_key_patterns
}
criterion_main!(tier1_hotpath_trie);
