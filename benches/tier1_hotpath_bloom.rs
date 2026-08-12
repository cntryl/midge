//! Tier 1 — Bloom filter hot path benchmarks
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers bloom filter hot paths:
//! - Hash computation and containment checks
//! - Single key lookups (hit/miss)

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_midge::sst::bloom::writer::BloomFilterOps;
use cntryl_midge::sst::bloom::BloomWriter;
use cntryl_midge::Bytes;
use cntryl_stress::{black_box, stress, stress_main, StressContext};

const PROBE_BATCH_SIZE: usize = 4096;
const MIXED_LOOKUP_REPEATS: usize = 64;
const HASH_MISS_BATCH_SIZE: usize = 4096;

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).expect("benchmark count fits in u64")
}

cntryl_stress::stress_allocator!();

fn build_filter(expected_keys: usize) -> (cntryl_midge::sst::bloom::BloomReader, Vec<Bytes>) {
    let mut builder = BloomWriter::with_defaults(expected_keys);
    let keys: Vec<Bytes> = (0..expected_keys)
        .map(|i| Bytes::from(format!("key_{i:010}")))
        .collect();
    for key in &keys {
        builder.insert(key);
    }
    (builder.finish(), keys)
}

#[stress(
    tier = 1,
    role = "diagnostic",
    metadata(
        component = "bloom",
        scenario = "maybe_contains_hit",
        validated_micro = "true"
    )
)]
fn maybe_contains_hit(ctx: &mut StressContext) {
    let (filter, keys) = build_filter(100);
    ctx.parameter("probe_batch_size", PROBE_BATCH_SIZE);
    ctx.parameter("logical_unit", "bloom_probe");

    stress_config::measure_hot_path_batch(
        ctx,
        "maybe_contains_hit",
        PROBE_BATCH_SIZE as u64,
        || {
            let mut matches = 0usize;
            for i in 0..PROBE_BATCH_SIZE {
                let hit_key = keys[i % keys.len()].as_ref();
                let result = filter.contains(black_box(hit_key));
                matches += usize::from(result.might_be_present());
            }
            black_box(matches);
        },
    );
}

#[stress(
    tier = 1,
    role = "diagnostic",
    metadata(
        component = "bloom",
        scenario = "maybe_contains_miss",
        validated_micro = "true"
    )
)]
fn maybe_contains_miss(ctx: &mut StressContext) {
    let (filter, _keys) = build_filter(100);
    let miss_keys: Vec<Vec<u8>> = (0..PROBE_BATCH_SIZE)
        .map(|i| format!("key_{:010}", 10_000 + i).into_bytes())
        .collect();
    ctx.parameter("probe_batch_size", PROBE_BATCH_SIZE);
    ctx.parameter("logical_unit", "bloom_probe");

    stress_config::measure_hot_path_batch(
        ctx,
        "maybe_contains_miss",
        PROBE_BATCH_SIZE as u64,
        || {
            let mut misses = 0usize;
            for key in &miss_keys {
                let result = filter.contains(black_box(key.as_slice()));
                misses += usize::from(result.definitely_not_present());
            }
            black_box(misses);
        },
    );
}

#[stress(
    tier = 1,
    metadata(component = "bloom", scenario = "batch_mixed_lookup")
)]
fn batch_100_lookups_mixed(ctx: &mut StressContext) {
    let (filter, keys) = build_filter(1000);
    let lookup_keys: Vec<(bool, Vec<u8>)> = (0..100)
        .map(|i| {
            if i % 2 == 0 {
                (true, keys[i * 5].to_vec())
            } else {
                (false, format!("miss_{i:010}").into_bytes())
            }
        })
        .collect();
    ctx.parameter("lookup_count", lookup_keys.len());
    ctx.parameter("lookup_repeats", MIXED_LOOKUP_REPEATS);
    ctx.parameter("logical_unit", "bloom_lookup_batch");
    ctx.parameter("lookups_per_logical_operation", lookup_keys.len());

    stress_config::measure_hot_path_batch(
        ctx,
        "batch_100_lookups_mixed",
        usize_to_u64(MIXED_LOOKUP_REPEATS),
        || {
            let mut count = 0u32;
            for _ in 0..MIXED_LOOKUP_REPEATS {
                for (_is_hit, key) in &lookup_keys {
                    if filter.contains(black_box(key)).might_be_present() {
                        count += 1;
                    }
                }
            }
            black_box(count);
        },
    );
}

#[stress(
    tier = 1,
    role = "diagnostic",
    metadata(
        component = "bloom",
        scenario = "hashes_via_miss",
        validated_micro = "true"
    )
)]
fn compute_hashes_via_miss(ctx: &mut StressContext) {
    let (filter, _keys) = build_filter(100);
    let miss_keys: Vec<Vec<u8>> = (0..HASH_MISS_BATCH_SIZE)
        .map(|i| format!("miss_hash_probe_{i:010}").into_bytes())
        .collect();
    ctx.parameter("probe_batch_size", HASH_MISS_BATCH_SIZE);
    ctx.parameter("logical_unit", "bloom_probe");

    stress_config::measure_hot_path_batch(
        ctx,
        "compute_hashes_via_miss",
        HASH_MISS_BATCH_SIZE as u64,
        || {
            let mut misses = 0usize;
            for key in &miss_keys {
                let result = filter.contains(black_box(key.as_slice()));
                misses += usize::from(result.definitely_not_present());
            }
            black_box(misses);
        },
    );
}

stress_main!();
