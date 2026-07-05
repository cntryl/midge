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
use cntryl_stress::{black_box, stress_main, stress_test, StressContext};

const PROBE_BATCH_SIZE: usize = 256;
const HASH_MISS_BATCH_SIZE: usize = 512;

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

#[stress_test(
    tier = 1,
    metadata(component = "bloom", scenario = "maybe_contains_hit")
)]
fn maybe_contains_hit(ctx: &mut StressContext) {
    let (filter, keys) = build_filter(100);
    let hit_key = keys[42].as_ref();
    ctx.parameter("probe_batch_size", PROBE_BATCH_SIZE);

    stress_config::measure_micro_batch(ctx, PROBE_BATCH_SIZE as u64, || {
        let mut matches = 0usize;
        for _ in 0..PROBE_BATCH_SIZE {
            let result = filter.contains(black_box(hit_key));
            matches += usize::from(result.might_be_present());
        }
        black_box(matches);
    });
}

#[stress_test(
    tier = 1,
    metadata(component = "bloom", scenario = "maybe_contains_miss")
)]
fn maybe_contains_miss(ctx: &mut StressContext) {
    let (filter, _keys) = build_filter(100);
    let miss_key = b"key_00001000";
    ctx.parameter("probe_batch_size", PROBE_BATCH_SIZE);

    stress_config::measure_micro_batch(ctx, PROBE_BATCH_SIZE as u64, || {
        let mut misses = 0usize;
        for _ in 0..PROBE_BATCH_SIZE {
            let result = filter.contains(black_box(miss_key));
            misses += usize::from(result.definitely_not_present());
        }
        black_box(misses);
    });
}

#[stress_test(
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

    stress_config::measure_micro_batch(ctx, lookup_keys.len() as u64, || {
        let mut count = 0u32;
        for (_is_hit, key) in &lookup_keys {
            if filter.contains(black_box(key)).might_be_present() {
                count += 1;
            }
        }
        black_box(count);
    });
}

#[stress_test(tier = 1, metadata(component = "bloom", scenario = "hashes_via_miss"))]
fn compute_hashes_via_miss(ctx: &mut StressContext) {
    let (filter, _keys) = build_filter(100);
    let miss_keys: Vec<Vec<u8>> = (0..HASH_MISS_BATCH_SIZE)
        .map(|i| format!("miss_hash_probe_{i:010}").into_bytes())
        .collect();
    ctx.parameter("probe_batch_size", HASH_MISS_BATCH_SIZE);

    stress_config::measure_micro_batch(ctx, HASH_MISS_BATCH_SIZE as u64, || {
        let mut misses = 0usize;
        for key in &miss_keys {
            let result = filter.contains(black_box(key.as_slice()));
            misses += usize::from(result.definitely_not_present());
        }
        black_box(misses);
    });
}

stress_main!();
