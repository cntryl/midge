//! Tier 2 — Bloom Build Benchmarks
//!
//! Measures bloom filter construction throughput for deterministic keysets.

use cntryl_midge::sst::bloom::BloomWriter;
use cntryl_stress::{black_box, stress, stress_main, StressContext};

fn make_test_keys(count: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|i| format!("key_{i:010}").into_bytes())
        .collect()
}

fn run_bloom_build(ctx: &mut StressContext, scenario: &'static str, count: usize) {
    let keys = make_test_keys(count);
    ctx.parameter("key_count", count);
    ctx.parameter("logical_unit", "bloom_key_insert");

    let _completed = ctx.measure_batch(scenario, count as u64, || {
        let mut builder = BloomWriter::with_defaults(count);
        for key in &keys {
            builder.insert(key);
        }
        black_box(builder.finish());
    });
}

#[stress(tier = 2, metadata(component = "bloom", scenario = "build_10k_keys"))]
fn build_10k_keys(ctx: &mut StressContext) {
    run_bloom_build(ctx, "build_10k_keys", 10_000);
}

#[stress(tier = 2, metadata(component = "bloom", scenario = "build_100k_keys"))]
fn build_100k_keys(ctx: &mut StressContext) {
    run_bloom_build(ctx, "build_100k_keys", 100_000);
}

#[stress(tier = 2, metadata(component = "bloom", scenario = "build_1m_keys"))]
fn build_1m_keys(ctx: &mut StressContext) {
    run_bloom_build(ctx, "build_1m_keys", 1_000_000);
}

stress_main!();
