//! Tier 2 — Bloom Build Benchmarks
//!
//! Measures bloom filter construction throughput for deterministic keysets.

use cntryl_midge::sst::bloom::BloomWriter;
use cntryl_stress::{black_box, stress_main, stress_test, StressContext};

fn make_test_keys(count: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|i| format!("key_{i:010}").into_bytes())
        .collect()
}

fn run_bloom_build(ctx: &mut StressContext, count: usize) {
    let keys = make_test_keys(count);
    ctx.parameter("key_count", count);

    let _completed = ctx.measure_counted(|| {
        let mut builder = BloomWriter::with_defaults(count);
        for key in &keys {
            builder.insert(key);
        }
        black_box(builder.finish());
        count as u64
    });
}

#[stress_test(tier = 2, metadata(component = "bloom", scenario = "build_10k_keys"))]
fn build_10k_keys(ctx: &mut StressContext) {
    run_bloom_build(ctx, 10_000);
}

#[stress_test(tier = 2, metadata(component = "bloom", scenario = "build_100k_keys"))]
fn build_100k_keys(ctx: &mut StressContext) {
    run_bloom_build(ctx, 100_000);
}

#[stress_test(tier = 2, metadata(component = "bloom", scenario = "build_1m_keys"))]
fn build_1m_keys(ctx: &mut StressContext) {
    run_bloom_build(ctx, 1_000_000);
}

stress_main!();
