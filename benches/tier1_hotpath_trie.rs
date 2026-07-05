//! Tier 1 — Trie Index Hot Path Benchmarks
//!
//! Covers exact key lookup, prefix range lookup, and key-shape sensitivity.

use cntryl_midge::sst::trie::{TrieBuilder, TrieReader};
use cntryl_stress::{black_box, stress, stress_main, StressContext};

#[path = "./stress_config.rs"]
mod stress_config;

const FIND_BLOCK_BATCH_SIZE: usize = 16_384;
const PREFIX_RANGE_BATCH_SIZE: usize = 2048;

fn build_profile_trie() -> Vec<u8> {
    let mut builder = TrieBuilder::new();
    for i in 0_u32..100 {
        let key = format!("user:{i:03}:profile");
        builder.add_key(key.as_bytes(), i).unwrap();
    }
    builder.finish()
}

fn measure_find_block(
    ctx: &mut StressContext,
    scenario: &'static str,
    reader: &TrieReader,
    key: &'static [u8],
    expected: Option<u32>,
) {
    assert_eq!(reader.find_block(key), expected);

    stress_config::measure_hot_path_batch(ctx, scenario, FIND_BLOCK_BATCH_SIZE as u64, || {
        let mut found = 0u32;
        for _ in 0..FIND_BLOCK_BATCH_SIZE {
            found = found.wrapping_add(reader.find_block(black_box(key)).unwrap_or(u32::MAX));
        }
        black_box(found);
    });
}

fn run_find_block(
    ctx: &mut StressContext,
    scenario: &'static str,
    key: &'static [u8],
    expected: Option<u32>,
) {
    let encoded = build_profile_trie();
    let reader = TrieReader::new(&encoded).unwrap();
    ctx.parameter("scenario", scenario);
    measure_find_block(ctx, scenario, &reader, key, expected);
}

#[stress(tier = 1, metadata(component = "trie", scenario = "find_hit"))]
fn find_hit(ctx: &mut StressContext) {
    run_find_block(ctx, "find_hit", b"user:050:profile", Some(50));
}

#[stress(tier = 1, metadata(component = "trie", scenario = "find_miss"))]
fn find_miss(ctx: &mut StressContext) {
    run_find_block(ctx, "find_miss", b"user:050:profily", None);
}

#[stress(
    tier = 1,
    metadata(component = "trie", scenario = "find_partial_match")
)]
fn find_partial_match(ctx: &mut StressContext) {
    run_find_block(ctx, "find_partial_match", b"user:050:prof", None);
}

fn build_hierarchical_trie() -> Vec<u8> {
    let mut builder = TrieBuilder::new();
    for user_id in 0_u32..10 {
        for resource in &["prefs", "profile", "settings"] {
            let key = format!("user:{user_id:02}:{resource}");
            builder.add_key(key.as_bytes(), user_id).unwrap();
        }
    }
    builder.finish()
}

fn run_prefix_range(ctx: &mut StressContext, scenario: &'static str, prefix: &'static [u8]) {
    let encoded = build_hierarchical_trie();
    let reader = TrieReader::new(&encoded).unwrap();
    ctx.parameter("scenario", scenario);

    stress_config::measure_hot_path_batch(ctx, scenario, PREFIX_RANGE_BATCH_SIZE as u64, || {
        let mut total = 0usize;
        for _ in 0..PREFIX_RANGE_BATCH_SIZE {
            let blocks = reader.find_prefix_range(black_box(prefix));
            total = total.wrapping_add(blocks.len());
        }
        black_box(total);
    });
}

#[stress(
    tier = 1,
    metadata(component = "trie", scenario = "prefix_single_user")
)]
fn prefix_single_user(ctx: &mut StressContext) {
    run_prefix_range(ctx, "prefix_single_user", b"user:05:");
}

#[stress(tier = 1, metadata(component = "trie", scenario = "prefix_all_users"))]
fn prefix_all_users(ctx: &mut StressContext) {
    run_prefix_range(ctx, "prefix_all_users", b"user:");
}

#[stress(tier = 1, metadata(component = "trie", scenario = "prefix_no_match"))]
fn prefix_no_match(ctx: &mut StressContext) {
    run_prefix_range(ctx, "prefix_no_match", b"user:09:zzzz");
}

fn build_short_key_trie() -> Vec<u8> {
    let mut builder = TrieBuilder::new();
    for i in 0_u32..100 {
        let key = format!("k{i:03}");
        builder.add_key(key.as_bytes(), i).unwrap();
    }
    builder.finish()
}

fn build_long_key_trie() -> Vec<u8> {
    let mut builder = TrieBuilder::new();
    for i in 0_u32..100 {
        let key = format!("very_long_shared_prefix_key_{i:010}");
        builder.add_key(key.as_bytes(), i).unwrap();
    }
    builder.finish()
}

#[stress(
    tier = 1,
    metadata(component = "trie", scenario = "short_keys_high_branch")
)]
fn short_keys_high_branch(ctx: &mut StressContext) {
    let encoded = build_short_key_trie();
    let reader = TrieReader::new(&encoded).unwrap();
    measure_find_block(ctx, "short_keys_high_branch", &reader, b"k050", Some(50));
}

#[stress(
    tier = 1,
    metadata(component = "trie", scenario = "long_keys_shared_prefix")
)]
fn long_keys_shared_prefix(ctx: &mut StressContext) {
    let encoded = build_long_key_trie();
    let reader = TrieReader::new(&encoded).unwrap();
    measure_find_block(
        ctx,
        "long_keys_shared_prefix",
        &reader,
        b"very_long_shared_prefix_key_0000000050",
        Some(50),
    );
}

stress_main!();
