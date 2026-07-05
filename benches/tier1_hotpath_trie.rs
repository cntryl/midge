//! Tier 1 — Trie Index Hot Path Benchmarks
//!
//! Covers exact key lookup, prefix range lookup, and key-shape sensitivity.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_midge::sst::trie::{TrieBuilder, TrieReader};
use cntryl_stress::{black_box, stress_main, stress_test, StressContext};

const TRIE_PREFIX_BATCH_SIZE: usize = 256;

cntryl_stress::stress_allocator!();

fn build_profile_trie() -> Vec<u8> {
    let mut builder = TrieBuilder::new();
    for i in 0_u32..100 {
        let key = format!("user:{i:03}:profile");
        builder.add_key(key.as_bytes(), i).unwrap();
    }
    builder.finish()
}

fn run_find_block(ctx: &mut StressContext, scenario: &'static str, key: &'static [u8]) {
    let encoded = build_profile_trie();
    let reader = TrieReader::new(&encoded).unwrap();
    ctx.parameter("scenario", scenario);

    ctx.measure_micro(|| {
        let block_id = reader.find_block(black_box(key));
        black_box(block_id);
    });
}

#[stress_test(tier = 1, metadata(component = "trie", scenario = "find_hit"))]
fn find_hit(ctx: &mut StressContext) {
    run_find_block(ctx, "find_hit", b"user:050:profile");
}

#[stress_test(tier = 1, metadata(component = "trie", scenario = "find_miss"))]
fn find_miss(ctx: &mut StressContext) {
    run_find_block(ctx, "find_miss", b"user:999:profile");
}

#[stress_test(
    tier = 1,
    metadata(component = "trie", scenario = "find_partial_match")
)]
fn find_partial_match(ctx: &mut StressContext) {
    run_find_block(ctx, "find_partial_match", b"user:050:prof");
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
    ctx.parameter("prefix_batch_size", TRIE_PREFIX_BATCH_SIZE);

    stress_config::measure_micro_batch(ctx, TRIE_PREFIX_BATCH_SIZE as u64, || {
        let mut total = 0usize;
        for _ in 0..TRIE_PREFIX_BATCH_SIZE {
            let blocks = reader.find_prefix_range(black_box(prefix));
            total = total.wrapping_add(blocks.len());
        }
        black_box(total);
    });
}

#[stress_test(
    tier = 1,
    metadata(component = "trie", scenario = "prefix_single_user")
)]
fn prefix_single_user(ctx: &mut StressContext) {
    run_prefix_range(ctx, "prefix_single_user", b"user:05:");
}

#[stress_test(tier = 1, metadata(component = "trie", scenario = "prefix_all_users"))]
fn prefix_all_users(ctx: &mut StressContext) {
    run_prefix_range(ctx, "prefix_all_users", b"user:");
}

#[stress_test(tier = 1, metadata(component = "trie", scenario = "prefix_no_match"))]
fn prefix_no_match(ctx: &mut StressContext) {
    run_prefix_range(ctx, "prefix_no_match", b"admin:");
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

#[stress_test(
    tier = 1,
    metadata(component = "trie", scenario = "short_keys_high_branch")
)]
fn short_keys_high_branch(ctx: &mut StressContext) {
    let encoded = build_short_key_trie();
    let reader = TrieReader::new(&encoded).unwrap();

    ctx.measure_micro(|| {
        let block_id = reader.find_block(black_box(b"k050"));
        black_box(block_id);
    });
}

#[stress_test(
    tier = 1,
    metadata(component = "trie", scenario = "long_keys_shared_prefix")
)]
fn long_keys_shared_prefix(ctx: &mut StressContext) {
    let encoded = build_long_key_trie();
    let reader = TrieReader::new(&encoded).unwrap();

    ctx.measure_micro(|| {
        let block_id = reader.find_block(black_box(b"very_long_shared_prefix_key_0000000050"));
        black_box(block_id);
    });
}

stress_main!();
