//! Tier 1 — Trie Index Hot Path Benchmarks
//!
//! Covers exact key lookup, prefix range lookup, and key-shape sensitivity.

use cntryl_midge::sst::trie::{TrieBuilder, TrieReader};
use cntryl_stress::{black_box, stress, stress_main, StressContext};

#[path = "./stress_config.rs"]
mod stress_config;

const FIND_BLOCK_BATCH_SIZE: usize = 65_536;
const TRIE_FINDS_PER_LOGICAL_OPERATION: usize = 32;
const PREFIX_RANGE_BATCH_SIZE: usize = 65_536;
const TRIE_PREFIX_RANGES_PER_LOGICAL_OPERATION: usize = 32;

type FindCase = (Vec<u8>, Option<u32>);

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).expect("benchmark count fits in u64")
}

fn find_logical_operation_count() -> u64 {
    usize_to_u64(FIND_BLOCK_BATCH_SIZE / TRIE_FINDS_PER_LOGICAL_OPERATION)
}

fn prefix_logical_operation_count() -> u64 {
    usize_to_u64(PREFIX_RANGE_BATCH_SIZE / TRIE_PREFIX_RANGES_PER_LOGICAL_OPERATION)
}

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
    cases: &[FindCase],
) {
    for (key, expected) in cases {
        assert_eq!(reader.find_block(key), *expected);
    }
    ctx.parameter("lookup_key_count", cases.len());
    ctx.parameter(
        "finds_per_logical_operation",
        TRIE_FINDS_PER_LOGICAL_OPERATION,
    );
    ctx.parameter("logical_unit", "trie_find_batch");

    stress_config::measure_hot_path_batch(ctx, scenario, find_logical_operation_count(), || {
        let mut found = 0u32;
        for i in 0..FIND_BLOCK_BATCH_SIZE {
            let (key, _) = &cases[i % cases.len()];
            found = found.wrapping_add(
                reader
                    .find_block(black_box(key.as_slice()))
                    .unwrap_or(u32::MAX),
            );
        }
        black_box(found);
    });
}

fn run_find_block(ctx: &mut StressContext, scenario: &'static str, cases: &[FindCase]) {
    let encoded = build_profile_trie();
    let reader = TrieReader::new(&encoded).unwrap();
    ctx.parameter("scenario", scenario);
    measure_find_block(ctx, scenario, &reader, cases);
}

fn profile_hit_cases() -> Vec<FindCase> {
    (0_u32..100)
        .map(|i| (format!("user:{i:03}:profile").into_bytes(), Some(i)))
        .collect()
}

fn profile_miss_cases(suffix: &'static str) -> Vec<FindCase> {
    (0_u32..100)
        .map(|i| (format!("user:{i:03}:{suffix}").into_bytes(), None))
        .collect()
}

#[stress(tier = 1, metadata(component = "trie", scenario = "find_hit"))]
fn find_hit(ctx: &mut StressContext) {
    let cases = profile_hit_cases();
    run_find_block(ctx, "find_hit", &cases);
}

#[stress(tier = 1, metadata(component = "trie", scenario = "find_miss"))]
fn find_miss(ctx: &mut StressContext) {
    let cases = profile_miss_cases("profily");
    run_find_block(ctx, "find_miss", &cases);
}

#[stress(
    tier = 1,
    metadata(component = "trie", scenario = "find_partial_match")
)]
fn find_partial_match(ctx: &mut StressContext) {
    let cases = profile_miss_cases("prof");
    run_find_block(ctx, "find_partial_match", &cases);
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

fn run_prefix_range(ctx: &mut StressContext, scenario: &'static str, prefixes: &[Vec<u8>]) {
    let encoded = build_hierarchical_trie();
    let reader = TrieReader::new(&encoded).unwrap();
    ctx.parameter("scenario", scenario);
    ctx.parameter("prefix_key_count", prefixes.len());
    ctx.parameter(
        "prefix_ranges_per_logical_operation",
        TRIE_PREFIX_RANGES_PER_LOGICAL_OPERATION,
    );
    ctx.parameter("logical_unit", "trie_prefix_range_batch");

    stress_config::measure_hot_path_batch(ctx, scenario, prefix_logical_operation_count(), || {
        let mut total = 0usize;
        for i in 0..PREFIX_RANGE_BATCH_SIZE {
            let prefix = &prefixes[i % prefixes.len()];
            let blocks = reader.find_prefix_range(black_box(prefix.as_slice()));
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
    let prefixes: Vec<Vec<u8>> = (0_u32..10)
        .map(|user_id| format!("user:{user_id:02}:").into_bytes())
        .collect();
    run_prefix_range(ctx, "prefix_single_user", &prefixes);
}

#[stress(tier = 1, metadata(component = "trie", scenario = "prefix_all_users"))]
fn prefix_all_users(ctx: &mut StressContext) {
    let prefixes = vec![b"user:".to_vec()];
    run_prefix_range(ctx, "prefix_all_users", &prefixes);
}

#[stress(tier = 1, metadata(component = "trie", scenario = "prefix_no_match"))]
fn prefix_no_match(ctx: &mut StressContext) {
    let prefixes: Vec<Vec<u8>> = (0_u32..10)
        .map(|user_id| format!("user:{user_id:02}:zzzz").into_bytes())
        .collect();
    run_prefix_range(ctx, "prefix_no_match", &prefixes);
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
    let cases: Vec<FindCase> = (0_u32..100)
        .map(|i| (format!("k{i:03}").into_bytes(), Some(i)))
        .collect();
    measure_find_block(ctx, "short_keys_high_branch", &reader, &cases);
}

#[stress(
    tier = 1,
    metadata(component = "trie", scenario = "long_keys_shared_prefix")
)]
fn long_keys_shared_prefix(ctx: &mut StressContext) {
    let encoded = build_long_key_trie();
    let reader = TrieReader::new(&encoded).unwrap();
    let cases: Vec<FindCase> = (0_u32..100)
        .map(|i| {
            (
                format!("very_long_shared_prefix_key_{i:010}").into_bytes(),
                Some(i),
            )
        })
        .collect();
    measure_find_block(ctx, "long_keys_shared_prefix", &reader, &cases);
}

stress_main!();
