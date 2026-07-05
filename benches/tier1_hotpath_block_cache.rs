//! Tier 1 — Hot Path Block Cache Benchmarks
//!
//! Covers single lookups, batch hit/miss probes, and bounded insert patterns.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_midge::sst::cache::{BlockCache, CacheKey, CachePolicyType};
use cntryl_midge::Bytes;
use cntryl_stress::{black_box, stress_main, stress_test, StressContext};
use std::thread;
use std::time::{Duration, Instant};

const INSERT_BATCH_ROUNDS: usize = 4;
const HOT_GET_BATCH_SIZE: usize = 1024;

cntryl_stress::stress_allocator!();

#[inline]
fn make_cache_key(block_offset: u64) -> CacheKey {
    CacheKey::for_data(1, block_offset)
}

#[inline]
fn make_cache_key_with_sst(sst_id: u64, block_offset: u64) -> CacheKey {
    CacheKey::for_data(sst_id, block_offset)
}

fn make_block_data(size: usize) -> Bytes {
    Bytes::from(vec![0xAB; size])
}

fn create_cache(capacity: u64) -> BlockCache {
    BlockCache::new(capacity, 16, CachePolicyType::Lru)
}

fn wait_for_cache_entries(cache: &BlockCache, expected_len: usize) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while cache.len() < expected_len {
        assert!(
            Instant::now() < deadline,
            "cache admission did not settle: expected {expected_len} entries, got {}",
            cache.len()
        );
        thread::yield_now();
    }
}

fn precompute_keys_and_blocks(num_blocks: usize, block_size: usize) -> (Vec<CacheKey>, Vec<Bytes>) {
    let mut keys = Vec::with_capacity(num_blocks);
    let mut blocks = Vec::with_capacity(num_blocks);
    for i in 0..num_blocks {
        let key = make_cache_key_with_sst((i / 100) as u64, (i % 100) as u64 * block_size as u64);
        let block = make_block_data(block_size);
        keys.push(key);
        blocks.push(block);
    }
    (keys, blocks)
}

#[stress_test(
    tier = 1,
    metadata(component = "block_cache", scenario = "get_hot_single_4k")
)]
fn get_hot_single_4k(ctx: &mut StressContext) {
    let cache = create_cache(10 * 1024 * 1024);
    for i in 0..1000 {
        let key = make_cache_key(i * 4096);
        let block = make_block_data(4096);
        cache.put(key, &block);
    }
    wait_for_cache_entries(&cache, 1000);
    let hot_key = make_cache_key(42 * 4096);
    assert!(cache.get(&hot_key).is_some());
    ctx.parameter("block_size", 4096);
    ctx.parameter("batch_size", HOT_GET_BATCH_SIZE);

    stress_config::measure_micro_batch(ctx, HOT_GET_BATCH_SIZE as u64, || {
        let mut hits = 0usize;
        for _ in 0..HOT_GET_BATCH_SIZE {
            hits += usize::from(cache.get(black_box(&hot_key)).is_some());
        }
        black_box(hits);
    });
}

#[stress_test(
    tier = 1,
    metadata(component = "block_cache", scenario = "get_batch_hit_1000")
)]
fn get_batch_hit_1000(ctx: &mut StressContext) {
    let cache_size = 10 * 1024 * 1024;
    let block_size = 4 * 1024;
    let num_blocks = 1000;
    let (keys, blocks) = precompute_keys_and_blocks(num_blocks, block_size);
    let cache = create_cache(cache_size);
    for i in 0..num_blocks {
        cache.put(keys[i], &blocks[i]);
    }
    wait_for_cache_entries(&cache, num_blocks);
    let warmed_hits = keys
        .iter()
        .take(num_blocks)
        .filter(|key| cache.get(key).is_some())
        .count();
    assert_eq!(warmed_hits, num_blocks);
    ctx.parameter("block_size", block_size);
    ctx.parameter("lookup_batch_size", num_blocks);

    stress_config::measure_micro_batch(ctx, num_blocks as u64, || {
        let mut count = 0;
        for key in keys.iter().take(num_blocks) {
            if cache.get(key).is_some() {
                count += 1;
            }
        }
        black_box(count);
    });
}

#[stress_test(
    tier = 1,
    metadata(component = "block_cache", scenario = "get_batch_miss_1000")
)]
fn get_batch_miss_1000(ctx: &mut StressContext) {
    let cache_size = 10 * 1024 * 1024;
    let block_size = 4 * 1024;
    let num_blocks = 1000;
    let (keys, blocks) = precompute_keys_and_blocks(num_blocks, block_size);
    let cache = create_cache(cache_size);
    for i in 0..num_blocks {
        cache.put(keys[i], &blocks[i]);
    }
    wait_for_cache_entries(&cache, num_blocks);

    let miss_keys: Vec<CacheKey> = (0..num_blocks)
        .map(|i| {
            make_cache_key_with_sst((100 + i / 100) as u64, (i % 100) as u64 * block_size as u64)
        })
        .collect();
    ctx.parameter("block_size", block_size);
    ctx.parameter("lookup_batch_size", num_blocks);

    stress_config::measure_micro_batch(ctx, num_blocks as u64, || {
        let mut count = 0;
        for key in &miss_keys {
            if cache.get(black_box(key)).is_some() {
                count += 1;
            }
        }
        black_box(count);
    });
}

#[stress_test(
    tier = 1,
    metadata(component = "block_cache", scenario = "insert_batch_100")
)]
fn insert_batch_100(ctx: &mut StressContext) {
    let cache_size = 10 * 1024 * 1024;
    let block_size = 4 * 1024;
    let num_blocks = 100;
    let (keys, blocks) = precompute_keys_and_blocks(num_blocks, block_size);
    let logical_ops = (num_blocks * INSERT_BATCH_ROUNDS) as u64;
    ctx.parameter("block_size", block_size);
    ctx.parameter("num_blocks", num_blocks);
    ctx.parameter("rounds", INSERT_BATCH_ROUNDS);

    stress_config::measure_micro_batch(ctx, logical_ops, || {
        let cache = create_cache(cache_size);
        for round in 0..INSERT_BATCH_ROUNDS {
            for i in 0..num_blocks {
                let idx = (i + round) % num_blocks;
                cache.put(keys[idx], &blocks[idx]);
            }
        }
        black_box(cache);
    });
}

stress_main!();
