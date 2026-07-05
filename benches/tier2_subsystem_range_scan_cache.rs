//! Tier 2 — Range Scan with Cache Warm/Cold
//!
//! Quantifies block-cache behavior for sequential and strided scan shapes.

use cntryl_midge::sst::cache::{BlockCache, CacheKey, CachePolicyType};
use cntryl_midge::Bytes;
use cntryl_stress::{black_box, stress, stress_main, StressContext};

const BLOCK_SIZE: usize = 4096;
const SST_ID: u64 = 1;
const WARM_SCAN_REPEATS: usize = 512;
const WARM_SCAN_REPEAT_OPS: u64 = 512;
const COLD_SCAN_REPEATS: usize = 32;
const COLD_SCAN_REPEAT_OPS: u64 = 32;

struct RangeScan {
    start_block: usize,
    num_blocks: usize,
}

impl RangeScan {
    const fn new(start_block: usize, num_blocks: usize) -> Self {
        Self {
            start_block,
            num_blocks,
        }
    }

    fn execute(&self, cache: &BlockCache, sst_id: u64, miss_block_data: &Bytes) -> (u32, u32) {
        let mut blocks_read = 0u32;
        let mut cache_hits = 0u32;

        for block_idx in self.start_block..(self.start_block + self.num_blocks) {
            let key = CacheKey::for_data(sst_id, (block_idx * BLOCK_SIZE) as u64);
            if cache.get(&key).is_some() {
                cache_hits += 1;
            } else {
                blocks_read += 1;
                cache.put(key, miss_block_data);
            }
        }

        (blocks_read, cache_hits)
    }
}

fn precompute_block_data() -> Bytes {
    Bytes::from_static(&[0xCD; BLOCK_SIZE])
}

fn populate_cache(cache: &BlockCache, sst_id: u64, start_block: usize, num_blocks: usize) {
    let block_data = precompute_block_data();
    for block_idx in start_block..(start_block + num_blocks) {
        let key = CacheKey::for_data(sst_id, (block_idx * BLOCK_SIZE) as u64);
        cache.put(key, &block_data);
    }
}

fn run_warm_scan(ctx: &mut StressContext, scenario: &'static str, num_blocks: usize) {
    let miss_block_data = precompute_block_data();
    let cache = BlockCache::new(10 * 1024 * 1024, 16, CachePolicyType::Lru);
    populate_cache(&cache, SST_ID, 0, num_blocks);
    let scan = RangeScan::new(0, num_blocks);
    ctx.parameter("cache_state", "warm");
    ctx.parameter("num_blocks", num_blocks);
    ctx.parameter("scan_repeats", WARM_SCAN_REPEATS);

    let _completed =
        ctx.measure_batch(scenario, (num_blocks as u64) * WARM_SCAN_REPEAT_OPS, || {
            let mut blocks_read = 0u32;
            let mut cache_hits = 0u32;
            for _ in 0..WARM_SCAN_REPEATS {
                let (read, hits) = scan.execute(&cache, SST_ID, &miss_block_data);
                blocks_read = blocks_read.saturating_add(read);
                cache_hits = cache_hits.saturating_add(hits);
            }
            black_box((blocks_read, cache_hits));
        });
}

fn run_cold_scan(ctx: &mut StressContext, scenario: &'static str, num_blocks: usize) {
    let miss_block_data = precompute_block_data();
    ctx.parameter("cache_state", "cold");
    ctx.parameter("num_blocks", num_blocks);
    ctx.parameter("scan_repeats", COLD_SCAN_REPEATS);

    let _completed =
        ctx.measure_batch(scenario, (num_blocks as u64) * COLD_SCAN_REPEAT_OPS, || {
            let mut blocks_read = 0u32;
            let mut cache_hits = 0u32;
            for _ in 0..COLD_SCAN_REPEATS {
                let cache = BlockCache::new(10 * 1024 * 1024, 16, CachePolicyType::Lru);
                let scan = RangeScan::new(0, num_blocks);
                let (read, hits) = scan.execute(&cache, SST_ID, &miss_block_data);
                blocks_read = blocks_read.saturating_add(read);
                cache_hits = cache_hits.saturating_add(hits);
            }
            black_box((blocks_read, cache_hits));
        });
}

#[stress(
    tier = 2,
    metadata(component = "range_scan_cache", scenario = "warm_10_blocks")
)]
fn warm_10_blocks(ctx: &mut StressContext) {
    run_warm_scan(ctx, "warm_10_blocks", 10);
}

#[stress(
    tier = 2,
    metadata(component = "range_scan_cache", scenario = "warm_100_blocks")
)]
fn warm_100_blocks(ctx: &mut StressContext) {
    run_warm_scan(ctx, "warm_100_blocks", 100);
}

#[stress(
    tier = 2,
    metadata(component = "range_scan_cache", scenario = "warm_1000_blocks")
)]
fn warm_1000_blocks(ctx: &mut StressContext) {
    run_warm_scan(ctx, "warm_1000_blocks", 1000);
}

#[stress(
    tier = 2,
    metadata(component = "range_scan_cache", scenario = "cold_10_blocks")
)]
fn cold_10_blocks(ctx: &mut StressContext) {
    run_cold_scan(ctx, "cold_10_blocks", 10);
}

#[stress(
    tier = 2,
    metadata(component = "range_scan_cache", scenario = "cold_100_blocks")
)]
fn cold_100_blocks(ctx: &mut StressContext) {
    run_cold_scan(ctx, "cold_100_blocks", 100);
}

#[stress(
    tier = 2,
    metadata(component = "range_scan_cache", scenario = "cold_1000_blocks")
)]
fn cold_1000_blocks(ctx: &mut StressContext) {
    run_cold_scan(ctx, "cold_1000_blocks", 1000);
}

#[stress(
    tier = 2,
    metadata(component = "range_scan_cache", scenario = "strided_warm")
)]
fn strided_warm(ctx: &mut StressContext) {
    let block_data = precompute_block_data();
    let stride = 10;
    let num_accesses = 100;
    let cache = BlockCache::new(10 * 1024 * 1024, 16, CachePolicyType::Lru);
    for i in 0..num_accesses {
        let block_idx = i * stride;
        let key = CacheKey::for_data(SST_ID, (block_idx * BLOCK_SIZE) as u64);
        cache.put(key, &block_data);
    }
    ctx.parameter("stride", stride);
    ctx.parameter("num_accesses", num_accesses);
    ctx.parameter("scan_repeats", WARM_SCAN_REPEATS);

    let _completed = ctx.measure_batch(
        "strided_warm",
        (num_accesses as u64) * WARM_SCAN_REPEAT_OPS,
        || {
            let mut cache_hits = 0u32;
            for _ in 0..WARM_SCAN_REPEATS {
                for i in 0..num_accesses {
                    let block_idx = i * stride;
                    let key = CacheKey::for_data(SST_ID, (block_idx * BLOCK_SIZE) as u64);
                    if cache.get(&key).is_some() {
                        cache_hits += 1;
                    }
                }
            }
            black_box(cache_hits);
        },
    );
}

#[stress(
    tier = 2,
    metadata(component = "range_scan_cache", scenario = "strided_cold")
)]
fn strided_cold(ctx: &mut StressContext) {
    let block_data = precompute_block_data();
    let stride = 10;
    let num_accesses = 100;
    ctx.parameter("stride", stride);
    ctx.parameter("num_accesses", num_accesses);
    ctx.parameter("scan_repeats", COLD_SCAN_REPEATS);

    let _completed = ctx.measure_batch(
        "strided_cold",
        (num_accesses as u64) * COLD_SCAN_REPEAT_OPS,
        || {
            let mut blocks_read = 0u32;
            for _ in 0..COLD_SCAN_REPEATS {
                let cache = BlockCache::new(10 * 1024 * 1024, 16, CachePolicyType::Lru);
                for i in 0..num_accesses {
                    let block_idx = i * stride;
                    let key = CacheKey::for_data(SST_ID, (block_idx * BLOCK_SIZE) as u64);
                    if cache.get(&key).is_none() {
                        blocks_read += 1;
                        cache.put(key, &block_data);
                    }
                }
            }
            black_box(blocks_read);
        },
    );
}

stress_main!();
