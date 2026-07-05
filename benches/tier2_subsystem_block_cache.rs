//! Tier 2 — Block Cache Subsystem Benchmarks
//!
//! Covers hot set rotation and LRU eviction under pressure.

use cntryl_midge::sst::cache::{BlockCache, CacheKey, CachePolicyType};
use cntryl_midge::Bytes;
use cntryl_stress::{black_box, stress_main, stress_test, StressContext};

const EVICTION_REPEATS: usize = 8;
const INSERT_SINGLE_REPEATS: usize = 4_096;

struct PrecomputedKeys {
    keys: Vec<CacheKey>,
}

impl PrecomputedKeys {
    fn linear(count: usize) -> Self {
        let keys = (0..count)
            .map(|i| CacheKey::for_data(0, (i * 4096) as u64))
            .collect();
        Self { keys }
    }

    #[inline]
    fn get_linear(&self, idx: usize) -> CacheKey {
        self.keys[idx]
    }
}

fn make_block_data_static() -> Bytes {
    Bytes::from_static(&[0xAB; 4096])
}

fn create_cache(capacity: u64) -> BlockCache {
    BlockCache::new(capacity, 16, CachePolicyType::Lru)
}

#[stress_test(
    tier = 2,
    metadata(component = "block_cache", scenario = "insert_single_4k")
)]
fn insert_single_4k(ctx: &mut StressContext) {
    let warm_keys = PrecomputedKeys::linear(100);
    let insert_keys = PrecomputedKeys::linear(INSERT_SINGLE_REPEATS + 1_000);
    let block = make_block_data_static();
    ctx.parameter("block_size", block.len());
    ctx.parameter("initial_blocks", 100);
    ctx.parameter("insert_blocks", INSERT_SINGLE_REPEATS);

    let _completed = ctx.measure_counted(|| {
        let cache = create_cache(100 * 1024 * 1024);
        for i in 0..100 {
            cache.put(warm_keys.get_linear(i), &block);
        }
        for i in 0..INSERT_SINGLE_REPEATS {
            cache.put(insert_keys.get_linear(1_000 + i), black_box(&block));
        }
        black_box(cache);
        INSERT_SINGLE_REPEATS as u64
    });
}

#[stress_test(
    tier = 2,
    metadata(component = "block_cache", scenario = "hotset_rotation")
)]
fn rotate_50_entries(ctx: &mut StressContext) {
    let keys = PrecomputedKeys::linear(100);
    let block = make_block_data_static();
    ctx.parameter("entries", 50);
    ctx.parameter("rounds", 10);

    let _completed = ctx.measure_counted(|| {
        let cache = create_cache(1024 * 1024);
        for i in 0..50 {
            cache.put(keys.get_linear(i), &block);
        }

        let mut completed = 0u64;
        for round in 0..10 {
            for i in 0..50 {
                let key = keys.get_linear((i + round) % 75);
                if cache.get(&key).is_none() {
                    cache.put(key, &block);
                }
                completed += 1;
            }
        }
        black_box(&cache);
        completed
    });
}

#[stress_test(
    tier = 2,
    metadata(component = "block_cache", scenario = "lru_eviction_10k")
)]
fn evict_10k(ctx: &mut StressContext) {
    let keys = PrecomputedKeys::linear(10_500);
    let block = make_block_data_static();
    ctx.parameter("initial_blocks", 500);
    ctx.parameter("insert_blocks", 10_000);
    ctx.parameter("eviction_repeats", EVICTION_REPEATS);

    let _completed = ctx.measure_counted(|| {
        for _ in 0..EVICTION_REPEATS {
            let cache = create_cache(2 * 1024 * 1024);
            for i in 0..500 {
                cache.put(keys.get_linear(i), &block);
            }
            for i in 500..10_500 {
                cache.put(keys.get_linear(i), &block);
            }
            black_box(cache);
        }
        (10_000 * EVICTION_REPEATS) as u64
    });
}

stress_main!();
