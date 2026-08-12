//! Tier 2 — Block Cache Subsystem Benchmarks
//!
//! Covers hot set rotation and LRU eviction under pressure.

use cntryl_midge::sst::cache::{BlockCache, CacheKey, CachePolicyType};
use cntryl_midge::Bytes;
use cntryl_stress::{black_box, stress, stress_main, StressContext};

const EVICTION_REPEATS: usize = 64;
const HOTSET_ROTATION_ROUNDS: usize = 1024;

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

#[stress(
    tier = 2,
    metadata(component = "block_cache", scenario = "hotset_rotation")
)]
fn rotate_50_entries(ctx: &mut StressContext) {
    let keys = PrecomputedKeys::linear(100);
    let block = make_block_data_static();
    ctx.parameter("entries", 50);
    ctx.parameter("rounds", HOTSET_ROTATION_ROUNDS);
    ctx.parameter("logical_unit", "cache_block_access");

    let _completed = ctx.benchmark("hotset_rotation").samples(10).measure_batch(
        (HOTSET_ROTATION_ROUNDS * 50) as u64,
        || {
            let cache = create_cache(1024 * 1024);
            for i in 0..50 {
                cache.put(keys.get_linear(i), &block);
            }

            let mut completed = 0u64;
            for round in 0..HOTSET_ROTATION_ROUNDS {
                for i in 0..50 {
                    let key = keys.get_linear((i + round) % 75);
                    if cache.get(&key).is_none() {
                        cache.put(key, &block);
                    }
                    completed += 1;
                }
            }
            black_box((&cache, completed));
        },
    );
}

#[stress(
    tier = 2,
    metadata(component = "block_cache", scenario = "lru_eviction_10k")
)]
fn evict_10k(ctx: &mut StressContext) {
    let keys = PrecomputedKeys::linear(500 + 10_000 * EVICTION_REPEATS);
    let block = make_block_data_static();
    ctx.parameter("initial_blocks", 500);
    ctx.parameter("insert_blocks", 10_000);
    ctx.parameter("eviction_repeats", EVICTION_REPEATS);
    ctx.parameter("logical_unit", "cache_block_insert");

    let _completed = ctx.benchmark("lru_eviction_10k").samples(10).measure_batch(
        (10_000 * EVICTION_REPEATS) as u64,
        || {
            let cache = create_cache(2 * 1024 * 1024);
            for i in 0..500 {
                cache.put(keys.get_linear(i), &block);
            }
            for repeat in 0..EVICTION_REPEATS {
                let offset = 500 + repeat * 10_000;
                for i in 500..10_500 {
                    cache.put(keys.get_linear(offset + i - 500), &block);
                }
            }
            black_box(cache);
        },
    );
}

stress_main!();
