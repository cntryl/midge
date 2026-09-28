//! Tier 2 — Block Cache Subsystem Benchmarks
//!
//! Covers hot set rotation and LRU eviction under pressure.

use cntryl_midge::__internal::sst::cache::{BlockCache, CacheKey, CachePolicyType};
use cntryl_midge::Bytes;
use cntryl_stress::{black_box, stress, stress_main, StressContext};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Barrier;

const EVICTION_REPEATS: usize = 64;
const HOTSET_ROTATION_ROUNDS: usize = 1024;
const CACHE_SHARDS: usize = 16;
const CONCURRENT_CACHE_CAPACITY_BYTES: u64 = 16 * 1024 * 1024;
const CONCURRENT_KEYS_PER_READER: usize = 16;
const CONCURRENT_READS_PER_READER: usize = 65_536;
const SKEWED_HOT_KEYS_PER_READER: usize = 4;

#[derive(Clone, Copy)]
enum ReaderShardDistribution {
    SameShard,
    SeparateShards,
    HashDistributed,
}

impl ReaderShardDistribution {
    const fn name(self) -> &'static str {
        match self {
            Self::SameShard => "same_shard",
            Self::SeparateShards => "separate_shards",
            Self::HashDistributed => "hash_distributed",
        }
    }
}

#[derive(Clone, Copy)]
enum CacheKeyAccessPattern {
    Uniform,
    HotSetSkewed,
}

impl CacheKeyAccessPattern {
    const fn name(self) -> &'static str {
        match self {
            Self::Uniform => "uniform",
            Self::HotSetSkewed => "hot_set_skewed",
        }
    }

    fn key_index(self, read_index: usize, key_count: usize) -> usize {
        match self {
            Self::Uniform => read_index % key_count,
            Self::HotSetSkewed => {
                assert!(key_count > SKEWED_HOT_KEYS_PER_READER);
                if read_index.is_multiple_of(10) {
                    SKEWED_HOT_KEYS_PER_READER
                        + (read_index / 10) % (key_count - SKEWED_HOT_KEYS_PER_READER)
                } else {
                    read_index % SKEWED_HOT_KEYS_PER_READER
                }
            }
        }
    }
}

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

fn keys_for_readers(
    readers: usize,
    shard_count: usize,
    distribution: ReaderShardDistribution,
) -> Vec<Vec<CacheKey>> {
    let mut keys = vec![Vec::with_capacity(CONCURRENT_KEYS_PER_READER); readers];
    let mut candidate = 0_u64;
    let mut next_reader = 0_usize;
    while keys
        .iter()
        .any(|reader_keys| reader_keys.len() < CONCURRENT_KEYS_PER_READER)
    {
        let key = CacheKey::for_data(0, candidate);
        candidate = candidate.wrapping_add(1);
        let shard = key.shard_index(shard_count);
        match distribution {
            ReaderShardDistribution::SameShard => {
                if shard == 0 {
                    let reader = (0..readers)
                        .map(|offset| (next_reader + offset) % readers)
                        .find(|reader| keys[*reader].len() < CONCURRENT_KEYS_PER_READER)
                        .expect("a same-shard reader still needs keys");
                    keys[reader].push(key);
                    next_reader = (reader + 1) % readers;
                }
            }
            ReaderShardDistribution::SeparateShards => {
                for (reader, reader_keys) in keys.iter_mut().enumerate() {
                    if shard == reader && reader_keys.len() < CONCURRENT_KEYS_PER_READER {
                        reader_keys.push(key);
                    }
                }
            }
            ReaderShardDistribution::HashDistributed => {
                if keys[next_reader].len() < CONCURRENT_KEYS_PER_READER {
                    keys[next_reader].push(key);
                }
                next_reader = (next_reader + 1) % readers;
                if keys[next_reader].len() == CONCURRENT_KEYS_PER_READER {
                    next_reader = (0..readers)
                        .find(|reader| keys[*reader].len() < CONCURRENT_KEYS_PER_READER)
                        .unwrap_or(next_reader);
                }
            }
        }
    }
    for (reader, reader_keys) in keys.iter().enumerate() {
        assert_eq!(reader_keys.len(), CONCURRENT_KEYS_PER_READER);
        match distribution {
            ReaderShardDistribution::SameShard => assert!(reader_keys
                .iter()
                .all(|key| key.shard_index(shard_count) == 0)),
            ReaderShardDistribution::SeparateShards => assert!(reader_keys
                .iter()
                .all(|key| key.shard_index(shard_count) == reader)),
            ReaderShardDistribution::HashDistributed => {}
        }
    }
    let mut unique = std::collections::HashSet::new();
    assert!(keys.iter().flatten().all(|key| unique.insert(*key)));
    keys
}

fn measure_concurrent_reads(
    ctx: &mut StressContext,
    readers: usize,
    shard_count: usize,
    distribution: ReaderShardDistribution,
    access_pattern: CacheKeyAccessPattern,
) {
    let keys = keys_for_readers(readers, shard_count, distribution);
    let cache = BlockCache::new(
        CONCURRENT_CACHE_CAPACITY_BYTES,
        shard_count,
        CachePolicyType::Lru,
    );
    let block = Bytes::from_static(&[0xAB; 4096]);
    for reader_keys in &keys {
        for key in reader_keys {
            assert!(
                cache.put(*key, &block),
                "warm cache entry should be admitted"
            );
        }
    }
    for reader_keys in &keys {
        for key in reader_keys {
            assert!(
                cache.get(key).is_some(),
                "warm cache entry should be readable"
            );
        }
    }

    let distribution_name = distribution.name();
    let access_pattern_name = access_pattern.name();
    let row_name = format!(
        "concurrent_reads_{distribution_name}_{access_pattern_name}_{readers}_readers_{shard_count}_shards"
    );
    let logical_operations = u64::try_from(readers)
        .expect("reader count fits in u64")
        .saturating_mul(
            u64::try_from(CONCURRENT_READS_PER_READER).expect("read count fits in u64"),
        );
    let start = Barrier::new(readers + 1);
    let finished = Barrier::new(readers + 1);
    let stop = AtomicBool::new(false);
    let failed = AtomicBool::new(false);
    let completed = std::thread::scope(|scope| {
        for reader_keys in &keys {
            let start = &start;
            let finished = &finished;
            let stop = &stop;
            let failed = &failed;
            let cache = &cache;
            scope.spawn(move || loop {
                start.wait();
                if stop.load(Ordering::Acquire) {
                    break;
                }
                let mut hits = 0_usize;
                for index in 0..CONCURRENT_READS_PER_READER {
                    let key = &reader_keys[access_pattern.key_index(index, reader_keys.len())];
                    hits += usize::from(cache.get(black_box(key)).is_some());
                }
                if hits != CONCURRENT_READS_PER_READER {
                    failed.store(true, Ordering::Release);
                }
                finished.wait();
            });
        }

        let completed = ctx
            .benchmark(row_name)
            .samples(10)
            .logical_unit(cntryl_stress::LogicalUnit::new("cache_block_access"))
            .parameter("reader_count", readers)
            .parameter("cache_shards", shard_count)
            .parameter("cache_capacity_bytes", CONCURRENT_CACHE_CAPACITY_BYTES)
            .parameter("shard_distribution", distribution_name)
            .parameter("access_pattern", access_pattern_name)
            .parameter("keys_per_reader", CONCURRENT_KEYS_PER_READER)
            .parameter("lookups_per_reader", CONCURRENT_READS_PER_READER)
            .parameter("batch_per_logical_operation", 1)
            .measure_batch(logical_operations, || {
                start.wait();
                finished.wait();
                black_box(failed.load(Ordering::Acquire));
            });

        stop.store(true, Ordering::Release);
        start.wait();
        completed
    });
    assert!(
        !failed.load(Ordering::Acquire),
        "every measured lookup must hit"
    );
    black_box(completed);
}

#[stress(
    tier = 2,
    metadata(component = "block_cache", scenario = "concurrent_readers")
)]
fn concurrent_readers(ctx: &mut StressContext) {
    for readers in [1, 2, 4, 8] {
        measure_concurrent_reads(
            ctx,
            readers,
            CACHE_SHARDS,
            ReaderShardDistribution::SameShard,
            CacheKeyAccessPattern::Uniform,
        );
    }
    for readers in [2, 4, 8] {
        measure_concurrent_reads(
            ctx,
            readers,
            CACHE_SHARDS,
            ReaderShardDistribution::SeparateShards,
            CacheKeyAccessPattern::Uniform,
        );
    }
    for shard_count in [16, 32, 64] {
        for readers in [2, 4, 8] {
            measure_concurrent_reads(
                ctx,
                readers,
                shard_count,
                ReaderShardDistribution::HashDistributed,
                CacheKeyAccessPattern::Uniform,
            );
        }
    }
    for shard_count in [16, 32, 64] {
        for readers in [4, 8] {
            measure_concurrent_reads(
                ctx,
                readers,
                shard_count,
                ReaderShardDistribution::HashDistributed,
                CacheKeyAccessPattern::HotSetSkewed,
            );
        }
    }
}

stress_main!();
