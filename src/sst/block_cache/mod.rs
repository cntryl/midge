//! World-class block cache for Midge.
//!
//! This module provides a sharded, size-aware, policy-driven block cache with:
//! - High hit rates via WTinyLFU-style eviction and admission control.
//! - Predictable latency through per-shard locking.
//! - Bounded memory with configurable capacity.
//! - Per-column-family accounting hooks.
//!
//! # Usage
//!
//! ```ignore
//! use midge::sst::block_cache::{BlockCache, BlockCacheOptions, ShardedBlockCache};
//!
//! let cache = ShardedBlockCache::new(BlockCacheOptions::with_capacity(128 * 1024 * 1024));
//! // Use cache.get() / cache.insert() from SST readers.
//! ```

pub mod admission;
pub mod config;
pub mod handle;
pub mod key;
pub mod metrics;
pub mod policy;
pub mod shard;
pub mod table;
pub mod value;

// ─── Public re-exports ───────────────────────────────────────────────────────

pub use config::{BlockCacheOptions, EvictionPolicy, SizeAccounting};
pub use handle::BlockHandle;
pub use key::{BlockKey, BlockKind};
pub use value::BlockData;

// ShardedBlockCache is exported after it's defined below

// ─── Stats ───────────────────────────────────────────────────────────────────

// Number of BlockKind variants for array sizing.
const NUM_BLOCK_KINDS: usize = 5;

/// Aggregated statistics for the block cache.
#[derive(Debug, Clone)]
pub struct BlockCacheStats {
    /// Number of cache hits.
    pub hits: u64,
    /// Number of cache misses.
    pub misses: u64,
    /// Number of entries evicted to make room.
    pub evictions: u64,
    /// Number of blocks admitted into the cache.
    pub admissions: u64,
    /// Number of blocks rejected by admission control (colder than victim).
    pub rejected: u64,
    /// Current bytes used.
    pub used_bytes: usize,
    /// Total capacity in bytes.
    pub capacity_bytes: usize,
    /// Per-BlockKind hit counts, indexed by BlockKind as u8.
    /// Order: [Data, Index, Filter, Meta, CompressionDict].
    pub hits_by_kind: [u64; NUM_BLOCK_KINDS],
    /// Per-BlockKind miss counts, indexed by BlockKind as u8.
    /// Order: [Data, Index, Filter, Meta, CompressionDict].
    pub misses_by_kind: [u64; NUM_BLOCK_KINDS],
}

impl Default for BlockCacheStats {
    fn default() -> Self {
        Self {
            hits: 0,
            misses: 0,
            evictions: 0,
            admissions: 0,
            rejected: 0,
            used_bytes: 0,
            capacity_bytes: 0,
            hits_by_kind: [0; NUM_BLOCK_KINDS],
            misses_by_kind: [0; NUM_BLOCK_KINDS],
        }
    }
}

impl BlockCacheStats {
    /// Compute the hit rate as a fraction in [0, 1].
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }

    /// Compute the hit rate for a specific block kind.
    pub fn hit_rate_by_kind(&self, kind: BlockKind) -> f64 {
        let idx = kind.as_u8() as usize;
        let total = self.hits_by_kind[idx] + self.misses_by_kind[idx];
        if total == 0 {
            0.0
        } else {
            self.hits_by_kind[idx] as f64 / total as f64
        }
    }

    /// Get the number of hits for a specific block kind.
    pub fn hits_for_kind(&self, kind: BlockKind) -> u64 {
        self.hits_by_kind[kind.as_u8() as usize]
    }

    /// Get the number of misses for a specific block kind.
    pub fn misses_for_kind(&self, kind: BlockKind) -> u64 {
        self.misses_by_kind[kind.as_u8() as usize]
    }
}

// ─── Trait ───────────────────────────────────────────────────────────────────

/// The public block cache interface.
///
/// All cache implementations (sharded, single-shard, mock) implement this trait.
pub trait BlockCache: Send + Sync {
    /// Lookup a block by key. Returns a pinned handle on hit.
    fn get(&self, key: &BlockKey) -> Option<BlockHandle>;

    /// Insert a block into the cache, returning a pinned handle.
    ///
    /// If the block is rejected by admission control, a handle is still
    /// returned but it will not be backed by the cache (`is_pinned() == false`).
    fn insert(&self, key: BlockKey, data: BlockData) -> BlockHandle;

    /// Insert a block only if it is not already present.
    ///
    /// If the key exists, returns a handle to the existing entry.
    fn insert_if_absent(&self, key: BlockKey, data: BlockData) -> BlockHandle;

    /// Total capacity in bytes.
    fn capacity_bytes(&self) -> usize;

    /// Current bytes used.
    fn used_bytes(&self) -> usize;

    /// Retrieve aggregated statistics.
    fn stats(&self) -> BlockCacheStats;

    /// Hint that a block will be needed soon (optional prefetch).
    ///
    /// The default implementation is a no-op.
    fn prefetch(&self, _key: BlockKey) {}
}

// ─── Sharded Block Cache ─────────────────────────────────────────────────────

use policy::{LruPolicy, Policy, WTinyLfuPolicy};
use shard::{BlockCacheShard, ShardStats};
pub use shard::CfCacheStats;

/// A sharded block cache that distributes entries across multiple shards.
///
/// Each shard has its own mutex, reducing contention under concurrent access.
/// Keys are assigned to shards based on their hash value.
pub struct ShardedBlockCache {
    shards: Box<[BlockCacheShard]>,
    shard_mask: usize,
    capacity_bytes: usize,
    per_cf_stats_enabled: bool,
}

impl ShardedBlockCache {
    /// Create a new sharded block cache with the given options.
    pub fn new(options: BlockCacheOptions) -> Self {
        let num_shards = options.num_shards;
        let capacity_per_shard = options.capacity_per_shard();
        let expected_entries_per_shard = capacity_per_shard / 4096; // assume 4KB avg block
        let per_cf_stats_enabled = options.per_cf_stats;

        let shards: Vec<BlockCacheShard> = (0..num_shards)
            .map(|i| {
                let policy: Box<dyn Policy + Send> = match options.eviction_policy {
                    EvictionPolicy::Lru | EvictionPolicy::Clock => {
                        Box::new(LruPolicy::new(expected_entries_per_shard))
                    }
                    EvictionPolicy::WTinyLfu => {
                        Box::new(WTinyLfuPolicy::new(expected_entries_per_shard))
                    }
                };
                BlockCacheShard::new(
                    i as u32,
                    capacity_per_shard,
                    options.size_accounting,
                    policy,
                    per_cf_stats_enabled,
                )
            })
            .collect();

        Self {
            shards: shards.into_boxed_slice(),
            shard_mask: num_shards - 1,
            capacity_bytes: options.capacity_bytes,
            per_cf_stats_enabled,
        }
    }

    /// Get the shard index for a given key.
    #[inline]
    fn shard_index(&self, key: &BlockKey) -> usize {
        (key.shard_hash() as usize) & self.shard_mask
    }

    /// Get a reference to the shard for a given key.
    #[inline]
    fn shard_for(&self, key: &BlockKey) -> &BlockCacheShard {
        &self.shards[self.shard_index(key)]
    }

    /// Aggregate stats from all shards.
    fn aggregate_stats(&self) -> ShardStats {
        let mut total = ShardStats::default();
        for shard in self.shards.iter() {
            total.merge(&shard.stats());
        }
        total
    }

    /// Get aggregated statistics for a specific column family.
    ///
    /// Returns `None` if per-CF stats are not enabled.
    pub fn cf_stats(&self, cf_id: u32) -> Option<CfCacheStats> {
        if !self.per_cf_stats_enabled {
            return None;
        }
        let mut total = CfCacheStats::default();
        for shard in self.shards.iter() {
            if let Some(stats) = shard.cf_stats(cf_id) {
                total.hits += stats.hits;
                total.misses += stats.misses;
                total.used_bytes += stats.used_bytes;
                total.entry_count += stats.entry_count;
            }
        }
        Some(total)
    }

    /// Get aggregated statistics for all column families.
    ///
    /// Returns `None` if per-CF stats are not enabled.
    pub fn all_cf_stats(&self) -> Option<std::collections::HashMap<u32, CfCacheStats>> {
        if !self.per_cf_stats_enabled {
            return None;
        }
        let mut total: std::collections::HashMap<u32, CfCacheStats> = std::collections::HashMap::new();
        for shard in self.shards.iter() {
            if let Some(shard_cf_stats) = shard.all_cf_stats() {
                for (cf_id, stats) in shard_cf_stats {
                    let entry = total.entry(cf_id).or_default();
                    entry.hits += stats.hits;
                    entry.misses += stats.misses;
                    entry.used_bytes += stats.used_bytes;
                    entry.entry_count += stats.entry_count;
                }
            }
        }
        Some(total)
    }
}

impl BlockCache for ShardedBlockCache {
    fn get(&self, key: &BlockKey) -> Option<BlockHandle> {
        self.shard_for(key).get(key)
    }

    fn insert(&self, key: BlockKey, data: BlockData) -> BlockHandle {
        self.shard_for(&key).insert(key, data)
    }

    fn insert_if_absent(&self, key: BlockKey, data: BlockData) -> BlockHandle {
        self.shard_for(&key).insert_if_absent(key, data)
    }

    fn capacity_bytes(&self) -> usize {
        self.capacity_bytes
    }

    fn used_bytes(&self) -> usize {
        self.shards.iter().map(|s| s.used_bytes()).sum()
    }

    fn stats(&self) -> BlockCacheStats {
        self.aggregate_stats().to_cache_stats()
    }

    fn prefetch(&self, _key: BlockKey) {
        // TODO: implement async prefetch
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_key(file: u64, offset: u64) -> BlockKey {
        BlockKey::new(file, offset, BlockKind::Data, 0)
    }

    fn make_data(size: usize) -> BlockData {
        BlockData::uncompressed(vec![0u8; size].into(), BlockKind::Data)
    }

    #[test]
    fn should_create_cache_given_options_when_new_called() {
        let cache = ShardedBlockCache::new(BlockCacheOptions::with_capacity(1024 * 1024));

        assert_eq!(cache.capacity_bytes(), 1024 * 1024);
        assert_eq!(cache.used_bytes(), 0);
    }

    #[test]
    fn should_insert_and_get_given_block_when_cached() {
        let cache = ShardedBlockCache::new(BlockCacheOptions::with_capacity(1024 * 1024));
        let key = make_key(1, 0);

        let _handle = cache.insert(key, make_data(100));
        let retrieved = cache.get(&key);

        assert!(retrieved.is_some());
    }

    #[test]
    fn should_return_none_given_missing_key_when_get_called() {
        let cache = ShardedBlockCache::new(BlockCacheOptions::with_capacity(1024 * 1024));
        let key = make_key(999, 0);

        let result = cache.get(&key);

        assert!(result.is_none());
    }

    #[test]
    fn should_distribute_keys_given_multiple_inserts_when_sharded() {
        let cache = ShardedBlockCache::new(
            BlockCacheOptions::with_capacity(1024 * 1024).num_shards(4),
        );

        // Insert keys that should land in different shards
        for i in 0..100 {
            cache.insert(make_key(i, 0), make_data(100));
        }

        let stats = cache.stats();
        assert_eq!(stats.admissions, 100);
        assert_eq!(stats.used_bytes, 100 * 100);
    }

    #[test]
    fn should_aggregate_stats_given_multiple_shards_when_stats_called() {
        let cache = ShardedBlockCache::new(
            BlockCacheOptions::with_capacity(1024 * 1024).num_shards(4),
        );

        for i in 0..10 {
            let key = make_key(i, 0);
            cache.insert(key, make_data(100));
            cache.get(&key); // Hit
        }
        cache.get(&make_key(999, 0)); // Miss

        let stats = cache.stats();
        assert_eq!(stats.hits, 10);
        assert_eq!(stats.misses, 1);
    }

    #[test]
    fn should_dedup_given_concurrent_insert_when_insert_if_absent_called() {
        let cache = ShardedBlockCache::new(BlockCacheOptions::with_capacity(1024 * 1024));
        let key = make_key(1, 0);

        let h1 = cache.insert(key, make_data(100));
        let h2 = cache.insert_if_absent(key, make_data(200));

        // Both should return the same data (first insert wins)
        assert_eq!(h1.data().bytes().len(), h2.data().bytes().len());
    }
}
