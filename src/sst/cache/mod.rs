//! Block cache module for caching SST blocks
//!
//! Provides a sharded LRU/TinyLFU/CLOCK-Pro cache for SST blocks with:
//! - **Sharding**: 16 independent shards to reduce lock contention
//! - **Pluggable policies**: LRU, `TinyLFU`, CLOCK-Pro eviction
//! - **Admission utilities**: Optional frequency helpers are available, but the default
//!   insertion path does not enforce a separate second-access admission gate.
//! - **Metrics**: Hit/miss/eviction tracking per shard
//!
//! Point-read paths populate the block cache synchronously. Range-scan paths
//! that use contiguous readahead avoid one-pass scan pollution by bypassing
//! cache insertion entirely.

pub mod admission;
pub mod key;
pub mod metrics;
pub mod policy;
pub mod shard;
pub mod value;

pub use key::{BlockType, CacheKey};
pub use metrics::CacheMetrics;
pub use policy::{CachePolicy, CachePolicyType};
pub use shard::CacheShard;
pub use value::CacheValue;

use bytes::Bytes;
use std::sync::Arc;

/// Sharded block cache
///
/// Divides cache into independent shards to reduce lock contention.
/// Each shard manages its own entries with its own eviction policy.
pub struct BlockCache {
    /// Array of shards shared by cache callers
    shards: Vec<Arc<CacheShard>>,
    /// Number of shards
    num_shards: usize,
    /// Eviction policy used for all shards.
    #[cfg(test)]
    policy_type: CachePolicyType,
}

impl BlockCache {
    /// Create a new block cache
    ///
    /// `capacity_bytes`: Total cache capacity in bytes
    /// `num_shards`: Number of shards (default 16)
    /// `policy_type`: Eviction policy
    #[must_use]
    pub fn new(capacity_bytes: u64, num_shards: usize, policy_type: CachePolicyType) -> Self {
        let requested_shards = num_shards.max(1);
        let num_shards = if capacity_bytes == 0 {
            1
        } else {
            let capacity_limited_shards = usize::try_from(capacity_bytes).unwrap_or(usize::MAX);
            requested_shards.min(capacity_limited_shards.max(1))
        };
        let shard_capacity = capacity_bytes / num_shards as u64;
        let mut shards = Vec::with_capacity(num_shards);

        for _ in 0..num_shards {
            shards.push(CacheShard::new(shard_capacity, policy_type));
        }

        Self {
            shards,
            num_shards,
            #[cfg(test)]
            policy_type,
        }
    }

    /// Create a new block cache with default settings (16 shards, LRU)
    #[must_use]
    pub fn new_default(capacity_bytes: u64) -> Self {
        Self::new(capacity_bytes, 16, CachePolicyType::Lru)
    }

    /// Get the shard for a key
    fn get_shard(&self, key: &CacheKey) -> &Arc<CacheShard> {
        let shard_idx = key.shard_index(self.num_shards);
        &self.shards[shard_idx]
    }

    /// Get a cached block
    #[must_use]
    pub fn get(&self, key: &CacheKey) -> Option<CacheValue> {
        self.get_shard(key).get(key)
    }

    /// Insert a block into the cache.
    ///
    /// Returns true if inserted and immediately visible in the cache.
    pub fn put(&self, key: CacheKey, value: &Bytes) -> bool {
        self.get_shard(&key).put(key, value)
    }

    /// Remove a block from the cache
    #[must_use]
    pub fn remove(&self, key: &CacheKey) -> Option<CacheValue> {
        self.get_shard(key).remove(key)
    }

    /// Remove all cached blocks belonging to one SST.
    pub fn remove_sst(&self, sst_id: u64) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.remove_sst(sst_id))
            .sum()
    }

    /// Clear all entries from all shards
    pub fn clear(&self) {
        for shard in &self.shards {
            shard.clear();
        }
    }

    /// Get aggregated metrics across all shards
    #[must_use]
    pub fn metrics(&self) -> CacheMetrics {
        let mut total_hits = 0u64;
        let mut total_misses = 0u64;
        let mut total_evictions = 0u64;
        let mut total_memory = 0u64;

        for shard in &self.shards {
            let m = shard.metrics();
            total_hits += m.hit_count();
            total_misses += m.miss_count();
            total_evictions += m.eviction_count();
            total_memory += m.memory_bytes();
        }

        let aggregated = CacheMetrics::new();
        aggregated
            .hits
            .store(total_hits, std::sync::atomic::Ordering::Relaxed);
        aggregated
            .misses
            .store(total_misses, std::sync::atomic::Ordering::Relaxed);
        aggregated
            .evictions
            .store(total_evictions, std::sync::atomic::Ordering::Relaxed);
        aggregated
            .memory_bytes
            .store(total_memory, std::sync::atomic::Ordering::Relaxed);
        aggregated
    }

    /// Get total size in bytes
    #[must_use]
    pub fn size_bytes(&self) -> u64 {
        self.shards.iter().map(|s| s.size_bytes()).sum()
    }

    /// Get total number of entries
    #[must_use]
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.len()).sum()
    }

    /// Check if cache is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|s| s.is_empty())
    }

    /// Get number of shards
    #[must_use]
    pub fn num_shards(&self) -> usize {
        self.num_shards
    }

    #[cfg(test)]
    pub(crate) fn policy_type(&self) -> CachePolicyType {
        self.policy_type
    }
}

impl Drop for BlockCache {
    fn drop(&mut self) {
        let drop_start = std::time::Instant::now();
        tracing::trace!(
            shards = self.num_shards,
            "BlockCache dropping, cleaning up shards"
        );

        // Explicitly drop shards in sequence to ensure deterministic cleanup.
        self.shards.clear();

        tracing::trace!(
            elapsed_ms = drop_start.elapsed().as_millis(),
            "BlockCache cleanup complete"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_with_shards() {
        // Arrange
        let cache = BlockCache::new(1024 * 1024, 16, CachePolicyType::Lru);

        // Act

        // Assert
        assert_eq!(cache.num_shards(), 16);
        assert!(cache.is_empty());
    }

    #[test]
    fn should_use_single_shard_when_zero_shards_requested() {
        // Arrange
        let cache = BlockCache::new(1024, 0, CachePolicyType::Lru);
        let key = CacheKey::for_data(1, 0);
        let value = Bytes::from(&b"data"[..]);

        // Act
        let inserted = cache.put(key, &value);
        let retrieved = cache.get(&key);

        // Assert
        assert!(inserted);
        assert_eq!(cache.num_shards(), 1);
        assert!(retrieved.is_some());
    }

    #[test]
    fn should_not_create_zero_byte_shards_when_capacity_is_less_than_requested_shards() {
        // Arrange
        let cache = BlockCache::new(2, 16, CachePolicyType::Lru);
        let key = CacheKey::for_data(1, 0);
        let value = Bytes::from(vec![7u8; 1]);

        // Act
        let inserted = cache.put(key, &value);

        // Assert
        assert_eq!(cache.num_shards(), 2);
        assert!(inserted);
        assert!(cache.get(&key).is_some());
    }

    #[test]
    fn should_retrieve_value_after_first_data_block_put_without_prior_admission() {
        // Arrange
        let cache = BlockCache::new_default(1024 * 1024);
        let key = CacheKey::for_data(1, 0);
        let value = Bytes::from(&b"test_block"[..]);

        // Act
        assert!(cache.put(key, &value));
        let retrieved = cache.get(&key);

        // Assert
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().data.to_vec(), value.to_vec());
    }

    #[test]
    fn should_remove_entry() {
        // Arrange
        let cache = BlockCache::new_default(1024 * 1024);
        let key = CacheKey::for_data(1, 0);

        // Act
        assert!(cache.put(key, &Bytes::from(&b"data"[..])));
        let removed = cache.remove(&key);
        let retrieved = cache.get(&key);

        // Assert
        assert!(removed.is_some());
        assert!(retrieved.is_none());
    }

    #[test]
    fn should_remove_all_blocks_for_sst_from_every_shard() {
        // Arrange
        let cache = BlockCache::new(1024 * 1024, 16, CachePolicyType::Lru);
        let target_data = CacheKey::for_data(42, 0);
        let target_index = CacheKey::for_index(42, 64);
        let other_data = CacheKey::for_data(43, 0);
        let value = Bytes::from(vec![1u8; 128]);
        assert!(cache.put(target_data, &value));
        assert!(cache.put(target_index, &value));
        assert!(cache.put(other_data, &value));

        // Act
        let removed = cache.remove_sst(42);

        // Assert
        assert_eq!(removed, 2);
        assert!(cache.get(&target_data).is_none());
        assert!(cache.get(&target_index).is_none());
        assert!(cache.get(&other_data).is_some());
    }

    #[test]
    fn should_distribute_across_shards() {
        // Arrange
        let cache = BlockCache::new(1024 * 1024, 16, CachePolicyType::Lru);

        // Act
        for sst_id in 0..100 {
            let key = CacheKey::for_data(sst_id, 0);
            assert!(cache.put(key, &Bytes::from(vec![1u8; 1024])));
        }

        // Assert - entries should be distributed
        let mut non_empty_shards = 0;
        for shard in &cache.shards {
            if !shard.is_empty() {
                non_empty_shards += 1;
            }
        }
        assert!(non_empty_shards > 1); // Should use multiple shards
    }

    #[test]
    fn should_clear_all_entries() {
        // Arrange
        let cache = BlockCache::new_default(1024 * 1024);
        for i in 0..10 {
            let key = CacheKey::for_data(i, 0);
            assert!(cache.put(key, &Bytes::from(vec![1u8; 100])));
        }

        // Act
        cache.clear();

        // Assert
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.size_bytes(), 0);
    }

    #[test]
    fn should_track_metrics() {
        // Arrange
        let cache = BlockCache::new_default(1024 * 1024);
        let key = CacheKey::for_data(1, 0);

        // Act
        assert!(cache.put(key, &Bytes::from(&b"test"[..])));
        let _ = cache.get(&key);
        let _ = cache.get(&key);
        let _ = cache.get(&CacheKey::for_data(999, 999));

        // Assert
        let metrics = cache.metrics();
        assert_eq!(metrics.hit_count(), 2);
        assert_eq!(metrics.miss_count(), 1);
    }

    #[test]
    fn should_respect_capacity_per_shard() {
        // Arrange
        let cache = BlockCache::new(100, 1, CachePolicyType::Lru);
        let data1 = vec![b'x'; 60];
        let data2 = vec![b'y'; 60];

        // Act
        assert!(cache.put(CacheKey::for_data(1, 0), &Bytes::from(data1)));
        assert!(cache.put(CacheKey::for_data(2, 0), &Bytes::from(data2)));

        // Assert - one should be evicted
        let metrics = cache.metrics();
        assert!(metrics.eviction_count() > 0);
    }

    #[test]
    fn should_support_different_policies() {
        // Arrange

        // Act
        let lru_cache = BlockCache::new(1024, 1, CachePolicyType::Lru);
        let tinylfu_cache = BlockCache::new(1024, 1, CachePolicyType::TinyLfu);
        let clockpro_cache = BlockCache::new(1024, 1, CachePolicyType::ClockPro);

        // Assert - all should be creatable
        assert!(lru_cache.is_empty());
        assert!(tinylfu_cache.is_empty());
        assert!(clockpro_cache.is_empty());
    }

    #[test]
    fn should_enforce_capacity_for_each_cache_policy() {
        // Arrange
        let policies = [
            CachePolicyType::Lru,
            CachePolicyType::TinyLfu,
            CachePolicyType::ClockPro,
        ];

        for policy in policies {
            let cache = BlockCache::new(90, 1, policy);

            // Act
            for sst_id in 0..6 {
                let byte = u8::try_from(sst_id).expect("fixture sst id should fit in u8");
                assert!(cache.put(CacheKey::for_data(sst_id, 0), &Bytes::from(vec![byte; 32])));
            }

            // Assert
            let metrics = cache.metrics();
            assert!(
                metrics.memory_bytes() > 0,
                "{policy:?} should admit cache entries"
            );
            assert!(
                metrics.memory_bytes() <= 90,
                "{policy:?} should evict until within capacity"
            );
            assert!(
                metrics.eviction_count() > 0,
                "{policy:?} should record evictions under pressure"
            );
        }
    }

    #[test]
    fn should_remain_safe_under_concurrent_cache_mutations() {
        // Arrange
        let capacity = 64 * 1024;
        let cache = Arc::new(BlockCache::new(capacity, 16, CachePolicyType::Lru));
        let mut handles = Vec::new();
        let thread_count = 8u64;
        let iterations_per_thread = 250u64;
        let clear_interval = 73u64;
        let missing_remove_interval = 31u64;

        // Act
        for thread_id in 0u64..thread_count {
            let cache = Arc::clone(&cache);
            handles.push(std::thread::spawn(move || {
                for offset in 0u64..iterations_per_thread {
                    let key = CacheKey::for_data(thread_id, offset);
                    let byte = u8::try_from(thread_id).expect("thread id should fit in u8");
                    let size = usize::try_from((offset % 7) + 1).expect("size should fit in usize");
                    let data = Bytes::from(vec![byte; size]);
                    let _ = cache.put(key, &data);
                    let _ = cache.get(&key);

                    if offset.is_multiple_of(2) {
                        let _ =
                            cache.remove(&CacheKey::for_data(thread_id, offset.saturating_sub(1)));
                    }
                    if offset.is_multiple_of(missing_remove_interval) {
                        let missing_key = CacheKey::for_data(9_999, offset);
                        let _ = cache.remove(&missing_key);
                    }
                    if offset.is_multiple_of(clear_interval) {
                        cache.clear();
                    }
                }
            }));
        }
        for handle in handles {
            handle
                .join()
                .expect("cache operation thread should complete");
        }

        // Assert
        assert!(cache.size_bytes() <= capacity);
        assert_eq!(cache.size_bytes(), cache.metrics().memory_bytes());
    }
}
