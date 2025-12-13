//! Block cache module for caching SST blocks
//!
//! Provides a sharded LRU/TinyLFU/CLOCK-Pro cache for SST blocks with:
//! - **Sharding**: 16 independent shards to reduce lock contention
//! - **Pluggable policies**: LRU, TinyLFU, CLOCK-Pro eviction
//! - **Admission control**: Prevent cache pollution from scans
//! - **Metrics**: Hit/miss/eviction tracking per shard

pub mod admission;
pub mod key;
pub mod metrics;
pub mod policy;
pub mod shard;
pub mod value;

pub use admission::AdmissionCounter;
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
    /// Array of shards
    shards: Vec<Arc<CacheShard>>,
    /// Number of shards
    num_shards: usize,
}

impl BlockCache {
    /// Create a new block cache
    ///
    /// `capacity_bytes`: Total cache capacity in bytes
    /// `num_shards`: Number of shards (default 16)
    /// `policy_type`: Eviction policy
    pub fn new(capacity_bytes: u64, num_shards: usize, policy_type: CachePolicyType) -> Self {
        let shard_capacity = capacity_bytes / num_shards as u64;
        let mut shards = Vec::with_capacity(num_shards);

        for _ in 0..num_shards {
            shards.push(Arc::new(CacheShard::new(shard_capacity, policy_type)));
        }

        Self { shards, num_shards }
    }

    /// Create a new block cache with default settings (16 shards, LRU)
    pub fn new_default(capacity_bytes: u64) -> Self {
        Self::new(capacity_bytes, 16, CachePolicyType::Lru)
    }

    /// Get the shard for a key
    fn get_shard(&self, key: &CacheKey) -> &Arc<CacheShard> {
        let shard_idx = key.shard_index(self.num_shards);
        &self.shards[shard_idx]
    }

    /// Get a cached block
    pub fn get(&self, key: &CacheKey) -> Option<CacheValue> {
        self.get_shard(key).get(key)
    }

    /// Insert a block into the cache
    ///
    /// Returns true if inserted, false if rejected by admission control
    pub fn put(&self, key: CacheKey, value: Bytes) -> bool {
        self.get_shard(&key).put(key, value)
    }

    /// Remove a block from the cache
    pub fn remove(&self, key: &CacheKey) -> Option<CacheValue> {
        self.get_shard(key).remove(key)
    }

    /// Clear all entries from all shards
    pub fn clear(&self) {
        for shard in &self.shards {
            shard.clear();
        }
    }

    /// Get aggregated metrics across all shards
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
    pub fn size_bytes(&self) -> u64 {
        self.shards.iter().map(|s| s.size_bytes()).sum()
    }

    /// Get total number of entries
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.len()).sum()
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|s| s.is_empty())
    }

    /// Get number of shards
    pub fn num_shards(&self) -> usize {
        self.num_shards
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_with_shards() {
        // Arrange
        let cache = BlockCache::new(1024 * 1024, 16, CachePolicyType::Lru);

        // Act & Assert
        assert_eq!(cache.num_shards(), 16);
        assert!(cache.is_empty());
    }

    #[test]
    fn should_retrieve_value_after_put() {
        // Arrange
        let cache = BlockCache::new_default(1024 * 1024);
        let key = CacheKey::for_data(1, 0);
        let value = Bytes::from(&b"test_block"[..]);

        // Act
        cache.put(key, value.clone());
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
        cache.put(key, Bytes::from(&b"data"[..]));
        let removed = cache.remove(&key);
        let retrieved = cache.get(&key);

        // Assert
        assert!(removed.is_some());
        assert!(retrieved.is_none());
    }

    #[test]
    fn should_distribute_across_shards() {
        // Arrange
        let cache = BlockCache::new(1024 * 1024, 16, CachePolicyType::Lru);

        // Act
        for sst_id in 0..100 {
            let key = CacheKey::for_data(sst_id, 0);
            cache.put(key, Bytes::from(vec![1u8; 1024]));
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
            cache.put(key, Bytes::from(vec![1u8; 100]));
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
        cache.put(key, Bytes::from(&b"test"[..]));
        cache.get(&key);
        cache.get(&key);
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
        cache.put(CacheKey::for_data(1, 0), Bytes::from(data1));
        cache.put(CacheKey::for_data(2, 0), Bytes::from(data2));

        // Assert - one should be evicted
        let metrics = cache.metrics();
        assert!(metrics.eviction_count() > 0);
    }

    #[test]
    fn should_support_different_policies() {
        // Arrange & Act
        let lru_cache = BlockCache::new(1024, 1, CachePolicyType::Lru);
        let tinylfu_cache = BlockCache::new(1024, 1, CachePolicyType::TinyLfu);
        let clockpro_cache = BlockCache::new(1024, 1, CachePolicyType::ClockPro);

        // Assert - all should be creatable
        assert!(lru_cache.is_empty());
        assert!(tinylfu_cache.is_empty());
        assert!(clockpro_cache.is_empty());
    }
}

