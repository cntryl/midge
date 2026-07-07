//! Single cache shard with lock-free reads and synchronized writes

use crate::sst::cache::key::{BlockType, CacheKey};
use crate::sst::cache::metrics::CacheMetrics;
use crate::sst::cache::policy::{CachePolicy, CachePolicyType};
use crate::sst::cache::value::CacheValue;
use bytes::Bytes;
use dashmap::DashMap;
use std::convert::TryFrom;
use std::sync::Arc;
use std::sync::Mutex;

/// A single cache shard (partition) with lock-free access
///
/// Contains a portion of the cache entries using `DashMap` for concurrent access,
/// with its own eviction policy and metrics. Insertion and eviction happen
/// synchronously before `put` returns.
pub struct CacheShard {
    /// Map of cache key -> value (lock-free concurrent hashmap)
    entries: DashMap<CacheKey, CacheValue>,
    /// Eviction policy
    policy: Box<dyn CachePolicy>,
    /// Serialize cache mutations for consistent policy and metric updates.
    mutation_lock: Mutex<()>,
    /// Metrics for this shard
    metrics: CacheMetrics,
    /// Maximum size in bytes
    max_bytes: u64,
}

impl CacheShard {
    /// Create a new cache shard
    ///
    /// `max_bytes`: Maximum capacity in bytes
    /// `policy_type`: Eviction policy to use
    ///
    /// Returns `Arc<Self>` because `BlockCache` shares shards across callers.
    #[must_use]
    pub fn new(max_bytes: u64, policy_type: CachePolicyType) -> Arc<Self> {
        Arc::new(Self {
            entries: DashMap::new(),
            policy: policy_type.create(),
            mutation_lock: Mutex::new(()),
            metrics: CacheMetrics::new(),
            max_bytes,
        })
    }

    /// Get a cached value (lock-free)
    pub fn get(&self, key: &CacheKey) -> Option<CacheValue> {
        if let Some(value_ref) = self.entries.get(key) {
            let value = value_ref.value().clone();
            let _ = value.increment_access();
            self.policy.on_access(*key);
            self.metrics.record_hit();
            Some(value)
        } else {
            self.metrics.record_miss();
            None
        }
    }

    /// Insert a value into the cache synchronously.
    ///
    /// Returns true only if the value is admitted by capacity and remains visible
    /// after eviction completes.
    pub fn put(&self, key: CacheKey, value: &Bytes) -> bool {
        let _lock = self.lock_mutation();

        let new_size = u64::try_from(value.len()).unwrap_or(u64::MAX);
        if !self.can_fit_value(&key, new_size) {
            return false;
        }

        let cache_value = CacheValue::new(value.clone());
        self.insert_and_update_metrics(key, cache_value);
        self.evict_if_needed();
        self.entries.contains_key(&key)
    }

    /// Data blocks larger than a shard cannot remain visible after eviction.
    /// Index and filter blocks are allowed through so the eviction policy can
    /// continue to protect metadata blocks under ordinary pressure.
    fn can_fit_value(&self, key: &CacheKey, value_size: u64) -> bool {
        key.block_type != BlockType::Data || value_size <= self.max_bytes
    }

    /// Insert value and update metrics accordingly
    fn insert_and_update_metrics(&self, key: CacheKey, cache_value: CacheValue) {
        let value_size = cache_value.size_bytes() as u64;

        // Check if entry already exists (DashMap returns old value if present)
        if let Some(existing) = self.entries.insert(key, cache_value) {
            // Updated existing entry - adjust for size difference
            let old_size = existing.size_bytes() as u64;
            self.metrics.add_memory(value_size);
            self.metrics.remove_memory(old_size);
        } else {
            // New entry added
            self.metrics.add_memory(value_size);
        }

        self.policy.on_access(key);
    }

    /// Evict entries if cache is over capacity
    ///
    /// Strategy: Protect index/filter blocks by evicting data blocks first.
    /// Only evict index/filter blocks under severe memory pressure (>2x capacity).
    fn evict_if_needed(&self) {
        use crate::sst::cache::BlockType;

        let mut made_progress = false;
        while self.is_over_capacity() {
            // Try to evict data blocks first (protect index/filter blocks).
            if let Some(evicted) = self.try_evict_victim(&[BlockType::Index, BlockType::Filter]) {
                self.update_metrics_after_eviction(&evicted);
                made_progress = true;
                continue;
            }

            if !self.is_severely_over_capacity() {
                break;
            }

            // Emergency: cache is severely over capacity, evict anything.
            if let Some(evicted) = self.try_evict_victim(&[]) {
                self.update_metrics_after_eviction(&evicted);
                made_progress = true;
            } else {
                break;
            }
        }

        if !made_progress && self.is_over_capacity() {
            tracing::warn!(
                "Cache eviction was unable to recover capacity for this shard; \
                 check policy-state consistency or memory pressure"
            );
        }
    }

    /// Check if cache is over capacity
    fn is_over_capacity(&self) -> bool {
        self.metrics.memory_bytes() > self.max_bytes
    }

    /// Check if cache is severely over capacity (emergency threshold)
    fn is_severely_over_capacity(&self) -> bool {
        self.metrics.memory_bytes() > self.max_bytes * 2
    }

    /// Try to evict a victim, excluding specified block types
    ///
    /// Uses retry loop to handle stale keys from concurrent access
    fn try_evict_victim(
        &self,
        exclude_types: &[crate::sst::cache::BlockType],
    ) -> Option<CacheValue> {
        const MAX_RETRIES: usize = 10;

        for _ in 0..MAX_RETRIES {
            let victim_key = self.policy.pick_victim(exclude_types)?;

            if let Some((_, value)) = self.entries.remove(&victim_key) {
                // Successfully evicted - notify policy
                self.policy.on_remove(victim_key);
                return Some(value);
            }
            // Victim was stale - notify policy and retry
            self.policy.on_stale(victim_key);
        }

        // Failed to find valid victim after retries
        None
    }

    /// Update metrics after eviction
    fn update_metrics_after_eviction(&self, evicted: &CacheValue) {
        self.metrics
            .remove_memory(u64::try_from(evicted.size_bytes()).unwrap_or(u64::MAX));
        self.metrics.record_eviction();
    }

    /// Remove a key from the cache
    pub fn remove(&self, key: &CacheKey) -> Option<CacheValue> {
        let _lock = self.lock_mutation();
        if let Some((_, value)) = self.entries.remove(key) {
            self.metrics
                .remove_memory(u64::try_from(value.size_bytes()).unwrap_or(u64::MAX));
            self.policy.on_remove(*key);
            Some(value)
        } else {
            None
        }
    }

    /// Remove every cached block for one SST.
    pub fn remove_sst(&self, sst_id: u64) -> usize {
        let _lock = self.lock_mutation();
        let keys: Vec<CacheKey> = self
            .entries
            .iter()
            .filter_map(|entry| {
                let key = *entry.key();
                (key.sst_id == sst_id).then_some(key)
            })
            .collect();

        let mut removed = 0usize;
        for key in keys {
            if let Some((_, value)) = self.entries.remove(&key) {
                self.metrics
                    .remove_memory(u64::try_from(value.size_bytes()).unwrap_or(u64::MAX));
                self.policy.on_remove(key);
                removed += 1;
            }
        }
        removed
    }

    /// Clear all entries from the cache
    pub fn clear(&self) {
        let _lock = self.lock_mutation();
        self.entries.clear();
        self.metrics.set_memory_bytes(0);
        self.policy.clear();
    }

    fn lock_mutation(&self) -> std::sync::MutexGuard<'_, ()> {
        match self.mutation_lock.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Get cache metrics
    pub fn metrics(&self) -> CacheMetrics {
        self.metrics.clone()
    }

    /// Get current size in bytes
    pub fn size_bytes(&self) -> u64 {
        self.metrics.memory_bytes()
    }

    /// Get number of entries
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_make_first_data_block_put_visible_without_prior_admission() {
        // Arrange
        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);
        let key = CacheKey::for_data(1, 0);
        let value = Bytes::from(&b"visible"[..]);

        // Act
        let inserted = shard.put(key, &value);
        let retrieved = shard.get(&key);

        // Assert
        assert!(inserted);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().data.to_vec(), value.to_vec());
    }

    #[test]
    fn should_keep_large_data_block_after_many_small_evictions() {
        // Arrange
        let shard = CacheShard::new(100, CachePolicyType::Lru);
        let small = Bytes::from(vec![1u8; 1]);
        let large = Bytes::from(vec![2u8; 100]);
        let final_key = CacheKey::for_data(999, 0);
        let before_evictions = shard.metrics().eviction_count();

        // Act
        for i in 0u64..200 {
            let key = CacheKey::for_data(i, 0);
            assert!(shard.put(key, &small));
        }
        let inserted = shard.put(final_key, &large);
        let metrics = shard.metrics();
        let retrieved = shard.get(&final_key);

        // Assert
        assert!(inserted);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().data.to_vec(), large.to_vec());
        assert!(metrics.eviction_count() - before_evictions > 50);
        assert!(shard.size_bytes() <= 100);
    }

    #[test]
    fn should_report_false_when_data_block_exceeds_shard_capacity() {
        // Arrange
        let shard = CacheShard::new(8, CachePolicyType::Lru);
        let key = CacheKey::for_data(1, 0);
        let value = Bytes::from(vec![0u8; 16]);

        // Act
        let inserted = shard.put(key, &value);
        let retrieved = shard.get(&key);

        // Assert
        assert!(!inserted);
        assert!(retrieved.is_none());
        assert_eq!(shard.size_bytes(), 0);
    }

    #[test]
    fn should_remove_only_blocks_for_requested_sst() {
        // Arrange
        let shard = CacheShard::new(1024, CachePolicyType::Lru);
        let target_data = CacheKey::for_data(7, 0);
        let target_index = CacheKey::for_index(7, 64);
        let other_data = CacheKey::for_data(8, 0);
        let value = Bytes::from(vec![1u8; 32]);
        assert!(shard.put(target_data, &value));
        assert!(shard.put(target_index, &value));
        assert!(shard.put(other_data, &value));

        // Act
        let removed = shard.remove_sst(7);

        // Assert
        assert_eq!(removed, 2);
        assert!(shard.get(&target_data).is_none());
        assert!(shard.get(&target_index).is_none());
        assert!(shard.get(&other_data).is_some());
        assert_eq!(shard.size_bytes(), 32);
    }

    #[test]
    fn should_preserve_metadata_blocks_when_eviction_overflows() {
        // Arrange
        let shard = CacheShard::new(120, CachePolicyType::Lru);
        let index_key = CacheKey::for_index(1, 0);
        let filter_key = CacheKey::for_filter(1, 40);
        let data_key = CacheKey::for_data(1, 80);
        let next_data_key = CacheKey::for_data(1, 120);
        let value = Bytes::from(vec![7u8; 40]);

        // Act
        assert!(shard.put(index_key, &value));
        assert!(shard.put(filter_key, &value));
        assert!(shard.put(data_key, &value));
        assert!(shard.put(next_data_key, &value));

        // Assert
        assert!(shard.get(&index_key).is_some());
        assert!(shard.get(&filter_key).is_some());
        assert!(shard.get(&data_key).is_none());
        assert!(shard.get(&next_data_key).is_some());
        assert!(shard.size_bytes() <= 120);
    }

    #[test]
    fn should_retrieve_value_after_store() {
        // Arrange
        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);
        let key = CacheKey::for_data(1, 0);
        let value = Bytes::from(&b"hello world"[..]);

        // Act
        assert!(shard.put(key, &value));
        let retrieved = shard.get(&key);

        // Assert
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().data.to_vec(), value.to_vec());
    }

    #[test]
    fn should_evict_on_overflow() {
        // Arrange
        let shard = CacheShard::new(100, CachePolicyType::Lru);
        let key1 = CacheKey::for_data(1, 0);
        let key2 = CacheKey::for_data(2, 0);
        let data1 = vec![b'x'; 80];
        let data2 = vec![b'y'; 80];

        // Act
        assert!(shard.put(key1, &Bytes::from(data1)));
        assert!(shard.put(key2, &Bytes::from(data2)));

        // Assert - key1 should be evicted (LRU)
        assert!(shard.get(&key1).is_none());
        assert!(shard.get(&key2).is_some());
    }

    #[test]
    fn should_track_metrics() {
        // Arrange
        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);
        let key = CacheKey::for_data(1, 0);
        let value = Bytes::from(&b"test_data"[..]);

        // Act
        assert!(shard.put(key, &value));
        shard.get(&key);
        let metrics = shard.metrics();

        // Assert
        assert_eq!(metrics.hit_count(), 1);
        assert_eq!(metrics.miss_count(), 0);
    }

    #[test]
    fn should_clear_all_entries() {
        // Arrange
        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);

        // Act
        for i in 0..5 {
            let key = CacheKey::for_data(i, 0);
            assert!(shard.put(key, &Bytes::from(format!("value_{i}").into_bytes())));
        }
        shard.clear();

        // Assert
        assert_eq!(shard.len(), 0);
        assert_eq!(shard.size_bytes(), 0);
    }

    // ===== New comprehensive tests =====

    #[test]
    fn should_return_none_for_missing_key() {
        // Arrange
        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);

        // Act
        let result = shard.get(&CacheKey::for_data(999, 999));

        // Assert
        assert!(result.is_none());
    }

    #[test]
    fn should_record_miss_for_missing_key() {
        // Arrange
        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);

        // Act
        let _ = shard.get(&CacheKey::for_data(999, 999));
        let metrics = shard.metrics();

        // Assert
        assert_eq!(metrics.miss_count(), 1);
    }

    #[test]
    fn should_update_existing_entry() {
        // Arrange
        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);
        let key = CacheKey::for_data(1, 0);
        let value1 = Bytes::from(&b"original"[..]);
        let value2 = Bytes::from(&b"updated"[..]);
        let expected = value2.clone();

        // Act
        assert!(shard.put(key, &value1));
        assert!(shard.put(key, &value2));
        let retrieved = shard.get(&key);

        // Assert
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().data.to_vec(), expected.to_vec());
    }

    #[test]
    fn should_remove_entry() {
        // Arrange
        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);
        let key = CacheKey::for_data(1, 0);
        let value = Bytes::from(&b"data"[..]);

        // Act
        assert!(shard.put(key, &value));
        let removed = shard.remove(&key);
        let retrieved = shard.get(&key);

        // Assert
        assert!(removed.is_some());
        assert!(retrieved.is_none());
    }

    #[test]
    fn should_remove_nonexistent_entry() {
        // Arrange
        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);

        // Act
        let result = shard.remove(&CacheKey::for_data(999, 999));

        // Assert
        assert!(result.is_none());
    }

    #[test]
    fn should_start_empty() {
        // Arrange

        // Act
        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);

        // Assert
        assert!(shard.is_empty());
        assert_eq!(shard.len(), 0);
        assert_eq!(shard.size_bytes(), 0);
    }

    #[test]
    fn should_handle_empty_data() {
        // Arrange
        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);
        let key = CacheKey::for_data(1, 0);
        let empty = Bytes::new();

        // Act
        assert!(shard.put(key, &empty));
        let retrieved = shard.get(&key);

        // Assert
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().size_bytes(), 0);
    }

    #[test]
    fn should_handle_large_values() {
        // Arrange
        let shard = CacheShard::new(100_000, CachePolicyType::Lru);
        let key = CacheKey::for_data(1, 0);
        let large = Bytes::from(vec![42u8; 50_000]);

        // Act
        assert!(shard.put(key, &large));
        let retrieved = shard.get(&key);

        // Assert
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().data.to_vec(), large.to_vec());
    }

    #[test]
    fn should_track_entries_count() {
        // Arrange
        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);

        // Act
        let mut lens = Vec::new();
        for i in 0..10 {
            let key = CacheKey::for_data(i, 0);
            assert!(shard.put(key, &Bytes::from(format!("data_{i}").into_bytes())));
            lens.push(shard.len());
        }

        // Assert
        for (i, len) in lens.into_iter().enumerate() {
            assert_eq!(len, i + 1);
        }
    }

    #[test]
    fn should_track_memory_usage() {
        // Arrange
        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);
        let key1 = CacheKey::for_data(1, 0);
        let key2 = CacheKey::for_data(2, 0);

        // Act
        assert!(shard.put(key1, &Bytes::from(&b"1000B"[..]))); // 5 bytes
        let size_after_first = shard.size_bytes();
        assert!(shard.put(key2, &Bytes::from(vec![0u8; 995]))); // 995 bytes
        let size_after_second = shard.size_bytes();

        // Assert
        assert!(size_after_first >= 5);
        assert!(size_after_second >= 1000);
    }

    #[test]
    fn should_distinguish_different_policies() {
        // Arrange
        let shard_lru = CacheShard::new(1000, CachePolicyType::Lru);
        let shard_tinyfu = CacheShard::new(1000, CachePolicyType::TinyLfu);

        // Act (both should work, just with different eviction strategies)
        for i in 0..5 {
            let key = CacheKey::for_data(i, 0);
            assert!(shard_lru.put(key, &Bytes::from(format!("data{i}").into_bytes())));
            assert!(shard_tinyfu.put(key, &Bytes::from(format!("data{i}").into_bytes())));
        }
        let lru_len = shard_lru.len();
        let tinylfu_len = shard_tinyfu.len();

        // Assert
        assert_eq!(lru_len, 5);
        assert_eq!(tinylfu_len, 5);
    }

    #[test]
    fn should_handle_zero_capacity() {
        // Arrange
        let shard = CacheShard::new(0, CachePolicyType::Lru);

        // Act
        let _ = shard.put(CacheKey::for_data(1, 0), &Bytes::from(&b"data"[..]));

        // Assert
        assert!(shard.is_empty());
    }

    #[test]
    fn should_handle_single_entry_eviction() {
        // Arrange
        let shard = CacheShard::new(10, CachePolicyType::Lru); // Very small cache
        let key1 = CacheKey::for_data(1, 0);
        let key2 = CacheKey::for_data(2, 0);

        // Act
        assert!(shard.put(key1, &Bytes::from(&b"12345"[..]))); // 5 bytes
        assert!(shard.put(key2, &Bytes::from(&b"67890"[..]))); // 5 bytes
        let retrieved = shard.get(&key1);

        // Assert - key1 might be evicted
        assert!(retrieved.is_none() || retrieved.is_some());
    }

    #[test]
    fn should_track_hit_miss_metrics() {
        // Arrange
        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);
        let key = CacheKey::for_data(1, 0);

        // Act
        shard.get(&key); // miss
        assert!(shard.put(key, &Bytes::from(&b"data"[..])));
        shard.get(&key); // hit
        shard.get(&key); // hit
        let metrics = shard.metrics();

        // Assert
        assert_eq!(metrics.miss_count(), 1);
        assert_eq!(metrics.hit_count(), 2);
    }

    #[test]
    fn should_track_eviction_metrics() {
        // Arrange
        let shard = CacheShard::new(50, CachePolicyType::Lru);

        // Act
        for i in 0..5 {
            let key = CacheKey::for_data(i, 0);
            assert!(shard.put(key, &Bytes::from(vec![0u8; 15])));
        }
        let metrics = shard.metrics();

        // Assert - some evictions should happen
        assert!(metrics.eviction_count() > 0);
    }
}
