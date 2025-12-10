//! Single cache shard with lock-based entry management

use crate::sst::cache::admission::AdmissionCounter;
use crate::sst::cache::key::CacheKey;
use crate::sst::cache::metrics::CacheMetrics;
use crate::sst::cache::policy::{CachePolicy, CachePolicyType};
use crate::sst::cache::value::CacheValue;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Mutex;

/// A single cache shard (partition) with independent lock
///
/// Contains a portion of the cache entries with its own lock,
/// eviction policy, and metrics.
pub struct CacheShard {
    /// Map of cache key -> value
    entries: Mutex<HashMap<CacheKey, CacheValue>>,
    /// Eviction policy
    policy: Box<dyn CachePolicy>,
    /// Admission control counter
    admission: AdmissionCounter,
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
    pub fn new(max_bytes: u64, policy_type: CachePolicyType) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            policy: policy_type.create(),
            admission: AdmissionCounter::new(64, 1000),
            metrics: CacheMetrics::new(),
            max_bytes,
        }
    }

    /// Get a cached value
    pub fn get(&self, key: &CacheKey) -> Option<CacheValue> {
        let entries = self.entries.lock().expect("cache shard lock");
        if let Some(value) = entries.get(key) {
            value.increment_access();
            self.policy.on_access(*key);
            self.metrics.record_hit();
            Some(value.clone())
        } else {
            self.metrics.record_miss();
            None
        }
    }

    /// Insert a value into the cache
    ///
    /// Returns true if the value was inserted, false if rejected by admission control
    pub fn put(&self, key: CacheKey, value: Bytes) -> bool {
        // Record SST access for admission control (counts this SST as seen)
        self.admission
            .record_access(key.sst_id.to_le_bytes().as_ref());

        let cache_value = CacheValue::new(value);
        let value_size = cache_value.size_bytes() as u64;

        let mut entries = self.entries.lock().expect("cache shard lock");

        // Check if entry already exists
        if let Some(existing) = entries.get(&key) {
            // Update existing entry
            let old_size = existing.size_bytes() as u64;
            entries.insert(key, cache_value);
            self.metrics.add_memory(value_size as u64);
            self.metrics.remove_memory(old_size);
            self.policy.on_access(key);
            return true;
        }

        // Add new entry
        entries.insert(key, cache_value);
        self.metrics.add_memory(value_size);
        self.policy.on_access(key);

        // Evict if over capacity
        while self.metrics.memory_bytes() > self.max_bytes {
            if let Some(victim) = self.policy.pick_victim() {
                if let Some(evicted) = entries.remove(&victim) {
                    self.metrics.remove_memory(evicted.size_bytes() as u64);
                    self.metrics.record_eviction();
                }
            } else {
                break; // Can't evict more
            }
        }

        true
    }

    /// Remove a key from the cache
    pub fn remove(&self, key: &CacheKey) -> Option<CacheValue> {
        let mut entries = self.entries.lock().expect("cache shard lock");
        if let Some(value) = entries.remove(key) {
            self.metrics.remove_memory(value.size_bytes() as u64);
            self.policy.remove(*key);
            Some(value)
        } else {
            None
        }
    }

    /// Clear all entries from the cache
    pub fn clear(&self) {
        let mut entries = self.entries.lock().expect("cache shard lock");
        entries.clear();
        self.metrics.set_memory_bytes(0);
        self.policy.clear();
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
        let entries = self.entries.lock().expect("cache shard lock");
        entries.len()
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
    fn should_store_and_retrieve() {
        // Arrange
        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);
        let key = CacheKey::new(1, 0);
        let value = Bytes::from(&b"hello world"[..]);

        // Act
        shard.put(key, value.clone());
        let retrieved = shard.get(&key);

        // Assert
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().data.to_vec(), value.to_vec());
    }

    #[test]
    fn should_evict_on_overflow() {
        // Arrange
        let shard = CacheShard::new(100, CachePolicyType::Lru);
        let key1 = CacheKey::new(1, 0);
        let key2 = CacheKey::new(2, 0);
        let data1 = vec![b'x'; 80];
        let data2 = vec![b'y'; 80];

        // Act
        shard.put(key1, Bytes::from(data1));
        shard.put(key2, Bytes::from(data2));

        // Assert - key1 should be evicted (LRU)
        assert!(shard.get(&key1).is_none());
        assert!(shard.get(&key2).is_some());
    }

    #[test]
    fn should_track_metrics() {
        // Arrange
        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);
        let key = CacheKey::new(1, 0);
        let value = Bytes::from(&b"test_data"[..]);

        // Act
        shard.put(key, value);
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
            let key = CacheKey::new(i, 0);
            shard.put(key, Bytes::from(format!("value_{}", i).into_bytes()));
        }
        shard.clear();

        // Assert
        assert_eq!(shard.len(), 0);
        assert_eq!(shard.size_bytes(), 0);
    }
}

