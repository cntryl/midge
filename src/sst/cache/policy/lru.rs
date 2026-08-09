//! Least Recently Used (LRU) eviction policy

use super::CachePolicy;
use crate::sst::cache::key::CacheKey;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// LRU eviction policy using generation counter
///
/// Tracks access order by assigning a monotonically increasing generation
/// counter to each key on access.
pub struct LruPolicy {
    /// Concurrent map from key to its last access generation.
    ///
    /// Hits only contend on the shard containing their key. The generation
    /// itself is atomic so every hit can synchronously publish exact recency
    /// without taking a policy-wide lock.
    generations: DashMap<CacheKey, AtomicU64>,
    /// Current generation counter (incremented on each access)
    generation: AtomicU64,
}

impl LruPolicy {
    /// Create a new LRU policy
    #[must_use]
    pub fn new() -> Self {
        Self {
            generations: DashMap::new(),
            generation: AtomicU64::new(0),
        }
    }

    fn publish_generation(entry: &AtomicU64, generation: u64) {
        // Accesses are assigned a generation before they touch the map. A
        // delayed thread must not publish an older generation over a newer
        // access, so publication is an atomic max rather than a plain store.
        let mut current = entry.load(Ordering::Acquire);
        while current < generation {
            match entry.compare_exchange(current, generation, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }
}

impl Default for LruPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl CachePolicy for LruPolicy {
    /// Record access to a key
    ///
    /// Assigns a new generation counter to track recency.
    #[inline]
    fn on_access(&self, key: CacheKey) {
        let generation = self.generation.fetch_add(1, Ordering::Relaxed);

        // The overwhelmingly common path is a hit on an already tracked key.
        // `DashMap::entry` takes that shard's write lock even when no insertion
        // is needed, turning a shared hot block into a serialized read path.
        // A read lookup keeps exact synchronous recency publication while
        // allowing concurrent hits to CAS the per-key generation.
        if let Some(entry) = self.generations.get(&key) {
            Self::publish_generation(entry.value(), generation);
            return;
        }

        let entry = self
            .generations
            .entry(key)
            .or_insert_with(|| AtomicU64::new(generation));
        Self::publish_generation(&entry, generation);
    }

    /// Pick a victim for eviction
    ///
    /// Finds the key with the smallest generation (least recently used)
    /// among non-excluded types.
    fn pick_victim(&self, exclude_types: &[crate::sst::cache::CacheBlockKind]) -> Option<CacheKey> {
        loop {
            let candidate = self
                .generations
                .iter()
                .filter(|entry| {
                    exclude_types.is_empty() || !exclude_types.contains(&entry.key().block_type)
                })
                .map(|entry| (*entry.key(), entry.value().load(Ordering::Acquire)))
                .min_by_key(|(_, generation)| *generation);

            let (key, generation) = candidate?;

            // A hit can update a candidate while the scan is in progress.
            // Remove only if the exact generation we selected is still live;
            // otherwise rescan to retain exact LRU ordering.
            if self
                .generations
                .remove_if(&key, |_, current_generation| {
                    current_generation.load(Ordering::Acquire) == generation
                })
                .is_some()
            {
                return Some(key);
            }
        }
    }

    /// Remove a key from tracking
    #[inline]
    fn on_remove(&self, key: CacheKey) {
        self.generations.remove(&key);
    }

    /// Mark a key as stale and remove it
    ///
    /// Called when a concurrent removal occurred during eviction.
    /// Just clean up tracking state.
    #[inline]
    fn on_stale(&self, key: CacheKey) {
        // `pick_victim` conditionally removes its selected generation. A stale
        // cache entry therefore has no policy entry left to clean up. Leaving
        // a concurrent fresh access alone is essential for exact recency.
        let _ = key;
    }

    /// Clear all state
    fn clear(&self) {
        self.generations.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::thread;

    #[test]
    fn should_evict_least_recently_used() {
        // Arrange
        let policy = LruPolicy::new();
        let key1 = CacheKey::for_data(1, 0);
        let key2 = CacheKey::for_data(2, 0);
        let key3 = CacheKey::for_data(3, 0);

        // Act
        policy.on_access(key1);
        policy.on_access(key2);
        policy.on_access(key3);
        let victim = policy.pick_victim(&[]);

        // Assert - key1 should be evicted (least recently used)
        assert_eq!(victim, Some(key1));
    }

    #[test]
    fn should_update_lru_on_reaccess() {
        // Arrange
        let policy = LruPolicy::new();
        let key1 = CacheKey::for_data(1, 0);
        let key2 = CacheKey::for_data(2, 0);

        // Act
        policy.on_access(key1);
        policy.on_access(key2);
        policy.on_access(key1); // Re-access key1 (move to end)
        let victim = policy.pick_victim(&[]);

        // Assert - key2 should be evicted (now least recently used)
        assert_eq!(victim, Some(key2));
    }

    #[test]
    fn should_remove_key_from_tracking() {
        // Arrange
        let policy = LruPolicy::new();
        let key1 = CacheKey::for_data(1, 0);
        let key2 = CacheKey::for_data(2, 0);

        // Act
        policy.on_access(key1);
        policy.on_access(key2);
        policy.on_remove(key1);
        let victim = policy.pick_victim(&[]);

        // Assert - key2 should be evicted (key1 was removed)
        assert_eq!(victim, Some(key2));
    }

    // ===== New comprehensive tests =====

    #[test]
    fn should_clear_all_state() {
        // Arrange
        let policy = LruPolicy::new();
        for i in 0..5 {
            policy.on_access(CacheKey::for_data(i, 0));
        }

        // Act
        policy.clear();
        let victim = policy.pick_victim(&[]);

        // Assert
        assert!(victim.is_none());
    }

    #[test]
    fn should_pick_none_when_empty() {
        // Arrange
        let policy = LruPolicy::new();

        // Act
        let victim = policy.pick_victim(&[]);

        // Assert
        assert!(victim.is_none());
    }

    #[test]
    fn should_have_default_instance() {
        // Arrange
        let policy = LruPolicy::default();

        // Act
        policy.on_access(CacheKey::for_data(1, 0));
        let victim = policy.pick_victim(&[]);

        // Assert
        assert_eq!(victim, Some(CacheKey::for_data(1, 0)));
    }

    #[test]
    fn should_handle_fifo_order_for_sequential_accesses() {
        // Arrange
        let policy = LruPolicy::new();
        let keys: Vec<CacheKey> = (1..=10).map(|i| CacheKey::for_data(i, 0)).collect();

        // Act
        for key in &keys {
            policy.on_access(*key);
        }

        // Assert - evict in FIFO order
        for key in &keys {
            let victim = policy.pick_victim(&[]);
            assert_eq!(victim, Some(*key));
        }
    }

    #[test]
    fn should_handle_mixed_accesses_with_removals() {
        // Arrange
        let policy = LruPolicy::new();
        let key1 = CacheKey::for_data(1, 0);
        let key2 = CacheKey::for_data(2, 0);
        let key3 = CacheKey::for_data(3, 0);

        // Act
        policy.on_access(key1);
        policy.on_access(key2);
        policy.on_remove(key2);
        policy.on_access(key3);
        let victim1 = policy.pick_victim(&[]);
        let victim2 = policy.pick_victim(&[]);

        // Assert
        assert_eq!(victim1, Some(key1));
        assert_eq!(victim2, Some(key3));
    }

    #[test]
    fn should_move_key_to_end_on_reaccess() {
        // Arrange
        let policy = LruPolicy::new();
        let keys: Vec<CacheKey> = (1..=5).map(|i| CacheKey::for_data(i, 0)).collect();

        // Act
        for key in &keys {
            policy.on_access(*key);
        }
        // Re-access middle key
        policy.on_access(keys[1]); // key 2

        // Assert - victim should be key 1 (oldest after re-access)
        let victim = policy.pick_victim(&[]);
        assert_eq!(victim, Some(keys[0]));
    }

    #[test]
    fn should_remove_nonexistent_key_safely() {
        // Arrange
        let policy = LruPolicy::new();

        // Act
        policy.on_remove(CacheKey::for_data(999, 999)); // Remove non-existent key
        let victim = policy.pick_victim(&[]);

        // Assert - should not panic
        assert!(victim.is_none());
    }

    #[test]
    fn should_handle_duplicate_accesses() {
        // Arrange
        let policy = LruPolicy::new();
        let key = CacheKey::for_data(1, 0);

        // Act
        policy.on_access(key);
        policy.on_access(key); // Access again
        policy.on_access(key); // And again
        let victim = policy.pick_victim(&[]);

        // Assert - key should still be evicted once
        assert_eq!(victim, Some(key));
        assert_eq!(policy.pick_victim(&[]), None);
    }

    #[test]
    fn should_preserve_fresh_access_when_stale_victim_is_reported() {
        // Arrange
        let policy = LruPolicy::new();
        let key = CacheKey::for_data(1, 0);

        // Act
        policy.on_access(key);
        assert_eq!(policy.pick_victim(&[]), Some(key));
        policy.on_access(key);
        policy.on_stale(key);

        // Assert - stale cleanup must not erase the new synchronous access.
        assert_eq!(policy.pick_victim(&[]), Some(key));
    }

    #[test]
    fn should_evict_cold_key_after_concurrent_hot_key_accesses() {
        // Arrange
        let policy = std::sync::Arc::new(LruPolicy::new());
        let cold_key = CacheKey::for_data(1, 0);
        let hot_key = CacheKey::for_data(2, 0);
        policy.on_access(cold_key);
        policy.on_access(hot_key);
        let barrier = std::sync::Arc::new(Barrier::new(9));
        let mut workers = Vec::new();

        // Act
        for _ in 0..8 {
            let policy = std::sync::Arc::clone(&policy);
            let barrier = std::sync::Arc::clone(&barrier);
            workers.push(thread::spawn(move || {
                barrier.wait();
                for _ in 0..1_000 {
                    policy.on_access(hot_key);
                }
            }));
        }
        barrier.wait();
        for worker in workers {
            worker.join().expect("hot-key worker should not panic");
        }

        // Assert
        assert_eq!(policy.pick_victim(&[]), Some(cold_key));
        assert_eq!(policy.pick_victim(&[]), Some(hot_key));
    }
}
