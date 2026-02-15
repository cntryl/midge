//! Least Recently Used (LRU) eviction policy

use super::CachePolicy;
use crate::sst::cache::key::CacheKey;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// LRU eviction policy using generation counter (O(1) access, O(n) eviction scan)
///
/// Tracks access order by assigning a monotonically increasing generation
/// counter to each key on access. Eviction finds the key with the lowest
/// generation (least recently used).
///
/// This approach eliminates O(n) position updates that occur on every access
/// by deferring the scan to only when picking a victim.
pub struct LruPolicy {
    /// Map from key to last access generation
    generations: Mutex<HashMap<CacheKey, u64>>,
    /// Current generation counter (incremented on each access)
    generation: AtomicU64,
}

impl LruPolicy {
    /// Create a new LRU policy
    pub fn new() -> Self {
        Self {
            generations: Mutex::new(HashMap::new()),
            generation: AtomicU64::new(0),
        }
    }
}

impl Default for LruPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl CachePolicy for LruPolicy {
    /// Record access to a key (O(1) operation)
    ///
    /// Assigns a new generation counter to track recency.
    /// No O(n) position updates needed.
    #[inline]
    fn on_access(&self, key: CacheKey) {
        let gen = self.generation.fetch_add(1, Ordering::Relaxed);
        let mut gens = self.generations.lock();
        gens.insert(key, gen);
    }

    /// Pick a victim for eviction (O(n) scan, only during eviction)
    ///
    /// Finds the key with the smallest generation (least recently used)
    /// among non-excluded types. This scan is O(n) in cache size,
    /// but only runs during eviction (infrequent), unlike on_access
    /// which ran O(n) on every hit.
    fn pick_victim(&self, exclude_types: &[crate::sst::cache::BlockType]) -> Option<CacheKey> {
        let mut gens = self.generations.lock();

        // Direct iteration to avoid closure overhead
        let mut min_gen = u64::MAX;
        let mut victim_key = None;

        for (&key, &gen) in gens.iter() {
            if !exclude_types.contains(&key.block_type) && gen < min_gen {
                min_gen = gen;
                victim_key = Some(key);
            }
        }

        // Remove the victim from tracking
        if let Some(key) = victim_key {
            gens.remove(&key);
        }

        victim_key
    }

    /// Remove a key from tracking (O(1) operation)
    #[inline]
    fn on_remove(&self, key: CacheKey) {
        let mut gens = self.generations.lock();
        gens.remove(&key);
    }

    /// Mark a key as stale and remove it (O(1) operation)
    ///
    /// Called when a concurrent removal occurred during eviction.
    /// Just clean up tracking state.
    #[inline]
    fn on_stale(&self, key: CacheKey) {
        self.on_remove(key);
    }

    /// Clear all state (O(n) in current cache size)
    fn clear(&self) {
        let mut gens = self.generations.lock();
        gens.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
