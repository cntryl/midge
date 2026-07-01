//! Least Recently Used (LRU) eviction policy

use super::CachePolicy;
use crate::sst::cache::key::CacheKey;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// LRU eviction policy using generation counter
///
/// Tracks access order by assigning a monotonically increasing generation
/// counter to each key on access.
pub struct LruPolicy {
    /// Map from key to last access generation
    generations: Mutex<HashMap<CacheKey, u64>>,
    /// Current generation counter (incremented on each access)
    generation: AtomicU64,
}

impl LruPolicy {
    /// Create a new LRU policy
    #[must_use]
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
    /// Record access to a key
    ///
    /// Assigns a new generation counter to track recency.
    #[inline]
    fn on_access(&self, key: CacheKey) {
        let gen = self.generation.fetch_add(1, Ordering::Relaxed);
        self.generations.lock().insert(key, gen);
    }

    /// Pick a victim for eviction
    ///
    /// Finds the key with the smallest generation (least recently used)
    /// among non-excluded types.
    fn pick_victim(&self, exclude_types: &[crate::sst::cache::BlockType]) -> Option<CacheKey> {
        let mut generations = self.generations.lock();

        let victim = if exclude_types.is_empty() {
            // Fast path: no exclusions, just find global minimum
            generations
                .iter()
                .min_by_key(|(_, &gen)| gen)
                .map(|(&key, _)| key)
        } else {
            // With exclusions, filter first
            generations
                .iter()
                .filter(|(key, _)| !exclude_types.contains(&key.block_type))
                .min_by_key(|(_, &gen)| gen)
                .map(|(&key, _)| key)
        };

        if let Some(key) = victim {
            generations.remove(&key);
        }

        victim
    }

    /// Remove a key from tracking
    #[inline]
    fn on_remove(&self, key: CacheKey) {
        self.generations.lock().remove(&key);
    }

    /// Mark a key as stale and remove it
    ///
    /// Called when a concurrent removal occurred during eviction.
    /// Just clean up tracking state.
    #[inline]
    fn on_stale(&self, key: CacheKey) {
        self.on_remove(key);
    }

    /// Clear all state
    fn clear(&self) {
        self.generations.lock().clear();
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
