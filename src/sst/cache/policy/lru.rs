//! Least Recently Used (LRU) eviction policy

use super::CachePolicy;
use crate::sst::cache::key::CacheKey;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};

/// LRU eviction policy
///
/// Tracks access order and evicts least recently used blocks first.
pub struct LruPolicy {
    /// Queue of keys in access order (front = oldest)
    queue: Mutex<VecDeque<CacheKey>>,
    /// Position of each key in the queue
    positions: Mutex<HashMap<CacheKey, usize>>,
}

impl LruPolicy {
    /// Create a new LRU policy
    pub fn new() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            positions: Mutex::new(HashMap::new()),
        }
    }
}

impl Default for LruPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl CachePolicy for LruPolicy {
    /// Lock order: always queue then positions; never the reverse (avoids deadlock).
    fn on_access(&self, key: CacheKey) {
        let mut queue = self.queue.lock();
        let mut positions = self.positions.lock();

        // If key already exists, remove it from queue
        if let Some(pos) = positions.remove(&key) {
            queue.remove(pos);
            // Update all positions after the removed element
            for (_, p) in positions.iter_mut() {
                if *p > pos {
                    *p -= 1;
                }
            }
        }

        // Add key to end (most recently used)
        queue.push_back(key);
        positions.insert(key, queue.len() - 1);
    }

    fn pick_victim(&self, exclude_types: &[crate::sst::cache::BlockType]) -> Option<CacheKey> {
        let mut queue = self.queue.lock();
        let mut positions = self.positions.lock();

        // Find first victim not in exclude list
        let mut victim_idx = None;
        for (idx, key) in queue.iter().enumerate() {
            if !exclude_types.contains(&key.block_type) {
                victim_idx = Some(idx);
                break;
            }
        }

        if let Some(idx) = victim_idx {
            let victim = queue.remove(idx).unwrap_or_else(|| unreachable!("victim index from same queue"));
            // Update all positions after the removed element
            for (_, p) in positions.iter_mut() {
                if *p > idx {
                    *p -= 1;
                }
            }
            positions.remove(&victim);
            Some(victim)
        } else {
            None
        }
    }

    fn on_remove(&self, key: CacheKey) {
        let mut queue = self.queue.lock();
        let mut positions = self.positions.lock();

        if let Some(pos) = positions.remove(&key) {
            queue.remove(pos);
            // Update all positions after the removed element
            for (_, p) in positions.iter_mut() {
                if *p > pos {
                    *p -= 1;
                }
            }
        }
    }

    fn on_stale(&self, key: CacheKey) {
        // Same as on_remove - just clean up tracking state
        self.on_remove(key);
    }

    fn clear(&self) {
        let mut queue = self.queue.lock();
        let mut positions = self.positions.lock();
        queue.clear();
        positions.clear();
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
