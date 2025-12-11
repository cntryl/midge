//! Least Recently Used (LRU) eviction policy

use super::CachePolicy;
use crate::sst::cache::key::CacheKey;
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

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
    fn on_access(&self, key: CacheKey) {
        let mut queue = self.queue.lock().expect("LRU queue lock");
        let mut positions = self.positions.lock().expect("LRU positions lock");

        // If key already exists, remove it from queue
        if let Some(&pos) = positions.get(&key) {
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

    fn pick_victim(&self) -> Option<CacheKey> {
        let mut queue = self.queue.lock().expect("LRU queue lock");
        let mut positions = self.positions.lock().expect("LRU positions lock");

        if let Some(victim) = queue.pop_front() {
            // Update all positions
            for (_, p) in positions.iter_mut() {
                if *p > 0 {
                    *p -= 1;
                }
            }
            positions.remove(&victim);
            Some(victim)
        } else {
            None
        }
    }

    fn remove(&self, key: CacheKey) {
        let mut queue = self.queue.lock().expect("LRU queue lock");
        let mut positions = self.positions.lock().expect("LRU positions lock");

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

    fn clear(&self) {
        let mut queue = self.queue.lock().expect("LRU queue lock");
        let mut positions = self.positions.lock().expect("LRU positions lock");
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
        let key1 = CacheKey::new(1, 0);
        let key2 = CacheKey::new(2, 0);
        let key3 = CacheKey::new(3, 0);

        // Act
        policy.on_access(key1);
        policy.on_access(key2);
        policy.on_access(key3);
        let victim = policy.pick_victim();

        // Assert - key1 should be evicted (least recently used)
        assert_eq!(victim, Some(key1));
    }

    #[test]
    fn should_update_lru_on_reaccess() {
        // Arrange
        let policy = LruPolicy::new();
        let key1 = CacheKey::new(1, 0);
        let key2 = CacheKey::new(2, 0);

        // Act
        policy.on_access(key1);
        policy.on_access(key2);
        policy.on_access(key1); // Re-access key1 (move to end)
        let victim = policy.pick_victim();

        // Assert - key2 should be evicted (now least recently used)
        assert_eq!(victim, Some(key2));
    }

    #[test]
    fn should_remove_key_from_tracking() {
        // Arrange
        let policy = LruPolicy::new();
        let key1 = CacheKey::new(1, 0);
        let key2 = CacheKey::new(2, 0);

        // Act
        policy.on_access(key1);
        policy.on_access(key2);
        policy.remove(key1);
        let victim = policy.pick_victim();

        // Assert - key2 should be evicted (key1 was removed)
        assert_eq!(victim, Some(key2));
    }
}
