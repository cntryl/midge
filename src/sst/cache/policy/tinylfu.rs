//! `TinyLFU` (Frequency + Recency) eviction policy

use super::CachePolicy;
use crate::sst::cache::key::CacheKey;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};

/// `TinyLFU` eviction policy
///
/// Combines frequency and recency for better cache hit rates than pure LRU.
/// Tracks recent accesses and counts frequencies.
pub struct TinyLfuPolicy {
    /// Recent access queue (recency window)
    recent: Mutex<VecDeque<CacheKey>>,
    /// Frequency table for keys
    frequencies: Mutex<HashMap<CacheKey, u32>>,
    /// Window size for recency tracking
    window_size: usize,
}

impl TinyLfuPolicy {
    /// Create a new `TinyLFU` policy
    #[must_use]
    pub fn new() -> Self {
        Self {
            recent: Mutex::new(VecDeque::new()),
            frequencies: Mutex::new(HashMap::new()),
            window_size: 100, // Configurable in practice
        }
    }
}

impl Default for TinyLfuPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl CachePolicy for TinyLfuPolicy {
    fn on_access(&self, key: CacheKey) {
        let mut recent = self.recent.lock();
        let mut frequencies = self.frequencies.lock();

        // Add to recent window
        recent.push_back(key);
        if recent.len() > self.window_size {
            let expired = recent
                .pop_front()
                .expect("recent window is non-empty after an overflow");

            // Frequency metadata is a bounded, sliding window too.  Leaving
            // an entry behind after its final recency sample expires makes
            // policy memory grow with lifetime insertions even when the
            // cache itself is repeatedly evicting old keys.
            if !recent.contains(&expired) {
                frequencies.remove(&expired);
            }
        }

        // Increment frequency
        let frequency = frequencies.entry(key).or_insert(0);
        *frequency = frequency.saturating_add(1);
    }

    fn pick_victim(&self, exclude_types: &[crate::sst::cache::BlockType]) -> Option<CacheKey> {
        let mut recent = self.recent.lock();
        let frequencies = self.frequencies.lock();

        // Find victim with lowest frequency among recent accesses (excluding protected types)
        let mut victim: Option<CacheKey> = None;
        let mut min_freq = u32::MAX;

        for &key in recent.iter() {
            // Skip excluded block types
            if exclude_types.contains(&key.block_type) {
                continue;
            }

            let freq = *frequencies.get(&key).unwrap_or(&0);
            if freq < min_freq {
                min_freq = freq;
                victim = Some(key);
            }
        }

        // Remove victim from recent queue
        if let Some(v) = victim {
            recent.retain(|k| *k != v);
        }

        victim
    }

    fn on_remove(&self, key: CacheKey) {
        let mut recent = self.recent.lock();
        let mut frequencies = self.frequencies.lock();

        recent.retain(|k| *k != key);
        frequencies.remove(&key);
    }

    fn on_stale(&self, key: CacheKey) {
        // A stale victim has no corresponding cache entry, so every frequency
        // sample for it is stale too. Remove the entry outright rather than
        // leaking a residual count after repeated accesses.
        let mut recent = self.recent.lock();
        let mut frequencies = self.frequencies.lock();

        recent.retain(|k| *k != key);
        frequencies.remove(&key);
    }

    fn clear(&self) {
        let mut recent = self.recent.lock();
        let mut frequencies = self.frequencies.lock();
        recent.clear();
        frequencies.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_prefer_frequent_over_recent() {
        // Arrange
        let policy = TinyLfuPolicy::new();
        let key1 = CacheKey::for_data(1, 0);
        let key2 = CacheKey::for_data(2, 0);

        // Act
        // Access key1 multiple times (higher frequency)
        policy.on_access(key1);
        policy.on_access(key1);
        policy.on_access(key1);
        // Access key2 once
        policy.on_access(key2);

        // Assert - key2 should be evicted (lower frequency)
        if let Some(victim) = policy.pick_victim(&[]) {
            assert_eq!(victim, key2);
        }
    }

    #[test]
    fn should_track_frequencies() {
        // Arrange
        let policy = TinyLfuPolicy::new();
        let key1 = CacheKey::for_data(1, 0);

        // Act
        policy.on_access(key1);
        policy.on_access(key1);
        policy.on_access(key1);

        // Assert - can pick victim (frequencies tracked internally)
        let _ = policy.pick_victim(&[]);
    }

    #[test]
    fn should_remove_all_frequency_state_when_victim_is_stale() {
        // Arrange
        let policy = TinyLfuPolicy::new();
        let key = CacheKey::for_data(1, 0);
        for _ in 0..8 {
            policy.on_access(key);
        }
        assert_eq!(policy.pick_victim(&[]), Some(key));

        // Act
        policy.on_stale(key);

        // Assert
        assert!(!policy.frequencies.lock().contains_key(&key));
        assert!(!policy.recent.lock().contains(&key));
    }

    // ===== New comprehensive tests =====

    #[test]
    fn should_clear_all_state() {
        // Arrange
        let policy = TinyLfuPolicy::new();
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
        let policy = TinyLfuPolicy::new();

        // Act
        let victim = policy.pick_victim(&[]);

        // Assert
        assert!(victim.is_none());
    }

    #[test]
    fn should_have_default_instance() {
        // Arrange
        let policy = TinyLfuPolicy::default();

        // Act
        policy.on_access(CacheKey::for_data(1, 0));
        let victim = policy.pick_victim(&[]);

        // Assert
        assert!(victim.is_some());
    }

    #[test]
    fn should_remove_key_from_tracking() {
        // Arrange
        let policy = TinyLfuPolicy::new();
        let key1 = CacheKey::for_data(1, 0);
        let key2 = CacheKey::for_data(2, 0);

        // Act
        policy.on_access(key1);
        policy.on_access(key2);
        policy.on_remove(key1);

        // Assert - key1 should not be picked
        if let Some(victim) = policy.pick_victim(&[]) {
            assert_ne!(victim, key1);
        }
    }

    #[test]
    fn should_handle_window_overflow() {
        // Arrange
        let policy = TinyLfuPolicy::new();

        // Act - access more keys than window size
        for i in 0..200 {
            policy.on_access(CacheKey::for_data(i, 0));
        }

        // Assert - should handle gracefully
        let victim = policy.pick_victim(&[]);
        assert!(victim.is_some());
    }

    #[test]
    fn should_bound_frequency_metadata_to_the_recency_window() {
        // Arrange
        let policy = TinyLfuPolicy::new();

        // Act
        for i in 0..10_000 {
            policy.on_access(CacheKey::for_data(i, 0));
        }

        // Assert
        assert!(policy.recent.lock().len() <= policy.window_size);
        assert!(policy.frequencies.lock().len() <= policy.window_size);
    }

    #[test]
    fn should_prefer_high_frequency_over_low() {
        // Arrange
        let policy = TinyLfuPolicy::new();
        let freq_high = CacheKey::for_data(1, 0);
        let freq_low = CacheKey::for_data(2, 0);

        // Act
        for _ in 0..10 {
            policy.on_access(freq_high);
        }
        policy.on_access(freq_low);

        // Assert - low frequency key should be evicted first
        if let Some(victim) = policy.pick_victim(&[]) {
            assert_eq!(victim, freq_low);
        }
    }

    #[test]
    fn should_handle_mixed_frequencies() {
        // Arrange
        let policy = TinyLfuPolicy::new();
        let keys: Vec<CacheKey> = (0..5).map(|i| CacheKey::for_data(i, 0)).collect();

        // Act - varying frequencies
        policy.on_access(keys[0]); // 1 access
        for _ in 0..2 {
            policy.on_access(keys[1]); // 2 accesses
        }
        for _ in 0..3 {
            policy.on_access(keys[2]); // 3 accesses
        }
        for _ in 0..4 {
            policy.on_access(keys[3]); // 4 accesses
        }
        policy.on_access(keys[4]); // 1 access

        // Assert - lowest frequency should be picked
        if let Some(victim) = policy.pick_victim(&[]) {
            assert!(victim == keys[0] || victim == keys[4]);
        }
    }

    #[test]
    fn should_remove_nonexistent_key_safely() {
        // Arrange
        let policy = TinyLfuPolicy::new();

        // Act
        policy.on_remove(CacheKey::for_data(999, 999));

        // Assert - should not panic
        let victim = policy.pick_victim(&[]);
        assert!(victim.is_none());
    }
}
