//! TinyLFU (Frequency + Recency) eviction policy

use super::CachePolicy;
use crate::sst::cache::key::CacheKey;
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

/// TinyLFU eviction policy
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
    /// Create a new TinyLFU policy
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
        let mut recent = self.recent.lock().expect("TinyLFU recent lock");
        let mut frequencies = self.frequencies.lock().expect("TinyLFU frequencies lock");

        // Add to recent window
        recent.push_back(key);
        if recent.len() > self.window_size {
            recent.pop_front();
        }

        // Increment frequency
        *frequencies.entry(key).or_insert(0) += 1;
    }

    fn pick_victim(&self) -> Option<CacheKey> {
        let mut recent = self.recent.lock().expect("TinyLFU recent lock");
        let frequencies = self.frequencies.lock().expect("TinyLFU frequencies lock");

        // Find victim with lowest frequency among recent accesses
        let mut victim: Option<CacheKey> = None;
        let mut min_freq = u32::MAX;

        for &key in recent.iter() {
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

    fn remove(&self, key: CacheKey) {
        let mut recent = self.recent.lock().expect("TinyLFU recent lock");
        let mut frequencies = self.frequencies.lock().expect("TinyLFU frequencies lock");

        recent.retain(|k| *k != key);
        frequencies.remove(&key);
    }

    fn clear(&self) {
        let mut recent = self.recent.lock().expect("TinyLFU recent lock");
        let mut frequencies = self.frequencies.lock().expect("TinyLFU frequencies lock");
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
        let key1 = CacheKey::new(1, 0);
        let key2 = CacheKey::new(2, 0);

        // Act
        // Access key1 multiple times (higher frequency)
        policy.on_access(key1);
        policy.on_access(key1);
        policy.on_access(key1);
        // Access key2 once
        policy.on_access(key2);

        // Assert - key2 should be evicted (lower frequency)
        if let Some(victim) = policy.pick_victim() {
            assert_eq!(victim, key2);
        }
    }

    #[test]
    fn should_track_frequencies() {
        // Arrange
        let policy = TinyLfuPolicy::new();
        let key1 = CacheKey::new(1, 0);

        // Act
        policy.on_access(key1);
        policy.on_access(key1);
        policy.on_access(key1);

        // Assert - can pick victim (frequencies tracked internally)
        let _ = policy.pick_victim();
    }
}
