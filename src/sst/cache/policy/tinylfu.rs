//! `TinyLFU` (Frequency + Recency) eviction policy

use super::CachePolicy;
use crate::sst::cache::key::CacheKey;
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap};

/// Oldest resident entries considered for each eviction.
const VICTIM_SAMPLE: usize = 16;
/// Halve every frequency after this many accesses per resident entry, so
/// old popularity fades instead of pinning a block forever.
const AGING_ACCESSES_PER_ENTRY: u64 = 10;

/// `TinyLFU` eviction policy
///
/// Every resident entry keeps a frequency count and a recency position until
/// it is removed. Eviction picks the least-frequent entry among the oldest
/// few, so a once-hot block cannot starve the current working set and no
/// resident block ever becomes unevictable.
pub struct TinyLfuPolicy {
    state: Mutex<TinyLfuState>,
}

#[derive(Default)]
struct TinyLfuState {
    /// Resident key -> (frequency, recency stamp).
    entries: HashMap<CacheKey, (u32, u64)>,
    /// Recency stamp -> key, oldest first.
    order: BTreeMap<u64, CacheKey>,
    next_stamp: u64,
    accesses_since_aging: u64,
}

impl TinyLfuState {
    fn forget(&mut self, key: &CacheKey) {
        if let Some((_, stamp)) = self.entries.remove(key) {
            self.order.remove(&stamp);
        }
    }

    fn age_if_due(&mut self) {
        let threshold = (self.entries.len() as u64)
            .saturating_mul(AGING_ACCESSES_PER_ENTRY)
            .max(AGING_ACCESSES_PER_ENTRY);
        if self.accesses_since_aging >= threshold {
            for (frequency, _) in self.entries.values_mut() {
                *frequency /= 2;
            }
            self.accesses_since_aging = 0;
        }
    }
}

impl TinyLfuPolicy {
    /// Create a new `TinyLFU` policy
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Mutex::new(TinyLfuState::default()),
        }
    }

    #[cfg(test)]
    fn frequency(&self, key: &CacheKey) -> Option<u32> {
        self.state
            .lock()
            .entries
            .get(key)
            .map(|(frequency, _)| *frequency)
    }

    #[cfg(test)]
    fn tracks(&self, key: &CacheKey) -> bool {
        self.state.lock().entries.contains_key(key)
    }
}

impl Default for TinyLfuPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl CachePolicy for TinyLfuPolicy {
    fn on_access(&self, key: CacheKey) {
        let mut state = self.state.lock();
        let stamp = state.next_stamp;
        state.next_stamp = state.next_stamp.wrapping_add(1);
        let previous = state.entries.get(&key).copied();
        let frequency = previous.map_or(1, |(frequency, old_stamp)| {
            state.order.remove(&old_stamp);
            frequency.saturating_add(1)
        });
        state.entries.insert(key, (frequency, stamp));
        state.order.insert(stamp, key);
        state.accesses_since_aging = state.accesses_since_aging.saturating_add(1);
        state.age_if_due();
    }

    fn pick_victim(&self, exclude_types: &[crate::sst::cache::CacheBlockKind]) -> Option<CacheKey> {
        let mut state = self.state.lock();
        // The newest entry is usually the block being admitted right now; with
        // one access it would always lose to the resident set and never stay.
        // Keep it unless nothing else is evictable.
        let newest = state.order.values().next_back().copied();
        let candidates = || {
            state
                .order
                .values()
                .filter(|key| !exclude_types.contains(&key.block_type))
        };
        let victim = candidates()
            .filter(|key| Some(**key) != newest)
            .take(VICTIM_SAMPLE)
            .min_by_key(|key| {
                state
                    .entries
                    .get(*key)
                    .map_or(0, |(frequency, _)| *frequency)
            })
            .or_else(|| candidates().next())
            .copied();
        if let Some(victim) = victim {
            state.forget(&victim);
        }
        victim
    }

    fn on_remove(&self, key: CacheKey) {
        self.state.lock().forget(&key);
    }

    fn on_stale(&self, key: CacheKey) {
        // A stale victim has no corresponding cache entry, so drop its state.
        self.state.lock().forget(&key);
    }

    fn clear(&self) {
        let mut state = self.state.lock();
        state.entries.clear();
        state.order.clear();
        state.accesses_since_aging = 0;
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
        // The newest entry is protected as the one being admitted.
        policy.on_access(CacheKey::for_data(3, 0));

        // Assert - key2 should be evicted (lower frequency)
        assert_eq!(policy.pick_victim(&[]), Some(key2));
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

        // Assert - each access increments the tracked frequency count
        assert_eq!(policy.frequency(&key1), Some(3));
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
        assert!(!policy.tracks(&key));
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

        // Assert - key1 was removed, so key2 is the only remaining victim
        assert_eq!(policy.pick_victim(&[]), Some(key2));
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
    fn should_bound_policy_metadata_to_resident_entries() {
        // Arrange: the cache evicts one victim per admission once full, so
        // policy state must track the resident set, not lifetime insertions.
        let policy = TinyLfuPolicy::new();
        let resident_limit = 100;

        // Act
        for i in 0..10_000 {
            policy.on_access(CacheKey::for_data(i, 0));
            if policy.state.lock().entries.len() > resident_limit {
                policy.pick_victim(&[]);
            }
        }

        // Assert
        let state = policy.state.lock();
        assert!(state.entries.len() <= resident_limit);
        assert_eq!(state.order.len(), state.entries.len());
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
        // The newest entry is protected as the one being admitted.
        policy.on_access(CacheKey::for_data(3, 0));

        // Assert - low frequency key should be evicted first
        assert_eq!(policy.pick_victim(&[]), Some(freq_low));
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

        // Assert - lowest frequency should be picked; keys[0] and keys[4]
        // are tied at frequency 1, but keys[0] is encountered first when
        // scanning the recency queue, so it wins the tie deterministically.
        assert_eq!(policy.pick_victim(&[]), Some(keys[0]));
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

#[cfg(test)]
mod residency_tests {
    use crate::sst::cache::{BlockCache, CacheKey, CachePolicyType};
    use bytes::Bytes;

    #[test]
    fn should_evict_blocks_outside_recent_window_when_tinylfu_shard_is_full() {
        // Arrange: one hot block pushes every other key out of a fixed-size
        // access window. Blocks that fall out of it must stay evictable.
        let block = Bytes::from(vec![0_u8; 1024]);
        let cache = BlockCache::new(10 * 1024, 1, CachePolicyType::TinyLfu);
        for index in 0..10 {
            cache.put(CacheKey::for_data(index, 0), &block);
        }
        for _ in 0..150 {
            let _ = cache.get(&CacheKey::for_data(0, 0));
        }

        // Act
        for index in 10..40 {
            let key = CacheKey::for_data(index, 0);
            cache.put(key, &block);
            let _ = cache.get(&key);
        }

        // Assert
        let resident = |range: std::ops::Range<u64>| {
            range
                .filter(|index| cache.get(&CacheKey::for_data(*index, 0)).is_some())
                .count()
        };
        assert!(
            resident(30..40) >= 5,
            "the current working set must be admitted: {} of 10 resident",
            resident(30..40)
        );
        assert_eq!(
            resident(1..10),
            0,
            "cold blocks from the old window must be evictable"
        );
    }
}
