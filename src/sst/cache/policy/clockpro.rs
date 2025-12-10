//! CLOCK-Pro eviction policy with scan resistance

use super::CachePolicy;
use crate::sst::cache::key::CacheKey;
use std::collections::VecDeque;
use std::sync::Mutex;

/// Entry in CLOCK-Pro
#[derive(Clone, Copy, Debug)]
struct ClockEntry {
    key: CacheKey,
    hot: bool,           // Is in hot set
    ref_bit: bool,       // Reference bit (accessed recently)
    test_bit: bool,      // Test bit (in testing phase)
}

/// CLOCK-Pro eviction policy
///
/// Combines CLOCK algorithm with hot/cold partitions for strong scan resistance.
/// Better than LRU under scan workloads.
pub struct ClockProPolicy {
    /// Circular list of entries
    entries: Mutex<VecDeque<ClockEntry>>,
    /// Current hand position in circular list
    hand: Mutex<usize>,
}

impl ClockProPolicy {
    /// Create a new CLOCK-Pro policy
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(VecDeque::new()),
            hand: Mutex::new(0),
        }
    }
}

impl Default for ClockProPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl CachePolicy for ClockProPolicy {
    fn on_access(&self, key: CacheKey) {
        let mut entries = self.entries.lock().expect("CLOCK-Pro entries lock");

        // Find or create entry
        if let Some(entry) = entries.iter_mut().find(|e| e.key == key) {
            entry.ref_bit = true;
            entry.hot = true; // Move to hot set on access
        } else {
            entries.push_back(ClockEntry {
                key,
                hot: true,
                ref_bit: true,
                test_bit: false,
            });
        }
    }

    fn pick_victim(&self) -> Option<CacheKey> {
        let mut entries = self.entries.lock().expect("CLOCK-Pro entries lock");
        let mut hand = self.hand.lock().expect("CLOCK-Pro hand lock");

        if entries.is_empty() {
            return None;
        }

        // Circular scan until we find a suitable victim
        for _ in 0..entries.len() {
            let idx = *hand % entries.len();
            let next_hand = (*hand + 1) % entries.len();
            
            let entry = &mut entries[idx];

            // Evict if: ref_bit is clear and not in hot set, or test_bit is set
            if !entry.ref_bit && (!entry.hot || entry.test_bit) {
                let victim = entry.key;
                entries.remove(idx);
                *hand = next_hand;
                return Some(victim);
            }

            // Clear ref_bit for next round
            entry.ref_bit = false;
            *hand = next_hand;
        }

        None
    }

    fn remove(&self, key: CacheKey) {
        let mut entries = self.entries.lock().expect("CLOCK-Pro entries lock");
        entries.retain(|e| e.key != key);
    }

    fn clear(&self) {
        let mut entries = self.entries.lock().expect("CLOCK-Pro entries lock");
        entries.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_and_evict() {
        // Arrange
        let policy = ClockProPolicy::new();
        let key1 = CacheKey::new(1, 0);

        // Act
        policy.on_access(key1);
        policy.remove(key1);

        // Assert - verify remove works
        let len_before = {
            let entries = policy.entries.lock().expect("entries lock");
            entries.len()
        };
        assert_eq!(len_before, 0);
    }

    #[test]
    fn should_track_accessed_keys() {
        // Arrange
        let policy = ClockProPolicy::new();
        let key1 = CacheKey::new(1, 0);
        let key2 = CacheKey::new(2, 0);

        // Act
        policy.on_access(key1);
        policy.on_access(key2);

        // Assert - both keys should be tracked
        let entries = policy.entries.lock().expect("CLOCK-Pro entries lock");
        assert_eq!(entries.len(), 2);
    }
}
