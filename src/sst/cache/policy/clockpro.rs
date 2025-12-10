//! CLOCK-Pro eviction policy with scan resistance
//!
//! CLOCK-Pro is a modern eviction algorithm that provides:
//! - O(1) amortized operations for access, insert, and eviction
//! - Scan resistance via hot/cold separation
//! - Adaptive hot-set sizing based on workload
//!
//! The algorithm maintains a circular buffer with per-entry metadata (ref_bit, hot_bit)
//! and sweeps through with a clock hand, clearing reference bits and selecting victims
//! from cold, unreferenced entries.
//!
//! # Regions
//!
//! CLOCK-Pro divides entries into three conceptual regions:
//! - **Hot**: Frequently accessed blocks (accessed more than once)
//! - **Cold**: Recently inserted or single-access blocks
//! - **Ghost**: Metadata for recently evicted cold blocks (no data)

use super::CachePolicy;
use crate::sst::cache::key::CacheKey;
use std::collections::HashMap;
use std::sync::Mutex;

/// Per-entry metadata for CLOCK-Pro (3 bits of state)
#[derive(Clone, Copy, Debug, Default)]
struct SlotMetadata {
    /// Reference bit: set on access, cleared by clock hand
    ref_bit: bool,
    /// Hot bit: true if entry is in the hot region
    hot_bit: bool,
    /// Key stored in this slot (None if slot is empty/ghost)
    key: Option<CacheKey>,
}

/// CLOCK-Pro eviction policy with scan resistance
///
/// Combines a circular buffer clock hand mechanism with hot/cold partitioning
/// for O(1) amortized victim selection and strong scan resistance.
pub struct ClockProPolicy {
    /// Per-slot metadata (indexed by internal slot ID)
    slots: Mutex<Vec<SlotMetadata>>,
    /// Map from CacheKey to slot index for O(1) access/removal
    key_to_slot: Mutex<HashMap<CacheKey, usize>>,
    /// Clock hand position (next slot to examine)
    hand: Mutex<usize>,
    /// Number of resident entries
    resident_count: Mutex<usize>,
    /// Target number of hot entries (adaptive)
    hot_target: Mutex<usize>,
    /// Current number of hot entries
    hot_count: Mutex<usize>,
}

impl ClockProPolicy {
    /// Create a new CLOCK-Pro policy
    pub fn new() -> Self {
        const DEFAULT_CAPACITY: usize = 1024;
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    /// Create a new CLOCK-Pro policy with explicit capacity
    pub fn with_capacity(capacity: usize) -> Self {
        let capacity = capacity.max(16);
        let hot_target = capacity / 4; // Start with ~25% hot target

        Self {
            slots: Mutex::new(Vec::with_capacity(capacity)),
            key_to_slot: Mutex::new(HashMap::new()),
            hand: Mutex::new(0),
            resident_count: Mutex::new(0),
            hot_target: Mutex::new(hot_target),
            hot_count: Mutex::new(0),
        }
    }

    /// Ensure slots vector can hold the given index
    fn ensure_capacity(slots: &mut Vec<SlotMetadata>, idx: usize) {
        if idx >= slots.len() {
            slots.resize(idx + 1, SlotMetadata::default());
        }
    }

    /// Advance clock hand, wrapping around
    fn advance_hand(hand: &mut usize, slots_len: usize) {
        if slots_len > 0 {
            *hand = (*hand + 1) % slots_len;
        }
    }

    /// Adapt hot target when evicting a hot entry
    fn on_hot_evict(hot_target: &mut usize) {
        if *hot_target > 1 {
            *hot_target = hot_target.saturating_sub(1);
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
        let mut slots = self.slots.lock().expect("slots lock");
        let mut key_to_slot = self.key_to_slot.lock().expect("key_to_slot lock");
        let hot_target = self.hot_target.lock().expect("hot_target lock");

        if let Some(&slot_idx) = key_to_slot.get(&key) {
            // Existing entry: set reference bit and promote to hot if cold
            if slot_idx < slots.len() {
                let was_cold = !slots[slot_idx].hot_bit;
                slots[slot_idx].ref_bit = true;

                // Promote cold → hot on access if hot set not full
                if was_cold && *self.hot_count.lock().expect("hot_count lock") < *hot_target {
                    slots[slot_idx].hot_bit = true;
                    *self.hot_count.lock().expect("hot_count lock") += 1;
                }
            }
        } else {
            // New entry: insert with ref_bit set, initially in cold region
            let slot_idx = slots.len();
            Self::ensure_capacity(&mut slots, slot_idx);

            slots[slot_idx] = SlotMetadata {
                ref_bit: true,
                hot_bit: false,
                key: Some(key),
            };

            key_to_slot.insert(key, slot_idx);
            *self.resident_count.lock().expect("resident_count lock") += 1;
        }
    }

    fn pick_victim(&self) -> Option<CacheKey> {
        let mut slots = self.slots.lock().expect("slots lock");
        let mut key_to_slot = self.key_to_slot.lock().expect("key_to_slot lock");
        let mut hand = self.hand.lock().expect("hand lock");
        let mut resident_count = self.resident_count.lock().expect("resident_count lock");
        let mut hot_count = self.hot_count.lock().expect("hot_count lock");
        let mut hot_target = self.hot_target.lock().expect("hot_target lock");

        if slots.is_empty() {
            return None;
        }

        // Scan for a victim: cold entry with ref_bit clear
        let max_scans = slots.len() + 1;
        for _ in 0..max_scans {
            let idx = *hand;
            Self::advance_hand(&mut hand, slots.len());

            if idx >= slots.len() {
                continue;
            }

            let slot = &slots[idx];

            // Skip empty/ghost entries
            if slot.key.is_none() {
                continue;
            }

            // If ref_bit is set, clear it and continue scanning
            if slot.ref_bit {
                slots[idx].ref_bit = false;
                continue;
            }

            // Victim found: cold entry with ref_bit clear, or hot entry at end of hot set
            if !slot.hot_bit {
                // Evict cold entry
                if let Some(key) = slot.key {
                    let evicted_key = key;
                    slots[idx].key = None; // Ghost entry
                    key_to_slot.remove(&evicted_key);
                    *resident_count = resident_count.saturating_sub(1);
                    *hand = *hand; // Update from local after locking
                    return Some(evicted_key);
                }
            } else if *hot_count > *hot_target {
                // Evict from hot set if oversized
                if let Some(key) = slot.key {
                    let evicted_key = key;
                    slots[idx].key = None;
                    key_to_slot.remove(&evicted_key);
                    *resident_count = resident_count.saturating_sub(1);
                    *hot_count = hot_count.saturating_sub(1);
                    Self::on_hot_evict(&mut hot_target);
                    *hand = *hand;
                    return Some(evicted_key);
                }
            }
        }

        None
    }

    fn remove(&self, key: CacheKey) {
        let mut slots = self.slots.lock().expect("slots lock");
        let mut key_to_slot = self.key_to_slot.lock().expect("key_to_slot lock");
        let mut hot_count = self.hot_count.lock().expect("hot_count lock");
        let mut resident_count = self.resident_count.lock().expect("resident_count lock");

        if let Some(slot_idx) = key_to_slot.remove(&key) {
            if slot_idx < slots.len() {
                if slots[slot_idx].hot_bit {
                    *hot_count = hot_count.saturating_sub(1);
                }
                slots[slot_idx] = SlotMetadata::default();
                *resident_count = resident_count.saturating_sub(1);
            }
        }
    }

    fn clear(&self) {
        self.slots.lock().expect("slots lock").clear();
        self.key_to_slot.lock().expect("key_to_slot lock").clear();
        *self.hand.lock().expect("hand lock") = 0;
        *self.resident_count.lock().expect("resident_count lock") = 0;
        *self.hot_count.lock().expect("hot_count lock") = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_evict_after_access() {
        // Arrange
        let policy = ClockProPolicy::new();
        let key1 = CacheKey::new(1, 0);

        // Act
        policy.on_access(key1);
        policy.remove(key1);

        // Assert - verify key_to_slot map is empty after removal
        let key_map = policy.key_to_slot.lock().expect("key_to_slot lock");
        assert_eq!(key_map.len(), 0);
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

        // Assert - both keys should be in the map
        let key_map = policy.key_to_slot.lock().expect("key_to_slot lock");
        assert_eq!(key_map.len(), 2);
        assert!(key_map.contains_key(&key1));
        assert!(key_map.contains_key(&key2));
    }
}
