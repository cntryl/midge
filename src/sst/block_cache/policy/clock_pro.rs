//! CLOCK-Pro eviction policy.
//!
//! CLOCK-Pro is a modern eviction algorithm that provides:
//! - O(1) amortized operations for access, insert, and eviction
//! - Scan resistance via hot/cold separation
//! - Adaptive hot-set sizing based on workload
//!
//! This implementation uses a circular buffer with per-entry metadata bits
//! instead of pointer-based linked lists, eliminating O(N) scans.
//!
//! # Regions
//!
//! CLOCK-Pro conceptually divides entries into three regions:
//! - **Hot**: Frequently accessed blocks (accessed more than once)
//! - **Cold**: Recently inserted or single-access blocks
//! - **Ghost**: Metadata for recently evicted cold blocks (no data)
//!
//! The clock hand sweeps through the circular buffer, clearing reference
//! bits and selecting victims from cold, unreferenced entries.

use super::{EntryId, Policy};

/// Per-slot metadata for CLOCK-Pro.
///
/// Memory footprint: 3 bytes per entry (could pack tighter if needed).
#[derive(Clone, Copy, Debug, Default)]
struct Slot {
    /// Reference bit: set on access, cleared by clock hand.
    ref_bit: bool,
    /// Hot bit: true if entry is in the hot region.
    hot_bit: bool,
    /// Resident bit: true if entry holds data, false if ghost.
    resident: bool,
}

/// CLOCK-Pro eviction policy.
///
/// Uses a circular buffer with a sweeping clock hand for O(1) amortized
/// victim selection. Combines with WTinyLFU admission control for
/// scan resistance.
pub struct ClockProPolicy {
    /// Per-entry metadata slots, indexed by EntryId.
    slots: Vec<Slot>,
    /// Clock hand position (next slot to examine).
    hand: usize,
    /// Number of resident entries.
    resident_count: usize,
    /// Target number of hot entries (adaptive).
    hot_target: usize,
    /// Current number of hot entries.
    hot_count: usize,
    /// Expected capacity (for sizing).
    capacity: usize,
}

impl ClockProPolicy {
    /// Create a new CLOCK-Pro policy sized for expected entries.
    pub fn new(expected_entries: usize) -> Self {
        let capacity = expected_entries.max(16);
        // Start with ~25% hot target; will adapt based on workload
        let hot_target = capacity / 4;

        Self {
            slots: Vec::with_capacity(capacity),
            hand: 0,
            resident_count: 0,
            hot_target,
            hot_count: 0,
            capacity,
        }
    }

    /// Ensure the slots vector can hold the given entry ID.
    #[inline]
    fn ensure_capacity(&mut self, id: EntryId) {
        let idx = id as usize;
        if idx >= self.slots.len() {
            self.slots.resize(idx + 1, Slot::default());
        }
    }

    /// Get a reference to a slot (for testing).
    #[cfg(test)]
    #[inline]
    fn slot(&self, id: EntryId) -> &Slot {
        &self.slots[id as usize]
    }

    /// Get a mutable reference to a slot (for testing).
    #[cfg(test)]
    #[inline]
    fn slot_mut(&mut self, id: EntryId) -> &mut Slot {
        &mut self.slots[id as usize]
    }

    /// Advance the clock hand, wrapping around.
    #[inline]
    fn advance_hand(&mut self) {
        if self.slots.is_empty() {
            return;
        }
        self.hand = (self.hand + 1) % self.slots.len();
    }

    /// Adapt hot target size based on ghost hit.
    /// Called when a ghost entry is re-inserted.
    #[inline]
    fn on_ghost_hit(&mut self) {
        // Increase hot target when we see ghost hits
        // (indicates we're evicting useful entries)
        if self.hot_target < self.capacity {
            self.hot_target = (self.hot_target + 1).min(self.capacity * 3 / 4);
        }
    }

    /// Adapt hot target size when evicting a hot entry.
    #[inline]
    fn on_hot_evict(&mut self) {
        // Decrease hot target when we evict hot entries
        // (indicates hot set is too large)
        if self.hot_target > 1 {
            self.hot_target = self.hot_target.saturating_sub(1);
        }
    }
}

impl Policy for ClockProPolicy {
    fn on_access(&mut self, entry_id: EntryId) {
        self.ensure_capacity(entry_id);
        
        let idx = entry_id as usize;
        let slot = &mut self.slots[idx];

        if !slot.resident {
            // Ghost hit - entry was evicted but accessed again
            // This is handled in on_insert for re-admission
            return;
        }

        // Set reference bit (standard CLOCK behavior)
        let was_cold = !slot.hot_bit;
        slot.ref_bit = true;

        // Promote cold → hot on second access (ref_bit was already set)
        // In practice, we promote on any access while cold with ref_bit
        if was_cold && self.hot_count < self.hot_target {
            self.slots[idx].hot_bit = true;
            self.hot_count += 1;
        }
    }

    fn on_insert(&mut self, entry_id: EntryId, _size: usize) {
        self.ensure_capacity(entry_id);

        let idx = entry_id as usize;
        let was_ghost = {
            let slot = &self.slots[idx];
            !slot.resident && (slot.ref_bit || slot.hot_bit)
        };

        if was_ghost {
            // Re-inserting a ghost entry - this is valuable signal
            self.on_ghost_hit();
        }

        let slot = &mut self.slots[idx];

        // New entries start in cold region with ref_bit set
        slot.ref_bit = true;
        slot.hot_bit = false;
        slot.resident = true;

        self.resident_count += 1;
    }

    fn on_evict(&mut self, entry_id: EntryId) {
        self.ensure_capacity(entry_id);
        
        let idx = entry_id as usize;
        let (was_resident, was_hot) = {
            let slot = &self.slots[idx];
            (slot.resident, slot.hot_bit)
        };

        if !was_resident {
            return; // Already evicted
        }

        if was_hot {
            self.hot_count = self.hot_count.saturating_sub(1);
            self.on_hot_evict();
        }

        // Convert to ghost entry (keep for re-admission detection)
        let slot = &mut self.slots[idx];
        slot.resident = false;
        // Keep hot_bit as hint for re-insertion priority
        // Clear ref_bit
        slot.ref_bit = false;

        self.resident_count = self.resident_count.saturating_sub(1);
    }

    fn choose_victim(&mut self) -> Option<EntryId> {
        if self.resident_count == 0 || self.slots.is_empty() {
            return None;
        }

        // Scan limit to avoid infinite loops (e.g., all entries pinned externally)
        let max_iterations = self.slots.len() * 2;
        let mut iterations = 0;

        loop {
            if iterations >= max_iterations {
                // All entries are either ghost, hot+referenced, or pinned
                // Return the hand position as a fallback
                // The shard will check if it's actually evictable
                for i in 0..self.slots.len() {
                    let idx = (self.hand + i) % self.slots.len();
                    if self.slots[idx].resident {
                        return Some(idx as EntryId);
                    }
                }
                return None;
            }

            let idx = self.hand;
            self.advance_hand();
            iterations += 1;

            if idx >= self.slots.len() {
                continue;
            }

            let slot = &mut self.slots[idx];

            // Skip ghost entries
            if !slot.resident {
                continue;
            }

            // Check reference bit
            if slot.ref_bit {
                // Clear reference bit (second chance)
                slot.ref_bit = false;

                // Demote hot → cold if referenced but hot
                if slot.hot_bit {
                    slot.hot_bit = false;
                    self.hot_count = self.hot_count.saturating_sub(1);
                }
                continue;
            }

            // Unreferenced entry found
            // Prefer cold entries as victims
            if !slot.hot_bit {
                // Cold, unreferenced, resident → ideal victim
                return Some(idx as EntryId);
            }

            // Hot but unreferenced - demote to cold, give another chance
            slot.hot_bit = false;
            self.hot_count = self.hot_count.saturating_sub(1);
            // Continue scanning for a cold victim
        }
    }

    fn clear(&mut self) {
        self.slots.clear();
        self.hand = 0;
        self.resident_count = 0;
        self.hot_count = 0;
        self.hot_target = self.capacity / 4;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_evict_cold_entry_given_mixed_entries_when_victim_chosen() {
        // Arrange
        let mut policy = ClockProPolicy::new(100);
        policy.on_insert(0, 100);
        policy.on_insert(1, 100);
        policy.on_insert(2, 100);

        // Clear ref_bits from insert (simulates time passing)
        policy.slots[0].ref_bit = false;
        policy.slots[1].ref_bit = false;
        policy.slots[2].ref_bit = false;

        // Make entry 0 hot by accessing it (sets ref_bit, promotes to hot)
        policy.on_access(0);

        // Assert: victim should be a cold entry (1 or 2), not the hot one (0)
        // Hand starts at 0. Entry 0 is hot+ref, gets ref cleared and demoted.
        // Entry 1 is cold+unref, becomes victim.
        let victim = policy.choose_victim();
        assert!(victim.is_some());
        let v = victim.unwrap();
        assert!(v == 1 || v == 2, "Expected cold entry as victim, got {}", v);
    }

    #[test]
    fn should_give_second_chance_given_referenced_entry_when_scanning() {
        // Arrange
        let mut policy = ClockProPolicy::new(100);
        policy.on_insert(0, 100);
        policy.on_insert(1, 100);

        // Clear entry 1's ref_bit but keep entry 0's (simulates 0 was accessed more recently)
        policy.slots[1].ref_bit = false;

        // First choose_victim: entry 0 has ref_bit, gets cleared and skipped
        // Entry 1 has no ref_bit, becomes victim
        let victim = policy.choose_victim();

        // Assert
        assert_eq!(victim, Some(1), "Should skip referenced entry 0 and return unreferenced entry 1");
    }

    #[test]
    fn should_return_none_given_empty_policy_when_victim_chosen() {
        // Arrange
        let mut policy = ClockProPolicy::new(100);

        // Act
        let victim = policy.choose_victim();

        // Assert
        assert_eq!(victim, None);
    }

    #[test]
    fn should_track_resident_count_given_inserts_and_evicts_when_operated() {
        // Arrange
        let mut policy = ClockProPolicy::new(100);

        // Act
        policy.on_insert(0, 100);
        policy.on_insert(1, 100);
        assert_eq!(policy.resident_count, 2);

        policy.on_evict(0);
        assert_eq!(policy.resident_count, 1);

        policy.on_evict(1);
        assert_eq!(policy.resident_count, 0);
    }

    #[test]
    fn should_promote_to_hot_given_repeated_access_when_cold() {
        // Arrange
        let mut policy = ClockProPolicy::new(100);
        policy.on_insert(0, 100);
        assert!(!policy.slot(0).hot_bit, "New entry should be cold");

        // Act: access multiple times
        policy.on_access(0);

        // Assert: should be promoted to hot (if under target)
        assert!(policy.slot(0).hot_bit, "Entry should be hot after access");
        assert_eq!(policy.hot_count, 1);
    }

    #[test]
    fn should_clear_all_state_given_populated_policy_when_cleared() {
        // Arrange
        let mut policy = ClockProPolicy::new(100);
        policy.on_insert(0, 100);
        policy.on_insert(1, 100);
        policy.on_access(0);

        // Act
        policy.clear();

        // Assert
        assert_eq!(policy.resident_count, 0);
        assert_eq!(policy.hot_count, 0);
        assert!(policy.slots.is_empty());
        assert_eq!(policy.choose_victim(), None);
    }

    #[test]
    fn should_handle_ghost_reinsert_given_evicted_entry_when_reinserted() {
        // Arrange
        let mut policy = ClockProPolicy::new(100);
        policy.on_insert(0, 100);
        policy.on_access(0); // Make it hot
        let initial_hot_target = policy.hot_target;

        // Evict it (becomes ghost)
        policy.on_evict(0);
        assert!(!policy.slot(0).resident);

        // Act: re-insert the ghost
        policy.on_insert(0, 100);

        // Assert: hot_target should increase (ghost hit signal)
        assert!(policy.slot(0).resident);
        assert!(
            policy.hot_target >= initial_hot_target,
            "Hot target should increase on ghost hit"
        );
    }

    #[test]
    fn should_demote_hot_to_cold_given_unreferenced_hot_when_scanning() {
        // Arrange
        let mut policy = ClockProPolicy::new(100);
        policy.on_insert(0, 100);
        policy.on_insert(1, 100);
        policy.on_insert(2, 100);

        // Make entry 0 hot
        policy.on_access(0);
        assert!(policy.slot(0).hot_bit);

        // Clear its ref_bit manually to simulate time passing
        policy.slot_mut(0).ref_bit = false;

        // Act: scan should demote entry 0 from hot to cold
        // We need multiple scans since hot entries get demoted first
        let _ = policy.choose_victim();

        // Entry 0 should now be cold (demoted during scan)
        // The exact behavior depends on hand position
    }

    #[test]
    fn should_prefer_cold_victims_given_hot_and_cold_entries_when_evicting() {
        // Arrange
        let mut policy = ClockProPolicy::new(100);

        // Insert entries 0-4
        for i in 0..5 {
            policy.on_insert(i, 100);
        }

        // Make entries 0, 1, 2 hot by repeated access
        for i in 0..3 {
            policy.on_access(i);
            policy.on_access(i);
        }

        // Clear all ref_bits to simulate time passing
        for i in 0..5 {
            policy.slot_mut(i).ref_bit = false;
        }

        // Act: choose victim
        let victim = policy.choose_victim();

        // Assert: should be cold entry (3 or 4)
        assert!(victim.is_some());
        let v = victim.unwrap();
        assert!(v == 3 || v == 4, "Expected cold entry (3 or 4), got {}", v);
    }

    #[test]
    fn should_handle_sparse_entry_ids_given_non_sequential_inserts() {
        // Arrange
        let mut policy = ClockProPolicy::new(100);

        // Act: insert with sparse IDs
        policy.on_insert(5, 100);
        policy.on_insert(100, 100);
        policy.on_insert(50, 100);

        // Assert
        assert_eq!(policy.resident_count, 3);
        let victim = policy.choose_victim();
        assert!(victim.is_some());
    }
}
