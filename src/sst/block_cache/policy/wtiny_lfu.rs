//! Windowed TinyLFU eviction policy.
//!
//! WTinyLFU combines recency (window LRU) with frequency (main segment) to
//! achieve high hit rates while resisting scan pollution. New entries start
//! in a small window segment; when evicted from the window, they compete
//! for admission to the main segment based on frequency.
//!
//! Segments:
//! - **Window** (~1% of capacity): Small LRU for recent entries.
//! - **Probation** (~20% of main): New entries from window, LRU eviction.
//! - **Protected** (~80% of main): Frequently accessed entries.

use std::collections::VecDeque;

use super::{EntryId, Policy};
use crate::sst::block_cache::admission::FrequencySketch;

/// Windowed TinyLFU eviction policy.
pub struct WTinyLfuPolicy {
    /// Window segment (small, LRU).
    window: VecDeque<EntryId>,
    /// Probation segment (new entries from window).
    probation: VecDeque<EntryId>,
    /// Protected segment (frequently accessed).
    protected: VecDeque<EntryId>,

    /// Frequency sketch for admission decisions.
    sketch: FrequencySketch,

    /// Maximum entries in window (~1% of total).
    window_capacity: usize,
    /// Maximum entries in protected (~80% of main).
    protected_capacity: usize,
}

impl WTinyLfuPolicy {
    /// Create a new WTinyLFU policy sized for the expected number of entries.
    pub fn new(expected_entries: usize) -> Self {
        // Ensure minimum viable capacity
        let expected_entries = expected_entries.max(10);
        let window_capacity = (expected_entries / 100).max(1); // 1%
        let main_capacity = expected_entries.saturating_sub(window_capacity);
        let protected_capacity = (main_capacity * 80) / 100; // 80% of main

        Self {
            window: VecDeque::with_capacity(window_capacity),
            probation: VecDeque::with_capacity(main_capacity.saturating_sub(protected_capacity)),
            protected: VecDeque::with_capacity(protected_capacity),
            sketch: FrequencySketch::new(expected_entries),
            window_capacity,
            protected_capacity,
        }
    }

    /// Remove an entry from all segments.
    fn remove_from_all(&mut self, entry_id: EntryId) {
        if let Some(pos) = self.window.iter().position(|&id| id == entry_id) {
            self.window.remove(pos);
            return;
        }
        if let Some(pos) = self.probation.iter().position(|&id| id == entry_id) {
            self.probation.remove(pos);
            return;
        }
        if let Some(pos) = self.protected.iter().position(|&id| id == entry_id) {
            self.protected.remove(pos);
        }
    }

    /// Find which segment an entry is in.
    fn find_segment(&self, entry_id: EntryId) -> Option<Segment> {
        if self.window.contains(&entry_id) {
            Some(Segment::Window)
        } else if self.probation.contains(&entry_id) {
            Some(Segment::Probation)
        } else if self.protected.contains(&entry_id) {
            Some(Segment::Protected)
        } else {
            None
        }
    }

    /// Promote from probation to protected on access.
    fn promote_to_protected(&mut self, entry_id: EntryId) {
        // Remove from probation
        if let Some(pos) = self.probation.iter().position(|&id| id == entry_id) {
            self.probation.remove(pos);
        }

        // If protected is full, demote LRU to probation
        if self.protected.len() >= self.protected_capacity {
            if let Some(demoted) = self.protected.pop_front() {
                self.probation.push_back(demoted);
            }
        }

        // Add to protected (MRU)
        self.protected.push_back(entry_id);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Segment {
    Window,
    Probation,
    Protected,
}

impl Policy for WTinyLfuPolicy {
    fn on_access(&mut self, entry_id: EntryId) {
        // Record in frequency sketch (use entry_id as hash for simplicity)
        self.sketch.increment(entry_id as u64);

        match self.find_segment(entry_id) {
            Some(Segment::Window) => {
                // Move to back of window (MRU)
                if let Some(pos) = self.window.iter().position(|&id| id == entry_id) {
                    self.window.remove(pos);
                    self.window.push_back(entry_id);
                }
            }
            Some(Segment::Probation) => {
                // Promote to protected
                self.promote_to_protected(entry_id);
            }
            Some(Segment::Protected) => {
                // Move to back of protected (MRU)
                if let Some(pos) = self.protected.iter().position(|&id| id == entry_id) {
                    self.protected.remove(pos);
                    self.protected.push_back(entry_id);
                }
            }
            None => {
                // Entry not tracked (shouldn't happen in normal operation)
            }
        }
    }

    fn on_insert(&mut self, entry_id: EntryId, _size: usize) {
        // Record in frequency sketch
        self.sketch.increment(entry_id as u64);

        // New entries go to window
        self.window.push_back(entry_id);

        // If window is full, evict to main (probation)
        while self.window.len() > self.window_capacity {
            if let Some(evicted) = self.window.pop_front() {
                // Candidate for main segment - add to probation
                self.probation.push_back(evicted);
            }
        }
    }

    fn on_evict(&mut self, entry_id: EntryId) {
        self.remove_from_all(entry_id);
    }

    fn choose_victim(&mut self) -> Option<EntryId> {
        // First try probation (new entries that haven't proven themselves)
        if let Some(&victim) = self.probation.front() {
            return Some(victim);
        }

        // Then try window
        if let Some(&victim) = self.window.front() {
            return Some(victim);
        }

        // Finally try protected (least recently used protected entry)
        if let Some(&victim) = self.protected.front() {
            return Some(victim);
        }

        None
    }

    fn clear(&mut self) {
        self.window.clear();
        self.probation.clear();
        self.protected.clear();
        self.sketch.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_insert_to_window_given_new_entry_when_inserted() {
        let mut policy = WTinyLfuPolicy::new(100);

        policy.on_insert(1, 100);

        assert!(policy.window.contains(&1));
    }

    #[test]
    fn should_move_to_probation_given_window_full_when_new_insert() {
        let mut policy = WTinyLfuPolicy::new(100);
        // Window capacity is 1% of 100 = 1

        policy.on_insert(1, 100);
        policy.on_insert(2, 100); // Should push 1 to probation

        assert!(policy.probation.contains(&1));
        assert!(policy.window.contains(&2));
    }

    #[test]
    fn should_promote_to_protected_given_probation_access_when_accessed() {
        let mut policy = WTinyLfuPolicy::new(100);

        // Fill window to push to probation
        policy.on_insert(1, 100);
        policy.on_insert(2, 100); // 1 goes to probation

        // Access entry in probation
        policy.on_access(1);

        assert!(policy.protected.contains(&1));
        assert!(!policy.probation.contains(&1));
    }

    #[test]
    fn should_choose_probation_victim_given_entries_in_all_segments() {
        let mut policy = WTinyLfuPolicy::new(100);

        policy.on_insert(1, 100);
        policy.on_insert(2, 100); // 1 to probation, 2 in window

        let victim = policy.choose_victim();
        assert_eq!(victim, Some(1)); // Probation first
    }

    #[test]
    fn should_clear_all_segments_given_populated_policy_when_cleared() {
        let mut policy = WTinyLfuPolicy::new(100);

        policy.on_insert(1, 100);
        policy.on_insert(2, 100);
        policy.on_access(1); // promote to protected

        policy.clear();

        assert!(policy.window.is_empty());
        assert!(policy.probation.is_empty());
        assert!(policy.protected.is_empty());
    }
}
