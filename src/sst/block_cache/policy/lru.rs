//! Simple LRU (Least Recently Used) eviction policy.
//!
//! This is a baseline policy. For production, consider WTinyLFU or Clock-Pro
//! which offer better scan resistance.

use std::collections::VecDeque;

use super::{EntryId, Policy};

/// LRU eviction policy using a simple deque.
///
/// Most recently accessed entries are at the back; LRU entries are at the front.
pub struct LruPolicy {
    /// Ordered list of entry IDs (front = LRU, back = MRU).
    order: VecDeque<EntryId>,
    /// Maximum entries to track (soft limit for the deque).
    /// Reserved for future capacity-based decisions.
    #[allow(dead_code)]
    max_entries: usize,
}

impl LruPolicy {
    /// Create a new LRU policy.
    pub fn new(max_entries: usize) -> Self {
        Self {
            order: VecDeque::with_capacity(max_entries.min(1024)),
            max_entries,
        }
    }

    /// Remove an entry from the order list (if present).
    fn remove_from_order(&mut self, entry_id: EntryId) {
        if let Some(pos) = self.order.iter().position(|&id| id == entry_id) {
            self.order.remove(pos);
        }
    }
}

impl Policy for LruPolicy {
    fn on_access(&mut self, entry_id: EntryId) {
        // Move to back (MRU position)
        self.remove_from_order(entry_id);
        self.order.push_back(entry_id);
    }

    fn on_insert(&mut self, entry_id: EntryId, _size: usize) {
        // New entries go to back (MRU)
        self.order.push_back(entry_id);
    }

    fn on_evict(&mut self, entry_id: EntryId) {
        self.remove_from_order(entry_id);
    }

    fn choose_victim(&mut self) -> Option<EntryId> {
        // Return the LRU entry (front of deque)
        self.order.front().copied()
    }

    fn clear(&mut self) {
        self.order.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_evict_oldest_given_lru_order_when_victim_chosen() {
        let mut policy = LruPolicy::new(100);

        policy.on_insert(0, 100);
        policy.on_insert(1, 100);
        policy.on_insert(2, 100);

        // Entry 0 is LRU
        assert_eq!(policy.choose_victim(), Some(0));
    }

    #[test]
    fn should_update_order_given_access_when_entry_touched() {
        let mut policy = LruPolicy::new(100);

        policy.on_insert(0, 100);
        policy.on_insert(1, 100);
        policy.on_insert(2, 100);

        // Access entry 0, making it MRU
        policy.on_access(0);

        // Now entry 1 is LRU
        assert_eq!(policy.choose_victim(), Some(1));
    }

    #[test]
    fn should_remove_from_order_given_eviction_when_entry_evicted() {
        let mut policy = LruPolicy::new(100);

        policy.on_insert(0, 100);
        policy.on_insert(1, 100);

        policy.on_evict(0);

        // Entry 1 is now LRU (and only entry)
        assert_eq!(policy.choose_victim(), Some(1));
    }

    #[test]
    fn should_return_none_given_empty_policy_when_victim_chosen() {
        let mut policy = LruPolicy::new(100);
        assert_eq!(policy.choose_victim(), None);
    }

    #[test]
    fn should_clear_all_given_populated_policy_when_cleared() {
        let mut policy = LruPolicy::new(100);
        policy.on_insert(0, 100);
        policy.on_insert(1, 100);

        policy.clear();

        assert_eq!(policy.choose_victim(), None);
    }
}
