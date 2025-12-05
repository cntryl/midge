//! Simple LRU (Least Recently Used) eviction policy.
//!
//! This is a baseline policy. For production, consider WTinyLFU or Clock-Pro
//! which offer better scan resistance, but this implementation is now
//! O(1) for access/insert/evict and suitable for hot paths.

use super::{EntryId, Policy};

#[derive(Clone, Copy, Debug, Default)]
struct Node {
    prev: Option<EntryId>,
    next: Option<EntryId>,
    present: bool,
}

/// LRU eviction policy backed by an intrusive doubly linked list.
///
/// - `head` is the LRU entry
/// - `tail` is the MRU entry
pub struct LruPolicy {
    nodes: Vec<Node>,
    head: Option<EntryId>,
    tail: Option<EntryId>,
    /// Soft hint about expected max entries (used only for initial capacity).
    #[allow(dead_code)]
    max_entries: usize,
}

impl LruPolicy {
    /// Create a new LRU policy.
    pub fn new(max_entries: usize) -> Self {
        Self {
            nodes: Vec::with_capacity(max_entries.min(1024)),
            head: None,
            tail: None,
            max_entries,
        }
    }

    /// Ensure the slots vector can hold the given entry ID.
    ///
    /// Note: This assumes `EntryId` values are dense and start from 0,
    /// matching the `entries: Vec<Option<BlockEntry>>` layout in the shard.
    #[inline]
    fn ensure_capacity(&mut self, id: EntryId) {
        let idx = id as usize;
        if idx >= self.nodes.len() {
            self.nodes.resize(idx + 1, Node::default());
        }
    }

    #[inline]
    fn node(&self, id: EntryId) -> &Node {
        &self.nodes[id as usize]
    }

    #[inline]
    fn node_mut(&mut self, id: EntryId) -> &mut Node {
        &mut self.nodes[id as usize]
    }

    /// Unlink a node from the list, if present.
    fn unlink(&mut self, id: EntryId) {
        self.ensure_capacity(id);
        if !self.node(id).present {
            return;
        }

        let (prev, next);
        {
            let node = self.node(id);
            prev = node.prev;
            next = node.next;
        }

        // Fix prev.next
        if let Some(p) = prev {
            self.node_mut(p).next = next;
        } else {
            // This was head
            self.head = next;
        }

        // Fix next.prev
        if let Some(n) = next {
            self.node_mut(n).prev = prev;
        } else {
            // This was tail
            self.tail = prev;
        }

        let n = self.node_mut(id);
        n.prev = None;
        n.next = None;
        n.present = false;
    }

    /// Link a node at the tail (MRU position).
    fn link_at_tail(&mut self, id: EntryId) {
        self.ensure_capacity(id);

        // If already present, unlink first to avoid duplicates.
        if self.node(id).present {
            self.unlink(id);
        }

        let old_tail = self.tail;
        let node = self.node_mut(id);

        node.prev = old_tail;
        node.next = None;
        node.present = true;

        if let Some(t) = old_tail {
            self.node_mut(t).next = Some(id);
        } else {
            // List was empty
            self.head = Some(id);
        }

        self.tail = Some(id);
    }
}

impl Policy for LruPolicy {
    fn on_access(&mut self, entry_id: EntryId) {
        // Move to MRU (tail)
        self.link_at_tail(entry_id);
    }

    fn on_insert(&mut self, entry_id: EntryId, _size: usize) {
        // New entries are MRU
        self.link_at_tail(entry_id);
    }

    fn on_evict(&mut self, entry_id: EntryId) {
        // Remove from list entirely
        self.unlink(entry_id);
    }

    fn choose_victim(&mut self) -> Option<EntryId> {
        // LRU is at head
        if let Some(id) = self.head {
            debug_assert!(self.node(id).present, "head must always be present");
            Some(id)
        } else {
            None
        }
    }

    fn clear(&mut self) {
        self.nodes.clear();
        self.head = None;
        self.tail = None;
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
