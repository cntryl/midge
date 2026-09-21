//! Least Recently Used (LRU) eviction policy

use super::CachePolicy;
use crate::sst::cache::key::{CacheBlockKind, CacheKey};
use std::collections::HashMap;

/// LRU eviction policy backed by indexed intrusive lists.
///
/// Recency lives in a doubly-linked list held in a slot arena, so recording an
/// access and picking a victim are both O(1). A generation map with a scan for
/// the minimum made every eviction cost O(entries in the shard) while holding
/// the shard's mutation lock, which is what gated concurrent reads.
///
/// One list per block kind keeps victim selection O(1) even when the caller
/// protects metadata blocks: the oldest candidate is the oldest head among the
/// kinds that are not excluded, rather than a scan past protected entries.
pub struct LruPolicy {
    state: parking_lot::Mutex<LruState>,
}

/// Position of one tracked key inside the arena.
struct Slot {
    key: CacheKey,
    /// Older neighbour in this key's list, or `None` at the head.
    previous: Option<usize>,
    /// Newer neighbour in this key's list, or `None` at the tail.
    next: Option<usize>,
    /// Access order across all lists, used to compare heads of different kinds.
    sequence: u64,
}

/// Head (least recently used) and tail (most recently used) of one list.
#[derive(Default, Clone, Copy)]
struct ListEnds {
    head: Option<usize>,
    tail: Option<usize>,
}

struct LruState {
    slots: Vec<Option<Slot>>,
    free_slots: Vec<usize>,
    index: HashMap<CacheKey, usize>,
    /// One list per block kind: Index, Data, Filter.
    lists: [ListEnds; 3],
    sequence: u64,
    /// Entries examined by the most recent victim selection, so a test can
    /// assert the cost does not grow with the number of tracked entries.
    #[cfg(test)]
    examined_by_last_pick: usize,
}

fn list_of(kind: CacheBlockKind) -> usize {
    match kind {
        CacheBlockKind::Index => 0,
        CacheBlockKind::Data => 1,
        CacheBlockKind::Filter => 2,
    }
}

const KINDS: [CacheBlockKind; 3] = [
    CacheBlockKind::Index,
    CacheBlockKind::Data,
    CacheBlockKind::Filter,
];

impl LruState {
    fn new() -> Self {
        Self {
            slots: Vec::new(),
            free_slots: Vec::new(),
            index: HashMap::new(),
            lists: [ListEnds::default(); 3],
            sequence: 0,
            #[cfg(test)]
            examined_by_last_pick: 0,
        }
    }

    fn slot(&self, position: usize) -> &Slot {
        self.slots[position].as_ref().expect("live slot")
    }

    fn slot_mut(&mut self, position: usize) -> &mut Slot {
        self.slots[position].as_mut().expect("live slot")
    }

    /// Unlink a slot from its list, leaving the slot itself allocated.
    fn unlink(&mut self, position: usize) {
        let (previous, next, list) = {
            let slot = self.slot(position);
            (slot.previous, slot.next, list_of(slot.key.block_type))
        };
        match previous {
            Some(previous) => self.slot_mut(previous).next = next,
            None => self.lists[list].head = next,
        }
        match next {
            Some(next) => self.slot_mut(next).previous = previous,
            None => self.lists[list].tail = previous,
        }
        let slot = self.slot_mut(position);
        slot.previous = None;
        slot.next = None;
    }

    /// Append a slot at the most-recently-used end of its list.
    fn link_as_newest(&mut self, position: usize) {
        let list = list_of(self.slot(position).key.block_type);
        let previous_tail = self.lists[list].tail;
        self.slot_mut(position).previous = previous_tail;
        self.slot_mut(position).next = None;
        match previous_tail {
            Some(tail) => self.slot_mut(tail).next = Some(position),
            None => self.lists[list].head = Some(position),
        }
        self.lists[list].tail = Some(position);
    }

    fn touch(&mut self, key: CacheKey) {
        self.sequence += 1;
        let sequence = self.sequence;
        if let Some(&position) = self.index.get(&key) {
            self.unlink(position);
            self.slot_mut(position).sequence = sequence;
            self.link_as_newest(position);
            return;
        }
        let slot = Slot {
            key,
            previous: None,
            next: None,
            sequence,
        };
        let position = if let Some(free) = self.free_slots.pop() {
            self.slots[free] = Some(slot);
            free
        } else {
            self.slots.push(Some(slot));
            self.slots.len() - 1
        };
        self.index.insert(key, position);
        self.link_as_newest(position);
    }

    fn release(&mut self, position: usize) {
        self.unlink(position);
        let key = self.slot(position).key;
        self.index.remove(&key);
        self.slots[position] = None;
        self.free_slots.push(position);
    }

    fn remove(&mut self, key: CacheKey) {
        if let Some(&position) = self.index.get(&key) {
            self.release(position);
        }
    }

    /// The least recently used key that is not of an excluded kind.
    fn oldest(&mut self, exclude_types: &[CacheBlockKind]) -> Option<usize> {
        #[cfg(test)]
        {
            self.examined_by_last_pick = 0;
        }
        let candidates: Vec<usize> = KINDS
            .iter()
            .filter(|kind| !exclude_types.contains(kind))
            .filter_map(|kind| self.lists[list_of(*kind)].head)
            .collect();
        #[cfg(test)]
        {
            self.examined_by_last_pick = candidates.len();
        }
        candidates
            .into_iter()
            .min_by_key(|position| self.slot(*position).sequence)
    }
}

impl LruPolicy {
    /// Create a new LRU policy
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: parking_lot::Mutex::new(LruState::new()),
        }
    }
}

impl Default for LruPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl CachePolicy for LruPolicy {
    /// Record access to a key, making it the most recently used.
    #[inline]
    fn on_access(&self, key: CacheKey) {
        self.state.lock().touch(key);
    }

    /// Pick the least recently used key that is not of an excluded kind.
    ///
    /// The selected key is removed from tracking, matching the previous
    /// behaviour that `on_stale` relies on.
    fn pick_victim(&self, exclude_types: &[CacheBlockKind]) -> Option<CacheKey> {
        let mut state = self.state.lock();
        let position = state.oldest(exclude_types)?;
        let key = state.slot(position).key;
        state.release(position);
        Some(key)
    }

    /// Remove a key from tracking
    #[inline]
    fn on_remove(&self, key: CacheKey) {
        self.state.lock().remove(key);
    }

    /// Mark a key as stale.
    ///
    /// `pick_victim` already removed its selected key, so a stale cache entry
    /// has no tracking left to clean up. Removing here would erase a fresh
    /// concurrent access instead.
    #[inline]
    fn on_stale(&self, key: CacheKey) {
        let _ = key;
    }

    /// Clear all state
    fn clear(&self) {
        let mut state = self.state.lock();
        *state = LruState::new();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::thread;

    #[test]
    fn should_evict_least_recently_used() {
        // Arrange
        let policy = LruPolicy::new();
        let key1 = CacheKey::for_data(1, 0);
        let key2 = CacheKey::for_data(2, 0);
        let key3 = CacheKey::for_data(3, 0);

        // Act
        policy.on_access(key1);
        policy.on_access(key2);
        policy.on_access(key3);
        let victim = policy.pick_victim(&[]);

        // Assert - key1 should be evicted (least recently used)
        assert_eq!(victim, Some(key1));
    }

    #[test]
    fn should_update_lru_on_reaccess() {
        // Arrange
        let policy = LruPolicy::new();
        let key1 = CacheKey::for_data(1, 0);
        let key2 = CacheKey::for_data(2, 0);

        // Act
        policy.on_access(key1);
        policy.on_access(key2);
        policy.on_access(key1); // Re-access key1 (move to end)
        let victim = policy.pick_victim(&[]);

        // Assert - key2 should be evicted (now least recently used)
        assert_eq!(victim, Some(key2));
    }

    #[test]
    fn should_remove_key_from_tracking() {
        // Arrange
        let policy = LruPolicy::new();
        let key1 = CacheKey::for_data(1, 0);
        let key2 = CacheKey::for_data(2, 0);

        // Act
        policy.on_access(key1);
        policy.on_access(key2);
        policy.on_remove(key1);
        let victim = policy.pick_victim(&[]);

        // Assert - key2 should be evicted (key1 was removed)
        assert_eq!(victim, Some(key2));
    }

    // ===== New comprehensive tests =====

    #[test]
    fn should_clear_all_state() {
        // Arrange
        let policy = LruPolicy::new();
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
        let policy = LruPolicy::new();

        // Act
        let victim = policy.pick_victim(&[]);

        // Assert
        assert!(victim.is_none());
    }

    #[test]
    fn should_have_default_instance() {
        // Arrange
        let policy = LruPolicy::default();

        // Act
        policy.on_access(CacheKey::for_data(1, 0));
        let victim = policy.pick_victim(&[]);

        // Assert
        assert_eq!(victim, Some(CacheKey::for_data(1, 0)));
    }

    #[test]
    fn should_handle_fifo_order_for_sequential_accesses() {
        // Arrange
        let policy = LruPolicy::new();
        let keys: Vec<CacheKey> = (1..=10).map(|i| CacheKey::for_data(i, 0)).collect();

        // Act
        for key in &keys {
            policy.on_access(*key);
        }

        // Assert - evict in FIFO order
        for key in &keys {
            let victim = policy.pick_victim(&[]);
            assert_eq!(victim, Some(*key));
        }
    }

    #[test]
    fn should_handle_mixed_accesses_with_removals() {
        // Arrange
        let policy = LruPolicy::new();
        let key1 = CacheKey::for_data(1, 0);
        let key2 = CacheKey::for_data(2, 0);
        let key3 = CacheKey::for_data(3, 0);

        // Act
        policy.on_access(key1);
        policy.on_access(key2);
        policy.on_remove(key2);
        policy.on_access(key3);
        let victim1 = policy.pick_victim(&[]);
        let victim2 = policy.pick_victim(&[]);

        // Assert
        assert_eq!(victim1, Some(key1));
        assert_eq!(victim2, Some(key3));
    }

    #[test]
    fn should_move_key_to_end_on_reaccess() {
        // Arrange
        let policy = LruPolicy::new();
        let keys: Vec<CacheKey> = (1..=5).map(|i| CacheKey::for_data(i, 0)).collect();

        // Act
        for key in &keys {
            policy.on_access(*key);
        }
        // Re-access middle key
        policy.on_access(keys[1]); // key 2

        // Assert - victim should be key 1 (oldest after re-access)
        let victim = policy.pick_victim(&[]);
        assert_eq!(victim, Some(keys[0]));
    }

    #[test]
    fn should_remove_nonexistent_key_safely() {
        // Arrange
        let policy = LruPolicy::new();

        // Act
        policy.on_remove(CacheKey::for_data(999, 999)); // Remove non-existent key
        let victim = policy.pick_victim(&[]);

        // Assert - should not panic
        assert!(victim.is_none());
    }

    #[test]
    fn should_handle_duplicate_accesses() {
        // Arrange
        let policy = LruPolicy::new();
        let key = CacheKey::for_data(1, 0);

        // Act
        policy.on_access(key);
        policy.on_access(key); // Access again
        policy.on_access(key); // And again
        let victim = policy.pick_victim(&[]);

        // Assert - key should still be evicted once
        assert_eq!(victim, Some(key));
        assert_eq!(policy.pick_victim(&[]), None);
    }

    #[test]
    fn should_preserve_fresh_access_when_stale_victim_is_reported() {
        // Arrange
        let policy = LruPolicy::new();
        let key = CacheKey::for_data(1, 0);

        // Act
        policy.on_access(key);
        assert_eq!(policy.pick_victim(&[]), Some(key));
        policy.on_access(key);
        policy.on_stale(key);

        // Assert - stale cleanup must not erase the new synchronous access.
        assert_eq!(policy.pick_victim(&[]), Some(key));
    }

    #[test]
    fn should_evict_cold_key_after_concurrent_hot_key_accesses() {
        // Arrange
        let policy = std::sync::Arc::new(LruPolicy::new());
        let cold_key = CacheKey::for_data(1, 0);
        let hot_key = CacheKey::for_data(2, 0);
        policy.on_access(cold_key);
        policy.on_access(hot_key);
        let barrier = std::sync::Arc::new(Barrier::new(9));
        let mut workers = Vec::new();

        // Act
        for _ in 0..8 {
            let policy = std::sync::Arc::clone(&policy);
            let barrier = std::sync::Arc::clone(&barrier);
            workers.push(thread::spawn(move || {
                barrier.wait();
                for _ in 0..1_000 {
                    policy.on_access(hot_key);
                }
            }));
        }
        barrier.wait();
        for worker in workers {
            worker.join().expect("hot-key worker should not panic");
        }

        // Assert
        assert_eq!(policy.pick_victim(&[]), Some(cold_key));
        assert_eq!(policy.pick_victim(&[]), Some(hot_key));
    }
    /// Eviction used to scan every tracked key to find the minimum
    /// generation, while holding the shard's mutation lock. Victim selection
    /// must cost the same whether the shard holds ten entries or a hundred
    /// thousand.
    #[test]
    fn should_examine_a_fixed_number_of_entries_when_picking_a_victim() {
        // Arrange
        let policy = LruPolicy::new();
        for id in 0..100_000u64 {
            policy.on_access(CacheKey::for_data(id, 0));
        }

        // Act
        let victim = policy.pick_victim(&[]);
        let examined = policy.state.lock().examined_by_last_pick;

        // Assert
        assert_eq!(victim, Some(CacheKey::for_data(0, 0)));
        assert!(
            examined <= KINDS.len(),
            "victim selection examined {examined} entries"
        );
    }

    /// Protecting metadata must not turn selection into a scan past every
    /// protected entry either.
    #[test]
    fn should_examine_a_fixed_number_of_entries_when_metadata_is_protected() {
        // Arrange
        let policy = LruPolicy::new();
        for id in 0..10_000u64 {
            policy.on_access(CacheKey::for_index(id, 0));
            policy.on_access(CacheKey::for_filter(id, 8));
        }
        policy.on_access(CacheKey::for_data(1, 16));

        // Act
        let victim = policy.pick_victim(&[CacheBlockKind::Index, CacheBlockKind::Filter]);
        let examined = policy.state.lock().examined_by_last_pick;

        // Assert
        assert_eq!(victim, Some(CacheKey::for_data(1, 16)));
        assert!(
            examined <= KINDS.len(),
            "victim selection examined {examined} entries"
        );
    }
}
