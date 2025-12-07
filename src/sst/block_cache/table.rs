//! Fixed-capacity hash table with Robin Hood linear probing.
//!
//! This table is designed for the block cache where:
//! - Capacity is known upfront (sized per shard).
//! - Keys are `BlockKey` with a precomputed `shard_hash()`.
//! - We want minimal allocations and good cache locality.

use std::mem;

/// Index into the entries vector. `u32::MAX` means empty/invalid.
pub type EntryId = u32;

pub const INVALID_ENTRY: EntryId = u32::MAX;

/// A single bucket in the hash table.
#[derive(Clone, Copy)]
struct Bucket {
    /// Cached hash value (0 means empty bucket).
    hash: u64,
    /// Index into the external entries storage.
    entry_id: EntryId,
}

impl Bucket {
    const EMPTY: Self = Self { hash: 0, entry_id: INVALID_ENTRY };

    #[inline]
    fn is_empty(&self) -> bool {
        self.hash == 0
    }
}

/// Fixed-capacity hash table using Robin Hood linear probing.
///
/// This table does NOT store values directly—it maps `hash -> EntryId`.
/// The actual entries are stored externally in a `Vec<BlockEntry>`.
///
/// # Invariants
/// - `buckets.len()` is always a power of two.
/// - Load factor should stay below ~0.9 for good performance.
pub struct HashTable {
    buckets: Box<[Bucket]>,
    mask: usize,
    len: usize,
}

impl HashTable {
    /// Create a new hash table with the given capacity (rounded up to power of two).
    pub fn with_capacity(capacity: usize) -> Self {
        let capacity = capacity.max(16).next_power_of_two();
        let buckets = vec![Bucket::EMPTY; capacity].into_boxed_slice();
        Self {
            mask: capacity - 1,
            buckets,
            len: 0,
        }
    }

    /// Number of entries in the table.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Is the table empty?
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Capacity (number of buckets).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.buckets.len()
    }

    /// Lookup an entry by hash. Returns the `EntryId` if found.
    ///
    /// Caller must verify the actual key matches (hash collisions are possible).
    #[inline]
    pub fn get(&self, hash: u64) -> Option<EntryId> {
        if hash == 0 {
            return None; // 0 is reserved for empty
        }

        let mut idx = (hash as usize) & self.mask;
        let mut dist = 0usize;

        loop {
            let bucket = &self.buckets[idx];

            if bucket.is_empty() {
                return None;
            }

            if bucket.hash == hash {
                return Some(bucket.entry_id);
            }

            // Robin Hood: if current bucket's probe distance is less than ours,
            // our key cannot be further ahead.
            let bucket_dist = self.probe_distance(bucket.hash, idx);
            if bucket_dist < dist {
                return None;
            }

            idx = (idx + 1) & self.mask;
            dist += 1;

            // Safety valve (shouldn't happen if load factor < 1)
            if dist > self.buckets.len() {
                return None;
            }
        }
    }

    /// Insert a hash -> entry_id mapping. Returns the displaced entry_id if any
    /// (for Robin Hood, we may displace an existing entry and re-insert it).
    ///
    /// # Panics
    /// Panics if the table is full (load factor = 1.0).
    pub fn insert(&mut self, hash: u64, entry_id: EntryId) {
        debug_assert!(hash != 0, "hash 0 is reserved for empty buckets");
        debug_assert!(self.len < self.buckets.len(), "table is full");

        let mut idx = (hash as usize) & self.mask;
        let mut current = Bucket { hash, entry_id };
        let mut dist = 0usize;

        loop {
            if self.buckets[idx].is_empty() {
                self.buckets[idx] = current;
                self.len += 1;
                return;
            }

            // Robin Hood: swap if current probe distance > existing probe distance
            let bucket_hash = self.buckets[idx].hash;
            let bucket_dist = self.probe_distance(bucket_hash, idx);
            if bucket_dist < dist {
                mem::swap(&mut self.buckets[idx], &mut current);
                dist = bucket_dist;
            }

            idx = (idx + 1) & self.mask;
            dist += 1;
        }
    }

    /// Remove an entry by hash. Returns the removed `EntryId` if found.
    ///
    /// Uses backward-shift deletion to maintain Robin Hood invariants.
    pub fn remove(&mut self, hash: u64) -> Option<EntryId> {
        if hash == 0 {
            return None;
        }

        let mut idx = (hash as usize) & self.mask;
        let mut dist = 0usize;

        // Find the bucket
        loop {
            let bucket = &self.buckets[idx];

            if bucket.is_empty() {
                return None;
            }

            if bucket.hash == hash {
                break;
            }

            let bucket_dist = self.probe_distance(bucket.hash, idx);
            if bucket_dist < dist {
                return None;
            }

            idx = (idx + 1) & self.mask;
            dist += 1;

            if dist > self.buckets.len() {
                return None;
            }
        }

        let removed_entry = self.buckets[idx].entry_id;
        self.len -= 1;

        // Backward-shift deletion
        let mut empty_idx = idx;
        loop {
            let next_idx = (empty_idx + 1) & self.mask;
            let next_bucket = &self.buckets[next_idx];

            if next_bucket.is_empty() {
                break;
            }

            let next_dist = self.probe_distance(next_bucket.hash, next_idx);
            if next_dist == 0 {
                break; // Next bucket is at its ideal position
            }

            // Shift backward
            self.buckets[empty_idx] = self.buckets[next_idx];
            empty_idx = next_idx;
        }

        self.buckets[empty_idx] = Bucket::EMPTY;
        Some(removed_entry)
    }

    /// Clear all entries.
    pub fn clear(&mut self) {
        self.buckets.fill(Bucket::EMPTY);
        self.len = 0;
    }

    /// Probe distance: how far this bucket is from its ideal position.
    #[inline]
    fn probe_distance(&self, hash: u64, current_idx: usize) -> usize {
        let ideal = (hash as usize) & self.mask;
        current_idx.wrapping_sub(ideal) & self.mask
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_retrieve_entry_given_inserted_value_when_queried() {
        // Arrange
        let mut table = HashTable::with_capacity(16);
        table.insert(12345, 0);

        // Act
        let result = table.get(12345);

        // Assert
        assert_eq!(result, Some(0));
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn should_return_none_given_missing_key_when_queried() {
        let table = HashTable::with_capacity(16);
        assert_eq!(table.get(99999), None);
    }

    #[test]
    fn should_handle_collisions_given_same_bucket_when_inserted() {
        // Arrange
        let mut table = HashTable::with_capacity(16);
        // These will collide (same lower bits)
        let h1 = 0x10;
        let h2 = 0x20; // different hash but may probe nearby

        // Act
        table.insert(h1, 1);
        table.insert(h2, 2);

        // Assert
        assert_eq!(table.get(h1), Some(1));
        assert_eq!(table.get(h2), Some(2));
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn should_remove_entry_given_existing_key_when_removed() {
        // Arrange
        let mut table = HashTable::with_capacity(16);
        table.insert(111, 5);
        table.insert(222, 6);

        // Act
        let removed = table.remove(111);

        // Assert
        assert_eq!(removed, Some(5));
        assert_eq!(table.get(111), None);
        assert_eq!(table.get(222), Some(6));
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn should_return_none_given_missing_key_when_removed() {
        // Arrange
        let mut table = HashTable::with_capacity(16);
        table.insert(111, 5);

        // Act
        let removed = table.remove(999);

        // Assert
        assert_eq!(removed, None);
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn should_clear_all_given_populated_table_when_cleared() {
        // Arrange
        let mut table = HashTable::with_capacity(16);
        table.insert(1, 0);
        table.insert(2, 1);
        table.insert(3, 2);

        // Act
        table.clear();

        // Assert
        assert!(table.is_empty());
        assert_eq!(table.get(1), None);
    }

    #[test]
    fn should_handle_high_load_given_many_entries_when_inserted() {
        // Arrange
        let mut table = HashTable::with_capacity(64);

        // Act - Insert 50 entries (load factor ~0.78)
        for i in 1u64..=50 {
            table.insert(i * 1000 + 1, i as EntryId); // +1 to avoid hash=0
        }

        // Assert
        assert_eq!(table.len(), 50);

        // Verify all are retrievable
        for i in 1u64..=50 {
            assert_eq!(table.get(i * 1000 + 1), Some(i as EntryId));
        }
    }

    #[test]
    fn should_maintain_invariants_given_insert_remove_cycles_when_stressed() {
        // Arrange
        let mut table = HashTable::with_capacity(32);

        for i in 1u64..=20 {
            table.insert(i, i as EntryId);
        }

        // Act - Remove odds
        for i in (1u64..=20).step_by(2) {
            table.remove(i);
        }

        // Assert
        assert_eq!(table.len(), 10);

        // Evens should still be there
        for i in (2u64..=20).step_by(2) {
            assert_eq!(table.get(i), Some(i as EntryId));
        }

        // Odds should be gone
        for i in (1u64..=20).step_by(2) {
            assert_eq!(table.get(i), None);
        }
    }
}
