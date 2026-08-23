//! Admission helpers that can be used to gate optional cache policies.
//!
//! The production block cache insertion path no longer calls these helpers by default.
//! They remain available for experiments and alternative cache strategies.

//! Admission control uses a probabilistic counter to track the frequency
//! of keys. Keys that fail the admission check are not added to the cache.

use std::convert::TryFrom;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::sst::cache::{CacheBlockKind, CacheKey};

/// Probabilistic frequency counter for admission control
///
/// Uses a Bloom-like structure to estimate key frequencies and decide
/// whether to admit new blocks into the cache.
pub struct AdmissionCounter {
    /// Counter cells (hash table slots)
    cells: Arc<Vec<AtomicU64>>,
    /// Reset interval (number of accesses before resetting counters)
    reset_interval: u64,
    /// Total accesses since last reset
    access_count: AtomicU64,
}

impl AdmissionCounter {
    /// Create a new admission counter
    ///
    /// `num_cells`: Size of counter table (typically 64 or 128)
    /// `reset_interval`: How often to reset counters (typically 1000)
    #[must_use]
    pub fn new(num_cells: usize, reset_interval: u64) -> Self {
        let cells: Vec<AtomicU64> = (0..num_cells).map(|_| AtomicU64::new(0)).collect();
        Self {
            cells: Arc::new(cells),
            reset_interval,
            access_count: AtomicU64::new(0),
        }
    }

    /// Hash a byte key to a cell index
    fn hash_key(key: &[u8]) -> u64 {
        let mut h = 5381u64;
        for &b in key {
            h = h.wrapping_mul(33).wrapping_add(u64::from(b));
        }
        h
    }

    fn cache_key_identity(key: &CacheKey) -> [u8; 17] {
        let mut identity = [0u8; 17];
        identity[..8].copy_from_slice(&key.sst_id.to_le_bytes());
        identity[8..16].copy_from_slice(&key.block_offset.to_le_bytes());
        identity[16] = match key.block_type {
            CacheBlockKind::Index => 0,
            CacheBlockKind::Data => 1,
            CacheBlockKind::Filter => 2,
        };
        identity
    }

    /// Estimate if a key should be admitted
    ///
    /// Returns true if the key has been seen before (or randomly with threshold probability)
    pub fn estimate(&self, key: &[u8]) -> bool {
        let hash = Self::hash_key(key);
        let cell_idx = usize::try_from(hash).unwrap_or(0) % self.cells.len();
        let counter = self.cells[cell_idx].load(Ordering::Relaxed);

        // Admit if counter is non-zero (key has been seen before)
        counter > 0
    }

    /// Check if a cache key should be admitted based on block type
    ///
    /// Index and filter blocks are always admitted (no check).
    /// Data blocks are checked by full cache key identity.
    pub fn should_admit(&self, key: &crate::sst::cache::CacheKey) -> bool {
        match key.block_type {
            CacheBlockKind::Index | CacheBlockKind::Filter => {
                // Always admit index and filter blocks
                true
            }
            CacheBlockKind::Data => {
                // Data blocks: use the complete block identity so one hot block
                // does not admit every other block from the same SST.
                self.estimate(&Self::cache_key_identity(key))
            }
        }
    }

    /// Record access to a full cache key identity.
    pub fn record_access_for_key(&self, key: &CacheKey) {
        self.record_access(&Self::cache_key_identity(key));
    }

    /// Record an access to a key
    pub fn record_access(&self, key: &[u8]) {
        let hash = Self::hash_key(key);
        let cell_idx = usize::try_from(hash).unwrap_or(0) % self.cells.len();
        let old_count = self.cells[cell_idx].fetch_add(1, Ordering::Relaxed);

        // Periodically reset to avoid saturation
        let access_count = self.access_count.fetch_add(1, Ordering::Relaxed);
        if access_count.is_multiple_of(self.reset_interval) && old_count > 0 {
            // Reset all counters by dividing by 2
            for cell in self.cells.iter() {
                let val = cell.load(Ordering::Relaxed);
                cell.store(val / 2, Ordering::Relaxed);
            }
        }
    }
}

impl Clone for AdmissionCounter {
    fn clone(&self) -> Self {
        Self {
            cells: Arc::clone(&self.cells),
            reset_interval: self.reset_interval,
            access_count: AtomicU64::new(self.access_count.load(Ordering::Relaxed)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_reject_new_keys() {
        // Arrange
        let counter = AdmissionCounter::new(64, 1000);
        let key = b"new_key";

        // Act
        let admitted = counter.estimate(key);

        // Assert - new key should be rejected (not seen before)
        assert!(!admitted);
    }

    #[test]
    fn should_admit_seen_keys() {
        // Arrange
        let counter = AdmissionCounter::new(64, 1000);
        let key = b"hot_key";

        // Act
        counter.record_access(key);
        let admitted = counter.estimate(key);

        // Assert - key should be admitted (seen before)
        assert!(admitted);
    }

    #[test]
    fn should_track_multiple_keys() {
        // Arrange
        let counter = AdmissionCounter::new(64, 1000);
        let key1 = b"key1";
        let key2 = b"key2";

        // Act
        counter.record_access(key1);
        counter.record_access(key2);

        // Assert - both keys should be trackable
        assert!(counter.estimate(key1));
        assert!(counter.estimate(key2));
    }

    #[test]
    fn should_reject_unseen_among_many() {
        // Arrange
        let counter = AdmissionCounter::new(64, 1000);
        let unseen_key = b"never_seen";
        let unseen_cell = usize::try_from(AdmissionCounter::hash_key(unseen_key)).unwrap_or(0) % 64;
        for i in 0..50 {
            let key = format!("key_{i}").into_bytes();
            // `hash_key` is deterministic, so we can guarantee this test is
            // not flaky by skipping any recorded key that happens to hash
            // into the same cell as the unseen key.
            let cell = usize::try_from(AdmissionCounter::hash_key(&key)).unwrap_or(0) % 64;
            if cell != unseen_cell {
                counter.record_access(&key);
            }
        }

        // Act
        let new_key_admitted = counter.estimate(unseen_key);

        // Assert - a key whose cell was never touched must be rejected
        assert!(!new_key_admitted);
    }

    // ===== New comprehensive tests =====

    #[test]
    fn should_accept_empty_keys() {
        // Arrange
        let counter = AdmissionCounter::new(64, 1000);

        // Act
        counter.record_access(b"");
        let admitted = counter.estimate(b"");

        // Assert
        assert!(admitted);
    }

    #[test]
    fn should_handle_large_keys() {
        // Arrange
        let counter = AdmissionCounter::new(64, 1000);
        let large_key = vec![42u8; 1000]; // 1KB key

        // Act
        counter.record_access(&large_key);
        let admitted = counter.estimate(&large_key);

        // Assert
        assert!(admitted);
    }

    #[test]
    fn should_handle_single_cell_counter() {
        // Arrange
        let counter = AdmissionCounter::new(1, 1000);

        // Act - all keys hash to same cell
        counter.record_access(b"key1");
        counter.record_access(b"key2");
        let key1_admitted = counter.estimate(b"key1");
        let key2_admitted = counter.estimate(b"key2");

        // Assert
        assert!(key1_admitted);
        assert!(key2_admitted);
    }

    #[test]
    fn should_handle_many_cells() {
        // Arrange
        let counter = AdmissionCounter::new(1024, 1000);
        let keys: Vec<Vec<u8>> = (0..100).map(|i| format!("key_{i}").into_bytes()).collect();

        // Act
        for key in &keys {
            counter.record_access(key);
        }

        // Assert - all recorded keys should be admitted
        for key in &keys {
            assert!(counter.estimate(key), "Key {key:?} should be admitted");
        }
    }

    #[test]
    fn should_clone_counter() {
        // Arrange
        let counter1 = AdmissionCounter::new(64, 1000);
        counter1.record_access(b"test_key");

        // Act
        let counter2 = counter1.clone();

        // Assert (both should see the same data via Arc)
        assert!(counter2.estimate(b"test_key"));
    }

    #[test]
    fn should_share_state_across_clones() {
        // Arrange
        let counter1 = AdmissionCounter::new(64, 1000);

        // Act
        let counter2 = counter1.clone();
        counter2.record_access(b"new_key");

        // Assert (counter1 should see the update from counter2)
        assert!(counter1.estimate(b"new_key"));
    }

    #[test]
    fn should_handle_hash_collisions() {
        // Arrange
        let counter = AdmissionCounter::new(4, 1000); // Small table to force collisions

        // Act - record many keys, some will collide
        for i in 0..50 {
            counter.record_access(format!("key_{i}").as_bytes());
        }

        // Assert - at least some keys should be tracked
        let mut tracked_count = 0;
        for i in 0..50 {
            if counter.estimate(format!("key_{i}").as_bytes()) {
                tracked_count += 1;
            }
        }
        assert!(tracked_count > 0);
    }

    #[test]
    fn should_handle_edge_case_keys() {
        // Arrange
        let counter = AdmissionCounter::new(64, 1000);

        // Act
        counter.record_access(b"");
        counter.record_access(&[255u8; 256]); // All 0xFF bytes
        counter.record_access(&[0u8; 256]); // All 0x00 bytes

        // Assert
        assert!(counter.estimate(b""));
        assert!(counter.estimate(&[255u8; 256]));
        assert!(counter.estimate(&[0u8; 256]));
    }

    #[test]
    fn should_maintain_consistent_hash_for_same_key() {
        // Arrange
        let counter = AdmissionCounter::new(64, 1000);

        // Act
        counter.record_access(b"test");
        let admitted1 = counter.estimate(b"test");
        let admitted2 = counter.estimate(b"test");

        // Assert
        assert_eq!(admitted1, admitted2);
    }

    #[test]
    fn should_have_deterministic_hash_function() {
        // Arrange
        let key = b"deterministic_key";

        // Act
        let hash1 = AdmissionCounter::hash_key(key);
        let hash2 = AdmissionCounter::hash_key(key);

        // Assert
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn should_track_data_blocks_by_full_cache_key_identity() {
        // Arrange
        let counter = AdmissionCounter::new(1024, 1000);
        let recorded = CacheKey::for_data(7, 4096);
        let same_sst_different_offset = CacheKey::for_data(7, 8192);
        let same_offset_different_type = CacheKey::for_index(7, 4096);

        // Act
        counter.record_access_for_key(&recorded);

        // Assert
        assert!(counter.should_admit(&recorded));
        assert!(!counter.should_admit(&same_sst_different_offset));
        assert!(counter.should_admit(&same_offset_different_type));
    }
}
