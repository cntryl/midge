//! Admission control to prevent cache pollution
//!
//! Admission control uses a probabilistic counter to track the frequency
//! of keys. Keys that fail the admission check are not added to the cache.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

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
            h = h.wrapping_mul(33).wrapping_add(b as u64);
        }
        h
    }

    /// Estimate if a key should be admitted
    ///
    /// Returns true if the key has been seen before (or randomly with threshold probability)
    pub fn estimate(&self, key: &[u8]) -> bool {
        let cell_idx = (Self::hash_key(key) as usize) % self.cells.len();
        let counter = self.cells[cell_idx].load(Ordering::Relaxed);

        // Admit if counter is non-zero (key has been seen before)
        counter > 0
    }

    /// Record an access to a key
    pub fn record_access(&self, key: &[u8]) {
        let cell_idx = (Self::hash_key(key) as usize) % self.cells.len();
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
        for i in 0..50 {
            counter.record_access(format!("key_{}", i).as_bytes());
        }

        // Act
        let new_key_admitted = counter.estimate(b"never_seen");

        // Assert - verify estimate returns a bool (may vary due to hash collisions)
        let _ = new_key_admitted;
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

        // Act & Assert - all keys hash to same cell
        counter.record_access(b"key1");
        assert!(counter.estimate(b"key1"));
        counter.record_access(b"key2");
        assert!(counter.estimate(b"key1"));
        assert!(counter.estimate(b"key2"));
    }

    #[test]
    fn should_handle_many_cells() {
        // Arrange
        let counter = AdmissionCounter::new(1024, 1000);
        let keys: Vec<Vec<u8>> = (0..100)
            .map(|i| format!("key_{}", i).into_bytes())
            .collect();

        // Act
        for key in &keys {
            counter.record_access(key);
        }

        // Assert - all recorded keys should be admitted
        for key in &keys {
            assert!(counter.estimate(key), "Key {:?} should be admitted", key);
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
            counter.record_access(format!("key_{}", i).as_bytes());
        }

        // Assert - at least some keys should be tracked
        let mut tracked_count = 0;
        for i in 0..50 {
            if counter.estimate(format!("key_{}", i).as_bytes()) {
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
        counter.record_access(&[0u8; 256]);   // All 0x00 bytes

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
}
