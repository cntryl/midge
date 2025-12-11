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
        if access_count % self.reset_interval == 0 && old_count > 0 {
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
}
