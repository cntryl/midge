//! TinyLFU frequency sketch for admission control.
//!
//! The frequency sketch approximates access frequency using a Count-Min Sketch
//! with 4-bit saturating counters. This enables scan-resistant caching by
//! rejecting cold blocks that would evict hotter ones.

/// A Count-Min Sketch with 4-bit saturating counters for frequency estimation.
///
/// Uses 4 hash functions and a power-of-two table size. Each counter saturates
/// at 15 to prevent overflow while still distinguishing hot from cold keys.
pub struct FrequencySketch {
    /// Table of 4-bit counters packed into u8s (2 counters per byte).
    /// Layout: 4 rows (one per hash function), each with `width` counters.
    table: Box<[u8]>,
    /// Number of counters per row (power of two).
    width: usize,
    /// Mask for fast modulo: width - 1.
    mask: usize,
    /// Total increments since last reset (for periodic halving).
    sample_count: u64,
    /// Reset threshold: halve all counters when sample_count exceeds this.
    reset_threshold: u64,
}

impl FrequencySketch {
    /// Create a new frequency sketch sized for the expected number of entries.
    ///
    /// The sketch uses ~1 byte per expected entry (4 counters × 4 bits each).
    pub fn new(expected_entries: usize) -> Self {
        // Size to ~10x expected entries for low collision rate
        let width = (expected_entries * 10).next_power_of_two().max(64);
        // 4 rows, 2 counters per byte
        let table_bytes = width * 4 / 2;

        Self {
            table: vec![0u8; table_bytes].into_boxed_slice(),
            width,
            mask: width - 1,
            sample_count: 0,
            // Reset when we've seen ~10x the table size in samples
            reset_threshold: (width * 10) as u64,
        }
    }

    /// Record an access to the given hash.
    #[inline]
    pub fn increment(&mut self, hash: u64) {
        let h1 = hash;
        let h2 = hash.wrapping_mul(0x9E3779B97F4A7C15); // golden ratio
        let h3 = hash.wrapping_mul(0xC6A4A7935BD1E995); // murmur constant
        let h4 = hash.rotate_left(32);

        self.increment_at(0, h1);
        self.increment_at(1, h2);
        self.increment_at(2, h3);
        self.increment_at(3, h4);

        self.sample_count += 1;
        if self.sample_count >= self.reset_threshold {
            self.halve_all();
        }
    }

    /// Estimate the frequency of the given hash.
    ///
    /// Returns the minimum counter value across all 4 hash positions.
    #[inline]
    pub fn estimate(&self, hash: u64) -> u8 {
        let h1 = hash;
        let h2 = hash.wrapping_mul(0x9E3779B97F4A7C15);
        let h3 = hash.wrapping_mul(0xC6A4A7935BD1E995);
        let h4 = hash.rotate_left(32);

        let c1 = self.get_at(0, h1);
        let c2 = self.get_at(1, h2);
        let c3 = self.get_at(2, h3);
        let c4 = self.get_at(3, h4);

        c1.min(c2).min(c3).min(c4)
    }

    /// Clear the sketch.
    pub fn clear(&mut self) {
        self.table.fill(0);
        self.sample_count = 0;
    }

    /// Increment the counter at (row, hash).
    #[inline]
    fn increment_at(&mut self, row: usize, hash: u64) {
        let idx = (hash as usize) & self.mask;
        let byte_idx = row * (self.width / 2) + idx / 2;
        let shift = (idx & 1) * 4;

        let byte = &mut self.table[byte_idx];
        let counter = (*byte >> shift) & 0x0F;
        if counter < 15 {
            *byte = (*byte & !(0x0F << shift)) | ((counter + 1) << shift);
        }
    }

    /// Get the counter at (row, hash).
    #[inline]
    fn get_at(&self, row: usize, hash: u64) -> u8 {
        let idx = (hash as usize) & self.mask;
        let byte_idx = row * (self.width / 2) + idx / 2;
        let shift = (idx & 1) * 4;

        (self.table[byte_idx] >> shift) & 0x0F
    }

    /// Halve all counters (aging/decay).
    fn halve_all(&mut self) {
        for byte in self.table.iter_mut() {
            // Halve both 4-bit counters in each byte
            let lo = (*byte & 0x0F) >> 1;
            let hi = (*byte & 0xF0) >> 1;
            *byte = lo | (hi & 0xF0);
        }
        self.sample_count = 0;
    }
}

/// Admission controller that decides whether to admit a new block.
///
/// Uses the frequency sketch to compare candidate vs victim frequency.
pub struct AdmissionController {
    sketch: FrequencySketch,
}

impl AdmissionController {
    /// Create a new admission controller.
    pub fn new(expected_entries: usize) -> Self {
        Self {
            sketch: FrequencySketch::new(expected_entries),
        }
    }

    /// Record an access to the given key hash.
    #[inline]
    pub fn record_access(&mut self, hash: u64) {
        self.sketch.increment(hash);
    }

    /// Decide whether to admit a candidate that would evict the victim.
    ///
    /// Returns `true` if the candidate should be admitted (is hotter or equal).
    #[inline]
    pub fn should_admit(&self, candidate_hash: u64, victim_hash: u64) -> bool {
        let candidate_freq = self.sketch.estimate(candidate_hash);
        let victim_freq = self.sketch.estimate(victim_hash);

        // Admit if candidate is at least as hot as victim
        // Tie-break in favor of the new block (recency)
        candidate_freq >= victim_freq
    }

    /// Get the estimated frequency for a hash.
    #[inline]
    pub fn frequency(&self, hash: u64) -> u8 {
        self.sketch.estimate(hash)
    }

    /// Clear the admission controller state.
    pub fn clear(&mut self) {
        self.sketch.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_estimate_zero_given_unseen_key_when_queried() {
        let sketch = FrequencySketch::new(100);
        assert_eq!(sketch.estimate(12345), 0);
    }

    #[test]
    fn should_increase_estimate_given_increments_when_queried() {
        let mut sketch = FrequencySketch::new(100);

        sketch.increment(12345);
        assert!(sketch.estimate(12345) >= 1);

        sketch.increment(12345);
        sketch.increment(12345);
        assert!(sketch.estimate(12345) >= 2);
    }

    #[test]
    fn should_saturate_at_15_given_many_increments_when_queried() {
        let mut sketch = FrequencySketch::new(100);

        for _ in 0..100 {
            sketch.increment(12345);
        }

        assert!(sketch.estimate(12345) <= 15);
    }

    #[test]
    fn should_distinguish_hot_from_cold_given_different_access_patterns() {
        let mut sketch = FrequencySketch::new(1000);

        // Hot key: many accesses
        for _ in 0..50 {
            sketch.increment(1);
        }

        // Cold key: few accesses
        for _ in 0..2 {
            sketch.increment(2);
        }

        assert!(sketch.estimate(1) > sketch.estimate(2));
    }

    #[test]
    fn should_admit_hotter_candidate_given_admission_decision() {
        let mut controller = AdmissionController::new(1000);

        // Make candidate hot
        for _ in 0..10 {
            controller.record_access(1);
        }

        // Make victim cold
        controller.record_access(2);

        assert!(controller.should_admit(1, 2)); // hot beats cold
        assert!(!controller.should_admit(2, 1)); // cold loses to hot
    }

    #[test]
    fn should_clear_given_populated_sketch_when_cleared() {
        let mut sketch = FrequencySketch::new(100);

        sketch.increment(12345);
        assert!(sketch.estimate(12345) >= 1);

        sketch.clear();
        assert_eq!(sketch.estimate(12345), 0);
    }
}
