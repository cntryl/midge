//! Phase 3.5: IndexTable Sequential Access Optimization
//!
//! This module implements optimizations for sequential block lookups,
//! which are common in range scans and streaming workloads.
//!
//! # Design
//!
//! - **Sequential Predictor**: Tracks last queried block index and tries next block first
//! - **Direct-Mapped Cache**: 64 entries (512 bytes) for sequential/repeated lookups
//! - **Cache-line Packing**: Fields ordered for CPU efficiency
//!
//! # Performance Targets
//!
//! - Predictor hit ratio: 85-95% for sequential scans
//! - Cache hit ratio: 60-80% for mixed workloads
//! - Throughput improvement: 10-15%

/// Sequential predictor and cache for block lookups (Phase 3.5)
///
/// Used by IndexTable to accelerate sequential block access patterns
/// common in range scans and streaming queries.
#[derive(Debug, Clone)]
pub struct SequentialAccessOptimizer {
    /// Last accessed block index (for sequential prediction)
    last_block_index: usize,

    /// Number of consecutive sequential hits (for predictor confidence)
    sequential_hits: u32,

    /// Direct-mapped cache: 64 entries, each stores a key prefix hash and block index
    /// Entry format: (key_hash, block_index)
    /// Hash collision probability is acceptable for streaming workloads
    cache: Vec<(u64, usize)>,

    /// Metrics: total lookups
    lookups: u64,

    /// Metrics: predictor hits
    predictor_hits: u64,

    /// Metrics: cache hits
    cache_hits: u64,
}

impl SequentialAccessOptimizer {
    /// Number of entries in the direct-mapped cache
    const CACHE_ENTRIES: usize = 64;

    /// Cache size in bytes (64 entries * 16 bytes each = 1024 bytes)
    const CACHE_SIZE_BYTES: usize = Self::CACHE_ENTRIES * 16;

    /// Create a new sequential access optimizer
    #[inline]
    pub fn new() -> Self {
        Self {
            last_block_index: 0,
            sequential_hits: 0,
            cache: vec![(0u64, 0usize); Self::CACHE_ENTRIES],
            lookups: 0,
            predictor_hits: 0,
            cache_hits: 0,
        }
    }

    /// Try to predict the next block based on sequential access pattern
    ///
    /// Returns `Some(block_index)` if predictor predicts a block,
    /// or `None` if predictor has no prediction.
    ///
    /// The caller should verify the prediction by checking fence pointers.
    #[inline]
    pub fn predict_next_block(&self) -> Option<usize> {
        if self.sequential_hits > 0 {
            Some(self.last_block_index + 1)
        } else {
            None
        }
    }

    /// Record a successful lookup for sequential prediction
    ///
    /// Call this when you successfully find a key at block_index.
    /// Used to build sequential access prediction state.
    #[inline]
    pub fn record_lookup(&mut self, key_hash: u64, block_index: usize) {
        self.lookups += 1;

        // Check if this is a sequential access
        if block_index == self.last_block_index + 1 {
            self.sequential_hits = self.sequential_hits.saturating_add(1);
            self.predictor_hits += 1;
        } else if block_index > self.last_block_index {
            // Still sequential (might skip blocks due to fence pointers)
            self.sequential_hits = 1;
        } else {
            // Non-sequential access (backward scan, new range, etc.)
            self.sequential_hits = 0;
        }

        self.last_block_index = block_index;

        // Update cache
        self.cache_insert(key_hash, block_index);
    }

    /// Query the direct-mapped cache for a key
    ///
    /// Returns `Some(block_index)` if cache contains an entry for the key,
    /// `None` otherwise.
    #[inline]
    pub fn cache_lookup(&mut self, key_hash: u64) -> Option<usize> {
        let entry_idx = (key_hash as usize) & (Self::CACHE_ENTRIES - 1);
        let (cached_hash, block_idx) = self.cache[entry_idx];

        if cached_hash == key_hash && key_hash != 0 {
            self.cache_hits += 1;
            return Some(block_idx);
        }

        None
    }

    /// Insert an entry into the direct-mapped cache
    #[inline]
    fn cache_insert(&mut self, key_hash: u64, block_index: usize) {
        if key_hash == 0 {
            return; // Skip null hashes
        }
        let entry_idx = (key_hash as usize) & (Self::CACHE_ENTRIES - 1);
        self.cache[entry_idx] = (key_hash, block_index);
    }

    /// Get cache hit ratio (0.0-1.0)
    #[inline]
    pub fn cache_hit_ratio(&self) -> f64 {
        if self.lookups == 0 {
            0.0
        } else {
            self.cache_hits as f64 / self.lookups as f64
        }
    }

    /// Get predictor hit ratio (0.0-1.0)
    #[inline]
    pub fn predictor_hit_ratio(&self) -> f64 {
        if self.lookups == 0 {
            0.0
        } else {
            self.predictor_hits as f64 / self.lookups as f64
        }
    }

    /// Get combined efficiency (max of cache or predictor hit ratio)
    #[inline]
    pub fn efficiency_ratio(&self) -> f64 {
        let cache_ratio = self.cache_hit_ratio();
        let pred_ratio = self.predictor_hit_ratio();
        cache_ratio.max(pred_ratio)
    }

    /// Get cache size in bytes
    #[inline]
    pub fn cache_size_bytes(&self) -> usize {
        Self::CACHE_SIZE_BYTES
    }

    /// Get metrics
    #[inline]
    pub fn metrics(&self) -> SequentialAccessMetrics {
        SequentialAccessMetrics {
            total_lookups: self.lookups,
            predictor_hits: self.predictor_hits,
            cache_hits: self.cache_hits,
            cache_size_bytes: Self::CACHE_SIZE_BYTES,
        }
    }

    /// Reset all metrics (keep predictor state)
    #[inline]
    pub fn reset_metrics(&mut self) {
        self.lookups = 0;
        self.predictor_hits = 0;
        self.cache_hits = 0;
    }

    /// Reset all state including predictor and cache
    #[inline]
    pub fn reset_all(&mut self) {
        self.last_block_index = 0;
        self.sequential_hits = 0;
        self.cache.iter_mut().for_each(|e| *e = (0u64, 0usize));
        self.reset_metrics();
    }
}

impl Default for SequentialAccessOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

/// Metrics from sequential access optimizer
#[derive(Debug, Clone)]
pub struct SequentialAccessMetrics {
    pub total_lookups: u64,
    pub predictor_hits: u64,
    pub cache_hits: u64,
    pub cache_size_bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_new_optimizer() {
        // Arrange & Act
        let opt = SequentialAccessOptimizer::new();

        // Assert
        assert_eq!(opt.last_block_index, 0);
        assert_eq!(opt.sequential_hits, 0);
        assert_eq!(opt.lookups, 0);
        assert_eq!(opt.cache_size_bytes(), 1024);
    }

    #[test]
    fn should_predict_sequential_access() {
        // Arrange
        let mut opt = SequentialAccessOptimizer::new();

        // Act: Record lookups at blocks 0, 1, 2
        opt.record_lookup(1, 0);
        // First prediction before any sequential data
        let pred1 = opt.predict_next_block();

        opt.record_lookup(2, 1); // Sequential hit increments sequential_hits
                                 // Now we can predict
        let pred2 = opt.predict_next_block();

        opt.record_lookup(3, 2); // Sequential hit again
        let pred3 = opt.predict_next_block();

        // Assert
        assert_eq!(pred1, None); // First lookup, no prediction yet
        assert_eq!(pred2, Some(2)); // After one sequential hit, predict next
        assert_eq!(pred3, Some(3)); // After another sequential, predict next
        assert_eq!(opt.predictor_hits, 2); // Two successful predictions
    }

    #[test]
    fn should_break_sequential_prediction_on_gap() {
        // Arrange
        let mut opt = SequentialAccessOptimizer::new();

        // Act: Record sequential lookups, then backward jump
        opt.record_lookup(1, 5);
        opt.record_lookup(2, 6); // Sequential hit
        let pred_before_jump = opt.predict_next_block();

        opt.record_lookup(3, 2); // Jump backward to block 2
        let pred_after_jump = opt.predict_next_block();

        // Assert
        assert_eq!(pred_before_jump, Some(7)); // Expects next sequential block
        assert_eq!(pred_after_jump, None); // No prediction after backward jump
    }

    #[test]
    fn should_cache_block_lookups() {
        // Arrange
        let mut opt = SequentialAccessOptimizer::new();

        // Act: Insert and lookup
        let key_hash = 12345u64;
        opt.record_lookup(key_hash, 5);
        let found = opt.cache_lookup(key_hash);

        // Assert
        assert_eq!(found, Some(5));
        assert_eq!(opt.cache_hits, 1);
    }

    #[test]
    fn should_calculate_hit_ratios() {
        // Arrange
        let mut opt = SequentialAccessOptimizer::new();

        // Act: Generate mix of hits and misses
        for i in 0..100 {
            opt.record_lookup(i as u64, i);
        }

        // Perform some cache lookups
        for i in 0..50 {
            let _ = opt.cache_lookup(i as u64);
        }

        // Assert - all ratios should be between 0.0 and 1.0 (inclusive)
        let cache_ratio = opt.cache_hit_ratio();
        let predictor_ratio = opt.predictor_hit_ratio();
        let efficiency = opt.efficiency_ratio();

        println!(
            "cache_ratio: {}, predictor_ratio: {}, efficiency: {}",
            cache_ratio, predictor_ratio, efficiency
        );
        assert!(
            cache_ratio >= 0.0 && cache_ratio <= 1.0,
            "cache_ratio {} out of bounds",
            cache_ratio
        );
        assert!(
            predictor_ratio >= 0.0 && predictor_ratio <= 1.0,
            "predictor_ratio {} out of bounds",
            predictor_ratio
        );
        assert!(
            efficiency >= 0.0 && efficiency <= 1.0,
            "efficiency {} out of bounds",
            efficiency
        );
    }

    #[test]
    fn should_handle_zero_hash_gracefully() {
        // Arrange
        let mut opt = SequentialAccessOptimizer::new();

        // Act: Try to insert and lookup with zero hash
        opt.record_lookup(0, 5); // Should skip zero hash
        let found = opt.cache_lookup(0);

        // Assert: Zero hash entries should not be cached
        assert_eq!(found, None);
    }

    #[test]
    fn should_reset_metrics_without_state() {
        // Arrange
        let mut opt = SequentialAccessOptimizer::new();
        opt.record_lookup(1, 0);
        opt.record_lookup(2, 1);

        // Act
        opt.reset_metrics();
        let metrics = opt.metrics();

        // Assert: Metrics reset but predictor state preserved
        assert_eq!(metrics.total_lookups, 0);
        assert_eq!(metrics.cache_hits, 0);
        assert_eq!(opt.last_block_index, 1); // State preserved
    }

    #[test]
    fn should_fit_in_l2_cache() {
        // Arrange
        let opt = SequentialAccessOptimizer::new();

        // Act
        let metrics = opt.metrics();

        // Assert: 1024 bytes should fit in L2 cache (typically 256 KB)
        assert_eq!(metrics.cache_size_bytes, 1024);
        assert!(metrics.cache_size_bytes < 4096);
    }
}
