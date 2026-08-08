//! Bloom filter observability metrics

use std::convert::TryFrom;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Bloom filter effectiveness metrics
#[derive(Debug, Clone)]
pub struct BloomMetrics {
    /// Number of bloom filter checks performed
    pub(crate) checks: Arc<AtomicU64>,
    /// Number of definite negatives (key not in SST/block)
    pub(crate) negatives: Arc<AtomicU64>,
    /// Number of false positives (bloom said maybe, but key not found)
    pub(crate) false_positives: Arc<AtomicU64>,
    /// Number of blocks skipped due to bloom rejection
    pub(crate) blocks_skipped: Arc<AtomicU64>,
}

impl BloomMetrics {
    /// Create a new metrics instance
    #[must_use]
    pub fn new() -> Self {
        Self {
            checks: Arc::new(AtomicU64::new(0)),
            negatives: Arc::new(AtomicU64::new(0)),
            false_positives: Arc::new(AtomicU64::new(0)),
            blocks_skipped: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Record a bloom filter check
    pub fn record_check(&self) {
        self.checks.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a definite negative (bloom rejection)
    pub fn record_negative(&self) {
        self.negatives.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a false positive
    pub fn record_false_positive(&self) {
        self.false_positives.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a block skipped by bloom
    pub fn record_block_skipped(&self) {
        self.blocks_skipped.fetch_add(1, Ordering::Relaxed);
    }

    /// Get total number of checks
    #[must_use]
    pub fn checks(&self) -> u64 {
        self.checks.load(Ordering::Relaxed)
    }

    /// Get number of negatives
    #[must_use]
    pub fn negatives(&self) -> u64 {
        self.negatives.load(Ordering::Relaxed)
    }

    /// Get number of false positives
    #[must_use]
    pub fn false_positives(&self) -> u64 {
        self.false_positives.load(Ordering::Relaxed)
    }

    /// Get number of blocks skipped
    #[must_use]
    pub fn blocks_skipped(&self) -> u64 {
        self.blocks_skipped.load(Ordering::Relaxed)
    }

    /// Calculate false positive rate (0.0 - 1.0)
    #[must_use]
    pub fn false_positive_rate(&self) -> f64 {
        let total = self.checks();
        if total == 0 {
            return 0.0;
        }
        let fps = self.false_positives();
        u64_to_f64(fps) / u64_to_f64(total)
    }

    /// Calculate negative rate (blocks avoided / total checks)
    #[must_use]
    pub fn negative_rate(&self) -> f64 {
        let total = self.checks();
        if total == 0 {
            return 0.0;
        }
        let negs = self.negatives();
        u64_to_f64(negs) / u64_to_f64(total)
    }
}

fn u64_to_f64(value: u64) -> f64 {
    let upper = u32::try_from(value >> 32).unwrap_or(u32::MAX);
    let lower = u32::try_from(value & u64::from(u32::MAX)).unwrap_or(u32::MAX);
    f64::from(upper) * 4_294_967_296.0 + f64::from(lower)
}

impl Default for BloomMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_preserve_bloom_rates_when_counters_exceed_u32_max() {
        // Arrange
        let metrics = BloomMetrics::new();
        let numerator = u64::from(u32::MAX) + 1;
        let checks = numerator * 2;
        metrics.checks.store(checks, Ordering::Relaxed);
        metrics.negatives.store(numerator, Ordering::Relaxed);
        metrics.false_positives.store(numerator, Ordering::Relaxed);

        // Act
        let negative_rate = metrics.negative_rate();
        let false_positive_rate = metrics.false_positive_rate();

        // Assert
        assert!((negative_rate - 0.5).abs() <= f64::EPSILON);
        assert!((false_positive_rate - 0.5).abs() <= f64::EPSILON);
    }
}
