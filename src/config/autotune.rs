//! Adaptive autotuning system for runtime optimization.
//!
//! Implements bounded adaptive control for a minimal set of parameters.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

/// Autotuner for adaptive parameter adjustment.
///
/// Adjusts WAL sync interval, compaction threads, and bloom bits
/// based on observed metrics.
#[derive(Debug)]
pub struct Autotuner {
    /// Current WAL sync interval in milliseconds (10-40ms bounds)
    wal_interval_ms: AtomicU64,

    /// Current compaction thread count (2-8 bounds)
    compaction_threads: AtomicUsize,

    /// Current bloom bits per key (baseline ± 2)
    bloom_bits: AtomicUsize,

    /// Baseline values for reset
    baseline: Baseline,

    /// Last adjustment timestamp
    last_adjustment: Arc<RwLock<Instant>>,

    /// Adjustment interval (default: 5 minutes)
    adjustment_interval: Duration,

    /// Observed metrics
    metrics: Arc<RwLock<ObservedMetrics>>,
}

/// Baseline configuration values.
#[derive(Debug, Clone)]
struct Baseline {
    wal_interval_ms: u64,
    compaction_threads: usize,
    bloom_bits: u32,
}

/// Metrics observed for autotuning decisions.
#[derive(Debug, Clone, Default)]
pub struct ObservedMetrics {
    /// p99 write latency in microseconds
    pub write_latency_p99_us: u64,

    /// L0 file count
    pub l0_file_count: usize,

    /// Cache hit ratio (0.0 - 1.0)
    pub cache_hit_ratio: f64,

    /// Bloom filter false positive rate (0.0 - 1.0)
    pub bloom_fpr: f64,

    /// p99 cloud upload latency in milliseconds (if applicable)
    pub cloud_upload_latency_p99_ms: Option<u64>,
}

impl Autotuner {
    /// Create a new autotuner with default baseline values.
    pub fn new() -> Self {
        Self::with_baselines(20, 4, 10) // Reasonable defaults
    }

    /// Create a new autotuner with baseline values.
    pub fn with_baselines(
        wal_interval_ms: u64,
        compaction_threads: usize,
        bloom_bits: u32,
    ) -> Self {
        Self {
            wal_interval_ms: AtomicU64::new(wal_interval_ms),
            compaction_threads: AtomicUsize::new(compaction_threads),
            bloom_bits: AtomicUsize::new(bloom_bits as usize),
            baseline: Baseline {
                wal_interval_ms,
                compaction_threads,
                bloom_bits,
            },
            last_adjustment: Arc::new(RwLock::new(Instant::now())),
            adjustment_interval: Duration::from_secs(5 * 60), // 5 minutes
            metrics: Arc::new(RwLock::new(ObservedMetrics::default())),
        }
    }

    /// Set adjustment interval (primarily for testing).
    #[cfg(test)]
    pub fn with_adjustment_interval(mut self, interval: Duration) -> Self {
        self.adjustment_interval = interval;
        self
    }

    /// Update observed metrics.
    pub fn update_metrics(&self, metrics: ObservedMetrics) {
        *self.metrics.write() = metrics;
    }

    /// Get current WAL interval in milliseconds.
    pub fn wal_interval_ms(&self) -> u64 {
        self.wal_interval_ms.load(Ordering::Relaxed)
    }

    /// Get current compaction thread count.
    pub fn compaction_threads(&self) -> usize {
        self.compaction_threads.load(Ordering::Relaxed)
    }

    /// Get current bloom bits per key.
    pub fn bloom_bits(&self) -> u32 {
        self.bloom_bits.load(Ordering::Relaxed) as u32
    }

    /// Attempt adjustment based on current metrics.
    ///
    /// Returns true if adjustments were made.
    pub fn adjust(&self) -> bool {
        // Check if enough time has passed since last adjustment
        let now = Instant::now();
        let mut last = self.last_adjustment.write();
        if now.duration_since(*last) < self.adjustment_interval {
            return false;
        }

        let metrics = self.metrics.read().clone();
        let mut adjusted = false;

        // Adjust WAL interval based on write latency
        adjusted |= self.adjust_wal_interval(&metrics);

        // Adjust compaction threads based on L0 backlog
        adjusted |= self.adjust_compaction_threads(&metrics);

        // Adjust bloom bits based on false positive rate
        adjusted |= self.adjust_bloom_bits(&metrics);

        if adjusted {
            *last = now;
        }

        adjusted
    }

    /// Adjust WAL sync interval based on write latency.
    fn adjust_wal_interval(&self, metrics: &ObservedMetrics) -> bool {
        let current = self.wal_interval_ms();
        let baseline = self.baseline.wal_interval_ms;

        // Target: p99 write latency < 5ms
        const TARGET_LATENCY_US: u64 = 5000;
        const DEVIATION_THRESHOLD: f64 = 0.20; // 20%

        let latency = metrics.write_latency_p99_us;
        let deviation = (latency as f64 - TARGET_LATENCY_US as f64) / TARGET_LATENCY_US as f64;

        if deviation.abs() < DEVIATION_THRESHOLD {
            return false; // Within acceptable range
        }

        let new_interval = if deviation > 0.0 {
            // Latency too high: reduce sync interval (more frequent syncs)
            (current as f64 * 0.9).max(10.0) as u64
        } else {
            // Latency good: can increase interval slightly
            (current as f64 * 1.1).min(40.0) as u64
        };

        if new_interval != current {
            self.wal_interval_ms.store(new_interval, Ordering::Relaxed);

            log::info!(
                "Autotuned WAL interval: {} ms -> {} ms (baseline: {} ms)",
                current,
                new_interval,
                baseline
            );
            true
        } else {
            false
        }
    }

    /// Adjust compaction threads based on L0 backlog.
    fn adjust_compaction_threads(&self, metrics: &ObservedMetrics) -> bool {
        let current = self.compaction_threads();

        // Target: L0 file count < 8
        const TARGET_L0_COUNT: usize = 8;
        const DEVIATION_THRESHOLD: f64 = 0.20; // 20%

        let l0_count = metrics.l0_file_count;
        let deviation = (l0_count as f64 - TARGET_L0_COUNT as f64) / TARGET_L0_COUNT as f64;

        if deviation.abs() < DEVIATION_THRESHOLD {
            return false; // Within acceptable range
        }

        let new_threads = if deviation > 0.0 {
            // Too many L0 files: increase threads
            (current + 1).min(8)
        } else {
            // L0 under control: can reduce threads
            (current.saturating_sub(1)).max(2)
        };

        if new_threads != current {
            self.compaction_threads
                .store(new_threads, Ordering::Relaxed);

            log::info!(
                "Autotuned compaction threads: {} -> {} (L0 count: {})",
                current,
                new_threads,
                l0_count
            );
            true
        } else {
            false
        }
    }

    /// Adjust bloom bits based on false positive rate.
    fn adjust_bloom_bits(&self, metrics: &ObservedMetrics) -> bool {
        let current = self.bloom_bits() as usize;
        let baseline = self.baseline.bloom_bits as usize;

        // Target: FPR < 1%
        const TARGET_FPR: f64 = 0.01;
        const DEVIATION_THRESHOLD: f64 = 0.20; // 20%

        let fpr = metrics.bloom_fpr;
        let deviation = (fpr - TARGET_FPR) / TARGET_FPR;

        if deviation.abs() < DEVIATION_THRESHOLD {
            return false; // Within acceptable range
        }

        let new_bits = if deviation > 0.0 {
            // FPR too high: increase bits
            (current + 1).min(baseline + 2)
        } else {
            // FPR good: can reduce bits
            current.saturating_sub(1).max(baseline.saturating_sub(2))
        };

        if new_bits != current {
            self.bloom_bits.store(new_bits, Ordering::Relaxed);

            log::info!(
                "Autotuned bloom bits: {} -> {} (FPR: {:.2}%)",
                current,
                new_bits,
                fpr * 100.0
            );
            true
        } else {
            false
        }
    }

    /// Reset all parameters to baseline values.
    pub fn reset(&self) {
        self.wal_interval_ms
            .store(self.baseline.wal_interval_ms, Ordering::Relaxed);
        self.compaction_threads
            .store(self.baseline.compaction_threads, Ordering::Relaxed);
        self.bloom_bits
            .store(self.baseline.bloom_bits as usize, Ordering::Relaxed);
        log::info!("Reset autotuner to baseline values");
    }
}

impl Default for Autotuner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_autotuner_with_baseline_values() {
        // Arrange
        let tuner = Autotuner::with_baselines(20, 4, 10);

        // Act
        let wal_interval = tuner.wal_interval_ms();
        let compaction_threads = tuner.compaction_threads();
        let bloom_bits = tuner.bloom_bits();

        // Assert
        assert_eq!(wal_interval, 20);
        assert_eq!(compaction_threads, 4);
        assert_eq!(bloom_bits, 10);
    }

    #[test]
    fn should_decrease_wal_interval_given_high_latency() {
        // Arrange
        let tuner =
            Autotuner::with_baselines(20, 4, 10).with_adjustment_interval(Duration::from_secs(0));
        tuner.update_metrics(ObservedMetrics {
            write_latency_p99_us: 10000, // 10ms - too high
            ..Default::default()
        });

        // Act
        let adjusted = tuner.adjust();

        // Assert
        assert!(adjusted);
        assert!(tuner.wal_interval_ms() < 20); // Should decrease
    }

    #[test]
    fn should_increase_compaction_threads_given_high_l0_count() {
        // Arrange
        let tuner =
            Autotuner::with_baselines(20, 4, 10).with_adjustment_interval(Duration::from_secs(0));
        tuner.update_metrics(ObservedMetrics {
            l0_file_count: 16, // Too many
            ..Default::default()
        });

        // Act
        let adjusted = tuner.adjust();

        // Assert
        assert!(adjusted);
        assert!(tuner.compaction_threads() > 4); // Should increase
    }

    #[test]
    fn should_increase_bloom_bits_given_high_false_positive_rate() {
        // Arrange
        let tuner =
            Autotuner::with_baselines(20, 4, 10).with_adjustment_interval(Duration::from_secs(0));
        tuner.update_metrics(ObservedMetrics {
            bloom_fpr: 0.02, // 2% - too high
            ..Default::default()
        });

        // Act
        let adjusted = tuner.adjust();

        // Assert
        assert!(adjusted);
        assert!(tuner.bloom_bits() > 10); // Should increase
    }

    #[test]
    fn should_restore_baseline_values_given_reset() {
        // Arrange
        let tuner = Autotuner::with_baselines(20, 4, 10);
        // Manually change values
        tuner.wal_interval_ms.store(30, Ordering::Relaxed);
        tuner.compaction_threads.store(6, Ordering::Relaxed);
        tuner.bloom_bits.store(12, Ordering::Relaxed);

        // Act
        tuner.reset();

        // Assert
        assert_eq!(tuner.wal_interval_ms(), 20);
        assert_eq!(tuner.compaction_threads(), 4);
        assert_eq!(tuner.bloom_bits(), 10);
    }

    #[test]
    fn should_enforce_bounds_on_adjusted_values() {
        // Arrange
        let tuner =
            Autotuner::with_baselines(20, 4, 10).with_adjustment_interval(Duration::from_secs(0));
        // Try to push WAL interval above max (40ms)
        tuner.wal_interval_ms.store(50, Ordering::Relaxed);
        tuner.update_metrics(ObservedMetrics {
            write_latency_p99_us: 1000, // Very low - would increase interval
            ..Default::default()
        });

        // Act
        tuner.adjust();

        // Assert
        assert!(tuner.wal_interval_ms() <= 40);
    }
}
