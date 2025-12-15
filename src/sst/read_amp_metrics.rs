//! Read amplification metrics for tracking SST access patterns
//!
//! This module provides observability into read amplification across the LSM tree:
//! - How many SSTs are touched per read
//! - L0 overlap patterns
//! - Read budget violations

use std::sync::atomic::{AtomicU64, Ordering};

/// Maximum number of SSTs a read should touch before triggering compaction
pub const MAX_SSTS_PER_READ: u64 = 5;

/// Maximum number of blocks a read should access before triggering compaction
pub const MAX_BLOCKS_PER_READ: u64 = 20;

/// Tracks read amplification patterns across SST reads
#[derive(Debug)]
pub struct ReadAmpMetrics {
    /// Total number of read operations
    reads_total: AtomicU64,

    /// Total SSTs touched across all reads
    ssts_touched_total: AtomicU64,

    /// Total L0 SSTs touched (always fully scanned due to overlap)
    l0_ssts_touched_total: AtomicU64,

    /// Total blocks read across all reads
    blocks_read_total: AtomicU64,

    /// Number of reads that exceeded SST budget
    sst_budget_violations: AtomicU64,

    /// Number of reads that exceeded block budget
    block_budget_violations: AtomicU64,
}

impl Default for ReadAmpMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadAmpMetrics {
    /// Create a new metrics instance
    pub fn new() -> Self {
        Self {
            reads_total: AtomicU64::new(0),
            ssts_touched_total: AtomicU64::new(0),
            l0_ssts_touched_total: AtomicU64::new(0),
            blocks_read_total: AtomicU64::new(0),
            sst_budget_violations: AtomicU64::new(0),
            block_budget_violations: AtomicU64::new(0),
        }
    }

    /// Record a read operation with SST and block counts
    /// Automatically checks budget violations
    pub fn record_read(&self, ssts_touched: u64, l0_ssts_touched: u64, blocks_read: u64) {
        self.reads_total.fetch_add(1, Ordering::Relaxed);
        self.ssts_touched_total
            .fetch_add(ssts_touched, Ordering::Relaxed);
        self.l0_ssts_touched_total
            .fetch_add(l0_ssts_touched, Ordering::Relaxed);
        self.blocks_read_total
            .fetch_add(blocks_read, Ordering::Relaxed);

        // Check budget violations
        if ssts_touched > MAX_SSTS_PER_READ {
            self.record_sst_budget_violation();
        }
        if blocks_read > MAX_BLOCKS_PER_READ {
            self.record_block_budget_violation();
        }
    }

    /// Record an SST budget violation
    pub fn record_sst_budget_violation(&self) {
        self.sst_budget_violations.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a block budget violation
    pub fn record_block_budget_violation(&self) {
        self.block_budget_violations.fetch_add(1, Ordering::Relaxed);
    }

    /// Get total number of reads
    pub fn reads_total(&self) -> u64 {
        self.reads_total.load(Ordering::Relaxed)
    }

    /// Get total SSTs touched across all reads
    pub fn ssts_touched_total(&self) -> u64 {
        self.ssts_touched_total.load(Ordering::Relaxed)
    }

    /// Get total L0 SSTs touched
    pub fn l0_ssts_touched_total(&self) -> u64 {
        self.l0_ssts_touched_total.load(Ordering::Relaxed)
    }

    /// Get total blocks read
    pub fn blocks_read_total(&self) -> u64 {
        self.blocks_read_total.load(Ordering::Relaxed)
    }

    /// Get average SSTs touched per read
    pub fn avg_ssts_per_read(&self) -> f64 {
        let reads = self.reads_total();
        if reads == 0 {
            return 0.0;
        }
        self.ssts_touched_total() as f64 / reads as f64
    }

    /// Get average L0 SSTs touched per read
    pub fn avg_l0_ssts_per_read(&self) -> f64 {
        let reads = self.reads_total();
        if reads == 0 {
            return 0.0;
        }
        self.l0_ssts_touched_total() as f64 / reads as f64
    }

    /// Get average blocks read per read operation
    pub fn avg_blocks_per_read(&self) -> f64 {
        let reads = self.reads_total();
        if reads == 0 {
            return 0.0;
        }
        self.blocks_read_total() as f64 / reads as f64
    }

    /// Get L0 overlap rate (what fraction of SST touches are L0)
    pub fn l0_overlap_rate(&self) -> f64 {
        let total_ssts = self.ssts_touched_total();
        if total_ssts == 0 {
            return 0.0;
        }
        self.l0_ssts_touched_total() as f64 / total_ssts as f64
    }

    /// Get SST budget violation rate
    pub fn sst_budget_violation_rate(&self) -> f64 {
        let reads = self.reads_total();
        if reads == 0 {
            return 0.0;
        }
        self.sst_budget_violations.load(Ordering::Relaxed) as f64 / reads as f64
    }

    /// Get block budget violation rate
    pub fn block_budget_violation_rate(&self) -> f64 {
        let reads = self.reads_total();
        if reads == 0 {
            return 0.0;
        }
        self.block_budget_violations.load(Ordering::Relaxed) as f64 / reads as f64
    }

    /// Reset all metrics to zero
    pub fn reset(&self) {
        self.reads_total.store(0, Ordering::Relaxed);
        self.ssts_touched_total.store(0, Ordering::Relaxed);
        self.l0_ssts_touched_total.store(0, Ordering::Relaxed);
        self.blocks_read_total.store(0, Ordering::Relaxed);
        self.sst_budget_violations.store(0, Ordering::Relaxed);
        self.block_budget_violations.store(0, Ordering::Relaxed);
    }
}

impl Clone for ReadAmpMetrics {
    fn clone(&self) -> Self {
        Self {
            reads_total: AtomicU64::new(self.reads_total.load(Ordering::Relaxed)),
            ssts_touched_total: AtomicU64::new(self.ssts_touched_total.load(Ordering::Relaxed)),
            l0_ssts_touched_total: AtomicU64::new(
                self.l0_ssts_touched_total.load(Ordering::Relaxed),
            ),
            blocks_read_total: AtomicU64::new(self.blocks_read_total.load(Ordering::Relaxed)),
            sst_budget_violations: AtomicU64::new(
                self.sst_budget_violations.load(Ordering::Relaxed),
            ),
            block_budget_violations: AtomicU64::new(
                self.block_budget_violations.load(Ordering::Relaxed),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_new_metrics() {
        let metrics = ReadAmpMetrics::new();
        assert_eq!(metrics.reads_total(), 0);
        assert_eq!(metrics.ssts_touched_total(), 0);
        assert_eq!(metrics.avg_ssts_per_read(), 0.0);
    }

    #[test]
    fn should_record_read_operations() {
        let metrics = ReadAmpMetrics::new();

        metrics.record_read(3, 1, 5);
        assert_eq!(metrics.reads_total(), 1);
        assert_eq!(metrics.ssts_touched_total(), 3);
        assert_eq!(metrics.l0_ssts_touched_total(), 1);
        assert_eq!(metrics.blocks_read_total(), 5);

        metrics.record_read(2, 0, 3);
        assert_eq!(metrics.reads_total(), 2);
        assert_eq!(metrics.ssts_touched_total(), 5);
        assert_eq!(metrics.l0_ssts_touched_total(), 1);
        assert_eq!(metrics.blocks_read_total(), 8);
    }

    #[test]
    fn should_calculate_avg_ssts_per_read() {
        let metrics = ReadAmpMetrics::new();

        metrics.record_read(4, 2, 10);
        metrics.record_read(2, 1, 5);
        metrics.record_read(6, 3, 15);

        assert_eq!(metrics.avg_ssts_per_read(), 4.0); // (4+2+6)/3 = 4.0
        assert_eq!(metrics.avg_l0_ssts_per_read(), 2.0); // (2+1+3)/3 = 2.0
        assert_eq!(metrics.avg_blocks_per_read(), 10.0); // (10+5+15)/3 = 10.0
    }

    #[test]
    fn should_calculate_l0_overlap_rate() {
        let metrics = ReadAmpMetrics::new();

        metrics.record_read(10, 8, 20); // 80% L0

        assert_eq!(metrics.l0_overlap_rate(), 0.8);
    }

    #[test]
    fn should_record_budget_violations() {
        let metrics = ReadAmpMetrics::new();

        // First read: 10 SSTs (>5 limit), 30 blocks (>20 limit) - both violations
        metrics.record_read(10, 5, 30);

        // Second read: 3 SSTs (ok), 5 blocks (ok) - no violations
        metrics.record_read(3, 1, 5);

        // Third read: 2 SSTs (ok), 50 blocks (>20 limit) - block violation only
        metrics.record_read(2, 1, 50);

        assert_eq!(metrics.reads_total(), 3);
        assert_eq!(metrics.sst_budget_violation_rate(), 1.0 / 3.0); // 1/3
        assert_eq!(metrics.block_budget_violation_rate(), 2.0 / 3.0); // 2/3
    }

    #[test]
    fn should_handle_zero_reads_gracefully() {
        let metrics = ReadAmpMetrics::new();

        assert_eq!(metrics.avg_ssts_per_read(), 0.0);
        assert_eq!(metrics.l0_overlap_rate(), 0.0);
        assert_eq!(metrics.sst_budget_violation_rate(), 0.0);
    }

    #[test]
    fn should_reset_metrics() {
        let metrics = ReadAmpMetrics::new();

        metrics.record_read(5, 2, 10);
        metrics.record_sst_budget_violation();

        metrics.reset();

        assert_eq!(metrics.reads_total(), 0);
        assert_eq!(metrics.ssts_touched_total(), 0);
        assert_eq!(metrics.avg_ssts_per_read(), 0.0);
    }

    #[test]
    fn should_clone_metrics() {
        let metrics = ReadAmpMetrics::new();
        metrics.record_read(5, 2, 10);

        let cloned = metrics.clone();

        assert_eq!(cloned.reads_total(), 1);
        assert_eq!(cloned.ssts_touched_total(), 5);
        assert_eq!(cloned.l0_ssts_touched_total(), 2);
        assert_eq!(cloned.blocks_read_total(), 10);
    }
}
