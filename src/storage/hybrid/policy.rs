//! Watermark and eviction policies for the Storage Budget Actor
//!
//! Defines thresholds for throttling, backpressure, and emergency behavior.

/// Storage budget policy with watermarks
#[derive(Debug, Clone)]
pub struct StorageBudgetPolicy {
    /// Maximum local disk capacity in bytes
    pub max_local_bytes: u64,
    /// High watermark: threshold to start backpressure (10% free = 90% used)
    pub high_watermark_percent: u32,
    /// Critical watermark: force cloud uploads and throttle (5% free = 95% used)
    pub critical_watermark_percent: u32,
    /// Emergency watermark: halt writes (2% free = 98% used)
    pub emergency_watermark_percent: u32,
}

impl StorageBudgetPolicy {
    pub fn new(max_local_bytes: u64) -> Self {
        Self {
            max_local_bytes,
            high_watermark_percent: 90,
            critical_watermark_percent: 95,
            emergency_watermark_percent: 98,
        }
    }

    /// Set custom watermarks
    pub fn with_watermarks(mut self, high: u32, critical: u32, emergency: u32) -> Self {
        self.high_watermark_percent = high;
        self.critical_watermark_percent = critical;
        self.emergency_watermark_percent = emergency;
        self
    }

    /// Check if we're in high watermark territory
    pub fn is_high_watermark(&self, usage_percent: u32) -> bool {
        usage_percent >= self.high_watermark_percent
    }

    /// Check if we're in critical watermark territory
    pub fn is_critical_watermark(&self, usage_percent: u32) -> bool {
        usage_percent >= self.critical_watermark_percent
    }

    /// Check if we're in emergency watermark territory
    pub fn is_emergency_watermark(&self, usage_percent: u32) -> bool {
        usage_percent >= self.emergency_watermark_percent
    }

    /// Bytes remaining before high watermark
    pub fn bytes_until_high_watermark(&self, used_bytes: u64) -> i64 {
        let high_threshold =
            (self.max_local_bytes as f64 * (self.high_watermark_percent as f64 / 100.0)) as u64;
        high_threshold as i64 - used_bytes as i64
    }
}

impl Default for StorageBudgetPolicy {
    fn default() -> Self {
        Self::new(2 * 1024 * 1024 * 1024) // 2 GB default
    }
}

/// Eviction strategy for local SST replicas
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[derive(Default)]
pub enum EvictionStrategy {
    /// Least Recently Used
    #[default]
    LRU,
    /// FIFO (oldest first)
    FIFO,
    /// Random eviction
    Random,
}

