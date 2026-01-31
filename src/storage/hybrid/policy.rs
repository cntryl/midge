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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EvictionStrategy {
    /// Least Recently Used
    #[default]
    Lru,
    /// FIFO (oldest first)
    Fifo,
    /// Random eviction
    Random,
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_policy_with_default_watermarks() {
        // Arrange
        let max_local_bytes = 1024 * 1024;

        // Act
        let policy = StorageBudgetPolicy::new(1024 * 1024);

        // Assert
        assert_eq!(policy.max_local_bytes, max_local_bytes);
        assert_eq!(policy.high_watermark_percent, 90);
        assert_eq!(policy.critical_watermark_percent, 95);
        assert_eq!(policy.emergency_watermark_percent, 98);
    }

    #[test]
    fn should_identify_high_watermark_correctly() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024);

        // Act
        let below = policy.is_high_watermark(89);
        let at = policy.is_high_watermark(90);
        let above_95 = policy.is_high_watermark(95);
        let above_98 = policy.is_high_watermark(98);

        // Assert
        assert!(!below);
        assert!(at);
        assert!(above_95);
        assert!(above_98);
    }

    #[test]
    fn should_identify_critical_watermark_correctly() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024);

        // Act
        let below = policy.is_critical_watermark(94);
        let at = policy.is_critical_watermark(95);
        let above = policy.is_critical_watermark(98);

        // Assert
        assert!(!below);
        assert!(at);
        assert!(above);
    }

    #[test]
    fn should_identify_emergency_watermark_correctly() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024);

        // Act
        let below = policy.is_emergency_watermark(97);
        let at = policy.is_emergency_watermark(98);
        let above_99 = policy.is_emergency_watermark(99);
        let max = policy.is_emergency_watermark(100);

        // Assert
        assert!(!below);
        assert!(at);
        assert!(above_99);
        assert!(max);
    }

    #[test]
    fn should_customize_watermarks() {
        // Arrange
        let high = 80;
        let critical = 85;
        let emergency = 90;

        // Act
        let policy = StorageBudgetPolicy::new(1024 * 1024).with_watermarks(80, 85, 90);

        // Assert
        assert_eq!(policy.high_watermark_percent, high);
        assert_eq!(policy.critical_watermark_percent, critical);
        assert_eq!(policy.emergency_watermark_percent, emergency);
    }

    #[test]
    fn should_calculate_bytes_until_high_watermark() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1000); // 1000 bytes, high at 90%

        // Act
        let remaining_at_0 = policy.bytes_until_high_watermark(0);
        let remaining_at_450 = policy.bytes_until_high_watermark(450);
        let remaining_at_threshold = policy.bytes_until_high_watermark(900);
        let remaining_over_threshold = policy.bytes_until_high_watermark(950);

        // Assert
        // High threshold = 900 bytes
        assert_eq!(remaining_at_0, 900); // 900 bytes free
        assert_eq!(remaining_at_450, 450); // 450 bytes free
        assert_eq!(remaining_at_threshold, 0); // At threshold
        assert_eq!(remaining_over_threshold, -50); // Over threshold
    }

    #[test]
    fn should_return_default_policy() {
        // Arrange

        // Act
        let policy = StorageBudgetPolicy::default();

        // Assert
        assert_eq!(policy.max_local_bytes, 2 * 1024 * 1024 * 1024); // 2 GB
        assert_eq!(policy.high_watermark_percent, 90);
    }

    #[test]
    fn should_have_eviction_strategies_with_default() {
        // Arrange

        // Act
        let default_strategy = EvictionStrategy::default();

        // Assert
        assert_eq!(default_strategy, EvictionStrategy::Lru);
        assert_ne!(EvictionStrategy::Lru, EvictionStrategy::Fifo);
        assert_ne!(EvictionStrategy::Fifo, EvictionStrategy::Random);
    }
}
