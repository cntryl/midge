//! Cache metrics for observability

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Cache performance metrics
#[derive(Debug, Clone)]
pub struct CacheMetrics {
    pub(crate) hits: Arc<AtomicU64>,
    pub(crate) misses: Arc<AtomicU64>,
    pub(crate) evictions: Arc<AtomicU64>,
    pub(crate) memory_bytes: Arc<AtomicU64>,
}

impl CacheMetrics {
    /// Create a new metrics instance
    pub fn new() -> Self {
        Self {
            hits: Arc::new(AtomicU64::new(0)),
            misses: Arc::new(AtomicU64::new(0)),
            evictions: Arc::new(AtomicU64::new(0)),
            memory_bytes: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Record a cache hit
    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cache miss
    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an eviction
    pub fn record_eviction(&self) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
    }

    /// Update memory usage
    pub fn set_memory_bytes(&self, bytes: u64) {
        self.memory_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Add to memory usage
    pub fn add_memory(&self, bytes: u64) {
        self.memory_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Remove from memory usage
    pub fn remove_memory(&self, bytes: u64) {
        self.memory_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }

    /// Get hit count
    pub fn hit_count(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Get miss count
    pub fn miss_count(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// Get eviction count
    pub fn eviction_count(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }

    /// Get total memory bytes
    pub fn memory_bytes(&self) -> u64 {
        self.memory_bytes.load(Ordering::Relaxed)
    }

    /// Calculate hit rate as percentage (0.0-100.0)
    pub fn hit_rate(&self) -> f64 {
        let hits = self.hit_count() as f64;
        let total = (self.hit_count() + self.miss_count()) as f64;
        if total == 0.0 {
            0.0
        } else {
            (hits / total) * 100.0
        }
    }
}

impl Default for CacheMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_track_cache_metrics() {
        // Arrange
        let metrics = CacheMetrics::new();

        // Act
        metrics.record_hit();
        metrics.record_hit();
        metrics.record_miss();

        // Assert
        assert_eq!(metrics.hit_count(), 2);
        assert_eq!(metrics.miss_count(), 1);
    }

    #[test]
    fn should_calculate_hit_rate() {
        // Arrange
        let metrics = CacheMetrics::new();

        // Act
        metrics.record_hit();
        metrics.record_hit();
        metrics.record_miss();

        // Assert
        let expected = (2.0 / 3.0) * 100.0;
        assert!((metrics.hit_rate() - expected).abs() < 0.01);
    }

    #[test]
    fn should_track_evictions() {
        // Arrange
        let metrics = CacheMetrics::new();

        // Act
        metrics.record_eviction();
        metrics.record_eviction();

        // Assert
        assert_eq!(metrics.eviction_count(), 2);
    }

    #[test]
    fn should_track_memory_usage() {
        // Arrange
        let metrics = CacheMetrics::new();

        // Act
        metrics.add_memory(1024);
        metrics.add_memory(2048);
        metrics.remove_memory(512);

        // Assert
        assert_eq!(metrics.memory_bytes(), 2560);
    }

    // ===== New comprehensive tests =====

    #[test]
    fn should_initialize_all_metrics_to_zero() {
        // Arrange & Act
        let metrics = CacheMetrics::new();

        // Assert
        assert_eq!(metrics.hit_count(), 0);
        assert_eq!(metrics.miss_count(), 0);
        assert_eq!(metrics.eviction_count(), 0);
        assert_eq!(metrics.memory_bytes(), 0);
    }

    #[test]
    fn should_have_zero_hit_rate_when_no_accesses() {
        // Arrange
        let metrics = CacheMetrics::new();

        // Act
        let hit_rate = metrics.hit_rate();

        // Assert
        assert_eq!(hit_rate, 0.0);
    }

    #[test]
    fn should_have_100_percent_hit_rate_with_only_hits() {
        // Arrange
        let metrics = CacheMetrics::new();

        // Act
        metrics.record_hit();
        metrics.record_hit();
        metrics.record_hit();

        // Assert
        assert_eq!(metrics.hit_rate(), 100.0);
    }

    #[test]
    fn should_have_zero_hit_rate_with_only_misses() {
        // Arrange
        let metrics = CacheMetrics::new();

        // Act
        metrics.record_miss();
        metrics.record_miss();

        // Assert
        assert_eq!(metrics.hit_rate(), 0.0);
    }

    #[test]
    fn should_monotonically_increase_hit_count() {
        // Arrange
        let metrics = CacheMetrics::new();

        // Act & Assert
        for i in 1..=10 {
            metrics.record_hit();
            assert_eq!(metrics.hit_count(), i);
        }
    }

    #[test]
    fn should_monotonically_increase_miss_count() {
        // Arrange
        let metrics = CacheMetrics::new();

        // Act & Assert
        for i in 1..=10 {
            metrics.record_miss();
            assert_eq!(metrics.miss_count(), i);
        }
    }

    #[test]
    fn should_monotonically_increase_eviction_count() {
        // Arrange
        let metrics = CacheMetrics::new();

        // Act & Assert
        for i in 1..=10 {
            metrics.record_eviction();
            assert_eq!(metrics.eviction_count(), i);
        }
    }

    #[test]
    fn should_handle_set_memory_bytes() {
        // Arrange
        let metrics = CacheMetrics::new();

        // Act
        metrics.set_memory_bytes(5000);

        // Assert
        assert_eq!(metrics.memory_bytes(), 5000);
    }

    #[test]
    fn should_handle_add_memory_monotonically() {
        // Arrange
        let metrics = CacheMetrics::new();

        // Act & Assert
        for i in 1..=5 {
            metrics.add_memory(1000);
            assert_eq!(metrics.memory_bytes(), i * 1000);
        }
    }

    #[test]
    fn should_handle_remove_memory() {
        // Arrange
        let metrics = CacheMetrics::new();
        metrics.add_memory(5000);

        // Act
        metrics.remove_memory(2000);

        // Assert
        assert_eq!(metrics.memory_bytes(), 3000);
    }

    #[test]
    fn should_calculate_fractional_hit_rates() {
        // Arrange
        let metrics = CacheMetrics::new();

        // Act
        metrics.record_hit();
        metrics.record_miss();
        metrics.record_miss();

        // Assert (1 hit, 2 misses -> 33.33%)
        let expected = (1.0 / 3.0) * 100.0;
        assert!((metrics.hit_rate() - expected).abs() < 0.01);
    }

    #[test]
    fn should_share_state_across_clones() {
        // Arrange
        let metrics1 = CacheMetrics::new();

        // Act
        metrics1.record_hit();
        let metrics2 = metrics1.clone();
        metrics2.record_hit();

        // Assert (both share the same Arc data)
        assert_eq!(metrics1.hit_count(), 2);
        assert_eq!(metrics2.hit_count(), 2);
    }

    #[test]
    fn should_have_default_instance() {
        // Arrange & Act
        let metrics = CacheMetrics::default();

        // Assert
        assert_eq!(metrics.hit_count(), 0);
        assert_eq!(metrics.miss_count(), 0);
    }

    #[test]
    fn should_handle_large_metric_values() {
        // Arrange
        let metrics = CacheMetrics::new();

        // Act
        for _ in 0..1000 {
            metrics.record_hit();
        }
        metrics.add_memory(u64::MAX / 2);

        // Assert
        assert_eq!(metrics.hit_count(), 1000);
        assert!(metrics.memory_bytes() > 0);
    }

    #[test]
    fn should_handle_concurrent_metric_updates() {
        // Arrange
        let metrics = std::sync::Arc::new(CacheMetrics::new());
        let mut handles = vec![];

        // Act
        for _ in 0..10 {
            let m = std::sync::Arc::clone(&metrics);
            let handle = std::thread::spawn(move || {
                for _ in 0..10 {
                    m.record_hit();
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Assert (10 threads * 10 hits = 100)
        assert_eq!(metrics.hit_count(), 100);
    }
}
