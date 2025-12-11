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
}
