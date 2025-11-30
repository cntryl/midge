//! Block cache metrics collection and reporting.
//!
//! Provides atomic counters for cache statistics that can be updated
//! from multiple threads without locking.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use super::BlockCacheStats;

/// Thread-safe metrics for a single cache shard.
#[derive(Debug, Default)]
pub struct ShardMetrics {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub evictions: AtomicU64,
    pub admissions: AtomicU64,
    pub rejections: AtomicU64,
    pub used_bytes: AtomicUsize,
}

impl ShardMetrics {
    /// Create new zeroed metrics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a cache hit.
    #[inline]
    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cache miss.
    #[inline]
    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an eviction.
    #[inline]
    pub fn record_eviction(&self) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an admission.
    #[inline]
    pub fn record_admission(&self) {
        self.admissions.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a rejection (admission control denied).
    #[inline]
    pub fn record_rejection(&self) {
        self.rejections.fetch_add(1, Ordering::Relaxed);
    }

    /// Update used bytes.
    #[inline]
    pub fn set_used_bytes(&self, bytes: usize) {
        self.used_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Add to used bytes.
    #[inline]
    pub fn add_used_bytes(&self, bytes: usize) {
        self.used_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Subtract from used bytes.
    #[inline]
    pub fn sub_used_bytes(&self, bytes: usize) {
        self.used_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }

    /// Get current snapshot.
    pub fn snapshot(&self) -> ShardMetricsSnapshot {
        ShardMetricsSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            admissions: self.admissions.load(Ordering::Relaxed),
            rejections: self.rejections.load(Ordering::Relaxed),
            used_bytes: self.used_bytes.load(Ordering::Relaxed),
        }
    }

    /// Reset all counters.
    pub fn reset(&self) {
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.evictions.store(0, Ordering::Relaxed);
        self.admissions.store(0, Ordering::Relaxed);
        self.rejections.store(0, Ordering::Relaxed);
        self.used_bytes.store(0, Ordering::Relaxed);
    }
}

/// Immutable snapshot of shard metrics.
#[derive(Debug, Clone, Default)]
pub struct ShardMetricsSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub admissions: u64,
    pub rejections: u64,
    pub used_bytes: usize,
}

impl ShardMetricsSnapshot {
    /// Merge another snapshot into this one (for aggregation).
    pub fn merge(&mut self, other: &ShardMetricsSnapshot) {
        self.hits += other.hits;
        self.misses += other.misses;
        self.evictions += other.evictions;
        self.admissions += other.admissions;
        self.rejections += other.rejections;
        self.used_bytes += other.used_bytes;
    }

    /// Convert to public stats type.
    pub fn to_cache_stats(&self, capacity_bytes: usize) -> BlockCacheStats {
        BlockCacheStats {
            hits: self.hits,
            misses: self.misses,
            evictions: self.evictions,
            admissions: self.admissions,
            rejected: self.rejections,
            used_bytes: self.used_bytes,
            capacity_bytes,
        }
    }
}

/// Aggregated metrics across all shards.
#[derive(Debug)]
pub struct CacheMetrics {
    pub shard_metrics: Vec<ShardMetrics>,
    pub capacity_bytes: usize,
}

impl CacheMetrics {
    /// Create metrics for a cache with the given number of shards.
    pub fn new(num_shards: usize, capacity_bytes: usize) -> Self {
        let shard_metrics = (0..num_shards).map(|_| ShardMetrics::new()).collect();
        Self {
            shard_metrics,
            capacity_bytes,
        }
    }

    /// Get aggregated stats.
    pub fn stats(&self) -> BlockCacheStats {
        let mut total = ShardMetricsSnapshot::default();
        for shard in &self.shard_metrics {
            total.merge(&shard.snapshot());
        }
        total.to_cache_stats(self.capacity_bytes)
    }
}
