//! Observability operations for MidgeEngine
//!
//! This module provides access to metrics, cache statistics, and memory usage
//! information for monitoring and debugging purposes.

use std::collections::HashMap;
use std::sync::Arc;

use crate::core::metrics::Metrics;

use super::super::MidgeEngine;

impl MidgeEngine {
    /// Get a reference to the block cache
    ///
    /// Returns `None` if block caching is disabled.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use cntryl_midge::{MidgeEngine, MidgeOptions};
    /// # let engine = MidgeEngine::open(MidgeOptions::default()).unwrap();
    /// if let Some(cache) = engine.block_cache() {
    ///     println!("Block cache enabled with capacity: {} bytes", cache.capacity());
    /// }
    /// ```
    pub fn block_cache(&self) -> Option<&Arc<crate::cache::BlockCache>> {
        self.block_cache.as_ref()
    }

    /// Get cache statistics
    ///
    /// Returns block cache hit rates, eviction counts, and other metrics.
    /// Returns `None` if block caching is disabled.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use cntryl_midge::{MidgeEngine, MidgeOptions};
    /// # let engine = MidgeEngine::open(MidgeOptions::default()).unwrap();
    /// if let Some(stats) = engine.cache_stats() {
    ///     println!("Cache hit rate: {:.2}%", stats.hit_rate() * 100.0);
    ///     println!("Total hits: {}, misses: {}", stats.hits(), stats.misses());
    /// }
    /// ```
    pub fn cache_stats(&self) -> Option<crate::cache::CacheStats> {
        self.block_cache.as_ref().map(|c| c.stats())
    }

    /// Get a reference to the table cache
    ///
    /// Returns `None` if table caching is disabled.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use cntryl_midge::{MidgeEngine, MidgeOptions};
    /// # let engine = MidgeEngine::open(MidgeOptions::default()).unwrap();
    /// if let Some(cache) = engine.table_cache() {
    ///     println!("Table cache enabled");
    /// }
    /// ```
    pub fn table_cache(&self) -> Option<&Arc<crate::sst::table_cache::TableCache>> {
        self.table_cache.as_ref()
    }

    /// Get table cache statistics
    ///
    /// Returns table cache hit rates and eviction counts.
    /// Returns `None` if table caching is disabled.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use cntryl_midge::{MidgeEngine, MidgeOptions};
    /// # let engine = MidgeEngine::open(MidgeOptions::default()).unwrap();
    /// if let Some(stats) = engine.table_cache_stats() {
    ///     println!("Table cache hit rate: {:.2}%", stats.hit_rate() * 100.0);
    /// }
    /// ```
    pub fn table_cache_stats(&self) -> Option<crate::sst::table_cache::TableCacheStats> {
        self.table_cache.as_ref().map(|c| c.stats())
    }

    /// Get a reference to the metrics collector
    ///
    /// Provides access to operational metrics including compaction statistics,
    /// snapshot counts, and tombstone removal metrics.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use cntryl_midge::{MidgeEngine, MidgeOptions};
    /// # let engine = MidgeEngine::open(MidgeOptions::default()).unwrap();
    /// let metrics = engine.metrics();
    /// println!("Active snapshots: {}", metrics.active_snapshots());
    /// ```
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    /// Get a reference to the performance metrics
    ///
    /// Provides real-time performance monitoring with:
    /// - WAL throughput and fsync latency
    /// - Memtable operation counters
    /// - SST read metrics and bloom filter effectiveness
    /// - Compaction throughput and write amplification
    /// - Block cache hit rates
    ///
    /// Use this for performance tuning, regression detection, and production monitoring.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use cntryl_midge::MidgeEngine;
    /// # let engine = MidgeEngine::open(Default::default()).unwrap();
    /// let metrics = engine.performance_metrics();
    /// println!("Cache hit rate: {:.2}%", metrics.cache.hit_rate() * 100.0);
    /// println!("WAL ops/sec: {}", metrics.wal.total_operations());
    /// ```
    pub fn performance_metrics(&self) -> &Arc<crate::core::metrics::PerformanceMetrics> {
        &self.performance_metrics
    }

    /// Get the current sequence number
    ///
    /// Returns the latest sequence number allocated by the engine.
    /// Useful for testing and debugging.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use cntryl_midge::{MidgeEngine, MidgeOptions};
    /// # let engine = MidgeEngine::open(MidgeOptions::default()).unwrap();
    /// let seq = engine.current_sequence();
    /// println!("Current sequence: {}", seq);
    /// ```
    pub fn current_sequence(&self) -> u64 {
        self.seq.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Get the total memory usage across all column families
    ///
    /// Returns the sum of all memtable sizes in bytes.
    /// Useful for monitoring and testing memory pressure scenarios.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use cntryl_midge::{MidgeEngine, MidgeOptions};
    /// # let engine = MidgeEngine::open(MidgeOptions::default()).unwrap();
    /// let usage = engine.total_memory_usage();
    /// println!("Total memtable memory: {} MB", usage / 1024 / 1024);
    /// ```
    pub fn total_memory_usage(&self) -> usize {
        let mut total = 0usize;

        for entry in self.cf_set.cfs.iter() {
            let mt = entry.value().memtable.read();
            total += mt.size_bytes();
        }

        total
    }

    /// Get memory usage per column family
    ///
    /// Returns a HashMap mapping CF IDs to their memory usage in bytes.
    /// Useful for testing memory budget distribution across CFs.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use cntryl_midge::{MidgeEngine, MidgeOptions};
    /// # let engine = MidgeEngine::open(MidgeOptions::default()).unwrap();
    /// let usage = engine.memory_usage_by_cf();
    /// for (cf_id, bytes) in usage {
    ///     println!("CF {}: {} KB", cf_id, bytes / 1024);
    /// }
    /// ```
    pub fn memory_usage_by_cf(&self) -> HashMap<u32, usize> {
        let mut result = HashMap::new();

        for entry in self.cf_set.cfs.iter() {
            let mt = entry.value().memtable.read();
            result.insert(*entry.key(), mt.size_bytes());
        }

        result
    }
}
