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
    pub fn block_cache(&self) -> Option<&Arc<crate::sst::BlockCache>> {
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
    pub fn cache_stats(&self) -> Option<crate::sst::CacheStats> {
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

    /// Get a snapshot of all metrics
    ///
    /// Returns a consistent point-in-time snapshot of all operational metrics.
    /// Useful for monitoring dashboards and performance analysis.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use cntryl_midge::{MidgeEngine, MidgeOptions};
    /// # let engine = MidgeEngine::open(MidgeOptions::default()).unwrap();
    /// let snapshot = engine.metrics_snapshot();
    /// println!("Total gets: {}", snapshot.get_count);
    /// println!("Total puts: {}", snapshot.put_count);
    /// println!("Cache hit rate: {:.2}%",
    ///     snapshot.block_cache_hits as f64 /
    ///     (snapshot.block_cache_hits + snapshot.block_cache_misses) as f64 * 100.0
    /// );
    /// ```
    pub fn metrics_snapshot(&self) -> crate::core::metrics::MetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Get the approximate read amplification factor
    ///
    /// Returns the ratio of bytes read from disk to bytes returned to user.
    /// Values > 1 indicate read amplification due to scanning multiple levels.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use cntryl_midge::{MidgeEngine, MidgeOptions};
    /// # let engine = MidgeEngine::open(MidgeOptions::default()).unwrap();
    /// let amplification = engine.read_amplification();
    /// if amplification > 10.0 {
    ///     println!("Warning: High read amplification: {:.2}x", amplification);
    /// }
    /// ```
    pub fn read_amplification(&self) -> f64 {
        let snapshot = self.metrics.snapshot();
        let manifest = self.manifest_cache.get();

        // Estimate: average number of SST levels checked per read
        // More accurate calculation would need per-level metrics
        let avg_levels_checked = if manifest.files.is_empty() {
            1.0
        } else {
            // Approximate based on manifest structure
            let max_level = manifest.files.iter().map(|f| f.level).max().unwrap_or(0);
            (max_level as f64 + 1.0) / 2.0
        };

        // Factor in bloom filter effectiveness
        let bloom_effectiveness = if snapshot.bloom_filter_checks > 0 {
            1.0 - (snapshot.bloom_filter_positives as f64 / snapshot.bloom_filter_checks as f64)
        } else {
            0.5
        };

        avg_levels_checked * (1.0 - bloom_effectiveness * 0.5)
    }

    /// Get the approximate write amplification factor
    ///
    /// Returns the ratio of bytes written to storage vs bytes written by user.
    /// Includes WAL writes, memtable flushes, and compaction rewrites.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use cntryl_midge::{MidgeEngine, MidgeOptions};
    /// # let engine = MidgeEngine::open(MidgeOptions::default()).unwrap();
    /// let amplification = engine.write_amplification();
    /// println!("Write amplification: {:.2}x", amplification);
    /// ```
    pub fn write_amplification(&self) -> f64 {
        let snapshot = self.metrics.snapshot();

        if snapshot.put_count == 0 {
            return 0.0;
        }

        // Estimate total bytes written:
        // 1. WAL writes (1x)
        // 2. Memtable flushes (1x)
        // 3. Compaction rewrites (varies by level)
        let wal_factor = 1.0;
        let flush_factor = 1.0;

        // Compaction factor depends on compaction bytes
        let compaction_factor = if snapshot.compaction_bytes_written > 0 {
            snapshot.compaction_bytes_written as f64 / (snapshot.put_count as f64 * 100.0)
        // Assume ~100 bytes per put
        } else {
            0.0
        };

        wal_factor + flush_factor + compaction_factor
    }

    /// Get the number of SST files across all levels
    ///
    /// Returns the total count of SST files in the database.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use cntryl_midge::{MidgeEngine, MidgeOptions};
    /// # let engine = MidgeEngine::open(MidgeOptions::default()).unwrap();
    /// let count = engine.sst_file_count();
    /// println!("Total SST files: {}", count);
    /// ```
    pub fn sst_file_count(&self) -> usize {
        let manifest = self.manifest_cache.get();
        manifest.files.len()
    }

    /// Get the total size of all SST files in bytes
    ///
    /// Returns the sum of all SST file sizes across all levels.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use cntryl_midge::{MidgeEngine, MidgeOptions};
    /// # let engine = MidgeEngine::open(MidgeOptions::default()).unwrap();
    /// let size = engine.total_sst_size();
    /// println!("Total SST size: {} MB", size / 1024 / 1024);
    /// ```
    pub fn total_sst_size(&self) -> u64 {
        let manifest = self.manifest_cache.get();
        manifest.files.iter().map(|f| f.size_bytes).sum()
    }
}
