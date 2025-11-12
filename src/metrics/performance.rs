//! Performance metrics collection for Midge engine
//!
//! This module provides real-time performance monitoring with:
//! - Throughput counters (ops/sec, bytes/sec)
//! - Latency histograms (p50/p95/p99)
//! - Cache hit rates
//! - Lock contention tracking
//!
//! Used for performance tuning and regression detection.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Performance metrics for the Midge engine
#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    /// WAL metrics
    pub wal: WalMetrics,
    /// Memtable metrics
    pub memtable: MemtableMetrics,
    /// SST read metrics
    pub sst: SstMetrics,
    /// Compaction metrics
    pub compaction: CompactionMetrics,
    /// Cache metrics
    pub cache: CacheMetrics,
}

impl PerformanceMetrics {
    /// Create new performance metrics tracker
    pub fn new() -> Self {
        Self {
            wal: WalMetrics::new(),
            memtable: MemtableMetrics::new(),
            sst: SstMetrics::new(),
            compaction: CompactionMetrics::new(),
            cache: CacheMetrics::new(),
        }
    }

    /// Reset all metrics to zero
    pub fn reset(&self) {
        self.wal.reset();
        self.memtable.reset();
        self.sst.reset();
        self.compaction.reset();
        self.cache.reset();
    }

    /// Get a human-readable summary
    pub fn summary(&self) -> String {
        format!(
            "WAL: {} ops, {} bytes | Memtable: {} inserts | SST: {} reads | Cache: {:.2}% hit rate",
            self.wal.total_operations(),
            self.wal.total_bytes_written(),
            self.memtable.total_inserts(),
            self.sst.total_reads(),
            self.cache.hit_rate() * 100.0
        )
    }
}

impl Default for PerformanceMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// WAL (Write-Ahead Log) performance metrics
#[derive(Debug, Clone)]
pub struct WalMetrics {
    /// Total write operations
    operations: Arc<AtomicU64>,
    /// Total bytes written
    bytes_written: Arc<AtomicU64>,
    /// Total fsync calls
    fsync_calls: Arc<AtomicU64>,
    /// Cumulative fsync time in microseconds
    fsync_time_us: Arc<AtomicU64>,
    /// Number of group commits performed
    group_commits: Arc<AtomicU64>,
    /// Operations batched in group commits
    batched_operations: Arc<AtomicU64>,
}

impl WalMetrics {
    pub fn new() -> Self {
        Self {
            operations: Arc::new(AtomicU64::new(0)),
            bytes_written: Arc::new(AtomicU64::new(0)),
            fsync_calls: Arc::new(AtomicU64::new(0)),
            fsync_time_us: Arc::new(AtomicU64::new(0)),
            group_commits: Arc::new(AtomicU64::new(0)),
            batched_operations: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Record a write operation
    pub fn record_write(&self, bytes: u64) {
        self.operations.fetch_add(1, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record an fsync operation
    pub fn record_fsync(&self, duration: Duration) {
        self.fsync_calls.fetch_add(1, Ordering::Relaxed);
        self.fsync_time_us
            .fetch_add(duration.as_micros() as u64, Ordering::Relaxed);
    }

    /// Record a group commit
    pub fn record_group_commit(&self, operations_batched: u64) {
        self.group_commits.fetch_add(1, Ordering::Relaxed);
        self.batched_operations
            .fetch_add(operations_batched, Ordering::Relaxed);
    }

    pub fn total_operations(&self) -> u64 {
        self.operations.load(Ordering::Relaxed)
    }

    pub fn total_bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }

    pub fn total_fsync_calls(&self) -> u64 {
        self.fsync_calls.load(Ordering::Relaxed)
    }

    /// Average fsync latency in microseconds
    pub fn avg_fsync_latency_us(&self) -> f64 {
        let calls = self.fsync_calls.load(Ordering::Relaxed);
        if calls == 0 {
            return 0.0;
        }
        self.fsync_time_us.load(Ordering::Relaxed) as f64 / calls as f64
    }

    /// Group commit effectiveness (ops per fsync)
    pub fn avg_batch_size(&self) -> f64 {
        let commits = self.group_commits.load(Ordering::Relaxed);
        if commits == 0 {
            return 0.0;
        }
        self.batched_operations.load(Ordering::Relaxed) as f64 / commits as f64
    }

    pub fn reset(&self) {
        self.operations.store(0, Ordering::Relaxed);
        self.bytes_written.store(0, Ordering::Relaxed);
        self.fsync_calls.store(0, Ordering::Relaxed);
        self.fsync_time_us.store(0, Ordering::Relaxed);
        self.group_commits.store(0, Ordering::Relaxed);
        self.batched_operations.store(0, Ordering::Relaxed);
    }
}

impl Default for WalMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Memtable performance metrics
#[derive(Debug, Clone)]
pub struct MemtableMetrics {
    /// Total insert operations
    inserts: Arc<AtomicU64>,
    /// Total delete operations
    deletes: Arc<AtomicU64>,
    /// Total point reads
    point_reads: Arc<AtomicU64>,
    /// Total range scans
    range_scans: Arc<AtomicU64>,
    /// Current memtable size in bytes
    current_size: Arc<AtomicU64>,
}

impl MemtableMetrics {
    pub fn new() -> Self {
        Self {
            inserts: Arc::new(AtomicU64::new(0)),
            deletes: Arc::new(AtomicU64::new(0)),
            point_reads: Arc::new(AtomicU64::new(0)),
            range_scans: Arc::new(AtomicU64::new(0)),
            current_size: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn record_insert(&self, bytes: u64) {
        self.inserts.fetch_add(1, Ordering::Relaxed);
        self.current_size.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_delete(&self) {
        self.deletes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_point_read(&self) {
        self.point_reads.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_range_scan(&self) {
        self.range_scans.fetch_add(1, Ordering::Relaxed);
    }

    pub fn update_size(&self, new_size: u64) {
        self.current_size.store(new_size, Ordering::Relaxed);
    }

    pub fn total_inserts(&self) -> u64 {
        self.inserts.load(Ordering::Relaxed)
    }

    pub fn total_deletes(&self) -> u64 {
        self.deletes.load(Ordering::Relaxed)
    }

    pub fn total_point_reads(&self) -> u64 {
        self.point_reads.load(Ordering::Relaxed)
    }

    pub fn total_range_scans(&self) -> u64 {
        self.range_scans.load(Ordering::Relaxed)
    }

    pub fn current_size_bytes(&self) -> u64 {
        self.current_size.load(Ordering::Relaxed)
    }

    pub fn reset(&self) {
        self.inserts.store(0, Ordering::Relaxed);
        self.deletes.store(0, Ordering::Relaxed);
        self.point_reads.store(0, Ordering::Relaxed);
        self.range_scans.store(0, Ordering::Relaxed);
        // Don't reset current_size as it's a gauge, not a counter
    }
}

impl Default for MemtableMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// SST (Sorted String Table) read metrics
#[derive(Debug, Clone)]
pub struct SstMetrics {
    /// Total SST reads
    reads: Arc<AtomicU64>,
    /// Bytes read from SSTs
    bytes_read: Arc<AtomicU64>,
    /// Bloom filter checks
    bloom_checks: Arc<AtomicU64>,
    /// Bloom filter hits (key found)
    bloom_hits: Arc<AtomicU64>,
    /// Index lookups
    index_lookups: Arc<AtomicU64>,
}

impl SstMetrics {
    pub fn new() -> Self {
        Self {
            reads: Arc::new(AtomicU64::new(0)),
            bytes_read: Arc::new(AtomicU64::new(0)),
            bloom_checks: Arc::new(AtomicU64::new(0)),
            bloom_hits: Arc::new(AtomicU64::new(0)),
            index_lookups: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn record_read(&self, bytes: u64) {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_bloom_check(&self, hit: bool) {
        self.bloom_checks.fetch_add(1, Ordering::Relaxed);
        if hit {
            self.bloom_hits.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_index_lookup(&self) {
        self.index_lookups.fetch_add(1, Ordering::Relaxed);
    }

    pub fn total_reads(&self) -> u64 {
        self.reads.load(Ordering::Relaxed)
    }

    pub fn total_bytes_read(&self) -> u64 {
        self.bytes_read.load(Ordering::Relaxed)
    }

    /// Bloom filter false positive rate
    pub fn bloom_false_positive_rate(&self) -> f64 {
        let checks = self.bloom_checks.load(Ordering::Relaxed);
        if checks == 0 {
            return 0.0;
        }
        let hits = self.bloom_hits.load(Ordering::Relaxed);
        1.0 - (hits as f64 / checks as f64)
    }

    pub fn reset(&self) {
        self.reads.store(0, Ordering::Relaxed);
        self.bytes_read.store(0, Ordering::Relaxed);
        self.bloom_checks.store(0, Ordering::Relaxed);
        self.bloom_hits.store(0, Ordering::Relaxed);
        self.index_lookups.store(0, Ordering::Relaxed);
    }
}

impl Default for SstMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Compaction performance metrics
#[derive(Debug, Clone)]
pub struct CompactionMetrics {
    /// Total compactions performed
    compactions: Arc<AtomicU64>,
    /// Bytes read during compaction
    bytes_read: Arc<AtomicU64>,
    /// Bytes written during compaction
    bytes_written: Arc<AtomicU64>,
    /// Total compaction time in milliseconds
    compaction_time_ms: Arc<AtomicU64>,
}

impl CompactionMetrics {
    pub fn new() -> Self {
        Self {
            compactions: Arc::new(AtomicU64::new(0)),
            bytes_read: Arc::new(AtomicU64::new(0)),
            bytes_written: Arc::new(AtomicU64::new(0)),
            compaction_time_ms: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn record_compaction(&self, bytes_read: u64, bytes_written: u64, duration: Duration) {
        self.compactions.fetch_add(1, Ordering::Relaxed);
        self.bytes_read.fetch_add(bytes_read, Ordering::Relaxed);
        self.bytes_written
            .fetch_add(bytes_written, Ordering::Relaxed);
        self.compaction_time_ms
            .fetch_add(duration.as_millis() as u64, Ordering::Relaxed);
    }

    pub fn total_compactions(&self) -> u64 {
        self.compactions.load(Ordering::Relaxed)
    }

    pub fn total_bytes_read(&self) -> u64 {
        self.bytes_read.load(Ordering::Relaxed)
    }

    pub fn total_bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }

    /// Write amplification factor
    pub fn write_amplification(&self) -> f64 {
        let read = self.bytes_read.load(Ordering::Relaxed);
        if read == 0 {
            return 0.0;
        }
        self.bytes_written.load(Ordering::Relaxed) as f64 / read as f64
    }

    /// Average compaction throughput (MB/s)
    pub fn avg_throughput_mbps(&self) -> f64 {
        let time_ms = self.compaction_time_ms.load(Ordering::Relaxed);
        if time_ms == 0 {
            return 0.0;
        }
        let bytes_written = self.bytes_written.load(Ordering::Relaxed);
        (bytes_written as f64 / 1_048_576.0) / (time_ms as f64 / 1000.0)
    }

    pub fn reset(&self) {
        self.compactions.store(0, Ordering::Relaxed);
        self.bytes_read.store(0, Ordering::Relaxed);
        self.bytes_written.store(0, Ordering::Relaxed);
        self.compaction_time_ms.store(0, Ordering::Relaxed);
    }
}

impl Default for CompactionMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Block cache performance metrics
#[derive(Debug, Clone)]
pub struct CacheMetrics {
    /// Total cache lookups
    lookups: Arc<AtomicU64>,
    /// Cache hits
    hits: Arc<AtomicU64>,
    /// Cache misses
    misses: Arc<AtomicU64>,
    /// Current cache size in bytes
    current_size: Arc<AtomicU64>,
    /// Cache evictions
    evictions: Arc<AtomicU64>,
}

impl CacheMetrics {
    pub fn new() -> Self {
        Self {
            lookups: Arc::new(AtomicU64::new(0)),
            hits: Arc::new(AtomicU64::new(0)),
            misses: Arc::new(AtomicU64::new(0)),
            current_size: Arc::new(AtomicU64::new(0)),
            evictions: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn record_hit(&self) {
        self.lookups.fetch_add(1, Ordering::Relaxed);
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_miss(&self) {
        self.lookups.fetch_add(1, Ordering::Relaxed);
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_eviction(&self) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn update_size(&self, new_size: u64) {
        self.current_size.store(new_size, Ordering::Relaxed);
    }

    /// Cache hit rate (0.0 to 1.0)
    pub fn hit_rate(&self) -> f64 {
        let lookups = self.lookups.load(Ordering::Relaxed);
        if lookups == 0 {
            return 0.0;
        }
        self.hits.load(Ordering::Relaxed) as f64 / lookups as f64
    }

    pub fn total_hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    pub fn total_misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    pub fn total_evictions(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }

    pub fn current_size_bytes(&self) -> u64 {
        self.current_size.load(Ordering::Relaxed)
    }

    pub fn reset(&self) {
        self.lookups.store(0, Ordering::Relaxed);
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.evictions.store(0, Ordering::Relaxed);
        // Don't reset current_size as it's a gauge
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
    fn should_track_wal_metrics() {
        let metrics = WalMetrics::new();

        metrics.record_write(1024);
        metrics.record_write(2048);
        metrics.record_fsync(Duration::from_micros(500));

        assert_eq!(metrics.total_operations(), 2);
        assert_eq!(metrics.total_bytes_written(), 3072);
        assert_eq!(metrics.total_fsync_calls(), 1);
        assert_eq!(metrics.avg_fsync_latency_us(), 500.0);
    }

    #[test]
    fn should_track_group_commit_effectiveness() {
        let metrics = WalMetrics::new();

        metrics.record_group_commit(10);
        metrics.record_group_commit(20);

        assert_eq!(metrics.avg_batch_size(), 15.0);
    }

    #[test]
    fn should_calculate_cache_hit_rate() {
        let metrics = CacheMetrics::new();

        metrics.record_hit();
        metrics.record_hit();
        metrics.record_miss();

        assert_eq!(metrics.hit_rate(), 2.0 / 3.0);
    }

    #[test]
    fn should_calculate_bloom_false_positive_rate() {
        let metrics = SstMetrics::new();

        metrics.record_bloom_check(true);
        metrics.record_bloom_check(true);
        metrics.record_bloom_check(false);

        // 2 hits out of 3 checks = 33% false positive rate
        assert!((metrics.bloom_false_positive_rate() - 0.333).abs() < 0.01);
    }

    #[test]
    fn should_calculate_write_amplification() {
        let metrics = CompactionMetrics::new();

        metrics.record_compaction(1_000_000, 5_000_000, Duration::from_secs(1));

        assert_eq!(metrics.write_amplification(), 5.0);
    }

    #[test]
    fn should_reset_metrics() {
        let metrics = PerformanceMetrics::new();

        metrics.wal.record_write(1024);
        metrics.memtable.record_insert(512);
        metrics.cache.record_hit();

        metrics.reset();

        assert_eq!(metrics.wal.total_operations(), 0);
        assert_eq!(metrics.memtable.total_inserts(), 0);
        assert_eq!(metrics.cache.total_hits(), 0);
    }
}
