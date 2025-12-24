//! Metrics and observability
//!
//! Comprehensive performance monitoring and statistics collection.
//!
//! Tracks:
//! - Operation counts and throughput (reads, writes, deletes, batch ops)
//! - Latency percentiles (p50, p95, p99)
//! - Storage statistics (size, levels, files)
//! - Compaction metrics (runs, bytes processed, duration)
//! - Memory usage and cache hit rates
//! - WAL statistics (writes, flushes, bytes written)

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Operation latency tracking (nanoseconds)
#[derive(Clone, Debug)]
pub struct LatencyMetric {
    count: Arc<AtomicU64>,
    sum: Arc<AtomicU64>,
    max: Arc<AtomicU64>,
    min: Arc<AtomicU64>,
}

impl LatencyMetric {
    pub fn new() -> Self {
        Self {
            count: Arc::new(AtomicU64::new(0)),
            sum: Arc::new(AtomicU64::new(0)),
            max: Arc::new(AtomicU64::new(0)),
            min: Arc::new(AtomicU64::new(u64::MAX)),
        }
    }

    pub fn record(&self, nanos: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(nanos, Ordering::Relaxed);

        // Update max
        let mut current_max = self.max.load(Ordering::Relaxed);
        while nanos > current_max
            && self
                .max
                .compare_exchange_weak(current_max, nanos, Ordering::Relaxed, Ordering::Relaxed)
                .is_err()
        {
            current_max = self.max.load(Ordering::Relaxed);
        }

        // Update min
        let mut current_min = self.min.load(Ordering::Relaxed);
        while nanos < current_min
            && self
                .min
                .compare_exchange_weak(current_min, nanos, Ordering::Relaxed, Ordering::Relaxed)
                .is_err()
        {
            current_min = self.min.load(Ordering::Relaxed);
        }
    }

    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    pub fn avg_nanos(&self) -> u64 {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 {
            0
        } else {
            self.sum.load(Ordering::Relaxed) / count
        }
    }

    pub fn max_nanos(&self) -> u64 {
        self.max.load(Ordering::Relaxed)
    }

    pub fn min_nanos(&self) -> u64 {
        let min = self.min.load(Ordering::Relaxed);
        if min == u64::MAX {
            0
        } else {
            min
        }
    }
}

impl Default for LatencyMetric {
    fn default() -> Self {
        Self::new()
    }
}

/// Comprehensive engine metrics
#[derive(Clone, Debug)]
pub struct EngineMetrics {
    // Operation counts
    pub read_ops: Arc<AtomicU64>,
    pub write_ops: Arc<AtomicU64>,
    pub delete_ops: Arc<AtomicU64>,
    pub batch_ops: Arc<AtomicU64>,
    pub range_ops: Arc<AtomicU64>,

    // Latency tracking
    pub read_latency_ns: LatencyMetric,
    pub write_latency_ns: LatencyMetric,
    pub delete_latency_ns: LatencyMetric,

    // Storage statistics
    pub total_bytes_written: Arc<AtomicU64>,
    pub total_bytes_read: Arc<AtomicU64>,
    pub sst_file_count: Arc<AtomicU64>,
    pub memtable_bytes: Arc<AtomicU64>,

    // Compaction metrics
    pub compaction_runs: Arc<AtomicU64>,
    pub compaction_bytes_read: Arc<AtomicU64>,
    pub compaction_bytes_written: Arc<AtomicU64>,
    pub compaction_duration_ns: LatencyMetric,

    // Cache metrics
    pub cache_hits: Arc<AtomicU64>,
    pub cache_misses: Arc<AtomicU64>,

    // WAL metrics
    pub wal_writes: Arc<AtomicU64>,
    pub wal_syncs: Arc<AtomicU64>,
    pub wal_bytes_written: Arc<AtomicU64>,

    // Error tracking
    pub errors: Arc<AtomicU64>,
}

impl EngineMetrics {
    pub fn new() -> Self {
        Self {
            read_ops: Arc::new(AtomicU64::new(0)),
            write_ops: Arc::new(AtomicU64::new(0)),
            delete_ops: Arc::new(AtomicU64::new(0)),
            batch_ops: Arc::new(AtomicU64::new(0)),
            range_ops: Arc::new(AtomicU64::new(0)),

            read_latency_ns: LatencyMetric::new(),
            write_latency_ns: LatencyMetric::new(),
            delete_latency_ns: LatencyMetric::new(),

            total_bytes_written: Arc::new(AtomicU64::new(0)),
            total_bytes_read: Arc::new(AtomicU64::new(0)),
            sst_file_count: Arc::new(AtomicU64::new(0)),
            memtable_bytes: Arc::new(AtomicU64::new(0)),

            compaction_runs: Arc::new(AtomicU64::new(0)),
            compaction_bytes_read: Arc::new(AtomicU64::new(0)),
            compaction_bytes_written: Arc::new(AtomicU64::new(0)),
            compaction_duration_ns: LatencyMetric::new(),

            cache_hits: Arc::new(AtomicU64::new(0)),
            cache_misses: Arc::new(AtomicU64::new(0)),

            wal_writes: Arc::new(AtomicU64::new(0)),
            wal_syncs: Arc::new(AtomicU64::new(0)),
            wal_bytes_written: Arc::new(AtomicU64::new(0)),

            errors: Arc::new(AtomicU64::new(0)),
        }
    }

    // Operation recording
    pub fn record_read(&self, nanos: u64) {
        self.read_ops.fetch_add(1, Ordering::Relaxed);
        self.read_latency_ns.record(nanos);
    }

    pub fn record_write(&self, bytes: u64, nanos: u64) {
        self.write_ops.fetch_add(1, Ordering::Relaxed);
        self.write_latency_ns.record(nanos);
        self.total_bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_delete(&self, nanos: u64) {
        self.delete_ops.fetch_add(1, Ordering::Relaxed);
        self.delete_latency_ns.record(nanos);
    }

    pub fn record_batch(&self, _op_count: u64) {
        self.batch_ops.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_range_scan(&self, bytes_read: u64) {
        self.range_ops.fetch_add(1, Ordering::Relaxed);
        self.total_bytes_read
            .fetch_add(bytes_read, Ordering::Relaxed);
    }

    // Compaction recording
    pub fn record_compaction_start(&self) -> CompactionGuard {
        self.compaction_runs.fetch_add(1, Ordering::Relaxed);
        CompactionGuard {
            metrics: self.clone(),
            start_time: std::time::Instant::now(),
        }
    }

    pub fn record_compaction_bytes(&self, read: u64, written: u64) {
        self.compaction_bytes_read
            .fetch_add(read, Ordering::Relaxed);
        self.compaction_bytes_written
            .fetch_add(written, Ordering::Relaxed);
    }

    // Cache metrics
    pub fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn cache_hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    // WAL metrics
    pub fn record_wal_write(&self, bytes: u64) {
        self.wal_writes.fetch_add(1, Ordering::Relaxed);
        self.wal_bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_wal_sync(&self) {
        self.wal_syncs.fetch_add(1, Ordering::Relaxed);
    }

    // Error tracking
    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    // Aggregated stats
    pub fn total_ops(&self) -> u64 {
        self.read_ops.load(Ordering::Relaxed)
            + self.write_ops.load(Ordering::Relaxed)
            + self.delete_ops.load(Ordering::Relaxed)
    }

    pub fn compaction_ratio(&self) -> f64 {
        let read = self.compaction_bytes_read.load(Ordering::Relaxed);
        let written = self.compaction_bytes_written.load(Ordering::Relaxed);
        if read == 0 {
            0.0
        } else {
            written as f64 / read as f64
        }
    }
}

impl Default for EngineMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII guard for measuring compaction duration
pub struct CompactionGuard {
    metrics: EngineMetrics,
    start_time: std::time::Instant,
}

impl Drop for CompactionGuard {
    fn drop(&mut self) {
        let elapsed = self.start_time.elapsed();
        self.metrics
            .compaction_duration_ns
            .record(elapsed.as_nanos() as u64);
    }
}

// Legacy API for backward compatibility
#[derive(Default, Clone, Debug)]
pub struct PerformanceMetrics {
    pub read_ops: u64,
    pub write_ops: u64,
    pub delete_ops: u64,
    pub compactions: u64,
}

impl PerformanceMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_read(&mut self) {
        self.read_ops += 1;
    }

    pub fn record_write(&mut self) {
        self.write_ops += 1;
    }

    pub fn record_delete(&mut self) {
        self.delete_ops += 1;
    }

    pub fn record_compaction(&mut self) {
        self.compactions += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_track_read_latency_when_recording_reads() {
        // Arrange
        let metrics = EngineMetrics::new();

        // Act
        metrics.record_read(1_000_000); // 1ms
        metrics.record_read(2_000_000); // 2ms
        metrics.record_read(1_500_000); // 1.5ms

        // Assert
        assert_eq!(metrics.read_ops.load(Ordering::Relaxed), 3);
        assert_eq!(metrics.read_latency_ns.count(), 3);
        assert_eq!(metrics.read_latency_ns.max_nanos(), 2_000_000);
        assert_eq!(metrics.read_latency_ns.min_nanos(), 1_000_000);
        assert_eq!(metrics.read_latency_ns.avg_nanos(), 1_500_000);
    }

    #[test]
    fn should_track_write_metrics_latency_and_bytes_when_recording_writes() {
        // Arrange
        let metrics = EngineMetrics::new();

        // Act
        metrics.record_write(1024, 500_000); // 1KB in 0.5ms
        metrics.record_write(2048, 1_000_000); // 2KB in 1ms

        // Assert
        assert_eq!(metrics.write_ops.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.total_bytes_written.load(Ordering::Relaxed), 3072);
        assert_eq!(metrics.write_latency_ns.count(), 2);
        assert_eq!(metrics.write_latency_ns.avg_nanos(), 750_000);
    }

    #[test]
    fn should_track_delete_latency_when_recording_deletes() {
        // Arrange
        let metrics = EngineMetrics::new();

        // Act
        metrics.record_delete(100_000);
        metrics.record_delete(200_000);

        // Assert
        assert_eq!(metrics.delete_ops.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.delete_latency_ns.avg_nanos(), 150_000);
    }

    #[test]
    fn should_calculate_total_ops_correctly() {
        // Arrange
        let metrics = EngineMetrics::new();

        // Act
        metrics.record_read(100_000);
        metrics.record_read(100_000);
        metrics.record_write(512, 200_000);
        metrics.record_delete(150_000);

        // Assert: 2 reads + 1 write + 1 delete = 4 ops
        assert_eq!(metrics.total_ops(), 4);
    }

    #[test]
    fn should_track_cache_hit_rate_accurately() {
        // Arrange
        let metrics = EngineMetrics::new();

        // Act
        metrics.record_cache_hit();
        metrics.record_cache_hit();
        metrics.record_cache_hit();
        metrics.record_cache_miss();
        metrics.record_cache_miss();

        // Assert: 3 hits, 2 misses = 60% hit rate
        let hit_rate = metrics.cache_hit_rate();
        assert!((hit_rate - 0.6).abs() < 0.001);
    }

    #[test]
    fn should_return_zero_cache_hit_rate_when_no_accesses() {
        // Arrange
        let metrics = EngineMetrics::new();

        // Act
        let hit_rate = metrics.cache_hit_rate();

        // Assert
        assert_eq!(hit_rate, 0.0);
    }

    #[test]
    fn should_track_compaction_metrics_when_recording_compaction() {
        // Arrange
        let metrics = EngineMetrics::new();

        // Act
        {
            let _guard = metrics.record_compaction_start();
            metrics.record_compaction_bytes(10_000_000, 8_000_000);
            // Guard drops, recording duration
        }

        // Assert
        assert_eq!(metrics.compaction_runs.load(Ordering::Relaxed), 1);
        assert_eq!(
            metrics.compaction_bytes_read.load(Ordering::Relaxed),
            10_000_000
        );
        assert_eq!(
            metrics.compaction_bytes_written.load(Ordering::Relaxed),
            8_000_000
        );
        assert!(metrics.compaction_duration_ns.count() > 0);
    }

    #[test]
    fn should_calculate_compaction_ratio_correctly() {
        // Arrange
        let metrics = EngineMetrics::new();

        // Act
        metrics.record_compaction_bytes(100_000_000, 80_000_000);

        // Assert: 80MB / 100MB = 0.8 ratio
        let ratio = metrics.compaction_ratio();
        assert!((ratio - 0.8).abs() < 0.001);
    }

    #[test]
    fn should_track_wal_write_then_sync_metrics() {
        // Arrange
        let metrics = EngineMetrics::new();

        // Act
        metrics.record_wal_write(1024);
        metrics.record_wal_write(2048);
        metrics.record_wal_sync();
        metrics.record_wal_sync();

        // Assert
        assert_eq!(metrics.wal_writes.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.wal_bytes_written.load(Ordering::Relaxed), 3072);
        assert_eq!(metrics.wal_syncs.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn should_track_range_scan_operations() {
        // Arrange
        let metrics = EngineMetrics::new();

        // Act
        metrics.record_range_scan(50_000);
        metrics.record_range_scan(30_000);

        // Assert
        assert_eq!(metrics.range_ops.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.total_bytes_read.load(Ordering::Relaxed), 80_000);
    }

    #[test]
    fn should_track_errors() {
        // Arrange
        let metrics = EngineMetrics::new();

        // Act
        metrics.record_error();
        metrics.record_error();
        metrics.record_error();

        // Assert
        assert_eq!(metrics.errors.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn should_be_thread_safe_for_concurrent_updates() {
        // Arrange
        let metrics = Arc::new(EngineMetrics::new());
        let mut handles = vec![];

        // Act: Spawn multiple threads incrementing metrics
        for _ in 0..10 {
            let metrics_clone = Arc::clone(&metrics);
            let handle = std::thread::spawn(move || {
                for _ in 0..100 {
                    metrics_clone.record_read(1000);
                    metrics_clone.record_write(512, 1000);
                    metrics_clone.record_cache_hit();
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Assert: All 1000 operations recorded correctly
        assert_eq!(metrics.read_ops.load(Ordering::Relaxed), 1000);
        assert_eq!(metrics.write_ops.load(Ordering::Relaxed), 1000);
        assert_eq!(metrics.cache_hits.load(Ordering::Relaxed), 1000);
    }

    #[test]
    fn should_measure_latency_metric_min_correctly() {
        // Arrange
        let latency = LatencyMetric::new();

        // Act
        latency.record(5_000_000);
        latency.record(1_000_000);
        latency.record(3_000_000);

        // Assert
        assert_eq!(latency.min_nanos(), 1_000_000);
    }

    #[test]
    fn should_return_zero_min_when_no_samples() {
        // Arrange
        let latency = LatencyMetric::new();

        // Act
        let min = latency.min_nanos();

        // Assert
        assert_eq!(min, 0);
    }

    #[test]
    fn should_track_batch_operations() {
        // Arrange
        let metrics = EngineMetrics::new();

        // Act
        metrics.record_batch(100);
        metrics.record_batch(200);
        metrics.record_batch(150);

        // Assert
        assert_eq!(metrics.batch_ops.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn should_return_zero_compaction_ratio_when_no_compactions() {
        // Arrange
        let metrics = EngineMetrics::new();

        // Act
        let ratio = metrics.compaction_ratio();

        // Assert
        assert_eq!(ratio, 0.0);
    }

    #[test]
    fn should_provide_backward_compatible_performance_metrics() {
        // Arrange
        let mut metrics = PerformanceMetrics::new();

        // Act
        metrics.record_read();
        metrics.record_write();
        metrics.record_delete();
        metrics.record_compaction();

        // Assert
        assert_eq!(metrics.read_ops, 1);
        assert_eq!(metrics.write_ops, 1);
        assert_eq!(metrics.delete_ops, 1);
        assert_eq!(metrics.compactions, 1);
    }

    // ========================================================================
    // LatencyMetric invariant tests
    // ========================================================================

    #[test]
    fn should_initialize_latency_metric_with_zero_count() {
        // Arrange / Act
        let latency = LatencyMetric::new();

        // Assert
        assert_eq!(latency.count(), 0);
        assert_eq!(latency.avg_nanos(), 0);
        assert_eq!(latency.max_nanos(), 0);
        assert_eq!(latency.min_nanos(), 0);
    }

    #[test]
    fn should_maintain_monotonic_count_on_record() {
        // Arrange
        let latency = LatencyMetric::new();

        // Act
        latency.record(1000);
        assert_eq!(latency.count(), 1);
        latency.record(2000);
        assert_eq!(latency.count(), 2);
        latency.record(3000);
        assert_eq!(latency.count(), 3);

        // Assert: count never decreases
        assert_eq!(latency.count(), 3);
    }

    #[test]
    fn should_never_exceed_max_latency() {
        // Arrange
        let latency = LatencyMetric::new();

        // Act
        latency.record(1_000_000);
        latency.record(5_000_000);
        latency.record(3_000_000);

        // Assert: max is truly the maximum
        assert_eq!(latency.max_nanos(), 5_000_000);
        assert!(latency.avg_nanos() <= latency.max_nanos());
    }

    #[test]
    fn should_never_fall_below_min_latency() {
        // Arrange
        let latency = LatencyMetric::new();

        // Act
        latency.record(5_000_000);
        latency.record(1_000_000);
        latency.record(3_000_000);

        // Assert: min is truly the minimum
        assert_eq!(latency.min_nanos(), 1_000_000);
        assert!(latency.avg_nanos() >= latency.min_nanos());
    }

    #[test]
    fn should_calculate_correct_average_latency() {
        // Arrange
        let latency = LatencyMetric::new();

        // Act
        latency.record(1_000_000); // 1ms
        latency.record(2_000_000); // 2ms
        latency.record(3_000_000); // 3ms

        // Assert: avg = (1 + 2 + 3) / 3 = 2ms
        assert_eq!(latency.avg_nanos(), 2_000_000);
    }

    #[test]
    fn should_handle_single_sample_latency_correctly() {
        // Arrange
        let latency = LatencyMetric::new();

        // Act
        latency.record(1_500_000);

        // Assert: single value is min, max, and avg
        assert_eq!(latency.count(), 1);
        assert_eq!(latency.avg_nanos(), 1_500_000);
        assert_eq!(latency.min_nanos(), 1_500_000);
        assert_eq!(latency.max_nanos(), 1_500_000);
    }

    // ========================================================================
    // EngineMetrics initialization invariants
    // ========================================================================

    #[test]
    fn should_initialize_engine_metrics_with_zeros() {
        // Arrange / Act
        let metrics = EngineMetrics::new();

        // Assert: all counters start at 0
        assert_eq!(metrics.read_ops.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.write_ops.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.delete_ops.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.batch_ops.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.range_ops.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.total_bytes_written.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.total_bytes_read.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.sst_file_count.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.memtable_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.compaction_runs.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.compaction_bytes_read.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.compaction_bytes_written.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.cache_hits.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.cache_misses.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.wal_writes.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.wal_syncs.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.wal_bytes_written.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.errors.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn should_initialize_engine_metrics_via_default() {
        // Arrange / Act
        let metrics = EngineMetrics::default();

        // Assert: default == new
        assert_eq!(metrics.read_ops.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.write_ops.load(Ordering::Relaxed), 0);
    }

    // ========================================================================
    // EngineMetrics monotonicity invariants
    // ========================================================================

    #[test]
    fn should_never_decrease_operation_counts() {
        // Arrange
        let metrics = EngineMetrics::new();

        // Act
        metrics.record_read(1000);
        let count1 = metrics.read_ops.load(Ordering::Relaxed);
        metrics.record_read(1000);
        let count2 = metrics.read_ops.load(Ordering::Relaxed);

        // Assert: counts only increase
        assert!(count2 >= count1);
        assert_eq!(count1, 1);
        assert_eq!(count2, 2);
    }

    #[test]
    fn should_never_decrease_byte_counters() {
        // Arrange
        let metrics = EngineMetrics::new();

        // Act
        metrics.record_write(100, 1000);
        let bytes1 = metrics.total_bytes_written.load(Ordering::Relaxed);
        metrics.record_write(200, 1000);
        let bytes2 = metrics.total_bytes_written.load(Ordering::Relaxed);

        // Assert: byte counts only increase
        assert!(bytes2 >= bytes1);
        assert_eq!(bytes1, 100);
        assert_eq!(bytes2, 300);
    }

    // ========================================================================
    // EngineMetrics consistency invariants
    // ========================================================================

    #[test]
    fn should_track_latency_count_matching_operation_count() {
        // Arrange
        let metrics = EngineMetrics::new();

        // Act
        for _ in 0..10 {
            metrics.record_read(1000);
        }

        // Assert: latency count matches operation count
        assert_eq!(metrics.read_ops.load(Ordering::Relaxed), 10);
        assert_eq!(metrics.read_latency_ns.count(), 10);
    }

    #[test]
    fn should_accumulate_write_bytes_per_record() {
        // Arrange
        let metrics = EngineMetrics::new();

        // Act
        metrics.record_write(100, 1000);
        metrics.record_write(200, 1000);
        metrics.record_write(300, 1000);

        // Assert: total is sum of all bytes
        assert_eq!(metrics.total_bytes_written.load(Ordering::Relaxed), 600);
    }

    #[test]
    fn should_accumulate_read_bytes_per_range_scan() {
        // Arrange
        let metrics = EngineMetrics::new();

        // Act
        metrics.record_range_scan(100);
        metrics.record_range_scan(200);
        metrics.record_range_scan(300);

        // Assert: total is sum of all bytes
        assert_eq!(metrics.total_bytes_read.load(Ordering::Relaxed), 600);
    }

    #[test]
    fn should_track_compaction_bytes_separately() {
        // Arrange
        let metrics = EngineMetrics::new();

        // Act
        metrics.record_compaction_bytes(1_000_000, 800_000);
        metrics.record_compaction_bytes(500_000, 400_000);

        // Assert: bytes are accumulated separately
        assert_eq!(
            metrics.compaction_bytes_read.load(Ordering::Relaxed),
            1_500_000
        );
        assert_eq!(
            metrics.compaction_bytes_written.load(Ordering::Relaxed),
            1_200_000
        );
    }

    #[test]
    fn should_track_wal_bytes_accumulated() {
        // Arrange
        let metrics = EngineMetrics::new();

        // Act
        metrics.record_wal_write(512);
        metrics.record_wal_write(1024);
        metrics.record_wal_write(2048);

        // Assert: total accumulated
        assert_eq!(metrics.wal_bytes_written.load(Ordering::Relaxed), 3584);
    }

    // ========================================================================
    // Cache hit rate invariant tests
    // ========================================================================

    #[test]
    fn should_maintain_cache_hit_rate_in_valid_range() {
        // Arrange
        let metrics = EngineMetrics::new();

        // Act
        metrics.record_cache_hit();
        metrics.record_cache_hit();
        metrics.record_cache_miss();

        // Assert: hit rate is between 0.0 and 1.0
        let hit_rate = metrics.cache_hit_rate();
        assert!(hit_rate >= 0.0);
        assert!(hit_rate <= 1.0);
        assert!((hit_rate - 2.0 / 3.0).abs() < 0.001);
    }

    #[test]
    fn should_return_one_when_all_hits() {
        // Arrange
        let metrics = EngineMetrics::new();

        // Act
        for _ in 0..100 {
            metrics.record_cache_hit();
        }

        // Assert: 100% hit rate
        assert_eq!(metrics.cache_hit_rate(), 1.0);
    }

    #[test]
    fn should_return_zero_when_all_misses() {
        // Arrange
        let metrics = EngineMetrics::new();

        // Act
        for _ in 0..100 {
            metrics.record_cache_miss();
        }

        // Assert: 0% hit rate
        assert_eq!(metrics.cache_hit_rate(), 0.0);
    }

    // ========================================================================
    // Compaction ratio invariant tests
    // ========================================================================

    #[test]
    fn should_calculate_compaction_ratio_in_valid_range() {
        // Arrange
        let metrics = EngineMetrics::new();

        // Act: written can't exceed read (compression)
        metrics.record_compaction_bytes(1_000_000, 500_000);

        // Assert: ratio is valid
        let ratio = metrics.compaction_ratio();
        assert!(ratio >= 0.0);
        assert!(ratio <= 1.0);
        assert!((ratio - 0.5).abs() < 0.001);
    }

    #[test]
    fn should_handle_expansion_ratio() {
        // Arrange
        let metrics = EngineMetrics::new();

        // Act: written can exceed read in rare cases
        metrics.record_compaction_bytes(1_000_000, 1_500_000);

        // Assert: ratio can be > 1.0
        let ratio = metrics.compaction_ratio();
        assert!((ratio - 1.5).abs() < 0.001);
    }

    // ========================================================================
    // CompactionGuard RAII tests
    // ========================================================================

    #[test]
    fn should_record_duration_when_guard_drops() {
        // Arrange
        let metrics = EngineMetrics::new();

        // Act
        {
            let _guard = metrics.record_compaction_start();
            std::thread::sleep(std::time::Duration::from_millis(1));
        } // Guard drops here

        // Assert: duration was recorded
        assert!(metrics.compaction_duration_ns.count() > 0);
        assert!(metrics.compaction_duration_ns.max_nanos() >= 1_000_000); // >= 1ms
    }

    #[test]
    fn should_increment_compaction_run_count_on_start() {
        // Arrange
        let metrics = EngineMetrics::new();

        // Act
        let _guard1 = metrics.record_compaction_start();
        assert_eq!(metrics.compaction_runs.load(Ordering::Relaxed), 1);
        let _guard2 = metrics.record_compaction_start();
        assert_eq!(metrics.compaction_runs.load(Ordering::Relaxed), 2);

        // Assert: run count incremented even if guards not dropped yet
        assert_eq!(metrics.compaction_runs.load(Ordering::Relaxed), 2);
    }

    // ========================================================================
    // Total ops aggregation tests
    // ========================================================================

    #[test]
    fn should_sum_only_read_write_delete_for_total_ops() {
        // Arrange
        let metrics = EngineMetrics::new();

        // Act
        metrics.record_read(1000);
        metrics.record_read(1000);
        metrics.record_write(100, 1000);
        metrics.record_delete(1000);
        metrics.record_batch(50); // Should NOT be included
        metrics.record_range_scan(100); // Should NOT be included

        // Assert: total includes only read+write+delete
        assert_eq!(metrics.total_ops(), 4);
    }

    #[test]
    fn should_return_zero_total_ops_when_empty() {
        // Arrange
        let metrics = EngineMetrics::new();

        // Act
        let total = metrics.total_ops();

        // Assert
        assert_eq!(total, 0);
    }

    // ========================================================================
    // Performance metrics backward compatibility
    // ========================================================================

    #[test]
    fn should_create_default_performance_metrics() {
        // Arrange / Act
        let metrics = PerformanceMetrics::default();

        // Assert
        assert_eq!(metrics.read_ops, 0);
        assert_eq!(metrics.write_ops, 0);
        assert_eq!(metrics.delete_ops, 0);
        assert_eq!(metrics.compactions, 0);
    }

    #[test]
    fn should_support_mutable_performance_metrics() {
        // Arrange
        let mut metrics = PerformanceMetrics::new();

        // Act
        metrics.record_read();
        metrics.record_read();
        metrics.record_write();

        // Assert
        assert_eq!(metrics.read_ops, 2);
        assert_eq!(metrics.write_ops, 1);
    }
}
