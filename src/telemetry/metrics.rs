//! Metrics collection for Midge

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::common::MidgeResult;
use crate::telemetry::config::TelemetryConfig;

/// Metric counters (atomic, zero-copy)
#[derive(Debug)]
pub struct Metrics {
    // Write operations
    pub puts: Arc<AtomicU64>,
    pub deletes: Arc<AtomicU64>,
    pub merges: Arc<AtomicU64>,

    // Read operations
    pub gets: Arc<AtomicU64>,
    pub range_scans: Arc<AtomicU64>,

    // Latency (microseconds)
    pub write_latency_us: Arc<AtomicU64>,
    pub read_latency_us: Arc<AtomicU64>,

    // WAL operations
    pub wal_appends: Arc<AtomicU64>,
    pub wal_syncs: Arc<AtomicU64>,
    pub wal_bytes_written: Arc<AtomicU64>,

    // Detailed WAL telemetry requested by perf investigation
    pub wal_append_count: Arc<AtomicU64>,
    pub wal_flush_count: Arc<AtomicU64>,
    pub wal_fsync_count: Arc<AtomicU64>,
    pub wal_append_ns_total: Arc<AtomicU64>,
    pub wal_fsync_ns_total: Arc<AtomicU64>,

    // Breakdowns
    pub wal_encode_count: Arc<AtomicU64>,
    pub wal_encode_ns_total: Arc<AtomicU64>,
    pub wal_lock_wait_count: Arc<AtomicU64>,
    pub wal_lock_wait_ns_total: Arc<AtomicU64>,
    pub wal_write_syscall_count: Arc<AtomicU64>,
    pub wal_write_syscall_ns_total: Arc<AtomicU64>,
    pub wal_backpressure_wait_count: Arc<AtomicU64>,
    pub wal_backpressure_wait_attempts_total: Arc<AtomicU64>,

    // SST operations
    pub sst_created: Arc<AtomicU64>,
    pub sst_loaded: Arc<AtomicU64>,

    // Compaction
    pub compactions_run: Arc<AtomicU64>,
    pub compaction_bytes_rewritten: Arc<AtomicU64>,

    // Cloud operations
    pub cloud_uploads: Arc<AtomicU64>,
    pub cloud_downloads: Arc<AtomicU64>,
    pub cloud_bytes_uploaded: Arc<AtomicU64>,
    pub cloud_bytes_downloaded: Arc<AtomicU64>,

    // CloudFirst WAL durability flow
    pub cloudfirst_wal_segments_sealed: Arc<AtomicU64>,
    pub cloudfirst_wal_bytes_sealed: Arc<AtomicU64>,
    pub cloudfirst_wal_seal_latency_us: Arc<AtomicU64>,

    pub cloudfirst_wal_uploads_started: Arc<AtomicU64>,
    pub cloudfirst_wal_uploads_completed: Arc<AtomicU64>,
    pub cloudfirst_wal_uploads_failed: Arc<AtomicU64>,
    pub cloudfirst_wal_upload_latency_us: Arc<AtomicU64>,

    pub cloudfirst_wal_ack_latency_us: Arc<AtomicU64>,

    // Cache
    pub cache_hits: Arc<AtomicU64>,
    pub cache_misses: Arc<AtomicU64>,

    // Write stalls
    pub write_stalls: Arc<AtomicU64>,

    // Phase 0 guardrails: Idempotency cache telemetry
    pub idempotency_cache_evictions: Arc<AtomicU64>,

    // Phase 3 observability: Transaction and sequence metrics
    /// Total number of pending transactions started (set pending_txn_min_seq)
    pub pending_txn_started: Arc<AtomicU64>,
    /// Total milliseconds transactions spent pending (sum of all durations)
    pub pending_txn_duration_ms_total: Arc<AtomicU64>,
    /// Maximum pending transaction duration seen (milliseconds)
    pub pending_txn_duration_ms_max: Arc<AtomicU64>,

    // Phase 3 observability: Idempotency cache metrics
    /// Total sequence allocations requested
    pub idempotency_alloc_total: Arc<AtomicU64>,
    /// Cache hits (reused cached sequences)
    pub idempotency_cache_hits: Arc<AtomicU64>,

    enabled: bool,
}

impl Metrics {
    /// Create a new metrics collector
    pub fn new(_config: &TelemetryConfig) -> MidgeResult<Self> {
        Ok(Self {
            puts: Arc::new(AtomicU64::new(0)),
            deletes: Arc::new(AtomicU64::new(0)),
            merges: Arc::new(AtomicU64::new(0)),
            gets: Arc::new(AtomicU64::new(0)),
            range_scans: Arc::new(AtomicU64::new(0)),
            write_latency_us: Arc::new(AtomicU64::new(0)),
            read_latency_us: Arc::new(AtomicU64::new(0)),
            wal_appends: Arc::new(AtomicU64::new(0)),
            wal_syncs: Arc::new(AtomicU64::new(0)),
            wal_bytes_written: Arc::new(AtomicU64::new(0)),

            wal_append_count: Arc::new(AtomicU64::new(0)),
            wal_flush_count: Arc::new(AtomicU64::new(0)),
            wal_fsync_count: Arc::new(AtomicU64::new(0)),
            wal_append_ns_total: Arc::new(AtomicU64::new(0)),
            wal_fsync_ns_total: Arc::new(AtomicU64::new(0)),

            wal_encode_count: Arc::new(AtomicU64::new(0)),
            wal_encode_ns_total: Arc::new(AtomicU64::new(0)),
            wal_lock_wait_count: Arc::new(AtomicU64::new(0)),
            wal_lock_wait_ns_total: Arc::new(AtomicU64::new(0)),
            wal_write_syscall_count: Arc::new(AtomicU64::new(0)),
            wal_write_syscall_ns_total: Arc::new(AtomicU64::new(0)),
            wal_backpressure_wait_count: Arc::new(AtomicU64::new(0)),
            wal_backpressure_wait_attempts_total: Arc::new(AtomicU64::new(0)),
            sst_created: Arc::new(AtomicU64::new(0)),
            sst_loaded: Arc::new(AtomicU64::new(0)),
            compactions_run: Arc::new(AtomicU64::new(0)),
            compaction_bytes_rewritten: Arc::new(AtomicU64::new(0)),
            cloud_uploads: Arc::new(AtomicU64::new(0)),
            cloud_downloads: Arc::new(AtomicU64::new(0)),
            cloud_bytes_uploaded: Arc::new(AtomicU64::new(0)),
            cloud_bytes_downloaded: Arc::new(AtomicU64::new(0)),

            cloudfirst_wal_segments_sealed: Arc::new(AtomicU64::new(0)),
            cloudfirst_wal_bytes_sealed: Arc::new(AtomicU64::new(0)),
            cloudfirst_wal_seal_latency_us: Arc::new(AtomicU64::new(0)),

            cloudfirst_wal_uploads_started: Arc::new(AtomicU64::new(0)),
            cloudfirst_wal_uploads_completed: Arc::new(AtomicU64::new(0)),
            cloudfirst_wal_uploads_failed: Arc::new(AtomicU64::new(0)),
            cloudfirst_wal_upload_latency_us: Arc::new(AtomicU64::new(0)),

            cloudfirst_wal_ack_latency_us: Arc::new(AtomicU64::new(0)),
            cache_hits: Arc::new(AtomicU64::new(0)),
            cache_misses: Arc::new(AtomicU64::new(0)),
            write_stalls: Arc::new(AtomicU64::new(0)),
            idempotency_cache_evictions: Arc::new(AtomicU64::new(0)),
            pending_txn_started: Arc::new(AtomicU64::new(0)),
            pending_txn_duration_ms_total: Arc::new(AtomicU64::new(0)),
            pending_txn_duration_ms_max: Arc::new(AtomicU64::new(0)),
            idempotency_alloc_total: Arc::new(AtomicU64::new(0)),
            idempotency_cache_hits: Arc::new(AtomicU64::new(0)),
            enabled: _config.enabled && _config.enable_metrics,
        })
    }

    #[inline]
    pub fn record_cloudfirst_wal_segment_sealed(&self, bytes: u64, seal_latency_us: u64) {
        if self.enabled {
            self.cloudfirst_wal_segments_sealed
                .fetch_add(1, Ordering::Relaxed);
            self.cloudfirst_wal_bytes_sealed
                .fetch_add(bytes, Ordering::Relaxed);
            self.cloudfirst_wal_seal_latency_us
                .fetch_add(seal_latency_us, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn record_cloudfirst_wal_upload_started(&self) {
        if self.enabled {
            self.cloudfirst_wal_uploads_started
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn record_cloudfirst_wal_upload_completed(&self, upload_latency_us: u64) {
        if self.enabled {
            self.cloudfirst_wal_uploads_completed
                .fetch_add(1, Ordering::Relaxed);
            self.cloudfirst_wal_upload_latency_us
                .fetch_add(upload_latency_us, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn record_cloudfirst_wal_upload_failed(&self) {
        if self.enabled {
            self.cloudfirst_wal_uploads_failed
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn record_cloudfirst_wal_ack_latency_us(&self, ack_latency_us: u64) {
        if self.enabled {
            self.cloudfirst_wal_ack_latency_us
                .fetch_add(ack_latency_us, Ordering::Relaxed);
        }
    }

    /// Record a put operation
    #[inline]
    pub fn record_put(&self) {
        if self.enabled {
            self.puts.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a delete operation
    #[inline]
    pub fn record_delete(&self) {
        if self.enabled {
            self.deletes.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a get operation
    #[inline]
    pub fn record_get(&self) {
        if self.enabled {
            self.gets.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a range scan operation
    #[inline]
    pub fn record_range_scan(&self) {
        if self.enabled {
            self.range_scans.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record write operation latency (microseconds)
    #[inline]
    pub fn record_write_latency_us(&self, latency_us: u64) {
        if self.enabled {
            self.write_latency_us
                .fetch_add(latency_us, Ordering::Relaxed);
        }
    }

    /// Record read operation latency (microseconds)
    #[inline]
    pub fn record_read_latency_us(&self, latency_us: u64) {
        if self.enabled {
            self.read_latency_us
                .fetch_add(latency_us, Ordering::Relaxed);
        }
    }

    /// Record a WAL append
    #[inline]
    pub fn record_wal_append(&self, bytes: u64) {
        if self.enabled {
            self.wal_appends.fetch_add(1, Ordering::Relaxed);
            self.wal_bytes_written.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// Record raw WAL append count (investigation metric)
    #[inline]
    pub fn record_wal_append_count(&self) {
        if self.enabled {
            self.wal_append_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record WAL append latency (ns)
    #[inline]
    pub fn record_wal_append_ns(&self, ns: u64) {
        if self.enabled {
            self.wal_append_ns_total.fetch_add(ns, Ordering::Relaxed);
        }
    }

    /// Record WAL encode
    #[inline]
    pub fn record_wal_encode(&self, ns: u64) {
        if self.enabled {
            self.wal_encode_count.fetch_add(1, Ordering::Relaxed);
            self.wal_encode_ns_total.fetch_add(ns, Ordering::Relaxed);
        }
    }

    /// Record WAL lock acquisition wait time
    #[inline]
    pub fn record_wal_lock_wait(&self, ns: u64) {
        if self.enabled {
            self.wal_lock_wait_count.fetch_add(1, Ordering::Relaxed);
            self.wal_lock_wait_ns_total.fetch_add(ns, Ordering::Relaxed);
        }
    }

    /// Record WAL write syscall latency
    #[inline]
    pub fn record_wal_write_syscall(&self, ns: u64) {
        if self.enabled {
            self.wal_write_syscall_count.fetch_add(1, Ordering::Relaxed);
            self.wal_write_syscall_ns_total
                .fetch_add(ns, Ordering::Relaxed);
        }
    }

    /// Record a WAL flush
    #[inline]
    pub fn record_wal_flush(&self) {
        if self.enabled {
            self.wal_flush_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a WAL fsync (count)
    #[inline]
    pub fn record_wal_fsync_count(&self) {
        if self.enabled {
            self.wal_fsync_count.fetch_add(1, Ordering::Relaxed);
            self.wal_syncs.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record WAL fsync latency (ns)
    #[inline]
    pub fn record_wal_fsync_ns(&self, ns: u64) {
        if self.enabled {
            self.wal_fsync_ns_total.fetch_add(ns, Ordering::Relaxed);
        }
    }

    /// Record WAL backpressure wait (when queue is full and producer must wait)
    #[inline]
    pub fn record_wal_backpressure_wait(&self, wait_attempts: u64) {
        if self.enabled {
            self.wal_backpressure_wait_count.fetch_add(1, Ordering::Relaxed);
            self.wal_backpressure_wait_attempts_total
                .fetch_add(wait_attempts, Ordering::Relaxed);
        }
    }

    /// Record a WAL sync
    #[inline]
    pub fn record_wal_sync(&self) {
        if self.enabled {
            self.wal_syncs.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record SST creation
    #[inline]
    pub fn record_sst_created(&self) {
        if self.enabled {
            self.sst_created.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record SST load
    #[inline]
    pub fn record_sst_loaded(&self) {
        if self.enabled {
            self.sst_loaded.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a compaction
    #[inline]
    pub fn record_compaction(&self, bytes_rewritten: u64) {
        if self.enabled {
            self.compactions_run.fetch_add(1, Ordering::Relaxed);
            self.compaction_bytes_rewritten
                .fetch_add(bytes_rewritten, Ordering::Relaxed);
        }
    }

    /// Record cloud upload
    #[inline]
    pub fn record_cloud_upload(&self, bytes: u64) {
        if self.enabled {
            self.cloud_uploads.fetch_add(1, Ordering::Relaxed);
            self.cloud_bytes_uploaded
                .fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// Record cloud download
    #[inline]
    pub fn record_cloud_download(&self, bytes: u64) {
        if self.enabled {
            self.cloud_downloads.fetch_add(1, Ordering::Relaxed);
            self.cloud_bytes_downloaded
                .fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// Record cache hit
    #[inline]
    pub fn record_cache_hit(&self) {
        if self.enabled {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record cache miss
    #[inline]
    pub fn record_cache_miss(&self) {
        if self.enabled {
            self.cache_misses.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record write stall
    #[inline]
    pub fn record_write_stall(&self) {
        if self.enabled {
            self.write_stalls.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record idempotency cache evictions (Phase 0 guardrail telemetry)
    #[inline]
    pub fn record_idempotency_cache_evictions(&self, count: u64) {
        if self.enabled {
            self.idempotency_cache_evictions
                .fetch_add(count, Ordering::Relaxed);
        }
    }

    // === Phase 3 Observability Metrics ===

    /// Record pending transaction started
    #[inline]
    pub fn record_pending_txn_started(&self) {
        if self.enabled {
            self.pending_txn_started.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record pending transaction duration upon completion
    #[inline]
    pub fn record_pending_txn_duration_ms(&self, duration_ms: u64) {
        if self.enabled {
            self.pending_txn_duration_ms_total
                .fetch_add(duration_ms, Ordering::Relaxed);

            // Update max duration (best-effort, may race but that's acceptable for observability)
            let current_max = self.pending_txn_duration_ms_max.load(Ordering::Relaxed);
            if duration_ms > current_max {
                self.pending_txn_duration_ms_max
                    .store(duration_ms, Ordering::Relaxed);
            }
        }
    }

    /// Record sequence allocation (for cache hit rate calculation)
    #[inline]
    pub fn record_idempotency_alloc(&self) {
        if self.enabled {
            self.idempotency_alloc_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record idempotency cache hit
    #[inline]
    pub fn record_idempotency_cache_hit(&self) {
        if self.enabled {
            self.idempotency_cache_hits.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Get idempotency cache hit rate (hits / total allocations)
    /// Returns None if no allocations have been made
    pub fn idempotency_cache_hit_rate(&self) -> Option<f64> {
        let total = self.idempotency_alloc_total.load(Ordering::Relaxed);
        if total == 0 {
            return None;
        }
        let hits = self.idempotency_cache_hits.load(Ordering::Relaxed);
        Some(hits as f64 / total as f64)
    }

    /// Get average pending transaction duration in milliseconds
    /// Returns None if no transactions have been tracked
    pub fn pending_txn_duration_ms_avg(&self) -> Option<f64> {
        let count = self.pending_txn_started.load(Ordering::Relaxed);
        if count == 0 {
            return None;
        }
        let total = self.pending_txn_duration_ms_total.load(Ordering::Relaxed);
        Some(total as f64 / count as f64)
    }

    /// Get all metrics as a snapshot
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            puts: self.puts.load(Ordering::Relaxed),
            deletes: self.deletes.load(Ordering::Relaxed),
            merges: self.merges.load(Ordering::Relaxed),
            gets: self.gets.load(Ordering::Relaxed),
            range_scans: self.range_scans.load(Ordering::Relaxed),
            write_latency_us: self.write_latency_us.load(Ordering::Relaxed),
            read_latency_us: self.read_latency_us.load(Ordering::Relaxed),
            wal_appends: self.wal_appends.load(Ordering::Relaxed),
            wal_syncs: self.wal_syncs.load(Ordering::Relaxed),
            wal_bytes_written: self.wal_bytes_written.load(Ordering::Relaxed),
            sst_created: self.sst_created.load(Ordering::Relaxed),
            sst_loaded: self.sst_loaded.load(Ordering::Relaxed),
            compactions_run: self.compactions_run.load(Ordering::Relaxed),
            compaction_bytes_rewritten: self.compaction_bytes_rewritten.load(Ordering::Relaxed),
            cloud_uploads: self.cloud_uploads.load(Ordering::Relaxed),
            cloud_downloads: self.cloud_downloads.load(Ordering::Relaxed),
            cloud_bytes_uploaded: self.cloud_bytes_uploaded.load(Ordering::Relaxed),
            cloud_bytes_downloaded: self.cloud_bytes_downloaded.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            write_stalls: self.write_stalls.load(Ordering::Relaxed),
            // New debug fields
            wal_append_count: self.wal_append_count.load(Ordering::Relaxed),
            wal_flush_count: self.wal_flush_count.load(Ordering::Relaxed),
            wal_fsync_count: self.wal_fsync_count.load(Ordering::Relaxed),
            wal_append_ns_total: self.wal_append_ns_total.load(Ordering::Relaxed),
            wal_fsync_ns_total: self.wal_fsync_ns_total.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time metrics snapshot
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub puts: u64,
    pub deletes: u64,
    pub merges: u64,
    pub gets: u64,
    pub range_scans: u64,
    pub write_latency_us: u64,
    pub read_latency_us: u64,
    pub wal_appends: u64,
    pub wal_syncs: u64,
    pub wal_bytes_written: u64,
    pub sst_created: u64,
    pub sst_loaded: u64,
    pub compactions_run: u64,
    pub compaction_bytes_rewritten: u64,
    pub cloud_uploads: u64,
    pub cloud_downloads: u64,
    pub cloud_bytes_uploaded: u64,
    pub cloud_bytes_downloaded: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub write_stalls: u64,
    // New debug fields
    pub wal_append_count: u64,
    pub wal_flush_count: u64,
    pub wal_fsync_count: u64,
    pub wal_append_ns_total: u64,
    pub wal_fsync_ns_total: u64,
}

impl MetricsSnapshot {
    /// Calculate cache hit ratio (0.0..=1.0)
    pub fn cache_hit_ratio(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total == 0 {
            0.0
        } else {
            self.cache_hits as f64 / total as f64
        }
    }

    /// Total write operations
    pub fn total_writes(&self) -> u64 {
        self.puts + self.deletes + self.merges
    }

    /// Total cloud bytes transferred
    pub fn total_cloud_bytes(&self) -> u64 {
        self.cloud_bytes_uploaded + self.cloud_bytes_downloaded
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_record_metrics_atomically() {
        // Arrange
        let config = TelemetryConfig::default().with_enabled(true);
        let metrics = Metrics::new(&config).unwrap();

        // Act
        metrics.record_put();
        metrics.record_put();
        metrics.record_delete();

        let snap = metrics.snapshot();

        // Assert
        assert_eq!(snap.puts, 2);
        assert_eq!(snap.deletes, 1);
    }

    #[test]
    fn should_calculate_cache_hit_ratio() {
        // Arrange
        let config = TelemetryConfig::default().with_enabled(true);
        let metrics = Metrics::new(&config).unwrap();

        // Act
        metrics.record_cache_hit();
        metrics.record_cache_hit();
        metrics.record_cache_miss();

        let snap = metrics.snapshot();

        // Assert
        assert!((snap.cache_hit_ratio() - (2.0 / 3.0)).abs() < 0.001);
    }

    #[test]
    fn should_not_record_when_disabled() {
        // Arrange
        let config = TelemetryConfig::default().with_enabled(false);
        let metrics = Metrics::new(&config).unwrap();

        // Act
        metrics.record_put();
        metrics.record_put();

        let snap = metrics.snapshot();

        // Assert
        assert_eq!(snap.puts, 0);
    }
}
