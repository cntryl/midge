//! Metrics collection for Midge

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::telemetry::config::TelemetryConfig;

trait AtomicU64SaturatingAdd {
    fn saturating_add(&self, value: u64, ordering: Ordering);
}

impl AtomicU64SaturatingAdd for AtomicU64 {
    fn saturating_add(&self, value: u64, ordering: Ordering) {
        let _ = self.fetch_update(ordering, ordering, |current| {
            Some(current.saturating_add(value))
        });
    }
}

/// Metric counters (atomic, zero-copy)
#[derive(Debug)]
pub struct Metrics {
    // Write operations
    #[cfg(test)]
    pub puts: Arc<AtomicU64>,
    #[cfg(test)]
    pub deletes: Arc<AtomicU64>,
    #[cfg(test)]
    pub merges: Arc<AtomicU64>,

    // Read operations
    #[cfg(test)]
    pub gets: Arc<AtomicU64>,
    #[cfg(test)]
    pub range_scans: Arc<AtomicU64>,

    // Latency (microseconds)
    #[cfg(test)]
    pub write_latency_us: Arc<AtomicU64>,
    #[cfg(test)]
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
    pub wal_fsync_ns_max: Arc<AtomicU64>,

    // Recovery operations
    pub wal_recovery_records_replayed: Arc<AtomicU64>,
    pub wal_recovery_bytes_replayed: Arc<AtomicU64>,
    pub intent_log_replay_runs: Arc<AtomicU64>,
    pub intent_log_entries_replayed: Arc<AtomicU64>,

    // Breakdowns
    pub wal_encode_count: Arc<AtomicU64>,
    pub wal_encode_ns_total: Arc<AtomicU64>,
    #[cfg(test)]
    pub wal_lock_wait_count: Arc<AtomicU64>,
    #[cfg(test)]
    pub wal_lock_wait_ns_total: Arc<AtomicU64>,
    pub wal_write_syscall_count: Arc<AtomicU64>,
    pub wal_write_syscall_ns_total: Arc<AtomicU64>,
    pub wal_backpressure_wait_count: Arc<AtomicU64>,
    pub wal_backpressure_wait_attempts_total: Arc<AtomicU64>,
    pub wal_buffer_pool_overflow_count: Arc<AtomicU64>,

    // Thread management
    pub thread_spawn_failures: Arc<AtomicU64>,

    // SST operations
    #[cfg(test)]
    pub sst_created: Arc<AtomicU64>,
    #[cfg(test)]
    pub sst_loaded: Arc<AtomicU64>,

    // Compaction
    pub compactions_run: Arc<AtomicU64>,
    pub compaction_bytes_rewritten: Arc<AtomicU64>,
    pub compaction_failures: Arc<AtomicU64>,

    // Cloud operations
    pub cloud_uploads: Arc<AtomicU64>,
    #[cfg(test)]
    pub cloud_downloads: Arc<AtomicU64>,
    pub cloud_bytes_uploaded: Arc<AtomicU64>,
    #[cfg(test)]
    pub cloud_bytes_downloaded: Arc<AtomicU64>,

    // CloudAsync WAL durability flow
    pub cloud_async_wal_segments_sealed: Arc<AtomicU64>,
    pub cloud_async_wal_bytes_sealed: Arc<AtomicU64>,
    pub cloud_async_wal_seal_latency_us: Arc<AtomicU64>,

    pub cloud_async_wal_uploads_started: Arc<AtomicU64>,
    pub cloud_async_wal_uploads_completed: Arc<AtomicU64>,
    pub cloud_async_wal_uploads_failed: Arc<AtomicU64>,
    pub cloud_async_wal_upload_latency_us: Arc<AtomicU64>,

    pub cloud_async_wal_ack_latency_us: Arc<AtomicU64>,

    // Cache
    pub cache_hits: Arc<AtomicU64>,
    pub cache_misses: Arc<AtomicU64>,

    // Write stalls
    pub write_stalls: Arc<AtomicU64>,
    pub write_stalls_memory: Arc<AtomicU64>,
    pub write_stalls_compaction: Arc<AtomicU64>,
    pub write_stalls_cloud: Arc<AtomicU64>,
    pub write_stalls_no_space: Arc<AtomicU64>,
    pub no_space_events: Arc<AtomicU64>,
    pub write_conflicts: Arc<AtomicU64>,
    pub write_conflicts_point: Arc<AtomicU64>,
    pub write_conflicts_range: Arc<AtomicU64>,

    // Phase 0 guardrails: Idempotency cache telemetry
    #[cfg(test)]
    pub idempotency_cache_evictions: Arc<AtomicU64>,

    // Phase 3 observability: Transaction and sequence metrics
    /// Total number of pending transactions started (set `pending_txn_min_seq`)
    pub pending_txn_started: Arc<AtomicU64>,
    /// Total milliseconds transactions spent pending (sum of all durations)
    pub pending_txn_duration_ms_total: Arc<AtomicU64>,
    /// Maximum pending transaction duration seen (milliseconds)
    pub pending_txn_duration_ms_max: Arc<AtomicU64>,

    // Phase 3 observability: Idempotency cache metrics
    /// Total sequence allocations requested
    #[cfg(test)]
    pub idempotency_alloc_total: Arc<AtomicU64>,
    /// Cache hits (reused cached sequences)
    #[cfg(test)]
    pub idempotency_cache_hits: Arc<AtomicU64>,

    // Event loop
    pub event_loop_wakes: Arc<AtomicU64>,
    pub event_loop_batch_total: Arc<AtomicU64>,

    enabled: bool,
}

impl Metrics {
    /// Create a new metrics collector
    #[must_use]
    pub fn new(config: &TelemetryConfig) -> Self {
        Self {
            #[cfg(test)]
            puts: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            deletes: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            merges: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            gets: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            range_scans: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            write_latency_us: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            read_latency_us: Arc::new(AtomicU64::new(0)),
            wal_appends: Arc::new(AtomicU64::new(0)),
            wal_syncs: Arc::new(AtomicU64::new(0)),
            wal_bytes_written: Arc::new(AtomicU64::new(0)),

            wal_append_count: Arc::new(AtomicU64::new(0)),
            wal_flush_count: Arc::new(AtomicU64::new(0)),
            wal_fsync_count: Arc::new(AtomicU64::new(0)),
            wal_append_ns_total: Arc::new(AtomicU64::new(0)),
            wal_fsync_ns_total: Arc::new(AtomicU64::new(0)),
            wal_fsync_ns_max: Arc::new(AtomicU64::new(0)),

            wal_recovery_records_replayed: Arc::new(AtomicU64::new(0)),
            wal_recovery_bytes_replayed: Arc::new(AtomicU64::new(0)),
            intent_log_replay_runs: Arc::new(AtomicU64::new(0)),
            intent_log_entries_replayed: Arc::new(AtomicU64::new(0)),

            wal_encode_count: Arc::new(AtomicU64::new(0)),
            wal_encode_ns_total: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            wal_lock_wait_count: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            wal_lock_wait_ns_total: Arc::new(AtomicU64::new(0)),
            wal_write_syscall_count: Arc::new(AtomicU64::new(0)),
            wal_write_syscall_ns_total: Arc::new(AtomicU64::new(0)),
            wal_backpressure_wait_count: Arc::new(AtomicU64::new(0)),
            wal_backpressure_wait_attempts_total: Arc::new(AtomicU64::new(0)),
            wal_buffer_pool_overflow_count: Arc::new(AtomicU64::new(0)),
            thread_spawn_failures: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            sst_created: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            sst_loaded: Arc::new(AtomicU64::new(0)),
            compactions_run: Arc::new(AtomicU64::new(0)),
            compaction_bytes_rewritten: Arc::new(AtomicU64::new(0)),
            compaction_failures: Arc::new(AtomicU64::new(0)),
            cloud_uploads: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            cloud_downloads: Arc::new(AtomicU64::new(0)),
            cloud_bytes_uploaded: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            cloud_bytes_downloaded: Arc::new(AtomicU64::new(0)),

            cloud_async_wal_segments_sealed: Arc::new(AtomicU64::new(0)),
            cloud_async_wal_bytes_sealed: Arc::new(AtomicU64::new(0)),
            cloud_async_wal_seal_latency_us: Arc::new(AtomicU64::new(0)),

            cloud_async_wal_uploads_started: Arc::new(AtomicU64::new(0)),
            cloud_async_wal_uploads_completed: Arc::new(AtomicU64::new(0)),
            cloud_async_wal_uploads_failed: Arc::new(AtomicU64::new(0)),
            cloud_async_wal_upload_latency_us: Arc::new(AtomicU64::new(0)),

            cloud_async_wal_ack_latency_us: Arc::new(AtomicU64::new(0)),
            cache_hits: Arc::new(AtomicU64::new(0)),
            cache_misses: Arc::new(AtomicU64::new(0)),
            write_stalls: Arc::new(AtomicU64::new(0)),
            write_stalls_memory: Arc::new(AtomicU64::new(0)),
            write_stalls_compaction: Arc::new(AtomicU64::new(0)),
            write_stalls_cloud: Arc::new(AtomicU64::new(0)),
            write_stalls_no_space: Arc::new(AtomicU64::new(0)),
            no_space_events: Arc::new(AtomicU64::new(0)),
            write_conflicts: Arc::new(AtomicU64::new(0)),
            write_conflicts_point: Arc::new(AtomicU64::new(0)),
            write_conflicts_range: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            idempotency_cache_evictions: Arc::new(AtomicU64::new(0)),
            pending_txn_started: Arc::new(AtomicU64::new(0)),
            pending_txn_duration_ms_total: Arc::new(AtomicU64::new(0)),
            pending_txn_duration_ms_max: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            idempotency_alloc_total: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            idempotency_cache_hits: Arc::new(AtomicU64::new(0)),
            event_loop_wakes: Arc::new(AtomicU64::new(0)),
            event_loop_batch_total: Arc::new(AtomicU64::new(0)),
            enabled: config.enabled && config.features.enable_metrics,
        }
    }

    #[inline]
    pub fn record_event_loop_wake(&self) {
        if self.enabled {
            self.event_loop_wakes.saturating_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn record_event_loop_batch(&self, batch: u64) {
        if self.enabled {
            self.event_loop_batch_total
                .saturating_add(batch, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn record_cloud_async_wal_segment_sealed(&self, bytes: u64, seal_latency_us: u64) {
        if self.enabled {
            self.cloud_async_wal_segments_sealed
                .saturating_add(1, Ordering::Relaxed);
            self.cloud_async_wal_bytes_sealed
                .saturating_add(bytes, Ordering::Relaxed);
            self.cloud_async_wal_seal_latency_us
                .saturating_add(seal_latency_us, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn record_cloud_async_wal_upload_started(&self) {
        if self.enabled {
            self.cloud_async_wal_uploads_started
                .saturating_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn record_cloud_async_wal_upload_completed(&self, upload_latency_us: u64) {
        if self.enabled {
            self.cloud_async_wal_uploads_completed
                .saturating_add(1, Ordering::Relaxed);
            self.cloud_async_wal_upload_latency_us
                .saturating_add(upload_latency_us, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn record_cloud_async_wal_upload_failed(&self) {
        if self.enabled {
            self.cloud_async_wal_uploads_failed
                .saturating_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn record_cloud_async_wal_ack_latency_us(&self, ack_latency_us: u64) {
        if self.enabled {
            self.cloud_async_wal_ack_latency_us
                .saturating_add(ack_latency_us, Ordering::Relaxed);
        }
    }

    /// Record a put operation
    #[inline]
    #[cfg(test)]
    pub fn record_put(&self) {
        if self.enabled {
            self.puts.saturating_add(1, Ordering::Relaxed);
        }
    }

    /// Record a delete operation
    #[inline]
    #[cfg(test)]
    pub fn record_delete(&self) {
        if self.enabled {
            self.deletes.saturating_add(1, Ordering::Relaxed);
        }
    }

    /// Record a get operation
    #[inline]
    #[cfg(test)]
    pub fn record_get(&self) {
        if self.enabled {
            self.gets.saturating_add(1, Ordering::Relaxed);
        }
    }

    /// Record a range scan operation
    #[inline]
    #[cfg(test)]
    pub fn record_range_scan(&self) {
        if self.enabled {
            self.range_scans.saturating_add(1, Ordering::Relaxed);
        }
    }

    /// Record write operation latency (microseconds)
    #[inline]
    #[cfg(test)]
    pub fn record_write_latency_us(&self, latency_us: u64) {
        if self.enabled {
            self.write_latency_us
                .saturating_add(latency_us, Ordering::Relaxed);
        }
    }

    /// Record read operation latency (microseconds)
    #[inline]
    #[cfg(test)]
    pub fn record_read_latency_us(&self, latency_us: u64) {
        if self.enabled {
            self.read_latency_us
                .saturating_add(latency_us, Ordering::Relaxed);
        }
    }

    /// Record a WAL append
    #[inline]
    pub fn record_wal_append(&self, bytes: u64) {
        if self.enabled {
            self.wal_appends.saturating_add(1, Ordering::Relaxed);
            self.wal_bytes_written
                .saturating_add(bytes, Ordering::Relaxed);
        }
    }

    /// Record raw WAL append count (investigation metric)
    #[inline]
    pub fn record_wal_append_count(&self) {
        if self.enabled {
            self.wal_append_count.saturating_add(1, Ordering::Relaxed);
        }
    }

    /// Record WAL append latency (ns)
    #[inline]
    pub fn record_wal_append_ns(&self, ns: u64) {
        if self.enabled {
            self.wal_append_ns_total
                .saturating_add(ns, Ordering::Relaxed);
        }
    }

    /// Record WAL encode
    #[inline]
    pub fn record_wal_encode(&self, ns: u64) {
        if self.enabled {
            self.wal_encode_count.saturating_add(1, Ordering::Relaxed);
            self.wal_encode_ns_total
                .saturating_add(ns, Ordering::Relaxed);
        }
    }

    /// Record WAL lock acquisition wait time
    #[inline]
    #[cfg(test)]
    pub fn record_wal_lock_wait(&self, ns: u64) {
        if self.enabled {
            self.wal_lock_wait_count
                .saturating_add(1, Ordering::Relaxed);
            self.wal_lock_wait_ns_total
                .saturating_add(ns, Ordering::Relaxed);
        }
    }

    /// Record WAL write syscall latency
    #[inline]
    pub fn record_wal_write_syscall(&self, ns: u64) {
        if self.enabled {
            self.wal_write_syscall_count
                .saturating_add(1, Ordering::Relaxed);
            self.wal_write_syscall_ns_total
                .saturating_add(ns, Ordering::Relaxed);
        }
    }

    /// Record a WAL flush
    #[inline]
    pub fn record_wal_flush(&self) {
        if self.enabled {
            self.wal_flush_count.saturating_add(1, Ordering::Relaxed);
        }
    }

    /// Record a WAL fsync (count)
    #[inline]
    pub fn record_wal_fsync_count(&self) {
        if self.enabled {
            self.wal_fsync_count.saturating_add(1, Ordering::Relaxed);
            #[cfg(test)]
            self.wal_syncs.saturating_add(1, Ordering::Relaxed);
        }
    }

    /// Record WAL fsync latency (ns)
    #[inline]
    pub fn record_wal_fsync_ns(&self, ns: u64) {
        if self.enabled {
            self.wal_fsync_ns_total
                .saturating_add(ns, Ordering::Relaxed);
            self.wal_fsync_ns_max.fetch_max(ns, Ordering::Relaxed);
        }
    }

    /// Record WAL recovery totals from startup replay.
    #[inline]
    pub fn record_wal_recovery(&self, records: u64, bytes: u64) {
        if self.enabled {
            self.wal_recovery_records_replayed
                .saturating_add(records, Ordering::Relaxed);
            self.wal_recovery_bytes_replayed
                .saturating_add(bytes, Ordering::Relaxed);
        }
    }

    /// Record intent-log replay performed at startup.
    #[inline]
    pub fn record_intent_log_replay(&self, entries: u64) {
        if self.enabled {
            self.intent_log_replay_runs
                .saturating_add(1, Ordering::Relaxed);
            self.intent_log_entries_replayed
                .saturating_add(entries, Ordering::Relaxed);
        }
    }

    /// Record WAL backpressure wait (when queue is full and producer must wait)
    #[inline]
    pub fn record_wal_backpressure_wait(&self, wait_attempts: u64) {
        if self.enabled {
            self.wal_backpressure_wait_count
                .saturating_add(1, Ordering::Relaxed);
            self.wal_backpressure_wait_attempts_total
                .saturating_add(wait_attempts, Ordering::Relaxed);
        }
    }

    /// Record WAL buffer pool overflow (when buffers are dropped instead of pooled)
    #[inline]
    pub fn record_wal_buffer_pool_overflow(&self, dropped_buffers: u64) {
        if self.enabled {
            self.wal_buffer_pool_overflow_count
                .saturating_add(dropped_buffers, Ordering::Relaxed);
        }
    }

    /// Record thread spawn failure (when system resources exhausted)
    #[inline]
    pub fn record_thread_spawn_failure(&self) {
        if self.enabled {
            self.thread_spawn_failures
                .saturating_add(1, Ordering::Relaxed);
        }
    }

    /// Record a WAL sync
    #[inline]
    pub fn record_wal_sync(&self) {
        if self.enabled {
            self.wal_syncs.saturating_add(1, Ordering::Relaxed);
        }
    }

    /// Record SST creation
    #[inline]
    #[cfg(test)]
    pub fn record_sst_created(&self) {
        if self.enabled {
            self.sst_created.saturating_add(1, Ordering::Relaxed);
        }
    }

    /// Record SST load
    #[inline]
    #[cfg(test)]
    pub fn record_sst_loaded(&self) {
        if self.enabled {
            self.sst_loaded.saturating_add(1, Ordering::Relaxed);
        }
    }

    /// Record a compaction
    #[inline]
    pub fn record_compaction(&self, bytes_rewritten: u64) {
        if self.enabled {
            self.compactions_run.saturating_add(1, Ordering::Relaxed);
            self.compaction_bytes_rewritten
                .saturating_add(bytes_rewritten, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn record_compaction_failure(&self) {
        if self.enabled {
            self.compaction_failures
                .saturating_add(1, Ordering::Relaxed);
        }
    }

    /// Record cloud upload
    #[inline]
    pub fn record_cloud_upload(&self, bytes: u64) {
        if self.enabled {
            self.cloud_uploads.saturating_add(1, Ordering::Relaxed);
            self.cloud_bytes_uploaded
                .saturating_add(bytes, Ordering::Relaxed);
        }
    }

    /// Record cloud download
    #[inline]
    #[cfg(test)]
    pub fn record_cloud_download(&self, bytes: u64) {
        if self.enabled {
            self.cloud_downloads.saturating_add(1, Ordering::Relaxed);
            self.cloud_bytes_downloaded
                .saturating_add(bytes, Ordering::Relaxed);
        }
    }

    /// Record cache hit
    #[inline]
    pub fn record_cache_hit(&self) {
        if self.enabled {
            self.cache_hits.saturating_add(1, Ordering::Relaxed);
        }
    }

    /// Record cache miss
    #[inline]
    pub fn record_cache_miss(&self) {
        if self.enabled {
            self.cache_misses.saturating_add(1, Ordering::Relaxed);
        }
    }

    /// Record write stall
    #[inline]
    #[cfg(test)]
    pub fn record_write_stall(&self) {
        if self.enabled {
            self.write_stalls.saturating_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn record_write_stall_memory(&self) {
        if self.enabled {
            self.write_stalls.saturating_add(1, Ordering::Relaxed);
            self.write_stalls_memory
                .saturating_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn record_write_stall_compaction(&self) {
        if self.enabled {
            self.write_stalls.saturating_add(1, Ordering::Relaxed);
            self.write_stalls_compaction
                .saturating_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn record_write_stall_cloud(&self) {
        if self.enabled {
            self.write_stalls.saturating_add(1, Ordering::Relaxed);
            self.write_stalls_cloud.saturating_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn record_write_stall_no_space(&self) {
        if self.enabled {
            self.write_stalls.saturating_add(1, Ordering::Relaxed);
            self.write_stalls_no_space
                .saturating_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn record_no_space_event(&self) {
        if self.enabled {
            self.no_space_events.saturating_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn record_write_conflict_point(&self) {
        if self.enabled {
            self.write_conflicts.saturating_add(1, Ordering::Relaxed);
            self.write_conflicts_point
                .saturating_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn record_write_conflict_range(&self) {
        if self.enabled {
            self.write_conflicts.saturating_add(1, Ordering::Relaxed);
            self.write_conflicts_range
                .saturating_add(1, Ordering::Relaxed);
        }
    }

    /// Record idempotency cache evictions (Phase 0 guardrail telemetry)
    #[inline]
    #[cfg(test)]
    pub fn record_idempotency_cache_evictions(&self, count: u64) {
        if self.enabled {
            self.idempotency_cache_evictions
                .saturating_add(count, Ordering::Relaxed);
        }
    }

    // === Phase 3 Observability Metrics ===

    /// Record pending transaction started
    #[inline]
    pub fn record_pending_txn_started(&self) {
        if self.enabled {
            self.pending_txn_started
                .saturating_add(1, Ordering::Relaxed);
        }
    }

    /// Record pending transaction duration upon completion
    #[inline]
    pub fn record_pending_txn_duration_ms(&self, duration_ms: u64) {
        if self.enabled {
            self.pending_txn_duration_ms_total
                .saturating_add(duration_ms, Ordering::Relaxed);

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
    #[cfg(test)]
    pub fn record_idempotency_alloc(&self) {
        if self.enabled {
            self.idempotency_alloc_total
                .saturating_add(1, Ordering::Relaxed);
        }
    }

    /// Record idempotency cache hit
    #[inline]
    #[cfg(test)]
    pub fn record_idempotency_cache_hit(&self) {
        if self.enabled {
            self.idempotency_cache_hits
                .saturating_add(1, Ordering::Relaxed);
        }
    }

    /// Get idempotency cache hit rate (hits / total allocations)
    /// Returns None if no allocations have been made
    #[cfg(test)]
    pub fn idempotency_cache_hit_rate(&self) -> Option<f64> {
        let total = self.idempotency_alloc_total.load(Ordering::Relaxed);
        if total == 0 {
            return None;
        }
        let hits = self.idempotency_cache_hits.load(Ordering::Relaxed);
        Some(u64_to_f64(hits) / u64_to_f64(total))
    }

    /// Get average pending transaction duration in milliseconds
    /// Returns None if no transactions have been tracked
    #[cfg(test)]
    pub fn pending_txn_duration_ms_avg(&self) -> Option<f64> {
        let count = self.pending_txn_started.load(Ordering::Relaxed);
        if count == 0 {
            return None;
        }
        let total = self.pending_txn_duration_ms_total.load(Ordering::Relaxed);
        Some(u64_to_f64(total) / u64_to_f64(count))
    }

    /// Get all metrics as a snapshot
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            #[cfg(test)]
            puts: self.puts.load(Ordering::Relaxed),
            #[cfg(test)]
            deletes: self.deletes.load(Ordering::Relaxed),
            #[cfg(test)]
            merges: self.merges.load(Ordering::Relaxed),
            #[cfg(test)]
            gets: self.gets.load(Ordering::Relaxed),
            #[cfg(test)]
            range_scans: self.range_scans.load(Ordering::Relaxed),
            #[cfg(test)]
            write_latency_us: self.write_latency_us.load(Ordering::Relaxed),
            #[cfg(test)]
            read_latency_us: self.read_latency_us.load(Ordering::Relaxed),
            #[cfg(test)]
            wal_appends: self.wal_appends.load(Ordering::Relaxed),
            #[cfg(test)]
            wal_syncs: self.wal_syncs.load(Ordering::Relaxed),
            #[cfg(test)]
            wal_bytes_written: self.wal_bytes_written.load(Ordering::Relaxed),
            #[cfg(test)]
            sst_created: self.sst_created.load(Ordering::Relaxed),
            #[cfg(test)]
            sst_loaded: self.sst_loaded.load(Ordering::Relaxed),
            compactions_run: self.compactions_run.load(Ordering::Relaxed),
            compaction_bytes_rewritten: self.compaction_bytes_rewritten.load(Ordering::Relaxed),
            compaction_failures: self.compaction_failures.load(Ordering::Relaxed),
            #[cfg(test)]
            cloud_uploads: self.cloud_uploads.load(Ordering::Relaxed),
            #[cfg(test)]
            cloud_downloads: self.cloud_downloads.load(Ordering::Relaxed),
            #[cfg(test)]
            cloud_bytes_uploaded: self.cloud_bytes_uploaded.load(Ordering::Relaxed),
            #[cfg(test)]
            cloud_bytes_downloaded: self.cloud_bytes_downloaded.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            write_stalls: self.write_stalls.load(Ordering::Relaxed),
            write_stalls_memory: self.write_stalls_memory.load(Ordering::Relaxed),
            write_stalls_compaction: self.write_stalls_compaction.load(Ordering::Relaxed),
            write_stalls_cloud: self.write_stalls_cloud.load(Ordering::Relaxed),
            write_stalls_no_space: self.write_stalls_no_space.load(Ordering::Relaxed),
            no_space_events: self.no_space_events.load(Ordering::Relaxed),
            write_conflicts: self.write_conflicts.load(Ordering::Relaxed),
            write_conflicts_point: self.write_conflicts_point.load(Ordering::Relaxed),
            write_conflicts_range: self.write_conflicts_range.load(Ordering::Relaxed),
            // New debug fields
            wal_append_count: self.wal_append_count.load(Ordering::Relaxed),
            wal_flush_count: self.wal_flush_count.load(Ordering::Relaxed),
            wal_fsync_count: self.wal_fsync_count.load(Ordering::Relaxed),
            wal_append_ns_total: self.wal_append_ns_total.load(Ordering::Relaxed),
            wal_fsync_ns_total: self.wal_fsync_ns_total.load(Ordering::Relaxed),
            wal_fsync_ns_max: self.wal_fsync_ns_max.load(Ordering::Relaxed),
            cloud_async_wal_segments_sealed: self
                .cloud_async_wal_segments_sealed
                .load(Ordering::Relaxed),
            cloud_async_wal_bytes_sealed: self.cloud_async_wal_bytes_sealed.load(Ordering::Relaxed),
            cloud_async_wal_seal_latency_us: self
                .cloud_async_wal_seal_latency_us
                .load(Ordering::Relaxed),
            cloud_async_wal_uploads_started: self
                .cloud_async_wal_uploads_started
                .load(Ordering::Relaxed),
            cloud_async_wal_uploads_completed: self
                .cloud_async_wal_uploads_completed
                .load(Ordering::Relaxed),
            cloud_async_wal_uploads_failed: self
                .cloud_async_wal_uploads_failed
                .load(Ordering::Relaxed),
            cloud_async_wal_upload_latency_us: self
                .cloud_async_wal_upload_latency_us
                .load(Ordering::Relaxed),
            cloud_async_wal_ack_latency_us: self
                .cloud_async_wal_ack_latency_us
                .load(Ordering::Relaxed),
            #[cfg(test)]
            wal_recovery_records_replayed: self
                .wal_recovery_records_replayed
                .load(Ordering::Relaxed),
            #[cfg(test)]
            wal_recovery_bytes_replayed: self.wal_recovery_bytes_replayed.load(Ordering::Relaxed),
            #[cfg(test)]
            intent_log_replay_runs: self.intent_log_replay_runs.load(Ordering::Relaxed),
            #[cfg(test)]
            intent_log_entries_replayed: self.intent_log_entries_replayed.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time metrics snapshot
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    #[cfg(test)]
    pub puts: u64,
    #[cfg(test)]
    pub deletes: u64,
    #[cfg(test)]
    pub merges: u64,
    #[cfg(test)]
    pub gets: u64,
    #[cfg(test)]
    pub range_scans: u64,
    #[cfg(test)]
    pub write_latency_us: u64,
    #[cfg(test)]
    pub read_latency_us: u64,
    #[cfg(test)]
    pub wal_appends: u64,
    #[cfg(test)]
    pub wal_syncs: u64,
    #[cfg(test)]
    pub wal_bytes_written: u64,
    #[cfg(test)]
    pub sst_created: u64,
    #[cfg(test)]
    pub sst_loaded: u64,
    pub compactions_run: u64,
    pub compaction_bytes_rewritten: u64,
    pub compaction_failures: u64,
    #[cfg(test)]
    pub cloud_uploads: u64,
    #[cfg(test)]
    pub cloud_downloads: u64,
    #[cfg(test)]
    pub cloud_bytes_uploaded: u64,
    #[cfg(test)]
    pub cloud_bytes_downloaded: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub write_stalls: u64,
    pub write_stalls_memory: u64,
    pub write_stalls_compaction: u64,
    pub write_stalls_cloud: u64,
    pub write_stalls_no_space: u64,
    pub no_space_events: u64,
    pub write_conflicts: u64,
    pub write_conflicts_point: u64,
    pub write_conflicts_range: u64,
    // New debug fields
    pub wal_append_count: u64,
    pub wal_flush_count: u64,
    pub wal_fsync_count: u64,
    pub wal_append_ns_total: u64,
    pub wal_fsync_ns_total: u64,
    pub wal_fsync_ns_max: u64,
    pub cloud_async_wal_segments_sealed: u64,
    pub cloud_async_wal_bytes_sealed: u64,
    pub cloud_async_wal_seal_latency_us: u64,
    pub cloud_async_wal_uploads_started: u64,
    pub cloud_async_wal_uploads_completed: u64,
    pub cloud_async_wal_uploads_failed: u64,
    pub cloud_async_wal_upload_latency_us: u64,
    pub cloud_async_wal_ack_latency_us: u64,
    #[cfg(test)]
    pub wal_recovery_records_replayed: u64,
    #[cfg(test)]
    pub wal_recovery_bytes_replayed: u64,
    #[cfg(test)]
    pub intent_log_replay_runs: u64,
    #[cfg(test)]
    pub intent_log_entries_replayed: u64,
}

impl MetricsSnapshot {
    /// Calculate cache hit ratio (0.0..=1.0)
    #[cfg(test)]
    pub fn cache_hit_ratio(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total == 0 {
            0.0
        } else {
            u64_to_f64(self.cache_hits) / u64_to_f64(total)
        }
    }

    /// Total write operations
    #[cfg(test)]
    pub fn total_writes(&self) -> u64 {
        self.puts + self.deletes + self.merges
    }

    /// Total cloud bytes transferred
    #[cfg(test)]
    pub fn total_cloud_bytes(&self) -> u64 {
        self.cloud_bytes_uploaded + self.cloud_bytes_downloaded
    }
}

#[cfg(test)]
fn u64_to_f64(value: u64) -> f64 {
    let upper = u32::try_from(value >> 32).unwrap_or(u32::MAX);
    let lower_mask = u64::from(u32::MAX);
    let lower = u32::try_from(value & lower_mask).unwrap_or(u32::MAX);
    f64::from(upper) * 4_294_967_296.0 + f64::from(lower)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_record_metrics_atomically() {
        // Arrange
        let config = TelemetryConfig::default().with_enabled(true);
        let metrics = Metrics::new(&config);

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
        let metrics = Metrics::new(&config);

        // Act
        metrics.record_cache_hit();
        metrics.record_cache_hit();
        metrics.record_cache_miss();

        let snap = metrics.snapshot();

        // Assert
        assert!((snap.cache_hit_ratio() - (2.0 / 3.0)).abs() < 0.001);
    }

    #[test]
    fn should_return_zero_cache_hit_ratio_given_no_cache_requests_when_snapshotting_metrics() {
        // Arrange
        let metrics = Metrics::new(&TelemetryConfig::default().with_enabled(true));

        // Act
        let snapshot = metrics.snapshot();

        // Assert
        assert!(snapshot.cache_hit_ratio().abs() < f64::EPSILON);
    }

    #[test]
    fn should_preserve_metric_saturation_given_counter_overflow_when_recording() {
        // Arrange
        let metrics = Metrics::new(&TelemetryConfig::default().with_enabled(true));
        metrics.cache_hits.store(u64::MAX - 1, Ordering::Relaxed);

        // Act
        metrics.record_cache_hit();
        metrics.record_cache_hit();
        let snapshot = metrics.snapshot();

        // Assert
        assert_eq!(snapshot.cache_hits, u64::MAX);
    }

    #[test]
    fn should_not_record_when_disabled() {
        // Arrange
        let config = TelemetryConfig::default().with_enabled(false);
        let metrics = Metrics::new(&config);

        // Act
        metrics.record_put();
        metrics.record_put();

        let snap = metrics.snapshot();

        // Assert
        assert_eq!(snap.puts, 0);
    }

    #[test]
    fn should_record_recovery_metrics_when_replay_occurs() {
        // Arrange
        let config = TelemetryConfig::default().with_enabled(true);
        let metrics = Metrics::new(&config);

        // Act
        metrics.record_wal_recovery(42, 4096);
        metrics.record_intent_log_replay(3);

        let snap = metrics.snapshot();

        // Assert
        assert_eq!(snap.wal_recovery_records_replayed, 42);
        assert_eq!(snap.wal_recovery_bytes_replayed, 4096);
        assert_eq!(snap.intent_log_replay_runs, 1);
        assert_eq!(snap.intent_log_entries_replayed, 3);
    }

    #[test]
    fn should_record_strict_write_conflict_metrics_with_breakdown() {
        // Arrange
        let config = TelemetryConfig::default().with_enabled(true);
        let metrics = Metrics::new(&config);

        // Act
        metrics.record_write_conflict_point();
        metrics.record_write_conflict_range();
        metrics.record_write_conflict_range();

        let snap = metrics.snapshot();

        // Assert
        assert_eq!(snap.write_conflicts, 3);
        assert_eq!(snap.write_conflicts_point, 1);
        assert_eq!(snap.write_conflicts_range, 2);
    }

    #[test]
    fn should_record_test_visible_metric_rates() {
        // Arrange
        let config = TelemetryConfig::default().with_enabled(true);
        let metrics = Metrics::new(&config);

        // Act
        metrics.record_put();
        metrics.record_delete();
        metrics.record_get();
        metrics.record_range_scan();
        metrics.record_write_latency_us(11);
        metrics.record_read_latency_us(7);
        metrics.record_wal_append(128);
        metrics.record_wal_sync();
        metrics.record_wal_lock_wait(3);
        metrics.record_sst_created();
        metrics.record_sst_loaded();
        metrics.record_cloud_upload(256);
        metrics.record_cloud_download(512);
        metrics.record_write_stall();
        metrics.record_idempotency_alloc();
        metrics.record_idempotency_cache_hit();
        metrics.record_pending_txn_started();
        metrics.record_pending_txn_duration_ms(9);

        let snap = metrics.snapshot();

        // Assert
        assert_eq!(snap.total_writes(), 2);
        assert_eq!(snap.merges, 0);
        assert_eq!(snap.gets, 1);
        assert_eq!(snap.range_scans, 1);
        assert_eq!(snap.write_latency_us, 11);
        assert_eq!(snap.read_latency_us, 7);
        assert_eq!(snap.wal_appends, 1);
        assert_eq!(snap.wal_syncs, 1);
        assert_eq!(snap.wal_bytes_written, 128);
        assert_eq!(snap.sst_created, 1);
        assert_eq!(snap.sst_loaded, 1);
        assert_eq!(snap.cloud_uploads, 1);
        assert_eq!(snap.cloud_downloads, 1);
        assert_eq!(snap.total_cloud_bytes(), 768);
        assert_eq!(snap.write_stalls, 1);
        assert_eq!(metrics.wal_lock_wait_count.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.wal_lock_wait_ns_total.load(Ordering::Relaxed), 3);
        assert_eq!(metrics.idempotency_cache_hit_rate(), Some(1.0));
        assert_eq!(metrics.pending_txn_duration_ms_avg(), Some(9.0));
    }
}
