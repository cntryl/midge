use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// Thread-safe metrics collector for database operations.
///
/// Provides counters and histograms for monitoring database performance,
/// resource usage, and operational health.
#[derive(Debug, Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

#[derive(Debug)]
struct MetricsInner {
    // Operation counters
    get_count: AtomicU64,
    put_count: AtomicU64,
    delete_count: AtomicU64,
    scan_count: AtomicU64,
    multi_get_count: AtomicU64,

    // Cache metrics
    block_cache_hits: AtomicU64,
    block_cache_misses: AtomicU64,
    table_cache_hits: AtomicU64,
    table_cache_misses: AtomicU64,

    // Bloom filter metrics
    bloom_filter_checks: AtomicU64,
    bloom_filter_positives: AtomicU64,
    bloom_filter_true_positives: AtomicU64,

    // Write path metrics
    memtable_writes: AtomicU64,
    memtable_flushes: AtomicU64,
    wal_writes: AtomicU64,
    wal_syncs: AtomicU64,

    // Compaction metrics
    compactions_started: AtomicU64,
    compactions_completed: AtomicU64,
    compactions_failed: AtomicU64,
    compaction_bytes_read: AtomicU64,
    compaction_bytes_written: AtomicU64,

    // Storage metrics
    sst_count: AtomicUsize,
    total_sst_bytes: AtomicU64,

    // Snapshot metrics
    active_snapshots: AtomicUsize,

    // Error metrics
    read_errors: AtomicU64,
    write_errors: AtomicU64,
    compaction_errors: AtomicU64,

    // Tombstone metrics
    point_tombstones_created: AtomicU64,
    range_tombstones_created: AtomicU64,
    tombstones_removed_compaction: AtomicU64,
    tombstones_coalesced: AtomicU64,
    tombstone_checks: AtomicU64,

    // Write stall metrics
    write_stalls: AtomicU64,
    write_stall_duration_ms: AtomicU64,
    background_write_stalls: AtomicU64,
    background_write_stall_duration_ms: AtomicU64,
    capacity_write_stalls: AtomicU64,
    capacity_write_stall_duration_ms: AtomicU64,

    // Autotuning metrics
    autotune_wal_interval_adjustments: AtomicU64,
    autotune_compaction_thread_adjustments: AtomicU64,
    autotune_bloom_bits_adjustments: AtomicU64,

    // Flush throughput metrics
    flush_bytes_total: AtomicU64,
    flush_duration_us_total: AtomicU64,
    flush_queue_depth: AtomicU64,
    flush_queue_high_watermark: AtomicU64,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MetricsInner {
                get_count: AtomicU64::new(0),
                put_count: AtomicU64::new(0),
                delete_count: AtomicU64::new(0),
                scan_count: AtomicU64::new(0),
                multi_get_count: AtomicU64::new(0),

                block_cache_hits: AtomicU64::new(0),
                block_cache_misses: AtomicU64::new(0),
                table_cache_hits: AtomicU64::new(0),
                table_cache_misses: AtomicU64::new(0),

                bloom_filter_checks: AtomicU64::new(0),
                bloom_filter_positives: AtomicU64::new(0),
                bloom_filter_true_positives: AtomicU64::new(0),

                memtable_writes: AtomicU64::new(0),
                memtable_flushes: AtomicU64::new(0),
                wal_writes: AtomicU64::new(0),
                wal_syncs: AtomicU64::new(0),

                compactions_started: AtomicU64::new(0),
                compactions_completed: AtomicU64::new(0),
                compactions_failed: AtomicU64::new(0),
                compaction_bytes_read: AtomicU64::new(0),
                compaction_bytes_written: AtomicU64::new(0),

                sst_count: AtomicUsize::new(0),
                total_sst_bytes: AtomicU64::new(0),

                active_snapshots: AtomicUsize::new(0),

                read_errors: AtomicU64::new(0),
                write_errors: AtomicU64::new(0),
                compaction_errors: AtomicU64::new(0),

                point_tombstones_created: AtomicU64::new(0),
                range_tombstones_created: AtomicU64::new(0),
                tombstones_removed_compaction: AtomicU64::new(0),
                tombstones_coalesced: AtomicU64::new(0),
                tombstone_checks: AtomicU64::new(0),

                write_stalls: AtomicU64::new(0),
                write_stall_duration_ms: AtomicU64::new(0),
                background_write_stalls: AtomicU64::new(0),
                background_write_stall_duration_ms: AtomicU64::new(0),
                capacity_write_stalls: AtomicU64::new(0),
                capacity_write_stall_duration_ms: AtomicU64::new(0),

                autotune_wal_interval_adjustments: AtomicU64::new(0),
                autotune_compaction_thread_adjustments: AtomicU64::new(0),
                autotune_bloom_bits_adjustments: AtomicU64::new(0),

                flush_bytes_total: AtomicU64::new(0),
                flush_duration_us_total: AtomicU64::new(0),
                flush_queue_depth: AtomicU64::new(0),
                flush_queue_high_watermark: AtomicU64::new(0),
            }),
        }
    }

    // Operation counters
    pub fn record_get(&self) {
        self.inner.get_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_put(&self) {
        self.inner.put_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_delete(&self) {
        self.inner.delete_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_scan(&self) {
        self.inner.scan_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_multi_get(&self, count: usize) {
        self.inner.multi_get_count.fetch_add(1, Ordering::Relaxed);
        self.inner
            .get_count
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    // Cache metrics
    pub fn record_block_cache_hit(&self) {
        self.inner.block_cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_block_cache_miss(&self) {
        self.inner
            .block_cache_misses
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_table_cache_hit(&self) {
        self.inner.table_cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_table_cache_miss(&self) {
        self.inner
            .table_cache_misses
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn block_cache_hit_rate(&self) -> f64 {
        let hits = self.inner.block_cache_hits.load(Ordering::Relaxed);
        let misses = self.inner.block_cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    pub fn table_cache_hit_rate(&self) -> f64 {
        let hits = self.inner.table_cache_hits.load(Ordering::Relaxed);
        let misses = self.inner.table_cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    // Bloom filter metrics
    pub fn record_bloom_check(&self, may_contain: bool, actually_exists: bool) {
        self.inner
            .bloom_filter_checks
            .fetch_add(1, Ordering::Relaxed);
        if may_contain {
            self.inner
                .bloom_filter_positives
                .fetch_add(1, Ordering::Relaxed);
            if actually_exists {
                self.inner
                    .bloom_filter_true_positives
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn bloom_false_positive_rate(&self) -> f64 {
        let positives = self.inner.bloom_filter_positives.load(Ordering::Relaxed);
        let true_positives = self
            .inner
            .bloom_filter_true_positives
            .load(Ordering::Relaxed);
        if positives == 0 {
            0.0
        } else {
            let false_positives = positives.saturating_sub(true_positives);
            false_positives as f64 / positives as f64
        }
    }

    // Write path metrics
    pub fn record_memtable_write(&self) {
        self.inner.memtable_writes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_memtable_flush(&self) {
        self.inner.memtable_flushes.fetch_add(1, Ordering::Relaxed);
    }

    /// Record flush throughput metrics: bytes written and duration.
    /// Call this after a flush completes with the size of data flushed.
    pub fn record_flush_throughput(&self, bytes: u64, duration_us: u64) {
        self.inner
            .flush_bytes_total
            .fetch_add(bytes, Ordering::Relaxed);
        self.inner
            .flush_duration_us_total
            .fetch_add(duration_us, Ordering::Relaxed);
    }

    /// Increment the flush queue depth (call when job is queued).
    pub fn increment_flush_queue(&self) {
        let new_depth = self.inner.flush_queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
        // Update high watermark if exceeded
        loop {
            let current = self.inner.flush_queue_high_watermark.load(Ordering::Relaxed);
            if new_depth <= current {
                break;
            }
            if self
                .inner
                .flush_queue_high_watermark
                .compare_exchange_weak(current, new_depth, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    /// Decrement the flush queue depth (call when job completes).
    pub fn decrement_flush_queue(&self) {
        self.inner.flush_queue_depth.fetch_sub(1, Ordering::Relaxed);
    }

    /// Get current flush queue depth for backpressure decisions.
    pub fn flush_queue_depth(&self) -> u64 {
        self.inner.flush_queue_depth.load(Ordering::Relaxed)
    }

    /// Get average flush throughput in bytes per second (0 if no flushes yet).
    pub fn flush_throughput_bytes_per_sec(&self) -> f64 {
        let bytes = self.inner.flush_bytes_total.load(Ordering::Relaxed);
        let duration_us = self.inner.flush_duration_us_total.load(Ordering::Relaxed);
        if duration_us == 0 {
            0.0
        } else {
            (bytes as f64 / duration_us as f64) * 1_000_000.0
        }
    }

    pub fn record_wal_write(&self) {
        self.inner.wal_writes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_wal_sync(&self) {
        self.inner.wal_syncs.fetch_add(1, Ordering::Relaxed);
    }

    // Compaction metrics
    pub fn record_compaction_started(&self) {
        self.inner
            .compactions_started
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_compaction_completed(&self, bytes_read: u64, bytes_written: u64) {
        self.inner
            .compactions_completed
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .compaction_bytes_read
            .fetch_add(bytes_read, Ordering::Relaxed);
        self.inner
            .compaction_bytes_written
            .fetch_add(bytes_written, Ordering::Relaxed);
    }

    pub fn record_compaction_failed(&self) {
        self.inner
            .compactions_failed
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn compaction_write_amplification(&self) -> f64 {
        let read = self.inner.compaction_bytes_read.load(Ordering::Relaxed);
        if read == 0 {
            0.0
        } else {
            let written = self.inner.compaction_bytes_written.load(Ordering::Relaxed);
            written as f64 / read as f64
        }
    }

    // Storage metrics
    pub fn set_sst_count(&self, count: usize) {
        self.inner.sst_count.store(count, Ordering::Relaxed);
    }

    pub fn set_total_sst_bytes(&self, bytes: u64) {
        self.inner.total_sst_bytes.store(bytes, Ordering::Relaxed);
    }

    pub fn get_sst_count(&self) -> usize {
        self.inner.sst_count.load(Ordering::Relaxed)
    }

    pub fn get_total_sst_bytes(&self) -> u64 {
        self.inner.total_sst_bytes.load(Ordering::Relaxed)
    }

    // Snapshot metrics
    pub fn snapshot_created(&self) {
        self.inner.active_snapshots.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot_released(&self) {
        self.inner.active_snapshots.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn active_snapshot_count(&self) -> usize {
        self.inner.active_snapshots.load(Ordering::Relaxed)
    }

    // Error metrics
    pub fn record_read_error(&self) {
        self.inner.read_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_write_error(&self) {
        self.inner.write_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_compaction_error(&self) {
        self.inner.compaction_errors.fetch_add(1, Ordering::Relaxed);
    }

    // Tombstone metrics
    pub fn record_point_tombstone_created(&self) {
        self.inner
            .point_tombstones_created
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_range_tombstone_created(&self) {
        self.inner
            .range_tombstones_created
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_tombstones_removed(&self, count: u64) {
        self.inner
            .tombstones_removed_compaction
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_tombstones_coalesced(&self, count: u64) {
        self.inner
            .tombstones_coalesced
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_tombstone_check(&self) {
        self.inner.tombstone_checks.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_point_tombstones_created(&self) -> u64 {
        self.inner.point_tombstones_created.load(Ordering::Relaxed)
    }

    pub fn get_range_tombstones_created(&self) -> u64 {
        self.inner.range_tombstones_created.load(Ordering::Relaxed)
    }

    pub fn get_tombstones_removed(&self) -> u64 {
        self.inner
            .tombstones_removed_compaction
            .load(Ordering::Relaxed)
    }

    pub fn get_tombstones_coalesced(&self) -> u64 {
        self.inner.tombstones_coalesced.load(Ordering::Relaxed)
    }

    pub fn get_tombstone_checks(&self) -> u64 {
        self.inner.tombstone_checks.load(Ordering::Relaxed)
    }

    // Write stall metrics
    pub fn record_write_stall(&self, stall_duration_ms: u64) {
        self.inner.write_stalls.fetch_add(1, Ordering::Relaxed);
        self.inner
            .write_stall_duration_ms
            .fetch_add(stall_duration_ms, Ordering::Relaxed);
    }

    pub fn record_background_write_stall(&self, stall_duration_ms: u64) {
        self.inner
            .background_write_stalls
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .background_write_stall_duration_ms
            .fetch_add(stall_duration_ms, Ordering::Relaxed);
    }

    pub fn record_capacity_write_stall(&self, stall_duration_ms: u64) {
        self.inner
            .capacity_write_stalls
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .capacity_write_stall_duration_ms
            .fetch_add(stall_duration_ms, Ordering::Relaxed);
    }

    pub fn get_write_stalls(&self) -> u64 {
        self.inner.write_stalls.load(Ordering::Relaxed)
    }

    pub fn get_write_stall_duration_ms(&self) -> u64 {
        self.inner.write_stall_duration_ms.load(Ordering::Relaxed)
    }

    pub fn get_background_write_stalls(&self) -> u64 {
        self.inner.background_write_stalls.load(Ordering::Relaxed)
    }

    pub fn get_background_write_stall_duration_ms(&self) -> u64 {
        self.inner
            .background_write_stall_duration_ms
            .load(Ordering::Relaxed)
    }

    pub fn get_capacity_write_stalls(&self) -> u64 {
        self.inner.capacity_write_stalls.load(Ordering::Relaxed)
    }

    pub fn get_capacity_write_stall_duration_ms(&self) -> u64 {
        self.inner
            .capacity_write_stall_duration_ms
            .load(Ordering::Relaxed)
    }

    // Autotuning metrics
    pub fn record_wal_interval_adjustment(&self, old_value: u64, new_value: u64) {
        self.inner
            .autotune_wal_interval_adjustments
            .fetch_add(1, Ordering::Relaxed);
        log::debug!(
            "Autotune: WAL interval adjusted {} ms → {} ms",
            old_value,
            new_value
        );
    }

    pub fn record_compaction_thread_adjustment(&self, old_value: usize, new_value: usize) {
        self.inner
            .autotune_compaction_thread_adjustments
            .fetch_add(1, Ordering::Relaxed);
        log::debug!(
            "Autotune: Compaction threads adjusted {} → {}",
            old_value,
            new_value
        );
    }

    pub fn record_bloom_bits_adjustment(&self, old_value: u32, new_value: u32) {
        self.inner
            .autotune_bloom_bits_adjustments
            .fetch_add(1, Ordering::Relaxed);
        log::debug!(
            "Autotune: Bloom bits adjusted {} → {}",
            old_value,
            new_value
        );
    }

    pub fn get_autotune_wal_interval_adjustments(&self) -> u64 {
        self.inner
            .autotune_wal_interval_adjustments
            .load(Ordering::Relaxed)
    }

    pub fn get_autotune_compaction_thread_adjustments(&self) -> u64 {
        self.inner
            .autotune_compaction_thread_adjustments
            .load(Ordering::Relaxed)
    }

    pub fn get_autotune_bloom_bits_adjustments(&self) -> u64 {
        self.inner
            .autotune_bloom_bits_adjustments
            .load(Ordering::Relaxed)
    }

    /// Get a snapshot of all current metrics
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            get_count: self.inner.get_count.load(Ordering::Relaxed),
            put_count: self.inner.put_count.load(Ordering::Relaxed),
            delete_count: self.inner.delete_count.load(Ordering::Relaxed),
            scan_count: self.inner.scan_count.load(Ordering::Relaxed),
            multi_get_count: self.inner.multi_get_count.load(Ordering::Relaxed),

            block_cache_hits: self.inner.block_cache_hits.load(Ordering::Relaxed),
            block_cache_misses: self.inner.block_cache_misses.load(Ordering::Relaxed),
            table_cache_hits: self.inner.table_cache_hits.load(Ordering::Relaxed),
            table_cache_misses: self.inner.table_cache_misses.load(Ordering::Relaxed),

            bloom_filter_checks: self.inner.bloom_filter_checks.load(Ordering::Relaxed),
            bloom_filter_positives: self.inner.bloom_filter_positives.load(Ordering::Relaxed),
            bloom_filter_true_positives: self
                .inner
                .bloom_filter_true_positives
                .load(Ordering::Relaxed),

            memtable_writes: self.inner.memtable_writes.load(Ordering::Relaxed),
            memtable_flushes: self.inner.memtable_flushes.load(Ordering::Relaxed),
            wal_writes: self.inner.wal_writes.load(Ordering::Relaxed),
            wal_syncs: self.inner.wal_syncs.load(Ordering::Relaxed),

            compactions_started: self.inner.compactions_started.load(Ordering::Relaxed),
            compactions_completed: self.inner.compactions_completed.load(Ordering::Relaxed),
            compactions_failed: self.inner.compactions_failed.load(Ordering::Relaxed),
            compaction_bytes_read: self.inner.compaction_bytes_read.load(Ordering::Relaxed),
            compaction_bytes_written: self.inner.compaction_bytes_written.load(Ordering::Relaxed),

            sst_count: self.inner.sst_count.load(Ordering::Relaxed),
            total_sst_bytes: self.inner.total_sst_bytes.load(Ordering::Relaxed),

            active_snapshots: self.inner.active_snapshots.load(Ordering::Relaxed),

            read_errors: self.inner.read_errors.load(Ordering::Relaxed),
            write_errors: self.inner.write_errors.load(Ordering::Relaxed),
            compaction_errors: self.inner.compaction_errors.load(Ordering::Relaxed),

            point_tombstones_created: self.inner.point_tombstones_created.load(Ordering::Relaxed),
            range_tombstones_created: self.inner.range_tombstones_created.load(Ordering::Relaxed),
            tombstones_removed_compaction: self
                .inner
                .tombstones_removed_compaction
                .load(Ordering::Relaxed),
            tombstones_coalesced: self.inner.tombstones_coalesced.load(Ordering::Relaxed),
            tombstone_checks: self.inner.tombstone_checks.load(Ordering::Relaxed),

            autotune_wal_interval_adjustments: self
                .inner
                .autotune_wal_interval_adjustments
                .load(Ordering::Relaxed),
            autotune_compaction_thread_adjustments: self
                .inner
                .autotune_compaction_thread_adjustments
                .load(Ordering::Relaxed),
            autotune_bloom_bits_adjustments: self
                .inner
                .autotune_bloom_bits_adjustments
                .load(Ordering::Relaxed),
        }
    }

    /// Reset all counters to zero
    pub fn reset(&self) {
        self.inner.get_count.store(0, Ordering::Relaxed);
        self.inner.put_count.store(0, Ordering::Relaxed);
        self.inner.delete_count.store(0, Ordering::Relaxed);
        self.inner.scan_count.store(0, Ordering::Relaxed);
        self.inner.multi_get_count.store(0, Ordering::Relaxed);

        self.inner.block_cache_hits.store(0, Ordering::Relaxed);
        self.inner.block_cache_misses.store(0, Ordering::Relaxed);
        self.inner.table_cache_hits.store(0, Ordering::Relaxed);
        self.inner.table_cache_misses.store(0, Ordering::Relaxed);

        self.inner.bloom_filter_checks.store(0, Ordering::Relaxed);
        self.inner
            .bloom_filter_positives
            .store(0, Ordering::Relaxed);
        self.inner
            .bloom_filter_true_positives
            .store(0, Ordering::Relaxed);

        self.inner.memtable_writes.store(0, Ordering::Relaxed);
        self.inner.memtable_flushes.store(0, Ordering::Relaxed);
        self.inner.wal_writes.store(0, Ordering::Relaxed);
        self.inner.wal_syncs.store(0, Ordering::Relaxed);

        self.inner.compactions_started.store(0, Ordering::Relaxed);
        self.inner.compactions_completed.store(0, Ordering::Relaxed);
        self.inner.compactions_failed.store(0, Ordering::Relaxed);
        self.inner.compaction_bytes_read.store(0, Ordering::Relaxed);
        self.inner
            .compaction_bytes_written
            .store(0, Ordering::Relaxed);

        // Note: Don't reset sst_count, total_sst_bytes, or active_snapshots
        // as these are current state, not counters

        self.inner.read_errors.store(0, Ordering::Relaxed);
        self.inner.write_errors.store(0, Ordering::Relaxed);
        self.inner.compaction_errors.store(0, Ordering::Relaxed);

        self.inner
            .point_tombstones_created
            .store(0, Ordering::Relaxed);
        self.inner
            .range_tombstones_created
            .store(0, Ordering::Relaxed);
        self.inner
            .tombstones_removed_compaction
            .store(0, Ordering::Relaxed);
        self.inner.tombstones_coalesced.store(0, Ordering::Relaxed);
        self.inner.tombstone_checks.store(0, Ordering::Relaxed);

        self.inner
            .autotune_wal_interval_adjustments
            .store(0, Ordering::Relaxed);
        self.inner
            .autotune_compaction_thread_adjustments
            .store(0, Ordering::Relaxed);
        self.inner
            .autotune_bloom_bits_adjustments
            .store(0, Ordering::Relaxed);
    }
}

/// Point-in-time snapshot of all metrics
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub get_count: u64,
    pub put_count: u64,
    pub delete_count: u64,
    pub scan_count: u64,
    pub multi_get_count: u64,

    pub block_cache_hits: u64,
    pub block_cache_misses: u64,
    pub table_cache_hits: u64,
    pub table_cache_misses: u64,

    pub bloom_filter_checks: u64,
    pub bloom_filter_positives: u64,
    pub bloom_filter_true_positives: u64,

    pub memtable_writes: u64,
    pub memtable_flushes: u64,
    pub wal_writes: u64,
    pub wal_syncs: u64,

    pub compactions_started: u64,
    pub compactions_completed: u64,
    pub compactions_failed: u64,
    pub compaction_bytes_read: u64,
    pub compaction_bytes_written: u64,

    pub sst_count: usize,
    pub total_sst_bytes: u64,

    pub active_snapshots: usize,

    pub read_errors: u64,
    pub write_errors: u64,
    pub compaction_errors: u64,

    pub point_tombstones_created: u64,
    pub range_tombstones_created: u64,
    pub tombstones_removed_compaction: u64,
    pub tombstones_coalesced: u64,
    pub tombstone_checks: u64,

    pub autotune_wal_interval_adjustments: u64,
    pub autotune_compaction_thread_adjustments: u64,
    pub autotune_bloom_bits_adjustments: u64,
}

impl MetricsSnapshot {
    /// Calculate total operation count
    pub fn total_operations(&self) -> u64 {
        self.get_count + self.put_count + self.delete_count + self.scan_count
    }

    /// Calculate block cache hit rate
    pub fn block_cache_hit_rate(&self) -> f64 {
        let total = self.block_cache_hits + self.block_cache_misses;
        if total == 0 {
            0.0
        } else {
            self.block_cache_hits as f64 / total as f64
        }
    }

    /// Calculate table cache hit rate
    pub fn table_cache_hit_rate(&self) -> f64 {
        let total = self.table_cache_hits + self.table_cache_misses;
        if total == 0 {
            0.0
        } else {
            self.table_cache_hits as f64 / total as f64
        }
    }

    /// Calculate bloom filter false positive rate
    pub fn bloom_false_positive_rate(&self) -> f64 {
        if self.bloom_filter_positives == 0 {
            0.0
        } else {
            let false_positives = self
                .bloom_filter_positives
                .saturating_sub(self.bloom_filter_true_positives);
            false_positives as f64 / self.bloom_filter_positives as f64
        }
    }

    /// Calculate compaction write amplification
    pub fn compaction_write_amplification(&self) -> f64 {
        if self.compaction_bytes_read == 0 {
            0.0
        } else {
            self.compaction_bytes_written as f64 / self.compaction_bytes_read as f64
        }
    }

    /// Calculate total tombstones created
    pub fn total_tombstones_created(&self) -> u64 {
        self.point_tombstones_created + self.range_tombstones_created
    }

    /// Calculate tombstone coalescing ratio (higher is better)
    pub fn tombstone_coalesce_ratio(&self) -> f64 {
        if self.range_tombstones_created == 0 {
            0.0
        } else {
            self.tombstones_coalesced as f64 / self.range_tombstones_created as f64
        }
    }

    /// Calculate tombstone removal efficiency during compaction
    pub fn tombstone_removal_ratio(&self) -> f64 {
        let total_created = self.total_tombstones_created();
        if total_created == 0 {
            0.0
        } else {
            self.tombstones_removed_compaction as f64 / total_created as f64
        }
    }

    /// Format metrics as a human-readable string
    pub fn format(&self) -> String {
        format!(
            "Midge Metrics:\n\
             Operations: {} gets, {} puts, {} deletes, {} scans\n\
             Block Cache: {:.2}% hit rate ({} hits, {} misses)\n\
             Table Cache: {:.2}% hit rate ({} hits, {} misses)\n\
             Bloom Filters: {:.2}% FP rate ({} checks)\n\
             Compactions: {} started, {} completed, {} failed\n\
             Write Amplification: {:.2}x\n\
             Storage: {} SSTs, {:.2} MB total\n\
             Active Snapshots: {}\n\
             Tombstones: {} point, {} range, {} coalesced ({:.1}% ratio), {} removed\n\
             Errors: {} read, {} write, {} compaction",
            self.get_count,
            self.put_count,
            self.delete_count,
            self.scan_count,
            self.block_cache_hit_rate() * 100.0,
            self.block_cache_hits,
            self.block_cache_misses,
            self.table_cache_hit_rate() * 100.0,
            self.table_cache_hits,
            self.table_cache_misses,
            self.bloom_false_positive_rate() * 100.0,
            self.bloom_filter_checks,
            self.compactions_started,
            self.compactions_completed,
            self.compactions_failed,
            self.compaction_write_amplification(),
            self.sst_count,
            self.total_sst_bytes as f64 / 1_048_576.0,
            self.active_snapshots,
            self.point_tombstones_created,
            self.range_tombstones_created,
            self.tombstones_coalesced,
            self.tombstone_coalesce_ratio() * 100.0,
            self.tombstones_removed_compaction,
            self.read_errors,
            self.write_errors,
            self.compaction_errors
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_track_operations_given_various_ops_when_recorded() {
        // Arrange
        let m = Metrics::new();

        // Act
        m.record_get();
        m.record_get();
        m.record_put();
        m.record_delete();
        m.record_scan();

        // Assert
        let snapshot = m.snapshot();
        assert_eq!(snapshot.get_count, 2);
        assert_eq!(snapshot.put_count, 1);
        assert_eq!(snapshot.delete_count, 1);
        assert_eq!(snapshot.scan_count, 1);
        assert_eq!(snapshot.total_operations(), 5);
    }

    #[test]
    fn should_calculate_cache_hit_rate_given_hits_and_misses_when_retrieved() {
        // Arrange
        let m = Metrics::new();

        // Act
        m.record_block_cache_hit();
        m.record_block_cache_hit();
        m.record_block_cache_hit();
        m.record_block_cache_miss();

        // Assert
        assert_eq!(m.block_cache_hit_rate(), 0.75); // 3/4
        let snapshot = m.snapshot();
        assert_eq!(snapshot.block_cache_hit_rate(), 0.75);
    }

    #[test]
    fn should_track_bloom_filter_efficiency_given_checks_when_recorded() {
        // Arrange
        let m = Metrics::new();

        // Act
        // 3 true positives
        m.record_bloom_check(true, true);
        m.record_bloom_check(true, true);
        m.record_bloom_check(true, true);
        // 1 false positive
        m.record_bloom_check(true, false);
        // 2 true negatives (correctly said "no")
        m.record_bloom_check(false, false);
        m.record_bloom_check(false, false);

        // Assert
        let snapshot = m.snapshot();
        assert_eq!(snapshot.bloom_filter_checks, 6);
        assert_eq!(snapshot.bloom_filter_positives, 4);
        assert_eq!(snapshot.bloom_filter_true_positives, 3);
        assert_eq!(snapshot.bloom_false_positive_rate(), 0.25); // 1/4
    }

    #[test]
    fn should_track_compaction_stats_given_compactions_when_recorded() {
        // Arrange
        let m = Metrics::new();

        // Act
        m.record_compaction_started();
        m.record_compaction_completed(1000, 1200);
        m.record_compaction_started();
        m.record_compaction_completed(500, 550);

        // Assert
        let snapshot = m.snapshot();
        assert_eq!(snapshot.compactions_started, 2);
        assert_eq!(snapshot.compactions_completed, 2);
        assert_eq!(snapshot.compaction_bytes_read, 1500);
        assert_eq!(snapshot.compaction_bytes_written, 1750);
        // Write amplification: 1750 / 1500 = 1.166...
        assert!((snapshot.compaction_write_amplification() - 1.1666).abs() < 0.01);
    }

    #[test]
    fn should_reset_all_metrics_given_reset_called_when_invoked() {
        // Arrange
        let m = Metrics::new();
        m.record_get();
        m.record_put();
        m.set_sst_count(10);
        let snapshot1 = m.snapshot();
        assert_eq!(snapshot1.get_count, 1);
        assert_eq!(snapshot1.sst_count, 10);

        // Act
        m.reset();

        // Assert
        let snapshot2 = m.snapshot();
        assert_eq!(snapshot2.get_count, 0);
        // SST count should NOT be reset
        assert_eq!(snapshot2.sst_count, 10);
    }
}
