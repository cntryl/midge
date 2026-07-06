//! Lightweight counters for SST point-read diagnostics.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

#[derive(Debug, Default)]
pub(crate) struct SstReadMetrics {
    reader_cache_hits: AtomicU64,
    reader_cache_misses: AtomicU64,
    block_cache_hits: AtomicU64,
    block_cache_misses: AtomicU64,
    candidate_sst_files_checked: AtomicU64,
    candidate_blocks_checked: AtomicU64,
    data_blocks_read: AtomicU64,
    bloom_rejects: AtomicU64,
    range_tombstone_scans: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SstReadMetricsSnapshot {
    pub reader_cache_hits: u64,
    pub reader_cache_misses: u64,
    pub block_cache_hits: u64,
    pub block_cache_misses: u64,
    pub candidate_sst_files_checked: u64,
    pub candidate_blocks_checked: u64,
    pub data_blocks_read: u64,
    pub bloom_rejects: u64,
    pub range_tombstone_scans: u64,
}

impl SstReadMetrics {
    pub(crate) fn record_reader_cache_hit(&self) {
        self.reader_cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_reader_cache_miss(&self) {
        self.reader_cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_block_cache_hit(&self) {
        self.block_cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_block_cache_miss(&self) {
        self.block_cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_candidate_sst_file_checked(&self) {
        self.candidate_sst_files_checked
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_candidate_blocks_checked(&self, count: usize) {
        self.candidate_blocks_checked
            .fetch_add(u64::try_from(count).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    pub(crate) fn record_data_block_read(&self) {
        self.data_blocks_read.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_bloom_reject(&self) {
        self.bloom_rejects.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_range_tombstone_scan(&self) {
        self.range_tombstone_scans.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> SstReadMetricsSnapshot {
        SstReadMetricsSnapshot {
            reader_cache_hits: self.reader_cache_hits.load(Ordering::Relaxed),
            reader_cache_misses: self.reader_cache_misses.load(Ordering::Relaxed),
            block_cache_hits: self.block_cache_hits.load(Ordering::Relaxed),
            block_cache_misses: self.block_cache_misses.load(Ordering::Relaxed),
            candidate_sst_files_checked: self.candidate_sst_files_checked.load(Ordering::Relaxed),
            candidate_blocks_checked: self.candidate_blocks_checked.load(Ordering::Relaxed),
            data_blocks_read: self.data_blocks_read.load(Ordering::Relaxed),
            bloom_rejects: self.bloom_rejects.load(Ordering::Relaxed),
            range_tombstone_scans: self.range_tombstone_scans.load(Ordering::Relaxed),
        }
    }
}

impl SstReadMetricsSnapshot {
    pub(crate) fn delta_since(self, start: Self) -> Self {
        Self {
            reader_cache_hits: self
                .reader_cache_hits
                .saturating_sub(start.reader_cache_hits),
            reader_cache_misses: self
                .reader_cache_misses
                .saturating_sub(start.reader_cache_misses),
            block_cache_hits: self.block_cache_hits.saturating_sub(start.block_cache_hits),
            block_cache_misses: self
                .block_cache_misses
                .saturating_sub(start.block_cache_misses),
            candidate_sst_files_checked: self
                .candidate_sst_files_checked
                .saturating_sub(start.candidate_sst_files_checked),
            candidate_blocks_checked: self
                .candidate_blocks_checked
                .saturating_sub(start.candidate_blocks_checked),
            data_blocks_read: self.data_blocks_read.saturating_sub(start.data_blocks_read),
            bloom_rejects: self.bloom_rejects.saturating_sub(start.bloom_rejects),
            range_tombstone_scans: self
                .range_tombstone_scans
                .saturating_sub(start.range_tombstone_scans),
        }
    }
}

static GLOBAL_SST_READ_METRICS: OnceLock<SstReadMetrics> = OnceLock::new();

pub(crate) fn global_sst_read_metrics() -> &'static SstReadMetrics {
    GLOBAL_SST_READ_METRICS.get_or_init(SstReadMetrics::default)
}

pub(crate) fn snapshot_global_sst_read_metrics() -> SstReadMetricsSnapshot {
    global_sst_read_metrics().snapshot()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_report_saturating_read_path_metric_deltas() {
        // Arrange
        let start = SstReadMetricsSnapshot {
            reader_cache_hits: 4,
            reader_cache_misses: 3,
            block_cache_hits: 2,
            block_cache_misses: 1,
            candidate_sst_files_checked: 10,
            candidate_blocks_checked: 9,
            data_blocks_read: 8,
            bloom_rejects: 7,
            range_tombstone_scans: 6,
        };
        let end = SstReadMetricsSnapshot {
            reader_cache_hits: 7,
            reader_cache_misses: 3,
            block_cache_hits: 9,
            block_cache_misses: 0,
            candidate_sst_files_checked: 18,
            candidate_blocks_checked: 20,
            data_blocks_read: 8,
            bloom_rejects: 10,
            range_tombstone_scans: 11,
        };

        // Act
        let delta = end.delta_since(start);

        // Assert
        assert_eq!(delta.reader_cache_hits, 3);
        assert_eq!(delta.reader_cache_misses, 0);
        assert_eq!(delta.block_cache_hits, 7);
        assert_eq!(delta.block_cache_misses, 0);
        assert_eq!(delta.candidate_sst_files_checked, 8);
        assert_eq!(delta.candidate_blocks_checked, 11);
        assert_eq!(delta.data_blocks_read, 0);
        assert_eq!(delta.bloom_rejects, 3);
        assert_eq!(delta.range_tombstone_scans, 5);
    }
}
