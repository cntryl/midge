//! Lightweight counters for SST point-read diagnostics.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub(crate) struct SstReadMetrics {
    reader_cache_hits: AtomicU64,
    reader_cache_misses: AtomicU64,
    block_cache_hits: AtomicU64,
    block_cache_misses: AtomicU64,
    candidate_sst_files_checked: AtomicU64,
    candidate_blocks_checked: AtomicU64,
    data_blocks_read: AtomicU64,
    bloom_checks: AtomicU64,
    bloom_rejects: AtomicU64,
    range_tombstone_scans: AtomicU64,
}

impl SstReadMetrics {
    pub(crate) fn reader_cache_hits(&self) -> u64 {
        self.reader_cache_hits.load(Ordering::Relaxed)
    }

    pub(crate) fn reader_cache_misses(&self) -> u64 {
        self.reader_cache_misses.load(Ordering::Relaxed)
    }

    pub(crate) fn block_cache_hits(&self) -> u64 {
        self.block_cache_hits.load(Ordering::Relaxed)
    }

    pub(crate) fn block_cache_misses(&self) -> u64 {
        self.block_cache_misses.load(Ordering::Relaxed)
    }

    pub(crate) fn candidate_sst_files_checked(&self) -> u64 {
        self.candidate_sst_files_checked.load(Ordering::Relaxed)
    }

    pub(crate) fn candidate_blocks_checked(&self) -> u64 {
        self.candidate_blocks_checked.load(Ordering::Relaxed)
    }

    pub(crate) fn data_blocks_read(&self) -> u64 {
        self.data_blocks_read.load(Ordering::Relaxed)
    }

    pub(crate) fn bloom_rejects(&self) -> u64 {
        self.bloom_rejects.load(Ordering::Relaxed)
    }

    pub(crate) fn bloom_checks(&self) -> u64 {
        self.bloom_checks.load(Ordering::Relaxed)
    }

    pub(crate) fn range_tombstone_scans(&self) -> u64 {
        self.range_tombstone_scans.load(Ordering::Relaxed)
    }

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

    pub(crate) fn record_bloom_check(&self) {
        self.bloom_checks.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_range_tombstone_scan(&self) {
        self.range_tombstone_scans.fetch_add(1, Ordering::Relaxed);
    }
}
