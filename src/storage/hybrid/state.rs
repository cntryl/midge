//! Disk state tracking for the Storage Budget Actor
//!
//! Maintains the current disk usage and reservation accounting.

use std::sync::atomic::{AtomicU64, Ordering};

/// Disk usage accounting
#[derive(Debug, Clone)]
pub struct DiskState {
    /// Total bytes used by WAL
    pub wal_bytes: u64,
    /// Total bytes used by SSTs
    pub sst_bytes: u64,
    /// Bytes reserved for pending compaction outputs
    pub compaction_reserve: u64,
    /// Bytes reserved for new SST writes
    pub new_sst_reserve: u64,
    /// Bytes reserved for WAL headroom
    pub wal_reserve: u64,
}

impl DiskState {
    pub fn new() -> Self {
        Self {
            wal_bytes: 0,
            sst_bytes: 0,
            compaction_reserve: 0,
            new_sst_reserve: 0,
            wal_reserve: 0,
        }
    }

    /// Total reserved + used space
    pub fn total_committed(&self) -> u64 {
        self.wal_bytes
            + self.sst_bytes
            + self.compaction_reserve
            + self.new_sst_reserve
            + self.wal_reserve
    }

    /// Available free space given a disk limit
    pub fn free_bytes(&self, limit: u64) -> u64 {
        limit.saturating_sub(self.total_committed())
    }

    /// Percentage of disk used
    pub fn usage_percent(&self, limit: u64) -> u32 {
        if limit == 0 {
            return 100;
        }
        ((self.total_committed() as f64 / limit as f64) * 100.0) as u32
    }
}

impl Default for DiskState {
    fn default() -> Self {
        Self::new()
    }
}

/// Atomic disk state for lock-free reads
pub struct AtomicDiskState {
    total_committed: AtomicU64,
}

impl AtomicDiskState {
    pub fn new() -> Self {
        Self {
            total_committed: AtomicU64::new(0),
        }
    }

    pub fn from_disk_state(state: &DiskState) -> Self {
        Self {
            total_committed: AtomicU64::new(state.total_committed()),
        }
    }

    pub fn total_committed(&self) -> u64 {
        self.total_committed.load(Ordering::Relaxed)
    }

    pub fn free_bytes(&self, limit: u64) -> u64 {
        limit.saturating_sub(self.total_committed())
    }

    pub fn update(&self, new_total: u64) {
        self.total_committed.store(new_total, Ordering::Release);
    }

    pub fn usage_percent(&self, limit: u64) -> u32 {
        if limit == 0 {
            return 100;
        }
        ((self.total_committed() as f64 / limit as f64) * 100.0) as u32
    }
}

impl Default for AtomicDiskState {
    fn default() -> Self {
        Self::new()
    }
}
