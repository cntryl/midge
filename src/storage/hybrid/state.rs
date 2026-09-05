//! Disk state tracking for the Storage Budget Actor
//!
//! Maintains the current disk usage and reservation accounting.

#[cfg(test)]
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
    /// Caller-owned transaction spill files and their staging allowance.
    pub scratch_bytes: u64,
    /// Residue measured after startup cleanup, distinct from live spill owners.
    pub startup_residue_bytes: u64,
}

impl DiskState {
    pub fn new() -> Self {
        Self {
            wal_bytes: 0,
            sst_bytes: 0,
            compaction_reserve: 0,
            new_sst_reserve: 0,
            wal_reserve: 0,
            scratch_bytes: 0,
            startup_residue_bytes: 0,
        }
    }

    /// Total reserved + used space
    pub fn total_committed(&self) -> u64 {
        self.wal_bytes
            .saturating_add(self.sst_bytes)
            .saturating_add(self.compaction_reserve)
            .saturating_add(self.new_sst_reserve)
            .saturating_add(self.wal_reserve)
            .saturating_add(self.scratch_bytes)
            .saturating_add(self.startup_residue_bytes)
    }

    /// Available free space given a disk limit
    pub fn free_bytes(&self, limit: u64) -> u64 {
        limit.saturating_sub(self.total_committed())
    }

    /// Percentage of disk used
    pub fn usage_percent(&self, limit: u64) -> u32 {
        usage_percent(self.total_committed(), limit)
    }
}

impl Default for DiskState {
    fn default() -> Self {
        Self::new()
    }
}

/// Atomic disk state for lock-free reads
#[cfg(test)]
pub struct AtomicDiskState {
    total_committed: AtomicU64,
}
#[cfg(test)]
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
        usage_percent(self.total_committed(), limit)
    }
}

fn usage_percent(total_committed: u64, limit: u64) -> u32 {
    if limit == 0 {
        return 100;
    }

    let percent = total_committed.saturating_mul(100) / limit;
    u32::try_from(percent).unwrap_or(u32::MAX)
}

#[cfg(test)]
impl Default for AtomicDiskState {
    fn default() -> Self {
        Self::new()
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_calculate_total_committed_correctly() {
        // Arrange
        let mut state = DiskState::new();
        state.wal_bytes = 100;
        state.sst_bytes = 200;
        state.compaction_reserve = 50;
        state.new_sst_reserve = 30;
        state.wal_reserve = 20;

        // Act
        let total = state.total_committed();

        // Assert
        assert_eq!(total, 400);
    }

    #[test]
    fn should_calculate_free_bytes_correctly() {
        // Arrange
        let mut state = DiskState::new();
        state.sst_bytes = 300;
        let limit = 1000;

        // Act
        let free = state.free_bytes(limit);

        // Assert
        assert_eq!(free, 700);
    }

    #[test]
    fn should_return_zero_free_when_over_limit() {
        // Arrange
        let mut state = DiskState::new();
        state.sst_bytes = 1000;
        let limit = 500;

        // Act
        let free = state.free_bytes(limit);

        // Assert
        assert_eq!(free, 0); // Saturating subtraction
    }

    #[test]
    fn should_calculate_usage_percent_correctly() {
        // Arrange
        let mut state = DiskState::new();
        state.sst_bytes = 500;
        let limit = 1000;

        // Act
        let percent = state.usage_percent(limit);

        // Assert
        assert_eq!(percent, 50);
    }

    #[test]
    fn should_not_cap_percent_when_over_limit() {
        // usage_percent does not clamp to 100; callers (watermark checks)
        // rely on the raw, uncapped ratio to distinguish degrees of overage.
        // Arrange
        let mut state = DiskState::new();
        state.sst_bytes = 1000;
        let limit = 500;

        // Act
        let percent = state.usage_percent(limit);

        // Assert
        assert_eq!(percent, 200); // 1000 / 500 * 100 = 200, not capped at 100
    }

    #[test]
    fn should_return_100_percent_when_limit_is_zero() {
        // Arrange
        let state = DiskState::new();
        let limit = 0;

        // Act
        let percent = state.usage_percent(limit);

        // Assert
        assert_eq!(percent, 100);
    }

    #[test]
    fn should_implement_default_for_disk_state() {
        // Arrange

        // Act
        let state = DiskState::default();

        // Assert
        assert_eq!(state.wal_bytes, 0);
        assert_eq!(state.sst_bytes, 0);
        assert_eq!(state.compaction_reserve, 0);
        assert_eq!(state.new_sst_reserve, 0);
        assert_eq!(state.wal_reserve, 0);
        assert_eq!(state.total_committed(), 0);
    }

    #[test]
    fn should_create_atomic_disk_state_with_zero() {
        // Arrange

        // Act
        let state = AtomicDiskState::new();

        // Assert
        assert_eq!(state.total_committed(), 0);
        assert_eq!(state.free_bytes(1000), 1000);
        assert_eq!(state.usage_percent(1000), 0);
    }

    #[test]
    fn should_create_atomic_disk_state_from_disk_state() {
        // Arrange
        let mut disk_state = DiskState::new();
        disk_state.sst_bytes = 250;
        disk_state.wal_bytes = 250;

        // Act
        let atomic_state = AtomicDiskState::from_disk_state(&disk_state);

        // Assert
        assert_eq!(atomic_state.total_committed(), 500);
    }

    #[test]
    fn should_update_atomic_disk_state() {
        // Arrange
        let state = AtomicDiskState::new();

        // Act
        state.update(750);

        // Assert
        assert_eq!(state.total_committed(), 750);
        assert_eq!(state.free_bytes(1000), 250);
    }

    #[test]
    fn should_calculate_atomic_usage_percent() {
        // Arrange
        let state = AtomicDiskState::new();
        state.update(400);

        // Act
        let percent = state.usage_percent(1000);

        // Assert
        assert_eq!(percent, 40);
    }

    #[test]
    fn should_handle_various_usage_levels() {
        // Arrange
        let mut state = DiskState::new();
        let limit = 10000;

        let cases = [(0, 0), (1000, 10), (5000, 50), (9000, 90), (10000, 100)];

        // Act
        let results: Vec<u32> = cases
            .iter()
            .map(|(sst_bytes, _expected)| {
                state.sst_bytes = *sst_bytes;
                state.usage_percent(limit)
            })
            .collect();

        // Assert
        for ((_, expected), actual) in cases.iter().zip(results.iter().copied()) {
            assert_eq!(actual, *expected);
        }
    }
}
