//! Snapshot API - Point-in-time consistent views
//!
//! A snapshot represents a consistent view of the database at a specific
//! sequence number, enabling point-in-time reads and preventing values from
//! being evicted while the snapshot is active.

use super::super::ColumnFamilyId;

/// A point-in-time snapshot of the database
///
/// Snapshots provide consistent, repeatable reads across the entire key space.
/// Multiple snapshots can coexist, and they persist even as new writes occur.
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Sequence number at which this snapshot was created
    sequence: u64,
    /// Which column family this snapshot is for (None = all CFs)
    cf_id: Option<ColumnFamilyId>,
    /// Unique snapshot ID for tracking
    snapshot_id: u64,
}

impl Snapshot {
    /// Create a new snapshot at a specific sequence number
    #[allow(dead_code)] // Used by engine when creating snapshots
    pub(crate) fn new(sequence: u64, cf_id: Option<ColumnFamilyId>, snapshot_id: u64) -> Self {
        Self {
            sequence,
            cf_id,
            snapshot_id,
        }
    }

    /// Get the sequence number this snapshot captures
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Get the column family this snapshot is for (None = default CF)
    pub fn column_family(&self) -> Option<ColumnFamilyId> {
        self.cf_id
    }

    /// Get the unique snapshot ID
    pub fn snapshot_id(&self) -> u64 {
        self.snapshot_id
    }
}

impl PartialEq for Snapshot {
    fn eq(&self, other: &Self) -> bool {
        self.snapshot_id == other.snapshot_id
    }
}

impl Eq for Snapshot {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_snapshot_with_sequence_when_initialized() {
        // Arrange & Act
        let snapshot = Snapshot::new(42, None, 1);

        // Assert
        assert_eq!(snapshot.sequence(), 42);
        assert_eq!(snapshot.column_family(), None);
        assert_eq!(snapshot.snapshot_id(), 1);
    }

    #[test]
    fn should_track_column_family_when_cf_specific_snapshot_created() {
        // Arrange
        let cf_id = ColumnFamilyId(2);

        // Act
        let snapshot = Snapshot::new(100, Some(cf_id), 5);

        // Assert
        assert_eq!(snapshot.sequence(), 100);
        assert_eq!(snapshot.column_family(), Some(cf_id));
        assert_eq!(snapshot.snapshot_id(), 5);
    }

    #[test]
    fn should_compare_snapshots_by_id_when_using_equality() {
        // Arrange
        let snap1 = Snapshot::new(10, None, 1);
        let snap2 = Snapshot::new(10, None, 1);
        let snap3 = Snapshot::new(20, None, 2);

        // Act & Assert
        assert_eq!(snap1, snap2);
        assert_ne!(snap1, snap3);
    }

    #[test]
    fn should_support_default_cf_when_cf_id_is_none() {
        // Arrange & Act
        let snapshot = Snapshot::new(50, None, 10);

        // Assert
        assert!(snapshot.column_family().is_none());
    }

    #[test]
    fn should_handle_multiple_snapshots_with_different_ids() {
        // Arrange
        let snap1 = Snapshot::new(1, None, 100);
        let snap2 = Snapshot::new(2, None, 101);
        let snap3 = Snapshot::new(3, None, 102);

        // Act
        let snapshots = vec![snap1, snap2, snap3];

        // Assert
        assert_eq!(snapshots.len(), 3);
        assert_eq!(snapshots[0].snapshot_id(), 100);
        assert_eq!(snapshots[1].snapshot_id(), 101);
        assert_eq!(snapshots[2].snapshot_id(), 102);
    }
}
