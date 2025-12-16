//! Snapshot API - Point-in-time consistent views
//!
//! A snapshot represents a consistent view of the database at a specific
//! sequence number, enabling point-in-time reads and preventing values from
//! being evicted while the snapshot is active.

use super::super::{ColumnFamilyHandle, ColumnFamilyId};
use crate::common::{MidgeError, MidgeResult};
use crate::runtime::{next_request_id, RuntimeHandle, RuntimeMsg, RuntimeResponse};
use bytes::Bytes;
use std::fmt;

/// A point-in-time snapshot of the database
///
/// Snapshots provide consistent, repeatable reads across the entire key space.
/// Multiple snapshots can coexist, and they persist even as new writes occur.
#[derive(Clone)]
pub struct Snapshot {
    /// Sequence number at which this snapshot was created
    sequence: u64,
    /// Which column family this snapshot is for (None = all CFs)
    cf_id: Option<ColumnFamilyId>,
    /// Unique snapshot ID for tracking
    snapshot_id: u64,
    /// Runtime handle used for point-in-time reads
    runtime: RuntimeHandle,
}

impl Snapshot {
    /// Create a new snapshot at a specific sequence number
    #[allow(dead_code)]
    pub(crate) fn new(
        sequence: u64,
        cf_id: Option<ColumnFamilyId>,
        snapshot_id: u64,
        runtime: RuntimeHandle,
    ) -> Self {
        Self {
            sequence,
            cf_id,
            snapshot_id,
            runtime,
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

    /// Get a value from the snapshot at this point in time
    pub fn get(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> MidgeResult<Option<Bytes>> {
        let response = self.runtime.send_and_wait(RuntimeMsg::Read {
            request_id: next_request_id(),
            cf_id: cf.id().as_u32(),
            key: key.to_vec(),
            sequence: self.sequence,
            requested_durability: crate::engine::api::Durability::Steady,
        })?;

        match response {
            RuntimeResponse::ReadValue { value, .. } => Ok(value.map(Bytes::from)),
            RuntimeResponse::Error { message, .. } => Err(MidgeError::Internal(message)),
            _ => Err(MidgeError::Internal(
                "Unexpected response to snapshot read".to_string(),
            )),
        }
    }

    /// Scan a range in the snapshot
    pub fn range(
        &self,
        cf: &ColumnFamilyHandle,
        start: &[u8],
        end: &[u8],
    ) -> MidgeResult<Vec<(Bytes, Bytes)>> {
        self.range_with_sequence(cf, start, end, self.sequence)
    }

    fn range_with_sequence(
        &self,
        cf: &ColumnFamilyHandle,
        start: &[u8],
        end: &[u8],
        sequence: u64,
    ) -> MidgeResult<Vec<(Bytes, Bytes)>> {
        let response = self.runtime.send_and_wait(RuntimeMsg::RangeScan {
            request_id: next_request_id(),
            cf_id: cf.id().as_u32(),
            start: start.to_vec(),
            end: end.to_vec(),
            sequence,
            requested_durability: crate::engine::api::Durability::Steady,
        })?;

        match response {
            RuntimeResponse::RangeScanResults { results, .. } => Ok(results
                .into_iter()
                .map(|(k, v)| (Bytes::from(k), Bytes::from(v)))
                .collect()),
            RuntimeResponse::Error { message, .. } => Err(MidgeError::Internal(message)),
            _ => Err(MidgeError::Internal(
                "Unexpected response to snapshot range".to_string(),
            )),
        }
    }

    /// Scan with a query in the snapshot
    pub fn scan(
        &self,
        cf: &ColumnFamilyHandle,
        query: &super::Query,
    ) -> MidgeResult<Vec<(Bytes, Bytes)>> {
        let start_owned;
        let start = if let Some(s) = query.effective_start() {
            s
        } else {
            start_owned = vec![];
            &start_owned[..]
        };

        let end_vec = query.effective_end();
        let end_sentinel = vec![0xFFu8; 256];
        let end = if let Some(ref e) = end_vec {
            &e[..]
        } else if query.prefix.is_none() && query.end.is_none() {
            &end_sentinel[..]
        } else {
            &[][..]
        };

        let mut results = self.range_with_sequence(cf, start, end, self.sequence)?;

        if let Some(limit) = query.limit {
            results.truncate(limit);
        }

        if query.reverse {
            results.reverse();
        }

        Ok(results)
    }
}

impl fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Snapshot")
            .field("sequence", &self.sequence)
            .field("cf_id", &self.cf_id)
            .field("snapshot_id", &self.snapshot_id)
            .finish()
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
    use crate::runtime::Runtime;

    fn test_snapshot(sequence: u64, cf_id: Option<ColumnFamilyId>, snapshot_id: u64) -> Snapshot {
        let (_runtime, handle) = Runtime::new().expect("failed to create runtime handle");
        Snapshot::new(sequence, cf_id, snapshot_id, handle)
    }

    #[test]
    fn should_create_snapshot_with_sequence_when_initialized() {
        let snapshot = test_snapshot(42, None, 1);

        assert_eq!(snapshot.sequence(), 42);
        assert_eq!(snapshot.column_family(), None);
        assert_eq!(snapshot.snapshot_id(), 1);
    }

    #[test]
    fn should_track_column_family_when_cf_specific_snapshot_created() {
        let cf_id = ColumnFamilyId(2);
        let snapshot = test_snapshot(100, Some(cf_id), 5);

        assert_eq!(snapshot.sequence(), 100);
        assert_eq!(snapshot.column_family(), Some(cf_id));
        assert_eq!(snapshot.snapshot_id(), 5);
    }

    #[test]
    fn should_compare_snapshots_by_id_when_using_equality() {
        let snap1 = test_snapshot(10, None, 1);
        let snap2 = test_snapshot(10, None, 1);
        let snap3 = test_snapshot(20, None, 2);

        assert_eq!(snap1, snap2);
        assert_ne!(snap1, snap3);
    }

    #[test]
    fn should_support_default_cf_when_cf_id_is_none() {
        let snapshot = test_snapshot(50, None, 10);
        assert!(snapshot.column_family().is_none());
    }

    #[test]
    fn should_handle_multiple_snapshots_with_different_ids() {
        let snap1 = test_snapshot(1, None, 100);
        let snap2 = test_snapshot(2, None, 101);
        let snap3 = test_snapshot(3, None, 102);

        let snapshots = [snap1, snap2, snap3];

        assert_eq!(snapshots.len(), 3);
        assert_eq!(snapshots[0].snapshot_id(), 100);
        assert_eq!(snapshots[1].snapshot_id(), 101);
        assert_eq!(snapshots[2].snapshot_id(), 102);
    }
}
