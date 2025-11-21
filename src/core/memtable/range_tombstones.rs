//! Range tombstone storage for delete_range operations.

use parking_lot::RwLock;
use std::sync::Arc;

/// A single range deletion: [start, end) at sequence number
#[derive(Debug, Clone)]
pub(super) struct RangeDel {
    pub(super) start: Vec<u8>,
    pub(super) end: Vec<u8>,
    pub(super) seq: u64,
}

/// Encapsulates range tombstone storage with interior mutability.
///
/// Range tombstones represent delete_range operations that delete all keys
/// in the range [start, end). They are stored separately from point deletions
/// in the skiplist for efficiency.
#[derive(Clone)]
pub(super) struct RangeTombstones {
    inner: Arc<RwLock<Vec<RangeDel>>>,
}

impl RangeTombstones {
    /// Create a new empty range tombstone collection.
    pub(super) fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Record a range deletion [start, end) with sequence number.
    pub(super) fn push(&self, start: Vec<u8>, end: Vec<u8>, seq: u64) {
        let mut tombstones = self.inner.write();
        tombstones.push(RangeDel { start, end, seq });
    }

    /// Drain and return all range tombstones, resetting the list.
    ///
    /// This is called when freezing a memtable for flushing to SST.
    pub(super) fn drain(&self) -> Vec<(Vec<u8>, Vec<u8>, u64)> {
        let mut tombstones = self.inner.write();
        tombstones
            .drain(..)
            .map(|r| (r.start, r.end, r.seq))
            .collect()
    }

    /// Returns true if any active (non-drained) range tombstone covers `key`.
    /// Coverage uses inclusive start, exclusive end semantics: [start, end).
    pub(super) fn covers(&self, key: &[u8]) -> bool {
        let tombstones = self.inner.read();
        for r in tombstones.iter() {
            if key >= r.start.as_slice() && key < r.end.as_slice() {
                return true;
            }
        }
        false
    }

    /// Returns true if a range tombstone with sequence <= `seq` covers `key`.
    /// Used for snapshot reads. Assumes MVCC where tombstone seq hides keys <= snapshot seq.
    pub(super) fn covers_at(&self, key: &[u8], seq: u64) -> bool {
        let tombstones = self.inner.read();
        for r in tombstones.iter() {
            if r.seq <= seq && key >= r.start.as_slice() && key < r.end.as_slice() {
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_drain_stored_range_tombstones() {
        // Arrange
        let rt = RangeTombstones::new();

        // Act
        rt.push(b"a".to_vec(), b"m".to_vec(), 10);
        rt.push(b"m".to_vec(), b"z".to_vec(), 20);
        let drained = rt.drain();

        // Assert
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0], (b"a".to_vec(), b"m".to_vec(), 10));
        assert_eq!(drained[1], (b"m".to_vec(), b"z".to_vec(), 20));
    }

    #[test]
    fn should_reset_after_drain() {
        // Arrange
        let rt = RangeTombstones::new();
        rt.push(b"a".to_vec(), b"z".to_vec(), 10);

        // Act
        let first_drain = rt.drain();
        let second_drain = rt.drain();

        // Assert
        assert_eq!(first_drain.len(), 1);
        assert_eq!(second_drain.len(), 0);
    }
}
