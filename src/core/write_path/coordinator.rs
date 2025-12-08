//! Unified Write Path Coordinator
//!
//! Centralizes all write operations (put, delete, merge, write_batch) behind a single
//! coordinator that handles:
//! - Sequence number allocation (monotonic, atomic)
//! - WAL append (durable logging)
//! - Memtable insertion (in-memory buffering)
//! - Flush signaling (background work via runtime)
//!
//! Benefits:
//! - Single point of control for write semantics
//! - Eliminates duplicated WAL/memtable/cache logic
//! - Enables future group commit and batching optimizations
//! - Clear error handling for all write paths

use crate::api::column_family::ColumnFamilyHandle;
use crate::error::MidgeResult;

/// Unified write path coordinator.
///
/// This struct is the central coordinator for all write operations in the engine.
/// It will own:
/// - Sequence number allocation logic
/// - WAL appending
/// - Memtable updates
/// - Background work signaling (flush/compaction)
///
/// Future enhancements:
/// - Group commit batching for write amortization
/// - Adaptive flush/compaction signaling
/// - Write rate limiting
pub struct WritePathCoordinator {}

impl WritePathCoordinator {
    /// Create a new write path coordinator.
    pub fn new() -> Self {
        Self {}
    }

    /// Apply a write to the engine.
    ///
    /// This is the unified entry point for all write operations (put, delete, merge, etc.).
    ///
    /// **Order of operations (critical for crash safety):**
    /// 1. Allocate monotonically increasing sequence number
    /// 2. Append all operations to WAL (before state change)
    /// 3. Apply mutations to memtable(s)
    /// 4. Signal background work if thresholds crossed (flush/compaction)
    /// 5. Return sequence number for MVCC visibility
    ///
    /// # Errors
    ///
    /// Returns error if WAL fails, memtable is full, or background coordination fails.
    pub fn apply_write(
        &self,
        _engine: &crate::core::engine::MidgeEngine,
        _cf_handle: &ColumnFamilyHandle,
        _ops: &[WriteOp],
    ) -> MidgeResult<u64> {
        // TODO: Implement unified write path
        // For now, this is a placeholder that demonstrates the interface
        Ok(0)
    }
}

/// Single write operation within a batch.
///
/// This represents one mutation: put, delete, merge, or delete_range.
#[derive(Debug, Clone)]
pub struct WriteOp {
    /// Operation kind (put, delete, merge, delete_range)
    pub kind: OpKind,
    /// User key for this operation
    pub key: Vec<u8>,
    /// Value (for put/merge operations)
    pub value: Option<Vec<u8>>,
    /// Range end key (for delete_range operations)
    pub range_end: Option<Vec<u8>>,
}

/// Kind of write operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpKind {
    Put,
    Delete,
    Merge,
    DeleteRange,
}

impl Default for WritePathCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_coordinator() {
        let coord = WritePathCoordinator::new();
        let _ = coord;
    }

    #[test]
    fn should_support_op_kind() {
        let put = OpKind::Put;
        let delete = OpKind::Delete;
        assert_ne!(put, delete);
    }
}
