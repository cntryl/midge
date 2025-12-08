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
use crate::core::engine::MidgeEngine;
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
#[derive(Debug)]
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
    ///
    /// # Note
    ///
    /// This is a framework method. Full implementation will be completed in Task 4.3
    /// when we have proper access to engine internals. For now, callers should use
    /// the individual write APIs (put, delete, merge, etc.) on MidgeEngine.
    pub fn apply_write(
        &self,
        _engine: &MidgeEngine,
        _cf_handle: &ColumnFamilyHandle,
        _ops: &[WriteOp],
    ) -> MidgeResult<u64> {
        // TODO: Full implementation in Task 4.3
        // For now, this demonstrates the interface
        // The actual coordination logic will:
        // 1. Allocate sequences for all ops
        // 2. Build WAL records
        // 3. Append to WAL
        // 4. Apply to memtable(s)
        // 5. Handle stalls and flush signaling
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
    /// TTL in seconds (0 = no TTL)
    pub ttl_seconds: u64,
}

impl WriteOp {
    /// Create a put operation
    pub fn put(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            kind: OpKind::Put,
            key,
            value: Some(value),
            range_end: None,
            ttl_seconds: 0,
        }
    }

    /// Create a put operation with TTL
    pub fn put_with_ttl(key: Vec<u8>, value: Vec<u8>, ttl_seconds: u64) -> Self {
        Self {
            kind: OpKind::Put,
            key,
            value: Some(value),
            range_end: None,
            ttl_seconds,
        }
    }

    /// Create a delete operation
    pub fn delete(key: Vec<u8>) -> Self {
        Self {
            kind: OpKind::Delete,
            key,
            value: None,
            range_end: None,
            ttl_seconds: 0,
        }
    }

    /// Create a delete_range operation
    pub fn delete_range(start: Vec<u8>, end: Vec<u8>) -> Self {
        Self {
            kind: OpKind::DeleteRange,
            key: start,
            value: None,
            range_end: Some(end),
            ttl_seconds: 0,
        }
    }

    /// Create a merge operation
    pub fn merge(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            kind: OpKind::Merge,
            key,
            value: Some(value),
            range_end: None,
            ttl_seconds: 0,
        }
    }

    /// Create a merge operation with TTL
    pub fn merge_with_ttl(key: Vec<u8>, value: Vec<u8>, ttl_seconds: u64) -> Self {
        Self {
            kind: OpKind::Merge,
            key,
            value: Some(value),
            range_end: None,
            ttl_seconds,
        }
    }
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
        // Verify it can be created and is properly initialized
        assert_eq!(std::mem::size_of_val(&coord), 0);
    }

    #[test]
    fn should_support_op_kind() {
        let put = OpKind::Put;
        let delete = OpKind::Delete;
        assert_ne!(put, delete);
    }

    #[test]
    fn should_build_put_operation() {
        let op = WriteOp::put(b"key".to_vec(), b"value".to_vec());
        assert_eq!(op.kind, OpKind::Put);
        assert_eq!(op.key, b"key");
        assert_eq!(op.value, Some(b"value".to_vec()));
        assert_eq!(op.range_end, None);
        assert_eq!(op.ttl_seconds, 0);
    }

    #[test]
    fn should_build_put_with_ttl_operation() {
        let op = WriteOp::put_with_ttl(b"key".to_vec(), b"value".to_vec(), 3600);
        assert_eq!(op.kind, OpKind::Put);
        assert_eq!(op.ttl_seconds, 3600);
    }

    #[test]
    fn should_build_delete_operation() {
        let op = WriteOp::delete(b"key".to_vec());
        assert_eq!(op.kind, OpKind::Delete);
        assert_eq!(op.key, b"key");
        assert_eq!(op.value, None);
        assert_eq!(op.range_end, None);
    }

    #[test]
    fn should_build_delete_range_operation() {
        let op = WriteOp::delete_range(b"start".to_vec(), b"end".to_vec());
        assert_eq!(op.kind, OpKind::DeleteRange);
        assert_eq!(op.key, b"start");
        assert_eq!(op.range_end, Some(b"end".to_vec()));
        assert_eq!(op.value, None);
    }

    #[test]
    fn should_build_merge_operation() {
        let op = WriteOp::merge(b"key".to_vec(), b"value".to_vec());
        assert_eq!(op.kind, OpKind::Merge);
        assert_eq!(op.value, Some(b"value".to_vec()));
    }

    #[test]
    fn should_build_merge_with_ttl_operation() {
        let op = WriteOp::merge_with_ttl(b"key".to_vec(), b"value".to_vec(), 7200);
        assert_eq!(op.kind, OpKind::Merge);
        assert_eq!(op.ttl_seconds, 7200);
    }
}
