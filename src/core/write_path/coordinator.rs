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
use crate::error::{MidgeError, MidgeResult};
use std::sync::atomic::Ordering;
use bytes::Bytes;
use crate::common::timestamp;

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
    /// 1. Allocate monotonically increasing sequence numbers
    /// 2. Build WAL records (before any state change)
    /// 3. Append to WAL (durable, atomic)
    /// 4. Apply mutations to memtable(s)
    /// 5. Handle stalls and signal flush/compaction
    /// 6. Return first sequence number for MVCC visibility
    ///
    /// # Errors
    ///
    /// Returns error if WAL fails, memtable is full, or background coordination fails.
    pub fn apply_write(
        &self,
        engine: &MidgeEngine,
        cf_handle: &ColumnFamilyHandle,
        ops: &[WriteOp],
    ) -> MidgeResult<u64> {
        if ops.is_empty() {
            return Ok(0);
        }

        // Validate preconditions
        if engine.read_only {
            return Err(MidgeError::invalid_config("Cannot write in read-only mode"));
        }

        let cf_id = cf_handle.id();

        // Get column family
        let cf_arc = engine.cf_set.cfs.get(&cf_id.as_u32()).ok_or_else(|| {
            MidgeError::invalid_config(format!(
                "Column family '{}' does not exist",
                cf_handle.name()
            ))
        })?;

        // Allocate sequences for all operations
        let mut sequences = Vec::with_capacity(ops.len());
        let now_millis = timestamp::now_millis();

        for op in ops {
            let seq = engine.seq.fetch_add(1, Ordering::SeqCst);

            let expiration = if op.ttl_seconds > 0 {
                Some(now_millis + op.ttl_seconds * 1000)
            } else {
                None
            };

            sequences.push((op, seq, expiration));
        }

        // Build WAL records (critical: before state change)
        let mut wal_records = Vec::with_capacity(ops.len());
        for (op, seq, _expiration) in &sequences {
            let wal_op_kind = match op.kind {
                OpKind::Put => crate::wal::WalOpKind::Put,
                OpKind::Delete => crate::wal::WalOpKind::Delete,
                OpKind::Merge => crate::wal::WalOpKind::Merge,
                OpKind::DeleteRange => crate::wal::WalOpKind::DeleteRange,
            };

            let record = if let Some(range_end) = &op.range_end {
                // DeleteRange operation
                crate::wal::WalRecord::new_delete_range(
                    cf_id,
                    Bytes::from(op.key.clone()),
                    Bytes::from(range_end.clone()),
                    *seq,
                )
            } else {
                // Regular operation
                let rec = crate::wal::WalRecord {
                    cf_id: cf_id.as_u32(),
                    op: wal_op_kind,
                    key: Bytes::from(op.key.clone()),
                    value: op.value.as_ref().map(|v| Bytes::from(v.clone())),
                    seq: *seq,
                    expiration: if op.ttl_seconds > 0 {
                        Some(now_millis + op.ttl_seconds * 1000)
                    } else {
                        None
                    },
                    range_end: None,
                    txn_id: None,
                    compression: None,
                };
                rec
            };

            wal_records.push(record);
        }

        // Append to WAL (before memtable, for durability)
        if wal_records.len() == 1 {
            engine.wal_coordinator.append_record(&wal_records[0])?;
        } else {
            engine.wal_coordinator.append_batch(&wal_records)?;
        }
        engine.sync_wal_if_needed()?;

        // Apply to memtable(s)
        let mut first_seq = 0u64;
        for (i, (op, seq, expiration)) in sequences.iter().enumerate() {
            if i == 0 {
                first_seq = *seq;
            }

            let mt = cf_arc.memtable.load();
            match op.kind {
                OpKind::Put => {
                    if let Some(value) = &op.value {
                        mt.put_with_seq_and_exp(&op.key, value, *seq, *expiration);
                    }
                }
                OpKind::Delete => {
                    mt.delete_with_seq(&op.key, *seq);
                }
                OpKind::Merge => {
                    if let Some(value) = &op.value {
                        mt.merge_with_seq_and_exp(&op.key, value, *seq, *expiration);
                    }
                }
                OpKind::DeleteRange => {
                    if let Some(range_end) = &op.range_end {
                        mt.delete_range_with_seq(&op.key, range_end, *seq);
                    }
                }
            }
        }

        // Handle memtable full / stalls
        if cf_arc.is_full() {
            let frozen = cf_arc.try_freeze_memtable();
            if frozen && cf_id == crate::api::column_family::DEFAULT_CF_ID {
                let _ = engine.flush();
            }
        }

        engine.handle_write_stall(cf_handle, &cf_arc)?;

        Ok(first_seq)
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
