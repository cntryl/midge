//! Write batching for group commit optimization
//!
//! Collects multiple write operations and commits them atomically to WAL and memtable.
//! This reduces WAL overhead by batching multiple operations into single writes.

use crate::api::ColumnFamilyId;
use crate::wal::WalOpKind;
use bytes::Bytes;

/// A batch of write operations to be committed atomically
#[derive(Debug, Default)]
pub struct WriteBatch {
    operations: Vec<WriteOp>,
}

#[derive(Debug, Clone)]
pub(crate) struct WriteOp {
    cf_id: ColumnFamilyId,
    kind: WalOpKind,
    key: Bytes,
    value: Option<Bytes>,
    ttl_seconds: u64,
}

impl WriteBatch {
    /// Create a new empty write batch
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a put operation to the batch for a specific column family
    pub fn put(&mut self, cf_id: ColumnFamilyId, key: Bytes, value: Bytes) {
        self.operations.push(WriteOp {
            cf_id,
            kind: WalOpKind::Put,
            key,
            value: Some(value),
            ttl_seconds: 0,
        });
    }

    /// Add a put with TTL operation to the batch for a specific column family
    pub fn put_with_ttl(&mut self, cf_id: ColumnFamilyId, key: Bytes, value: Bytes, ttl_seconds: u64) {
        self.operations.push(WriteOp {
            cf_id,
            kind: WalOpKind::Put,
            key,
            value: Some(value),
            ttl_seconds,
        });
    }

    /// Add a delete operation to the batch for a specific column family
    pub fn delete(&mut self, cf_id: ColumnFamilyId, key: Bytes) {
        self.operations.push(WriteOp {
            cf_id,
            kind: WalOpKind::Delete,
            key,
            value: None,
            ttl_seconds: 0,
        });
    }

    /// Get the number of operations in this batch
    pub fn len(&self) -> usize {
        self.operations.len()
    }

    /// Check if the batch is empty
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    /// Clear all operations from the batch
    pub fn clear(&mut self) {
        self.operations.clear();
    }

    /// Get an iterator over the operations
    pub(crate) fn operations(&self) -> impl Iterator<Item = &WriteOp> {
        self.operations.iter()
    }
}

impl WriteOp {
    pub(crate) fn cf_id(&self) -> ColumnFamilyId {
        self.cf_id
    }

    pub(crate) fn kind(&self) -> WalOpKind {
        self.kind
    }

    pub(crate) fn key(&self) -> &Bytes {
        &self.key
    }

    pub(crate) fn value(&self) -> Option<&Bytes> {
        self.value.as_ref()
    }

    pub(crate) fn ttl_seconds(&self) -> u64 {
        self.ttl_seconds
    }
}
