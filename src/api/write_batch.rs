//! Write batching for group commit optimization
//!
//! Collects multiple write operations and commits them atomically to WAL and memtable.
//! This reduces WAL overhead by batching multiple operations into single writes.

use crate::api::ColumnFamilyId;
use bytes::Bytes;

/// Internal operation kind (mirrors WAL operation types)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpKind {
    Put,
    Delete,
}

/// A batch of write operations to be committed atomically
#[derive(Debug, Default)]
pub struct WriteBatch {
    operations: Vec<WriteOp>,
}

#[derive(Debug, Clone)]
pub(crate) struct WriteOp {
    cf_id: ColumnFamilyId,
    kind: OpKind,
    key: Bytes,
    value: Option<Bytes>,
    ttl_seconds: u64,
}

impl WriteBatch {
    /// Create a new empty write batch
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new write batch with pre-allocated capacity
    ///
    /// Useful when the expected batch size is known upfront, avoiding reallocation
    /// during the put/delete operations that fill the batch.
    ///
    /// # Example
    /// ```ignore
    /// let mut batch = WriteBatch::with_capacity(100);
    /// for i in 0..100 {
    ///     batch.put(0, key, value);
    /// }
    /// // No reallocations during puts
    /// ```
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            operations: Vec::with_capacity(capacity),
        }
    }

    /// Add a put operation to the batch for a specific column family
    pub fn put(&mut self, cf_id: ColumnFamilyId, key: Bytes, value: Bytes) {
        self.operations.push(WriteOp {
            cf_id,
            kind: OpKind::Put,
            key,
            value: Some(value),
            ttl_seconds: 0,
        });
    }

    /// Add a put with TTL operation to the batch for a specific column family
    pub fn put_with_ttl(
        &mut self,
        cf_id: ColumnFamilyId,
        key: Bytes,
        value: Bytes,
        ttl_seconds: u64,
    ) {
        self.operations.push(WriteOp {
            cf_id,
            kind: OpKind::Put,
            key,
            value: Some(value),
            ttl_seconds,
        });
    }

    /// Add a delete operation to the batch for a specific column family
    pub fn delete(&mut self, cf_id: ColumnFamilyId, key: Bytes) {
        self.operations.push(WriteOp {
            cf_id,
            kind: OpKind::Delete,
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

    pub(crate) fn kind(&self) -> OpKind {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::DEFAULT_CF_ID;

    #[test]
    fn should_be_empty_given_new_write_batch() {
        // Arrange
        let batch = WriteBatch::new();

        // Act
        let is_empty = batch.is_empty();

        // Assert
        assert!(is_empty);
        assert_eq!(batch.len(), 0);
    }

    #[test]
    fn should_add_put_operation_given_put_called() {
        // Arrange
        let mut batch = WriteBatch::new();

        // Act
        batch.put(DEFAULT_CF_ID, Bytes::from("k1"), Bytes::from("v1"));

        // Assert
        assert_eq!(batch.len(), 1);
        let op = batch.operations().next().unwrap();
        assert_eq!(op.cf_id(), DEFAULT_CF_ID);
        assert_eq!(op.kind(), OpKind::Put);
        assert_eq!(op.key(), &Bytes::from("k1"));
        assert_eq!(op.value().unwrap(), &Bytes::from("v1"));
        assert_eq!(op.ttl_seconds(), 0);
    }

    #[test]
    fn should_add_delete_operation_given_delete_called() {
        // Arrange
        let mut batch = WriteBatch::new();
        batch.put(DEFAULT_CF_ID, Bytes::from("k1"), Bytes::from("v1"));

        // Act
        batch.delete(DEFAULT_CF_ID, Bytes::from("k1"));

        // Assert
        assert_eq!(batch.len(), 2);
        let mut iter = batch.operations();
        let first = iter.next().unwrap();
        let second = iter.next().unwrap();
        assert_eq!(first.kind(), OpKind::Put);
        assert_eq!(second.kind(), OpKind::Delete);
        assert!(second.value().is_none());
        assert_eq!(second.ttl_seconds(), 0);
    }

    #[test]
    fn should_add_put_with_ttl_operation_given_put_with_ttl_called() {
        // Arrange
        let mut batch = WriteBatch::new();

        // Act
        batch.put_with_ttl(DEFAULT_CF_ID, Bytes::from("k1"), Bytes::from("v1"), 3600);

        // Assert
        assert_eq!(batch.len(), 1);
        let op = batch.operations().next().unwrap();
        assert_eq!(op.kind(), OpKind::Put);
        assert_eq!(op.ttl_seconds(), 3600);
    }

    #[test]
    fn should_iterate_operations_in_order_given_multiple_writes() {
        // Arrange
        let mut batch = WriteBatch::with_capacity(3);

        // Act
        batch.put(DEFAULT_CF_ID, Bytes::from("a"), Bytes::from("va"));
        batch.put(DEFAULT_CF_ID, Bytes::from("b"), Bytes::from("vb"));
        batch.delete(DEFAULT_CF_ID, Bytes::from("a"));

        // Assert
        let ops: Vec<_> = batch.operations().collect();
        assert_eq!(ops.len(), 3);
        assert_eq!(ops[0].key(), &Bytes::from("a"));
        assert_eq!(ops[1].key(), &Bytes::from("b"));
        assert_eq!(ops[2].kind(), OpKind::Delete);
    }
}
