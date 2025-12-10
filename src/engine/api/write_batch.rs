//! Write batch API - batched writes for efficiency
//!
//! WriteBatch collects multiple operations and applies them atomically
//! to reduce WAL I/O overhead and improve throughput.

use super::super::ColumnFamilyId;

/// A batch of write operations to be applied atomically
#[derive(Debug, Clone)]
pub struct WriteBatch {
    /// Operations in the batch
    operations: Vec<BatchOp>,
}

/// A single operation in a write batch
#[derive(Debug, Clone)]
#[allow(dead_code)] // These will be used when write_batch is integrated into engine
enum BatchOp {
    /// Put operation
    Put {
        cf_id: ColumnFamilyId,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_seconds: Option<u64>,
    },
    /// Delete operation
    Delete { cf_id: ColumnFamilyId, key: Vec<u8> },
}

impl WriteBatch {
    /// Create a new empty write batch
    pub fn new() -> Self {
        Self {
            operations: Vec::new(),
        }
    }

    /// Add a put operation to the batch for the default column family
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> &mut Self {
        self.put_cf(ColumnFamilyId::DEFAULT, key, value);
        self
    }

    /// Add a put operation to the batch for a specific column family
    pub fn put_cf(&mut self, cf_id: ColumnFamilyId, key: Vec<u8>, value: Vec<u8>) -> &mut Self {
        self.operations.push(BatchOp::Put { cf_id, key, value, ttl_seconds: None });
        self
    }

    /// Add a put operation with TTL to the batch
    pub fn put_with_ttl(&mut self, cf_id: ColumnFamilyId, key: bytes::Bytes, value: bytes::Bytes, ttl_seconds: u64) -> &mut Self {
        self.operations.push(BatchOp::Put { 
            cf_id, 
            key: key.to_vec(), 
            value: value.to_vec(), 
            ttl_seconds: if ttl_seconds == 0 { None } else { Some(ttl_seconds) }
        });
        self
    }

    /// Add a delete operation to the batch for the default column family
    pub fn delete(&mut self, key: Vec<u8>) -> &mut Self {
        self.delete_cf(ColumnFamilyId::DEFAULT, key);
        self
    }

    /// Add a delete operation to the batch for a specific column family
    pub fn delete_cf(&mut self, cf_id: ColumnFamilyId, key: Vec<u8>) -> &mut Self {
        self.operations.push(BatchOp::Delete { cf_id, key });
        self
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

    /// Iterate over put operations in this batch
    #[allow(dead_code)] // Used by runtime when processing batch
    pub(crate) fn iter_puts(&self) -> impl Iterator<Item = (ColumnFamilyId, &[u8], &[u8])> {
        self.operations.iter().filter_map(|op| match op {
            BatchOp::Put { cf_id, key, value, .. } => Some((*cf_id, key.as_slice(), value.as_slice())),
            _ => None,
        })
    }

    /// Iterate over delete operations in this batch
    #[allow(dead_code)] // Used by runtime when processing batch
    pub(crate) fn iter_deletes(&self) -> impl Iterator<Item = (ColumnFamilyId, &[u8])> {
        self.operations.iter().filter_map(|op| match op {
            BatchOp::Delete { cf_id, key } => Some((*cf_id, key.as_slice())),
            _ => None,
        })
    }
}

impl Default for WriteBatch {
    fn default() -> Self {
        Self::new()
    }
}

impl BatchOp {
    /// Check if this is a put operation
    #[allow(dead_code)]
    pub(crate) fn is_put(&self) -> bool {
        matches!(self, Self::Put { .. })
    }

    /// Check if this is a delete operation
    #[allow(dead_code)]
    pub(crate) fn is_delete(&self) -> bool {
        matches!(self, Self::Delete { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_empty_batch_when_initialized() {
        // Arrange & Act
        let batch = WriteBatch::new();

        // Assert
        assert_eq!(batch.len(), 0);
        assert!(batch.is_empty());
    }

    #[test]
    fn should_add_put_operations_when_calling_put() {
        // Arrange
        let mut batch = WriteBatch::new();

        // Act
        batch.put(vec![1, 2, 3], vec![4, 5, 6]);
        batch.put(vec![7, 8], vec![9]);

        // Assert
        assert_eq!(batch.len(), 2);
        assert!(!batch.is_empty());
        assert!(batch.operations[0].is_put());
        assert!(batch.operations[1].is_put());
    }

    #[test]
    fn should_add_delete_operations_when_calling_delete() {
        // Arrange
        let mut batch = WriteBatch::new();

        // Act
        batch.delete(vec![1, 2, 3]);
        batch.delete(vec![7, 8]);

        // Assert
        assert_eq!(batch.len(), 2);
        assert!(batch.operations[0].is_delete());
        assert!(batch.operations[1].is_delete());
    }

    #[test]
    fn should_support_mixed_operations_when_building_batch() {
        // Arrange
        let mut batch = WriteBatch::new();

        // Act
        batch.put(vec![1], vec![2]);
        batch.delete(vec![3]);
        batch.put(vec![4], vec![5]);

        // Assert
        assert_eq!(batch.len(), 3);
        assert!(batch.operations[0].is_put());
        assert!(batch.operations[1].is_delete());
        assert!(batch.operations[2].is_put());
    }

    #[test]
    fn should_assign_cf_id_when_using_put_cf() {
        // Arrange
        let mut batch = WriteBatch::new();
        let cf1 = ColumnFamilyId(1);
        let cf2 = ColumnFamilyId(2);

        // Act
        batch.put_cf(cf1, vec![1, 2], vec![3, 4]);
        batch.put_cf(cf2, vec![5, 6], vec![7, 8]);

        // Assert
        let puts: Vec<_> = batch.iter_puts().collect();
        assert_eq!(puts.len(), 2);
        assert_eq!(puts[0].0, cf1);
        assert_eq!(puts[1].0, cf2);
    }

    #[test]
    fn should_clear_all_operations_when_clearing_batch() {
        // Arrange
        let mut batch = WriteBatch::new();
        batch.put(vec![1], vec![2]);
        batch.delete(vec![3]);
        assert_eq!(batch.len(), 2);

        // Act
        batch.clear();

        // Assert
        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);
    }

    #[test]
    fn should_support_builder_pattern_with_chaining() {
        // Arrange
        let cf1 = ColumnFamilyId(1);

        // Act
        let batch = {
            let mut b = WriteBatch::new();
            b.put(vec![1], vec![2])
                .delete(vec![3])
                .put_cf(cf1, vec![4], vec![5]);
            b
        };

        // Assert
        assert_eq!(batch.len(), 3);
        let puts: Vec<_> = batch.iter_puts().collect();
        let deletes: Vec<_> = batch.iter_deletes().collect();
        assert_eq!(puts.len(), 2);
        assert_eq!(deletes.len(), 1);
        assert_eq!(puts[0].0, ColumnFamilyId::DEFAULT);
        assert_eq!(puts[1].0, cf1);
    }
}
