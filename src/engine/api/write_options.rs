//! Write options for transactions and operations

use crate::common::MidgeResult;

/// Options for write operations (transactions and batch writes)
#[derive(Debug, Clone)]
pub struct WriteOptions {
    /// Whether to synchronously wait for the write to be durable
    pub sync: bool,
    /// Disable WAL for this write (unsafe, not recommended)
    pub disable_wal: bool,
}

impl WriteOptions {
    /// Create default write options
    pub fn new() -> Self {
        Self {
            sync: false,
            disable_wal: false,
        }
    }

    /// Enable synchronous durability
    pub fn sync(mut self) -> Self {
        self.sync = true;
        self
    }

    /// Disable WAL (dangerous - only for testing)
    pub fn disable_wal(mut self) -> Self {
        self.disable_wal = true;
        self
    }
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// User-facing transaction trait
///
/// Provides ergonomic interface for transactional operations.
/// Transactions are scoped to a single column family for safety.
pub trait KvTransaction: Send {
    /// Read a key within the transaction
    fn get(&self, key: &[u8]) -> MidgeResult<Option<bytes::Bytes>>;

    /// Write a key within the transaction
    fn put(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()>;

    /// Delete a key within the transaction
    fn delete(&mut self, key: &[u8]) -> MidgeResult<()>;

    /// Get the transaction ID
    fn id(&self) -> u64;

    /// Check if transaction is active
    fn is_active(&self) -> bool;
}

/// Concrete transaction implementation wrapping api::Transaction
pub struct TransactionImpl {
    inner: crate::engine::api::Transaction,
    cf_id: crate::engine::ColumnFamilyId,
}

impl TransactionImpl {
    /// Create a new transaction for a specific column family
    pub fn new(cf_id: crate::engine::ColumnFamilyId, txn: crate::engine::api::Transaction) -> Self {
        Self { inner: txn, cf_id }
    }

    /// Get the inner transaction
    pub fn inner(&self) -> &crate::engine::api::Transaction {
        &self.inner
    }

    /// Consume and return the inner transaction
    pub fn into_inner(self) -> crate::engine::api::Transaction {
        self.inner
    }
}

impl KvTransaction for TransactionImpl {
    fn get(&self, _key: &[u8]) -> MidgeResult<Option<bytes::Bytes>> {
        // For now, transactions don't track reads (no MVCC implemented)
        // This is a stub for API compatibility
        Ok(None)
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()> {
        self.inner.put(self.cf_id, key.to_vec(), value.to_vec())
    }

    fn delete(&mut self, key: &[u8]) -> MidgeResult<()> {
        self.inner.delete(self.cf_id, key.to_vec())
    }

    fn id(&self) -> u64 {
        self.inner.id()
    }

    fn is_active(&self) -> bool {
        self.inner.state() == crate::engine::api::TransactionState::Active
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========== WriteOptions Initialization Tests ==========
    // Tests for WriteOptions::new() invariants: all fields initialized with defaults

    #[test]
    fn should_initialize_all_fields_to_false_when_creating_new_options() {
        // Arrange & Act
        let opts = WriteOptions::new();

        // Assert
        assert!(!opts.sync);
        assert!(!opts.disable_wal);
    }

    #[test]
    fn should_create_options_with_all_default_values_when_calling_default() {
        // Arrange & Act
        let opts = WriteOptions::default();

        // Assert - all fields should match new()
        assert!(!opts.sync);
        assert!(!opts.disable_wal);
    }

    #[test]
    fn should_have_new_and_default_return_equivalent_options() {
        // Arrange
        let new_opts = WriteOptions::new();
        let default_opts = WriteOptions::default();

        // Act & Assert
        assert_eq!(new_opts.sync, default_opts.sync);
        assert_eq!(new_opts.disable_wal, default_opts.disable_wal);
    }

    // ========== WriteOptions Sync Method Tests ==========
    // Tests for sync() method: sets sync field to true, returns self for chaining

    #[test]
    fn should_set_sync_to_true_when_calling_sync() {
        // Arrange & Act
        let opts = WriteOptions::new().sync();

        // Assert
        assert!(opts.sync);
        assert!(!opts.disable_wal);
    }

    #[test]
    fn should_return_self_for_chaining_when_calling_sync() {
        // Arrange & Act
        let opts = WriteOptions::new()
            .sync()
            .disable_wal();

        // Assert
        assert!(opts.sync);
        assert!(opts.disable_wal);
    }

    #[test]
    fn should_keep_sync_true_when_calling_sync_multiple_times() {
        // Arrange & Act
        let opts = WriteOptions::new()
            .sync()
            .sync();

        // Assert
        assert!(opts.sync);
    }

    // ========== WriteOptions Disable WAL Method Tests ==========
    // Tests for disable_wal() method: sets disable_wal field to true, returns self for chaining

    #[test]
    fn should_set_disable_wal_to_true_when_calling_disable_wal() {
        // Arrange & Act
        let opts = WriteOptions::new().disable_wal();

        // Assert
        assert!(!opts.sync);
        assert!(opts.disable_wal);
    }

    #[test]
    fn should_return_self_for_chaining_when_calling_disable_wal() {
        // Arrange & Act
        let opts = WriteOptions::new()
            .disable_wal()
            .sync();

        // Assert
        assert!(opts.sync);
        assert!(opts.disable_wal);
    }

    #[test]
    fn should_keep_disable_wal_true_when_calling_disable_wal_multiple_times() {
        // Arrange & Act
        let opts = WriteOptions::new()
            .disable_wal()
            .disable_wal();

        // Assert
        assert!(opts.disable_wal);
    }

    // ========== WriteOptions Clone Tests ==========
    // Tests for Clone trait: independent copies

    #[test]
    fn should_clone_options_with_all_defaults() {
        // Arrange
        let original = WriteOptions::new();

        // Act
        let cloned = original.clone();

        // Assert
        assert_eq!(cloned.sync, original.sync);
        assert_eq!(cloned.disable_wal, original.disable_wal);
    }

    #[test]
    fn should_clone_options_with_sync_enabled() {
        // Arrange
        let original = WriteOptions::new().sync();

        // Act
        let cloned = original.clone();

        // Assert
        assert!(cloned.sync);
        assert!(!cloned.disable_wal);
    }

    #[test]
    fn should_clone_options_with_wal_disabled() {
        // Arrange
        let original = WriteOptions::new().disable_wal();

        // Act
        let cloned = original.clone();

        // Assert
        assert!(!cloned.sync);
        assert!(cloned.disable_wal);
    }

    #[test]
    fn should_clone_options_with_both_flags_set() {
        // Arrange
        let original = WriteOptions::new()
            .sync()
            .disable_wal();

        // Act
        let cloned = original.clone();

        // Assert
        assert!(cloned.sync);
        assert!(cloned.disable_wal);
    }

    #[test]
    fn should_be_independent_after_cloning() {
        // Arrange
        let original = WriteOptions::new();

        // Act
        let cloned = original.clone();
        let modified_cloned = cloned.sync();

        // Assert - original unchanged
        assert!(!original.sync);
        assert!(modified_cloned.sync);
    }

    // ========== WriteOptions Debug Trait Tests ==========

    #[test]
    fn should_debug_format_options_with_defaults() {
        // Arrange & Act
        let opts = WriteOptions::new();
        let debug_str = format!("{:?}", opts);

        // Assert
        assert!(debug_str.contains("WriteOptions"));
    }

    #[test]
    fn should_debug_format_options_with_sync() {
        // Arrange & Act
        let opts = WriteOptions::new().sync();
        let debug_str = format!("{:?}", opts);

        // Assert
        assert!(debug_str.contains("sync"));
    }

    // ========== WriteOptions Fluent API Tests ==========
    // Tests for method chaining with all combinations

    #[test]
    fn should_support_full_fluent_api_chain() {
        // Arrange & Act
        let opts = WriteOptions::new()
            .sync()
            .disable_wal();

        // Assert
        assert!(opts.sync);
        assert!(opts.disable_wal);
    }

    #[test]
    fn should_allow_methods_in_any_order() {
        // Arrange
        let opts1 = WriteOptions::new()
            .sync()
            .disable_wal();

        let opts2 = WriteOptions::new()
            .disable_wal()
            .sync();

        // Act & Assert
        assert_eq!(opts1.sync, opts2.sync);
        assert_eq!(opts1.disable_wal, opts2.disable_wal);
    }

    // ========== Edge Cases ==========

    #[test]
    fn should_handle_multiple_sync_calls() {
        // Arrange & Act
        let opts = WriteOptions::new()
            .sync()
            .sync()
            .sync();

        // Assert
        assert!(opts.sync);
    }

    #[test]
    fn should_handle_alternating_method_calls() {
        // Arrange & Act
        let opts = WriteOptions::new()
            .sync()
            .disable_wal()
            .sync()
            .disable_wal();

        // Assert
        assert!(opts.sync);
        assert!(opts.disable_wal);
    }

    #[test]
    fn should_preserve_field_values_through_chaining() {
        // Arrange
        let intermediate = WriteOptions::new().sync();

        // Act
        let final_opts = intermediate.disable_wal();

        // Assert
        assert!(final_opts.sync);
        assert!(final_opts.disable_wal);
    }
}
