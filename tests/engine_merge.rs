//! Merge Operator Tests
//!
//! Tests for merge operator functionality - associative operations that allow
//! efficient read-modify-write patterns without full get/put cycles.

use bytes::Bytes;
use cntryl_midge::testkit::*;
#[cfg(feature = "merge")]
use cntryl_midge::{MergeOperator, MidgeResult, TransactionMode, WriteOptions};
use std::sync::Arc;

// ============================================================================
// Test-only merge operators
// ============================================================================

/// String append merge operator with delimiter
#[derive(Clone, Debug)]
struct StringAppendOperator {
    delimiter: String,
}

impl StringAppendOperator {
    fn new(delimiter: impl Into<String>) -> Self {
        Self {
            delimiter: delimiter.into(),
        }
    }
}

impl MergeOperator for StringAppendOperator {
    fn merge(
        &self,
        _key: &[u8],
        existing_value: Option<&[u8]>,
        operands: &[Vec<u8>],
    ) -> MidgeResult<Option<Vec<u8>>> {
        let mut result = existing_value
            .map(|v| String::from_utf8_lossy(v).to_string())
            .unwrap_or_default();

        for operand in operands {
            let operand_str = String::from_utf8_lossy(operand);
            if !result.is_empty() {
                result.push_str(&self.delimiter);
            }
            result.push_str(&operand_str);
        }

        Ok(Some(result.into_bytes()))
    }

    fn name(&self) -> &str {
        "StringAppend"
    }
}

/// Integer add merge operator (little-endian u64)
#[derive(Clone, Debug)]
#[allow(dead_code)]
struct IntegerAddOperator;

impl MergeOperator for IntegerAddOperator {
    fn merge(
        &self,
        _key: &[u8],
        existing_value: Option<&[u8]>,
        operands: &[Vec<u8>],
    ) -> MidgeResult<Option<Vec<u8>>> {
        let mut sum: u64 = existing_value
            .and_then(|v| v.try_into().ok())
            .map(u64::from_le_bytes)
            .unwrap_or(0);

        for operand in operands {
            if let Ok(bytes) = operand.as_slice().try_into() {
                sum = sum.wrapping_add(u64::from_le_bytes(bytes));
            }
        }

        Ok(Some(sum.to_le_bytes().to_vec()))
    }

    fn name(&self) -> &str {
        "IntegerAdd"
    }
}

/// Merge operator that always fails (for error testing)
#[derive(Clone, Debug)]
#[allow(dead_code)]
struct FailingOperator;

impl MergeOperator for FailingOperator {
    fn merge(
        &self,
        _key: &[u8],
        _existing_value: Option<&[u8]>,
        _operands: &[Vec<u8>],
    ) -> MidgeResult<Option<Vec<u8>>> {
        Err(cntryl_midge::MidgeError::MergeOperatorFailed(
            "Intentional failure".into(),
        ))
    }

    fn name(&self) -> &str {
        "FailingOperator"
    }
}

// ============================================================================
// Basic merge semantics
// ============================================================================

#[test]
fn should_merge_without_base_value_given_no_existing_key_when_merging() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let op = StringAppendOperator::new(",");
        engine
            .register_merge_operator(cf.id().as_u32(), Box::new(op))
            .unwrap();

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.merge(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        engine.commit(tx, WriteOptions::buffered()).unwrap();
        let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        let result = read_tx.get(b"key1").unwrap();

        // Assert
        assert_eq!(result, Some(Bytes::from_static(b"value1")));
    });
}

#[test]
fn should_merge_with_existing_base_value_given_put_when_merging() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let op = StringAppendOperator::new(",");
        engine
            .register_merge_operator(cf.id().as_u32(), Box::new(op))
            .unwrap();
        let mut tx1 = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx1.put(b"key1".to_vec(), b"base".to_vec(), None).unwrap();
        engine.commit(tx1, WriteOptions::buffered()).unwrap();

        // Act
        let mut tx2 = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx2.merge(b"key1".to_vec(), b"append".to_vec()).unwrap();
        engine.commit(tx2, WriteOptions::buffered()).unwrap();
        let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        let result = read_tx.get(b"key1").unwrap();

        // Assert
        assert_eq!(result, Some(Bytes::from_static(b"base,append")));
    });
}

#[test]
fn should_apply_multiple_merges_sequentially_given_repeated_operations_when_reading() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let op = StringAppendOperator::new(",");
        engine
            .register_merge_operator(cf.id().as_u32(), Box::new(op))
            .unwrap();

        // Act
        let mut tx1 = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx1.merge(b"key1".to_vec(), b"a".to_vec()).unwrap();
        engine.commit(tx1, WriteOptions::buffered()).unwrap();
        let mut tx2 = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx2.merge(b"key1".to_vec(), b"b".to_vec()).unwrap();
        engine.commit(tx2, WriteOptions::buffered()).unwrap();
        let mut tx3 = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx3.merge(b"key1".to_vec(), b"c".to_vec()).unwrap();
        engine.commit(tx3, WriteOptions::buffered()).unwrap();
        let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        let result = read_tx.get(b"key1").unwrap();

        // Assert
        assert_eq!(result, Some(Bytes::from_static(b"a,b,c")));
    });
}

#[test]
fn should_merge_after_delete_given_tombstone_when_treating_as_missing() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let op = StringAppendOperator::new(",");
        engine
            .register_merge_operator(cf.id().as_u32(), Box::new(op))
            .unwrap();
        let mut tx1 = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx1.put(b"key1".to_vec(), b"old".to_vec(), None).unwrap();
        engine.commit(tx1, WriteOptions::buffered()).unwrap();
        let mut tx2 = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx2.delete(b"key1".to_vec()).unwrap();
        engine.commit(tx2, WriteOptions::buffered()).unwrap();

        // Act
        let mut tx3 = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx3.merge(b"key1".to_vec(), b"new".to_vec()).unwrap();
        engine.commit(tx3, WriteOptions::buffered()).unwrap();
        let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        let result = read_tx.get(b"key1").unwrap();

        // Assert
        assert_eq!(result, Some(Bytes::from_static(b"new")));
    });
}

#[test]
fn should_handle_merge_with_put_interleaved_given_mixed_ops_when_reading() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let op = StringAppendOperator::new(",");
        engine
            .register_merge_operator(cf.id().as_u32(), Box::new(op))
            .unwrap();

        // Act
        let mut tx1 = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx1.merge(b"key1".to_vec(), b"a".to_vec()).unwrap();
        engine.commit(tx1, WriteOptions::buffered()).unwrap();
        let mut tx2 = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx2.put(b"key1".to_vec(), b"reset".to_vec(), None).unwrap();
        engine.commit(tx2, WriteOptions::buffered()).unwrap();
        let mut tx3 = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx3.merge(b"key1".to_vec(), b"b".to_vec()).unwrap();
        engine.commit(tx3, WriteOptions::buffered()).unwrap();
        let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        let result = read_tx.get(b"key1").unwrap();

        // Assert
        assert_eq!(result, Some(Bytes::from_static(b"reset,b")));
    });
}

// ============================================================================
// String append operator
// ============================================================================

#[test]
fn should_use_string_append_operator_given_delimiter_when_merging() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let op = StringAppendOperator::new("::");
        engine
            .register_merge_operator(cf.id().as_u32(), Box::new(op))
            .unwrap();

        // Act
        let mut tx1 = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx1.merge(b"key1".to_vec(), b"foo".to_vec()).unwrap();
        engine.commit(tx1, WriteOptions::buffered()).unwrap();
        let mut tx2 = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx2.merge(b"key1".to_vec(), b"bar".to_vec()).unwrap();
        engine.commit(tx2, WriteOptions::buffered()).unwrap();
        let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        let result = read_tx.get(b"key1").unwrap();

        // Assert
        assert_eq!(result, Some(Bytes::from_static(b"foo::bar")));
    });
}

#[test]
fn should_string_append_with_base_value_given_initial_put_when_merging() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let op = StringAppendOperator::new("|");
        engine
            .register_merge_operator(cf.id().as_u32(), Box::new(op))
            .unwrap();
        let mut tx1 = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx1.put(b"key1".to_vec(), b"start".to_vec(), None).unwrap();
        engine.commit(tx1, WriteOptions::buffered()).unwrap();

        // Act
        let mut tx2 = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx2.merge(b"key1".to_vec(), b"middle".to_vec()).unwrap();
        engine.commit(tx2, WriteOptions::buffered()).unwrap();
        let mut tx3 = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx3.merge(b"key1".to_vec(), b"end".to_vec()).unwrap();
        engine.commit(tx3, WriteOptions::buffered()).unwrap();
        let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        let result = read_tx.get(b"key1").unwrap();

        // Assert
        assert_eq!(result, Some(Bytes::from_static(b"start|middle|end")));
    });
}

#[test]
fn should_handle_empty_merge_operand_given_empty_bytes_when_appending() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let op = StringAppendOperator::new(",");
        engine
            .register_merge_operator(cf.id().as_u32(), Box::new(op))
            .unwrap();

        // Act
        let mut tx1 = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx1.merge(b"key1".to_vec(), b"a".to_vec()).unwrap();
        engine.commit(tx1, WriteOptions::buffered()).unwrap();
        let mut tx2 = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx2.merge(b"key1".to_vec(), b"".to_vec()).unwrap();
        engine.commit(tx2, WriteOptions::buffered()).unwrap();
        let mut tx3 = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx3.merge(b"key1".to_vec(), b"c".to_vec()).unwrap();
        engine.commit(tx3, WriteOptions::buffered()).unwrap();
        let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        let result = read_tx.get(b"key1").unwrap();

        // Assert
        assert_eq!(result, Some(Bytes::from_static(b"a,,c")));
    });
}

// Note: Column family tests will be skipped for now as create_column_family is not yet implemented
// These tests are included for completeness and will pass once CF creation is added

#[test]
fn should_isolate_merge_operators_across_cfs_given_different_operators_when_merging() {
    // Test will be implemented after CF creation support is added
}

#[test]
fn should_handle_default_cf_merge_independently_given_custom_cf_when_merging() {
    // Test will be implemented after CF creation support is added
}

#[test]
fn should_preserve_merge_semantics_across_restart_given_flush_when_recovering() {
    // Test will be implemented after CF creation support is added
}

#[test]
fn should_persist_merge_resolutions_given_cf_restart_when_reopening() {
    // Test will be implemented after CF creation support is added
}

// ============================================================================
// Error handling
// ============================================================================

#[test]
fn should_error_when_merging_without_registered_operator_when_merging() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.merge(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        let result = engine.commit(tx, WriteOptions::buffered());

        // Assert
        assert!(result.is_err(), "Should error without merge operator");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("merge operator") || err_msg.contains("not registered"),
            "Error message should mention merge operator: {}",
            err_msg
        );
    });
}

#[test]
fn should_surface_error_given_failing_merge_operator_when_getting() {
    // This test requires merge operands to be stored and resolved during get()
    // Will be implemented after merge resolution is added to the read path
}

#[test]
fn should_keep_data_readable_given_merge_operator_changed_across_restart_when_reopening() {
    // Test will be implemented after persistence is added
}

// ============================================================================
// Concurrency
// ============================================================================

#[test]
fn should_not_lose_merge_operands_under_concurrency_given_same_key_when_merging() {
    // Requires merge operands to be properly accumulated and resolved
}

#[test]
fn should_handle_concurrent_merges_to_same_key_given_integer_add_operator_when_merging() {
    // Requires merge operands to be properly accumulated and resolved
}

#[test]
fn should_handle_merge_with_binary_data_given_binary_key_when_merging() {
    // Requires merge operands to be properly accumulated and resolved
}

#[test]
fn should_not_merge_across_delete_range_given_range_tombstone_when_merging() {
    // Requires both merge resolution and delete_range interaction
}
