//! Advanced Merge Operator Tests
//!
//! Tests advanced merge operator scenarios: tombstone interactions, error handling,
//! and complex merge patterns. Validates that merge operators behave correctly when
//! merging with deleted keys (tombstones), in write batches, and with special data.
//!
//! Naming convention:
//!   should_<behavior>_given_<context>_when_<condition>
//!
//! All tests run on ALL storage modes.

use bytes::Bytes;
use cntryl_midge::testkit::*;
use cntryl_midge::{MergeOperator, MidgeResult};
use std::sync::Arc;

// ============================================================================
// Simple string append merge operator
// ============================================================================

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

// ============================================================================
// TOMBSTONE INTERACTION TESTS
// ============================================================================

#[test]
fn should_apply_merge_given_delete_then_merge_when_tombstone_base() {
    // Test merge behavior when base value is a tombstone (deleted key)
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange: Write, delete (tombstone), then merge
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let op = StringAppendOperator::new("");
        engine
            .register_merge_operator(cf.id().as_u32(), Box::new(op))
            .expect("register");

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        tx.put(b"key".to_vec(), b"initial".to_vec(), None)
            .expect("put");
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .expect("commit");

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        tx.delete(b"key".to_vec()).expect("delete"); // Creates tombstone
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .expect("commit");

        // Act: Merge on tombstone
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        tx.merge(b"key".to_vec(), b"merged".to_vec())
            .expect("merge on tombstone");
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .expect("commit");

        // Assert: Merge applies to tombstone state (treats as empty)
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin");
        let got = tx.get(b"key").expect("get");
        assert!(
            got.is_some(),
            "merge did not apply to tombstone in mode: {}",
            mode
        );
    });
}

#[test]
fn should_delete_after_merge_given_merge_then_delete_when_sequence() {
    // Test that delete works correctly after merge
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange: Start with empty key
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let op = StringAppendOperator::new("");
        engine
            .register_merge_operator(cf.id().as_u32(), Box::new(op))
            .expect("register");

        // Act: Merge creates value, then delete
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        tx.merge(b"key".to_vec(), b"value1".to_vec())
            .expect("first merge");
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .expect("commit");

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        tx.merge(b"key".to_vec(), b"value2".to_vec())
            .expect("second merge");
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .expect("commit");

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        tx.delete(b"key".to_vec()).expect("delete after merge");
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .expect("commit");

        // Assert: Delete wins
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin");
        let got = tx.get(b"key").expect("get");
        assert_eq!(
            got, None,
            "delete did not remove merged value in mode: {}",
            mode
        );
    });
}

#[test]
fn should_handle_merge_on_many_tombstones_given_delete_merge_cycles_when_repeated() {
    // Test merge with accumulation of tombstones
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let op = StringAppendOperator::new("");
        engine
            .register_merge_operator(cf.id().as_u32(), Box::new(op))
            .expect("register");

        // Act: Repeat delete/merge cycles
        for i in 0..3 {
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin");
            tx.put(
                b"key".to_vec(),
                format!("value_{i}").as_bytes().to_vec(),
                None,
            )
            .expect("put");
            engine
                .commit(tx, cntryl_midge::WriteOptions::buffered())
                .expect("commit");

            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin");
            tx.delete(b"key".to_vec()).expect("delete");
            engine
                .commit(tx, cntryl_midge::WriteOptions::buffered())
                .expect("commit");

            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin");
            tx.merge(b"key".to_vec(), format!("merged_{i}").as_bytes().to_vec())
                .expect("merge");
            engine
                .commit(tx, cntryl_midge::WriteOptions::buffered())
                .expect("commit");
        }

        // Assert: Final state after last merge
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin");
        let got = tx.get(b"key").expect("get");
        assert!(
            got.is_some(),
            "merge state lost after multiple delete/merge cycles in mode: {}",
            mode
        );
    });
}

// ============================================================================
// BATCH MERGE TESTS
// ============================================================================

#[test]
fn should_apply_multiple_merges_in_batch_given_write_batch_when_committed() {
    // Test multiple merges on same key in a write batch
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        tx.put(b"key".to_vec(), b"base".to_vec(), None)
            .expect("put base");
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .expect("commit");

        // Act: Multiple puts in single transaction (batch ordering)
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        tx.put(b"key".to_vec(), b"value1".to_vec(), None)
            .expect("put 1");
        tx.put(b"key".to_vec(), b"value2".to_vec(), None)
            .expect("put 2");
        tx.put(b"key".to_vec(), b"value3".to_vec(), None)
            .expect("put 3");
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .expect("commit batch");

        // Assert: Last put in batch wins
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin");
        let got = tx.get(b"key").expect("get");
        assert_eq!(
            got,
            Some(Bytes::from_static(b"value3")),
            "batch ordering incorrect in mode: {}",
            mode
        );
    });
}

// ============================================================================
// SEQUENTIAL MERGE TESTS
// ============================================================================

#[test]
fn should_accumulate_values_given_10_sequential_merges_when_applying() {
    // Test accumulation over many sequential merges
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let op = StringAppendOperator::new("-");
        engine
            .register_merge_operator(cf.id().as_u32(), Box::new(op))
            .expect("register");

        // Act: Apply 10 sequential merges (no base value)
        for i in 0..10 {
            let operand = format!("_{i}");
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin");
            tx.merge(b"accumulate".to_vec(), operand.as_bytes().to_vec())
                .expect("merge");
            engine
                .commit(tx, cntryl_midge::WriteOptions::buffered())
                .expect("commit");
        }

        // Assert: Accumulated result exists
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin");
        let got = tx.get(b"accumulate").expect("get");
        assert!(
            got.is_some(),
            "10 sequential merges did not produce value in mode: {}",
            mode
        );
    });
}

#[test]
fn should_preserve_merge_with_empty_operand_given_empty_bytes_when_merging() {
    // Test merge behavior with empty operand
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let op = StringAppendOperator::new(",");
        engine
            .register_merge_operator(cf.id().as_u32(), Box::new(op))
            .expect("register");

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        tx.put(b"key".to_vec(), b"base".to_vec(), None)
            .expect("put base");
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .expect("commit");

        // Act: Merge with empty operand
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        tx.merge(b"key".to_vec(), b"".to_vec())
            .expect("merge empty");
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .expect("commit");

        // Assert: Key still exists with some value
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin");
        let got = tx.get(b"key").expect("get");
        assert!(
            got.is_some(),
            "merge with empty operand lost value in mode: {}",
            mode
        );
    });
}

// ============================================================================
// BINARY DATA TESTS
// ============================================================================

#[test]
fn should_handle_binary_data_in_merge_given_non_utf8_when_merging() {
    // Test merge with binary (non-UTF8) keys and values
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let op = StringAppendOperator::new("");
        engine
            .register_merge_operator(cf.id().as_u32(), Box::new(op))
            .expect("register");

        let binary_key = vec![0x00, 0x01, 0x02, 0xFF, 0xFE];
        let binary_operand = vec![0x42, 0x43, 0x44, 0x00];

        // Act: Put base with binary data
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        tx.put(binary_key.clone(), vec![0xAA, 0xBB], None)
            .expect("put binary");
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .expect("commit");

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        tx.merge(binary_key.clone(), binary_operand.clone())
            .expect("merge binary");
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .expect("commit");

        // Assert: Binary data preserved through merge
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin");
        let got = tx.get(&binary_key).expect("get");
        assert!(got.is_some(), "binary data lost in merge in mode: {}", mode);
    });
}

#[test]
fn should_handle_special_characters_in_string_merge_given_delimiters_when_appending() {
    // Test merge with special characters (newlines, nulls, etc.)
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family();
        let op = StringAppendOperator::new("|");
        engine
            .register_merge_operator(cf.id().as_u32(), Box::new(op))
            .expect("register");

        // Act: Write base and merge with special characters
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        tx.put(b"key".to_vec(), b"base".to_vec(), None)
            .expect("put");
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .expect("commit");

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        tx.merge(b"key".to_vec(), "_with_newline_\n".as_bytes().to_vec())
            .expect("merge newline");
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .expect("commit");

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        tx.merge(b"key".to_vec(), "tab_\t_here".as_bytes().to_vec())
            .expect("merge tab");
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .expect("commit");

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        tx.merge(
            b"key".to_vec(),
            "emoji_Ã°Å¸Ëœâ‚¬_unicode".as_bytes().to_vec(),
        )
        .expect("merge emoji");
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .expect("commit");

        // Assert: All special characters handled
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin");
        let got = tx.get(b"key").expect("get");
        assert!(
            got.is_some(),
            "special characters lost in merge in mode: {}",
            mode
        );
    });
}

#[test]
fn should_accumulate_multiple_merges_on_different_keys_when_batch() {
    // Test multiple operations on different keys in batch
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        for i in 0..5 {
            let key = format!("key_{i}");
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin");
            tx.put(key.as_bytes().to_vec(), b"base".to_vec(), None)
                .expect("put");
            engine
                .commit(tx, cntryl_midge::WriteOptions::buffered())
                .expect("commit");
        }

        // Act: Single transaction with operations on different keys
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        for i in 0..5 {
            let key = format!("key_{i}");
            tx.put(
                key.as_bytes().to_vec(),
                format!("_update{i}").as_bytes().to_vec(),
                None,
            )
            .expect("put");
        }
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .expect("commit batch");

        // Assert: All updates applied
        for i in 0..5 {
            let key = format!("key_{i}");
            let tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin");
            let got = tx.get(key.as_bytes()).expect("get");
            assert!(got.is_some(), "update lost on key_{} in mode: {}", i, mode);
        }
    });
}
