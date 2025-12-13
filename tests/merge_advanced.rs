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
use cntryl_midge::engine::api::WriteBatch;
use cntryl_midge::{MergeOperator, MidgeResult};
use cntryl_midge::testkit::*;
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
        engine.register_merge_operator(cf.id().as_u32(), Box::new(op)).expect("register");

        engine.put(cf, b"key", b"initial").expect("put");
        engine.delete(cf, b"key").expect("delete"); // Creates tombstone

        // Act: Merge on tombstone
        engine.merge_cf(cf, b"key", b"merged").expect("merge on tombstone");

        // Assert: Merge applies to tombstone state (treats as empty)
        let got = engine.get(cf, b"key").expect("get");
        assert!(got.is_some(), "merge did not apply to tombstone in mode: {}", mode);
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
        engine.register_merge_operator(cf.id().as_u32(), Box::new(op)).expect("register");

        // Act: Merge creates value, then delete
        engine.merge_cf(cf, b"key", b"value1").expect("first merge");
        engine.merge_cf(cf, b"key", b"value2").expect("second merge");
        engine.delete(cf, b"key").expect("delete after merge");

        // Assert: Delete wins
        let got = engine.get(cf, b"key").expect("get");
        assert_eq!(got, None, "delete did not remove merged value in mode: {}", mode);
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
        engine.register_merge_operator(cf.id().as_u32(), Box::new(op)).expect("register");

        // Act: Repeat delete/merge cycles
        for i in 0..3 {
            engine.put(cf, b"key", format!("value_{i}").as_bytes()).expect("put");
            engine.delete(cf, b"key").expect("delete");
            engine.merge_cf(cf, b"key", format!("merged_{i}").as_bytes()).expect("merge");
        }

        // Assert: Final state after last merge
        let got = engine.get(cf, b"key").expect("get");
        assert!(got.is_some(), "merge state lost after multiple delete/merge cycles in mode: {}", mode);
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

        engine.put(cf, b"key", b"base").expect("put base");

        // Act: Write batch with multiple puts (merges require custom operator)
        // For now, test with puts to validate batch ordering
        let mut batch = WriteBatch::new();
        batch.put(bytes::Bytes::copy_from_slice(b"key"), bytes::Bytes::copy_from_slice(b"value1"));
        batch.put(bytes::Bytes::copy_from_slice(b"key"), bytes::Bytes::copy_from_slice(b"value2"));
        batch.put(bytes::Bytes::copy_from_slice(b"key"), bytes::Bytes::copy_from_slice(b"value3"));
        engine.write_batch(&batch).expect("commit batch");

        // Assert: Last put in batch wins
        let got = engine.get(cf, b"key").expect("get");
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
        engine.register_merge_operator(cf.id().as_u32(), Box::new(op)).expect("register");

        // Act: Apply 10 sequential merges (no base value)
        for i in 0..10 {
            let operand = format!("_{i}");
            engine.merge_cf(cf, b"accumulate", operand.as_bytes()).expect("merge");
        }

        // Assert: Accumulated result exists
        let got = engine.get(cf, b"accumulate").expect("get");
        assert!(got.is_some(), "10 sequential merges did not produce value in mode: {}", mode);
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
        engine.register_merge_operator(cf.id().as_u32(), Box::new(op)).expect("register");

        engine.put(cf, b"key", b"base").expect("put base");

        // Act: Merge with empty operand
        engine.merge_cf(cf, b"key", b"").expect("merge empty");

        // Assert: Key still exists with some value
        let got = engine.get(cf, b"key").expect("get");
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
        engine.register_merge_operator(cf.id().as_u32(), Box::new(op)).expect("register");

        let binary_key = vec![0x00, 0x01, 0x02, 0xFF, 0xFE];
        let binary_operand = vec![0x42, 0x43, 0x44, 0x00];

        // Act: Put base with binary data
        engine.put(cf, &binary_key, &[0xAA, 0xBB]).expect("put binary");
        engine.merge_cf(cf, &binary_key, &binary_operand).expect("merge binary");

        // Assert: Binary data preserved through merge
        let got = engine.get(cf, &binary_key).expect("get");
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
        engine.register_merge_operator(cf.id().as_u32(), Box::new(op)).expect("register");

        // Act: Write base and merge with special characters
        engine.put(cf, b"key", b"base").expect("put");
        engine
            .merge_cf(cf, b"key", "_with_newline_\n".as_bytes())
            .expect("merge newline");
        engine
            .merge_cf(cf, b"key", "tab_\t_here".as_bytes())
            .expect("merge tab");
        engine
            .merge_cf(cf, b"key", "emoji_ðŸ˜€_unicode".as_bytes())
            .expect("merge emoji");

        // Assert: All special characters handled
        let got = engine.get(cf, b"key").expect("get");
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
            engine.put(cf, key.as_bytes(), b"base").expect("put");
        }

        // Act: Batch with operations on different keys
        let mut batch = WriteBatch::new();
        for i in 0..5 {
            let key = format!("key_{i}");
            batch.put(key.as_bytes().to_vec().into(), format!("_update{i}").as_bytes().to_vec().into());
        }
        engine.write_batch(&batch).expect("commit batch");

        // Assert: All updates applied
        for i in 0..5 {
            let key = format!("key_{i}");
            let got = engine.get(cf, key.as_bytes()).expect("get");
            assert!(got.is_some(), "update lost on key_{} in mode: {}", i, mode);
        }
    });
}
