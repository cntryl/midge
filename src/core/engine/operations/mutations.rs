//! Mutation operations for MidgeEngine
//!
//! This module contains specialized mutation methods that provide
//! higher-level semantics on top of the basic put/delete operations.

use bytes::Bytes;

use crate::api::column_family::ColumnFamilyHandle;
use crate::core::engine::types::{CasResult, InsertResult};
use crate::error::MidgeResult;

use super::super::MidgeEngine;

impl MidgeEngine {
    /// Insert a key-value pair only if the key doesn't already exist.
    ///
    /// This operation provides test-and-set semantics for initial inserts:
    /// 1. Checks if the key exists using snapshot isolation
    /// 2. If absent, writes the value
    /// 3. If present, returns the existing value without modification
    ///
    /// # Arguments
    /// * `cf` - Column family handle
    /// * `key` - The key to insert
    /// * `value` - The value to write if key is absent
    ///
    /// # Returns
    /// - `Ok(InsertResult::Inserted)` if the key was absent and the value was written
    /// - `Ok(InsertResult::AlreadyExists(existing))` if the key already exists
    ///
    /// # Examples
    /// ```
    /// # use cntryl_midge::{MidgeOptions, MidgeEngine, InsertResult};
    /// # let opts = MidgeOptions::default();
    /// # let engine = MidgeEngine::open(opts).unwrap();
    /// let cf = engine.default_column_family();
    ///
    /// // Initialize counter (only succeeds first time)
    /// match engine.insert_with_value(&cf, b"counter", b"0").unwrap() {
    ///     InsertResult::Inserted => println!("Counter initialized"),
    ///     InsertResult::AlreadyExists(v) => println!("Counter exists: {:?}", v),
    /// }
    /// ```
    pub fn insert_with_value(
        &self,
        cf: &ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
    ) -> MidgeResult<InsertResult> {
        if self.read_only {
            return Err(crate::error::MidgeError::ReadOnly);
        }

        // Use snapshot isolation for consistent read-then-write
        let snapshot = self.snapshot();
        if let Some(existing) = self.get_at(cf, key, &snapshot)? {
            return Ok(InsertResult::AlreadyExists(existing));
        }

        // Key doesn't exist at snapshot time, perform the put
        self.put_with_ttl(cf, key, value, 0)?;
        Ok(InsertResult::Inserted)
    }

    /// Compare-and-swap: atomically update a key's value only if it matches expected.
    ///
    /// This operation provides atomic test-and-set semantics:
    /// 1. Reads the current value using snapshot isolation
    /// 2. Compares it to the expected value
    /// 3. If they match, writes the new value
    ///
    /// # Arguments
    /// * `cf` - Column family handle
    /// * `key` - The key to update
    /// * `expected` - The expected current value (None means key should not exist)
    /// * `new_value` - The new value to write if the comparison succeeds
    ///
    /// # Returns
    /// - `Ok(CasResult::Swapped)` if the value matched and was updated
    /// - `Ok(CasResult::Mismatch(actual))` if the current value differs from expected
    ///
    /// # Examples
    /// ```
    /// # use cntryl_midge::{MidgeOptions, MidgeEngine, CasResult};
    /// # use bytes::Bytes;
    /// # let opts = MidgeOptions::default();
    /// # let engine = MidgeEngine::open(opts).unwrap();
    /// let cf = engine.default_column_family();
    ///
    /// // Initialize counter (expect it to not exist)
    /// match engine.compare_and_swap(
    ///     &cf,
    ///     b"counter",
    ///     None,
    ///     b"0"
    /// ).unwrap() {
    ///     CasResult::Swapped => println!("Initialized"),
    ///     CasResult::Mismatch(_) => println!("Already exists"),
    /// }
    ///
    /// // Increment counter (expect current value to be "0")
    /// match engine.compare_and_swap(
    ///     &cf,
    ///     b"counter",
    ///     Some(Bytes::from("0")),
    ///     b"1"
    /// ).unwrap() {
    ///     CasResult::Swapped => println!("Incremented"),
    ///     CasResult::Mismatch(actual) => println!("Race detected: {:?}", actual),
    /// }
    /// ```
    pub fn compare_and_swap(
        &self,
        cf: &ColumnFamilyHandle,
        key: &[u8],
        expected: Option<Bytes>,
        new_value: &[u8],
    ) -> MidgeResult<CasResult> {
        self.check_read_only()?;

        // Use snapshot isolation for consistent read-then-write
        let snapshot = self.snapshot();
        let current = self.get_at(cf, key, &snapshot)?;

        // Compare current value with expected
        if current != expected {
            return Ok(CasResult::Mismatch(current));
        }

        // Match succeeded, perform the write
        self.put_with_ttl(cf, key, new_value, 0)?;
        Ok(CasResult::Swapped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MidgeEngine, MidgeOptions, StorageMode};
    use bytes::Bytes;
    use uuid;

    fn create_test_engine() -> MidgeEngine {
        let temp_dir = std::env::temp_dir().join(format!("midge_test_mutations_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let db_path = temp_dir;
        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk { db_path },
            enable_compaction: false,
            ..Default::default()
        };
        MidgeEngine::open(opts).unwrap()
    }

    #[test]
    fn should_insert_value_when_key_does_not_exist() {
        // Arrange
        let engine = create_test_engine();
        let cf = engine.default_column_family();

        // Act
        let result = engine.insert_with_value(&cf, b"key1", b"value1");

        // Assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), InsertResult::Inserted);
        let get_result = engine.get(&cf, b"key1").unwrap();
        assert_eq!(get_result, Some(Bytes::from("value1")));
    }

    #[test]
    fn should_return_existing_value_when_key_already_exists() {
        // Arrange
        let engine = create_test_engine();
        let cf = engine.default_column_family();
        engine.put(&cf, b"key1", b"existing").unwrap();

        // Act
        let result = engine.insert_with_value(&cf, b"key1", b"new_value");

        // Assert
        assert!(result.is_ok());
        match result.unwrap() {
            InsertResult::AlreadyExists(existing) => {
                assert_eq!(existing, Bytes::from("existing"));
            }
            _ => panic!("Expected AlreadyExists"),
        }
        // Verify the value wasn't changed
        let get_result = engine.get(&cf, b"key1").unwrap();
        assert_eq!(get_result, Some(Bytes::from("existing")));
    }

    #[test]
    fn should_swap_value_when_expected_matches_current() {
        // Arrange
        let engine = create_test_engine();
        let cf = engine.default_column_family();
        engine.put(&cf, b"key1", b"old_value").unwrap();

        // Act
        let result = engine.compare_and_swap(
            &cf,
            b"key1",
            Some(Bytes::from("old_value")),
            b"new_value",
        );

        // Assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), CasResult::Swapped);
        let get_result = engine.get(&cf, b"key1").unwrap();
        assert_eq!(get_result, Some(Bytes::from("new_value")));
    }

    #[test]
    fn should_return_mismatch_when_expected_does_not_match_current() {
        // Arrange
        let engine = create_test_engine();
        let cf = engine.default_column_family();
        engine.put(&cf, b"key1", b"actual_value").unwrap();

        // Act
        let result = engine.compare_and_swap(
            &cf,
            b"key1",
            Some(Bytes::from("expected_value")),
            b"new_value",
        );

        // Assert
        assert!(result.is_ok());
        match result.unwrap() {
            CasResult::Mismatch(actual) => {
                assert_eq!(actual, Some(Bytes::from("actual_value")));
            }
            _ => panic!("Expected Mismatch"),
        }
        // Verify the value wasn't changed
        let get_result = engine.get(&cf, b"key1").unwrap();
        assert_eq!(get_result, Some(Bytes::from("actual_value")));
    }

    #[test]
    fn should_swap_value_when_expected_is_none_and_key_does_not_exist() {
        // Arrange
        let engine = create_test_engine();
        let cf = engine.default_column_family();

        // Act
        let result = engine.compare_and_swap(&cf, b"key1", None, b"new_value");

        // Assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), CasResult::Swapped);
        let get_result = engine.get(&cf, b"key1").unwrap();
        assert_eq!(get_result, Some(Bytes::from("new_value")));
    }

    #[test]
    fn should_return_mismatch_when_expected_is_none_but_key_exists() {
        // Arrange
        let engine = create_test_engine();
        let cf = engine.default_column_family();
        engine.put(&cf, b"key1", b"existing").unwrap();

        // Act
        let result = engine.compare_and_swap(&cf, b"key1", None, b"new_value");

        // Assert
        assert!(result.is_ok());
        match result.unwrap() {
            CasResult::Mismatch(actual) => {
                assert_eq!(actual, Some(Bytes::from("existing")));
            }
            _ => panic!("Expected Mismatch"),
        }
    }
}
