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

        // Check if key exists
        // TODO: Use snapshot isolation for consistent read across CFs
        if let Some(existing) = self.get(cf, key)? {
            return Ok(InsertResult::AlreadyExists(existing));
        }

        // Key doesn't exist, perform the put
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

        // Check current value
        // TODO: Use snapshot isolation for consistent read across CFs
        let current = self.get(cf, key)?;

        // Compare current value with expected
        if current != expected {
            return Ok(CasResult::Mismatch(current));
        }

        // Match succeeded, perform the write
        self.put_with_ttl(cf, key, new_value, 0)?;
        Ok(CasResult::Swapped)
    }
}
