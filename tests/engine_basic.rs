//! Core KV Engine Integration Tests
//!
//! Tests the basic put/get/delete operations end-to-end using the public
//! MidgeEngine API. These tests are **storage-mode invariant**: every supported
//! backend (Memory, FS, Cloud) must pass with identical behavior.
//!
//! Naming convention:
//!   should_<behavior>_given_<context>_when_<condition>
//!
//! These tests run across all storage modes.

use bytes::Bytes;
mod common;
use cntryl_midge::{TransactionMode, WriteOptions};
use common::*;

// ============================================================================
// BASIC PUT/GET/DELETE OPERATIONS
// ============================================================================

#[test]
fn should_get_value_given_existing_key_when_put() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin_tx");
        tx.put(b"key".to_vec(), b"value".to_vec(), None)
            .expect("put");
        tx.commit(WriteOptions::buffered()).expect("commit");

        // Assert
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin_tx");
        let got = tx.get(b"key").expect("get");
        assert_eq!(
            got,
            Some(Bytes::from_static(b"value")),
            "unexpected value in mode: {}",
            mode
        );
    });
}

#[test]
fn should_return_none_given_nonexistent_key_when_get() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin_tx");
        let got = tx.get(b"nonexistent").expect("get");

        // Assert
        assert_eq!(got, None, "expected None in mode: {}", mode);
    });
}

#[test]
fn should_overwrite_value_given_existing_key_when_put() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin_tx");
        tx.put(b"key".to_vec(), b"value1".to_vec(), None)
            .expect("put initial");
        tx.commit(WriteOptions::buffered()).expect("commit");

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin_tx");
        tx.put(b"key".to_vec(), b"value2".to_vec(), None)
            .expect("put overwrite");
        tx.commit(WriteOptions::buffered()).expect("commit");

        // Assert
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin_tx");
        let got = tx.get(b"key").expect("get");
        assert_eq!(
            got,
            Some(Bytes::from_static(b"value2")),
            "incorrect overwrite behavior in mode: {}",
            mode
        );
    });
}

#[test]
fn should_handle_empty_value_when_put() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin_tx");
        tx.put(b"key".to_vec(), b"".to_vec(), None)
            .expect("put empty");
        tx.commit(WriteOptions::buffered()).expect("commit");

        // Assert
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin_tx");
        let got = tx.get(b"key").expect("get empty");
        assert_eq!(got, Some(Bytes::new()), "failed in mode: {}", mode);
    });
}

#[test]
fn should_handle_binary_data_when_put() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");
        let data = vec![0, 1, 2, 3, 255, 254, 253];

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin_tx");
        tx.put(b"binary_key".to_vec(), data.clone(), None)
            .expect("put binary");
        tx.commit(WriteOptions::buffered()).expect("commit");

        // Assert
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin_tx");
        let got = tx.get(b"binary_key").expect("get binary");
        assert_eq!(
            got,
            Some(Bytes::from(data)),
            "binary mismatch in mode: {}",
            mode
        );
    });
}

#[test]
fn should_return_none_given_deleted_key_when_get() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin_tx");
        tx.put(b"key".to_vec(), b"value".to_vec(), None)
            .expect("put");
        tx.commit(WriteOptions::buffered()).expect("commit");

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin_tx");
        tx.delete(b"key".to_vec()).expect("delete");
        tx.commit(WriteOptions::buffered()).expect("commit");

        // Assert
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin_tx");
        let got = tx.get(b"key").expect("get");
        assert_eq!(got, None, "expected None after delete in mode: {}", mode);
    });
}

#[test]
fn should_succeed_given_nonexistent_key_when_delete() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin_tx");
        tx.delete(b"nonexistent".to_vec()).expect("delete");
        let result = tx.commit(WriteOptions::buffered());

        // Assert
        result.expect("delete nonexistent");
    });
}

#[test]
fn should_handle_many_operations_when_sequential() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");
        const COUNT: usize = 100;

        // Act
        for i in 0..COUNT {
            let key = format!("key_{i}");
            let val = format!("value_{i}");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(key.as_bytes().to_vec(), val.as_bytes().to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::buffered()).expect("commit");
        }

        // Assert
        for i in 0..COUNT {
            let key = format!("key_{i}");
            let expected = format!("value_{i}");
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            let got = tx.get(key.as_bytes()).expect("get");

            assert_eq!(
                got,
                Some(Bytes::from(expected)),
                "mismatch for key {} in mode: {}",
                key,
                mode
            );
        }
    });
}

#[test]
fn should_retrieve_written_data_across_storage_modes() {
    // Validate that data written is retrievable across all storage modes.
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange: Open engine and write data
        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Act: Perform various operations
        for i in 0..50 {
            let key = format!("artifact_test_{i}");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(key.as_bytes().to_vec(), b"test_value".to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::buffered()).expect("commit");
        }

        // Assert: All data is readable (operations succeeded)
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin_tx");
        let got = tx.get(b"artifact_test_0").expect("get");
        assert!(
            got.is_some(),
            "failed to retrieve written data in mode: {}",
            mode
        );
    });
}
