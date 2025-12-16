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
use cntryl_midge::testkit::*;

// ============================================================================
// BASIC PUT/GET/DELETE OPERATIONS
// ============================================================================

#[test]
fn should_get_value_given_existing_key_when_put() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        // Act
        engine.put(cf, b"key", b"value").expect("put");

        // Assert
        let got = engine.get(cf, b"key").expect("get");
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
        let cf = engine.default_column_family();

        // Act
        let got = engine.get(cf, b"nonexistent").expect("get");

        // Assert
        assert_eq!(got, None, "expected None in mode: {}", mode);
    });
}

#[test]
fn should_overwrite_value_given_existing_key_when_put() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();
        engine.put(cf, b"key", b"value1").expect("put initial");

        // Act
        engine.put(cf, b"key", b"value2").expect("put overwrite");

        // Assert
        let got = engine.get(cf, b"key").expect("get");
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
        let cf = engine.default_column_family();

        // Act
        engine.put(cf, b"key", b"").expect("put empty");

        // Assert
        let got = engine.get(cf, b"key").expect("get empty");
        assert_eq!(got, Some(Bytes::new()), "failed in mode: {}", mode);
    });
}

#[test]
fn should_handle_binary_data_when_put() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();
        let data = vec![0, 1, 2, 3, 255, 254, 253];

        // Act
        engine.put(cf, b"binary_key", &data).expect("put binary");

        // Assert
        let got = engine.get(cf, b"binary_key").expect("get binary");
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
        let cf = engine.default_column_family();

        engine.put(cf, b"key", b"value").expect("put");

        // Act
        engine.delete(cf, b"key").expect("delete");

        // Assert
        let got = engine.get(cf, b"key").expect("get");
        assert_eq!(got, None, "expected None after delete in mode: {}", mode);
    });
}

#[test]
fn should_succeed_given_nonexistent_key_when_delete() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        // Act
        let result = engine.delete(cf, b"nonexistent");

        // Assert
        result.expect("delete nonexistent");
    });
}

#[test]
fn should_handle_many_operations_when_sequential() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();
        const COUNT: usize = 100;

        // Act
        for i in 0..COUNT {
            let key = format!("key_{i}");
            let val = format!("value_{i}");
            engine.put(cf, key.as_bytes(), val.as_bytes()).expect("put");
        }

        // Assert
        for i in 0..COUNT {
            let key = format!("key_{i}");
            let expected = format!("value_{i}");
            let got = engine.get(cf, key.as_bytes()).expect("get");

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
        let cf = engine.default_column_family();

        // Act: Perform various operations
        for i in 0..50 {
            let key = format!("artifact_test_{i}");
            engine.put(cf, key.as_bytes(), b"test_value").expect("put");
        }

        // Assert: All data is readable (operations succeeded)
        let got = engine.get(cf, b"artifact_test_0").expect("get");
        assert!(
            got.is_some(),
            "failed to retrieve written data in mode: {}",
            mode
        );
    });
}
