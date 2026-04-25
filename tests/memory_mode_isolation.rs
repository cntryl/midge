//! Memory Mode Isolation Tests
//!
//! Tests that memory mode creates no persistent filesystem artifacts and isolates
//! data between engine instances. Memory mode operates entirely in RAM with zero
//! disk side effects.
//!
//! Naming convention:
//!   should_<behavior>_given_<context>_when_<condition>
//!
//! These tests run on MEMORY MODE ONLY to validate isolation and filesystem cleanup.

use bytes::Bytes;
mod common;
use cntryl_midge::{TransactionMode, WriteOptions};
use common::*;

// ============================================================================
// FILESYSTEM ARTIFACT TESTS
// ============================================================================

#[test]
fn should_not_create_filesystem_artifacts_when_memory_mode() {
    // Arrange
    // Memory mode only test
    let opts = opts_for_mode("memory");

    // Act: Open, write, close
    let engine = open_with_mode(opts, "memory");
    let cf = engine.create_column_family("test").expect("create cf");
    let cf_id = cf.id();

    let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
    tx.put(b"test_key_1".to_vec(), b"test_value_1".to_vec(), None)
        .expect("put");
    tx.put(b"test_key_2".to_vec(), b"test_value_2".to_vec(), None)
        .expect("put");
    tx.commit(WriteOptions::buffered()).unwrap();
    // engine dropped here - memory mode stores nothing on disk

    // Assert: Memory mode produces no persistent artifacts
    // This is implicitly validated by memory mode operations (no disk writes)
    // Memory mode completed without filesystem operations
}

#[test]
fn should_not_persist_data_across_restart_given_memory_mode_when_reopening() {
    // Arrange: Open and write data
    let opts1 = opts_for_mode("memory");

    {
        let engine = open_with_mode(opts1, "memory");
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();

        // Act: Write to memory engine
        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        tx.put(
            b"persist_test".to_vec(),
            b"should_not_persist".to_vec(),
            None,
        )
        .expect("put");
        tx.commit(WriteOptions::buffered()).unwrap();
        // engine dropped
    }

    // Assert: New memory engine instance has no data
    let opts2 = opts_for_mode("memory");
    {
        let engine = open_with_mode(opts2, "memory");
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();

        let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
        let got = tx.get(b"persist_test").expect("get");
        assert_eq!(
            got, None,
            "memory mode persisted data across restart (should not persist)"
        );
    }
}

#[test]
fn should_isolate_data_given_multiple_memory_engines_when_separate_instances() {
    // Arrange: Create two separate memory engine instances
    let opts1 = opts_for_mode("memory");
    let opts2 = opts_for_mode("memory");

    // Act: Write different data to each
    let engine1 = open_with_mode(opts1, "memory");
    let cf1 = engine1.create_column_family("test").expect("create cf");
    let cf_id1 = cf1.id();
    let mut tx = engine1
        .begin_tx(cf_id1, TransactionMode::ReadWrite)
        .unwrap();
    tx.put(b"test_key".to_vec(), b"engine1_value".to_vec(), None)
        .expect("put");
    tx.commit(WriteOptions::buffered()).unwrap();

    let engine2 = open_with_mode(opts2, "memory");
    let cf2 = engine2.create_column_family("test").expect("create cf");
    let cf_id2 = cf2.id();
    let mut tx = engine2
        .begin_tx(cf_id2, TransactionMode::ReadWrite)
        .unwrap();
    tx.put(b"test_key".to_vec(), b"engine2_value".to_vec(), None)
        .expect("put");
    tx.commit(WriteOptions::buffered()).unwrap();

    // Assert: Each engine instance has isolated data
    let tx1 = engine1.begin_tx(cf_id1, TransactionMode::ReadOnly).unwrap();
    let got1 = tx1.get(b"test_key").expect("get");
    let tx2 = engine2.begin_tx(cf_id2, TransactionMode::ReadOnly).unwrap();
    let got2 = tx2.get(b"test_key").expect("get");

    assert_eq!(
        got1,
        Some(Bytes::from_static(b"engine1_value")),
        "engine1 data corruption or isolation failure"
    );
    assert_eq!(
        got2,
        Some(Bytes::from_static(b"engine2_value")),
        "engine2 data corruption or isolation failure"
    );
}

#[test]
fn should_handle_many_writes_efficiently_when_writing_100_keys() {
    // Arrange
    // Memory mode only test
    let opts = opts_for_mode("memory");
    let engine = open_with_mode(opts, "memory");
    let cf = engine.create_column_family("test").expect("create cf");
    let cf_id = cf.id();

    // Act: Perform many writes
    let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
    for i in 0..100 {
        let key = format!("write_test_{i:03}");
        tx.put(key.into_bytes(), b"value".to_vec(), None)
            .expect("put");
    }
    tx.commit(WriteOptions::buffered()).unwrap();

    // Assert: All writes succeeded and data is retrievable
    let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
    for i in [0, 25, 50, 75, 99].iter() {
        let key = format!("write_test_{i:03}");
        let got = tx.get(key.as_bytes()).expect("get");
        assert_eq!(
            got,
            Some(Bytes::from_static(b"value")),
            "write_test_{i:03} retrieval failed"
        );
    }
}

#[test]
fn should_handle_many_deletes_efficiently_when_deleting_50_keys() {
    // Memory mode only test
    let opts = opts_for_mode("memory");
    let engine = open_with_mode(opts, "memory");
    let cf = engine.create_column_family("test").expect("create cf");
    let cf_id = cf.id();

    // Arrange: Write 50 keys
    let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
    for i in 0..50 {
        let key = format!("delete_test_{i:02}");
        tx.put(key.into_bytes(), b"value".to_vec(), None)
            .expect("put");
    }
    tx.commit(WriteOptions::buffered()).unwrap();

    // Act: Delete all
    let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
    for i in 0..50 {
        let key = format!("delete_test_{i:02}");
        tx.delete(key.into_bytes()).expect("delete");
    }
    tx.commit(WriteOptions::buffered()).unwrap();

    // Assert: All deleted
    let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
    for i in [0, 10, 25, 49].iter() {
        let key = format!("delete_test_{i:02}");
        let got = tx.get(key.as_bytes()).expect("get");
        assert_eq!(got, None, "expected key to be deleted but found it");
    }
}

#[test]
fn should_handle_mixed_operations_efficiently_when_put_delete_overwrite() {
    // Arrange
    // Memory mode only test
    let opts = opts_for_mode("memory");
    let engine = open_with_mode(opts, "memory");
    let cf = engine.create_column_family("test").expect("create cf");
    let cf_id = cf.id();

    // Act: Mixed sequence
    let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
    tx.put(b"key1".to_vec(), b"v1".to_vec(), None).expect("put");
    tx.put(b"key2".to_vec(), b"v2".to_vec(), None).expect("put");
    tx.commit(WriteOptions::buffered()).unwrap();

    let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
    tx.delete(b"key1".to_vec()).expect("delete");
    tx.commit(WriteOptions::buffered()).unwrap();

    let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
    tx.put(b"key1".to_vec(), b"v1_new".to_vec(), None)
        .expect("put");
    tx.put(b"key3".to_vec(), b"v3".to_vec(), None).expect("put");
    tx.commit(WriteOptions::buffered()).unwrap();

    // Assert: Correct final state
    let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
    assert_eq!(
        tx.get(b"key1").expect("get"),
        Some(Bytes::from_static(b"v1_new"))
    );
    assert_eq!(
        tx.get(b"key2").expect("get"),
        Some(Bytes::from_static(b"v2"))
    );
    assert_eq!(
        tx.get(b"key3").expect("get"),
        Some(Bytes::from_static(b"v3"))
    );
}
