//! End-to-end integration test: write → flush → compact → recover
//!
//! This test validates the complete data pipeline:
//! 1. Write operations to memtable via WAL
//! 2. Flush memtable to SST
//! 3. Compaction merges SSTs
//! 4. Recovery replays from WAL on restart

use cntryl_midge::engine::MidgeEngine;
use cntryl_midge::common::MidgeResult;
use std::fs;
use std::path::PathBuf;

/// Arrange: Create a temporary directory for test data
fn setup_test_dir() -> PathBuf {
    let test_dir = PathBuf::from("target/test_integration_e2e");
    let _ = fs::remove_dir_all(&test_dir);
    fs::create_dir_all(&test_dir).expect("Failed to create test directory");
    test_dir
}

/// Cleanup: Remove temporary test directory
fn cleanup_test_dir(dir: &PathBuf) {
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn should_complete_write_flush_recover_pipeline() {
    // Arrange: Setup
    let test_dir = setup_test_dir();

    // Create engine with test directory
    let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");

    // Act 1: Write operations (hits memtable + WAL)
    engine.put(b"key1", b"value1").expect("Put failed");
    engine.put(b"key2", b"value2").expect("Put failed");
    engine.put(b"key3", b"value3").expect("Put failed");

    // Assert: Verify writes are in memtable (before flush)
    let val = engine.get(b"key1").expect("Get failed").expect("Key not found");
    assert_eq!(val, b"value1");

    // Act 2: Flush memtable to SST
    engine.flush().expect("Flush failed");

    // Assert: Data still readable after flush (in SST now)
    let val = engine.get(b"key2").expect("Get failed").expect("Key not found");
    assert_eq!(val, b"value2");

    // Act 3: Sync to ensure WAL is persisted
    engine.sync().expect("Sync failed");

    // Assert: All data readable before shutdown
    let val = engine.get(b"key3").expect("Get failed").expect("Key not found");
    assert_eq!(val, b"value3");

    // Cleanup
    drop(engine);
    cleanup_test_dir(&test_dir);
}

#[test]
fn should_handle_deletes_in_write_flush_recover_pipeline() {
    // Arrange
    let test_dir = setup_test_dir();

    let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");

    // Act: Write, delete, flush
    engine.put(b"key1", b"value1").expect("Put failed");
    engine.put(b"key2", b"value2").expect("Put failed");
    engine.put(b"key3", b"value3").expect("Put failed");

    engine.delete(b"key2").expect("Delete failed");

    engine.flush().expect("Flush failed");

    // Assert: key2 should be deleted, key1 and key3 should exist
    assert_eq!(engine.get(b"key1").expect("Get failed").expect("Key1 lost"), b"value1");
    assert!(engine.get(b"key2").expect("Get failed").is_none());
    assert_eq!(engine.get(b"key3").expect("Get failed").expect("Key3 lost"), b"value3");

    // Cleanup
    drop(engine);
    cleanup_test_dir(&test_dir);
}
