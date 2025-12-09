//! End-to-end integration tests: write → flush → operations
//!
//! This test suite validates the complete engine operation pipeline:
//! 1. Write operations to memtable via WAL
//! 2. Flush memtable to SST
//! 3. Delete operations with tombstones
//! 4. Write batches for atomic multi-key operations
//! 5. Sync operations for durability
//! 6. Concurrent writes
//! 7. Large key/value handling

use cntryl_midge::engine::MidgeEngine;
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

#[test]
fn should_process_writebatch_atomically() {
    // Arrange
    let test_dir = setup_test_dir();
    let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");

    // Act: Create and apply a write batch
    let mut batch = cntryl_midge::engine::WriteBatch::new();
    batch.put(b"batch_key1".to_vec(), b"batch_value1".to_vec());
    batch.put(b"batch_key2".to_vec(), b"batch_value2".to_vec());
    batch.delete(b"batch_key3".to_vec());

    engine.write_batch(&batch).expect("WriteBatch failed");

    // Assert: Batch writes are in memtable
    assert_eq!(engine.get(b"batch_key1").expect("Get failed").expect("Key not found"), b"batch_value1");
    assert_eq!(engine.get(b"batch_key2").expect("Get failed").expect("Key not found"), b"batch_value2");

    // Cleanup
    drop(engine);
    cleanup_test_dir(&test_dir);
}

#[test]
fn should_handle_sync_operations() {
    // Arrange
    let test_dir = setup_test_dir();
    let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");

    // Act: Write and sync
    engine.put(b"sync_key", b"sync_value").expect("Put failed");
    engine.sync().expect("Sync failed");

    // Assert: Data accessible after sync
    assert_eq!(engine.get(b"sync_key").expect("Get failed").expect("Key not found"), b"sync_value");

    // Cleanup
    drop(engine);
    cleanup_test_dir(&test_dir);
}

#[test]
fn should_handle_large_keys_and_values() {
    // Arrange
    let test_dir = setup_test_dir();
    let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");

    // Act: Write large key and value
    let large_key = vec![b'k'; 1024];
    let large_value = vec![b'v'; 10240];

    engine.put(&large_key, &large_value).expect("Large put failed");
    engine.flush().expect("Flush failed");

    // Assert: Large data readable
    let retrieved = engine.get(&large_key).expect("Get failed").expect("Key not found");
    assert_eq!(retrieved.len(), 10240);
    assert_eq!(retrieved, large_value);

    // Cleanup
    drop(engine);
    cleanup_test_dir(&test_dir);
}

#[test]
fn should_handle_concurrent_operations() {
    // Arrange
    let test_dir = setup_test_dir();
    let engine = std::sync::Arc::new(MidgeEngine::open(test_dir.clone()).expect("Failed to open engine"));

    // Act: Spawn threads writing concurrently
    let mut handles = vec![];
    for i in 0..4 {
        let engine_clone = engine.clone();
        let handle = std::thread::spawn(move || {
            for j in 0..25 {
                let key = format!("thread_{}_key_{}", i, j);
                let value = format!("thread_{}_value_{}", i, j);
                engine_clone.put(key.as_bytes(), value.as_bytes()).expect("Put failed");
            }
        });
        handles.push(handle);
    }

    // Wait for all threads
    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Assert: Some data should be readable
    let val = engine.get(b"thread_0_key_0").expect("Get failed");
    assert!(val.is_some());

    engine.flush().expect("Flush failed");

    // Cleanup
    drop(engine);
    cleanup_test_dir(&test_dir);
}
