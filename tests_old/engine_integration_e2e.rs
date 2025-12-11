//! End-to-end integration tests: write â†’ flush â†’ operations
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
use std::sync::atomic::{AtomicU64, Ordering};

static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Arrange: Create a temporary directory for test data
fn setup_test_dir() -> PathBuf {
    let test_num = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let test_dir = PathBuf::from(format!("target/test_integration_e2e_{}", test_num));
    let _ = fs::remove_dir_all(&test_dir);
    fs::create_dir_all(&test_dir).expect("Failed to create test directory");
    test_dir
}

/// Cleanup: Remove temporary test directory
fn cleanup_test_dir(dir: &PathBuf) {
    std::thread::sleep(std::time::Duration::from_millis(50));
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn should_persist_writes_to_memtable() {
    // Arrange: Setup
    let test_dir = setup_test_dir();
    let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");

    // Act
    engine.put(b"key1", b"value1").expect("Put failed");
    engine.put(b"key2", b"value2").expect("Put failed");
    engine.put(b"key3", b"value3").expect("Put failed");

    // Assert: Verify writes are in memtable (before flush)
    let val = engine
        .get(b"key1")
        .expect("Get failed")
        .expect("Key not found");
    assert_eq!(val.to_vec(), b"value1".to_vec());

    // Cleanup
    drop(engine);
    cleanup_test_dir(&test_dir);
}

#[test]
fn should_persist_data_after_flush_to_sst() {
    // Arrange: Setup
    let test_dir = setup_test_dir();
    let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");
    engine.put(b"key2", b"value2").expect("Put failed");

    // Act: Flush memtable to SST
    engine.flush().expect("Flush failed");

    // Assert: Data still readable after flush (in SST now)
    let val = engine
        .get(b"key2")
        .expect("Get failed")
        .expect("Key not found");
    assert_eq!(val.to_vec(), b"value2".to_vec());

    // Cleanup
    drop(engine);
    cleanup_test_dir(&test_dir);
}

#[test]
fn should_persist_data_after_sync() {
    // Arrange: Setup
    let test_dir = setup_test_dir();
    let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");
    engine.put(b"key3", b"value3").expect("Put failed");

    // Act: Sync to ensure WAL is persisted
    engine.sync().expect("Sync failed");

    // Assert: All data readable after sync
    let val = engine
        .get(b"key3")
        .expect("Get failed")
        .expect("Key not found");
    assert_eq!(val.to_vec(), b"value3".to_vec());

    // Cleanup
    drop(engine);
    cleanup_test_dir(&test_dir);
}

#[test]
#[cfg_attr(target_os = "windows", ignore)]
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
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Assert: key2 should be deleted, key1 and key3 should exist
    assert_eq!(
        engine.get(b"key1").expect("Get failed").expect("Key1 lost"),
        b"value1"
    );
    assert!(engine.get(b"key2").expect("Get failed").is_none());
    assert_eq!(
        engine.get(b"key3").expect("Get failed").expect("Key3 lost"),
        b"value3"
    );

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
    assert_eq!(
        engine
            .get(b"batch_key1")
            .expect("Get failed")
            .expect("Key not found"),
        b"batch_value1"
    );
    assert_eq!(
        engine
            .get(b"batch_key2")
            .expect("Get failed")
            .expect("Key not found"),
        b"batch_value2"
    );

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
    assert_eq!(
        engine
            .get(b"sync_key")
            .expect("Get failed")
            .expect("Key not found"),
        b"sync_value"
    );

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

    engine
        .put(&large_key, &large_value)
        .expect("Large put failed");
    engine.flush().expect("Flush failed");

    // Assert: Large data readable
    let retrieved = engine
        .get(&large_key)
        .expect("Get failed")
        .expect("Key not found");
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
    let engine =
        std::sync::Arc::new(MidgeEngine::open(test_dir.clone()).expect("Failed to open engine"));

    // Act: Spawn threads writing concurrently
    let mut handles = vec![];
    for i in 0..4 {
        let engine_clone = engine.clone();
        let handle = std::thread::spawn(move || {
            for j in 0..25 {
                let key = format!("thread_{}_key_{}", i, j);
                let value = format!("thread_{}_value_{}", i, j);
                engine_clone
                    .put(key.as_bytes(), value.as_bytes())
                    .expect("Put failed");
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

#[test]
fn should_read_from_sst_after_flush() {
    // Arrange: Create engine and write data
    let test_dir = setup_test_dir();
    let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");

    // Act: Write data and flush to SST
    engine.put(b"sst_key1", b"sst_value1").expect("Put failed");
    engine.put(b"sst_key2", b"sst_value2").expect("Put failed");
    engine.put(b"sst_key3", b"sst_value3").expect("Put failed");

    engine.flush().expect("Flush failed");

    // Assert: All data readable from SST
    assert_eq!(
        engine
            .get(b"sst_key1")
            .expect("Get failed")
            .expect("Key1 not found"),
        b"sst_value1"
    );
    assert_eq!(
        engine
            .get(b"sst_key2")
            .expect("Get failed")
            .expect("Key2 not found"),
        b"sst_value2"
    );
    assert_eq!(
        engine
            .get(b"sst_key3")
            .expect("Get failed")
            .expect("Key3 not found"),
        b"sst_value3"
    );

    // Cleanup
    drop(engine);
    cleanup_test_dir(&test_dir);
}

#[test]
fn should_read_from_memtable_before_flush() {
    // Arrange: Create engine
    let test_dir = setup_test_dir();
    let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");

    // Act: Write data but don't flush
    engine.put(b"mem_key1", b"mem_value1").expect("Put failed");
    engine.put(b"mem_key2", b"mem_value2").expect("Put failed");
    engine.put(b"mem_key3", b"mem_value3").expect("Put failed");

    // Assert: Data readable from memtable
    assert_eq!(
        engine
            .get(b"mem_key1")
            .expect("Get failed")
            .expect("Key1 not found"),
        b"mem_value1"
    );
    assert_eq!(
        engine
            .get(b"mem_key2")
            .expect("Get failed")
            .expect("Key2 not found"),
        b"mem_value2"
    );
    assert_eq!(
        engine
            .get(b"mem_key3")
            .expect("Get failed")
            .expect("Key3 not found"),
        b"mem_value3"
    );

    // Cleanup
    drop(engine);
    cleanup_test_dir(&test_dir);
}

#[test]
fn should_return_none_for_nonexistent_keys() {
    // Arrange: Create engine
    let test_dir = setup_test_dir();
    let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");

    // Act: Try to read nonexistent key
    let result = engine.get(b"nonexistent").expect("Get failed");

    // Assert: Result is None
    assert!(result.is_none());

    // Cleanup
    drop(engine);
    cleanup_test_dir(&test_dir);
}

#[test]
fn should_read_deleted_keys_as_none() {
    // Arrange: Create engine
    let test_dir = setup_test_dir();
    let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");

    // Act: Write, delete, read
    engine
        .put(b"delete_key", b"delete_value")
        .expect("Put failed");
    engine.delete(b"delete_key").expect("Delete failed");

    // Assert: Deleted key returns None
    assert!(engine.get(b"delete_key").expect("Get failed").is_none());

    // Cleanup
    drop(engine);
    cleanup_test_dir(&test_dir);
}

#[test]
fn should_read_after_multiple_flushes() {
    // Arrange: Create engine
    let test_dir = setup_test_dir();
    let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");

    // Act: Write, flush, write, flush
    engine
        .put(b"flush1_key", b"flush1_value")
        .expect("Put failed");
    engine.flush().expect("Flush 1 failed");

    engine
        .put(b"flush2_key", b"flush2_value")
        .expect("Put failed");
    engine.flush().expect("Flush 2 failed");

    engine
        .put(b"flush3_key", b"flush3_value")
        .expect("Put failed");
    engine.flush().expect("Flush 3 failed");

    // Assert: All data from all flushes readable
    assert_eq!(
        engine
            .get(b"flush1_key")
            .expect("Get failed")
            .expect("Key1 not found"),
        b"flush1_value"
    );
    assert_eq!(
        engine
            .get(b"flush2_key")
            .expect("Get failed")
            .expect("Key2 not found"),
        b"flush2_value"
    );
    assert_eq!(
        engine
            .get(b"flush3_key")
            .expect("Get failed")
            .expect("Key3 not found"),
        b"flush3_value"
    );

    // Cleanup
    drop(engine);
    cleanup_test_dir(&test_dir);
}

#[test]
fn should_prefer_memtable_over_sst_for_recent_writes() {
    // Arrange: Create engine
    let test_dir = setup_test_dir();
    let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");

    // Act: Write key, flush to SST, then update in memtable
    engine
        .put(b"update_key", b"original_value")
        .expect("Put failed");
    engine.flush().expect("Flush failed");

    engine
        .put(b"update_key", b"updated_value")
        .expect("Put failed");

    // Assert: Get returns updated value (from memtable, not SST)
    assert_eq!(
        engine
            .get(b"update_key")
            .expect("Get failed")
            .expect("Key not found"),
        b"updated_value"
    );

    // Cleanup
    drop(engine);
    cleanup_test_dir(&test_dir);
}

#[test]
#[cfg_attr(target_os = "windows", ignore)]
fn should_handle_mixed_read_write_operations() {
    // Arrange: Create engine
    let test_dir = setup_test_dir();
    let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");

    // Act: Mixed operations
    engine.put(b"key1", b"value1").expect("Put failed");
    assert_eq!(
        engine
            .get(b"key1")
            .expect("Get failed")
            .expect("Key1 not found"),
        b"value1"
    );

    engine.put(b"key2", b"value2").expect("Put failed");
    engine.flush().expect("Flush failed");
    std::thread::sleep(std::time::Duration::from_millis(100));

    assert_eq!(
        engine
            .get(b"key2")
            .expect("Get failed")
            .expect("Key2 not found"),
        b"value2"
    );

    engine.put(b"key3", b"value3").expect("Put failed");
    engine.delete(b"key1").expect("Delete failed");

    assert!(engine.get(b"key1").expect("Get failed").is_none());
    assert_eq!(
        engine
            .get(b"key3")
            .expect("Get failed")
            .expect("Key3 not found"),
        b"value3"
    );

    engine.flush().expect("Flush failed");
    std::thread::sleep(std::time::Duration::from_millis(100));

    assert_eq!(
        engine
            .get(b"key2")
            .expect("Get failed")
            .expect("Key2 not found"),
        b"value2"
    );
    assert!(engine.get(b"key1").expect("Get failed").is_none());
    assert_eq!(
        engine
            .get(b"key3")
            .expect("Get failed")
            .expect("Key3 not found"),
        b"value3"
    );

    // Cleanup
    drop(engine);
    cleanup_test_dir(&test_dir);
}

// ============================================================================
// === Column Family Lifecycle Tests ===
// ============================================================================

#[test]
fn should_create_column_family_successfully() {
    // Arrange
    let test_dir = setup_test_dir();
    let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");

    // Act: Create a new column family
    let cf = engine
        .create_column_family("test_cf")
        .expect("Failed to create column family");

    // Assert: Verify CF was created with correct ID and name
    assert_eq!(cf.id().as_u32(), 1); // ID 0 is default, so first custom CF is 1
    assert_eq!(cf.name(), "test_cf");

    // Cleanup
    drop(engine);
    cleanup_test_dir(&test_dir);
}

#[test]
fn should_prevent_duplicate_column_family_creation() {
    // Arrange
    let test_dir = setup_test_dir();
    let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");

    // Act: Create a column family
    engine
        .create_column_family("dup_cf")
        .expect("Failed to create first CF");

    // Act: Try to create CF with same name (should fail)
    let result = engine.create_column_family("dup_cf");

    // Assert: Verify error on duplicate creation
    assert!(result.is_err(), "Should not allow duplicate CF name");

    // Cleanup
    drop(engine);
    cleanup_test_dir(&test_dir);
}

#[test]
fn should_create_multiple_column_families() {
    // Arrange
    let test_dir = setup_test_dir();
    let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");

    // Act: Create multiple column families
    let cf1 = engine
        .create_column_family("cf1")
        .expect("Failed to create cf1");
    let cf2 = engine
        .create_column_family("cf2")
        .expect("Failed to create cf2");
    let cf3 = engine
        .create_column_family("cf3")
        .expect("Failed to create cf3");

    // Assert: Verify IDs are sequential
    assert_eq!(cf1.id().as_u32(), 1);
    assert_eq!(cf2.id().as_u32(), 2);
    assert_eq!(cf3.id().as_u32(), 3);

    // Cleanup
    drop(engine);
    cleanup_test_dir(&test_dir);
}

#[test]
fn should_drop_column_family() {
    // Arrange
    let test_dir = setup_test_dir();
    let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");

    // Act: Create a CF
    let cf = engine
        .create_column_family("drop_cf")
        .expect("Failed to create CF");

    // Act: Drop the CF
    let result = engine.drop_column_family(cf.id());

    // Assert: Drop succeeds
    assert!(result.is_ok(), "Drop should succeed");

    // Cleanup
    drop(engine);
    cleanup_test_dir(&test_dir);
}

#[test]
fn should_error_on_drop_nonexistent_cf() {
    // Arrange
    let test_dir = setup_test_dir();
    let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");

    // Act: Try to drop a non-existent CF
    let result = engine.drop_column_family(cntryl_midge::engine::ColumnFamilyId(999));

    // Assert: Should error
    assert!(result.is_err(), "Should error on nonexistent CF");

    // Cleanup
    drop(engine);
    cleanup_test_dir(&test_dir);
}

#[test]
fn should_list_column_families() {
    // Arrange
    let test_dir = setup_test_dir();
    let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");

    // Act: Create some CFs
    engine
        .create_column_family("list_cf1")
        .expect("Failed to create CF1");
    engine
        .create_column_family("list_cf2")
        .expect("Failed to create CF2");

    // Act: List all CFs
    let cfs = engine
        .list_column_families()
        .expect("Failed to list column families");

    // Assert: At minimum, should have default CF
    assert!(cfs.len() >= 1, "Should have at least the default CF");
    assert_eq!(
        cfs[0].id(),
        cntryl_midge::engine::ColumnFamilyId::DEFAULT,
        "First CF should be default"
    );

    // Cleanup
    drop(engine);
    cleanup_test_dir(&test_dir);
}

#[test]
fn should_write_to_custom_column_family() {
    // Arrange
    let test_dir = setup_test_dir();
    let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");

    // Act: Create a CF and write to it
    let cf = engine
        .create_column_family("data_cf")
        .expect("Failed to create CF");

    engine
        .put_cf(&cf, b"cf_key", b"cf_value")
        .expect("Failed to put in CF");

    // Assert: Verify data is written to CF
    let value = engine
        .get_cf(&cf, b"cf_key")
        .expect("Failed to get from CF");
    assert_eq!(value, Some(b"cf_value".to_vec()));

    // Cleanup
    drop(engine);
    cleanup_test_dir(&test_dir);
}

#[test]
fn should_isolate_data_between_column_families() {
    // Arrange
    let test_dir = setup_test_dir();
    let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");

    // Act: Create two CFs
    let cf1 = engine
        .create_column_family("cf_isolated_1")
        .expect("Failed to create CF1");
    let cf2 = engine
        .create_column_family("cf_isolated_2")
        .expect("Failed to create CF2");

    // Act: Write different data to each CF
    engine
        .put_cf(&cf1, b"key", b"value_cf1")
        .expect("Failed to put in CF1");
    engine
        .put_cf(&cf2, b"key", b"value_cf2")
        .expect("Failed to put in CF2");

    // Assert: Verify data isolation
    assert_eq!(
        engine.get_cf(&cf1, b"key").expect("Get from CF1 failed"),
        Some(b"value_cf1".to_vec())
    );
    assert_eq!(
        engine.get_cf(&cf2, b"key").expect("Get from CF2 failed"),
        Some(b"value_cf2".to_vec())
    );

    // Cleanup
    drop(engine);
    cleanup_test_dir(&test_dir);
}

#[test]
fn should_read_custom_column_family_from_sst() {
    // Arrange
    let test_dir = setup_test_dir();
    let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");

    // Act: Create CF and write data
    let cf = engine
        .create_column_family("flush_cf")
        .expect("Failed to create CF");

    engine
        .put_cf(&cf, b"flush_key", b"flush_value")
        .expect("Failed to put in CF");

    // Act: Flush to SST
    engine.flush().expect("Flush failed");
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Assert: Verify read from SST
    let value = engine
        .get_cf(&cf, b"flush_key")
        .expect("Failed to get from CF");
    assert_eq!(
        value,
        Some(b"flush_value".to_vec()),
        "Should read from SST after flush"
    );

    // Cleanup
    drop(engine);
    cleanup_test_dir(&test_dir);
}
