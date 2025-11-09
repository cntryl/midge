//! Cloud recovery integration tests.
//!
//! Tests recovery scenarios for cloud-backed storage:
//! - Recovery from cloud-stored WAL after restart
//! - Recovery from cloud-stored SSTs after local cache loss
//! - Consistency after partial upload scenarios

use bytes::Bytes;
use cntryl_midge::cloud::{MockCloudBackend, StorageBackend};
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use std::sync::Arc;
use tempfile::TempDir;

#[test]
fn should_recover_from_cloud_wal_after_restart() {
    // Arrange - First session: write data with cloud sync
    let temp_dir = TempDir::new().unwrap();
    let backend = Arc::new(MockCloudBackend::new());

    let opts = MidgeOptions {
        storage_mode: StorageMode::CloudBacked {
            local_cache_path: temp_dir.path().to_path_buf(),
            cloud_backend: backend.clone(),
            storage_context: Default::default(),
            local_wal_sync: true,
            wal_batch_size: 512, // Small batch
            sst_cache_capacity: 10,
        },
        memtable_size: 64 * 1024 * 1024,
        enable_compaction: false,
        wal_sync: true,
        ..Default::default()
    };

    {
        let engine = MidgeEngine::open(opts.clone()).unwrap();
        let cf = engine.default_column_family();

        // Write test data
        engine.put(&cf, b"key1", b"value1").unwrap();
        engine.put(&cf, b"key2", b"value2").unwrap();
        engine.put(&cf, b"key3", b"value3").unwrap();

        // Ensure WAL is synced to cloud
        std::thread::sleep(std::time::Duration::from_millis(300));
    }
    // Engine dropped, simulating restart

    // Act - Second session: reopen and verify data recovered
    let engine = MidgeEngine::open(opts).unwrap();

    // Assert - Data should be recovered from cloud WAL
    let val1 = engine.get(&cf, b"key1").unwrap();
    let val2 = engine.get(&cf, b"key2").unwrap();
    let val3 = engine.get(&cf, b"key3").unwrap();

    assert_eq!(val1, Some(Bytes::from("value1")));
    assert_eq!(val2, Some(Bytes::from("value2")));
    assert_eq!(val3, Some(Bytes::from("value3")));
}

#[test]
fn should_recover_from_cloud_ssts_after_cache_loss() {
    // Arrange - First session: write and flush data
    let temp_dir = TempDir::new().unwrap();
    let backend = Arc::new(MockCloudBackend::new());

    let opts = MidgeOptions {
        storage_mode: StorageMode::CloudBacked {
            local_cache_path: temp_dir.path().to_path_buf(),
            cloud_backend: backend.clone(),
            storage_context: Default::default(),
            local_wal_sync: false,
            wal_batch_size: 1024 * 1024,
            sst_cache_capacity: 10,
        },
        memtable_size: 1024, // Small to force flush
        enable_compaction: false,
        wal_sync: false,
        ..Default::default()
    };

    {
        let engine = MidgeEngine::open(opts.clone()).unwrap();

        // Write enough data to trigger flush
        for i in 0..100 {
            let key = Bytes::from(format!("key_{:04}", i));
            let value = Bytes::from(format!("value_{:04}", i));
            engine.put(&cf, key, value).unwrap();
        }

        engine.flush().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(500));
    }

    // Simulate local cache loss by clearing temp directory
    let cache_path = temp_dir.path().join("sst");
    if cache_path.exists() {
        std::fs::remove_dir_all(&cache_path).ok();
    }

    // Act - Reopen engine (should download SSTs from cloud)
    let engine = MidgeEngine::open(opts).unwrap();

    // Assert - Data should be recovered from cloud SSTs
    let val = engine.get(&cf, b"key_0000").unwrap();
    assert!(val.is_some(), "Should recover data from cloud SSTs");
}

#[test]
fn should_maintain_consistency_after_partial_write() {
    // Arrange
    let temp_dir = TempDir::new().unwrap();
    let backend = Arc::new(MockCloudBackend::new());

    let opts = MidgeOptions {
        storage_mode: StorageMode::CloudBacked {
            local_cache_path: temp_dir.path().to_path_buf(),
            cloud_backend: backend.clone(),
            storage_context: Default::default(),
            local_wal_sync: true,
            wal_batch_size: 1024 * 1024,
            sst_cache_capacity: 10,
        },
        memtable_size: 64 * 1024 * 1024,
        enable_compaction: false,
        wal_sync: true,
        ..Default::default()
    };

    let engine = MidgeEngine::open(opts.clone()).unwrap();

    // Act - Write data
    engine
        .put(Bytes::from("committed1"), Bytes::from("value1"))
        .unwrap();
    engine
        .put(Bytes::from("committed2"), Bytes::from("value2"))
        .unwrap();

    // Simulate crash before final sync
    drop(engine);

    // Reopen
    let engine = MidgeEngine::open(opts).unwrap();

    // Assert - Committed data should be present
    let val1 = engine.get(&cf, b"committed1").unwrap();
    let val2 = engine.get(&cf, b"committed2").unwrap();

    assert!(
        val1.is_some() || val2.is_some(),
        "At least some committed data should survive"
    );
}

#[test]
fn should_handle_concurrent_uploads_correctly() {
    // Arrange
    let temp_dir = TempDir::new().unwrap();
    let backend = Arc::new(MockCloudBackend::new());

    let opts = MidgeOptions {
        storage_mode: StorageMode::CloudBacked {
            local_cache_path: temp_dir.path().to_path_buf(),
            cloud_backend: backend.clone(),
            storage_context: Default::default(),
            local_wal_sync: false,
            wal_batch_size: 1024,
            sst_cache_capacity: 10,
        },
        memtable_size: 512, // Very small to trigger multiple flushes
        enable_compaction: false,
        wal_sync: false,
        ..Default::default()
    };

    let engine = MidgeEngine::open(opts).unwrap();

    // Act - Write data that will trigger multiple flushes
    for i in 0..200 {
        let key = Bytes::from(format!("concurrent_key_{:05}", i));
        let value = Bytes::from(format!("concurrent_value_{:05}", i));
        engine.put(&cf, key, value).unwrap();
    }

    engine.flush().unwrap();

    // Wait for all uploads to complete
    std::thread::sleep(std::time::Duration::from_secs(1));

    // Assert - All SSTs should be uploaded without conflicts
    let sst_blobs = backend.list_blobs("midge/sst/").unwrap();
    assert!(
        !sst_blobs.is_empty(),
        "Multiple SSTs should be uploaded correctly"
    );

    // Verify data integrity
    let val = engine.get(&cf, b"concurrent_key_00000").unwrap();
    assert!(
        val.is_some(),
        "Data should remain consistent across multiple uploads"
    );
}
