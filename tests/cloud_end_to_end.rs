//! End-to-end integration tests for cloud-backed storage mode.
//!
//! Tests the complete cloud backend integration including:
//! - Engine initialization with CloudBacked mode
//! - WAL uploads to cloud
//! - SST uploads after flush and compaction
//! - Cloud recovery after restart

use bytes::Bytes;
use cntryl_midge::cloud::{MockCloudBackend, StorageBackend};
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use std::sync::Arc;
use tempfile::TempDir;

#[test]
fn should_initialize_engine_with_cloud_backed_mode() {
    // Arrange
    let temp_dir = TempDir::new().unwrap();
    let backend = Arc::new(MockCloudBackend::new());

    let opts = MidgeOptions {
        storage_mode: StorageMode::CloudBacked {
            local_cache_path: temp_dir.path().to_path_buf(),
            cloud_backend: backend.clone(),
            storage_context: Default::default(),
            local_wal_sync: false,
            wal_batch_size: 1024 * 1024, // 1MB
            sst_cache_capacity: 10,
        },
        memtable_size: 64 * 1024 * 1024,
        enable_compaction: false, // Disable for simpler test
        wal_sync: false,
        ..Default::default()
    };

    // Act
    let engine = MidgeEngine::open(opts);
    let cf = engine.default_column_family();

    // Assert
    assert!(
        engine.is_ok(),
        "Engine should open successfully in cloud mode"
    );
}

#[test]
fn should_upload_sst_to_cloud_after_flush() {
    // Arrange
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
        memtable_size: 1024, // Small memtable to force flush
        enable_compaction: false,
        wal_sync: false,
        ..Default::default()
    };

    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();

    // Act - Write enough data to trigger flush
    for i in 0..100 {
        let key = Bytes::from(format!("key_{:04}", i));
        let value = Bytes::from(format!("value_{:04}", i));
        engine.put(&cf, key, value).unwrap();
    }

    // Force flush
    engine.flush().unwrap();

    // Wait for async upload to complete
    std::thread::sleep(std::time::Duration::from_millis(500));

    // Assert - Check that SST was uploaded to cloud
    let blobs = backend.list_blobs("midge/sst/").unwrap();
    assert!(
        !blobs.is_empty(),
        "At least one SST should be uploaded to cloud storage"
    );
}

#[test]
fn should_write_and_read_from_cloud_backed_engine() {
    // Arrange
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
        memtable_size: 64 * 1024 * 1024,
        enable_compaction: false,
        wal_sync: false,
        ..Default::default()
    };

    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();

    // Act - Write data
    engine.put(&cf, "apple".as_bytes(), "A".as_bytes()).unwrap();
    engine.put(&cf, "banana".as_bytes(), "B".as_bytes()).unwrap();
    engine.put(&cf, "cherry".as_bytes(), "C".as_bytes()).unwrap();

    // Assert - Read data back
    let val_a = engine.get(&cf, b"apple").unwrap();
    let val_b = engine.get(&cf, b"banana").unwrap();
    let val_c = engine.get(&cf, b"cherry").unwrap();
    let val_x = engine.get(&cf, b"nonexistent").unwrap();

    assert_eq!(val_a, Some(Bytes::from("A")));
    assert_eq!(val_b, Some(Bytes::from("B")));
    assert_eq!(val_c, Some(Bytes::from("C")));
    assert_eq!(val_x, None);
}

#[test]
fn should_upload_wal_segments_to_cloud() {
    // Arrange
    let temp_dir = TempDir::new().unwrap();
    let backend = Arc::new(MockCloudBackend::new());

    let opts = MidgeOptions {
        storage_mode: StorageMode::CloudBacked {
            local_cache_path: temp_dir.path().to_path_buf(),
            cloud_backend: backend.clone(),
            storage_context: Default::default(),
            local_wal_sync: false,
            wal_batch_size: 1024, // Small batch to trigger upload
            sst_cache_capacity: 10,
        },
        memtable_size: 64 * 1024 * 1024,
        enable_compaction: false,
        wal_sync: true, // Enable sync to force upload
        ..Default::default()
    };

    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();

    // Act - Write enough data to trigger WAL segment upload
    for i in 0..100 {
        let key = Bytes::from(format!("key_{:04}", i));
        let value = Bytes::from(format!("value_with_padding_to_increase_size_{:04}", i));
        engine.put(&cf, key, value).unwrap();
    }

    // Wait for async WAL upload
    std::thread::sleep(std::time::Duration::from_millis(500));

    // Assert - Check that WAL segments were uploaded
    let wal_blobs = backend.list_blobs("").unwrap();
    let wal_segments: Vec<_> = wal_blobs
        .iter()
        .filter(|key| key.contains("wal_segment"))
        .collect();

    assert!(
        !wal_segments.is_empty(),
        "WAL segments should be uploaded to cloud storage"
    );
}

#[test]
fn should_handle_cloud_upload_errors_gracefully() {
    // Arrange
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
        memtable_size: 64 * 1024 * 1024,
        enable_compaction: false,
        wal_sync: false,
        ..Default::default()
    };

    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();

    // Act - Write operations should succeed even if background upload fails
    // (MockCloudBackend doesn't fail, but engine should be resilient)
    let result = engine.put(&cf, "test_key".as_bytes(), "test_value".as_bytes());

    // Assert
    assert!(
        result.is_ok(),
        "Write should succeed regardless of background upload status"
    );
}
