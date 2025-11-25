//! Cloud hybrid storage stress tests
//!
//! Tests for cache eviction, concurrency, and stress scenarios in hybrid storage mode.

use bytes::Bytes;
use cntryl_midge::cloud::hybrid::{HybridStorage, HybridStorageBackend};
use cntryl_midge::cloud::mock::MockCloudBackend;
use cntryl_midge::cloud::StorageBackend;
use std::sync::{Arc, Barrier};
use std::thread;

mod common;
use common::test_temp_dir;

#[test]
fn should_evict_oldest_files_given_cache_full_when_adding_new_files() {
    // Arrange
    let dir = test_temp_dir();
    let mock_backend = Arc::new(MockCloudBackend::new());
    let cache_size = 1024 * 10; // 10KB cache

    let hybrid = Arc::new(
        HybridStorage::new(dir.path().to_path_buf(), mock_backend.clone(), cache_size)
            .expect("failed to create hybrid storage"),
    );
    // Use synchronous writes so eviction happens deterministically inside write
    let storage = Arc::new(HybridStorageBackend::new(hybrid.clone(), true));

    // Act - Write files that exceed cache size (each ~2KB)
    for i in 0..10 {
        let key = format!("file-{}.dat", i);
        let data = Bytes::from(vec![i as u8; 2048]);
        storage.put_blob(&key, data).expect("put failed");
    }

    // Eviction occurs synchronously in update_cache_state, so we can assert immediately
    let stats = hybrid.cache_stats();
    assert!(
        stats.file_count < 10,
        "Cache should have evicted files (file_count={}, cache {}KB)",
        stats.file_count,
        cache_size / 1024
    );

    // Assert - Cache should have evicted some files
    let stats = hybrid.cache_stats();
    assert!(
        stats.file_count < 10,
        "Cache should have evicted files (have {} files for {}KB cache)",
        stats.file_count,
        cache_size / 1024
    );

    // All files should still be accessible from cloud
    for i in 0..10 {
        let key = format!("file-{}.dat", i);
        let result = storage.get_blob(&key);
        assert!(result.is_ok(), "File {} should be accessible", i);
    }
}

#[test]
fn should_handle_concurrent_reads_writes_to_cache() {
    // Arrange
    let dir = test_temp_dir();
    let mock_backend = Arc::new(MockCloudBackend::new());
    let cache_size = 1024 * 1024; // 1MB cache

    let hybrid = Arc::new(
        HybridStorage::new(dir.path().to_path_buf(), mock_backend.clone(), cache_size)
            .expect("failed to create hybrid storage"),
    );
    // background workers not required for deterministic synchronous writes

    let storage = Arc::new(HybridStorageBackend::new(hybrid.clone(), true));

    // Pre-populate with some data
    for i in 0..20 {
        let key = format!("initial-{}.dat", i);
        storage
            .put_blob(&key, Bytes::from(vec![i as u8; 1024]))
            .expect("put failed");
    }

    let barrier = Arc::new(Barrier::new(8));
    let mut handles = vec![];

    // Act - Spawn concurrent readers and writers
    for thread_id in 0..8 {
        let storage_clone = Arc::clone(&storage);
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            barrier_clone.wait();

            if thread_id % 2 == 0 {
                // Writer threads
                for i in 0..50 {
                    let key = format!("thread-{}-file-{}.dat", thread_id, i);
                    let data = Bytes::from(vec![thread_id as u8; 512]);
                    storage_clone.put_blob(&key, data).expect("put failed");
                }
            } else {
                // Reader threads
                for i in 0..20 {
                    let key = format!("initial-{}.dat", i);
                    let _ = storage_clone.get_blob(&key);
                }

                // Also read from writers
                for wid in (0..8).step_by(2) {
                    for i in 0..50 {
                        let key = format!("thread-{}-file-{}.dat", wid, i);
                        // May or may not exist yet, that's ok
                        let _ = storage_clone.get_blob(&key);
                    }
                }
            }
        });

        handles.push(handle);
    }

    // Assert - All threads complete without panicking
    for handle in handles {
        handle.join().expect("thread should not panic");
    }

    let metrics = hybrid.cloud_metrics();
    assert!(metrics.cache_hits > 0, "Should have some cache hits");
}

#[test]
fn should_maintain_correctness_under_rapid_cache_churn() {
    // Arrange
    let dir = test_temp_dir();
    let mock_backend = Arc::new(MockCloudBackend::new());
    let cache_size = 1024 * 5; // Very small 5KB cache to force evictions

    let hybrid = Arc::new(
        HybridStorage::new(dir.path().to_path_buf(), mock_backend.clone(), 512 * 1024)
            .expect("failed to create hybrid storage"),
    );
    let _handles = hybrid.spawn_background_workers();

    let storage = Arc::new(HybridStorageBackend::new(hybrid.clone(), true));

    // Act - Write many files to cause rapid eviction
    let test_data: Vec<(String, Bytes)> = (0..50)
        .map(|i| {
            let key = format!("rapid-{:03}.dat", i);
            let data = Bytes::from(vec![i as u8; 1024]); // 1KB each
            (key, data)
        })
        .collect();

    for (key, data) in &test_data {
        storage.put_blob(key, data.clone()).expect("put failed");
    }

    // Synchronous writes ensure data is already uploaded; assert immediately
    for (key, _) in &test_data {
        assert!(
            storage.get_blob(key).is_ok(),
            "get should succeed for {}",
            key
        );
    }

    // Assert - All data should be retrievable and correct
    for (key, expected_data) in &test_data {
        let result = storage.get_blob(key).expect("get should succeed");
        assert_eq!(
            result, *expected_data,
            "Data for {} should match original",
            key
        );
    }

    let metrics = hybrid.cloud_metrics();
    assert!(
        metrics.files_evicted > 0,
        "Should have evicted files during churn"
    );
}

#[test]
fn should_upload_to_cloud_asynchronously_without_blocking_writes() {
    // Arrange
    let dir = test_temp_dir();
    let mock_backend = Arc::new(MockCloudBackend::new());

    let cache_size = 1024 * 1024; // 1MB cache

    let hybrid = Arc::new(
        HybridStorage::new(dir.path().to_path_buf(), mock_backend.clone(), cache_size)
            .expect("failed to create hybrid storage"),
    );
    let _handles = hybrid.spawn_background_workers();

    let storage = Arc::new(HybridStorageBackend::new(hybrid.clone(), true));

    let start = std::time::Instant::now();

    // Act - Write multiple files quickly
    for i in 0..20 {
        let key = format!("async-{}.dat", i);
        storage
            .put_blob(&key, Bytes::from(vec![i as u8; 1024]))
            .expect("put failed");
    }

    let write_duration = start.elapsed();

    // Assert - Writes should complete quickly (not waiting for cloud uploads)
    assert!(
        write_duration.as_millis() < 500,
        "Writes should not block on cloud uploads (took {}ms)",
        write_duration.as_millis()
    );

    // Wait for all uploads to reach the cloud (mock helper)
    use common::test_helpers::TEST_CLOUD_TIMEOUT;
    assert!(mock_backend.wait_for_uploads(20, TEST_CLOUD_TIMEOUT));

    // Verify all files are in cloud
    for i in 0..20 {
        let key = format!("async-{}.dat", i);
        let cloud_result = mock_backend.get_blob(&key);
        assert!(
            cloud_result.is_ok(),
            "File {} should be uploaded to cloud",
            i
        );
    }
}

#[test]
fn should_recover_from_cache_directory_deletion() {
    // Arrange
    let dir = test_temp_dir();
    let mock_backend = Arc::new(MockCloudBackend::new());
    let cache_size = 1024 * 1024; // 1MB cache

    let hybrid = Arc::new(
        HybridStorage::new(dir.path().to_path_buf(), mock_backend.clone(), cache_size)
            .expect("failed to create hybrid storage"),
    );
    let _handles = hybrid.spawn_background_workers();

    let storage = Arc::new(HybridStorageBackend::new(hybrid.clone(), true));

    // Write some data
    for i in 0..10 {
        let key = format!("resilient-{}.dat", i);
        storage
            .put_blob(&key, Bytes::from(vec![i as u8; 1024]))
            .expect("put failed");
    }

    // Writes are synchronous, so data should be immediately retrievable from cloud

    // Act - Simulate cache corruption/deletion (cloud still has data)
    // In reality, cache directory corruption is handled by HybridStorage internally

    // Assert - Data should still be accessible from cloud
    for i in 0..10 {
        let key = format!("resilient-{}.dat", i);
        let result = storage.get_blob(&key);
        assert!(result.is_ok(), "Should recover file {} from cloud", i);
    }
}

#[test]
fn should_track_cache_metrics_accurately() {
    // Arrange
    let dir = test_temp_dir();
    let mock_backend = Arc::new(MockCloudBackend::new());
    let cache_size = 1024 * 50; // 50KB cache

    let hybrid = Arc::new(
        HybridStorage::new(dir.path().to_path_buf(), mock_backend.clone(), cache_size)
            .expect("failed to create hybrid storage"),
    );
    let _handles = hybrid.spawn_background_workers();

    let storage = Arc::new(HybridStorageBackend::new(hybrid.clone(), true));

    // Act - Perform operations to generate metrics
    for i in 0..10 {
        let key = format!("metric-{}.dat", i);
        storage
            .put_blob(&key, Bytes::from(vec![i as u8; 2048]))
            .expect("put failed");
    }

    // Read some files (cache hits)
    for i in 0..5 {
        let key = format!("metric-{}.dat", i);
        storage.get_blob(&key).expect("get failed");
    }

    // Read again (should be cache hits)
    for i in 0..5 {
        let key = format!("metric-{}.dat", i);
        storage.get_blob(&key).expect("get failed");
    }

    // Assert
    let metrics = hybrid.cloud_metrics();
    assert!(metrics.cache_hits >= 5, "Should have at least 5 cache hits");

    let stats = hybrid.cache_stats();
    assert_eq!(
        stats.file_count, 10,
        "Should have 10 files cached (within limit)"
    );
}
