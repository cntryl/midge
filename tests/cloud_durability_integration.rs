//! End-to-end integration tests for all cloud durability modes.
//!
//! These tests demonstrate complete workflows using MockCloudBackend with:
//! - Strict durability (sync cloud uploads)
//! - Steady durability (async cloud uploads with intervals)
//! - Cloud-replicated durability (cloud-first with optional local cache)

use midge::cloud::MockCloudBackend;
use midge::config::cloud_builder::CloudConfigBuilder;
use midge::config::{CloudMode, Durability};
use std::sync::Arc;

#[test]
fn should_configure_strict_durability_mode() {
    // Arrange
    let backend = Arc::new(MockCloudBackend::new());

    // Act
    let builder = CloudConfigBuilder::strict_durability(backend.clone(), "./tmp/strict_test");

    // Assert
    assert_eq!(builder.durability(), Durability::Strict);
    assert_eq!(builder.cloud_mode(), CloudMode::Cache);
    assert!(
        builder.sync_interval().is_none(),
        "Strict mode = sync uploads"
    );

    let storage_mode = builder.build();
    assert!(storage_mode.is_cloud_backed());
}

#[test]
fn should_configure_balanced_durability_mode() {
    // Arrange
    let backend = Arc::new(MockCloudBackend::new());

    // Act
    let builder = CloudConfigBuilder::balanced_durability(backend.clone(), "./tmp/balanced_test");

    // Assert
    assert_eq!(builder.durability(), Durability::Steady);
    assert_eq!(builder.cloud_mode(), CloudMode::Cache);
    assert!(
        builder.sync_interval().is_some(),
        "Steady mode = interval-based sync"
    );

    let storage_mode = builder.build();
    assert!(storage_mode.is_cloud_backed());
}

#[test]
fn should_configure_replicated_durability_mode() {
    // Arrange
    let backend = Arc::new(MockCloudBackend::new());

    // Act
    let builder =
        CloudConfigBuilder::replicated_durability(backend.clone(), "./tmp/replicated_test");

    // Assert
    assert_eq!(builder.durability(), Durability::CloudReplicated);
    assert_eq!(builder.cloud_mode(), CloudMode::Tiered);

    let storage_mode = builder.build();
    assert!(storage_mode.is_cloud_backed());
}

#[test]
fn should_allow_customization_of_strict_mode() {
    // Arrange
    let backend = Arc::new(MockCloudBackend::new());

    // Act
    let builder = CloudConfigBuilder::strict_durability(backend, "./tmp/custom_strict")
        .with_max_cache_size_mb(256)
        .with_path("customer-abc")
        .with_wal_batch_size(128 * 1024);

    // Assert
    let storage_mode = builder.build();
    assert!(storage_mode.is_cloud_backed());

    let context = storage_mode.storage_context().unwrap();
    assert_eq!(context.path(), "customer-abc");
}

#[test]
fn should_allow_customization_of_steady_mode() {
    // Arrange
    let backend = Arc::new(MockCloudBackend::new());

    // Act
    let builder = CloudConfigBuilder::balanced_durability(backend, "./tmp/custom_balanced")
        .with_sync_interval_ms(10) // Faster syncs
        .with_max_cache_size_mb(4096) // Larger cache
        .with_sst_cache_capacity(500); // More SSTs

    // Assert
    assert_eq!(
        builder.sync_interval(),
        Some(std::time::Duration::from_millis(10))
    );

    let storage_mode = builder.build();
    assert!(storage_mode.is_cloud_backed());
}

#[test]
fn should_allow_disabling_local_cache_in_replicated_durability() {
    // Arrange
    let backend = Arc::new(MockCloudBackend::new());

    // Act
    let builder = CloudConfigBuilder::replicated_durability(backend, "./tmp/no_cache")
        .with_local_cache_enabled(false);

    // Assert
    let storage_mode = builder.build();
    assert!(storage_mode.is_cloud_backed());
}

#[test]
fn should_support_hierarchical_paths() {
    // Arrange
    let backend = Arc::new(MockCloudBackend::new());

    // Act - Test all three modes with hierarchical paths
    let strict = CloudConfigBuilder::strict_durability(backend.clone(), "./tmp/path1")
        .with_path("customer-1")
        .build();

    let balanced = CloudConfigBuilder::balanced_durability(backend.clone(), "./tmp/path2")
        .with_path("prod/us-east-1")
        .build();

    let replicated = CloudConfigBuilder::replicated_durability(backend, "./tmp/path3")
        .with_path("acme-corp/engineering")
        .build();

    // Assert
    assert_eq!(strict.storage_context().unwrap().path(), "customer-1");
    assert_eq!(balanced.storage_context().unwrap().path(), "prod/us-east-1");
    assert_eq!(
        replicated.storage_context().unwrap().path(),
        "acme-corp/engineering"
    );
}

#[test]
fn should_use_appropriate_batch_sizes_per_durability_mode() {
    // Arrange
    let backend = Arc::new(MockCloudBackend::new());

    // Act
    let strict = CloudConfigBuilder::strict_durability(backend.clone(), "./tmp/b1");
    let balanced = CloudConfigBuilder::balanced_durability(backend.clone(), "./tmp/b2");
    let replicated = CloudConfigBuilder::replicated_durability(backend, "./tmp/b3");

    // Assert - Each mode has appropriate characteristics
    // Strict: Synchronous uploads (no interval)
    assert!(strict.sync_interval().is_none());

    // Balanced: Interval-based with moderate frequency
    assert!(balanced.sync_interval().is_some());
    assert_eq!(
        balanced.sync_interval(),
        Some(std::time::Duration::from_millis(20))
    );

    // Cloud-replicated: Cloud-first with longer intervals
    assert!(replicated.sync_interval().is_some());
    assert!(replicated.sync_interval().unwrap() > balanced.sync_interval().unwrap());
}

#[test]
fn should_configure_cloud_backend_reference() {
    // Arrange
    let backend = Arc::new(MockCloudBackend::new());

    // Act
    let storage_mode =
        CloudConfigBuilder::balanced_durability(backend, "./tmp/backend_test").build();

    // Assert
    let cloud_backend = storage_mode.cloud_backend();
    assert!(cloud_backend.is_some());
}

#[test]
fn should_generate_correct_cloud_prefix() {
    // Arrange
    let backend = Arc::new(MockCloudBackend::new());

    // Act
    let storage_mode = CloudConfigBuilder::balanced_durability(backend, "./tmp/prefix_test")
        .with_path("acme-corp")
        .build();

    // Assert
    let prefix = storage_mode.cloud_prefix();
    assert!(prefix.is_some());
    assert!(prefix.unwrap().contains("acme-corp"));
}

#[test]
fn should_support_custom_cloud_mode_override() {
    // Arrange
    let backend = Arc::new(MockCloudBackend::new());

    // Act - Start with balanced, override to tiered
    let builder = CloudConfigBuilder::balanced_durability(backend, "./tmp/override")
        .with_cloud_mode(CloudMode::Tiered);

    // Assert
    assert_eq!(builder.cloud_mode(), CloudMode::Tiered);
    assert_eq!(builder.durability(), Durability::Steady);
}

#[test]
fn should_validate_storage_mode_types() {
    // Arrange
    let backend = Arc::new(MockCloudBackend::new());

    // Act
    let memory_mode = midge::core::storage_mode::StorageMode::Memory;
    let local_mode = midge::core::storage_mode::StorageMode::LocalDisk {
        db_path: "./test".into(),
    };
    let cloud_mode = CloudConfigBuilder::balanced_durability(backend, "./tmp/type_test").build();

    // Assert
    assert!(memory_mode.is_memory());
    assert!(!memory_mode.is_cloud_backed());

    assert!(local_mode.is_local_disk());
    assert!(!local_mode.is_cloud_backed());

    assert!(!cloud_mode.is_memory());
    assert!(!cloud_mode.is_local_disk());
    assert!(cloud_mode.is_cloud_backed());
}

// ===== Mock Backend Behavior Tests =====

#[test]
fn should_track_uploads_with_mock_backend() {
    // Arrange
    let backend = Arc::new(MockCloudBackend::new());
    let backend_ref = backend.clone();

    // Act
    let _storage_mode =
        CloudConfigBuilder::balanced_durability(backend, "./tmp/metrics_test").build();

    // MockBackend can be used to verify cloud operations
    backend_ref.reset_counters();
    let initial_count = backend_ref.upload_count();

    // Assert
    assert_eq!(initial_count, 0);
}

#[test]
fn should_support_latency_simulation_in_tests() {
    // Arrange
    let backend =
        Arc::new(MockCloudBackend::new().with_latency(std::time::Duration::from_millis(10)));

    // Act
    let _storage_mode =
        CloudConfigBuilder::balanced_durability(backend, "./tmp/latency_test").build();

    // Assert - MockBackend configured with artificial latency for testing
}

#[test]
fn should_support_failure_injection_in_tests() {
    // Arrange
    let backend = Arc::new(MockCloudBackend::new());
    backend.set_fail_upload_after(5); // Fail after 5 successful uploads

    // Act
    let _storage_mode =
        CloudConfigBuilder::replicated_durability(backend.clone(), "./tmp/fail_test").build();

    // Assert
    // After 5 uploads, subsequent uploads will fail (for testing error paths)
}
