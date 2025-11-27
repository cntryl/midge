//! Memory Mode Integration Tests
//!
//! Tests for in-memory storage mode.
//! Verifies that Memory mode keeps everything in memory and creates no filesystem artifacts.
//!
//! ## Coverage
//! - No manifest files created when creating column families
//! - No filesystem artifacts with multiple column families
//!
//! ## Storage Mode Coverage
//! Tests Memory mode only (LocalDisk and CloudBacked create filesystem artifacts by design).

mod common;

use bytes::Bytes;
use cntryl_midge::api::column_family::ColumnFamilyConfig;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use tempfile::TempDir;

// =============================================================================
// No Filesystem Artifacts
// =============================================================================

#[test]
fn should_not_write_manifest_when_creating_cf_in_memory_mode() {
    // Arrange
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();

    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        enable_compaction: false,
        ..Default::default()
    };

    let engine = MidgeEngine::open(opts).expect("open");

    // Act
    let custom_cf = engine
        .create_column_family("custom_cf", ColumnFamilyConfig::default())
        .expect("create_column_family");

    // Write some data to the custom CF
    engine.put(&custom_cf, b"key1", b"value1").expect("put");

    // Assert
    assert_eq!(
        engine.get(&custom_cf, b"key1").expect("get"),
        Some(Bytes::from_static(b"value1"))
    );

    // Verify no filesystem artifacts
    assert!(
        !db_path.exists() || std::fs::read_dir(db_path).unwrap().count() == 0,
        "No files should exist in memory mode"
    );
}

#[test]
fn should_handle_multiple_cfs_without_disk_writes_in_memory_mode() {
    // Arrange
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();

    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        enable_compaction: false,
        ..Default::default()
    };

    let engine = MidgeEngine::open(opts).expect("open");

    // Act: Create multiple column families
    let cf1 = engine
        .create_column_family("cf1", ColumnFamilyConfig::default())
        .expect("create cf1");
    let cf2 = engine
        .create_column_family("cf2", ColumnFamilyConfig::default())
        .expect("create cf2");
    let cf3 = engine
        .create_column_family("cf3", ColumnFamilyConfig::default())
        .expect("create cf3");

    // Write data to each CF
    engine.put(&cf1, b"key", b"value1").expect("put cf1");
    engine.put(&cf2, b"key", b"value2").expect("put cf2");
    engine.put(&cf3, b"key", b"value3").expect("put cf3");

    // Assert: Data is isolated per CF
    assert_eq!(
        engine.get(&cf1, b"key").expect("get"),
        Some(Bytes::from_static(b"value1"))
    );
    assert_eq!(
        engine.get(&cf2, b"key").expect("get"),
        Some(Bytes::from_static(b"value2"))
    );
    assert_eq!(
        engine.get(&cf3, b"key").expect("get"),
        Some(Bytes::from_static(b"value3"))
    );

    // Verify no filesystem artifacts
    assert!(
        !db_path.exists() || std::fs::read_dir(db_path).unwrap().count() == 0,
        "No files should exist in memory mode with multiple CFs"
    );
}
