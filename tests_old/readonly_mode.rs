//! Read-Only Mode Integration Tests
//!
//! Tests for read-only database access.
//! Verifies that Midge correctly enforces read-only semantics.
//!
//! ## Coverage
//! - Write rejection in read-only mode
//! - Read operations in read-only mode
//! - Delete/insert rejection in read-only mode
//!
//! ## Storage Mode Coverage
//! Tests LocalDisk and CloudBacked modes (disk-based modes that support reopening).
//! Memory mode tests verify read-only flag works but cannot test persistence.

mod common;

use bytes::Bytes;
use cntryl_midge::{
    cloud::MockCloudBackend, config::cloud::StorageContext, MidgeEngine, MidgeOptions, Query,
    StorageMode,
};
use std::sync::Arc;
use tempfile::TempDir;

/// Helper to create storage mode options that can be reopened with read_only flag.
/// Returns (opts_write, opts_readonly).
fn create_reopenable_storage_modes(
    mode: &str,
    temp_dir: &TempDir,
    cloud_backend: Option<Arc<MockCloudBackend>>,
) -> (MidgeOptions, MidgeOptions) {
    match mode {
        "LocalDisk" => {
            let opts_write = MidgeOptions {
                storage_mode: StorageMode::LocalDisk {
                    db_path: temp_dir.path().to_path_buf(),
                },
                enable_compaction: false,
                ..Default::default()
            };
            let opts_ro = MidgeOptions {
                storage_mode: StorageMode::LocalDisk {
                    db_path: temp_dir.path().to_path_buf(),
                },
                enable_compaction: false,
                read_only: true,
                ..Default::default()
            };
            (opts_write, opts_ro)
        }
        "CloudBacked" => {
            let backend = cloud_backend.unwrap_or_else(|| Arc::new(MockCloudBackend::new()));
            let opts_write = MidgeOptions {
                storage_mode: StorageMode::CloudBacked {
                    local_cache_path: temp_dir.path().to_path_buf(),
                    cloud_backend: backend.clone(),
                    storage_context: StorageContext::default(),
                    local_wal_sync: false,
                    wal_batch_size: 4 * 1024 * 1024,
                    sst_cache_capacity: 16,
                },
                enable_compaction: false,
                ..Default::default()
            };
            let opts_ro = MidgeOptions {
                storage_mode: StorageMode::CloudBacked {
                    local_cache_path: temp_dir.path().to_path_buf(),
                    cloud_backend: backend,
                    storage_context: StorageContext::default(),
                    local_wal_sync: false,
                    wal_batch_size: 4 * 1024 * 1024,
                    sst_cache_capacity: 16,
                },
                enable_compaction: false,
                read_only: true,
                ..Default::default()
            };
            (opts_write, opts_ro)
        }
        _ => panic!("Unknown storage mode: {}", mode),
    }
}

// =============================================================================
// Write Rejection
// =============================================================================

#[test]
fn should_reject_put_given_read_only_mode_when_write_attempted() {
    for mode in &["LocalDisk", "CloudBacked"] {
        // Arrange: prepare an existing DB
        let tmp = TempDir::new().unwrap();
        let backend = Arc::new(MockCloudBackend::new());
        let (opts_write, opts_ro) =
            create_reopenable_storage_modes(mode, &tmp, Some(backend.clone()));

        let db = MidgeEngine::open(opts_write).unwrap();
        let cf = db.default_column_family();
        db.put(&cf, b"k", b"v").unwrap();
        db.flush().unwrap();
        drop(db);

        // Act: open read-only and attempt a write
        let db_ro = MidgeEngine::open(opts_ro).unwrap();
        let cf_ro = db_ro.default_column_family();
        let err = db_ro.put(&cf_ro, b"k2", b"v2").unwrap_err();

        // Assert: error indicates read-only
        let msg = format!("{}", err);
        assert!(
            msg.contains("read-only") || msg.contains("ReadOnly"),
            "Failed for {}: expected ReadOnly error, got: {}",
            mode,
            msg
        );
    }
}

#[test]
fn should_reject_insert_given_read_only_mode_when_memory_mode() {
    // Arrange - This test uses Memory mode to verify read-only flag works even without persistence
    let opts_ro = MidgeOptions {
        storage_mode: StorageMode::Memory,
        read_only: true,
        ..Default::default()
    };
    let engine_ro = MidgeEngine::open(opts_ro).unwrap();
    let cf_ro = engine_ro.default_column_family();

    // Act
    let result = engine_ro.insert(&cf_ro, b"key1", b"value1");

    // Assert
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        cntryl_midge::error::MidgeError::ReadOnly
    ));
}

#[test]
fn should_reject_insert_given_read_only_mode_when_disk_based_storage() {
    for mode in &["LocalDisk", "CloudBacked"] {
        // Arrange
        let tmp = TempDir::new().unwrap();
        let backend = Arc::new(MockCloudBackend::new());
        let (opts_write, opts_ro) =
            create_reopenable_storage_modes(mode, &tmp, Some(backend.clone()));

        let db = MidgeEngine::open(opts_write).unwrap();
        let cf = db.default_column_family();
        db.put(&cf, b"existing", b"value").unwrap();
        db.flush().unwrap();
        drop(db);

        // Act: open read-only and attempt insert
        let db_ro = MidgeEngine::open(opts_ro).unwrap();
        let cf_ro = db_ro.default_column_family();
        let result = db_ro.insert(&cf_ro, b"newkey", b"newvalue");

        // Assert
        assert!(
            result.is_err(),
            "Insert should fail in read-only mode for {}",
            mode
        );
        assert!(matches!(
            result.unwrap_err(),
            cntryl_midge::error::MidgeError::ReadOnly
        ));
    }
}

#[test]
fn should_reject_delete_given_read_only_mode_when_delete_attempted() {
    for mode in &["LocalDisk", "CloudBacked"] {
        // Arrange
        let tmp = TempDir::new().unwrap();
        let backend = Arc::new(MockCloudBackend::new());
        let (opts_write, opts_ro) =
            create_reopenable_storage_modes(mode, &tmp, Some(backend.clone()));

        let db = MidgeEngine::open(opts_write).unwrap();
        let cf = db.default_column_family();
        db.put(&cf, b"k", b"v").unwrap();
        db.flush().unwrap();
        drop(db);

        // Act: open read-only and attempt delete
        let db_ro = MidgeEngine::open(opts_ro).unwrap();
        let cf_ro = db_ro.default_column_family();
        let result = db_ro.delete(&cf_ro, b"k");

        // Assert
        assert!(
            result.is_err(),
            "Delete should fail in read-only mode for {}",
            mode
        );
        let msg = format!("{}", result.unwrap_err());
        assert!(
            msg.contains("read-only") || msg.contains("ReadOnly"),
            "Failed for {}: expected ReadOnly error, got: {}",
            mode,
            msg
        );
    }
}

// =============================================================================
// Read Operations
// =============================================================================

#[test]
fn should_allow_get_given_read_only_mode_when_reading_existing_key() {
    for mode in &["LocalDisk", "CloudBacked"] {
        // Arrange: create DB and write a key, then close
        let tmp = TempDir::new().unwrap();
        let backend = Arc::new(MockCloudBackend::new());
        let (opts_write, opts_ro) =
            create_reopenable_storage_modes(mode, &tmp, Some(backend.clone()));

        let db = MidgeEngine::open(opts_write).unwrap();
        let cf = db.default_column_family();
        db.put(&cf, b"k", b"v").unwrap();
        db.flush().unwrap();
        drop(db);

        // Act: Re-open read-only
        let db_ro = MidgeEngine::open(opts_ro).unwrap();
        let cf_ro = db_ro.default_column_family();

        // Assert: reads work
        let got = db_ro.get(&cf_ro, b"k").unwrap();
        assert_eq!(
            got,
            Some(Bytes::from_static(b"v")),
            "Get should work in read-only mode for {}",
            mode
        );
    }
}

#[test]
fn should_allow_scan_given_read_only_mode_when_scanning_range() {
    for mode in &["LocalDisk", "CloudBacked"] {
        // Arrange
        let tmp = TempDir::new().unwrap();
        let backend = Arc::new(MockCloudBackend::new());
        let (opts_write, opts_ro) =
            create_reopenable_storage_modes(mode, &tmp, Some(backend.clone()));

        let db = MidgeEngine::open(opts_write).unwrap();
        let cf = db.default_column_family();
        for i in 0..10 {
            let key = format!("key{:02}", i);
            let value = format!("value{}", i);
            db.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
        }
        db.flush().unwrap();
        drop(db);

        // Act: Re-open read-only and scan
        let db_ro = MidgeEngine::open(opts_ro).unwrap();
        let cf_ro = db_ro.default_column_family();
        let results = db_ro
            .scan(
                &cf_ro,
                Query::new()
                    .start_key(Bytes::from_static(b"key00"))
                    .end_key(Bytes::from_static(b"key05")),
            )
            .unwrap();

        // Assert: scan returns results
        assert_eq!(
            results.len(),
            5,
            "Should scan 5 keys (key00-key04) for {}",
            mode
        );
    }
}

#[test]
fn should_return_none_given_read_only_mode_when_key_not_exists() {
    for mode in &["LocalDisk", "CloudBacked"] {
        // Arrange
        let tmp = TempDir::new().unwrap();
        let backend = Arc::new(MockCloudBackend::new());
        let (opts_write, opts_ro) =
            create_reopenable_storage_modes(mode, &tmp, Some(backend.clone()));

        let db = MidgeEngine::open(opts_write).unwrap();
        let cf = db.default_column_family();
        db.put(&cf, b"existing", b"value").unwrap();
        db.flush().unwrap();
        drop(db);

        // Act: Re-open read-only
        let db_ro = MidgeEngine::open(opts_ro).unwrap();
        let cf_ro = db_ro.default_column_family();
        let result = db_ro.get(&cf_ro, b"nonexistent").unwrap();

        // Assert
        assert_eq!(
            result, None,
            "Should return None for nonexistent key in read-only mode for {}",
            mode
        );
    }
}
