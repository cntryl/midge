//! Paranoid checksum mode tests
//!
//! Tests for paranoid checksum verification which validates data integrity
//! on every block read from SST files.

use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use tempfile::TempDir;

// ============================================================================
// PARANOID MODE CONFIGURATION
// ============================================================================

#[test]
fn should_enable_paranoid_checksums_given_configuration_when_reading() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: temp_dir.path().to_path_buf(),
        },
        paranoid_checksums: true,
        ..Default::default()
    };

    // Act
    let db = MidgeEngine::open(opts).expect("open database");
    let cf = db.default_column_family();

    for i in 0..1000 {
        let key = format!("key_{:04}", i);
        let value = format!("value_{:04}", i);
        db.put(&cf, key.as_bytes(), value.as_bytes())
            .expect("put should succeed");
    }

    db.flush().expect("flush should succeed");

    // Assert
    for i in 0..1000 {
        let key = format!("key_{:04}", i);
        let expected_value = format!("value_{:04}", i);
        let result = db.get(&cf, key.as_bytes()).expect("get should succeed");
        assert_eq!(
            result.as_deref(),
            Some(expected_value.as_bytes()),
            "value should match for key {}",
            key
        );
    }
}

#[test]
fn should_work_without_paranoid_checksums_given_disabled_when_reading() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: temp_dir.path().to_path_buf(),
        },
        paranoid_checksums: false,
        ..Default::default()
    };

    // Act
    let db = MidgeEngine::open(opts).expect("open database");
    let cf = db.default_column_family();

    for i in 0..1000 {
        let key = format!("key_{:04}", i);
        let value = format!("value_{:04}", i);
        db.put(&cf, key.as_bytes(), value.as_bytes())
            .expect("put should succeed");
    }
    db.flush().expect("flush should succeed");

    // Assert
    for i in 0..1000 {
        let key = format!("key_{:04}", i);
        let expected_value = format!("value_{:04}", i);
        let result = db.get(&cf, key.as_bytes()).expect("get should succeed");
        assert_eq!(
            result.as_deref(),
            Some(expected_value.as_bytes()),
            "value should match for key {}",
            key
        );
    }
}

#[test]
fn should_verify_checksums_on_compressed_blocks_given_paranoid_mode_when_reading() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: temp_dir.path().to_path_buf(),
        },
        paranoid_checksums: true,
        compression: cntryl_midge::common::codec::CompressionType::Lz4,
        ..Default::default()
    };

    // Act
    let db = MidgeEngine::open(opts).expect("open database");
    let cf = db.default_column_family();

    for i in 0..100 {
        let key = format!("key_{:04}", i);
        let value = format!("large_value_{:04}", i).repeat(100);
        db.put(&cf, key.as_bytes(), value.as_bytes())
            .expect("put should succeed");
    }
    db.flush().expect("flush should succeed");

    // Assert
    for i in 0..100 {
        let key = format!("key_{:04}", i);
        let expected_value = format!("large_value_{:04}", i).repeat(100);
        let result = db.get(&cf, key.as_bytes()).expect("get should succeed");
        assert_eq!(
            result.as_deref(),
            Some(expected_value.as_bytes()),
            "decompressed value should match for key {}",
            key
        );
    }
}

#[test]
fn should_default_paranoid_mode_off_given_default_options_when_checking() {
    // Arrange
    // Act
    let opts = MidgeOptions::default();

    // Assert
    assert!(
        !opts.paranoid_checksums,
        "paranoid_checksums should default to false for performance"
    );
}
