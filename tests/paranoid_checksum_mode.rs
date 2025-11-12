use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use tempfile::TempDir;

#[test]
fn should_enable_paranoid_checksums_when_configured() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: temp_dir.path().to_path_buf(),
        },
        paranoid_checksums: true, // Enable paranoid mode
        ..Default::default()
    };

    // Act - open database with paranoid checksums
    let db = MidgeEngine::open(opts).expect("open database");
    let cf = db.default_column_family();

    // Write some data that will be flushed to SST
    for i in 0..1000 {
        let key = format!("key_{:04}", i);
        let value = format!("value_{:04}", i);
        db.put(&cf, key.as_bytes(), value.as_bytes())
            .expect("put should succeed");
    }

    // Force flush to create SST files
    db.flush().expect("flush should succeed");

    // Assert - read data back (paranoid mode will verify checksums on every block read)
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
fn should_work_without_paranoid_checksums_when_disabled() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: temp_dir.path().to_path_buf(),
        },
        paranoid_checksums: false, // Explicitly disable (default)
        ..Default::default()
    };

    // Act - open database without paranoid checksums
    let db = MidgeEngine::open(opts).expect("open database");
    let cf = db.default_column_family();

    // Write and flush
    for i in 0..1000 {
        let key = format!("key_{:04}", i);
        let value = format!("value_{:04}", i);
        db.put(&cf, key.as_bytes(), value.as_bytes())
            .expect("put should succeed");
    }
    db.flush().expect("flush should succeed");

    // Assert - read data back (normal checksum verification)
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
fn should_verify_checksums_on_compressed_blocks_when_paranoid() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: temp_dir.path().to_path_buf(),
        },
        paranoid_checksums: true,
        compression: cntryl_midge::common::codec::CompressionType::Lz4, // Use compression
        ..Default::default()
    };

    // Act
    let db = MidgeEngine::open(opts).expect("open database");
    let cf = db.default_column_family();

    // Write large values to ensure compression
    for i in 0..100 {
        let key = format!("key_{:04}", i);
        let value = format!("large_value_{:04}", i).repeat(100); // Large repeated value
        db.put(&cf, key.as_bytes(), value.as_bytes())
            .expect("put should succeed");
    }
    db.flush().expect("flush should succeed");

    // Assert - paranoid mode verifies decompressed data integrity
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
fn should_use_paranoid_mode_with_default_off() {
    // Arrange
    let opts = MidgeOptions::default();

    // Assert
    assert_eq!(
        opts.paranoid_checksums, false,
        "paranoid_checksums should default to false for performance"
    );
}
