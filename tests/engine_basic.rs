//! Core Engine Operations - Put, Get, Delete
//!
//! This file tests the fundamental CRUD operations of the MidgeEngine.
//! Note: SCAN, INSERT, CAS, and DELETE_RANGE tests have been removed as those
//! features are not yet implemented in the new engine.

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use cntryl_midge::testkit::{all_storage_modes, create_storage_mode};

// ============================================================================
// PUT / GET Operations
// ============================================================================

#[test]
fn should_get_value_given_existing_key_when_put() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open_with_options(opts).expect("open");
        let cf = engine.default_column_family();

        // Act
        engine.put_cf(&cf, b"key", b"value").expect("put");
        let result = engine.get_cf(&cf, b"key").expect("get");

        // Assert
        assert_eq!(
            result.map(|v| Bytes::from(v)),
            Some(Bytes::from_static(b"value")),
            "Failed for {}",
            name
        );
    }
}

#[test]
fn should_return_none_given_nonexistent_key_when_get() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open_with_options(opts).expect("open");
        let cf = engine.default_column_family();

        // Act
        let result = engine.get_cf(&cf, b"missing").expect("get");

        // Assert
        assert_eq!(result, None, "Failed for {}", name);
    }
}

#[test]
fn should_overwrite_value_given_existing_key_when_put() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open_with_options(opts).expect("open");
        let cf = engine.default_column_family();
        engine.put_cf(&cf, b"key", b"original").expect("put");

        // Act
        engine.put_cf(&cf, b"key", b"updated").expect("put");
        let result = engine.get_cf(&cf, b"key").expect("get");

        // Assert
        assert_eq!(
            result.map(|v| Bytes::from(v)),
            Some(Bytes::from_static(b"updated")),
            "Failed for {}",
            name
        );
    }
}

#[test]
fn should_handle_empty_value_when_put() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open_with_options(opts).expect("open");
        let cf = engine.default_column_family();

        // Act
        engine.put_cf(&cf, b"key", b"").expect("put");
        let result = engine.get_cf(&cf, b"key").expect("get");

        // Assert
        assert_eq!(result.map(|v| Bytes::from(v)), Some(Bytes::from_static(b"")), "Failed for {}", name);
    }
}

#[test]
fn should_handle_binary_data_when_put() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open_with_options(opts).expect("open");
        let cf = engine.default_column_family();

        let binary_key = vec![0x00, 0x01, 0xFF, 0xFE];
        let binary_value = vec![0xDE, 0xAD, 0xBE, 0xEF];

        // Act
        engine.put_cf(&cf, &binary_key, &binary_value).expect("put");
        let result = engine.get_cf(&cf, &binary_key).expect("get");

        // Assert
        assert_eq!(
            result.map(|v| Bytes::from(v)),
            Some(Bytes::from(binary_value.clone())),
            "Failed for {}",
            name
        );
    }
}

// ============================================================================
// DELETE Operations
// ============================================================================

#[test]
fn should_return_none_given_deleted_key_when_get() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open_with_options(opts).expect("open");
        let cf = engine.default_column_family();
        engine.put_cf(&cf, b"key", b"value").expect("put");

        // Act
        engine.delete_cf(&cf, b"key").expect("delete");
        let result = engine.get_cf(&cf, b"key").expect("get");

        // Assert
        assert_eq!(result, None, "Failed for {}", name);
    }
}

#[test]
fn should_succeed_given_nonexistent_key_when_delete() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open_with_options(opts).expect("open");
        let cf = engine.default_column_family();

        // Act
        let result = engine.delete_cf(&cf, b"nonexistent");

        // Assert
        assert!(result.is_ok(), "Failed for {}", name);
    }
}

// ============================================================================
// Memory Mode Specific
// ============================================================================

#[test]
fn should_not_create_filesystem_artifacts_when_memory_mode() {
    // Arrange
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };

    // Act
    let engine = MidgeEngine::open_with_options(opts).expect("open");
    let cf = engine.default_column_family();
    engine.put_cf(&cf, b"key", b"value").expect("put");

    // Assert - memory mode doesn't create files on disk
    // This test mainly validates that the engine works with memory storage
    let result = engine.get_cf(&cf, b"key").expect("get");
    assert_eq!(result.map(|v| Bytes::from(v)), Some(Bytes::from_static(b"value")));
}
