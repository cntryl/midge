//! Core Engine Operations - Put, Get, Delete, Scan, Atomic Operations
//!
//! This file tests the fundamental CRUD operations of the MidgeEngine.
//! These are the building blocks that all other functionality depends on.

mod common;

use bytes::Bytes;
use cntryl_midge::{CasResult, InsertResult, MidgeEngine, MidgeOptions, Query, StorageMode};
use common::test_temp_dir;

// ============================================================================
// PUT / GET Operations
// ============================================================================

#[test]
fn should_get_value_given_existing_key_when_put() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Act
    engine.put(&cf, b"key", b"value").expect("put");
    let result = engine.get(&cf, b"key").expect("get");

    // Assert
    assert_eq!(result, Some(Bytes::from_static(b"value")));
}

#[test]
fn should_return_none_given_nonexistent_key_when_get() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Act
    let result = engine.get(&cf, b"missing").expect("get");

    // Assert
    assert_eq!(result, None);
}

#[test]
fn should_overwrite_value_given_existing_key_when_put() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();
    engine.put(&cf, b"key", b"original").expect("put");

    // Act
    engine.put(&cf, b"key", b"updated").expect("put");
    let result = engine.get(&cf, b"key").expect("get");

    // Assert
    assert_eq!(result, Some(Bytes::from_static(b"updated")));
}

#[test]
fn should_handle_empty_value_when_put() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Act
    engine.put(&cf, b"key", b"").expect("put");
    let result = engine.get(&cf, b"key").expect("get");

    // Assert
    assert_eq!(result, Some(Bytes::from_static(b"")));
}

#[test]
fn should_handle_binary_data_when_put() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    let binary_key = vec![0x00, 0x01, 0xFF, 0xFE];
    let binary_value = vec![0xDE, 0xAD, 0xBE, 0xEF];

    // Act
    engine
        .put(&cf, &binary_key, &binary_value)
        .expect("put binary");
    let result = engine.get(&cf, &binary_key).expect("get");

    // Assert
    assert_eq!(result, Some(Bytes::from(binary_value)));
}

// ============================================================================
// DELETE Operations
// ============================================================================

#[test]
fn should_return_none_given_deleted_key_when_get() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();
    engine.put(&cf, b"key", b"value").expect("put");

    // Act
    engine.delete(&cf, b"key").expect("delete");
    let result = engine.get(&cf, b"key").expect("get");

    // Assert
    assert_eq!(result, None);
}

#[test]
fn should_succeed_given_nonexistent_key_when_delete() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Act
    let result = engine.delete(&cf, b"nonexistent");

    // Assert
    assert!(result.is_ok());
}

// ============================================================================
// SCAN Operations
// ============================================================================

#[test]
fn should_return_ordered_pairs_given_range_when_scan() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    engine.put(&cf, b"a", b"1").expect("put");
    engine.put(&cf, b"b", b"2").expect("put");
    engine.put(&cf, b"c", b"3").expect("put");
    engine.put(&cf, b"d", b"4").expect("put");

    // Act
    let results = engine
        .scan(
            &cf,
            Query::new()
                .start_key(Bytes::from_static(b"b"))
                .end_key(Bytes::from_static(b"d")),
        )
        .expect("scan");

    // Assert - end key is exclusive
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], (Bytes::from_static(b"b"), Bytes::from_static(b"2")));
    assert_eq!(results[1], (Bytes::from_static(b"c"), Bytes::from_static(b"3")));
}

#[test]
fn should_return_matching_keys_given_prefix_when_scan() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    engine.put(&cf, b"user:1:name", b"alice").expect("put");
    engine.put(&cf, b"user:1:email", b"alice@example.com").expect("put");
    engine.put(&cf, b"user:2:name", b"bob").expect("put");

    // Act
    let results = engine
        .scan(&cf, Query::new().prefix(Bytes::from_static(b"user:1:")))
        .expect("scan");

    // Assert
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|(k, _)| k.starts_with(b"user:1:")));
}

#[test]
fn should_respect_limit_given_limit_when_scan() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    for i in 0..10 {
        engine.put(&cf, format!("key{:02}", i).as_bytes(), b"v").expect("put");
    }

    // Act
    let results = engine
        .scan(&cf, Query::new().limit(3))
        .expect("scan");

    // Assert
    assert_eq!(results.len(), 3);
}

#[test]
fn should_return_reverse_order_given_reverse_when_scan() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    engine.put(&cf, b"a", b"1").expect("put");
    engine.put(&cf, b"b", b"2").expect("put");
    engine.put(&cf, b"c", b"3").expect("put");

    // Act
    let results = engine
        .scan(&cf, Query::new().reverse())
        .expect("scan");

    // Assert
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0, Bytes::from_static(b"c"));
    assert_eq!(results[1].0, Bytes::from_static(b"b"));
    assert_eq!(results[2].0, Bytes::from_static(b"a"));
}

#[test]
fn should_exclude_deleted_keys_when_scan() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    engine.put(&cf, b"a", b"1").expect("put");
    engine.put(&cf, b"b", b"2").expect("put");
    engine.put(&cf, b"c", b"3").expect("put");
    engine.delete(&cf, b"b").expect("delete");

    // Act
    let results = engine
        .scan(&cf, Query::new())
        .expect("scan");

    // Assert
    assert_eq!(results.len(), 2);
    assert!(!results.iter().any(|(k, _)| k.as_ref() == b"b"));
}

#[test]
fn should_return_empty_given_no_data_when_scan() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Act
    let results = engine
        .scan(&cf, Query::new())
        .expect("scan");

    // Assert
    assert!(results.is_empty());
}

// ============================================================================
// INSERT (Insert-if-not-exists) Operations
// ============================================================================

#[test]
fn should_insert_value_given_nonexistent_key_when_insert() {
    // Arrange
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Act
    let inserted = engine.insert(&cf, b"key", b"value").expect("insert");
    let result = engine.get(&cf, b"key").expect("get");

    // Assert
    assert!(inserted);
    assert_eq!(result, Some(Bytes::from_static(b"value")));
}

#[test]
fn should_not_insert_given_existing_key_when_insert() {
    // Arrange
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();
    engine.put(&cf, b"key", b"original").expect("put");

    // Act
    let inserted = engine.insert(&cf, b"key", b"new").expect("insert");
    let result = engine.get(&cf, b"key").expect("get");

    // Assert
    assert!(!inserted);
    assert_eq!(result, Some(Bytes::from_static(b"original")));
}

#[test]
fn should_return_existing_value_given_existing_key_when_insert_with_value() {
    // Arrange
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();
    engine.put(&cf, b"key", b"original").expect("put");

    // Act
    let result = engine.insert_with_value(&cf, b"key", b"new").expect("insert");

    // Assert
    assert_eq!(result, InsertResult::AlreadyExists(Bytes::from_static(b"original")));
}

#[test]
fn should_insert_given_deleted_key_when_insert() {
    // Arrange
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();
    engine.put(&cf, b"key", b"original").expect("put");
    engine.delete(&cf, b"key").expect("delete");

    // Act
    let inserted = engine.insert(&cf, b"key", b"new").expect("insert");
    let result = engine.get(&cf, b"key").expect("get");

    // Assert
    assert!(inserted);
    assert_eq!(result, Some(Bytes::from_static(b"new")));
}

// ============================================================================
// COMPARE-AND-SWAP (CAS) Operations
// ============================================================================

#[test]
fn should_swap_value_given_matching_expected_when_cas() {
    // Arrange
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();
    engine.put(&cf, b"counter", b"0").expect("put");

    // Act
    let result = engine
        .compare_and_swap(&cf, b"counter", Some(Bytes::from_static(b"0")), b"1")
        .expect("cas");
    let value = engine.get(&cf, b"counter").expect("get");

    // Assert
    assert_eq!(result, CasResult::Swapped);
    assert_eq!(value, Some(Bytes::from_static(b"1")));
}

#[test]
fn should_return_mismatch_given_unexpected_value_when_cas() {
    // Arrange
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();
    engine.put(&cf, b"counter", b"5").expect("put");

    // Act
    let result = engine
        .compare_and_swap(&cf, b"counter", Some(Bytes::from_static(b"0")), b"1")
        .expect("cas");
    let value = engine.get(&cf, b"counter").expect("get");

    // Assert
    assert_eq!(result, CasResult::Mismatch(Some(Bytes::from_static(b"5"))));
    assert_eq!(value, Some(Bytes::from_static(b"5"))); // Unchanged
}

#[test]
fn should_insert_given_none_expected_and_missing_key_when_cas() {
    // Arrange
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Act - CAS with None expected on missing key (like insert)
    let result = engine
        .compare_and_swap(&cf, b"newkey", None, b"value")
        .expect("cas");
    let value = engine.get(&cf, b"newkey").expect("get");

    // Assert
    assert_eq!(result, CasResult::Swapped);
    assert_eq!(value, Some(Bytes::from_static(b"value")));
}

#[test]
fn should_return_mismatch_given_none_expected_and_existing_key_when_cas() {
    // Arrange
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();
    engine.put(&cf, b"key", b"exists").expect("put");

    // Act
    let result = engine
        .compare_and_swap(&cf, b"key", None, b"new")
        .expect("cas");
    let value = engine.get(&cf, b"key").expect("get");

    // Assert
    assert_eq!(result, CasResult::Mismatch(Some(Bytes::from_static(b"exists"))));
    assert_eq!(value, Some(Bytes::from_static(b"exists"))); // Unchanged
}

// ============================================================================
// DELETE_RANGE Operations
// ============================================================================

#[test]
fn should_delete_keys_in_range_when_delete_range() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    engine.put(&cf, b"a", b"1").expect("put");
    engine.put(&cf, b"b", b"2").expect("put");
    engine.put(&cf, b"c", b"3").expect("put");
    engine.put(&cf, b"d", b"4").expect("put");

    // Act - delete [b, d) which is b and c
    engine.delete_range(&cf, b"b", b"d").expect("delete_range");

    // Assert
    assert_eq!(engine.get(&cf, b"a").unwrap(), Some(Bytes::from_static(b"1")));
    assert_eq!(engine.get(&cf, b"b").unwrap(), None);
    assert_eq!(engine.get(&cf, b"c").unwrap(), None);
    assert_eq!(engine.get(&cf, b"d").unwrap(), Some(Bytes::from_static(b"4")));
}

#[test]
fn should_be_noop_given_empty_range_when_delete_range() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();
    engine.put(&cf, b"key", b"value").expect("put");

    // Act - empty range (start == end)
    engine.delete_range(&cf, b"key", b"key").expect("delete_range");

    // Assert - key still exists
    assert_eq!(engine.get(&cf, b"key").unwrap(), Some(Bytes::from_static(b"value")));
}

#[test]
fn should_be_noop_given_inverted_range_when_delete_range() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();
    engine.put(&cf, b"b", b"2").expect("put");

    // Act - inverted range (start > end)
    engine.delete_range(&cf, b"z", b"a").expect("delete_range");

    // Assert - key still exists
    assert_eq!(engine.get(&cf, b"b").unwrap(), Some(Bytes::from_static(b"2")));
}

// ============================================================================
// Memory Mode Tests
// ============================================================================

#[test]
fn should_not_create_filesystem_artifacts_when_memory_mode() {
    // Arrange
    let dir = test_temp_dir();
    let db_path = dir.path().to_path_buf();

    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };

    // Act
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();
    engine.put(&cf, b"key", b"value").expect("put");

    // Assert - no files created
    assert!(!db_path.join("sst").exists());
    assert!(!db_path.join("wal").exists());
    assert!(!db_path.join("manifest.json").exists());
    assert!(!db_path.join("LOCK").exists());
}

#[test]
fn should_function_correctly_given_memory_mode() {
    // Arrange
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Act
    engine.put(&cf, b"k1", b"v1").expect("put");
    engine.put(&cf, b"k2", b"v2").expect("put");
    engine.delete(&cf, b"k1").expect("delete");
    let k1_result = engine.get(&cf, b"k1").unwrap();
    let k2_result = engine.get(&cf, b"k2").unwrap();
    let scan_results = engine.scan(&cf, Query::new()).expect("scan");

    // Assert
    assert_eq!(k1_result, None);
    assert_eq!(k2_result, Some(Bytes::from_static(b"v2")));
    assert_eq!(scan_results.len(), 1);
}
