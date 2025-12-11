//! Column Family Lifecycle Tests
//!
//! Tests for column family creation, deletion, isolation, persistence, and error handling.
//!
//! # Test Coverage
//! - Create: creating column families with various configurations
//! - Drop: dropping column families and data cleanup
//! - List: listing existing column families
//! - Isolation: key isolation between column families
//! - Persistence: column family metadata persistence across restarts
//! - Error Handling: invalid operations and edge cases

mod common;

use cntryl_midge::{ColumnFamilyConfig, MidgeEngine, MidgeOptions, StorageMode};
use cntryl_midge::testkit::{
    assert_get_equals_cf, assert_key_absent_cf, new_engine, test_temp_dir, with_engine_restart,
};

// ============================================================================
// CREATE TESTS
// ============================================================================

#[test]
fn should_create_column_family_given_valid_name_when_engine_open() {
    // Arrange
    let (_dir, eng) = new_engine();

    // Act
    let result = eng.create_column_family("test_cf", ColumnFamilyConfig::default());

    // Assert
    assert!(result.is_ok());
    let cf = result.unwrap();
    assert_eq!(cf.name(), "test_cf");
}

#[test]
fn should_create_multiple_column_families_given_unique_names_when_engine_open() {
    // Arrange
    let (_dir, eng) = new_engine();

    // Act
    let cf1 = eng
        .create_column_family("cf1", ColumnFamilyConfig::default())
        .expect("create cf1");
    let cf2 = eng
        .create_column_family("cf2", ColumnFamilyConfig::default())
        .expect("create cf2");
    let cf3 = eng
        .create_column_family("cf3", ColumnFamilyConfig::default())
        .expect("create cf3");

    // Assert
    assert_eq!(cf1.name(), "cf1");
    assert_eq!(cf2.name(), "cf2");
    assert_eq!(cf3.name(), "cf3");
    assert_ne!(cf1.id(), cf2.id());
    assert_ne!(cf2.id(), cf3.id());
}

#[test]
fn should_fail_create_column_family_given_duplicate_name_when_cf_exists() {
    // Arrange
    let (_dir, eng) = new_engine();
    eng.create_column_family("test_cf", ColumnFamilyConfig::default())
        .expect("create first cf");

    // Act
    let result = eng.create_column_family("test_cf", ColumnFamilyConfig::default());

    // Assert
    assert!(result.is_err());
}

#[test]
fn should_create_column_family_with_custom_config_given_config_when_creating() {
    // Arrange
    let (_dir, eng) = new_engine();
    let config = ColumnFamilyConfig {
        memtable_max_bytes: 32 * 1024 * 1024, // 32 MB
        bloom_bits_per_key: 15,
        ..Default::default()
    };

    // Act
    let result = eng.create_column_family("custom_cf", config);

    // Assert
    assert!(result.is_ok());
}

// ============================================================================
// DROP TESTS
// ============================================================================

#[test]
fn should_drop_column_family_given_empty_cf_when_requested() {
    // Arrange
    let (_dir, eng) = new_engine();
    let cf = eng
        .create_column_family("temp_cf", ColumnFamilyConfig::default())
        .expect("create cf");

    // Act
    let result = eng.drop_column_family(&cf);

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_drop_column_family_given_flushed_data_when_requested() {
    // Arrange
    let (_dir, eng) = new_engine();
    let cf = eng
        .create_column_family("temp_cf", ColumnFamilyConfig::default())
        .expect("create cf");
    eng.put(&cf, b"key1", b"value1").expect("put");
    eng.flush_cf(&cf).expect("flush");

    // Act
    let result = eng.drop_column_family(&cf);

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_fail_drop_column_family_given_unflushed_data_when_memtable_not_empty() {
    // Arrange
    let (_dir, eng) = new_engine();
    let cf = eng
        .create_column_family("temp_cf", ColumnFamilyConfig::default())
        .expect("create cf");
    eng.put(&cf, b"key1", b"value1").expect("put");
    // No flush - data still in memtable

    // Act
    let result = eng.drop_column_family(&cf);

    // Assert
    assert!(result.is_err());
}

#[test]
fn should_fail_drop_default_column_family_given_drop_request_when_default_cf() {
    // Arrange
    let (_dir, eng) = new_engine();
    let default_cf = eng.default_column_family();

    // Act
    let result = eng.drop_column_family(&default_cf);

    // Assert
    assert!(result.is_err());
}

#[test]
fn should_invalidate_handle_given_cf_dropped_when_accessing() {
    // Arrange
    let (_dir, eng) = new_engine();
    let cf = eng
        .create_column_family("test_cf", ColumnFamilyConfig::default())
        .expect("create cf");
    eng.put(&cf, b"key1", b"val1").expect("put");
    eng.flush_cf(&cf).expect("flush");

    // Act
    eng.drop_column_family(&cf).expect("drop");
    let result = eng.get(&cf, b"key1");

    // Assert - operations on dropped CF should fail
    assert!(result.is_err());
}

#[test]
fn should_delete_cf_data_given_cf_dropped_when_persisted() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");

    let cf = eng
        .create_column_family("temp_cf", ColumnFamilyConfig::default())
        .expect("create cf");
    for i in 0..100 {
        let key = format!("key{:03}", i);
        eng.put(&cf, key.as_bytes(), b"val").expect("put");
    }
    eng.flush_cf(&cf).expect("flush");
    eng.drop_column_family(&cf).expect("drop");
    drop(eng);

    // Act - reopen and recreate CF with same name
    let opts2 = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng2 = MidgeEngine::open(opts2).expect("reopen");
    let cf_new = eng2
        .create_column_family("temp_cf", ColumnFamilyConfig::default())
        .expect("recreate cf");

    // Assert - fresh CF should have no data from old CF
    assert_key_absent_cf(&eng2, &cf_new, b"key050");
}

#[test]
fn should_allow_recreate_cf_with_same_name_given_cf_dropped_when_creating() {
    // Arrange
    let (_dir, eng) = new_engine();
    let cf1 = eng
        .create_column_family("reusable_cf", ColumnFamilyConfig::default())
        .expect("create first");
    eng.put(&cf1, b"key1", b"value1").expect("put");
    eng.flush_cf(&cf1).expect("flush");
    eng.drop_column_family(&cf1).expect("drop");

    // Act
    let cf2 = eng.create_column_family("reusable_cf", ColumnFamilyConfig::default());

    // Assert
    assert!(cf2.is_ok());
    let cf2 = cf2.unwrap();
    assert_key_absent_cf(&eng, &cf2, b"key1"); // Fresh CF, no old data
}

// ============================================================================
// LIST TESTS
// ============================================================================

#[test]
fn should_list_default_cf_only_given_no_custom_cfs_when_listing() {
    // Arrange
    let (_dir, eng) = new_engine();

    // Act
    let cfs = eng.list_column_families();

    // Assert
    assert_eq!(cfs.len(), 1);
    assert_eq!(cfs[0].name(), "default");
}

#[test]
fn should_list_all_column_families_given_multiple_cfs_when_listing() {
    // Arrange
    let (_dir, eng) = new_engine();
    eng.create_column_family("cf1", ColumnFamilyConfig::default())
        .expect("create cf1");
    eng.create_column_family("cf2", ColumnFamilyConfig::default())
        .expect("create cf2");

    // Act
    let cfs = eng.list_column_families();

    // Assert
    assert_eq!(cfs.len(), 3);
    let names: Vec<&str> = cfs.iter().map(|cf| cf.name()).collect();
    assert!(names.contains(&"default"));
    assert!(names.contains(&"cf1"));
    assert!(names.contains(&"cf2"));
}

#[test]
fn should_not_list_dropped_cf_given_cf_dropped_when_listing() {
    // Arrange
    let (_dir, eng) = new_engine();
    let cf = eng
        .create_column_family("temp_cf", ColumnFamilyConfig::default())
        .expect("create cf");
    eng.drop_column_family(&cf).expect("drop");

    // Act
    let cfs = eng.list_column_families();

    // Assert
    let names: Vec<&str> = cfs.iter().map(|cf| cf.name()).collect();
    assert!(!names.contains(&"temp_cf"));
}

// ============================================================================
// ISOLATION TESTS
// ============================================================================

#[test]
fn should_isolate_keys_given_same_key_in_different_cfs_when_reading() {
    // Arrange
    let (_dir, eng) = new_engine();
    let default_cf = eng.default_column_family();
    let cf2 = eng
        .create_column_family("cf2", ColumnFamilyConfig::default())
        .expect("create CF");

    // Act - write same key to both CFs
    eng.put(&default_cf, b"shared_key", b"default_value")
        .expect("put default");
    eng.put(&cf2, b"shared_key", b"cf2_value").expect("put cf2");

    // Assert - each CF should only see its own value
    assert_get_equals_cf(&eng, &default_cf, b"shared_key", b"default_value");
    assert_get_equals_cf(&eng, &cf2, b"shared_key", b"cf2_value");
}

#[test]
fn should_isolate_deletes_given_delete_in_one_cf_when_other_cf_has_same_key() {
    // Arrange
    let (_dir, eng) = new_engine();
    let cf1 = eng
        .create_column_family("cf1", ColumnFamilyConfig::default())
        .expect("create cf1");
    let cf2 = eng
        .create_column_family("cf2", ColumnFamilyConfig::default())
        .expect("create cf2");

    eng.put(&cf1, b"shared_key", b"value1").expect("put cf1");
    eng.put(&cf2, b"shared_key", b"value2").expect("put cf2");

    // Act - delete from cf1 only
    eng.delete(&cf1, b"shared_key").expect("delete cf1");

    // Assert - cf2 should still have its value
    assert_key_absent_cf(&eng, &cf1, b"shared_key");
    assert_get_equals_cf(&eng, &cf2, b"shared_key", b"value2");
}

#[test]
fn should_isolate_data_given_different_data_volumes_when_reading() {
    // Arrange
    let (_dir, eng) = new_engine();
    let default_cf = eng.default_column_family();
    let cf2 = eng
        .create_column_family("cf2", ColumnFamilyConfig::default())
        .expect("create CF");

    // Act - write different amounts to each CF
    for i in 0..100 {
        eng.put(
            &default_cf,
            format!("key{:03}", i).as_bytes(),
            b"value_default",
        )
        .expect("put default");
    }
    for i in 0..200 {
        eng.put(&cf2, format!("key{:03}", i).as_bytes(), b"value_cf2")
            .expect("put cf2");
    }

    // Assert - both CFs should maintain their data independently
    assert_get_equals_cf(&eng, &default_cf, b"key050", b"value_default");
    assert_get_equals_cf(&eng, &cf2, b"key050", b"value_cf2");
    assert_get_equals_cf(&eng, &cf2, b"key150", b"value_cf2");

    // Verify cf2's extra keys don't exist in default
    assert_key_absent_cf(&eng, &default_cf, b"key150");
}

#[test]
fn should_isolate_compaction_given_per_cf_data_when_compacting() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: true,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");

    let cf1 = eng
        .create_column_family("cf1", ColumnFamilyConfig::default())
        .expect("create cf1");
    let cf2 = eng
        .create_column_family("cf2", ColumnFamilyConfig::default())
        .expect("create cf2");

    // Write to both CFs
    for i in 0..100 {
        let key = format!("key{:03}", i);
        eng.put(&cf1, key.as_bytes(), b"val1").expect("put cf1");
        eng.put(&cf2, key.as_bytes(), b"val2").expect("put cf2");
    }
    eng.flush().expect("flush");

    // Act - compact only cf1
    eng.compact_range(&cf1, Some(b""), Some(b"~"))
        .expect("compact cf1");

    // Assert - both CFs should still have data after compacting one
    assert_get_equals_cf(&eng, &cf1, b"key050", b"val1");
    assert_get_equals_cf(&eng, &cf2, b"key050", b"val2");
}

// ============================================================================
// PERSISTENCE TESTS
// ============================================================================

#[test]
fn should_persist_cf_metadata_given_restart_when_cf_created() {
    // Arrange
    let dir = test_temp_dir();
    {
        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk {
                db_path: dir.path().to_path_buf(),
            },
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");

        let cf = eng
            .create_column_family("persistent_cf", ColumnFamilyConfig::default())
            .expect("create cf");
        eng.put(&cf, b"key1", b"val1").expect("put");
        eng.flush_cf(&cf).expect("flush");
    }

    // Act - reopen
    let opts2 = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng2 = MidgeEngine::open(opts2).expect("reopen");

    // Assert - CF should exist and contain data
    let all_cfs = eng2.list_column_families();
    let cf_names: Vec<&str> = all_cfs.iter().map(|cf| cf.name()).collect();
    assert!(
        cf_names.contains(&"persistent_cf"),
        "CF should persist across restarts"
    );
}

#[test]
fn should_persist_cf_data_given_restart_when_data_flushed() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };

    // Act
    with_engine_restart(
        opts.clone(),
        |eng| {
            let cf = eng
                .create_column_family("data_cf", ColumnFamilyConfig::default())
                .expect("create cf");
            eng.put(&cf, b"persistent_key", b"persistent_value")
                .expect("put");
            eng.flush_cf(&cf).expect("flush");
        },
        |eng| {
            // Assert - data should persist
            let cf = eng.get_column_family("data_cf").expect("get cf");
            assert_get_equals_cf(eng, &cf, b"persistent_key", b"persistent_value");
        },
    );
}

#[test]
fn should_persist_multiple_cfs_given_restart_when_all_flushed() {
    // Arrange
    let dir = test_temp_dir();
    {
        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk {
                db_path: dir.path().to_path_buf(),
            },
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");

        let cf1 = eng
            .create_column_family("cf1", ColumnFamilyConfig::default())
            .expect("create cf1");
        let cf2 = eng
            .create_column_family("cf2", ColumnFamilyConfig::default())
            .expect("create cf2");

        eng.put(&cf1, b"key1", b"value1").expect("put cf1");
        eng.put(&cf2, b"key2", b"value2").expect("put cf2");
        eng.flush().expect("flush all");
    }

    // Act - reopen
    let opts2 = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng2 = MidgeEngine::open(opts2).expect("reopen");

    // Assert - all CFs and their data should persist
    let all_cfs = eng2.list_column_families();
    assert!(all_cfs.len() >= 3); // default + cf1 + cf2

    let cf1 = eng2.get_column_family("cf1").expect("get cf1");
    let cf2 = eng2.get_column_family("cf2").expect("get cf2");
    assert_get_equals_cf(&eng2, &cf1, b"key1", b"value1");
    assert_get_equals_cf(&eng2, &cf2, b"key2", b"value2");
}

#[test]
fn should_persist_cf_drop_given_restart_when_cf_was_dropped() {
    // Arrange
    let dir = test_temp_dir();
    {
        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk {
                db_path: dir.path().to_path_buf(),
            },
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");

        let cf = eng
            .create_column_family("dropped_cf", ColumnFamilyConfig::default())
            .expect("create cf");
        eng.put(&cf, b"key1", b"value1").expect("put");
        eng.flush_cf(&cf).expect("flush");
        eng.drop_column_family(&cf).expect("drop");
    }

    // Act - reopen
    let opts2 = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng2 = MidgeEngine::open(opts2).expect("reopen");

    // Assert - dropped CF should not exist
    let all_cfs = eng2.list_column_families();
    let cf_names: Vec<&str> = all_cfs.iter().map(|cf| cf.name()).collect();
    assert!(
        !cf_names.contains(&"dropped_cf"),
        "Dropped CF should not persist"
    );
}

// ============================================================================
// GET/LOOKUP TESTS
// ============================================================================

#[test]
fn should_get_column_family_by_name_given_existing_cf_when_querying() {
    // Arrange
    let (_dir, eng) = new_engine();
    eng.create_column_family("lookup_cf", ColumnFamilyConfig::default())
        .expect("create cf");

    // Act
    let result = eng.get_column_family("lookup_cf");

    // Assert
    assert!(result.is_ok());
    assert_eq!(result.unwrap().name(), "lookup_cf");
}

#[test]
fn should_fail_get_column_family_given_nonexistent_name_when_querying() {
    // Arrange
    let (_dir, eng) = new_engine();

    // Act
    let result = eng.get_column_family("nonexistent_cf");

    // Assert
    assert!(result.is_err());
}

#[test]
fn should_get_default_column_family_given_fresh_engine_when_querying() {
    // Arrange
    let (_dir, eng) = new_engine();

    // Act
    let default_cf = eng.default_column_family();

    // Assert
    assert_eq!(default_cf.name(), "default");
}

// ============================================================================
// EDGE CASES
// ============================================================================

#[test]
fn should_isolate_cf_after_flush_given_same_key_when_reading() {
    // Arrange
    let (_dir, eng) = new_engine();
    let cf1 = eng
        .create_column_family("cf1", ColumnFamilyConfig::default())
        .expect("create cf1");
    let cf2 = eng
        .create_column_family("cf2", ColumnFamilyConfig::default())
        .expect("create cf2");

    eng.put(&cf1, b"key", b"value1").expect("put cf1");
    eng.put(&cf2, b"key", b"value2").expect("put cf2");

    // Act - flush both
    eng.flush().expect("flush");

    // Assert - isolation should be maintained after flush
    assert_get_equals_cf(&eng, &cf1, b"key", b"value1");
    assert_get_equals_cf(&eng, &cf2, b"key", b"value2");
}

#[test]
fn should_handle_operations_on_default_cf_given_custom_cfs_exist_when_operating() {
    // Arrange
    let (_dir, eng) = new_engine();
    let default_cf = eng.default_column_family();
    eng.create_column_family("cf1", ColumnFamilyConfig::default())
        .expect("create cf1");
    eng.create_column_family("cf2", ColumnFamilyConfig::default())
        .expect("create cf2");

    // Act - operations on default CF
    eng.put(&default_cf, b"default_key", b"default_value")
        .expect("put");
    eng.flush_cf(&default_cf).expect("flush");

    // Assert - default CF should work normally
    assert_get_equals_cf(&eng, &default_cf, b"default_key", b"default_value");
}

#[test]
fn should_maintain_cf_isolation_given_many_cfs_when_operating() {
    // Arrange
    let (_dir, eng) = new_engine();
    let mut cfs = vec![eng.default_column_family()];

    // Create 5 additional CFs
    for i in 1..=5 {
        let cf = eng
            .create_column_family(&format!("cf{}", i), ColumnFamilyConfig::default())
            .expect("create cf");
        cfs.push(cf);
    }

    // Act - write unique data to each CF
    for (i, cf) in cfs.iter().enumerate() {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        eng.put(cf, key.as_bytes(), value.as_bytes()).expect("put");
    }

    // Assert - each CF should only see its own data
    for (i, cf) in cfs.iter().enumerate() {
        let key = format!("key_{}", i);
        let expected_value = format!("value_{}", i);
        assert_get_equals_cf(&eng, cf, key.as_bytes(), expected_value.as_bytes());

        // Verify other keys don't exist
        let other_key = format!("key_{}", (i + 1) % cfs.len());
        if other_key != key {
            assert_key_absent_cf(&eng, cf, other_key.as_bytes());
        }
    }
}
