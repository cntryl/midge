// Column family isolation tests
// Verifies that operations on different CFs are properly isolated.

use bytes::Bytes;
use cntryl_midge::api::column_family::ColumnFamilyConfig;
use cntryl_midge::{MidgeEngine, MidgeOptions, Query, StorageMode};

fn temp_dir() -> tempfile::TempDir {
    let cf = engine.default_column_family();
    tempfile::tempdir().expect("tempdir")
}

#[test]
fn should_isolate_keys_across_column_families() {
    let cf = engine.default_column_family();
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let cf1 = engine
        .create_column_family("cf1", ColumnFamilyConfig::default())
        .expect("create cf1");
    let cf2 = engine
        .create_column_family("cf2", ColumnFamilyConfig::default())
        .expect("create cf2");

    // Act - Write same key with different values to different CFs
    engine
        .put(&cf, &cf1, b"key", b"value_cf1")
        .expect("put cf1");
    engine
        .put(&cf, &cf2, b"key", b"value_cf2")
        .expect("put cf2");

    // Assert
    let result1 = engine.get(&cf1, b"key").expect("get cf1");
    let result2 = engine.get(&cf2, b"key").expect("get cf2");

    assert_eq!(result1, Some(Bytes::from_static(b"value_cf1")));
    assert_eq!(result2, Some(Bytes::from_static(b"value_cf2")));
}

#[test]
fn should_not_see_deleted_keys_from_other_cfs() {
    let cf = engine.default_column_family();
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let cf1 = engine
        .create_column_family("cf1", ColumnFamilyConfig::default())
        .expect("create cf1");
    let cf2 = engine
        .create_column_family("cf2", ColumnFamilyConfig::default())
        .expect("create cf2");

    // Write same key to both CFs
    engine.put(&cf, &cf1, b"key", b"value1").expect("put cf1");
    engine.put(&cf, &cf2, b"key", b"value2").expect("put cf2");

    // Act - Delete from cf1 only
    engine.delete(&cf1, b"key").expect("delete cf1");

    // Assert
    let result1 = engine.get(&cf1, b"key").expect("get cf1");
    let result2 = engine.get(&cf2, b"key").expect("get cf2");

    assert_eq!(result1, None, "Key should be deleted from cf1");
    assert_eq!(
        result2,
        Some(Bytes::from_static(b"value2")),
        "Key should still exist in cf2"
    );
}

#[test]
fn should_scan_only_within_column_family() {
    let cf = engine.default_column_family();
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let cf1 = engine
        .create_column_family("cf1", ColumnFamilyConfig::default())
        .expect("create cf1");
    let cf2 = engine
        .create_column_family("cf2", ColumnFamilyConfig::default())
        .expect("create cf2");

    // Write data to both CFs with overlapping key ranges
    engine.put(&cf, &cf1, b"key1", b"cf1_value1").expect("put");
    engine.put(&cf, &cf1, b"key2", b"cf1_value2").expect("put");
    engine.put(&cf, &cf1, b"key3", b"cf1_value3").expect("put");

    engine.put(&cf, &cf2, b"key1", b"cf2_value1").expect("put");
    engine.put(&cf, &cf2, b"key2", b"cf2_value2").expect("put");
    engine.put(&cf, &cf2, b"key3", b"cf2_value3").expect("put");

    // Act - Scan cf1
    let results_cf1 = engine
        .scan(
            &cf1,
            Query::new()
                .start_key(Bytes::from_static(b"key1"))
                .end_key(Bytes::from_static(b"key9")),
        )
        .expect("scan cf1");

    // Scan cf2
    let results_cf2 = engine
        .scan(
            &cf2,
            Query::new()
                .start_key(Bytes::from_static(b"key1"))
                .end_key(Bytes::from_static(b"key9")),
        )
        .expect("scan cf2");

    // Assert
    assert_eq!(results_cf1.len(), 3);
    assert_eq!(results_cf1[0].1, Bytes::from_static(b"cf1_value1"));
    assert_eq!(results_cf1[1].1, Bytes::from_static(b"cf1_value2"));
    assert_eq!(results_cf1[2].1, Bytes::from_static(b"cf1_value3"));

    assert_eq!(results_cf2.len(), 3);
    assert_eq!(results_cf2[0].1, Bytes::from_static(b"cf2_value1"));
    assert_eq!(results_cf2[1].1, Bytes::from_static(b"cf2_value2"));
    assert_eq!(results_cf2[2].1, Bytes::from_static(b"cf2_value3"));
}

#[test]
fn should_isolate_after_flush() {
    let cf = engine.default_column_family();
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let cf1 = engine
        .create_column_family("cf1", ColumnFamilyConfig::default())
        .expect("create cf1");
    let cf2 = engine
        .create_column_family("cf2", ColumnFamilyConfig::default())
        .expect("create cf2");

    // Write and flush data
    engine
        .put(&cf, &cf1, b"key", b"value_cf1")
        .expect("put cf1");
    engine
        .put(&cf, &cf2, b"key", b"value_cf2")
        .expect("put cf2");

    engine.flush().expect("flush");

    // Write more data after flush
    engine
        .put(&cf, &cf1, b"key2", b"value2_cf1")
        .expect("put cf1");
    engine
        .put(&cf, &cf2, b"key2", b"value2_cf2")
        .expect("put cf2");

    // Act & Assert - Read from both CFs
    let result1 = engine.get(&cf1, b"key").expect("get cf1");
    let result2 = engine.get(&cf2, b"key").expect("get cf2");
    let result3 = engine.get(&cf1, b"key2").expect("get cf1 key2");
    let result4 = engine.get(&cf2, b"key2").expect("get cf2 key2");

    assert_eq!(result1, Some(Bytes::from_static(b"value_cf1")));
    assert_eq!(result2, Some(Bytes::from_static(b"value_cf2")));
    assert_eq!(result3, Some(Bytes::from_static(b"value2_cf1")));
    assert_eq!(result4, Some(Bytes::from_static(b"value2_cf2")));
}

#[test]
fn should_isolate_across_restart() {
    // Arrange
    let dir = temp_dir();
    let db_path = dir.path().to_path_buf();

    // First session: write data to both CFs
    {
        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk {
                db_path: db_path.clone(),
            },
            enable_compaction: false,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");

        let cf1 = engine
            .create_column_family("cf1", ColumnFamilyConfig::default())
            .expect("create cf1");
        let cf2 = engine
            .create_column_family("cf2", ColumnFamilyConfig::default())
            .expect("create cf2");

        engine
            .put(&cf1, &cf1, b"shared_key", b"cf1_value")
            .expect("put cf1");
        engine
            .put(&cf2, &cf2, b"shared_key", b"cf2_value")
            .expect("put cf2");
    }

    // Second session: verify isolation persists
    let opts2 = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_path.clone(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let engine2 = MidgeEngine::open(opts2).expect("reopen");

    let cf1 = engine2.get_column_family("cf1").expect("get cf1");
    let cf2 = engine2.get_column_family("cf2").expect("get cf2");

    // Act
    let result1 = engine2.get(&cf1, b"shared_key").expect("get cf1");
    let result2 = engine2.get(&cf2, b"shared_key").expect("get cf2");

    // Assert
    assert_eq!(result1, Some(Bytes::from_static(b"cf1_value")));
    assert_eq!(result2, Some(Bytes::from_static(b"cf2_value")));
}

#[test]
fn should_not_read_default_cf_data_from_custom_cf() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let default_cf = engine.default_column_family();
    let custom_cf = engine
        .create_column_family("custom", ColumnFamilyConfig::default())
        .expect("create custom");

    // Write to default CF
    engine
        .put(&cf, &default_cf, b"key", b"default_value")
        .expect("put default");

    // Act - Try to read from custom CF
    let result = engine.get(&custom_cf, b"key").expect("get custom");

    // Assert
    assert_eq!(result, None, "Custom CF should not see default CF data");

    // Verify data exists in default CF
    let default_result = engine.get(&default_cf, b"key").expect("get default");
    assert_eq!(default_result, Some(Bytes::from_static(b"default_value")));
}

#[test]
fn should_handle_concurrent_writes_to_different_cfs() {
    let cf = engine.default_column_family();
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let cf1 = engine
        .create_column_family("cf1", ColumnFamilyConfig::default())
        .expect("create cf1");
    let cf2 = engine
        .create_column_family("cf2", ColumnFamilyConfig::default())
        .expect("create cf2");

    // Act - Interleave writes to different CFs
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        engine
            .put(&cf, &cf1, key.as_bytes(), b"cf1_val")
            .expect("put cf1");
        engine
            .put(&cf, &cf2, key.as_bytes(), b"cf2_val")
            .expect("put cf2");
    }

    // Assert - Verify all writes went to correct CFs
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        let val1 = engine.get(&cf1, key.as_bytes()).expect("get cf1");
        let val2 = engine.get(&cf2, key.as_bytes()).expect("get cf2");

        assert_eq!(val1, Some(Bytes::from_static(b"cf1_val")));
        assert_eq!(val2, Some(Bytes::from_static(b"cf2_val")));
    }
}
