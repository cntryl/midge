// Column family drop prevention test
// Verifies that dropping a CF with unflushed data returns an error.

use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use cntryl_midge::api::column_family::ColumnFamilyConfig;

fn temp_dir() -> tempfile::TempDir {
    tempfile::tempdir().expect("tempdir")
}

#[test]
fn should_reject_drop_when_active_memtable_has_data() {
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
    
    let cf = engine
        .create_column_family("test_cf", ColumnFamilyConfig::default())
        .expect("create CF");
    
    // Write data but don't flush
    engine.put(&cf, b"key1", b"value1").expect("put");
    engine.put(&cf, b"key2", b"value2").expect("put");

    // Act
    let result = engine.drop_column_family(&cf);

    // Assert
    assert!(
        result.is_err(),
        "Expected error when dropping CF with unflushed data"
    );
    
    let err_msg = format!("{:?}", result.unwrap_err());
    assert!(
        err_msg.contains("unflushed data") || err_msg.contains("flush"),
        "Error message should mention unflushed data or flush requirement. Got: {}",
        err_msg
    );
}

#[test]
fn should_preserve_cf_after_restart_when_drop_fails() {
    // Arrange
    let dir = temp_dir();
    let db_path = dir.path().to_path_buf();
    
    // First session: create CF with data
    {
        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk {
                db_path: db_path.clone(),
            },
            enable_compaction: false,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        
        let cf = engine
            .create_column_family("test_cf", ColumnFamilyConfig::default())
            .expect("create CF");
        
        engine.put(&cf, b"key1", b"value1").expect("put");
        
        // Try to drop with unflushed data - should fail
        let result = engine.drop_column_family(&cf);
        assert!(result.is_err(), "Drop should fail with unflushed data");
    }
    
    // Second session: verify CF still exists after restart
    let opts2 = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_path.clone(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let engine2 = MidgeEngine::open(opts2).expect("reopen");
    
    // Act
    let cf_list = engine2.list_column_families();
    let names: Vec<_> = cf_list.iter().map(|h| h.name()).collect();
    
    // Assert - CF should still exist since drop failed
    assert!(
        names.contains(&"test_cf"),
        "CF should still exist after failed drop attempt"
    );
    
    // Verify data is still accessible
    let cf = engine2.get_column_family("test_cf").expect("get CF");
    let value = engine2.get(&cf, b"key1").expect("get");
    assert_eq!(value, Some(bytes::Bytes::from_static(b"value1")));
}

#[test]
fn should_allow_drop_when_cf_has_no_data() {
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
    
    let cf = engine
        .create_column_family("empty_cf", ColumnFamilyConfig::default())
        .expect("create CF");

    // Act - Drop CF without writing any data
    let result = engine.drop_column_family(&cf);

    // Assert
    assert!(result.is_ok(), "Should allow dropping empty CF");
    
    let cf_list = engine.list_column_families();
    let names: Vec<_> = cf_list.iter().map(|h| h.name()).collect();
    assert!(!names.contains(&"empty_cf"));
}

#[test]
fn should_reject_drop_with_immutable_memtables() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        memtable_size: 100, // Very small to force freeze
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    
    let cf = engine
        .create_column_family("test_cf", ColumnFamilyConfig::default())
        .expect("create CF");
    
    // Write enough data to trigger memtable freeze (creating immutables)
    for i in 0..1000 {
        let key = format!("key_{:08}", i);
        let value = format!("value_{:08}", i);
        engine.put(&cf, key.as_bytes(), value.as_bytes()).expect("put");
    }
    
    // Don't flush - this should leave immutable memtables

    // Act
    let result = engine.drop_column_family(&cf);

    // Assert
    assert!(
        result.is_err(),
        "Expected error when dropping CF with immutable memtables"
    );
    
    let err_msg = format!("{:?}", result.unwrap_err());
    assert!(
        err_msg.contains("unflushed") || err_msg.contains("immutable") || err_msg.contains("flush"),
        "Error message should mention unflushed/immutable memtables. Got: {}",
        err_msg
    );
}

#[test]
fn should_allow_drop_other_cfs_independently() {
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
    
    // Write to cf1 only
    engine.put(&cf1, b"key", b"value").expect("put");

    // Act - Try to drop cf2 (empty) while cf1 has unflushed data
    let result_cf2 = engine.drop_column_family(&cf2);
    
    // Assert
    assert!(
        result_cf2.is_ok(),
        "Should allow dropping empty CF even if other CF has unflushed data"
    );
    
    // cf1 should not be droppable since it has unflushed data
    let result_cf1 = engine.drop_column_family(&cf1);
    assert!(
        result_cf1.is_err(),
        "cf1 should not be droppable with unflushed data"
    );
    
    // Verify cf2 was dropped but cf1 still exists
    let cf_list = engine.list_column_families();
    let names: Vec<_> = cf_list.iter().map(|h| h.name()).collect();
    assert!(names.contains(&"cf1"), "cf1 should still exist");
    assert!(!names.contains(&"cf2"), "cf2 should be dropped");
}
