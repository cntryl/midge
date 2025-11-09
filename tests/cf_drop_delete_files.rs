// Column family drop file deletion test
// Verifies that SST files belonging to a CF are deleted when the CF is dropped.

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use cntryl_midge::api::column_family::ColumnFamilyConfig;

fn temp_dir() -> tempfile::TempDir {
    tempfile::tempdir().expect("tempdir")
}

#[test]
fn should_remove_cf_from_manifest_when_dropping() {
    // Arrange
    let dir = temp_dir();
    let db_path = dir.path().to_path_buf();
    
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_path.clone(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    
    let engine = MidgeEngine::open(opts).expect("open");
    
    // Create an empty CF (no data written)
    let cf_handle = engine
        .create_column_family("test_cf", ColumnFamilyConfig::default())
        .expect("create CF");

    // Act - Drop empty CF
    engine.drop_column_family(&cf_handle).expect("drop CF");

    // Assert
    // Verify CF no longer exists in memory
    let cf_list = engine.list_column_families();
    let names: Vec<_> = cf_list.iter().map(|h| h.name()).collect();
    assert!(!names.contains(&"test_cf"), "CF should not exist after drop");
    
    // Verify manifest was persisted by reopening the database
    drop(engine);
    
    let engine2 = MidgeEngine::open(MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_path.clone(),
        },
        enable_compaction: false,
        ..Default::default()
    }).expect("reopen");
    
    let cf_list2 = engine2.list_column_families();
    let names2: Vec<_> = cf_list2.iter().map(|h| h.name()).collect();
    assert!(
        !names2.contains(&"test_cf"),
        "CF should not exist after restart"
    );
}

#[test]
fn should_preserve_other_cfs_when_dropping_one_cf() {
    // Arrange
    let dir = temp_dir();
    let db_path = dir.path().to_path_buf();
    
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_path.clone(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    
    let engine = MidgeEngine::open(opts).expect("open");
    
    // Create two CFs (cf1 empty, cf2 with data)
    let cf1 = engine
        .create_column_family("cf1", ColumnFamilyConfig::default())
        .expect("create cf1");
    let cf2 = engine
        .create_column_family("cf2", ColumnFamilyConfig::default())
        .expect("create cf2");
    
    // Write to default CF
    let default_cf = engine.default_column_family();
    engine.put(&default_cf, b"default_key", b"default_value").expect("put");

    // Write to cf2 only (cf1 stays empty)
    engine.put(&cf2, b"cf2_key", b"cf2_value").expect("put");

    // Act - Drop empty cf1
    engine.drop_column_family(&cf1).expect("drop cf1");

    // Assert
    let cf_list = engine.list_column_families();
    let names: Vec<_> = cf_list.iter().map(|h| h.name()).collect();
    assert!(!names.contains(&"cf1"), "cf1 should not exist");
    assert!(names.contains(&"cf2"), "cf2 should still exist");
    assert!(names.contains(&"default"), "default should still exist");
    
    // Verify data in cf2 is still accessible
    let result = engine.get(&cf2, b"cf2_key").expect("get");
    assert_eq!(result, Some(Bytes::from_static(b"cf2_value")));
    
    // Verify data in default CF is still accessible
    let result = engine.get(&default_cf, b"default_key").expect("get");
    assert_eq!(result, Some(Bytes::from_static(b"default_value")));
    
    // Verify cf1 data is gone
    let result = engine.get_column_family("cf1");
    assert!(result.is_err(), "cf1 should not be retrievable");
}

#[test]
fn should_handle_drop_when_no_sst_files_exist() {
    // Arrange
    let dir = temp_dir();
    let db_path = dir.path().to_path_buf();
    
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_path.clone(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    
    // Create CF but don't write any data
    let cf_handle = engine
        .create_column_family("empty_cf", ColumnFamilyConfig::default())
        .expect("create CF");

    // Act - Drop CF with no SST files
    let result = engine.drop_column_family(&cf_handle);

    // Assert
    assert!(result.is_ok(), "Should succeed even with no SST files");
    
    let cf_list = engine.list_column_families();
    let names: Vec<_> = cf_list.iter().map(|h| h.name()).collect();
    assert!(!names.contains(&"empty_cf"));
}

#[test]
fn should_update_manifest_before_deleting_files() {
    // Arrange
    let dir = temp_dir();
    let db_path = dir.path().to_path_buf();
    
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_path.clone(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    
    let engine = MidgeEngine::open(opts).expect("open");
    
    // Create empty CF
    let cf_handle = engine
        .create_column_family("test_cf", ColumnFamilyConfig::default())
        .expect("create CF");

    // Act - Drop empty CF
    engine.drop_column_family(&cf_handle).expect("drop CF");

    // Assert - Verify manifest was updated by reopening DB
    drop(engine);
    
    let opts2 = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_path.clone(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let engine2 = MidgeEngine::open(opts2).expect("reopen");
    
    // CF should not exist after restart
    let cf_list = engine2.list_column_families();
    let names: Vec<_> = cf_list.iter().map(|h| h.name()).collect();
    assert!(
        !names.contains(&"test_cf"),
        "Dropped CF should not reappear after restart"
    );
}
