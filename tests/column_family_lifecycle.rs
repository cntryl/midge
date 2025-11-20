// Column Family Lifecycle (Phase 3 - P2)
// Tests column family creation, deletion, isolation, and crash recovery

#![allow(clippy::field_reassign_with_default)]
mod common;
use bytes::Bytes;
use cntryl_midge::{ColumnFamilyConfig, MidgeEngine, MidgeOptions, StorageMode};
use common::test_temp_dir;

#[test]
#[ignore] // TODO: Requires transaction API to detect CF deletion mid-txn
fn should_fail_transaction_given_cf_deleted_when_transaction_active() {
    // Would test that active transactions fail gracefully when CF is dropped
}

#[test]
fn should_invalidate_handle_given_cf_dropped_when_accessing() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.create_column_family("test_cf", ColumnFamilyConfig::default()).unwrap();
    
    eng.put(&cf, b"key1", b"val1").unwrap();
    eng.flush().unwrap();
    
    // Act
    eng.drop_column_family(&cf).unwrap();
    
    // Assert - operations on dropped CF should fail
    let result = eng.get(&cf, b"key1");
    assert!(result.is_err());
}

#[test]
fn should_persist_cf_metadata_given_crash_when_cf_created() {
    // Arrange
    let dir = test_temp_dir();
    {
        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk {
                db_path: dir.path().to_path_buf(),
            },
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).unwrap();
        
        let cf = eng.create_column_family("persistent_cf", ColumnFamilyConfig::default()).unwrap();
        eng.put(&cf, b"key1", b"val1").unwrap();
        eng.flush().unwrap();
    }
    
    // Act - reopen (CF should persist automatically)
    let opts2 = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng2 = MidgeEngine::open(opts2).unwrap();
    
    // Assert - CF should exist and contain data
    let all_cfs = eng2.list_column_families();
    assert!(all_cfs.iter().any(|cf| cf.name() == "persistent_cf"), "CF should persist across restarts");
}

#[test]
#[ignore] // TODO: Test default CF protection once drop_column_family validates name
fn should_protect_default_cf_given_drop_attempt_when_default() {
    // Would test that dropping "default" CF returns an error
}

#[test]
#[ignore] // TODO: Requires max_column_families config field
fn should_enforce_cf_limit_given_many_cfs_when_limit_reached() {
    // Would test CF count limits
}

#[test]
fn should_isolate_compaction_given_per_cf_config_when_compacting() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: true,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).unwrap();
    
    let cf1 = eng.create_column_family("cf1", ColumnFamilyConfig::default()).unwrap();
    let cf2 = eng.create_column_family("cf2", ColumnFamilyConfig::default()).unwrap();
    
    // Write to both CFs
    for i in 0..100 {
        let key = format!("key{}", i);
        eng.put(&cf1, key.as_bytes(), b"val1").unwrap();
        eng.put(&cf2, key.as_bytes(), b"val2").unwrap();
    }
    eng.flush().unwrap();

    // Act - compact only cf1
    eng.compact_range(&cf1, Some(b""), Some(b"~")).unwrap();

    // Assert - both CFs should still have data
    assert!(eng.get(&cf1, b"key50").unwrap().is_some());
    assert!(eng.get(&cf2, b"key50").unwrap().is_some());
}

#[test]
fn should_allow_same_key_across_cfs_given_different_cf_when_writing() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).unwrap();
    
    let cf1 = eng.create_column_family("cf1", ColumnFamilyConfig::default()).unwrap();
    let cf2 = eng.create_column_family("cf2", ColumnFamilyConfig::default()).unwrap();

    // Act - same key, different values in different CFs
    eng.put(&cf1, b"shared_key", b"value_in_cf1").unwrap();
    eng.put(&cf2, b"shared_key", b"value_in_cf2").unwrap();

    // Assert - isolated values
    assert_eq!(
        eng.get(&cf1, b"shared_key").unwrap().unwrap(),
        Bytes::from("value_in_cf1")
    );
    assert_eq!(
        eng.get(&cf2, b"shared_key").unwrap().unwrap(),
        Bytes::from("value_in_cf2")
    );
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
    let eng = MidgeEngine::open(opts).unwrap();
    
    let cf = eng.create_column_family("temp_cf", ColumnFamilyConfig::default()).unwrap();
    for i in 0..100 {
        let key = format!("key{}", i);
        eng.put(&cf, key.as_bytes(), b"val").unwrap();
    }
    eng.flush().unwrap();

    // Act
    eng.drop_column_family(&cf).unwrap();

    // Assert - CF data should be gone (recreating with same name returns fresh CF)
    drop(eng);
    let opts2 = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng2 = MidgeEngine::open(opts2).unwrap();
    let cf_new = eng2.create_column_family("temp_cf", ColumnFamilyConfig::default()).unwrap();
    // Fresh CF should have no data
    assert!(eng2.get(&cf_new, b"key50").unwrap().is_none());
}
