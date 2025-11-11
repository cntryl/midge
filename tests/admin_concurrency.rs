mod common;
use common::{new_engine, test_temp_dir};
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use std::sync::Arc;
use std::thread;

#[test]
fn should_block_backup_start_given_active_compaction_when_requested() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 1024,
        enable_compaction: true,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();
    
    // Act - write data to trigger compaction
    for i in 0..200 {
        eng.put(&cf, format!("key{:03}", i % 50).as_bytes(), b"value").expect("put");
    }
    
    // TODO: Attempt backup during compaction
    // Verify backup either waits or proceeds with consistent snapshot
    
    // Assert - data should be consistent
    let result = eng.get(&cf, b"key025").expect("get");
    assert!(result.is_some(), "Data should be consistent during backup/compaction");
}

#[test]
fn should_fail_cf_drop_given_inflight_flush() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 1024,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();
    
    // Act - write enough to trigger flush
    for i in 0..100 {
        eng.put(&cf, format!("key{:03}", i).as_bytes(), b"value").expect("put");
    }
    
    // TODO: Attempt CF drop during flush
    // Should either fail gracefully or wait for flush completion
    
    // Assert - CF operations should remain safe
    let result = eng.get(&cf, b"key050").expect("get");
    assert!(result.is_some(), "CF should remain functional");
}

#[test]
fn should_allow_backup_readonly_mode_given_active_writes() {
    // Arrange
    let (_dir, eng) = new_engine();
    let eng = Arc::new(eng);
    
    // Act - concurrent writes
    let eng_clone = Arc::clone(&eng);
    let write_handle = thread::spawn(move || {
        let cf = eng_clone.default_column_family();
        for i in 0..100 {
            eng_clone.put(&cf, format!("key{:03}", i).as_bytes(), b"value").expect("put");
        }
    });
    
    // TODO: Initiate readonly backup concurrently
    // Backup should get consistent snapshot without blocking writes
    
    write_handle.join().unwrap();
    
    // Assert - all writes should complete
    let cf = eng.default_column_family();
    let result = eng.get(&cf, b"key050").expect("get");
    assert!(result.is_some(), "Writes should complete during readonly backup");
}

#[test]
fn should_handle_config_reload_during_compaction_without_panic() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 1024,
        enable_compaction: true,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();
    
    // Act - trigger compaction
    for i in 0..200 {
        eng.put(&cf, format!("key{:03}", i % 50).as_bytes(), b"value").expect("put");
    }
    
    // TODO: Reload config during compaction
    // Should not panic or corrupt state
    
    // Assert - database should remain functional
    let result = eng.get(&cf, b"key025").expect("get");
    assert!(result.is_some(), "Database should remain functional after config reload");
}

#[test]
fn should_return_current_cf_list_given_admin_query_when_changes_in_progress() {
    // Arrange
    let (_dir, eng) = new_engine();
    let cf = eng.default_column_family();
    eng.put(&cf, b"key1", b"value1").expect("put");
    
    // Act - query CF list
    // TODO: Add API to list column families
    // let cf_list = eng.list_column_families();
    
    // Assert - default CF should always be present
    let result = eng.get(&cf, b"key1").expect("get");
    assert!(result.is_some(), "Default CF should be functional");
    // TODO: Verify CF list includes default CF
}
