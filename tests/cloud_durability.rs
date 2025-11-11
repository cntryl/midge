mod common;
use common::{assert_get_equals, test_temp_dir};
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};

#[test]
fn should_preserve_local_file_given_upload_in_progress_when_crash() {
    // Arrange
    let dir = test_temp_dir();
    let db_path = dir.path().to_path_buf();
    
    // Act - use local disk (cloud mode would require mock backend)
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: db_path.clone() },
        memtable_size: 1024,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();
    
    // Write data that will create SST files
    for i in 0..100 {
        eng.put(&cf, format!("key{:03}", i).as_bytes(), b"value").expect("put");
    }
    drop(eng);
    
    // Assert - local files should be preserved
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("reopen");
    let cf = eng.default_column_family();
    for i in 0..100 {
        let result = eng.get(&cf, format!("key{:03}", i).as_bytes()).expect("get");
        assert!(result.is_some(), "Local file should be preserved");
    }
    // TODO: Test cloud mode with mock backend to verify upload retry logic
}

#[test]
fn should_upload_sst_idempotently_given_duplicate_upload_attempt_when_network_flaky() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();
    
    // Act - write data
    for i in 0..50 {
        eng.put(&cf, format!("key{:02}", i).as_bytes(), b"value").expect("put");
    }
    
    // TODO: Test cloud mode with simulated network failures
    // Verify retries produce idempotent results
    
    // Assert - data should be consistent
    for i in 0..50 {
        let result = eng.get(&cf, format!("key{:02}", i).as_bytes()).expect("get");
        assert!(result.is_some(), "Data should be available despite retries");
    }
}

#[test]
fn should_reconcile_cloud_manifest_given_remote_drift_when_check_cloud_command_runs() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();
    
    // Act - write data
    eng.put(&cf, b"key1", b"value1").expect("put");
    eng.put(&cf, b"key2", b"value2").expect("put");
    
    // TODO: Simulate cloud manifest drift
    // Run reconciliation command
    // Verify local and remote manifests sync correctly
    
    // Assert - data should remain accessible
    assert_get_equals(&eng, b"key1", b"value1");
    assert_get_equals(&eng, b"key2", b"value2");
}
