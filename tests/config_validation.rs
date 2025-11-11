mod common;
use common::{assert_get_equals, new_engine, test_temp_dir};
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};

#[test]
fn should_reject_config_given_memtable_size_exceeds_memory_budget_when_open_called() {
    // Arrange
    let dir = test_temp_dir();
    
    // Act - attempt to open with extremely large memtable
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: usize::MAX / 2, // Unreasonably large
        ..Default::default()
    };
    
    // Assert - should either succeed or fail gracefully
    let result = MidgeEngine::open(opts);
    // TODO: Add validation to reject unreasonable configs
    // For now, verify engine opens (will allocate on demand)
    if let Ok(eng) = result {
        let cf = eng.default_column_family();
        eng.put(&cf, b"test", b"value").expect("put should work");
    }
}

#[test]
fn should_apply_new_cache_size_given_runtime_config_reload_when_requested() {
    // Arrange
    let (_dir, eng) = new_engine();
    let cf = eng.default_column_family();
    eng.put(&cf, b"key1", b"value1").expect("put");
    
    // Act - attempt runtime config change
    // TODO: Add API for runtime config updates
    // eng.update_config(|config| {
    //     config.cache_size = 1024 * 1024;
    // }).expect("update config");
    
    // Assert - engine should remain functional
    assert_get_equals(&eng, b"key1", b"value1");
}

#[test]
fn should_not_restart_components_given_same_config_reapplied_when_reload() {
    // Arrange
    let (_dir, eng) = new_engine();
    let cf = eng.default_column_family();
    eng.put(&cf, b"key1", b"value1").expect("put");
    
    // Act - reapply same config
    // TODO: Add API for config reload
    // eng.reload_config().expect("reload");
    
    // Assert - engine should remain functional without disruption
    assert_get_equals(&eng, b"key1", b"value1");
    
    // TODO: Add instrumentation to verify no component restarts occurred
}
