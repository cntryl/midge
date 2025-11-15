mod common;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use common::{assert_get_equals, new_engine, test_temp_dir};
use std::sync::Arc;

#[test]
fn should_reject_config_given_memtable_size_exceeds_memory_budget_when_open_called() {
    // Arrange
    let dir = test_temp_dir();

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: usize::MAX / 2, // Unreasonably large
        ..Default::default()
    };

    // Act
    let result = MidgeEngine::open(opts);

    // Assert
    assert!(
        result.is_err(),
        "Should reject config with excessively large memtable_size"
    );
    if let Err(err) = result {
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("memtable_size"),
            "Error message should mention memtable_size, got: {}",
            err_msg
        );
    }
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

#[test]
fn should_handle_multiple_config_validations_concurrently_when_engines_open() {
    // Arrange
    const NUM_THREADS: usize = 10;
    let base_dir = test_temp_dir();
    let base_path = base_dir.path().to_path_buf();

    // Act - spawn multiple threads each opening an engine with different but valid configs
    let handles: Vec<_> = (0..NUM_THREADS)
        .map(|i| {
            let path = base_path.join(format!("engine_{}", i));
            std::fs::create_dir_all(&path).expect("create dir");
            std::thread::spawn(move || {
                let opts = MidgeOptions {
                    storage_mode: StorageMode::LocalDisk {
                        db_path: path,
                    },
                    memtable_size: 4096 * (i + 1),
                    ..Default::default()
                };
                MidgeEngine::open(opts).expect("open with valid config")
            })
        })
        .collect();

    // Assert - all configs should be valid
    for h in handles {
        let _engine = h.join().expect("Thread panicked");
    }
}

#[test]
fn should_preserve_config_across_rapid_writes_given_valid_configuration_when_running() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();
    let engine = Arc::new(engine);
    const NUM_THREADS: usize = 20;
    const ITERATIONS: usize = 50;

    // Act - concurrent writes with valid config
    let handles: Vec<_> = (0..NUM_THREADS)
        .map(|i| {
            let eng = engine.clone();
            let cf_clone = cf.clone();
            std::thread::spawn(move || {
                for j in 0..ITERATIONS {
                    let key = format!("key_{}_{}_{}", i, j, i * j).into_bytes();
                    let value = format!("value_{}", i * ITERATIONS + j).into_bytes();
                    eng.put(&cf_clone, &key, &value).expect("put during stress");
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("Thread panicked");
    }

    // Assert - verify engine remained stable and operational
    let test_key = b"key_0_0_0";
    let result = engine.get(&cf, test_key).expect("get after stress");
    assert!(
        result.is_some(),
        "Engine should preserve data through rapid writes with valid config"
    );
}

#[test]
fn should_recover_data_after_restart_with_same_valid_config_when_engine_reopened() {
    // Arrange
    let dir = test_temp_dir();
    let path = dir.path().to_path_buf();
    
    let opts_original = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: path.clone(),
        },
        memtable_size: 65536,
        ..Default::default()
    };

    let engine = MidgeEngine::open(opts_original.clone()).expect("initial open");
    let cf = engine.default_column_family();

    // Write 1000 keys
    for i in 0..1000 {
        let key = format!("recovery_test_key_{:04}", i).into_bytes();
        let value = format!("value_{}", i).into_bytes();
        engine.put(&cf, &key, &value).expect("put");
    }

    drop(engine);

    // Act - reopen with same config
    let engine_reopen = MidgeEngine::open(opts_original).expect("reopen with same config");

    // Assert - verify sample of keys persisted across restart
    for i in (0..1000).step_by(100) {
        let key = format!("recovery_test_key_{:04}", i).into_bytes();
        let result = engine_reopen
            .get(&cf, &key)
            .expect("get after reopen")
            .expect("key should exist after recovery");
        let expected = format!("value_{}", i).into_bytes();
        assert_eq!(
            result, expected,
            "Data mismatch for key {} after restart with same config",
            i
        );
    }
}
