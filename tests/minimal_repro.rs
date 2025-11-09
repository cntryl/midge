// Minimal reproduction of the duplicate key bug from compact_all benchmark

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};

#[test]
fn should_not_create_duplicate_keys_when_compacting_50k_entries() {
    let cf = engine.default_column_family();
    // Arrange
    let path = std::env::temp_dir().join("midge_test_minimal_repro_50k");
    let _ = std::fs::remove_dir_all(&path);
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: path.clone(),
        },
        memtable_size: 4 * 1024 * 1024,
        enable_compaction: false,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    for i in 0..50_000 {
        let key = format!("key_{:010}", i);
        let value = format!("value_{:010}_data_padding", i);
        engine.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
    }
    engine.flush().unwrap();

    // Act
    let result = engine.compact_all();

    // Assert
    assert!(result.is_ok());

    // Cleanup
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn should_not_create_duplicate_keys_when_compacting_small_dataset() {
    let cf = engine.default_column_family();
    // Arrange
    let path = std::env::temp_dir().join("midge_test_minimal_repro_small");
    let _ = std::fs::remove_dir_all(&path);
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: path.clone(),
        },
        memtable_size: 4 * 1024 * 1024,
        enable_compaction: false,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        let value = format!("value_{}", i);
        engine.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
    }
    engine.flush().unwrap();

    // Act
    let result = engine.compact_all();

    // Assert
    assert!(result.is_ok());

    // Cleanup
    let _ = std::fs::remove_dir_all(&path);
}
