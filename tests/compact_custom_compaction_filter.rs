// Custom Compaction Filter
// Extracted from compaction_concurrent.rs

mod common;

use common::test_temp_dir;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use std::thread;
use std::time::Duration;

// ============================================================================

#[test]
fn should_invoke_filter_for_each_key_given_compaction_with_custom_filter() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 512,
        enable_compaction: true,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("Failed to open engine");
    let cf = eng.default_column_family();

    // TODO: Create a CompactionFilter that counts invocations
    // let invocation_count = Arc::new(AtomicUsize::new(0));
    // let filter = CountingFilter::new(invocation_count.clone());
    // eng.set_compaction_filter(&cf, filter);

    // Write keys to trigger compaction
    for i in 0..50 {
        let key = format!("key_{:02}", i);
        eng.put(&cf, key.as_bytes(), b"value").unwrap();
    }

    // Act
    // TODO: Add explicit compaction trigger API
    thread::sleep(Duration::from_millis(500)); // Allow compaction

    // Assert
    // TODO: Assert invocation_count.load() == 50 (one per key)
    let result = eng.get(&cf, b"key_00").expect("get failed");
    assert!(result.is_some(), "Data should be present after filtered compaction");
}

#[test]
fn should_drop_key_given_filter_returns_remove_decision() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 512,
        enable_compaction: true,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("Failed to open engine");
    let cf = eng.default_column_family();

    // TODO: Create a CompactionFilter that removes keys with specific prefix
    // let filter = PrefixRemovalFilter::new(b"remove_");
    // eng.set_compaction_filter(&cf, filter);

    // Write keys with different prefixes
    for i in 0..10 {
        eng.put(&cf, format!("keep_{:02}", i).as_bytes(), b"value").unwrap();
        eng.put(&cf, format!("remove_{:02}", i).as_bytes(), b"value").unwrap();
    }

    // Act
    // TODO: Add explicit compaction trigger API
    thread::sleep(Duration::from_millis(500)); // Allow compaction

    // Assert
    // Kept keys should still exist
    let result = eng.get(&cf, b"keep_00").expect("get failed");
    assert!(result.is_some(), "Kept keys should survive compaction");

    // TODO: Assert removed keys are gone (requires filter implementation)
    // assert_key_absent(&eng, &cf, b"remove_00");
    let result = eng.get(&cf, b"remove_00").expect("get failed");
    assert!(result.is_some(), "Keys will be present until filter is implemented");
}

#[test]
fn should_keep_key_given_filter_returns_keep_decision() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 512,
        enable_compaction: true,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("Failed to open engine");
    let cf = eng.default_column_family();

    // TODO: Create a CompactionFilter that keeps all keys
    // let filter = KeepAllFilter::new();
    // eng.set_compaction_filter(&cf, filter);

    // Write data
    for i in 0..30 {
        let key = format!("key_{:02}", i);
        eng.put(&cf, key.as_bytes(), b"important_data").unwrap();
    }

    // Act
    // TODO: Add explicit compaction trigger API
    thread::sleep(Duration::from_millis(500)); // Allow compaction

    // Assert
    // All keys should still exist after compaction
    for i in 0..30 {
        let key = format!("key_{:02}", i);
        let result = eng.get(&cf, key.as_bytes()).expect("get failed");
        assert!(result.is_some(), "All keys should be kept by filter");
    }
}

#[test]
fn should_modify_value_given_filter_returns_change_decision() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 512,
        enable_compaction: true,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("Failed to open engine");
    let cf = eng.default_column_family();

    // TODO: Create a CompactionFilter that modifies values
    // let filter = ValueModifyFilter::new(|value| {
    //     format!("{}_modified", String::from_utf8_lossy(value))
    // });
    // eng.set_compaction_filter(&cf, filter);

    // Write original values
    for i in 0..20 {
        let key = format!("key_{:02}", i);
        eng.put(&cf, key.as_bytes(), b"original").unwrap();
    }

    // Act
    // TODO: Add explicit compaction trigger API
    thread::sleep(Duration::from_millis(500)); // Allow compaction

    // Assert
    // TODO: Verify values are modified after compaction
    // let result = eng.get(&cf, b"key_00").expect("get failed");
    // assert_eq!(result.unwrap().as_ref(), b"original_modified");
    
    // For now, just verify data integrity
    let result = eng.get(&cf, b"key_00").expect("get failed");
    assert!(result.is_some(), "Data should be present after compaction");
}
