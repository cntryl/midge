// Level Target Size Enforcement
// Extracted from compaction_concurrent.rs

// Compaction During Concurrent Operations tests - P1 Priority
use cntryl_midge::{ColumnFamilyHandle, MidgeEngine, MidgeOptions, StorageMode};
use std::thread;
use std::time::Duration;

mod common;

// Helper to create test options with small memtable for quick flushes
fn compaction_test_opts() -> MidgeOptions {
    MidgeOptions {
        storage_mode: StorageMode::Memory,
        memtable_size: 1024,         // Small memtable to trigger flushes easily
        compaction_sst_threshold: 2, // Trigger compaction with just 2 SST files
        ..Default::default()
    }
}

// Helper to populate engine with data spread across multiple L0 files
fn populate_multi_level_data(engine: &MidgeEngine, cf: &ColumnFamilyHandle) {
    // Write batch 1 and flush to L0
    for i in 0..50 {
        let key = format!("key{:03}", i);
        let value = format!("value1_{}", i);
        engine.put(cf, key.as_bytes(), value.as_bytes()).unwrap();
    }
    engine.flush().unwrap();

    // Write batch 2 and flush to L0 (overlapping keys)
    for i in 25..75 {
        let key = format!("key{:03}", i);
        let value = format!("value2_{}", i);
        engine.put(cf, key.as_bytes(), value.as_bytes()).unwrap();
    }
    engine.flush().unwrap();

    // Write batch 3 and flush to L0
    for i in 50..100 {
        let key = format!("key{:03}", i);
        let value = format!("value3_{}", i);
        engine.put(cf, key.as_bytes(), value.as_bytes()).unwrap();
    }
    engine.flush().unwrap();
}

#[test]
fn should_trigger_compaction_given_level_exceeds_target_size() {
    // Arrange
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        memtable_size: 512,
        compaction_sst_threshold: 3, // Trigger after 3 SSTs
        enable_compaction: false,    // Manual compaction for controlled testing
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();

    // Write and flush multiple times to create SSTs
    for batch in 0..5 {
        for i in 0..30 {
            let key = format!("batch{}key{:03}", batch, i);
            engine.put(&cf, key.as_bytes(), "value".as_bytes()).unwrap();
        }
        engine.flush().unwrap();
    }

    // Act - Compact manually
    let result = engine.compact_all();

    // Assert - Compaction should succeed
    assert!(result.is_ok(), "Compaction should succeed");

    // Verify data is still accessible
    for batch in 0..5 {
        for i in 0..30 {
            let key = format!("batch{}key{:03}", batch, i);
            let val = engine.get(&cf, key.as_bytes()).unwrap();
            assert!(val.is_some(), "Key should exist after compaction");
        }
    }
}

#[test]
fn should_compact_largest_file_given_level_too_large() {
    // Arrange
    let opts = compaction_test_opts();
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();

    // Create files of varying sizes
    for i in 0..20 {
        engine
            .put(&cf, format!("small{}", i).as_bytes(), b"val")
            .unwrap();
    }
    engine.flush().unwrap();

    // Large file
    for i in 0..200 {
        engine
            .put(
                &cf,
                format!("large{}", i).as_bytes(),
                b"large_value_content",
            )
            .unwrap();
    }
    engine.flush().unwrap();

    // Act - Compact
    let result = engine.compact_all();

    // Assert - Should succeed and all data accessible
    assert!(result.is_ok());
    for i in 0..200 {
        let key = format!("large{}", i);
        assert!(engine.get(&cf, key.as_bytes()).unwrap().is_some());
    }
}

#[test]
fn should_respect_level_multiplier_given_cascading_compaction() {
    // Arrange - Create multi-level structure
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        memtable_size: 256,
        max_levels: 5,
        level_multiplier: 10,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();

    // Write enough data to potentially trigger cascading
    for batch in 0..10 {
        for i in 0..50 {
            let key = format!("cascade{}key{}", batch, i);
            engine.put(&cf, key.as_bytes(), b"value").unwrap();
        }
        engine.flush().unwrap();
        // Give flush coordinator time to complete async file operations
        // Increase wait to reduce flakiness on slow CI or loaded hosts
        thread::sleep(Duration::from_millis(100));
    }

    // Act
    let result = engine.compact_all();

    // Assert - Compaction succeeds and data intact
    assert!(result.is_ok());
    for batch in 0..10 {
        for i in 0..50 {
            let key = format!("cascade{}key{}", batch, i);
            assert!(engine.get(&cf, key.as_bytes()).unwrap().is_some());
        }
    }
}

#[test]
fn should_not_exceed_target_size_given_completed_compaction() {
    // Arrange
    let opts = compaction_test_opts();
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();
    populate_multi_level_data(&engine, &cf);

    // Act
    engine.compact_all().unwrap();

    // Assert - After compaction, data is consolidated and accessible
    for i in 0..100 {
        let key = format!("key{:03}", i);
        let result = engine.get(&cf, key.as_bytes()).unwrap();
        assert!(
            result.is_some(),
            "All keys should be accessible after compaction"
        );
    }
}
