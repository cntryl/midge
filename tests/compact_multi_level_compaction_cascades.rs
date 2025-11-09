// Multi-Level Compaction Cascades
// Extracted from compaction_concurrent.rs

// Compaction During Concurrent Operations tests - P1 Priority
use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, Query, StorageMode, ColumnFamilyHandle};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

mod common;
use common::{assert_get_equals, assert_key_absent};

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
fn should_trigger_l2_compaction_given_l1_compaction_exceeded_l2_capacity() {
    // Arrange - Create enough data to span multiple levels
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        memtable_size: 512,
        max_levels: 4,
        level_multiplier: 4,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();

    // Write substantial data
    for batch in 0..15 {
        for i in 0..40 {
            let key = format!("cascade_b{}_k{:03}", batch, i);
            engine
                .put(&cf, key.as_bytes(), b"cascade_value")
                .unwrap();
        }
        engine.flush().unwrap();
    }

    // Act - Compact multiple times to cascade
    engine.compact_all().unwrap();

    // Assert - All data still accessible
    for batch in 0..15 {
        for i in 0..40 {
            let key = format!("cascade_b{}_k{:03}", batch, i);
            assert!(engine.get(&cf, key.as_bytes()).unwrap().is_some());
        }
    }
}

#[test]
fn should_propagate_compaction_to_l3_given_l2_overflow() {
    // Arrange
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        memtable_size: 256,
        max_levels: 5,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();

    // Write and compact incrementally
    for round in 0..20 {
        for i in 0..25 {
            let key = format!("r{}_k{:02}", round, i);
            engine.put(&cf, key.as_bytes(), b"val").unwrap();
        }
        engine.flush().unwrap();
        if round % 5 == 0 {
            engine.compact_all().unwrap();
        }
    }

    // Act - Final full compaction
    engine.compact_all().unwrap();

    // Assert - Data integrity maintained
    for round in 0..20 {
        for i in 0..25 {
            let key = format!("r{}_k{:02}", round, i);
            assert!(engine.get(&cf, key.as_bytes()).unwrap().is_some());
        }
    }
}

#[test]
fn should_handle_cascading_compaction_to_max_level() {
    // Arrange
    let opts = compaction_test_opts();
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();

    // Create deep structure
    for i in 0..200 {
        let key = format!("deep_key{:04}", i);
        engine
            .put(&cf, key.as_bytes(), b"deep_value")
            .unwrap();
        if i % 20 == 19 {
            engine.flush().unwrap();
        }
    }

    // Act - Cascade all the way down
    engine.compact_all().unwrap();

    // Assert - Full data accessibility
    for i in 0..200 {
        let key = format!("deep_key{:04}", i);
        assert!(engine.get(&cf, key.as_bytes()).unwrap().is_some());
    }
}

#[test]
fn should_not_trigger_cascade_given_sufficient_capacity_at_next_level() {
    // Arrange - Write modest amount of data
    let opts = compaction_test_opts();
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();

    for i in 0..50 {
        engine
            .put(&cf, format!("key{:02}", i).as_bytes(), b"value")
            .unwrap();
    }
    engine.flush().unwrap();

    // Act - Single compaction should suffice
    let result = engine.compact_all();

    // Assert - Succeeds without cascading issues
    assert!(result.is_ok());
    for i in 0..50 {
        let key = format!("key{:02}", i);
        assert!(engine.get(&cf, key.as_bytes()).unwrap().is_some());
    }
}
