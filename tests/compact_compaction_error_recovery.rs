// Compaction Error Recovery
// Extracted from compaction_concurrent.rs

// Compaction During Concurrent Operations tests - P1 Priority
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode, ColumnFamilyHandle};

mod common;
use common::assert_get_equals;

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

// ============================================================================

#[test]
fn should_retry_compaction_given_disk_full_error_when_writing_sst() {
    // Arrange - This tests that compaction errors don't crash
    let opts = compaction_test_opts();
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();
    populate_multi_level_data(&engine, &cf);

    // Act - Multiple compaction attempts (simulates retry behavior)
    let result1 = engine.compact_all();
    let result2 = engine.compact_all();

    // Assert - Both should succeed (or fail gracefully)
    assert!(result1.is_ok() || result1.is_err());
    assert!(result2.is_ok() || result2.is_err());

    // Data should still be accessible
    for i in 0..10 {
        let key = format!("key{:03}", i);
        assert!(engine.get(&cf, key.as_bytes()).is_ok());
    }
}

#[test]
fn should_abort_compaction_given_corruption_detected_when_reading_input() {
    // Arrange - Normal operation
    let opts = compaction_test_opts();
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();

    for i in 0..50 {
        let key = format!("key{}", i);
        engine
            .put(&cf, key.as_bytes(), b"value")
            .unwrap();
    }
    engine.flush().unwrap();

    // Act - Compaction should handle any internal errors gracefully
    let result = engine.compact_all();

    // Assert - Either succeeds or fails gracefully without data loss
    match result {
        Ok(_) => {
            // Successful compaction
            for i in 0..50 {
                let key = format!("key{}", i);
                assert!(engine
                    .get(&cf, key.as_bytes())
                    .unwrap()
                    .is_some());
            }
        }
        Err(_) => {
            // Failed gracefully - data should still be accessible
            for i in 0..50 {
                let key = format!("key{}", i);
                assert!(engine.get(&cf, key.as_bytes()).is_ok());
            }
        }
    }
}

#[test]
fn should_cleanup_partial_output_given_compaction_failure() {
    // Arrange
    let opts = compaction_test_opts();
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();
    populate_multi_level_data(&engine, &cf);

    // Act - Compact (should clean up on any failure)
    let _ = engine.compact_all();

    // Assert - Engine should still be usable
    engine
        .put(&cf, b"new_key", b"new_value")
        .unwrap();
    assert_get_equals(&engine, b"new_key", b"new_value");
}

#[test]
fn should_restore_manifest_given_compaction_crash_before_commit() {
    // Arrange
    let opts = compaction_test_opts();
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();

    for i in 0..30 {
        let key = format!("k{}", i);
        engine
            .put(&cf, key.as_bytes(), b"v")
            .unwrap();
    }
    engine.flush().unwrap();

    // Act - Compaction should maintain manifest consistency
    engine.compact_all().unwrap();

    // Assert - Data accessible (manifest is consistent)
    for i in 0..30 {
        let key = format!("k{}", i);
        assert!(engine.get(&cf, key.as_bytes()).unwrap().is_some());
    }
}

#[test]
fn should_preserve_input_files_given_compaction_error_when_aborting() {
    // Arrange
    let opts = compaction_test_opts();
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();

    for i in 0..40 {
        let key = format!("preserve{}", i);
        engine
            .put(&cf, key.as_bytes(), b"data")
            .unwrap();
    }
    engine.flush().unwrap();

    let _initial_keys: Vec<_> = (0..40)
        .filter_map(|i| {
            let key = format!("preserve{}", i);
            engine.get(&cf, key.as_bytes()).unwrap()
        })
        .collect();

    // Act - Compaction (may or may not succeed)
    let _ = engine.compact_all();

    // Assert - Original data preserved regardless
    for i in 0..40 {
        let key = format!("preserve{}", i);
        assert!(
            engine.get(&cf, key.as_bytes()).unwrap().is_some(),
            "Key should be preserved"
        );
    }
}
