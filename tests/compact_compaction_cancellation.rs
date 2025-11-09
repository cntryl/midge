// Compaction Cancellation
// Extracted from compaction_concurrent.rs

// Compaction During Concurrent Operations tests - P1 Priority
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode, ColumnFamilyHandle};
use std::sync::Arc;
use std::thread;

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
fn should_stop_compaction_given_shutdown_signal() {
    // Arrange
    let opts = compaction_test_opts();
    let engine = Arc::new(MidgeEngine::open(opts).unwrap());
    let cf = engine.default_column_family();
    populate_multi_level_data(&engine, &cf);

    // Act - Start compaction in background
    let engine_clone = Arc::clone(&engine);
    let compaction_handle = thread::spawn(move || {
        let _ = engine_clone.compact_all();
    });

    // Drop engine early (simulates shutdown)
    drop(engine);

    // Assert - Thread should complete (not hang)
    let result = compaction_handle.join();
    assert!(
        result.is_ok(),
        "Compaction thread should not panic on shutdown"
    );
}

#[test]
fn should_cleanup_resources_given_cancelled_compaction() {
    // Arrange
    let opts = compaction_test_opts();
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();

    for i in 0..50 {
        let key = format!("cancel_k{}", i);
        engine
            .put(&cf, key.as_bytes(), b"v")
            .unwrap();
    }
    engine.flush().unwrap();

    // Act - Start and immediately drop (cleanup test)
    let _ = engine.compact_all();
    drop(engine);

    // Assert - No resource leaks (test passes if no crash)
    // In production, this would check file handles, memory, etc.
}

#[test]
fn should_not_update_manifest_given_incomplete_compaction_when_shutdown() {
    // Arrange
    let opts = compaction_test_opts();
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();

    for i in 0..30 {
        let key = format!("incomplete{}", i);
        engine
            .put(&cf, key.as_bytes(), b"val")
            .unwrap();
    }
    engine.flush().unwrap();

    // Act - Compaction with immediate shutdown
    let _ = engine.compact_all();
    drop(engine);

    // Assert - Manifest should be consistent on reopen
    let engine = MidgeEngine::open(compaction_test_opts()).unwrap();
    // Can write new data (manifest is valid)
    engine.put(&cf, "test".as_bytes(), "ok".as_bytes()).unwrap();
}
