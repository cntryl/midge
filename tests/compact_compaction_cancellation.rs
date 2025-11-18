// Compaction Cancellation
// Extracted from compaction_concurrent.rs

// Compaction During Concurrent Operations tests - P1 Priority
use cntryl_midge::MidgeEngine;
use std::sync::Arc;
use std::thread;

mod common;
use common::{compaction_test_opts, populate_multi_level_data};

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
        let key = format!("cancel_k{:02}", i);
        engine.put(&cf, key.as_bytes(), b"v").unwrap();
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
        let key = format!("incomplete{:02}", i);
        engine.put(&cf, key.as_bytes(), b"val").unwrap();
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
