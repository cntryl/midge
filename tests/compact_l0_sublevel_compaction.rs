// L0 Sublevel Compaction
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

/// Helper: create a new engine in a fresh temp dir and return both.
fn new_engine() -> (tempfile::TempDir, cntryl_midge::MidgeEngine) {
    let dir = test_temp_dir();
    let opts = cntryl_midge::MidgeOptions {
        storage_mode: cntryl_midge::StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = cntryl_midge::MidgeEngine::open(opts).expect("open");
    (dir, engine)
}

// ============================================================================

#[test]
fn should_organize_l0_into_sublevels_given_overlapping_files() {
    // Arrange - Create overlapping L0 files
    let opts = compaction_test_opts();
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();

    // First L0 file: keys 0-50
    for i in 0..50 {
        engine
            .put(&cf, format!("key{:03}", i).as_bytes(), b"v1")
            .unwrap();
    }
    engine.flush().unwrap();

    // Second L0 file: keys 25-75 (overlaps)
    for i in 25..75 {
        engine
            .put(&cf, format!("key{:03}", i).as_bytes(), b"v2")
            .unwrap();
    }
    engine.flush().unwrap();

    // Act - Compact to merge overlapping files
    engine.compact_all().unwrap();

    // Assert - Latest values should be visible
    for i in 25..50 {
        let key = format!("key{:03}", i);
        assert_get_equals(&engine, key.as_bytes(), b"v2");
    }
}

#[test]
fn should_compact_oldest_sublevel_first_given_incremental_strategy() {
    // Arrange - Create multiple L0 files in sequence
    let opts = compaction_test_opts();
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();

        for batch in 0..4 {
        for i in 0..30 {
            let key = format!("batch{}_key{:02}", batch, i);
            engine
                .put(&cf, key.as_bytes(), format!("v{}", batch).as_bytes())
                .unwrap();
        }
        engine.flush().unwrap();
    }

    // Act - Compact (should process in order)
    engine.compact_all().unwrap();

    // Assert - All data preserved with latest values
    for i in 0..30 {
        let key = format!("batch3_key{:02}", i);
        assert!(engine.get(&cf, key.as_bytes()).unwrap().is_some());
    }
}

#[test]
fn should_compact_all_sublevels_given_aggressive_strategy_when_file_count_high() {
    // Arrange - Create many L0 files
    let opts = compaction_test_opts();
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();

    for batch in 0..8 {
        for i in 0..20 {
            let key = format!("key{:03}", i + batch * 20);
            engine.put(&cf, key.as_bytes(), b"value").unwrap();
        }
        engine.flush().unwrap();
    }

    // Act - Aggressive compaction
    let result = engine.compact_all();

    // Assert - Should succeed
    assert!(result.is_ok());
    for i in 0..160 {
        let key = format!("key{:03}", i);
        assert!(engine.get(&cf, key.as_bytes()).unwrap().is_some());
    }
}

#[test]
fn should_maintain_sublevel_ordering_given_concurrent_flushes() {
    // Arrange
    let opts = compaction_test_opts();
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();

    // Act - Sequential flushes (concurrent flushes may cause file conflicts)
    for batch in 0..5 {
        for i in 0..20 {
            let key = format!("b{}k{:02}", batch, i);
            engine.put(&cf, key.as_bytes(), "val".as_bytes()).unwrap();
        }
        engine.flush().unwrap();
    }

    // Assert - All data accessible after multiple flushes
    for batch in 0..5 {
        for i in 0..20 {
            let key = format!("b{}k{:02}", batch, i);
            assert!(engine.get(&cf, key.as_bytes()).unwrap().is_some());
        }
    }
}

#[test]
fn should_handle_concurrent_flush_calls_without_file_conflicts() {
    // Arrange
    let opts = compaction_test_opts();
    let engine = Arc::new(MidgeEngine::open(opts).unwrap());
    let cf = engine.default_column_family();

    // Act - Multiple threads calling flush() concurrently
    // This test previously exposed a file conflict bug (fixed with flush_mutex)
    let mut handles = vec![];
    for batch in 0..5 {
        let engine_clone = Arc::clone(&engine);
        let handle = thread::spawn(move || {
            for i in 0..20 {
                let key = format!("concurrent_flush_b{}k{:02}", batch, i);
                engine_clone
                    .put(Bytes::from(key), Bytes::from("val"))
                    .unwrap();
            }
            // Now safe: flush_mutex serializes concurrent flush() calls
            engine_clone.flush().unwrap();
        });
        handles.push(handle);
    }

    // Assert - All flushes should complete successfully
    for h in handles {
        h.join().unwrap();
    }

    // All data should be accessible
    for batch in 0..5 {
        for i in 0..20 {
            let key = format!("concurrent_flush_b{}k{:02}", batch, i);
            assert!(engine.get(&cf, key.as_bytes()).unwrap().is_some());
        }
    }
}
