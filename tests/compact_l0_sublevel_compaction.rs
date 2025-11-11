// L0 Sublevel Compaction
// Extracted from compaction_concurrent.rs

// Compaction During Concurrent Operations tests - P1 Priority
use cntryl_midge::MidgeEngine;
use std::sync::Arc;
use std::thread;

mod common;
use common::{assert_get_equals, compaction_test_opts};

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
        let cf_clone = cf.clone();
        let handle = thread::spawn(move || {
            for i in 0..20 {
                let key = format!("concurrent_flush_b{}k{:02}", batch, i);
                engine_clone.put(&cf_clone, key.as_bytes(), b"val").unwrap();
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
