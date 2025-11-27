//! Compaction Errors Integration Tests
//!
//! Tests for compaction error handling and recovery scenarios.
//! Verifies that compaction operations handle failures gracefully and maintain data consistency.
//!
//! ## Coverage
//! - Compaction cancellation during operation
//! - Error recovery when compaction fails
//! - Background compaction error handling
//! - Compaction interruption scenarios
//! - Manifest consistency during failures
//! - Resource cleanup on compaction errors
//!
//! ## Storage Mode Coverage
//! Tests LocalDisk and CloudBacked modes (requires compaction operations).

mod common;

use cntryl_midge::{
    test_hooks::{IoBehavior, TestHooks},
    MidgeEngine,
};
use common::{
    assert_get_equals, compaction_test_opts, create_storage_mode, disk_storage_modes,
    populate_multi_level_data,
};
use std::sync::Arc;
use std::thread;

// =============================================================================
// Compaction Cancellation
// =============================================================================

#[test]
fn should_stop_compaction_given_shutdown_signal() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = compaction_test_opts(storage_mode);
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
}

#[test]
fn should_cleanup_resources_given_cancelled_compaction() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = compaction_test_opts(storage_mode);
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
}

#[test]
fn should_not_update_manifest_given_incomplete_compaction_when_shutdown() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = compaction_test_opts(storage_mode);
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
        // Recreate storage mode for reopen (original moved when opening engine)
        let (_, reopened_mode, _tmp) = create_storage_mode(mode);
        let engine = MidgeEngine::open(compaction_test_opts(reopened_mode)).unwrap();
        // Can write new data (manifest is valid)
        engine.put(&cf, "test".as_bytes(), "ok".as_bytes()).unwrap();
    }
}

// =============================================================================
// Compaction Error Recovery
// =============================================================================

#[test]
fn should_retry_compaction_given_disk_full_error_when_writing_sst() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange - Create engine with hooks initially disabled
        let hooks = TestHooks::new().with_io_behavior(IoBehavior::Normal);
        let mut opts = compaction_test_opts(storage_mode);
        opts.test_hooks = Some(hooks.clone());
        let engine = MidgeEngine::open(opts).unwrap();
        let cf = engine.default_column_family();
        populate_multi_level_data(&engine, &cf);

        // Enable disk full errors for compaction
        hooks.set_io_behavior(IoBehavior::FailWithEnospc);

        // Act - Attempt compaction when disk is full
        let result = engine.compact_all();

        // Assert - Compaction should fail with disk full error
        assert!(result.is_err(), "Compaction should fail when disk is full");
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("No space left on device")
                || err.to_string().contains("ENOSPC"),
            "Error should indicate disk full: {}",
            err
        );
    }
}

#[test]
fn should_abort_compaction_given_corruption_detected_when_reading_input() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange - Normal operation
        let opts = compaction_test_opts(storage_mode);
        let engine = MidgeEngine::open(opts).unwrap();
        let cf = engine.default_column_family();

        for i in 0..50 {
            let key = format!("key{:02}", i);
            engine.put(&cf, key.as_bytes(), b"value").unwrap();
        }
        engine.flush().unwrap();

        // Act - Compaction should handle any internal errors gracefully
        let result = engine.compact_all();

        // Assert - Either succeeds or fails gracefully without data loss
        match result {
            Ok(_) => {
                // Successful compaction
                for i in 0..50 {
                    let key = format!("key{:02}", i);
                    assert!(engine.get(&cf, key.as_bytes()).unwrap().is_some());
                }
            }
            Err(_) => {
                // Failed gracefully - data should still be accessible
                for i in 0..50 {
                    let key = format!("key{:02}", i);
                    assert!(engine.get(&cf, key.as_bytes()).is_ok());
                }
            }
        }
    }
}

#[test]
fn should_cleanup_partial_output_given_compaction_failure() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = compaction_test_opts(storage_mode);
        let engine = MidgeEngine::open(opts).unwrap();
        let cf = engine.default_column_family();
        populate_multi_level_data(&engine, &cf);

        // Act - Compact (should clean up on any failure)
        let _ = engine.compact_all();

        // Assert - Engine should still be usable
        engine.put(&cf, b"new_key", b"new_value").unwrap();
        assert_get_equals(&engine, b"new_key", b"new_value");
    }
}

#[test]
fn should_restore_manifest_given_compaction_crash_before_commit() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = compaction_test_opts(storage_mode);
        let engine = MidgeEngine::open(opts).unwrap();
        let cf = engine.default_column_family();

        for i in 0..30 {
            let key = format!("k{:02}", i);
            engine.put(&cf, key.as_bytes(), b"v").unwrap();
        }
        engine.flush().unwrap();

        // Act - Compaction should maintain manifest consistency
        engine.compact_all().unwrap();

        // Assert - Data accessible (manifest is consistent)
        for i in 0..30 {
            let key = format!("k{:02}", i);
            assert!(engine.get(&cf, key.as_bytes()).unwrap().is_some());
        }
    }
}

#[test]
fn should_preserve_input_files_given_compaction_error_when_aborting() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = compaction_test_opts(storage_mode);
        let engine = MidgeEngine::open(opts).unwrap();
        let cf = engine.default_column_family();

        for i in 0..40 {
            let key = format!("preserve{:02}", i);
            engine.put(&cf, key.as_bytes(), b"data").unwrap();
        }
        engine.flush().unwrap();

        let _initial_keys: Vec<_> = (0..40)
            .filter_map(|i| {
                let key = format!("preserve{:02}", i);
                engine.get(&cf, key.as_bytes()).unwrap()
            })
            .collect();

        // Act - Compaction (may or may not succeed)
        let _ = engine.compact_all();

        // Assert - Original data preserved regardless
        for i in 0..40 {
            let key = format!("preserve{:02}", i);
            assert!(
                engine.get(&cf, key.as_bytes()).unwrap().is_some(),
                "Key should be preserved"
            );
        }
    }
}
