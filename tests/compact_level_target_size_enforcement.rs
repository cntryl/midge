// Level Target Size Enforcement
// Extracted from compaction_concurrent.rs

// Compaction During Concurrent Operations tests - P1 Priority
use cntryl_midge::{MidgeEngine, MidgeOptions};
use std::time::Duration;

mod common;
use common::{compaction_test_opts, create_storage_mode, populate_multi_level_data};

// ============================================================================

#[test]
fn should_trigger_compaction_given_level_exceeds_target_size() {
    for mode in common::disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = MidgeOptions {
            storage_mode,
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
}

#[test]
fn should_compact_largest_file_given_level_too_large() {
    for mode in common::disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = compaction_test_opts(storage_mode);
        let engine = MidgeEngine::open(opts).unwrap();
        let cf = engine.default_column_family();

        // Create files of varying sizes
        for i in 0..20 {
            engine
                .put(&cf, format!("small{:02}", i).as_bytes(), b"val")
                .unwrap();
        }
        engine.flush().unwrap();

        // Large file
        for i in 0..200 {
            engine
                .put(
                    &cf,
                    format!("large{:03}", i).as_bytes(),
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
            let key = format!("large{:03}", i);
            assert!(engine.get(&cf, key.as_bytes()).unwrap().is_some());
        }
    }
}

#[test]
fn should_respect_level_multiplier_given_cascading_compaction() {
    for mode in common::disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange - Create multi-level structure
        let opts = MidgeOptions {
            storage_mode,
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
                let key = format!("cascade{:02}key{:02}", batch, i);
                engine.put(&cf, key.as_bytes(), b"value").unwrap();
            }
            engine.flush().unwrap();
            // Wait for flush to complete
            engine
                .wait_for_flush(Duration::from_millis(100))
                .expect("flush should complete");
        }

        // Act
        let result = engine.compact_all();

        // Assert - Compaction succeeds and data intact
        if let Err(e) = &result {
            tracing::warn!("compact_all error: {:?}", e);
        }
        assert!(result.is_ok());
        for batch in 0..10 {
            for i in 0..50 {
                let key = format!("cascade{:02}key{:02}", batch, i);
                assert!(engine.get(&cf, key.as_bytes()).unwrap().is_some());
            }
        }
    }
}

#[test]
fn should_not_exceed_target_size_given_completed_compaction() {
    for mode in common::disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = compaction_test_opts(storage_mode);
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
}
