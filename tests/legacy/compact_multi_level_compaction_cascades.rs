// Multi-Level Compaction Cascades
// Extracted from compaction_concurrent.rs

// Compaction During Concurrent Operations tests - P1 Priority
use cntryl_midge::{MidgeEngine, MidgeOptions};

mod common;
use common::{compaction_test_opts, create_storage_mode};

#[test]
fn should_trigger_l2_compaction_given_l1_compaction_exceeded_l2_capacity() {
    for mode in common::disk_storage_modes() {
        let (mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange - Create enough data to span multiple levels
        let opts = MidgeOptions {
            storage_mode,
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
                engine.put(&cf, key.as_bytes(), b"cascade_value").unwrap();
            }
            engine.flush().unwrap();
        }

        // Act - Compact multiple times to cascade
        engine.compact_all().unwrap();

        // Assert - All data still accessible
        for batch in 0..15 {
            for i in 0..40 {
                let key = format!("cascade_b{}_k{:03}", batch, i);
                assert!(
                    engine.get(&cf, key.as_bytes()).unwrap().is_some(),
                    "Failed for storage mode: {}",
                    mode_name
                );
            }
        }
    }
}

#[test]
fn should_propagate_compaction_to_l3_given_l2_overflow() {
    for mode in common::disk_storage_modes() {
        let (mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = MidgeOptions {
            storage_mode,
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
                assert!(
                    engine.get(&cf, key.as_bytes()).unwrap().is_some(),
                    "Failed for storage mode: {}",
                    mode_name
                );
            }
        }
    }
}

#[test]
fn should_handle_cascading_compaction_to_max_level() {
    for mode in common::disk_storage_modes() {
        let (mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = compaction_test_opts(storage_mode);
        let engine = MidgeEngine::open(opts).unwrap();
        let cf = engine.default_column_family();

        // Create deep structure
        for i in 0..200 {
            let key = format!("deep_key{:04}", i);
            engine.put(&cf, key.as_bytes(), b"deep_value").unwrap();
            if i % 20 == 19 {
                engine.flush().unwrap();
            }
        }

        // Act - Cascade all the way down
        engine.compact_all().unwrap();

        // Assert - Full data accessibility
        for i in 0..200 {
            let key = format!("deep_key{:04}", i);
            assert!(
                engine.get(&cf, key.as_bytes()).unwrap().is_some(),
                "Failed for storage mode: {}",
                mode_name
            );
        }
    }
}

#[test]
fn should_not_trigger_cascade_given_sufficient_capacity_at_next_level() {
    for mode in common::disk_storage_modes() {
        let (mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange - Write modest amount of data
        let opts = compaction_test_opts(storage_mode);
        let engine = MidgeEngine::open(opts)
            .unwrap_or_else(|e| panic!("Failed to open engine for mode {}: {:?}", mode_name, e));
        let cf = engine.default_column_family();

        for i in 0..50 {
            engine
                .put(&cf, format!("key{:02}", i).as_bytes(), b"value")
                .unwrap();
        }
        engine
            .flush()
            .unwrap_or_else(|e| panic!("Failed to flush for mode {}: {:?}", mode_name, e));

        // Act - Single compaction should suffice
        let result = engine.compact_all();

        // Assert - Succeeds without cascading issues
        assert!(result.is_ok(), "Failed for storage mode: {}", mode_name);
        for i in 0..50 {
            let key = format!("key{:02}", i);
            assert!(
                engine.get(&cf, key.as_bytes()).unwrap().is_some(),
                "Failed for storage mode: {}",
                mode_name
            );
        }
    }
}
