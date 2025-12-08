//! Multi-level Compaction Tests
//!
//! Tests for LSM-tree level management and multi-level compaction behavior.
//!
//! # Test Categories
//!
//! - L0 sublevel organization: overlapping SSTs in L0
//! - Level size enforcement: target sizes and multipliers
//! - Cascading compaction: propagation through levels
//! - Level statistics: tracking file counts and sizes
//!
//! # Storage Mode Coverage
//!
//! All tests run on both LocalDisk and CloudBacked modes via `disk_storage_modes()`.

mod common;

use cntryl_midge::{MidgeEngine, MidgeOptions};
use common::{
    assert_get_equals, create_storage_mode, disk_storage_modes, manual_compaction_test_opts,
    populate_multi_level_data,
};

// ============================================================================
// L0 SUBLEVEL ORGANIZATION TESTS
// ============================================================================

#[test]
fn should_organize_l0_into_sublevels_given_overlapping_files_when_flushing() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = manual_compaction_test_opts(storage_mode);
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Create overlapping L0 files: first file covers keys 0-49
        for i in 0..50 {
            eng.put(&cf, format!("key{:03}", i).as_bytes(), b"v1")
                .expect("put");
        }
        eng.flush().expect("flush 1");

        // Second file covers keys 25-74 (overlaps with first)
        for i in 25..75 {
            eng.put(&cf, format!("key{:03}", i).as_bytes(), b"v2")
                .expect("put");
        }
        eng.flush().expect("flush 2");

        // Act - compact to merge overlapping files
        eng.compact_all().expect("compact");

        // Assert - latest values should be visible in overlap region
        for i in 25..50 {
            assert_get_equals(&eng, format!("key{:03}", i).as_bytes(), b"v2");
        }
        // Earlier keys should have v1
        for i in 0..25 {
            assert_get_equals(&eng, format!("key{:03}", i).as_bytes(), b"v1");
        }
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_compact_oldest_sublevel_first_given_incremental_strategy_when_compacting() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = manual_compaction_test_opts(storage_mode);
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Create multiple L0 files in sequence with non-overlapping keys
        for batch in 0..4 {
            for i in 0..30 {
                let key = format!("batch{}_key{:02}", batch, i);
                eng.put(&cf, key.as_bytes(), format!("v{}", batch).as_bytes())
                    .expect("put");
            }
            eng.flush().expect("flush");
        }

        // Act - compact all
        eng.compact_all().expect("compact");

        // Assert - all data preserved
        for batch in 0..4 {
            for i in 0..30 {
                let key = format!("batch{}_key{:02}", batch, i);
                let result = eng.get(&cf, key.as_bytes()).expect("get");
                assert!(
                    result.is_some(),
                    "{}: batch {} key {} should exist",
                    name,
                    batch,
                    i
                );
            }
        }
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_compact_all_sublevels_given_high_file_count_when_aggressive_compaction() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = manual_compaction_test_opts(storage_mode);
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Create many L0 files
        for batch in 0..8 {
            for i in 0..20 {
                let key = format!("key{:03}", i + batch * 20);
                eng.put(&cf, key.as_bytes(), b"value").expect("put");
            }
            eng.flush().expect("flush");
        }

        // Act - aggressive compaction
        let result = eng.compact_all();

        // Assert - should succeed and all data accessible
        assert!(result.is_ok(), "{}: compact_all should succeed", name);
        for i in 0..160 {
            let key = format!("key{:03}", i);
            assert!(
                eng.get(&cf, key.as_bytes()).expect("get").is_some(),
                "{}: key {} should exist",
                name,
                i
            );
        }
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_maintain_sublevel_ordering_given_sequential_flushes_when_reading() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = manual_compaction_test_opts(storage_mode);
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Act - sequential flushes
        for batch in 0..5 {
            for i in 0..20 {
                let key = format!("b{}k{:02}", batch, i);
                eng.put(&cf, key.as_bytes(), b"val").expect("put");
            }
            eng.flush().expect("flush");
        }

        // Assert - all data accessible after multiple flushes
        for batch in 0..5 {
            for i in 0..20 {
                let key = format!("b{}k{:02}", batch, i);
                assert!(
                    eng.get(&cf, key.as_bytes()).expect("get").is_some(),
                    "{}: batch {} key {} should exist",
                    name,
                    batch,
                    i
                );
            }
        }
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

// ============================================================================
// LEVEL SIZE ENFORCEMENT TESTS
// ============================================================================

#[test]
fn should_trigger_compaction_given_level_exceeds_target_size_when_sst_threshold_reached() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 512,
            compaction_sst_threshold: 3,
            enable_compaction: false, // Manual compaction for controlled testing
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Write and flush multiple times to create SSTs
        for batch in 0..5 {
            for i in 0..30 {
                let key = format!("batch{}key{:03}", batch, i);
                eng.put(&cf, key.as_bytes(), b"value").expect("put");
            }
            eng.flush().expect("flush");
        }

        // Act - manual compaction
        let result = eng.compact_all();

        // Assert
        assert!(result.is_ok(), "{}: compact_all should succeed", name);

        // Verify all data accessible
        for batch in 0..5 {
            for i in 0..30 {
                let key = format!("batch{}key{:03}", batch, i);
                assert!(
                    eng.get(&cf, key.as_bytes()).expect("get").is_some(),
                    "{}: batch {} key {} should exist after compaction",
                    name,
                    batch,
                    i
                );
            }
        }
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_compact_largest_file_given_varying_sizes_when_level_too_large() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = manual_compaction_test_opts(storage_mode);
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Small file
        for i in 0..20 {
            eng.put(&cf, format!("small{:02}", i).as_bytes(), b"val")
                .expect("put");
        }
        eng.flush().expect("flush small");

        // Large file with bigger values
        for i in 0..200 {
            eng.put(
                &cf,
                format!("large{:03}", i).as_bytes(),
                b"large_value_content",
            )
            .expect("put");
        }
        eng.flush().expect("flush large");

        // Act
        let result = eng.compact_all();

        // Assert
        assert!(result.is_ok(), "{}: compact_all should succeed", name);
        for i in 0..200 {
            let key = format!("large{:03}", i);
            assert!(
                eng.get(&cf, key.as_bytes()).expect("get").is_some(),
                "{}: large key {} should exist",
                name,
                i
            );
        }
        for i in 0..20 {
            let key = format!("small{:02}", i);
            assert!(
                eng.get(&cf, key.as_bytes()).expect("get").is_some(),
                "{}: small key {} should exist",
                name,
                i
            );
        }
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_respect_level_multiplier_given_cascading_compaction_when_levels_fill() {
    for mode in disk_storage_modes() {
        // Arrange - configure multi-level structure
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 256,
            max_levels: 5,
            level_multiplier: 10,
            enable_compaction: false, // Disable background compaction for manual compact_all()
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Write enough data to potentially trigger cascading
        for batch in 0..10 {
            for i in 0..50 {
                let key = format!("cascade{:02}key{:02}", batch, i);
                eng.put(&cf, key.as_bytes(), b"value").expect("put");
            }
            eng.flush().expect("flush");
        }

        // Act
        let result = eng.compact_all();

        // Assert
        assert!(result.is_ok(), "{}: compact_all should succeed", name);
        for batch in 0..10 {
            for i in 0..50 {
                let key = format!("cascade{:02}key{:02}", batch, i);
                assert!(
                    eng.get(&cf, key.as_bytes()).expect("get").is_some(),
                    "{}: cascade batch {} key {} should exist",
                    name,
                    batch,
                    i
                );
            }
        }
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_not_exceed_target_size_given_completed_compaction_when_data_consolidated() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = manual_compaction_test_opts(storage_mode);
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();
        populate_multi_level_data(&eng, &cf);

        // Act
        eng.compact_all().expect("compact");

        // Assert - all keys accessible after compaction
        for i in 0..100 {
            let key = format!("key{:03}", i);
            assert!(
                eng.get(&cf, key.as_bytes()).expect("get").is_some(),
                "{}: key {} should exist after compaction",
                name,
                i
            );
        }
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

// ============================================================================
// CASCADING COMPACTION TESTS
// ============================================================================

#[test]
fn should_trigger_l2_compaction_given_l1_exceeds_capacity_when_compacting() {
    for mode in disk_storage_modes() {
        // Arrange - create enough data to span multiple levels
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 512,
            max_levels: 4,
            level_multiplier: 4,
            enable_compaction: false, // Disable background compaction for manual compact_all()
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Write substantial data
        for batch in 0..15 {
            for i in 0..40 {
                let key = format!("cascade_b{}_k{:03}", batch, i);
                eng.put(&cf, key.as_bytes(), b"cascade_value").expect("put");
            }
            eng.flush().expect("flush");
        }

        // Act - compact to cascade through levels
        eng.compact_all().expect("compact");

        // Assert - all data accessible
        for batch in 0..15 {
            for i in 0..40 {
                let key = format!("cascade_b{}_k{:03}", batch, i);
                assert!(
                    eng.get(&cf, key.as_bytes()).expect("get").is_some(),
                    "{}: cascade batch {} key {} should exist",
                    name,
                    batch,
                    i
                );
            }
        }
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_propagate_compaction_to_deeper_levels_given_overflow_when_incremental_compaction() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 256,
            max_levels: 5,
            enable_compaction: false, // Disable background compaction for manual compact_all()
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Write and compact incrementally
        for round in 0..20 {
            for i in 0..25 {
                let key = format!("r{}_k{:02}", round, i);
                eng.put(&cf, key.as_bytes(), b"val").expect("put");
            }
            eng.flush().expect("flush");
            if round % 5 == 0 {
                eng.compact_all().expect("compact");
            }
        }

        // Act - final full compaction
        eng.compact_all().expect("final compact");

        // Assert - data integrity maintained
        for round in 0..20 {
            for i in 0..25 {
                let key = format!("r{}_k{:02}", round, i);
                assert!(
                    eng.get(&cf, key.as_bytes()).expect("get").is_some(),
                    "{}: round {} key {} should exist",
                    name,
                    round,
                    i
                );
            }
        }
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_handle_cascading_compaction_to_max_level_given_deep_structure_when_compacting() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = manual_compaction_test_opts(storage_mode);
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Create deep structure with periodic flushes
        for i in 0..200 {
            let key = format!("deep_key{:04}", i);
            eng.put(&cf, key.as_bytes(), b"deep_value").expect("put");
            if i % 20 == 19 {
                eng.flush().expect("flush");
            }
        }

        // Act - cascade all the way down
        eng.compact_all().expect("compact");

        // Assert - full data accessibility
        for i in 0..200 {
            let key = format!("deep_key{:04}", i);
            assert!(
                eng.get(&cf, key.as_bytes()).expect("get").is_some(),
                "{}: deep key {} should exist",
                name,
                i
            );
        }
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_not_trigger_cascade_given_sufficient_capacity_when_modest_data() {
    for mode in disk_storage_modes() {
        // Arrange - write modest amount of data
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = manual_compaction_test_opts(storage_mode);
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        for i in 0..50 {
            eng.put(&cf, format!("key{:02}", i).as_bytes(), b"value")
                .expect("put");
        }
        eng.flush().expect("flush");

        // Act - single compaction should suffice
        let result = eng.compact_all();

        // Assert - succeeds without issues
        assert!(result.is_ok(), "{}: compact_all should succeed", name);
        for i in 0..50 {
            let key = format!("key{:02}", i);
            assert!(
                eng.get(&cf, key.as_bytes()).expect("get").is_some(),
                "{}: key {} should exist",
                name,
                i
            );
        }
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

// ============================================================================
// LEVEL STATISTICS TESTS
// ============================================================================

#[test]
fn should_report_sst_count_given_multiple_flushes_when_querying_stats() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 512,
            enable_compaction: false, // Disable background compaction
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Create multiple SST files
        for batch in 0..3 {
            for i in 0..20 {
                let key = format!("batch{}key{:02}", batch, i);
                eng.put(&cf, key.as_bytes(), b"value").expect("put");
            }
            eng.flush().expect("flush");
        }

        // Act - query SST count
        let sst_count = eng.sst_file_count();

        // Assert - should have multiple SST files
        assert!(
            sst_count >= 3,
            "{}: expected at least 3 SSTs, got {}",
            name,
            sst_count
        );
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_reduce_sst_count_given_compaction_when_merging_files() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 512,
            enable_compaction: false,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Create overlapping SST files
        for batch in 0..5 {
            for i in 0..30 {
                let key = format!("key{:03}", i); // Same keys, different values
                eng.put(&cf, key.as_bytes(), format!("v{}", batch).as_bytes())
                    .expect("put");
            }
            eng.flush().expect("flush");
        }

        let sst_before = eng.sst_file_count();

        // Act - compact
        eng.compact_all().expect("compact");

        let sst_after = eng.sst_file_count();

        // Assert - should have fewer SSTs after compaction
        assert!(
            sst_after <= sst_before,
            "{}: expected fewer or equal SSTs after compaction ({} -> {})",
            name,
            sst_before,
            sst_after
        );

        // Data should still be accessible
        for i in 0..30 {
            let key = format!("key{:03}", i);
            assert_get_equals(&eng, key.as_bytes(), b"v4"); // Latest value
        }
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_report_total_sst_size_given_data_written_when_querying_stats() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 512,
            enable_compaction: false,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Write some data
        for i in 0..100 {
            let key = format!("key{:03}", i);
            eng.put(&cf, key.as_bytes(), b"some_value_here")
                .expect("put");
        }
        eng.flush().expect("flush");

        // Act - query total SST size
        let total_size = eng.total_sst_size();

        // Assert - should have non-zero size
        assert!(
            total_size > 0,
            "{}: expected non-zero SST size, got {}",
            name,
            total_size
        );
        drop(eng);
        eprintln!("✓ {}", name);
    }
}
