//! Compaction Basic Tests
//!
//! Tests for basic compaction operations including triggering compaction,
//! data correctness after compaction, tombstone cleanup, and snapshot preservation.
//!
//! All tests run on both LocalDisk and CloudBacked storage modes.
//!
//! # Test Coverage
//! - Manual compaction: compact_all, compact_level, compact_range
//! - Data correctness: values preserved, newest values win
//! - Tombstone handling: deleted keys removed after compaction
//! - Snapshot preservation: snapshots see old data across compaction
//! - Background compaction: automatic triggering when threshold exceeded

mod common;

use bytes::Bytes;
use cntryl_midge::test_hooks::TestHooks;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use common::{create_storage_mode, disk_storage_modes, test_temp_dir};

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

fn assert_get_equals(eng: &MidgeEngine, key: &[u8], expected: &[u8]) {
    let cf = eng.default_column_family();
    let result = eng.get(&cf, key).expect("get");
    assert_eq!(
        result,
        Some(Bytes::copy_from_slice(expected)),
        "Value mismatch for key: {:?}",
        String::from_utf8_lossy(key)
    );
}

fn assert_key_absent(eng: &MidgeEngine, key: &[u8]) {
    let cf = eng.default_column_family();
    let result = eng.get(&cf, key).expect("get");
    assert!(
        result.is_none(),
        "Key should be absent: {:?}",
        String::from_utf8_lossy(key)
    );
}

// ============================================================================
// MANUAL COMPACTION TESTS
// ============================================================================

#[test]
fn should_compact_all_given_multiple_ssts_when_triggered() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 1024,
            enable_compaction: false, // Manual only
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Create multiple SSTs with overlapping keys
        for round in 0..3 {
            for i in 0..20 {
                eng.put(
                    &cf,
                    format!("key{:02}", i).as_bytes(),
                    format!("v{}", round).as_bytes(),
                )
                .expect("put");
            }
            eng.flush_cf(&cf).expect("flush");
        }

        // Act
        let result = eng.compact_all();

        // Assert
        assert!(result.is_ok(), "Failed for {}: {:?}", name, result);
        // Verify data is correct (latest values)
        for i in 0..20 {
            assert_get_equals(&eng, format!("key{:02}", i).as_bytes(), b"v2");
        }
    }
}

#[test]
fn should_compact_level_given_l0_ssts_when_triggered() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 1024,
            enable_compaction: true, // Required for manual compaction
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Create multiple L0 SSTs with overlapping keys
        for round in 0..3 {
            for i in 0..50 {
                eng.put(
                    &cf,
                    format!("key{:02}", i).as_bytes(),
                    format!("v{}", round).as_bytes(),
                )
                .expect("put");
            }
            eng.flush_cf(&cf).expect("flush");
        }

        // Act
        let result = eng.compact_level(&cf, 0);

        // Assert
        assert!(result.is_ok(), "Failed for {}: {:?}", name, result);
        assert_get_equals(&eng, b"key25", b"v2");
    }
}

#[test]
fn should_compact_range_given_key_range_when_triggered() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 1024,
            enable_compaction: true, // Required for manual compaction
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Create overlapping SSTs
        for round in 0..3 {
            for i in 0..100 {
                eng.put(
                    &cf,
                    format!("key{:03}", i).as_bytes(),
                    format!("v{}", round).as_bytes(),
                )
                .expect("put");
            }
            eng.flush_cf(&cf).expect("flush");
        }

        // Act - compact a specific range
        let result = eng.compact_range(&cf, Some(b"key000"), Some(b"key050"));

        // Assert
        assert!(result.is_ok(), "Failed for {}: {:?}", name, result);
        assert_get_equals(&eng, b"key025", b"v2");
        assert_get_equals(&eng, b"key075", b"v2");
    }
}

// ============================================================================
// DATA CORRECTNESS TESTS
// ============================================================================

#[test]
fn should_keep_newest_value_given_overwrites_when_compacting() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 1024,
            enable_compaction: false,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Write same key multiple times with different values across flushes
        eng.put(&cf, b"key", b"v1").expect("put v1");
        eng.flush_cf(&cf).expect("flush 1");
        eng.put(&cf, b"key", b"v2").expect("put v2");
        eng.flush_cf(&cf).expect("flush 2");
        eng.put(&cf, b"key", b"v3").expect("put v3");
        eng.flush_cf(&cf).expect("flush 3");

        // Act
        eng.compact_all().expect("compact");

        // Assert - should see latest value
        assert_get_equals(&eng, b"key", b"v3");
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_preserve_data_across_restart_given_compaction_completed_when_reopening() {
    // Note: CloudBacked with MockCloudBackend doesn't support restart
    // because MockCloudBackend is in-memory. Test LocalDisk only.
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 1024,
        enable_compaction: false,
        ..Default::default()
    };

    // Act - write data and compact
    {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let cf = eng.default_column_family();
        for round in 0..3 {
            for i in 0..50 {
                eng.put(
                    &cf,
                    format!("key{:02}", i).as_bytes(),
                    format!("v{}", round).as_bytes(),
                )
                .expect("put");
            }
            eng.flush_cf(&cf).expect("flush");
        }
        eng.compact_all().expect("compact");
    }

    // Assert - data should persist after compaction and restart
    let eng = MidgeEngine::open(opts).expect("reopen");
    for i in 0..50 {
        assert_get_equals(&eng, format!("key{:02}", i).as_bytes(), b"v2");
    }
}

#[test]
fn should_maintain_deterministic_output_given_same_input_when_compacting_twice() {
    for mode in disk_storage_modes() {
        // Arrange
        fn run_compaction(storage_mode: StorageMode) -> Vec<(Vec<u8>, Vec<u8>)> {
            let opts = MidgeOptions {
                storage_mode,
                memtable_size: 1024,
                enable_compaction: false,
                ..Default::default()
            };
            let eng = MidgeEngine::open(opts).expect("open");
            let cf = eng.default_column_family();

            // Same operations
            for round in 0..3 {
                for i in 0..20 {
                    eng.put(
                        &cf,
                        format!("key{:02}", i).as_bytes(),
                        format!("v{}", round).as_bytes(),
                    )
                    .expect("put");
                }
                eng.flush_cf(&cf).expect("flush");
            }
            eng.compact_all().expect("compact");

            // Collect all data
            use cntryl_midge::api::query::Query;
            let entries = eng.scan(&cf, Query::new()).expect("scan");
            let mut result: Vec<_> = entries
                .into_iter()
                .map(|(k, v)| (k.to_vec(), v.to_vec()))
                .collect();
            result.sort_by(|a, b| a.0.cmp(&b.0));
            result
        }

        let (name, storage_mode1, _dir1) = create_storage_mode(mode);
        let (_, storage_mode2, _dir2) = create_storage_mode(mode);

        // Act
        let result1 = run_compaction(storage_mode1);
        let result2 = run_compaction(storage_mode2);

        // Assert - same input should produce same output
        assert_eq!(
            result1, result2,
            "Compaction should be deterministic for {}",
            name
        );
    }
}

// ============================================================================
// TOMBSTONE HANDLING TESTS
// ============================================================================

#[test]
fn should_remove_deleted_keys_given_tombstones_when_compacting() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 1024,
            enable_compaction: false,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Write keys
        for i in 0..100 {
            eng.put(&cf, format!("key{:03}", i).as_bytes(), b"value")
                .expect("put");
        }
        // Delete half of them
        for i in 0..50 {
            eng.delete(&cf, format!("key{:03}", i).as_bytes())
                .expect("delete");
        }
        eng.flush_cf(&cf).expect("flush");

        // Act
        eng.compact_all().expect("compact");

        // Assert - deleted keys should be absent
        for i in 0..50 {
            assert_key_absent(&eng, format!("key{:03}", i).as_bytes());
        }
        // Remaining keys should be present
        for i in 50..100 {
            assert_get_equals(&eng, format!("key{:03}", i).as_bytes(), b"value");
        }
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_merge_overlapping_ssts_given_delete_after_put_when_compacting() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 1024,
            enable_compaction: false,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // SST1: put a=1, b=2
        eng.put(&cf, b"a", b"1").expect("put a");
        eng.put(&cf, b"b", b"2").expect("put b");
        eng.flush_cf(&cf).expect("flush 1");

        // SST2: update b, add c
        eng.put(&cf, b"b", b"2_updated").expect("put b updated");
        eng.put(&cf, b"c", b"3").expect("put c");
        eng.flush_cf(&cf).expect("flush 2");

        // SST3: delete a
        eng.delete(&cf, b"a").expect("delete a");
        eng.flush_cf(&cf).expect("flush 3");

        // Act
        eng.compact_all().expect("compact");

        // Assert
        assert_key_absent(&eng, b"a"); // Deleted
        assert_get_equals(&eng, b"b", b"2_updated"); // Updated
        assert_get_equals(&eng, b"c", b"3"); // Added
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

// ============================================================================
// SNAPSHOT PRESERVATION TESTS
// ============================================================================

#[test]
fn should_preserve_snapshot_view_given_compaction_runs_when_snapshot_held() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 1024,
            enable_compaction: false,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        eng.put(&cf, b"key", b"v1").expect("put v1");
        eng.flush_cf(&cf).expect("flush");
        let snap = eng.snapshot();

        eng.put(&cf, b"key", b"v2").expect("put v2");
        eng.flush_cf(&cf).expect("flush");

        // Act - compact while holding snapshot
        eng.compact_all().expect("compact");

        // Assert - snapshot should still see old value
        let snapshot_view = eng.get_at(&cf, b"key", &snap).expect("get_at");
        assert_eq!(
            snapshot_view,
            Some(Bytes::from("v1")),
            "{}: snapshot should see v1",
            name
        );

        // Current view should see latest
        let current_view = eng.get(&cf, b"key").expect("get");
        assert_eq!(
            current_view,
            Some(Bytes::from("v2")),
            "{}: current should see v2",
            name
        );
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_preserve_deleted_key_in_snapshot_given_tombstone_when_compacting() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 1024,
            enable_compaction: false,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        eng.put(&cf, b"key", b"value").expect("put");
        eng.flush_cf(&cf).expect("flush");
        let snap = eng.snapshot();

        eng.delete(&cf, b"key").expect("delete");
        eng.flush_cf(&cf).expect("flush");

        // Act - compact with tombstone
        eng.compact_all().expect("compact");

        // Assert - snapshot sees original, current sees deletion
        let snapshot_view = eng.get_at(&cf, b"key", &snap).expect("get_at");
        assert_eq!(
            snapshot_view,
            Some(Bytes::from("value")),
            "{}: snapshot should see value",
            name
        );

        let current_view = eng.get(&cf, b"key").expect("get");
        assert!(current_view.is_none(), "{}: current should see none", name);
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

// ============================================================================
// BACKGROUND COMPACTION TESTS
// ============================================================================

#[test]
fn should_trigger_background_compaction_given_threshold_exceeded_when_ssts_accumulate() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let hooks = TestHooks::new();
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 1024,
            enable_compaction: true,
            compaction_sst_threshold: 2,
            compaction_check_interval_ms: 50,
            test_hooks: Some(hooks.clone()),
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Act - create multiple SSTs to trigger background compaction
        for round in 0..4 {
            for i in 0..20 {
                eng.put(
                    &cf,
                    format!("key{:02}", i).as_bytes(),
                    format!("v{}", round).as_bytes(),
                )
                .expect("put");
            }
            eng.flush_cf(&cf).expect("flush");
        }

        // Wait for background compaction
        let _ = eng.wait_for_compaction(std::time::Duration::from_secs(10));

        // Assert - data should still be correct
        for i in 0..20 {
            assert_get_equals(&eng, format!("key{:02}", i).as_bytes(), b"v3");
        }
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

// ============================================================================
// CONCURRENT WRITE TESTS
// ============================================================================

#[test]
fn should_preserve_concurrent_writes_given_compaction_running_when_writes_continue() {
    use std::sync::Arc;

    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 4096,
            enable_compaction: false,
            ..Default::default()
        };
        let eng = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = eng.default_column_family();

        // Create initial data
        for i in 0..50 {
            eng.put(&cf, format!("key{:03}", i).as_bytes(), b"initial")
                .expect("put");
        }
        eng.flush_cf(&cf).expect("flush");

        // Act - concurrent writes and compaction
        let eng_clone = Arc::clone(&eng);
        let cf_clone = cf.clone();
        let writer = std::thread::spawn(move || {
            for i in 50..100 {
                eng_clone
                    .put(&cf_clone, format!("key{:03}", i).as_bytes(), b"concurrent")
                    .expect("put");
            }
        });

        eng.compact_all().expect("compact");
        writer.join().expect("writer join");
        eng.flush_cf(&cf).expect("flush final");

        // Assert - all data should be present
        for i in 0..50 {
            assert_get_equals(&eng, format!("key{:03}", i).as_bytes(), b"initial");
        }
        for i in 50..100 {
            assert_get_equals(&eng, format!("key{:03}", i).as_bytes(), b"concurrent");
        }
        eprintln!("✓ {}", name);
    }
}

// ============================================================================
// MULTI-CF COMPACTION TESTS
// ============================================================================

#[test]
fn should_compact_cf_independently_given_multiple_cfs_when_compacting_one() {
    use cntryl_midge::ColumnFamilyConfig;

    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 1024,
            enable_compaction: true, // Required for manual compaction
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf1 = eng
            .create_column_family("cf1", ColumnFamilyConfig::default())
            .expect("create cf1");
        let cf2 = eng
            .create_column_family("cf2", ColumnFamilyConfig::default())
            .expect("create cf2");

        // Write data to both CFs
        for i in 0..50 {
            eng.put(&cf1, format!("key{:02}", i).as_bytes(), b"cf1_value")
                .expect("put cf1");
            eng.put(&cf2, format!("key{:02}", i).as_bytes(), b"cf2_value")
                .expect("put cf2");
        }
        eng.flush().expect("flush all");

        // Act - compact only cf1
        eng.compact_range(&cf1, Some(b""), Some(b"~"))
            .expect("compact cf1");

        // Assert - both CFs should have their data intact
        for i in 0..50 {
            let key = format!("key{:02}", i);
            let cf1_val = eng.get(&cf1, key.as_bytes()).expect("get cf1");
            let cf2_val = eng.get(&cf2, key.as_bytes()).expect("get cf2");
            assert_eq!(
                cf1_val,
                Some(Bytes::from("cf1_value")),
                "cf1 {} {}",
                name,
                i
            );
            assert_eq!(
                cf2_val,
                Some(Bytes::from("cf2_value")),
                "cf2 {} {}",
                name,
                i
            );
        }
        eprintln!("✓ {}", name);
    }
}

// ============================================================================
// EDGE CASES
// ============================================================================

#[test]
fn should_handle_empty_compaction_given_no_data_when_triggered() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");

        // Act
        let result = eng.compact_all();

        // Assert - should succeed with no error
        assert!(result.is_ok(), "Failed for {}: {:?}", name, result);
    }
}

#[test]
fn should_handle_compaction_with_single_sst_given_one_flush_when_triggered() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 1024,
            enable_compaction: false,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        eng.put(&cf, b"key", b"value").expect("put");
        eng.flush_cf(&cf).expect("flush");

        // Act
        let result = eng.compact_all();

        // Assert
        assert!(result.is_ok(), "Failed for {}: {:?}", name, result);
        assert_get_equals(&eng, b"key", b"value");
    }
}

#[test]
fn should_handle_large_values_given_compaction_when_triggered() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 64 * 1024, // 64KB
            enable_compaction: false,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        let large_value = vec![b'x'; 10 * 1024]; // 10KB values
        for i in 0..10 {
            eng.put(&cf, format!("key{:02}", i).as_bytes(), &large_value)
                .expect("put");
        }
        eng.flush_cf(&cf).expect("flush");

        // Act
        eng.compact_all().expect("compact");

        // Assert - all large values should be preserved
        for i in 0..10 {
            let result = eng
                .get(&cf, format!("key{:02}", i).as_bytes())
                .expect("get");
            assert_eq!(
                result,
                Some(Bytes::from(large_value.clone())),
                "Large value failed for {} key {}",
                name,
                i
            );
        }
    }
}
