//! Compaction Concurrent Tests
//!
//! These tests verify that concurrent operations work correctly during compaction:
//! - Reads during compaction (point reads and scans)
//! - Writes during compaction (memtable and flush)
//! - Snapshot consistency during compaction
//! - Tombstone handling during concurrent reads
//! - Multi-level compaction with concurrent operations
//!
//! # Storage Mode Coverage
//! - Uses `LocalDisk` and `CloudBacked` modes (compaction requires SST files)

mod common;

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, Query};
use common::{
    assert_get_equals, assert_key_absent, compaction_test_opts, create_storage_mode,
    disk_storage_modes, manual_compaction_test_opts, populate_multi_level_data,
    test_helpers::wait_for_signal_default,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::thread;

// ============================================================================
// READS DURING COMPACTION
// ============================================================================

#[test]
fn should_serve_reads_given_compaction_in_progress_when_reading() {
    for mode in disk_storage_modes() {
        let (mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);

        // Arrange
        let opts = manual_compaction_test_opts(storage_mode);
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();
        populate_multi_level_data(&engine, &cf);

        // Act - Trigger compaction and coordinate with readers
        let (start_tx, start_rx) = channel::<()>();
        let started = Arc::new(AtomicBool::new(false));
        let engine_clone = Arc::clone(&engine);
        let started_clone = Arc::clone(&started);
        let compaction_handle = thread::spawn(move || {
            wait_for_signal_default(&start_rx);
            started_clone.store(true, Ordering::SeqCst);
            let _ = engine_clone.compact_all();
        });

        // Perform concurrent reads while compaction runs
        let mut read_handles = vec![];
        for _ in 0..10 {
            let engine_clone = Arc::clone(&engine);
            let cf_clone = cf.clone();
            let started_clone = Arc::clone(&started);
            let handle = thread::spawn(move || {
                while !started_clone.load(Ordering::SeqCst) {
                    std::thread::yield_now();
                }

                for i in 0..100 {
                    let key = format!("key{:03}", i);
                    let result = engine_clone.get(&cf_clone, key.as_bytes());
                    assert!(result.is_ok(), "Read should succeed during compaction");
                }
            });
            read_handles.push(handle);
        }

        let _ = start_tx.send(());

        for handle in read_handles {
            handle.join().unwrap();
        }
        compaction_handle.join().unwrap();

        // Assert
        for i in 0..100 {
            let key = format!("key{:03}", i);
            let result = engine.get(&cf, key.as_bytes()).unwrap();
            assert!(
                result.is_some(),
                "Key should exist after compaction for mode: {}",
                mode_name
            );
        }
    }
}

#[test]
fn should_return_correct_value_given_key_being_compacted_when_reading() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);

        // Arrange
        let opts = manual_compaction_test_opts(storage_mode);
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();

        // Write overlapping data across multiple L0 files
        engine.put(&cf, b"target_key", b"old_value").unwrap();
        engine.flush().unwrap();

        engine.put(&cf, b"target_key", b"new_value").unwrap();
        engine.flush().unwrap();

        for i in 0..100 {
            engine
                .put(
                    &cf,
                    format!("key{:03}", i).as_bytes(),
                    format!("val{:03}", i).as_bytes(),
                )
                .unwrap();
        }
        engine.flush().unwrap();

        // Act
        let engine_clone = Arc::clone(&engine);
        let compaction_handle = thread::spawn(move || {
            let _ = engine_clone.compact_all();
        });

        let engine_clone = Arc::clone(&engine);
        let read_handle = thread::spawn(move || {
            for _ in 0..100 {
                let result = engine_clone.get(&cf, b"target_key").unwrap();
                assert!(
                    result.is_some(),
                    "Read should return a value during compaction"
                );
                let val = result.unwrap();
                assert!(
                    val.as_ref() == b"new_value" || val.as_ref() == b"old_value",
                    "Unexpected value during compaction: {:?}",
                    val
                );
                std::thread::yield_now();
            }
        });

        read_handle.join().unwrap();
        compaction_handle.join().unwrap();

        // Assert
        assert_get_equals(&engine, b"target_key", b"new_value");
    }
}

#[test]
fn should_handle_scan_given_files_being_merged_when_scanning() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);

        // Arrange
        let opts = manual_compaction_test_opts(storage_mode);
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();
        populate_multi_level_data(&engine, &cf);

        // Act
        let (started_tx, start_rx) = channel::<()>();
        let engine_clone = Arc::clone(&engine);
        let compaction_handle = thread::spawn(move || {
            let _ = started_tx.send(());
            let _ = engine_clone.compact_all();
        });

        wait_for_signal_default(&start_rx);

        let engine_clone = Arc::clone(&engine);
        let cf_clone = cf.clone();
        let scan_handle = thread::spawn(move || {
            for _ in 0..20 {
                let query = Query::new()
                    .start_key(Bytes::from("key000"))
                    .end_key(Bytes::from("key099"));
                let results = engine_clone.scan(&cf_clone, query);
                assert!(results.is_ok(), "Scan should succeed during compaction");
                assert!(!results.unwrap().is_empty(), "Scan should return results");
                std::thread::yield_now();
            }
        });

        scan_handle.join().unwrap();
        compaction_handle.join().unwrap();

        // Assert
        let query = Query::new()
            .start_key(Bytes::from("key000"))
            .end_key(Bytes::from("key099"));
        let results = engine.scan(&cf, query).unwrap();
        assert!(
            !results.is_empty(),
            "Should have keys in range after compaction"
        );
    }
}

#[test]
fn should_not_expose_deleted_keys_given_tombstone_compaction_in_progress_when_reading() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);

        // Arrange
        let opts = manual_compaction_test_opts(storage_mode);
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();

        for i in 0..50 {
            let key = format!("key{:03}", i);
            engine.put(&cf, key.as_bytes(), "value".as_bytes()).unwrap();
        }
        engine.flush().unwrap();

        for i in 10..40 {
            let key = format!("key{:03}", i);
            engine.delete(&cf, key.as_bytes()).unwrap();
        }
        engine.flush().unwrap();

        for i in 40..80 {
            let key = format!("key{:03}", i);
            engine.put(&cf, key.as_bytes(), b"value2").unwrap();
        }
        engine.flush().unwrap();

        // Act
        let engine_clone = Arc::clone(&engine);
        let compaction_handle = thread::spawn(move || {
            let _ = engine_clone.compact_all();
        });

        let engine_clone = Arc::clone(&engine);
        let cf_clone = cf.clone();
        let read_handle = thread::spawn(move || {
            for _ in 0..50 {
                for i in 10..40 {
                    let key = format!("key{:03}", i);
                    let _ = engine_clone.get(&cf_clone, key.as_bytes()).unwrap();
                }
                std::thread::yield_now();
            }
        });

        read_handle.join().unwrap();
        compaction_handle.join().unwrap();

        // Assert
        for i in 10..40 {
            let key = format!("key{:03}", i);
            assert_key_absent(&engine, key.as_bytes());
        }
    }
}

#[test]
fn should_maintain_read_consistency_given_compaction_updates_manifest_when_reading() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);

        // Arrange
        let opts = manual_compaction_test_opts(storage_mode);
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();
        populate_multi_level_data(&engine, &cf);

        let snapshot = Arc::new(engine.snapshot());
        let mut expected_values = std::collections::HashMap::new();
        for i in 0..100 {
            let key = format!("key{:03}", i);
            if let Ok(Some(value)) = engine.get_at(&cf, key.as_bytes(), &snapshot) {
                expected_values.insert(key, value);
            }
        }

        // Act
        let engine_clone = Arc::clone(&engine);
        let compaction_handle = thread::spawn(move || {
            let _ = engine_clone.compact_all();
        });

        let engine_clone = Arc::clone(&engine);
        let expected_clone = expected_values.clone();
        let snapshot_clone = Arc::clone(&snapshot);
        let cf_clone = cf.clone();
        let consistency_handle = thread::spawn(move || {
            for _ in 0..100 {
                for (key, expected_value) in &expected_clone {
                    let result = engine_clone
                        .get_at(&cf_clone, key.as_bytes(), &snapshot_clone)
                        .unwrap();
                    assert_eq!(result, Some(expected_value.clone()));
                }
                std::thread::yield_now();
            }
        });

        consistency_handle.join().unwrap();
        compaction_handle.join().unwrap();

        // Assert
        for (key, expected_value) in &expected_values {
            let got = engine.get_at(&cf, key.as_bytes(), &snapshot).unwrap();
            assert_eq!(&got, &Some(expected_value.clone()));
        }
    }
}

// ============================================================================
// WRITES DURING COMPACTION
// ============================================================================

#[test]
fn should_allow_writes_given_l0_l1_compaction_running_when_writing() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);

        // Arrange
        let opts = manual_compaction_test_opts(storage_mode);
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();
        populate_multi_level_data(&engine, &cf);

        // Act
        let engine_clone = Arc::clone(&engine);
        let compaction_handle = thread::spawn(move || {
            let _ = engine_clone.compact_all();
        });

        let engine_clone = Arc::clone(&engine);
        let cf_clone = cf.clone();
        let write_handle = thread::spawn(move || {
            for i in 0..100 {
                let key = format!("new_key{:03}", i);
                let value = format!("new_value{}", i);
                let result = engine_clone.put(&cf_clone, key.as_bytes(), value.as_bytes());
                assert!(result.is_ok(), "Write should succeed during compaction");
            }
        });

        write_handle.join().unwrap();
        compaction_handle.join().unwrap();

        // Assert
        for i in 0..100 {
            let key = format!("new_key{:03}", i);
            let expected_value = format!("new_value{}", i);
            assert_get_equals(&engine, key.as_bytes(), expected_value.as_bytes());
        }
    }
}

#[test]
fn should_handle_put_to_compacting_key_range_when_writing() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);

        // Arrange
        let opts = manual_compaction_test_opts(storage_mode);
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();

        for i in 0..100 {
            let key = format!("key{:03}", i);
            engine.put(&cf, key.as_bytes(), b"old_value").unwrap();
        }
        engine.flush().unwrap();

        for i in 0..100 {
            let key = format!("key{:03}", i);
            engine.put(&cf, key.as_bytes(), b"updated_value").unwrap();
        }
        engine.flush().unwrap();

        // Act
        let (start_tx, start_rx) = channel::<()>();
        let (started_tx, started_rx) = channel::<()>();
        let engine_clone = Arc::clone(&engine);
        let compaction_handle = thread::spawn(move || {
            wait_for_signal_default(&start_rx);
            started_tx.send(()).unwrap();
            let _ = engine_clone.compact_all();
        });

        let engine_clone = Arc::clone(&engine);
        let write_handle = thread::spawn(move || {
            wait_for_signal_default(&started_rx);
            for i in 25..75 {
                let key = format!("key{:03}", i);
                let result = engine_clone.put(&cf, key.as_bytes(), "newest_value".as_bytes());
                assert!(result.is_ok(), "Write to compacting range should succeed");
            }
        });

        let _ = start_tx.send(());
        write_handle.join().unwrap();
        compaction_handle.join().unwrap();

        // Assert
        for i in 25..75 {
            let key = format!("key{:03}", i);
            assert_get_equals(&engine, key.as_bytes(), b"newest_value");
        }
    }
}

#[test]
fn should_flush_to_new_sst_given_ongoing_compaction_when_flushing() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);

        // Arrange
        let opts = manual_compaction_test_opts(storage_mode);
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();
        populate_multi_level_data(&engine, &cf);

        // Act
        let (start_tx, start_rx) = channel::<()>();
        let (started_tx, started_rx) = channel::<()>();
        let engine_clone = Arc::clone(&engine);
        let compaction_handle = thread::spawn(move || {
            wait_for_signal_default(&start_rx);
            started_tx.send(()).unwrap();
            let _ = engine_clone.compact_all();
        });

        let engine_clone = Arc::clone(&engine);
        let cf_clone = cf.clone();
        let flush_handle = thread::spawn(move || {
            wait_for_signal_default(&started_rx);

            for i in 200..250 {
                let key = format!("flush_key{:03}", i);
                engine_clone
                    .put(&cf_clone, key.as_bytes(), b"flush_value")
                    .unwrap();
            }

            let result = engine_clone.flush();
            assert!(result.is_ok(), "Flush should succeed during compaction");
        });

        let _ = start_tx.send(());
        flush_handle.join().unwrap();
        compaction_handle.join().unwrap();

        // Assert
        for i in 200..250 {
            let key = format!("flush_key{:03}", i);
            assert_get_equals(&engine, key.as_bytes(), b"flush_value");
        }
    }
}

#[test]
fn should_not_corrupt_newly_flushed_files_given_compaction_in_progress_when_flushing() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);

        // Arrange
        let opts = manual_compaction_test_opts(storage_mode);
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();
        populate_multi_level_data(&engine, &cf);

        // Act
        let (start_tx, start_rx) = channel::<()>();
        let (started_tx, started_rx) = channel::<()>();
        let engine_clone = Arc::clone(&engine);
        let compaction_handle = thread::spawn(move || {
            wait_for_signal_default(&start_rx);
            started_tx.send(()).unwrap();
            let _ = engine_clone.compact_all();
        });

        let engine_clone = Arc::clone(&engine);
        let cf_clone = cf.clone();
        let flush_handle = thread::spawn(move || {
            wait_for_signal_default(&started_rx);
            for i in 300..350 {
                let key = format!("late_key{:03}", i);
                engine_clone
                    .put(&cf_clone, key.as_bytes(), b"late_value")
                    .unwrap();
            }
            engine_clone.flush().unwrap();
        });

        let _ = start_tx.send(());
        flush_handle.join().unwrap();
        compaction_handle.join().unwrap();

        // Assert
        for i in 300..350 {
            let key = format!("late_key{:03}", i);
            let result = engine.get(&cf, key.as_bytes()).unwrap();
            assert!(result.is_some(), "Newly flushed key should exist");
            assert_eq!(result.unwrap().as_ref(), b"late_value");
        }
    }
}

// ============================================================================
// SNAPSHOT ISOLATION DURING COMPACTION
// ============================================================================

#[test]
fn should_preserve_snapshot_view_given_compaction_in_progress_when_snapshot_read() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);

        // Arrange - use smaller dataset for reliable snapshot test
        let opts = compaction_test_opts(storage_mode);
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();

        // Write initial values and flush to SST
        for i in 0..10 {
            let key = format!("key{:02}", i);
            let value = format!("value{:02}", i);
            engine.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
        }
        engine.flush().unwrap();

        // Create snapshot after flush completes
        let snapshot = engine.snapshot();

        // Write new values and flush to create more L0 files
        for i in 0..10 {
            let key = format!("key{:02}", i);
            engine.put(&cf, key.as_bytes(), b"new_value").unwrap();
        }
        engine.flush().unwrap();

        // Act - trigger compaction using compact_range for more targeted operation
        engine.compact_range(&cf, Some(b""), Some(b"~")).unwrap();

        // Assert - snapshot still valid after compaction
        for i in 0..10 {
            let key = format!("key{:02}", i);
            let expected_value = format!("value{:02}", i);
            let result = engine.get_at(&cf, key.as_bytes(), &snapshot).unwrap();
            assert_eq!(
                result,
                Some(Bytes::from(expected_value)),
                "Snapshot read should return old value after compaction"
            );
        }

        // Also verify current values are updated
        for i in 0..10 {
            let key = format!("key{:02}", i);
            let result = engine.get(&cf, key.as_bytes()).unwrap();
            assert_eq!(
                result,
                Some(Bytes::from_static(b"new_value")),
                "Current read should return new value after compaction"
            );
        }
    }
}

// ============================================================================
// ITERATOR STABILITY DURING COMPACTION
// ============================================================================

#[test]
fn should_maintain_iterator_stability_given_compaction_in_progress_when_iterating() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);

        // Arrange
        let opts = manual_compaction_test_opts(storage_mode);
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();
        populate_multi_level_data(&engine, &cf);

        let snapshot = engine.snapshot();

        // Act
        let engine_clone = Arc::clone(&engine);
        let compaction_handle = thread::spawn(move || {
            let _ = engine_clone.compact_all();
        });

        // Perform repeated scans during compaction using snapshot
        for _ in 0..10 {
            let query = Query::new()
                .start_key(Bytes::from("key000"))
                .end_key(Bytes::from("key099"));
            let results = engine.scan_at(&cf, query, &snapshot).unwrap();
            assert!(
                !results.is_empty(),
                "Scan should return results during compaction"
            );
            std::thread::yield_now();
        }

        compaction_handle.join().unwrap();

        // Assert
        let query = Query::new()
            .start_key(Bytes::from("key000"))
            .end_key(Bytes::from("key099"));
        let results = engine.scan_at(&cf, query, &snapshot).unwrap();
        assert!(
            !results.is_empty(),
            "Scan should return results after compaction"
        );
    }
}

// ============================================================================
// CONCURRENT COMPACTIONS
// ============================================================================

#[test]
fn should_serialize_concurrent_compaction_requests_when_multiple_triggered() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);

        // Arrange
        let opts = manual_compaction_test_opts(storage_mode);
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();
        populate_multi_level_data(&engine, &cf);

        // Act - Trigger multiple compactions concurrently
        let handles: Vec<_> = (0..5)
            .map(|_| {
                let engine_clone = Arc::clone(&engine);
                thread::spawn(move || {
                    let _ = engine_clone.compact_all();
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // Assert - Data should be intact
        for i in 0..100 {
            let key = format!("key{:03}", i);
            let result = engine.get(&cf, key.as_bytes()).unwrap();
            assert!(
                result.is_some(),
                "Key should exist after concurrent compactions"
            );
        }
    }
}
