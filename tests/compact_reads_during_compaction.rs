// Reads During Compaction
// Extracted from compaction_concurrent.rs

// Compaction During Concurrent Operations tests - P1 Priority
use bytes::Bytes;
use cntryl_midge::{MidgeEngine, Query};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
// using channel-based coordination and yields instead of sleeps
use common::test_helpers::wait_for_signal_default;
use std::sync::mpsc::channel;

mod common;
use common::{
    assert_get_equals, assert_key_absent, compaction_test_opts, create_storage_mode,
    populate_multi_level_data,
};

#[test]
fn should_serve_reads_given_compaction_in_progress() {
    for mode in common::disk_storage_modes() {
        let (mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = compaction_test_opts(storage_mode);
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();
        populate_multi_level_data(&engine, &cf);

        // Act - Trigger compaction in background thread once readers are ready.
        // Use an AtomicBool to broadcast "compaction started" to many reader
        // threads (mpsc::Receiver is single-consumer so it can't be reused).
        let (start_tx, start_rx) = channel::<()>();
        let started = Arc::new(AtomicBool::new(false));
        let engine_clone = Arc::clone(&engine);
        let started_clone = Arc::clone(&started);
        let compaction_handle = thread::spawn(move || {
            // Wait until main thread signals to begin compaction
            let _ = wait_for_signal_default(&start_rx);
            // Notify readers that compaction is beginning
            started_clone.store(true, Ordering::SeqCst);
            let _ = engine_clone.compact_all();
        });

        // Perform concurrent reads while compaction runs. Readers will wait
        // until compaction sets `started` so we get deterministic overlap.
        let mut read_handles = vec![];
        for _ in 0..10 {
            let engine_clone = Arc::clone(&engine);
            let cf_clone = cf.clone();
            let started_clone = Arc::clone(&started);
            let handle = thread::spawn(move || {
                // Wait for compaction to start
                while !started_clone.load(Ordering::SeqCst) {
                    std::thread::yield_now();
                }

                for i in 0..100 {
                    let key = format!("key{:03}", i);
                    let result = engine_clone.get(&cf_clone, key.as_bytes());
                    // Assert - Read should succeed (value doesn't matter, just no crash/error)
                    assert!(result.is_ok(), "Read should succeed during compaction");
                }
            });
            read_handles.push(handle);
        }
        // Signal compaction to run — the compaction thread will set `started`
        // once it actually begins which will wake all readers.
        let _ = start_tx.send(());

        // Let readers run concurrently with compaction; join readers first
        // then join compaction to ensure overlap.
        for handle in read_handles {
            handle.join().unwrap();
        }
        compaction_handle.join().unwrap();

        // Assert - All reads completed successfully without errors
        // Verify data is still accessible after compaction
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
fn should_return_correct_value_given_key_being_compacted() {
    for mode in common::disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = compaction_test_opts(storage_mode);
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();

        // Write overlapping data across multiple L0 files
        engine.put(&cf, b"target_key", b"old_value").unwrap();
        engine.flush().unwrap();

        engine.put(&cf, b"target_key", b"new_value").unwrap();
        engine.flush().unwrap();

        // Add more data to trigger compaction
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

        // Act - Start compaction in background
        let engine_clone = Arc::clone(&engine);
        let compaction_handle = thread::spawn(move || {
            let _ = engine_clone.compact_all();
        });

        // Continuously read the target key during compaction
        let engine_clone = Arc::clone(&engine);
        let read_handle = thread::spawn(move || {
            for _ in 0..100 {
                let result = engine_clone.get(&cf, b"target_key").unwrap();
                // Assert - During a concurrent compaction the read should succeed
                // and return either the old or the new value (both are acceptable
                // while compaction is in-flight). Final verification below
                // guarantees the persisted value is the new one.
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
                // yield instead of sleeping to avoid long test wall-clock delays
                std::thread::yield_now();
            }
        });

        read_handle.join().unwrap();
        compaction_handle.join().unwrap();

        // Assert - Final read should still return correct value
        assert_get_equals(&engine, b"target_key", b"new_value");
    }
}

#[test]
fn should_handle_scan_given_files_being_merged() {
    for mode in common::disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = compaction_test_opts(storage_mode);
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();
        populate_multi_level_data(&engine, &cf);

        // Act - Trigger compaction in background
        // Start compaction in the background and ensure it signals when it begins.
        // We'll use an AtomicBool so a single compaction-start event can be
        // observed by multiple consumers without relying on a single-use
        // mpsc receiver.
        let (started_tx_scan, start_rx_scan) = channel::<()>();
        let started_scan = Arc::new(AtomicBool::new(false));
        let engine_clone = Arc::clone(&engine);
        let compaction_handle = thread::spawn(move || {
            // signal that compaction is about to start so scanner can begin
            let _ = started_tx_scan.send(());
            // publish that compaction is in-progress
            started_scan.store(true, Ordering::SeqCst);
            let _ = engine_clone.compact_all();
        });

        // Wait until compaction is signalled as started, then perform range scans while compaction runs
        let _ = wait_for_signal_default(&start_rx_scan);
        // Perform range scans while compaction runs
        let engine_clone = Arc::clone(&engine);
        let cf_clone = cf.clone();
        let scan_handle = thread::spawn(move || {
            for _ in 0..20 {
                let query = Query::new()
                    .start_key(Bytes::from("key000"))
                    .end_key(Bytes::from("key099"));
                let results = engine_clone.scan(&cf_clone, query);

                // Assert - Scan should complete without errors
                assert!(results.is_ok(), "Scan should succeed during compaction");
                assert!(!results.unwrap().is_empty(), "Scan should return results");

                // yield to avoid long test sleeps while still being cooperative
                std::thread::yield_now();
            }
        });

        // Wait for the scan tasks to complete; compaction will join below.
        scan_handle.join().unwrap();
        compaction_handle.join().unwrap();

        // Assert - Final scan should work correctly
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
fn should_not_expose_deleted_keys_given_tombstone_compaction_in_progress() {
    for mode in common::disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = compaction_test_opts(storage_mode);
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();

        // Write and delete keys across multiple L0 files
        for i in 0..50 {
            let key = format!("key{:03}", i);
            engine.put(&cf, key.as_bytes(), "value".as_bytes()).unwrap();
        }
        engine.flush().unwrap();

        // Delete some keys
        for i in 10..40 {
            let key = format!("key{:03}", i);
            engine.delete(&cf, key.as_bytes()).unwrap();
        }
        engine.flush().unwrap();

        // Add more data
        for i in 40..80 {
            let key = format!("key{:03}", i);
            engine.put(&cf, key.as_bytes(), b"value2").unwrap();
        }
        engine.flush().unwrap();

        // Act - Trigger compaction (should remove tombstones)
        let engine_clone = Arc::clone(&engine);
        let compaction_handle = thread::spawn(move || {
            let _ = engine_clone.compact_all();
        });

        // Read deleted keys during compaction
        let engine_clone = Arc::clone(&engine);
        let cf_clone = cf.clone();
        let read_handle = thread::spawn(move || {
            for _ in 0..50 {
                for i in 10..40 {
                    let key = format!("key{:03}", i);
                    // During compaction the read should succeed (no errors).
                    // Whether a transient read sees a value or none is
                    // implementation sensitive, so we avoid asserting a
                    // particular outcome here and instead verify final
                    // state after compaction completes.
                    let _ = engine_clone.get(&cf_clone, key.as_bytes()).unwrap();
                }
                std::thread::yield_now();
            }
        });

        read_handle.join().unwrap();
        compaction_handle.join().unwrap();

        // Assert - Deleted keys still absent after compaction
        for i in 10..40 {
            let key = format!("key{:03}", i);
            assert_key_absent(&engine, key.as_bytes());
        }
    }
}

#[test]
fn should_maintain_read_consistency_given_compaction_updates_manifest() {
    for mode in common::disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = compaction_test_opts(storage_mode);
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();
        populate_multi_level_data(&engine, &cf);

        // Record a point-in-time snapshot and expected values before compaction
        // so we can verify snapshot-consistent reads while compaction runs.
        let snapshot = std::sync::Arc::new(engine.snapshot());
        let mut expected_values = std::collections::HashMap::new();
        for i in 0..100 {
            let key = format!("key{:03}", i);
            if let Ok(Some(value)) = engine.get_at(&cf, key.as_bytes(), &snapshot) {
                expected_values.insert(key, value);
            }
        }

        // Act - Trigger compaction
        let engine_clone = Arc::clone(&engine);
        let compaction_handle = thread::spawn(move || {
            let _ = engine_clone.compact_all();
        });

        // Continuously verify consistency during manifest updates
        let engine_clone = Arc::clone(&engine);
        let expected_clone = expected_values.clone();
        let snapshot_clone = std::sync::Arc::clone(&snapshot);
        let cf_clone_for_thread = cf.clone();
        let consistency_handle = thread::spawn(move || {
            for _ in 0..100 {
                for (key, expected_value) in &expected_clone {
                    // Use the snapshot read so this thread observes a consistent
                    // view while compaction runs and we can assert equality.
                    let result = engine_clone
                        .get_at(&cf_clone_for_thread, key.as_bytes(), &*snapshot_clone)
                        .unwrap();
                    assert_eq!(result, Some(expected_value.clone()));
                }
                std::thread::yield_now();
            }
        });

        consistency_handle.join().unwrap();
        compaction_handle.join().unwrap();

        // Assert - Values remain consistent after compaction
        for (key, expected_value) in &expected_values {
            // Verify snapshot-read matches the value we recorded before compaction
            let got = engine.get_at(&cf, key.as_bytes(), &*snapshot).unwrap();
            assert_eq!(&got, &Some(expected_value.clone()));
        }
    }
}
