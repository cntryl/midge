// Reads During Compaction
// Extracted from compaction_concurrent.rs

// Compaction During Concurrent Operations tests - P1 Priority
use bytes::Bytes;
use cntryl_midge::{ColumnFamilyHandle, MidgeEngine, MidgeOptions, Query, StorageMode};
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

#[test]
fn should_serve_reads_given_compaction_in_progress() {
    // Arrange
    let opts = compaction_test_opts();
    let engine = Arc::new(MidgeEngine::open(opts).unwrap());
    let cf = engine.default_column_family();
    populate_multi_level_data(&engine, &cf);

    // Act - Trigger compaction in background thread
    let engine_clone = Arc::clone(&engine);
    let compaction_handle = thread::spawn(move || {
        thread::sleep(Duration::from_millis(10)); // Small delay
        let _ = engine_clone.compact_all();
    });

    // Perform concurrent reads while compaction runs
    let mut read_handles = vec![];
    for _ in 0..10 {
        let engine_clone = Arc::clone(&engine);
        let cf_clone = cf.clone();
        let handle = thread::spawn(move || {
            for i in 0..100 {
                let key = format!("key{:03}", i);
                let result = engine_clone.get(&cf_clone, key.as_bytes());
                // Assert - Read should succeed (value doesn't matter, just no crash/error)
                assert!(result.is_ok(), "Read should succeed during compaction");
            }
        });
        read_handles.push(handle);
    }

    for handle in read_handles {
        handle.join().unwrap();
    }
    compaction_handle.join().unwrap();

    // Assert - All reads completed successfully without errors
    // Verify data is still accessible after compaction
    for i in 0..100 {
        let key = format!("key{:03}", i);
        let result = engine.get(&cf, key.as_bytes()).unwrap();
        assert!(result.is_some(), "Key should exist after compaction");
    }
}

#[test]
fn should_return_correct_value_given_key_being_compacted() {
    // Arrange
    let opts = compaction_test_opts();
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
            // Assert - Should always return the latest value
            assert!(result.is_some());
            assert_eq!(result.unwrap().as_ref(), b"new_value");
            thread::sleep(Duration::from_micros(100));
        }
    });

    read_handle.join().unwrap();
    compaction_handle.join().unwrap();

    // Assert - Final read should still return correct value
    assert_get_equals(&engine, b"target_key", b"new_value");
}

#[test]
fn should_handle_scan_given_files_being_merged() {
    // Arrange
    let opts = compaction_test_opts();
    let engine = Arc::new(MidgeEngine::open(opts).unwrap());
    let cf = engine.default_column_family();
    populate_multi_level_data(&engine, &cf);

    // Act - Trigger compaction in background
    let engine_clone = Arc::clone(&engine);
    let compaction_handle = thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        let _ = engine_clone.compact_all();
    });

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

            thread::sleep(Duration::from_millis(5));
        }
    });

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

#[test]
fn should_not_expose_deleted_keys_given_tombstone_compaction_in_progress() {
    // Arrange
    let opts = compaction_test_opts();
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
                let result = engine_clone.get(&cf_clone, key.as_bytes()).unwrap();
                // Assert - Deleted keys should not be visible
                assert!(result.is_none(), "Deleted key should not be visible");
            }
            thread::sleep(Duration::from_millis(2));
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

#[test]
fn should_maintain_read_consistency_given_compaction_updates_manifest() {
    // Arrange
    let opts = compaction_test_opts();
    let engine = Arc::new(MidgeEngine::open(opts).unwrap());
    let cf = engine.default_column_family();
    populate_multi_level_data(&engine, &cf);

    // Record expected values before compaction
    let mut expected_values = std::collections::HashMap::new();
    for i in 0..100 {
        let key = format!("key{:03}", i);
        if let Ok(Some(value)) = engine.get(&cf, key.as_bytes()) {
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
    let consistency_handle = thread::spawn(move || {
        for _ in 0..100 {
            for (key, expected_value) in &expected_clone {
                let result = engine_clone.get(&cf, key.as_bytes()).unwrap();
                // Assert - Should always read the same value (read consistency)
                assert!(result.is_some(), "Key should exist");
                assert_eq!(result.unwrap().as_ref(), expected_value.as_ref());
            }
            thread::sleep(Duration::from_millis(2));
        }
    });

    consistency_handle.join().unwrap();
    compaction_handle.join().unwrap();

    // Assert - Values remain consistent after compaction
    for (key, expected_value) in &expected_values {
        assert_get_equals(&engine, key.as_bytes(), expected_value.as_ref());
    }
}
