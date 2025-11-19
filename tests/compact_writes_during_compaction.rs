// Writes During Compaction
// Extracted from compaction_concurrent.rs

// Compaction During Concurrent Operations tests - P1 Priority
use cntryl_midge::MidgeEngine;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

mod common;
use common::{
    assert_get_equals, compaction_test_opts, create_storage_mode, populate_multi_level_data,
};

#[test]
fn should_allow_writes_given_l0_l1_compaction_running() {
    for mode in common::disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = compaction_test_opts(storage_mode);
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();
        populate_multi_level_data(&engine, &cf);

        // Act - Trigger compaction in background
        let engine_clone = Arc::clone(&engine);
        let compaction_handle = thread::spawn(move || {
            let _ = engine_clone.compact_all();
        });

        // Perform concurrent writes while compaction runs
        let engine_clone = Arc::clone(&engine);
        let cf_clone = cf.clone();
        let write_handle = thread::spawn(move || {
            for i in 0..100 {
                let key = format!("new_key{:03}", i);
                let value = format!("new_value{}", i);
                let result = engine_clone.put(&cf_clone, key.as_bytes(), value.as_bytes());
                // Assert - Writes should succeed during compaction
                assert!(result.is_ok(), "Write should succeed during compaction");
            }
        });

        write_handle.join().unwrap();
        compaction_handle.join().unwrap();

        // Assert - All new writes should be readable
        for i in 0..100 {
            let key = format!("new_key{:03}", i);
            let expected_value = format!("new_value{}", i);
            assert_get_equals(&engine, key.as_bytes(), expected_value.as_bytes());
        }
    }
}

#[test]
fn should_handle_put_to_compacting_key_range() {
    for mode in common::disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = compaction_test_opts(storage_mode);
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();

        // Write initial data that will be compacted
        for i in 0..100 {
            let key = format!("key{:03}", i);
            engine.put(&cf, key.as_bytes(), b"old_value").unwrap();
        }
        engine.flush().unwrap();

        // Add more L0 files to trigger compaction
        for i in 0..100 {
            let key = format!("key{:03}", i);
            engine.put(&cf, key.as_bytes(), b"updated_value").unwrap();
        }
        engine.flush().unwrap();

        // Act - Start compaction in background
        let engine_clone = Arc::clone(&engine);
        let compaction_handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            let _ = engine_clone.compact_all();
        });

        // Write to keys that are being compacted
        let engine_clone = Arc::clone(&engine);
        let write_handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(5)); // Let compaction start
            for i in 25..75 {
                let key = format!("key{:03}", i);
                let result = engine_clone.put(&cf, key.as_bytes(), "newest_value".as_bytes());
                // Assert - Writes to compacting range should succeed
                assert!(result.is_ok(), "Write to compacting range should succeed");
            }
        });

        write_handle.join().unwrap();
        compaction_handle.join().unwrap();

        // Assert - Latest writes should be visible
        for i in 25..75 {
            let key = format!("key{:03}", i);
            assert_get_equals(&engine, key.as_bytes(), b"newest_value");
        }
    }
}

#[test]
fn should_write_to_new_sst_given_ongoing_compaction_when_flush() {
    for mode in common::disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = compaction_test_opts(storage_mode);
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();
        populate_multi_level_data(&engine, &cf);

        // Act - Trigger compaction in background
        let engine_clone = Arc::clone(&engine);
        let compaction_handle = thread::spawn(move || {
            let _ = engine_clone.compact_all();
        });

        // Write new data and flush during compaction
        let engine_clone = Arc::clone(&engine);
        let cf_clone = cf.clone();
        let flush_handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10)); // Let compaction start

            // Write to memtable
            for i in 200..250 {
                let key = format!("flush_key{:03}", i);
                engine_clone
                    .put(&cf_clone, key.as_bytes(), b"flush_value")
                    .unwrap();
            }

            // Flush to create new SST during compaction
            let result = engine_clone.flush();
            // Assert - Flush should succeed even during compaction
            assert!(result.is_ok(), "Flush should succeed during compaction");
        });

        flush_handle.join().unwrap();
        compaction_handle.join().unwrap();

        // Assert - Flushed data should be readable
        for i in 200..250 {
            let key = format!("flush_key{:03}", i);
            assert_get_equals(&engine, key.as_bytes(), b"flush_value");
        }
    }
}

#[test]
fn should_not_compact_newly_flushed_files_given_compaction_in_progress() {
    for mode in common::disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = compaction_test_opts(storage_mode);
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();
        populate_multi_level_data(&engine, &cf);

        // Act - Start compaction and flush new data concurrently
        let engine_clone = Arc::clone(&engine);
        let compaction_handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(15)); // Let flush happen first
            let _ = engine_clone.compact_all();
        });

        // Flush new data shortly after compaction starts
        let engine_clone = Arc::clone(&engine);
        let cf_clone = cf.clone();
        let flush_handle = thread::spawn(move || {
            // Write and flush new data
            for i in 300..350 {
                let key = format!("late_key{:03}", i);
                engine_clone
                    .put(&cf_clone, key.as_bytes(), b"late_value")
                    .unwrap();
            }
            engine_clone.flush().unwrap();
        });

        flush_handle.join().unwrap();
        compaction_handle.join().unwrap();

        // Assert - Newly flushed data should be intact (not corrupted by ongoing compaction)
        for i in 300..350 {
            let key = format!("late_key{:03}", i);
            let result = engine.get(&cf, key.as_bytes()).unwrap();
            assert!(result.is_some(), "Newly flushed key should exist");
            assert_eq!(result.unwrap().as_ref(), b"late_value");
        }
    }
}
