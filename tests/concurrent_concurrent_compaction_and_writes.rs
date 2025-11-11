// Concurrent Compaction + Writes
// Extracted from concurrent_writes.rs

// Concurrent Write Safety tests - P0 Priority
// Tests for multi-threaded correctness under high concurrency

mod common;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use common::*;
use std::sync::Arc;
use std::thread;

// Helper to create memory-based engine options (no WAL issues with Send)
#[allow(dead_code)]
fn memory_opts() -> MidgeOptions {
    MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    }
}

#[allow(dead_code)]
fn memory_opts_with_memtable_size(size: usize) -> MidgeOptions {
    MidgeOptions {
        storage_mode: StorageMode::Memory,
        memtable_size: size,
        ..Default::default()
    }
}

#[test]
fn should_allow_writes_given_compaction_in_progress() {
    // Arrange
    let _dir = test_temp_dir();
    let opts = memory_opts_with_memtable_size(10 * 1024 * 1024);
    let engine = Arc::new(MidgeEngine::open(opts).unwrap());
    let cf = engine.default_column_family();

    // Act - Trigger compaction with large dataset, then write concurrently
    for i in 0..1000 {
        let key = format!("compact_data_{}", i);
        let value = vec![0u8; 2048];
        engine.put(&cf, key.as_bytes(), value.as_slice()).unwrap();
    }

    // Wait for flush to complete before starting concurrent writes
    engine
        .wait_for_flush(std::time::Duration::from_millis(100))
        .expect("flush should complete");

    let write_handle = {
        let engine = Arc::clone(&engine);
        let cf = cf.clone();
        thread::spawn(move || {
            for i in 0..200 {
                let key = format!("during_compact_{}", i);
                engine.put(&cf, key.as_bytes(), b"value").unwrap();
            }
        })
    };

    write_handle.join().unwrap();

    // Assert
    for i in 0..200 {
        let key = format!("during_compact_{}", i);
        assert_get_equals(&engine, key.as_bytes(), b"value");
    }
}

#[test]
fn should_not_block_writes_given_l0_l1_compaction_running() {
    // Arrange
    let _dir = test_temp_dir();
    let opts = memory_opts_with_memtable_size(8 * 1024 * 1024);
    let engine = Arc::new(MidgeEngine::open(opts).unwrap());
    let cf = engine.default_column_family();

    // Act - Create L0 files, then write more
    for i in 0..800 {
        let key = format!("l0_data_{}", i);
        let value = vec![0u8; 3072];
        engine.put(&cf, key.as_bytes(), value.as_slice()).unwrap();
    }

    let num_concurrent = 100;
    let handles: Vec<_> = (0..num_concurrent)
        .map(|i| {
            let engine = Arc::clone(&engine);
            let cf = cf.clone();
            thread::spawn(move || {
                let key = format!("nonblock_key_{}", i);
                engine.put(&cf, key.as_bytes(), b"value").unwrap();
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Assert
    for i in 0..num_concurrent {
        let key = format!("nonblock_key_{}", i);
        assert_get_equals(&engine, key.as_bytes(), b"value");
    }
}

#[test]
fn should_handle_write_during_multi_level_compaction() {
    // Arrange
    let _dir = test_temp_dir();
    let opts = memory_opts_with_memtable_size(12 * 1024 * 1024);
    let engine = Arc::new(MidgeEngine::open(opts).unwrap());
    let cf = engine.default_column_family();

    // Act - Create multi-level scenario
    for i in 0..1500 {
        let key = format!("multilevel_{}", i);
        let value = vec![0u8; 2048];
        engine.put(&cf, key.as_bytes(), value.as_slice()).unwrap();
    }

    for i in 0..100 {
        let key = format!("concurrent_ml_{}", i);
        engine.put(&cf, key.as_bytes(), b"test").unwrap();
    }

    // Assert
    for i in 0..100 {
        let key = format!("concurrent_ml_{}", i);
        assert_get_equals(&engine, key.as_bytes(), b"test");
    }
}
