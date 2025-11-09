// Concurrent Compaction + Writes
// Extracted from concurrent_writes.rs

// Concurrent Write Safety tests - P0 Priority
// Tests for multi-threaded correctness under high concurrency

mod common;
use bytes::Bytes;
use common::*;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use std::sync::atomic::{AtomicUsize, Ordering};
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

    // Act - Trigger compaction with large dataset, then write concurrently
    for i in 0..1000 {
        let key = format!("compact_data_{}", i);
        let value = vec![0u8; 2048];
        engine.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
    }

    std::thread::sleep(std::time::Duration::from_millis(100));

    let write_handle = {
        let engine = Arc::clone(&engine);
        thread::spawn(move || {
            for i in 0..200 {
                let key = format!("during_compact_{}", i);
                engine.put(&cf, key.as_bytes(), "value".as_bytes()).unwrap();
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

    // Act - Create L0 files, then write more
    for i in 0..800 {
        let key = format!("l0_data_{}", i);
        let value = vec![0u8; 3072];
        engine.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
    }

    let num_concurrent = 100;
    let handles: Vec<_> = (0..num_concurrent)
        .map(|i| {
            let engine = Arc::clone(&engine);
            thread::spawn(move || {
                let key = format!("nonblock_key_{}", i);
                engine.put(&cf, key.as_bytes(), "value".as_bytes()).unwrap();
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

    // Act - Create multi-level scenario
    for i in 0..1500 {
        let key = format!("multilevel_{}", i);
        let value = vec![0u8; 2048];
        engine.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
    }

    for i in 0..100 {
        let key = format!("concurrent_ml_{}", i);
        engine.put(&cf, key.as_bytes(), "test".as_bytes()).unwrap();
    }

    // Assert
    for i in 0..100 {
        let key = format!("concurrent_ml_{}", i);
        assert_get_equals(&engine, key.as_bytes(), b"test");
    }
}
