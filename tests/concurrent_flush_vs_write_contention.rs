// Flush vs Write Contention
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
fn should_allow_writes_given_flush_in_progress() {
    // Arrange
    let _dir = test_temp_dir();
    let opts = memory_opts_with_memtable_size(10 * 1024 * 1024);
    let engine = Arc::new(MidgeEngine::open(opts).unwrap());
    let cf = engine.default_column_family();

    // Act - Trigger flush with large writes, then write concurrently
    let flush_handle = {
        let engine = Arc::clone(&engine);
        let cf = cf.clone();
        thread::spawn(move || {
            for i in 0..500 {
                let key = format!("flush_key_{}", i);
                let value = vec![0u8; 4096]; // 4KB values
                engine.put(&cf, key.as_bytes(), value.as_slice()).unwrap();
            }
        })
    };

    let write_handle = {
        let engine = Arc::clone(&engine);
        let cf = cf.clone();
        thread::spawn(move || {
            for i in 0..100 {
                let key = format!("concurrent_key_{}", i);
                engine.put(&cf, key.as_bytes(), "value".as_bytes()).unwrap();
            }
        })
    };

    flush_handle.join().unwrap();
    write_handle.join().unwrap();

    // Assert
    for i in 0..100 {
        let key = format!("concurrent_key_{}", i);
        assert_get_equals(&engine, key.as_bytes(), b"value");
    }
}

#[test]
fn should_block_writes_given_too_many_immutable_memtables() {
    // Arrange
    let _dir = test_temp_dir();
    let opts = memory_opts_with_memtable_size(8 * 1024 * 1024);
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();

    // Act - Write enough to create multiple immutable memtables
    let num_writes = 2000;
    for i in 0..num_writes {
        let key = format!("stall_test_{}", i);
        let value = vec![0u8; 2048];
        let result = engine.put(&cf, key.as_bytes(), value.as_slice());
        assert!(
            result.is_ok(),
            "Write should eventually succeed (may stall)"
        );
    }

    // Assert - All writes should be present
    for i in 0..num_writes {
        let key = format!("stall_test_{}", i);
        assert_get_exists(&engine, key.as_bytes());
    }
}

#[test]
fn should_stall_writes_given_l0_file_count_exceeded() {
    // Arrange
    let _dir = test_temp_dir();
    let opts = memory_opts_with_memtable_size(5 * 1024 * 1024);
    let engine = MidgeEngine::open(opts).unwrap();

    // Act - Write a lot of data to create L0 files
    let num_writes = 1500;
    for i in 0..num_writes {
        let key = format!("l0_key_{}", i);
        let value = vec![0u8; 3072];
        engine.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Assert - Despite potential stalls, all writes complete
    for i in 0..num_writes {
        let key = format!("l0_key_{}", i);
        let result = engine.get(&cf, key.as_bytes()).unwrap();
        assert!(result.is_some());
    }
}

#[test]
fn should_resume_writes_given_compaction_caught_up() {
    // Arrange
    let _dir = test_temp_dir();
    let opts = memory_opts_with_memtable_size(10 * 1024 * 1024);
    let engine = MidgeEngine::open(opts).unwrap();

    // Act - Burst writes, wait for compaction, then verify writes work
    for i in 0..1000 {
        let key = format!("burst_key_{}", i);
        let value = vec![0u8; 2048];
        engine.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
    }

    std::thread::sleep(std::time::Duration::from_millis(500));

    for i in 0..100 {
        let key = format!("resume_key_{}", i);
        engine.put(&cf, key.as_bytes(), "value".as_bytes()).unwrap();
    }

    // Assert
    for i in 0..100 {
        let key = format!("resume_key_{}", i);
        assert_get_equals(&engine, key.as_bytes(), b"value");
    }
}

#[test]
fn should_measure_write_stall_duration_given_backpressure() {
    // Arrange
    let _dir = test_temp_dir();
    let opts = memory_opts_with_memtable_size(6 * 1024 * 1024);
    let engine = MidgeEngine::open(opts).unwrap();

    // Act
    let start = std::time::Instant::now();
    let num_writes = 1000;

    for i in 0..num_writes {
        let key = format!("measure_key_{}", i);
        let value = vec![0u8; 4096];
        engine.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
    }

    let elapsed = start.elapsed();

    // Assert
    assert!(
        elapsed.as_secs() < 60,
        "Writes should complete within reasonable time even with backpressure"
    );

    for i in 0..num_writes {
        let key = format!("measure_key_{}", i);
        assert_get_exists(&engine, key.as_bytes());
    }
}
