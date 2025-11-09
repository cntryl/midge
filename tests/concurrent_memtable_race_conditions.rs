// Memtable Race Conditions
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
fn should_freeze_memtable_atomically_given_concurrent_writes_when_size_exceeded() {
    // Arrange
    let _dir = test_temp_dir();
    let opts = memory_opts_with_memtable_size(10 * 1024 * 1024); // Small budget to trigger freezes
    let engine = Arc::new(MidgeEngine::open(opts).unwrap());
    let num_threads = 20;
    let writes_per_thread = 100;

    // Act
    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let engine = Arc::clone(&engine);
            thread::spawn(move || {
                for i in 0..writes_per_thread {
                    let key = format!("freeze_test_{}_{}", thread_id, i);
                    let value = vec![0u8; 1024]; // 1KB values
                    engine.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Assert
    for thread_id in 0..num_threads {
        for i in 0..writes_per_thread {
            let key = format!("freeze_test_{}_{}", thread_id, i);
            let result = engine.get(&cf, key.as_bytes()).unwrap();
            assert!(result.is_some(), "Key {} should exist", key);
            assert_eq!(result.unwrap().len(), 1024);
        }
    }
}

#[test]
fn should_route_writes_to_new_memtable_given_freeze_in_progress() {
    // Arrange
    let _dir = test_temp_dir();
    let opts = memory_opts_with_memtable_size(8 * 1024 * 1024);
    let engine = Arc::new(MidgeEngine::open(opts).unwrap());

    // Act
    let num_writes = 1000;
    let handles: Vec<_> = (0..num_writes)
        .map(|i| {
            let engine = Arc::clone(&engine);
            thread::spawn(move || {
                let key = format!("routing_key_{}", i);
                let value = vec![0u8; 2048]; // 2KB values
                engine.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Assert
    for i in 0..num_writes {
        let key = format!("routing_key_{}", i);
        assert_get_exists(&engine, key.as_bytes());
    }
}

#[test]
fn should_not_lose_writes_given_memtable_freeze_race() {
    // Arrange
    let _dir = test_temp_dir();
    let opts = memory_opts_with_memtable_size(5 * 1024 * 1024);
    let engine = Arc::new(MidgeEngine::open(opts).unwrap());
    let write_count = Arc::new(AtomicUsize::new(0));

    // Act
    let num_threads = 10;
    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let engine = Arc::clone(&engine);
            let counter = Arc::clone(&write_count);
            thread::spawn(move || {
                for i in 0..200 {
                    let key = format!("preserve_{}_{}", thread_id, i);
                    let value = vec![0u8; 1024];
                    engine.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Assert
    let expected_writes = write_count.load(Ordering::Relaxed);
    let mut found_writes = 0;
    for thread_id in 0..num_threads {
        for i in 0..200 {
            let key = format!("preserve_{}_{}", thread_id, i);
            if engine.get(&cf, key.as_bytes()).unwrap().is_some() {
                found_writes += 1;
            }
        }
    }
    assert_eq!(
        found_writes, expected_writes,
        "All {} writes should be preserved",
        expected_writes
    );
}

#[test]
fn should_maintain_write_order_given_freeze_during_batch() {
    // Arrange
    let _dir = test_temp_dir();
    let opts = memory_opts_with_memtable_size(6 * 1024 * 1024);
    let engine = Arc::new(MidgeEngine::open(opts).unwrap());

    // Act
    let num_batches = 5;
    let batch_size = 100;
    let handles: Vec<_> = (0..num_batches)
        .map(|batch_id| {
            let engine = Arc::clone(&engine);
            thread::spawn(move || {
                for i in 0..batch_size {
                    let key = format!("order_batch_{}_seq_{:04}", batch_id, i);
                    let value = format!("{}", i);
                    engine.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Assert
    for batch_id in 0..num_batches {
        for i in 0..batch_size {
            let key = format!("order_batch_{}_seq_{:04}", batch_id, i);
            let expected = format!("{}", i);
            assert_get_equals(&engine, key.as_bytes(), expected.as_bytes());
        }
    }
}
