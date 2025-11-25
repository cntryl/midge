// WAL Concurrency
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
fn should_serialize_wal_writes_given_concurrent_put_operations() {
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());
    let engine = Arc::new(MidgeEngine::open(opts).unwrap());
    let num_threads = 20;
    let writes_per_thread = 50;

    let cf = engine.default_column_family();

    // Act
    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let engine = Arc::clone(&engine);
            let cf = cf.clone();
            thread::spawn(move || {
                for i in 0..writes_per_thread {
                    let key = format!("wal_{}_{}", thread_id, i);
                    engine.put(&cf, key.as_bytes(), b"value").unwrap();
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    drop(engine);

    // Assert - Reopen and verify all writes persisted
    let engine = MidgeEngine::open(durability_opts(dir.path().to_path_buf())).unwrap();
    for thread_id in 0..num_threads {
        for i in 0..writes_per_thread {
            let key = format!("wal_{}_{}", thread_id, i);
            assert_get_equals(&engine, key.as_bytes(), b"value");
        }
    }
}

#[test]
fn should_maintain_wal_order_given_concurrent_batches() {
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());
    let engine = Arc::new(MidgeEngine::open(opts).unwrap());
    let cf = engine.default_column_family();
    let num_batches = 10;
    let batch_size = 20;

    // Act
    let handles: Vec<_> = (0..num_batches)
        .map(|batch_id| {
            let engine = Arc::clone(&engine);
            let cf = cf.clone();
            thread::spawn(move || {
                for i in 0..batch_size {
                    let key = format!("batch_{}_item_{}", batch_id, i);
                    let value = format!("batch{}", batch_id);
                    engine.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    drop(engine);

    // Assert - Verify after restart
    let engine = MidgeEngine::open(durability_opts(dir.path().to_path_buf())).unwrap();
    for batch_id in 0..num_batches {
        for i in 0..batch_size {
            let key = format!("batch_{}_item_{}", batch_id, i);
            let expected = format!("batch{}", batch_id);
            assert_get_equals(&engine, key.as_bytes(), expected.as_bytes());
        }
    }
}

#[test]
fn should_handle_wal_rotation_during_concurrent_writes() {
    // Arrange
    let dir = test_temp_dir();
    let db_path = dir.path().to_path_buf();
    let opts = durability_opts(db_path.clone());
    let engine = Arc::new(MidgeEngine::open(opts).unwrap());
    let cf = engine.default_column_family();
    let num_writers = 15;
    let writes_per_writer = 100;

    // Act - Write enough to trigger WAL rotation
    let handles: Vec<_> = (0..num_writers)
        .map(|writer_id| {
            let engine = Arc::clone(&engine);
            let cf = cf.clone();
            thread::spawn(move || {
                for i in 0..writes_per_writer {
                    let key = format!("rotate_{}_{}", writer_id, i);
                    let value = vec![0u8; 1024]; // 1KB per write
                    engine.put(&cf, key.as_bytes(), value.as_slice()).unwrap();
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    drop(engine);

    // Assert
    let engine = MidgeEngine::open(durability_opts(db_path.clone())).unwrap();
    for writer_id in 0..num_writers {
        for i in 0..writes_per_writer {
            let key = format!("rotate_{}_{}", writer_id, i);
            let result = engine.get(&cf, key.as_bytes()).unwrap();
            assert!(result.is_some());
            assert_eq!(result.unwrap().len(), 1024);
        }
    }
}
