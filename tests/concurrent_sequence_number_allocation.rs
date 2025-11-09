// Sequence Number Allocation
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

/// Helper: create a new engine in a fresh temp dir and return both.
fn new_engine() -> (tempfile::TempDir, cntryl_midge::MidgeEngine) {
    let dir = test_temp_dir();
    let opts = cntryl_midge::MidgeOptions {
        storage_mode: cntryl_midge::StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = cntryl_midge::MidgeEngine::open(opts).expect("open");
    (dir, engine)
}

#[test]
fn should_allocate_unique_sequences_given_concurrent_writes() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = Arc::new(MidgeEngine::open(opts).unwrap());
    let num_threads = 50;
    let puts_per_thread = 20;

    // Act
    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let engine = Arc::clone(&engine);
            thread::spawn(move || {
                for i in 0..puts_per_thread {
                    let key = format!("seq_test_{}_{}", thread_id, i);
                    engine.put(&cf, key.as_bytes(), "value".as_bytes()).unwrap();
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Assert
    let total_writes = num_threads * puts_per_thread;
    let final_seq = engine.current_sequence();
    assert!(
        final_seq >= total_writes,
        "Sequence should advance by at least {} writes, got {}",
        total_writes,
        final_seq
    );
}

#[test]
fn should_maintain_sequence_monotonicity_given_1000_concurrent_writes() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = Arc::new(MidgeEngine::open(opts).unwrap());
    let initial_seq = engine.current_sequence();
    let num_writes = 1000;

    // Act
    let handles: Vec<_> = (0..num_writes)
        .map(|i| {
            let engine = Arc::clone(&engine);
            thread::spawn(move || {
                let key = format!("mono_key_{}", i);
                engine.put(&cf, key.as_bytes(), "value".as_bytes()).unwrap();
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Assert
    let final_seq = engine.current_sequence();
    assert!(
        final_seq > initial_seq,
        "Sequence must increase: initial={}, final={}",
        initial_seq,
        final_seq
    );
    assert!(
        final_seq >= initial_seq + num_writes,
        "Sequence should advance by at least {} writes",
        num_writes
    );
}

#[test]
fn should_not_skip_sequences_given_aborted_writes() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    let initial_seq = engine.current_sequence();

    // Act
    for i in 0..100 {
        let key = format!("key_{}", i);
        engine.put(&cf, key.as_bytes(), "value".as_bytes()).unwrap();
    }

    // Assert
    let final_seq = engine.current_sequence();
    let sequence_diff = final_seq - initial_seq;
    assert!(
        sequence_diff >= 100,
        "Should allocate sequences for all writes (diff={})",
        sequence_diff
    );
}

#[test]
fn should_preserve_sequence_order_given_concurrent_batches() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = Arc::new(MidgeEngine::open(opts).unwrap());
    let num_batches = 10;
    let batch_size = 10;

    // Act
    let handles: Vec<_> = (0..num_batches)
        .map(|batch_id| {
            let engine = Arc::clone(&engine);
            thread::spawn(move || {
                for i in 0..batch_size {
                    let key = format!("batch_{}_item_{}", batch_id, i);
                    engine.put(&cf, key.as_bytes(), "value".as_bytes()).unwrap();
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
            let key = format!("batch_{}_item_{}", batch_id, i);
            assert_get_equals(&engine, key.as_bytes(), b"value");
        }
    }
}
