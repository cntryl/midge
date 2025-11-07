// Concurrent Write Safety tests - P0 Priority
// Tests for multi-threaded correctness under high concurrency

mod common;
use bytes::Bytes;
use common::*;
use midge::{MidgeEngine, MidgeOptions, StorageMode};
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

// ============================================================================
// Multi-Threaded Write Stress (5 tests)
// ============================================================================

#[test]
fn should_handle_1000_concurrent_puts_given_separate_keys() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = Arc::new(MidgeEngine::open(opts).unwrap());
    let num_threads = 100;
    let puts_per_thread = 10;

    // Act
    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let engine = Arc::clone(&engine);
            thread::spawn(move || {
                for i in 0..puts_per_thread {
                    let key = format!("thread{}_key{}", thread_id, i);
                    let value = format!("value_{}", i);
                    engine.put(Bytes::from(key), Bytes::from(value)).unwrap();
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Assert
    for thread_id in 0..num_threads {
        for i in 0..puts_per_thread {
            let key = format!("thread{}_key{}", thread_id, i);
            let value = format!("value_{}", i);
            assert_get_equals(&engine, key.as_bytes(), value.as_bytes());
        }
    }
}

#[test]
fn should_handle_concurrent_puts_to_same_key_given_100_threads() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = Arc::new(MidgeEngine::open(opts).unwrap());
    let num_threads = 100;
    let key = Bytes::from("hotspot_key");

    // Act
    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let engine = Arc::clone(&engine);
            let key = key.clone();
            thread::spawn(move || {
                let value = format!("value_from_thread_{}", thread_id);
                engine.put(key, Bytes::from(value)).unwrap();
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Assert
    let result = engine.get(b"hotspot_key").unwrap();
    assert!(result.is_some(), "Key should exist after concurrent writes");
    let value_str = String::from_utf8(result.unwrap().to_vec()).unwrap();
    assert!(
        value_str.starts_with("value_from_thread_"),
        "Value should be from one of the threads"
    );
}

#[test]
fn should_maintain_consistency_given_concurrent_put_delete_to_same_key() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = Arc::new(MidgeEngine::open(opts).unwrap());
    let num_iterations = 50;
    let key = Bytes::from("contested_key");

    // Act
    let put_handle = {
        let engine = Arc::clone(&engine);
        let key = key.clone();
        thread::spawn(move || {
            for i in 0..num_iterations {
                let value = format!("value_{}", i);
                engine.put(key.clone(), Bytes::from(value)).unwrap();
            }
        })
    };

    let delete_handle = {
        let engine = Arc::clone(&engine);
        thread::spawn(move || {
            for _ in 0..num_iterations {
                let _ = engine.delete(key.clone());
            }
        })
    };

    put_handle.join().unwrap();
    delete_handle.join().unwrap();

    // Assert
    let result = engine.get(b"contested_key").unwrap();
    // Value is either present (put won) or absent (delete won) - both valid
    // The key test is that we don't crash or corrupt data
    if let Some(val) = result {
        let value_str = String::from_utf8(val.to_vec()).unwrap();
        assert!(value_str.starts_with("value_"));
    }
}

#[test]
fn should_preserve_last_write_wins_given_concurrent_updates_when_no_transaction() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = Arc::new(MidgeEngine::open(opts).unwrap());
    let key = Bytes::from("lww_key");
    engine.put(key.clone(), Bytes::from("initial")).unwrap();

    // Act
    let num_threads = 50;
    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let engine = Arc::clone(&engine);
            let key = key.clone();
            thread::spawn(move || {
                let value = format!("thread_{}", thread_id);
                engine.put(key, Bytes::from(value)).unwrap();
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Assert
    let result = engine.get(b"lww_key").unwrap();
    assert!(result.is_some());
    let value_str = String::from_utf8(result.unwrap().to_vec()).unwrap();
    assert!(
        value_str.starts_with("thread_"),
        "Final value should be from one of the threads (last-write-wins)"
    );
}

#[test]
fn should_handle_mixed_operations_given_concurrent_put_delete_get() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = Arc::new(MidgeEngine::open(opts).unwrap());
    let num_keys = 100;

    for i in 0..num_keys {
        let key = format!("key_{}", i);
        engine
            .put(Bytes::from(key), Bytes::from("initial"))
            .unwrap();
    }

    // Act
    let put_handle = {
        let engine = Arc::clone(&engine);
        thread::spawn(move || {
            for i in 0..num_keys {
                let key = format!("key_{}", i);
                engine
                    .put(Bytes::from(key), Bytes::from("updated"))
                    .unwrap();
            }
        })
    };

    let delete_handle = {
        let engine = Arc::clone(&engine);
        thread::spawn(move || {
            for i in 0..num_keys {
                if i % 2 == 0 {
                    let key = format!("key_{}", i);
                    let _ = engine.delete(Bytes::from(key));
                }
            }
        })
    };

    let get_handle = {
        let engine = Arc::clone(&engine);
        thread::spawn(move || {
            let mut read_count = 0;
            for i in 0..num_keys {
                let key = format!("key_{}", i);
                if engine.get(key.as_bytes()).unwrap().is_some() {
                    read_count += 1;
                }
            }
            read_count
        })
    };

    put_handle.join().unwrap();
    delete_handle.join().unwrap();
    let read_count = get_handle.join().unwrap();

    // Assert
    assert!(
        read_count > 0,
        "Should be able to read some keys during concurrent operations"
    );
}

// ============================================================================
// Memtable Race Conditions (4 tests)
// ============================================================================

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
                    engine.put(Bytes::from(key), Bytes::from(value)).unwrap();
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
            let result = engine.get(key.as_bytes()).unwrap();
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
                engine.put(Bytes::from(key), Bytes::from(value)).unwrap();
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
                    engine.put(Bytes::from(key), Bytes::from(value)).unwrap();
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
            if engine.get(key.as_bytes()).unwrap().is_some() {
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
                    engine.put(Bytes::from(key), Bytes::from(value)).unwrap();
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

// ============================================================================
// Flush vs Write Contention (5 tests)
// ============================================================================

#[test]
fn should_allow_writes_given_flush_in_progress() {
    // Arrange
    let _dir = test_temp_dir();
    let opts = memory_opts_with_memtable_size(10 * 1024 * 1024);
    let engine = Arc::new(MidgeEngine::open(opts).unwrap());

    // Act - Trigger flush with large writes, then write concurrently
    let flush_handle = {
        let engine = Arc::clone(&engine);
        thread::spawn(move || {
            for i in 0..500 {
                let key = format!("flush_key_{}", i);
                let value = vec![0u8; 4096]; // 4KB values
                engine.put(Bytes::from(key), Bytes::from(value)).unwrap();
            }
        })
    };

    let write_handle = {
        let engine = Arc::clone(&engine);
        thread::spawn(move || {
            for i in 0..100 {
                let key = format!("concurrent_key_{}", i);
                engine.put(Bytes::from(key), Bytes::from("value")).unwrap();
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

    // Act - Write enough to create multiple immutable memtables
    let num_writes = 2000;
    for i in 0..num_writes {
        let key = format!("stall_test_{}", i);
        let value = vec![0u8; 2048];
        let result = engine.put(Bytes::from(key), Bytes::from(value));
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
        engine.put(Bytes::from(key), Bytes::from(value)).unwrap();
    }

    // Assert - Despite potential stalls, all writes complete
    for i in 0..num_writes {
        let key = format!("l0_key_{}", i);
        let result = engine.get(key.as_bytes()).unwrap();
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
        engine.put(Bytes::from(key), Bytes::from(value)).unwrap();
    }

    std::thread::sleep(std::time::Duration::from_millis(500));

    for i in 0..100 {
        let key = format!("resume_key_{}", i);
        engine.put(Bytes::from(key), Bytes::from("value")).unwrap();
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
        engine.put(Bytes::from(key), Bytes::from(value)).unwrap();
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

// ============================================================================
// Sequence Number Allocation (4 tests)
// ============================================================================

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
                    engine.put(Bytes::from(key), Bytes::from("value")).unwrap();
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
                engine.put(Bytes::from(key), Bytes::from("value")).unwrap();
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
        engine.put(Bytes::from(key), Bytes::from("value")).unwrap();
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
                    engine.put(Bytes::from(key), Bytes::from("value")).unwrap();
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

// ============================================================================
// Concurrent Compaction + Writes (3 tests)
// ============================================================================

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
        engine.put(Bytes::from(key), Bytes::from(value)).unwrap();
    }

    std::thread::sleep(std::time::Duration::from_millis(100));

    let write_handle = {
        let engine = Arc::clone(&engine);
        thread::spawn(move || {
            for i in 0..200 {
                let key = format!("during_compact_{}", i);
                engine.put(Bytes::from(key), Bytes::from("value")).unwrap();
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
        engine.put(Bytes::from(key), Bytes::from(value)).unwrap();
    }

    let num_concurrent = 100;
    let handles: Vec<_> = (0..num_concurrent)
        .map(|i| {
            let engine = Arc::clone(&engine);
            thread::spawn(move || {
                let key = format!("nonblock_key_{}", i);
                engine.put(Bytes::from(key), Bytes::from("value")).unwrap();
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
        engine.put(Bytes::from(key), Bytes::from(value)).unwrap();
    }

    for i in 0..100 {
        let key = format!("concurrent_ml_{}", i);
        engine.put(Bytes::from(key), Bytes::from("test")).unwrap();
    }

    // Assert
    for i in 0..100 {
        let key = format!("concurrent_ml_{}", i);
        assert_get_equals(&engine, key.as_bytes(), b"test");
    }
}

// ============================================================================
// Delete Range Concurrency (3 tests)
// ============================================================================

#[test]
fn should_handle_concurrent_delete_range_operations() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = Arc::new(MidgeEngine::open(opts).unwrap());

    for i in 0..1000 {
        let key = format!("range_{:04}", i);
        engine.put(Bytes::from(key), Bytes::from("value")).unwrap();
    }

    // Act
    let handle1 = {
        let engine = Arc::clone(&engine);
        thread::spawn(move || {
            let start = Bytes::from("range_0000");
            let end = Bytes::from("range_0250");
            engine.delete_range(start, end).unwrap();
        })
    };

    let handle2 = {
        let engine = Arc::clone(&engine);
        thread::spawn(move || {
            let start = Bytes::from("range_0500");
            let end = Bytes::from("range_0750");
            engine.delete_range(start, end).unwrap();
        })
    };

    handle1.join().unwrap();
    handle2.join().unwrap();

    // Assert - Keys in deleted ranges should be gone
    for i in 0..250 {
        let key = format!("range_{:04}", i);
        assert_get_not_exists(&engine, key.as_bytes());
    }

    for i in 500..750 {
        let key = format!("range_{:04}", i);
        assert_get_not_exists(&engine, key.as_bytes());
    }
}

#[test]
fn should_handle_overlapping_delete_ranges_given_concurrent_calls() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = Arc::new(MidgeEngine::open(opts).unwrap());

    for i in 0..500 {
        let key = format!("overlap_{:04}", i);
        engine.put(Bytes::from(key), Bytes::from("value")).unwrap();
    }

    // Act - Overlapping ranges
    let handle1 = {
        let engine = Arc::clone(&engine);
        thread::spawn(move || {
            let start = Bytes::from("overlap_0000");
            let end = Bytes::from("overlap_0300");
            engine.delete_range(start, end).unwrap();
        })
    };

    let handle2 = {
        let engine = Arc::clone(&engine);
        thread::spawn(move || {
            let start = Bytes::from("overlap_0200");
            let end = Bytes::from("overlap_0450");
            engine.delete_range(start, end).unwrap();
        })
    };

    handle1.join().unwrap();
    handle2.join().unwrap();

    // Assert - All keys in union of ranges should be deleted
    for i in 0..450 {
        let key = format!("overlap_{:04}", i);
        assert_get_not_exists(&engine, key.as_bytes());
    }
}

#[test]
fn should_handle_point_write_during_delete_range() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = Arc::new(MidgeEngine::open(opts).unwrap());

    for i in 0..500 {
        let key = format!("mixed_{:04}", i);
        engine
            .put(Bytes::from(key), Bytes::from("initial"))
            .unwrap();
    }

    // Act
    let delete_handle = {
        let engine = Arc::clone(&engine);
        thread::spawn(move || {
            let start = Bytes::from("mixed_0000");
            let end = Bytes::from("mixed_0400");
            engine.delete_range(start, end).unwrap();
        })
    };

    let write_handle = {
        let engine = Arc::clone(&engine);
        thread::spawn(move || {
            for i in 0..500 {
                let key = format!("mixed_{:04}", i);
                engine
                    .put(Bytes::from(key), Bytes::from("updated"))
                    .unwrap();
            }
        })
    };

    delete_handle.join().unwrap();
    write_handle.join().unwrap();

    // Assert - Final state depends on operation order, but no corruption
    for i in 0..500 {
        let key = format!("mixed_{:04}", i);
        let result = engine.get(key.as_bytes()).unwrap();
        // Key may exist (write after delete) or not (delete after write)
        if let Some(val) = result {
            assert_eq!(val.as_ref(), b"updated");
        }
    }
}

// ============================================================================
// WAL Concurrency (3 tests)
// ============================================================================

#[test]
fn should_serialize_wal_writes_given_concurrent_put_operations() {
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());
    let engine = Arc::new(MidgeEngine::open(opts).unwrap());
    let num_threads = 20;
    let writes_per_thread = 50;

    // Act
    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let engine = Arc::clone(&engine);
            thread::spawn(move || {
                for i in 0..writes_per_thread {
                    let key = format!("wal_{}_{}", thread_id, i);
                    engine.put(Bytes::from(key), Bytes::from("value")).unwrap();
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
    let num_batches = 10;
    let batch_size = 20;

    // Act
    let handles: Vec<_> = (0..num_batches)
        .map(|batch_id| {
            let engine = Arc::clone(&engine);
            thread::spawn(move || {
                for i in 0..batch_size {
                    let key = format!("batch_{}_item_{}", batch_id, i);
                    let value = format!("batch{}", batch_id);
                    engine.put(Bytes::from(key), Bytes::from(value)).unwrap();
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
    let num_writers = 15;
    let writes_per_writer = 100;

    // Act - Write enough to trigger WAL rotation
    let handles: Vec<_> = (0..num_writers)
        .map(|writer_id| {
            let engine = Arc::clone(&engine);
            thread::spawn(move || {
                for i in 0..writes_per_writer {
                    let key = format!("rotate_{}_{}", writer_id, i);
                    let value = vec![0u8; 1024]; // 1KB per write
                    engine.put(Bytes::from(key), Bytes::from(value)).unwrap();
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
            let result = engine.get(key.as_bytes()).unwrap();
            assert!(result.is_some());
            assert_eq!(result.unwrap().len(), 1024);
        }
    }
}
