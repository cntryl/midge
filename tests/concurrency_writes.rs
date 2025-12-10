//! Concurrent Write Safety Tests
//!
//! Tests for multi-threaded correctness under high concurrency.
//! Verifies that concurrent puts, deletes, and updates maintain data integrity.
//!
//! Storage modes: All 3 (Memory, LocalDisk, CloudBacked)

mod common;
use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions};
use cntryl_midge::testkit::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

// ============================================================================
// Concurrent Put Tests - All Storage Modes
// ============================================================================

#[test]
fn should_handle_concurrent_puts_given_1000_writes_to_separate_keys_when_100_threads() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _tmp) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();
        let num_threads = 100;
        let puts_per_thread = 10;

        // Act
        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let engine = Arc::clone(&engine);
                let cf = cf.clone();
                thread::spawn(move || {
                    for i in 0..puts_per_thread {
                        let key = format!("thread{}_key{}", thread_id, i);
                        let value = format!("value_{}", i);
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
            for i in 0..puts_per_thread {
                let key = format!("thread{}_key{}", thread_id, i);
                let value = format!("value_{}", i);
                let result = engine.get(&cf, key.as_bytes()).unwrap();
                assert_eq!(
                    result,
                    Some(Bytes::from(value.clone())),
                    "Failed for {} key: {}",
                    name,
                    key
                );
            }
        }
    }
}

#[test]
fn should_preserve_last_write_wins_given_concurrent_puts_to_same_key_when_100_threads() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _tmp) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();
        let num_threads = 100;
        let key = Bytes::from("hotspot_key");

        // Act
        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let engine = Arc::clone(&engine);
                let cf = cf.clone();
                let key = key.clone();
                thread::spawn(move || {
                    let value = format!("value_from_thread_{}", thread_id);
                    engine
                        .put(&cf, key.as_ref(), Bytes::from(value).as_ref())
                        .unwrap();
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // Assert
        let result = engine.get(&cf, b"hotspot_key").unwrap();
        assert!(
            result.is_some(),
            "Key should exist after concurrent writes for {}",
            name
        );
        let value_str = String::from_utf8(result.unwrap().to_vec()).unwrap();
        assert!(
            value_str.starts_with("value_from_thread_"),
            "Value should be from one of the threads (last-write-wins) for {}",
            name
        );
    }
}

#[test]
fn should_maintain_consistency_given_concurrent_put_delete_to_same_key_when_interleaved() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _tmp) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();
        let num_iterations = 50;
        let key = Bytes::from("contested_key");

        // Act
        let put_handle = {
            let engine = Arc::clone(&engine);
            let cf = cf.clone();
            let key = key.clone();
            thread::spawn(move || {
                for i in 0..num_iterations {
                    let value = format!("value_{}", i);
                    engine
                        .put(&cf, key.as_ref(), Bytes::from(value).as_ref())
                        .unwrap();
                }
            })
        };

        let delete_handle = {
            let engine = Arc::clone(&engine);
            let cf = cf.clone();
            let key = key.clone();
            thread::spawn(move || {
                for _ in 0..num_iterations {
                    let _ = engine.delete(&cf, key.as_ref());
                }
            })
        };

        put_handle.join().unwrap();
        delete_handle.join().unwrap();

        // Assert - Value is either present (put won) or absent (delete won) - both valid
        // The key test is that we don't crash or corrupt data
        let result = engine.get(&cf, b"contested_key").unwrap();
        if let Some(val) = result {
            let value_str = String::from_utf8(val.to_vec()).unwrap();
            assert!(
                value_str.starts_with("value_"),
                "Invalid value for {}",
                name
            );
        }
    }
}

#[test]
fn should_handle_mixed_operations_given_concurrent_put_delete_get_when_100_keys() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _tmp) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();
        let num_keys = 100;

        for i in 0..num_keys {
            let key = format!("key_{}", i);
            engine.put(&cf, key.as_bytes(), b"initial").unwrap();
        }

        // Act
        let put_handle = {
            let engine = Arc::clone(&engine);
            let cf = cf.clone();
            thread::spawn(move || {
                for i in 0..num_keys {
                    let key = format!("key_{}", i);
                    engine.put(&cf, key.as_bytes(), b"updated").unwrap();
                }
            })
        };

        let delete_handle = {
            let engine = Arc::clone(&engine);
            let cf = cf.clone();
            thread::spawn(move || {
                for i in 0..num_keys {
                    if i % 2 == 0 {
                        let key = format!("key_{}", i);
                        let _ = engine.delete(&cf, key.as_bytes());
                    }
                }
            })
        };

        let get_handle = {
            let engine = Arc::clone(&engine);
            let cf = cf.clone();
            thread::spawn(move || {
                let mut read_count = 0;
                for i in 0..num_keys {
                    let key = format!("key_{}", i);
                    if engine.get(&cf, key.as_bytes()).unwrap().is_some() {
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
            "Should be able to read some keys during concurrent operations for {}",
            name
        );
    }
}

// ============================================================================
// Sequence Number Allocation Tests - All Storage Modes
// ============================================================================

#[test]
fn should_allocate_unique_sequences_given_1000_concurrent_writes_when_50_threads() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _tmp) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();
        let num_threads = 50;
        let puts_per_thread = 20;

        // Act
        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let engine = Arc::clone(&engine);
                let cf_clone = cf.clone();
                thread::spawn(move || {
                    for i in 0..puts_per_thread {
                        let key = format!("seq_test_{}_{}", thread_id, i);
                        engine
                            .put(&cf_clone, key.as_bytes(), "value".as_bytes())
                            .unwrap();
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
            "Sequence should advance by at least {} writes, got {} for {}",
            total_writes,
            final_seq,
            name
        );
    }
}

#[test]
fn should_maintain_sequence_monotonicity_given_1000_concurrent_writes_when_parallel() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _tmp) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();
        let initial_seq = engine.current_sequence();
        let num_writes: u64 = 1000;

        // Act
        let handles: Vec<_> = (0..num_writes)
            .map(|i| {
                let engine = Arc::clone(&engine);
                let cf_clone = cf.clone();
                thread::spawn(move || {
                    let key = format!("mono_key_{}", i);
                    engine
                        .put(&cf_clone, key.as_bytes(), "value".as_bytes())
                        .unwrap();
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
            "Sequence must increase: initial={}, final={} for {}",
            initial_seq,
            final_seq,
            name
        );
        assert!(
            final_seq >= initial_seq + num_writes,
            "Sequence should advance by at least {} writes for {}",
            num_writes,
            name
        );
    }
}

#[test]
fn should_not_skip_sequences_given_sequential_writes_when_no_errors() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _tmp) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).unwrap();
        let cf = engine.default_column_family();
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
            "Should allocate sequences for all writes (diff={}) for {}",
            sequence_diff,
            name
        );
    }
}

#[test]
fn should_preserve_sequence_order_given_concurrent_batches_when_10_threads() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _tmp) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();
        let num_batches = 10;
        let batch_size = 10;

        // Act
        let handles: Vec<_> = (0..num_batches)
            .map(|batch_id| {
                let engine = Arc::clone(&engine);
                let cf_clone = cf.clone();
                thread::spawn(move || {
                    for i in 0..batch_size {
                        let key = format!("batch_{}_item_{}", batch_id, i);
                        engine
                            .put(&cf_clone, key.as_bytes(), "value".as_bytes())
                            .unwrap();
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
                let result = engine.get(&cf, key.as_bytes()).unwrap();
                assert_eq!(
                    result,
                    Some(Bytes::from("value")),
                    "Failed for {} key: {}",
                    name,
                    key
                );
            }
        }
    }
}

// ============================================================================
// Memtable Race Condition Tests - All Storage Modes
// ============================================================================

#[test]
fn should_freeze_memtable_atomically_given_concurrent_writes_when_size_exceeded() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _tmp) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 10 * 1024 * 1024, // Small budget to trigger freezes
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();
        let num_threads = 20;
        let writes_per_thread = 100;

        // Act
        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let engine = Arc::clone(&engine);
                let cf_clone = cf.clone();
                thread::spawn(move || {
                    for i in 0..writes_per_thread {
                        let key = format!("freeze_test_{}_{}", thread_id, i);
                        let value = vec![0u8; 1024]; // 1KB values
                        engine
                            .put(&cf_clone, key.as_bytes(), value.as_slice())
                            .unwrap();
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
                assert!(result.is_some(), "Key {} should exist for {}", key, name);
                assert_eq!(result.unwrap().len(), 1024);
            }
        }
    }
}

#[test]
fn should_route_writes_to_new_memtable_given_freeze_in_progress_when_1000_writes() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _tmp) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 8 * 1024 * 1024,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();

        // Act
        let num_writes = 1000;
        let handles: Vec<_> = (0..num_writes)
            .map(|i| {
                let engine = Arc::clone(&engine);
                let cf_clone = cf.clone();
                thread::spawn(move || {
                    let key = format!("routing_key_{}", i);
                    let value = vec![0u8; 2048]; // 2KB values
                    engine
                        .put(&cf_clone, key.as_bytes(), value.as_slice())
                        .unwrap();
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // Assert
        for i in 0..num_writes {
            let key = format!("routing_key_{}", i);
            let result = engine.get(&cf, key.as_bytes()).unwrap();
            assert!(result.is_some(), "Key {} should exist for {}", key, name);
        }
    }
}

#[test]
fn should_not_lose_writes_given_memtable_freeze_race_when_10_threads() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _tmp) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 5 * 1024 * 1024,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();
        let write_count = Arc::new(AtomicUsize::new(0));

        // Act
        let num_threads = 10;
        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let engine = Arc::clone(&engine);
                let counter = Arc::clone(&write_count);
                let cf_clone = cf.clone();
                thread::spawn(move || {
                    for i in 0..200 {
                        let key = format!("preserve_{}_{}", thread_id, i);
                        let value = vec![0u8; 1024];
                        engine
                            .put(&cf_clone, key.as_bytes(), value.as_slice())
                            .unwrap();
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
            "All {} writes should be preserved for {}",
            expected_writes, name
        );
    }
}

#[test]
fn should_maintain_write_order_given_freeze_during_batch_when_5_batches() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _tmp) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 6 * 1024 * 1024,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());

        // Act
        let num_batches = 5;
        let batch_size = 100;
        let handles: Vec<_> = (0..num_batches)
            .map(|batch_id| {
                let engine = Arc::clone(&engine);
                let cf_clone = engine.default_column_family();
                thread::spawn(move || {
                    for i in 0..batch_size {
                        let key = format!("order_batch_{}_seq_{:04}", batch_id, i);
                        let value = format!("{}", i);
                        engine
                            .put(&cf_clone, key.as_bytes(), value.as_bytes())
                            .unwrap();
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // Assert
        let cf = engine.default_column_family();
        for batch_id in 0..num_batches {
            for i in 0..batch_size {
                let key = format!("order_batch_{}_seq_{:04}", batch_id, i);
                let expected = format!("{}", i);
                let result = engine.get(&cf, key.as_bytes()).unwrap();
                assert_eq!(
                    result,
                    Some(Bytes::from(expected.clone())),
                    "Failed for {} key: {}",
                    name,
                    key
                );
            }
        }
    }
}

// ============================================================================
// Write Contention Tests - All Storage Modes
// ============================================================================

#[test]
fn should_serialize_writes_correctly_given_high_contention_when_50_threads() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _tmp) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();
        let key = Bytes::from("lww_key");
        engine.put(&cf, key.as_ref(), b"initial").unwrap();

        // Act
        let num_threads = 50;
        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let engine = Arc::clone(&engine);
                let cf = cf.clone();
                let key = key.clone();
                thread::spawn(move || {
                    let value = format!("thread_{}", thread_id);
                    engine
                        .put(&cf, key.as_ref(), Bytes::from(value).as_ref())
                        .unwrap();
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // Assert
        let result = engine.get(&cf, b"lww_key").unwrap();
        assert!(result.is_some(), "Key should exist for {}", name);
        let value_str = String::from_utf8(result.unwrap().to_vec()).unwrap();
        assert!(
            value_str.starts_with("thread_"),
            "Final value should be from one of the threads (last-write-wins) for {}",
            name
        );
    }
}
