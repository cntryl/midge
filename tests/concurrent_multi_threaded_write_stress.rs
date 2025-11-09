// Multi-Threaded Write Stress
// Extracted from concurrent_writes.rs

// Concurrent Write Safety tests - P0 Priority
// Tests for multi-threaded correctness under high concurrency

mod common;
use bytes::Bytes;
use common::*;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
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
                engine.put(&cf, key.as_ref(), Bytes::from(value).as_ref()).unwrap();
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Assert
    let result = engine.get(&cf, b"hotspot_key").unwrap();
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
                engine.put(&cf, key.as_ref(), Bytes::from(value).as_ref()).unwrap();
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

    // Assert
    let result = engine.get(&cf, b"contested_key").unwrap();
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
                engine.put(&cf, key.as_ref(), Bytes::from(value).as_ref()).unwrap();
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Assert
    let result = engine.get(&cf, b"lww_key").unwrap();
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
    let cf = engine.default_column_family();
    let num_keys = 100;

    for i in 0..num_keys {
        let key = format!("key_{}", i);
        engine
            .put(&cf, key.as_bytes(), b"initial")
            .unwrap();
    }

    // Act
    let put_handle = {
        let engine = Arc::clone(&engine);
        let cf = cf.clone();
        thread::spawn(move || {
            for i in 0..num_keys {
                let key = format!("key_{}", i);
                engine
                    .put(&cf, key.as_bytes(), b"updated")
                    .unwrap();
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
        "Should be able to read some keys during concurrent operations"
    );
}
