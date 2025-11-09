// Delete Range Concurrency
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
        engine.put(&cf, key.as_bytes(), "value".as_bytes()).unwrap();
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
        engine.put(&cf, key.as_bytes(), "value".as_bytes()).unwrap();
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
        let result = engine.get(&cf, key.as_bytes()).unwrap();
        // Key may exist (write after delete) or not (delete after write)
        if let Some(val) = result {
            assert_eq!(val.as_ref(), b"updated");
        }
    }
}
