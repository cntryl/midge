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
    let cf = engine.default_column_family();

    for i in 0..1000 {
        let key = format!("range_{:04}", i);
        engine.put(&cf, key.as_bytes(), "value".as_bytes()).unwrap();
    }

    // Act
    let handle1 = {
        let engine = Arc::clone(&engine);
        let cf = cf.clone();
        thread::spawn(move || {
            engine.delete_range(&cf, b"range_0000", b"range_0250").unwrap();
        })
    };

    let handle2 = {
        let engine = Arc::clone(&engine);
        let cf = cf.clone();
        thread::spawn(move || {
            engine.delete_range(&cf, b"range_0500", b"range_0750").unwrap();
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
    let cf = engine.default_column_family();

    for i in 0..500 {
        let key = format!("overlap_{:04}", i);
        engine.put(&cf, key.as_bytes(), "value".as_bytes()).unwrap();
    }

    // Act - Overlapping ranges
    let handle1 = {
        let engine = Arc::clone(&engine);
        let cf = cf.clone();
        thread::spawn(move || {
            engine.delete_range(&cf, b"overlap_0000", b"overlap_0300").unwrap();
        })
    };

    let handle2 = {
        let engine = Arc::clone(&engine);
        let cf = cf.clone();
        thread::spawn(move || {
            engine.delete_range(&cf, b"overlap_0200", b"overlap_0450").unwrap();
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
    let cf = engine.default_column_family();

    for i in 0..500 {
        let key = format!("mixed_{:04}", i);
        engine
            .put(&cf, key.as_bytes(), b"initial")
            .unwrap();
    }

    // Act
    let delete_handle = {
        let engine = Arc::clone(&engine);
        let cf = cf.clone();
        thread::spawn(move || {
            engine.delete_range(&cf, b"mixed_0000", b"mixed_0400").unwrap();
        })
    };

    let write_handle = {
        let engine = Arc::clone(&engine);
        let cf = cf.clone();
        thread::spawn(move || {
            for i in 0..500 {
                let key = format!("mixed_{:04}", i);
                engine
                    .put(&cf, key.as_bytes(), b"updated")
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
