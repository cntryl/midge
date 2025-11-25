//! Concurrent Delete Range Tests
//!
//! Tests for concurrent delete range operations including non-overlapping ranges,
//! overlapping ranges, and interleaved point writes with delete ranges.
//!
//! Storage modes: All 3 (Memory, LocalDisk, CloudBacked)

mod common;
use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions};
use common::*;
use std::sync::Arc;
use std::thread;

// ============================================================================
// Concurrent Delete Range Tests - All Storage Modes
// ============================================================================

#[test]
fn should_handle_concurrent_delete_ranges_given_non_overlapping_ranges_when_2_threads() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _tmp) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
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
                engine
                    .delete_range(&cf, b"range_0000", b"range_0250")
                    .unwrap();
            })
        };

        let handle2 = {
            let engine = Arc::clone(&engine);
            let cf = cf.clone();
            thread::spawn(move || {
                engine
                    .delete_range(&cf, b"range_0500", b"range_0750")
                    .unwrap();
            })
        };

        handle1.join().unwrap();
        handle2.join().unwrap();

        // Assert - Keys in deleted ranges should be gone
        for i in 0..250 {
            let key = format!("range_{:04}", i);
            let result = engine.get(&cf, key.as_bytes()).unwrap();
            assert!(
                result.is_none(),
                "Key {} should be deleted for {}",
                key,
                name
            );
        }

        for i in 500..750 {
            let key = format!("range_{:04}", i);
            let result = engine.get(&cf, key.as_bytes()).unwrap();
            assert!(
                result.is_none(),
                "Key {} should be deleted for {}",
                key,
                name
            );
        }

        // Keys outside deleted ranges should still exist
        for i in 250..500 {
            let key = format!("range_{:04}", i);
            let result = engine.get(&cf, key.as_bytes()).unwrap();
            assert_eq!(
                result,
                Some(Bytes::from("value")),
                "Key {} should exist for {}",
                key,
                name
            );
        }

        for i in 750..1000 {
            let key = format!("range_{:04}", i);
            let result = engine.get(&cf, key.as_bytes()).unwrap();
            assert_eq!(
                result,
                Some(Bytes::from("value")),
                "Key {} should exist for {}",
                key,
                name
            );
        }
    }
}

#[test]
fn should_handle_overlapping_delete_ranges_given_concurrent_calls_when_2_threads() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _tmp) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
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
                engine
                    .delete_range(&cf, b"overlap_0000", b"overlap_0300")
                    .unwrap();
            })
        };

        let handle2 = {
            let engine = Arc::clone(&engine);
            let cf = cf.clone();
            thread::spawn(move || {
                engine
                    .delete_range(&cf, b"overlap_0200", b"overlap_0450")
                    .unwrap();
            })
        };

        handle1.join().unwrap();
        handle2.join().unwrap();

        // Assert - All keys in union of ranges should be deleted
        for i in 0..450 {
            let key = format!("overlap_{:04}", i);
            let result = engine.get(&cf, key.as_bytes()).unwrap();
            assert!(
                result.is_none(),
                "Key {} should be deleted for {}",
                key,
                name
            );
        }

        // Keys outside the union should still exist
        for i in 450..500 {
            let key = format!("overlap_{:04}", i);
            let result = engine.get(&cf, key.as_bytes()).unwrap();
            assert_eq!(
                result,
                Some(Bytes::from("value")),
                "Key {} should exist for {}",
                key,
                name
            );
        }
    }
}

#[test]
fn should_handle_point_write_during_delete_range_when_interleaved_operations() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _tmp) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();

        for i in 0..500 {
            let key = format!("mixed_{:04}", i);
            engine.put(&cf, key.as_bytes(), b"initial").unwrap();
        }

        // Act
        let delete_handle = {
            let engine = Arc::clone(&engine);
            let cf = cf.clone();
            thread::spawn(move || {
                engine
                    .delete_range(&cf, b"mixed_0000", b"mixed_0400")
                    .unwrap();
            })
        };

        let write_handle = {
            let engine = Arc::clone(&engine);
            let cf = cf.clone();
            thread::spawn(move || {
                for i in 0..500 {
                    let key = format!("mixed_{:04}", i);
                    engine.put(&cf, key.as_bytes(), b"updated").unwrap();
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
                assert_eq!(
                    val.as_ref(),
                    b"updated",
                    "If key {} exists, it should have updated value for {}",
                    key,
                    name
                );
            }
        }
    }
}

#[test]
fn should_handle_reads_during_delete_range_when_concurrent_get_operations() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _tmp) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();

        for i in 0..500 {
            let key = format!("read_test_{:04}", i);
            engine.put(&cf, key.as_bytes(), "value".as_bytes()).unwrap();
        }

        // Act
        let delete_handle = {
            let engine = Arc::clone(&engine);
            let cf = cf.clone();
            thread::spawn(move || {
                engine
                    .delete_range(&cf, b"read_test_0100", b"read_test_0300")
                    .unwrap();
            })
        };

        let read_handle = {
            let engine = Arc::clone(&engine);
            let cf = cf.clone();
            thread::spawn(move || {
                let mut read_count = 0;
                for i in 0..500 {
                    let key = format!("read_test_{:04}", i);
                    // Reads should not crash or hang
                    if engine.get(&cf, key.as_bytes()).unwrap().is_some() {
                        read_count += 1;
                    }
                }
                read_count
            })
        };

        delete_handle.join().unwrap();
        let read_count = read_handle.join().unwrap();

        // Assert - Some keys were read (may vary based on timing)
        // The important thing is no crash or corruption
        assert!(
            read_count > 0,
            "Should be able to read at least some keys during concurrent delete range for {}",
            name
        );
    }
}
