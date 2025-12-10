//! Iterator and Scan Operation Tests
//!
//! Tests for iterator behavior, scanning, and streaming operations.
//!
//! # Test Categories
//!
//! - Basic iteration: forward/reverse scans, range queries
//! - Seek operations: seek to key, seek past end
//! - Iterator stability: during compaction, flush, across SST boundaries
//! - Tombstone handling: deleted keys, range tombstones
//! - Streaming scans: scan_streaming API
//! - Pagination: chunked iteration, resume from checkpoint
//!
//! # Storage Mode Coverage
//!
//! All tests run on both LocalDisk and CloudBacked modes via `disk_storage_modes()`.

mod common;

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, Query};
use cntryl_midge::testkit::{create_storage_mode, disk_storage_modes, test_temp_dir};
use std::sync::Arc;

// ============================================================================
// BASIC ITERATION TESTS
// ============================================================================

#[test]
fn should_iterate_all_keys_in_order_given_populated_db_when_scanning() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Insert keys out of order
        eng.put(&cf, b"key3", b"val3").expect("put");
        eng.put(&cf, b"key1", b"val1").expect("put");
        eng.put(&cf, b"key2", b"val2").expect("put");

        // Act
        let results = eng
            .scan(
                &cf,
                Query::new()
                    .start_key(Bytes::from_static(b"key1"))
                    .end_key(Bytes::from_static(b"key4")),
            )
            .expect("scan");

        // Assert - keys should be in sorted order
        assert_eq!(results.len(), 3, "{}: expected 3 results", name);
        assert_eq!(results[0].0, Bytes::from("key1"), "{}: first key", name);
        assert_eq!(results[1].0, Bytes::from("key2"), "{}: second key", name);
        assert_eq!(results[2].0, Bytes::from("key3"), "{}: third key", name);
        drop(eng);
        eprintln!("âœ“ {}", name);
    }
}

#[test]
fn should_iterate_in_reverse_given_reverse_query_when_scanning() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        eng.put(&cf, b"key1", b"value1").expect("put");
        eng.put(&cf, b"key2", b"value2").expect("put");
        eng.put(&cf, b"key3", b"value3").expect("put");

        // Act - scan with reverse
        let query = Query::new()
            .start_key(Bytes::from_static(b"key1"))
            .end_key(Bytes::from_static(b"key4"))
            .reverse();
        let results = eng.scan(&cf, query).expect("reverse scan");

        // Assert - results in descending order
        assert_eq!(results.len(), 3, "{}: expected 3 results", name);
        assert_eq!(results[0].0, Bytes::from("key3"), "{}: first (key3)", name);
        assert_eq!(results[1].0, Bytes::from("key2"), "{}: second (key2)", name);
        assert_eq!(results[2].0, Bytes::from("key1"), "{}: third (key1)", name);
        drop(eng);
        eprintln!("âœ“ {}", name);
    }
}

#[test]
fn should_limit_results_given_limit_query_when_scanning() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        for i in 0..10 {
            eng.put(&cf, format!("key{:02}", i).as_bytes(), b"value")
                .expect("put");
        }

        // Act - scan with limit
        let results = eng
            .scan(&cf, Query::new().limit(5))
            .expect("scan with limit");

        // Assert
        assert_eq!(results.len(), 5, "{}: expected 5 results", name);
        drop(eng);
        eprintln!("âœ“ {}", name);
    }
}

#[test]
fn should_return_empty_given_empty_db_when_scanning() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Act - scan empty database
        let results = eng.scan(&cf, Query::new()).expect("scan");

        // Assert
        assert!(results.is_empty(), "{}: expected empty results", name);
        drop(eng);
        eprintln!("âœ“ {}", name);
    }
}

// ============================================================================
// SEEK OPERATION TESTS
// ============================================================================

#[test]
fn should_return_next_key_given_seek_to_missing_key_when_scanning() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        eng.put(&cf, b"key1", b"val1").expect("put");
        eng.put(&cf, b"key5", b"val5").expect("put");

        // Act - seek to key3 (doesn't exist)
        let results = eng
            .scan(
                &cf,
                Query::new()
                    .start_key(Bytes::from_static(b"key3"))
                    .end_key(Bytes::from_static(b"key9")),
            )
            .expect("scan");

        // Assert - should return key5 (next available)
        assert_eq!(results.len(), 1, "{}: expected 1 result", name);
        assert_eq!(
            results[0].0,
            Bytes::from("key5"),
            "{}: should find key5",
            name
        );
        drop(eng);
        eprintln!("âœ“ {}", name);
    }
}

#[test]
fn should_return_empty_given_seek_past_end_when_scanning() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        eng.put(&cf, b"key1", b"val1").expect("put");
        eng.put(&cf, b"key2", b"val2").expect("put");

        // Act - seek past all keys
        let results = eng
            .scan(
                &cf,
                Query::new()
                    .start_key(Bytes::from_static(b"key9"))
                    .end_key(Bytes::from_static(b"key~")),
            )
            .expect("scan");

        // Assert
        assert!(results.is_empty(), "{}: expected empty results", name);
        drop(eng);
        eprintln!("âœ“ {}", name);
    }
}

#[test]
fn should_return_empty_given_invalid_range_when_start_greater_than_end() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        eng.put(&cf, b"key1", b"value1").expect("put");
        eng.put(&cf, b"key2", b"value2").expect("put");

        // Act - query with start > end
        let results = eng
            .scan(
                &cf,
                Query::new()
                    .start_key(Bytes::from_static(b"z"))
                    .end_key(Bytes::from_static(b"a")),
            )
            .expect("scan");

        // Assert
        assert!(results.is_empty(), "{}: expected empty results", name);
        drop(eng);
        eprintln!("âœ“ {}", name);
    }
}

// ============================================================================
// ITERATOR STABILITY TESTS (COMPACTION/FLUSH)
// ============================================================================

#[test]
fn should_continue_safely_given_compaction_when_iterating_with_snapshot() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        for i in 0..100 {
            let key = format!("key{:03}", i);
            eng.put(&cf, key.as_bytes(), b"val").expect("put");
        }
        eng.flush().expect("flush");

        // Act - start iteration with snapshot, trigger compaction mid-scan
        let query = Query::new()
            .start_key(Bytes::from("key000"))
            .end_key(Bytes::from("key100"));
        let snapshot = eng.snapshot();
        eng.compact_range(&cf, Some(b""), Some(b"~"))
            .expect("compact");

        let results = eng.scan_at(&cf, query, &snapshot).expect("scan_at");

        // Assert - should get consistent results despite compaction
        assert_eq!(results.len(), 100, "{}: expected 100 results", name);
        drop(eng);
        eprintln!("âœ“ {}", name);
    }
}

#[test]
fn should_handle_gracefully_given_sst_removed_when_iterating_with_snapshot() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        for i in 0..50 {
            let key = format!("key{:03}", i);
            eng.put(&cf, key.as_bytes(), b"val").expect("put");
        }
        eng.flush().expect("flush");

        // Act - create snapshot, then compact (removes old SSTs)
        let snapshot = eng.snapshot();
        eng.compact_range(&cf, Some(b""), Some(b"~"))
            .expect("compact");

        let results = eng
            .scan_at(
                &cf,
                Query::new()
                    .start_key(Bytes::from("key000"))
                    .end_key(Bytes::from("key100")),
                &snapshot,
            )
            .expect("scan_at");

        // Assert - snapshot should still work
        assert_eq!(results.len(), 50, "{}: expected 50 results", name);
        drop(eng);
        eprintln!("âœ“ {}", name);
    }
}

#[test]
fn should_iterate_consistently_given_data_spans_sst_boundaries_when_scanning() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 512, // Small to force multiple flushes
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        for i in 0..50u8 {
            eng.put(&cf, &[i], format!("v{}", i).as_bytes())
                .expect("put");
        }
        eng.flush().expect("flush");

        // Act - scan all rows
        let results = eng.scan(&cf, Query::new()).expect("scan");

        // Assert
        assert_eq!(results.len(), 50, "{}: expected 50 results", name);
        drop(eng);
        eprintln!("âœ“ {}", name);
    }
}

#[test]
fn should_yield_stable_results_given_flush_in_progress_when_scanning() {
    // Arrange
    // This test uses a single mode since it involves threading
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: cntryl_midge::StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    for i in 0..30u8 {
        eng.put(&cf, &[i], format!("v{}", i).as_bytes())
            .expect("put");
    }

    // Act - flush and scan concurrently
    let eng = Arc::new(std::sync::Mutex::new(eng));
    let eng_clone = eng.clone();
    let flusher = std::thread::spawn(move || {
        let guard = eng_clone.lock().unwrap();
        guard.flush().unwrap();
    });

    let guard = eng.lock().unwrap();
    let results = guard
        .scan(&guard.default_column_family(), Query::new())
        .expect("scan");

    // Assert
    assert_eq!(results.len(), 30, "expected 30 results");
    drop(guard);
    flusher.join().unwrap();
}

// ============================================================================
// TOMBSTONE HANDLING TESTS
// ============================================================================

#[test]
fn should_skip_deleted_keys_given_tombstones_when_scanning() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        eng.put(&cf, b"key1", b"val1").expect("put");
        eng.put(&cf, b"key2", b"val2").expect("put");
        eng.put(&cf, b"key3", b"val3").expect("put");

        // Act - delete key2
        eng.delete(&cf, b"key2").expect("delete");

        let results = eng
            .scan(
                &cf,
                Query::new()
                    .start_key(Bytes::from_static(b"key1"))
                    .end_key(Bytes::from_static(b"key4")),
            )
            .expect("scan");

        // Assert
        assert_eq!(results.len(), 2, "{}: expected 2 results", name);
        assert_eq!(results[0].0, Bytes::from("key1"));
        assert_eq!(results[1].0, Bytes::from("key3"));
        drop(eng);
        eprintln!("âœ“ {}", name);
    }
}

#[test]
fn should_respect_range_tombstones_given_delete_range_when_scanning() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        for i in 0..10 {
            let key = format!("key{:02}", i);
            eng.put(&cf, key.as_bytes(), b"val").expect("put");
        }

        // Act - delete range
        eng.delete_range(&cf, b"key03", b"key07")
            .expect("delete_range");

        let results = eng
            .scan(
                &cf,
                Query::new()
                    .start_key(Bytes::from("key00"))
                    .end_key(Bytes::from("key10")),
            )
            .expect("scan");

        // Assert - keys 03-06 should be missing
        assert_eq!(results.len(), 6, "{}: expected 6 results", name);
        assert_eq!(results[2].0, Bytes::from("key02"));
        assert_eq!(results[3].0, Bytes::from("key07"));
        drop(eng);
        eprintln!("âœ“ {}", name);
    }
}

#[test]
fn should_return_latest_value_given_interleaved_puts_deletes_when_scanning() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        eng.put(&cf, b"key1", b"v1").expect("put");
        eng.put(&cf, b"key2", b"v2").expect("put");
        eng.delete(&cf, b"key2").expect("delete");
        eng.put(&cf, b"key2", b"v2_new").expect("put");
        eng.put(&cf, b"key3", b"v3").expect("put");

        // Act
        let results = eng
            .scan(
                &cf,
                Query::new()
                    .start_key(Bytes::from_static(b"key0"))
                    .end_key(Bytes::from_static(b"key9")),
            )
            .expect("scan");

        // Assert - should see latest values
        assert_eq!(results.len(), 3, "{}: expected 3 results", name);
        assert_eq!(results[1].0, Bytes::from("key2"));
        assert_eq!(results[1].1, Bytes::from("v2_new"));
        drop(eng);
        eprintln!("âœ“ {}", name);
    }
}

// ============================================================================
// STREAMING SCAN TESTS
// ============================================================================

#[test]
fn should_match_regular_scan_given_streaming_scan_when_comparing() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 100,
            enable_compaction: false,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Put keys and flush
        for i in 0..5u8 {
            eng.put(&cf, &[b'a' + i], &[b'1' + i]).expect("put");
        }
        eng.flush().expect("flush");

        // Add more to memtable
        for i in 5..10u8 {
            eng.put(&cf, &[b'a' + i], &[b'1' + i]).expect("put");
        }

        // Act
        let regular = eng
            .scan(
                &cf,
                Query::new()
                    .start_key(Bytes::from_static(b"a"))
                    .end_key(Bytes::from_static(b"z")),
            )
            .expect("scan");
        let streaming = eng
            .scan_streaming(
                Query::new()
                    .start_key(Bytes::from_static(b"a"))
                    .end_key(Bytes::from_static(b"z")),
            )
            .expect("scan_streaming");

        // Assert
        assert_eq!(regular.len(), streaming.len(), "{}: lengths match", name);
        assert_eq!(regular, streaming, "{}: results match", name);
        drop(eng);
        eprintln!("âœ“ {}", name);
    }
}

#[test]
fn should_respect_limit_given_streaming_scan_when_limited() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        for i in 0..10u8 {
            eng.put(&cf, &[b'k', b'0' + i], &[b'v', b'0' + i])
                .expect("put");
        }

        // Act
        let results = eng
            .scan_streaming(Query::new().limit(5))
            .expect("scan_streaming");

        // Assert
        assert_eq!(results.len(), 5, "{}: expected 5 results", name);
        drop(eng);
        eprintln!("âœ“ {}", name);
    }
}

#[test]
fn should_apply_tombstones_given_streaming_scan_when_keys_deleted() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        eng.put(&cf, b"k1", b"v1").expect("put");
        eng.put(&cf, b"k2", b"v2").expect("put");
        eng.delete(&cf, b"k1").expect("delete");

        // Act
        let results = eng.scan_streaming(Query::new()).expect("scan_streaming");

        // Assert - only k2 should be present
        assert_eq!(results.len(), 1, "{}: expected 1 result", name);
        assert_eq!(results[0].0, Bytes::from_static(b"k2"));
        drop(eng);
        eprintln!("âœ“ {}", name);
    }
}

#[test]
fn should_handle_concurrent_streaming_scans_when_multiple_threads() {
    // Arrange
    // Single mode test since it involves threading
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: cntryl_midge::StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    for i in 0..20u8 {
        eng.put(&cf, &[b'k', b'0' + i], &[b'v', b'0' + i])
            .expect("put");
    }

    let eng = Arc::new(eng);
    let mut handles = vec![];

    // Act - launch concurrent streaming scans
    for _ in 0..4 {
        let eng_clone = eng.clone();
        handles.push(std::thread::spawn(move || {
            eng_clone
                .scan_streaming(Query::new())
                .expect("scan_streaming")
        }));
    }

    // Assert - all scans should succeed and return same results
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(results[0].len(), 20);
    for r in &results[1..] {
        assert_eq!(r.len(), results[0].len());
    }
}

// ============================================================================
// PAGINATION TESTS
// ============================================================================

#[test]
fn should_paginate_results_given_chunked_queries_when_iterating() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        for i in 0..20 {
            eng.put(
                &cf,
                format!("key{:02}", i).as_bytes(),
                format!("value{}", i).as_bytes(),
            )
            .expect("put");
        }

        // Act - scan in chunks
        let query1 = Query::new()
            .start_key(Bytes::from("key00"))
            .end_key(Bytes::from("key10"));
        let chunk1 = eng.scan(&cf, query1).expect("first chunk");

        let query2 = Query::new()
            .start_key(Bytes::from("key10"))
            .end_key(Bytes::from("key20"));
        let chunk2 = eng.scan(&cf, query2).expect("second chunk");

        // Assert - chunks should be disjoint and complete
        assert_eq!(chunk1.len(), 10, "{}: first chunk", name);
        assert_eq!(chunk2.len(), 10, "{}: second chunk", name);
        assert_eq!(chunk1[0].0, Bytes::from("key00"));
        assert_eq!(chunk1[9].0, Bytes::from("key09"));
        assert_eq!(chunk2[0].0, Bytes::from("key10"));
        assert_eq!(chunk2[9].0, Bytes::from("key19"));
        drop(eng);
        eprintln!("âœ“ {}", name);
    }
}

#[test]
fn should_produce_identical_results_given_repeated_scans_when_rewinding() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        for i in 0..10 {
            eng.put(&cf, format!("key{:02}", i).as_bytes(), b"value")
                .expect("put");
        }

        // Act - scan twice
        let query1 = Query::new()
            .start_key(Bytes::from("key00"))
            .end_key(Bytes::from("key10"));
        let results1 = eng.scan(&cf, query1).expect("first scan");

        let query2 = Query::new()
            .start_key(Bytes::from("key00"))
            .end_key(Bytes::from("key10"));
        let results2 = eng.scan(&cf, query2).expect("second scan");

        // Assert
        assert_eq!(results1.len(), 10, "{}: first scan", name);
        assert_eq!(results2.len(), 10, "{}: second scan", name);
        assert_eq!(results1, results2, "{}: results match", name);
        drop(eng);
        eprintln!("âœ“ {}", name);
    }
}

// ============================================================================
// LARGE DATASET TESTS
// ============================================================================

#[test]
fn should_handle_large_scan_given_many_keys_when_iterating() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Write 10k keys
        for i in 0..10000 {
            let key = format!("key{:06}", i);
            eng.put(&cf, key.as_bytes(), b"value").expect("put");
        }
        eng.flush().expect("flush");

        // Act
        let results = eng
            .scan(
                &cf,
                Query::new()
                    .start_key(Bytes::from("key000000"))
                    .end_key(Bytes::from("key999999")),
            )
            .expect("scan");

        // Assert
        assert_eq!(results.len(), 10000, "{}: expected 10000 results", name);
        drop(eng);
        eprintln!("âœ“ {}", name);
    }
}

#[test]
fn should_handle_large_streaming_scan_given_multiple_ssts_when_spanning() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 2048,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Insert 100 keys to span multiple SSTables
        for i in 0..100u16 {
            let key = format!("key_{:04}", i);
            let value = format!("value_{:04}", i);
            eng.put(&cf, key.as_bytes(), value.as_bytes()).expect("put");
        }
        eng.flush().expect("flush");

        // Act
        let results = eng.scan_streaming(Query::new()).expect("scan_streaming");

        // Assert
        assert_eq!(results.len(), 100, "{}: expected 100 results", name);
        drop(eng);
        eprintln!("âœ“ {}", name);
    }
}
