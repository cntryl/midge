// Streaming Scan Operations
// Extracted from engine.rs

#![allow(clippy::field_reassign_with_default)]
// Engine integration tests consolidated per repo preference
// Structure: Arrange // Act // Assert, one behavior per test, behavior-first names
use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, Query, StorageMode};

mod common;
use common::test_temp_dir;
#[test]
fn should_streaming_scan_match_regular_scan() {
    // Arrange: create a database with multiple keys across memtable and SSTs
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 100, // Small to force flush
        enable_compaction: false,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    // Put some keys and force flush
    for i in 0..5u8 {
        let key = vec![b'a' + i];
        let val = vec![b'1' + i];
        eng.put(&cf, &key, &val).expect("put");
    }

    // Wait for flush
    eng.wait_for_flush(std::time::Duration::from_millis(100))
        .expect("flush should complete");

    // Add more keys to memtable
    for i in 5..10u8 {
        let key = vec![b'a' + i];
        let val = vec![b'1' + i];
        eng.put(&cf, &key, &val).expect("put");
    }

    // Act: scan with both methods
    let regular_results = eng
        .scan(
            &cf,
            Query::new()
                .start_key(Bytes::from_static(b"a"))
                .end_key(Bytes::from_static(b"z")),
        )
        .expect("scan");
    let streaming_results = eng
        .scan_streaming(
            Query::new()
                .start_key(Bytes::from_static(b"a"))
                .end_key(Bytes::from_static(b"z")),
        )
        .expect("scan_streaming");

    // Assert: both should return the same results
    assert_eq!(regular_results.len(), streaming_results.len());
    assert_eq!(regular_results, streaming_results);
}

#[test]
fn should_streaming_scan_respect_limit() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    // Put 10 keys
    for i in 0..10u8 {
        let key = vec![b'k', b'0' + i];
        let val = vec![b'v', b'0' + i];
        eng.put(&cf, &key, &val).expect("put");
    }

    // Act: scan with limit of 5
    let results = eng
        .scan_streaming(Query::new().limit(5))
        .expect("scan_streaming");

    // Assert
    assert_eq!(results.len(), 5);
}

#[test]
fn should_streaming_scan_apply_tombstones() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    eng.put(&cf, b"k1", b"v1").expect("put");
    eng.put(&cf, b"k2", b"v2").expect("put");

    // Delete k1
    eng.delete(&cf, b"k1").expect("delete");

    // Act
    let results = eng.scan_streaming(Query::new()).expect("scan_streaming");

    // Assert: only k2 should be present
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, Bytes::from_static(b"k2"));
    assert_eq!(results[0].1, Bytes::from_static(b"v2"));
}

#[test]
fn should_handle_streaming_scan_on_empty_database() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");

    // Act
    let results = eng.scan_streaming(Query::new()).expect("scan_streaming");

    // Assert - Should return empty vec, not error
    assert_eq!(results.len(), 0);
}

#[test]
fn should_handle_streaming_scan_with_invalid_range() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    eng.put(&cf, b"key1", b"value1").expect("put");
    eng.put(&cf, b"key2", b"value2").expect("put");

    // Act - Query with start > end (empty range)
    let results = eng
        .scan_streaming(
            Query::new()
                .start_key(Bytes::from_static(b"z"))
                .end_key(Bytes::from_static(b"a")),
        )
        .expect("scan_streaming");

    // Assert - Should return empty results
    assert_eq!(results.len(), 0);
}

#[test]
fn should_handle_streaming_scan_after_engine_flush() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 50,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    // Add data and flush
    for i in 0..5u8 {
        eng.put(&cf, &[b'k', b'0' + i], &[b'v', b'0' + i])
            .expect("put");
    }
    eng.wait_for_flush(std::time::Duration::from_millis(100))
        .expect("flush");

    // Act - Stream after flush
    let results = eng.scan_streaming(Query::new()).expect("scan_streaming");

    // Assert - All data should be accessible
    assert_eq!(results.len(), 5);
}

#[test]
fn should_handle_streaming_scan_with_zero_limit() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    eng.put(&cf, b"key1", b"value1").expect("put");
    eng.put(&cf, b"key2", b"value2").expect("put");

    // Act - Stream with limit 0
    let results = eng
        .scan_streaming(Query::new().limit(0))
        .expect("scan_streaming");

    // Assert - Should return no results
    assert_eq!(results.len(), 0);
}

#[test]
fn should_handle_concurrent_streaming_scans() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
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

    let eng = std::sync::Arc::new(eng);
    let mut handles = vec![];

    // Act - Launch concurrent streaming scans
    for _ in 0..4 {
        let eng_clone = eng.clone();
        handles.push(std::thread::spawn(move || {
            eng_clone
                .scan_streaming(Query::new())
                .expect("scan_streaming")
        }));
    }

    // Assert - All scans should succeed and return same results
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    assert_eq!(results[0].len(), 20);
    for r in &results[1..] {
        assert_eq!(r.len(), results[0].len());
    }
}

#[test]
fn should_streaming_scan_handle_large_dataset() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 2048, // Small to force multiple flushes
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    // Insert 100 keys to span multiple SSTables (still meaningful test)
    for i in 0..100u16 {
        let key = format!("key_{:04}", i);
        let value = format!("value_{:04}", i);
        eng.put(&cf, key.as_bytes(), value.as_bytes()).expect("put");
    }

    // Wait for flushes
    eng.wait_for_flush(std::time::Duration::from_millis(200))
        .expect("flush");

    // Act - Stream result set spanning multiple SSTables
    let results = eng.scan_streaming(Query::new()).expect("scan_streaming");

    // Assert - Should handle multi-SSTable scan without error
    assert_eq!(results.len(), 100);
}
