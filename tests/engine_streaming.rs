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
    std::thread::sleep(std::time::Duration::from_millis(100));

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
