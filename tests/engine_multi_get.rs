// Multi-Get Operations
// Extracted from engine.rs

#![allow(clippy::field_reassign_with_default)]
// Engine integration tests consolidated per repo preference
// Structure: Arrange // Act // Assert, one behavior per test, behavior-first names
use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};

mod common;
use common::test_temp_dir;
#[test]
fn should_multi_get_all_keys_from_memtable() {
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

    eng.put(&cf, b"k1", b"v1")
        .expect("put");
    eng.put(&cf, b"k2", b"v2")
        .expect("put");
    eng.put(&cf, b"k3", b"v3")
        .expect("put");

    // Act
    let keys: Vec<&[u8]> = vec![b"k1", b"k2", b"k3", b"k4"];
    let results: Vec<(Bytes, Option<Bytes>)> = keys
        .iter()
        .map(|k| {
            let v = eng.get(&cf, k).expect("get");
            (Bytes::from_static(k), v)
        })
        .collect();

    // Assert
    assert_eq!(results.len(), 4);
    assert_eq!(results[0].0, Bytes::from_static(b"k1"));
    assert_eq!(results[0].1, Some(Bytes::from_static(b"v1")));
    assert_eq!(results[1].0, Bytes::from_static(b"k2"));
    assert_eq!(results[1].1, Some(Bytes::from_static(b"v2")));
    assert_eq!(results[2].0, Bytes::from_static(b"k3"));
    assert_eq!(results[2].1, Some(Bytes::from_static(b"v3")));
    assert_eq!(results[3].0, Bytes::from_static(b"k4"));
    assert_eq!(results[3].1, None); // Not found
}


#[test]
fn should_multi_get_respect_tombstones() {
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

    eng.put(&cf, b"k1", b"v1")
        .expect("put");
    eng.put(&cf, b"k2", b"v2")
        .expect("put");
    eng.delete(&cf, b"k1").expect("delete");

    // Act
    let keys: Vec<&[u8]> = vec![b"k1", b"k2"];
    let results: Vec<(Bytes, Option<Bytes>)> = keys
        .iter()
        .map(|k| {
            let v = eng.get(&cf, k).expect("get");
            (Bytes::from_static(k), v)
        })
        .collect();

    // Assert
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, Bytes::from_static(b"k1"));
    assert_eq!(results[0].1, None); // Deleted
    assert_eq!(results[1].0, Bytes::from_static(b"k2"));
    assert_eq!(results[1].1, Some(Bytes::from_static(b"v2")));
}


#[test]
fn should_multi_get_from_ssts_after_flush() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        wal_buffer_size: 64, // Small WAL buffer to force rotation/flush
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    // Write data and force flush via WAL rotation
    eng.put(&cf, b"key000", b"value000").expect("put");
    eng.put(&cf, b"key005", b"value005").expect("put");

    // Force WAL rotation with a large write
    let big = vec![b'x'; 128];
    eng.put(&cf, b"key009", big.as_slice()).expect("put");

    // Give flush time to complete
    std::thread::sleep(std::time::Duration::from_millis(150));

    // Act: Read keys that should be in SSTs (from before rotation)
    let keys: Vec<&[u8]> = vec![b"key000", b"key005", b"key009", b"key999"];
    let results: Vec<(Bytes, Option<Bytes>)> = keys
        .iter()
        .map(|k| {
            let v = eng.get(&cf, k).expect("get");
            (Bytes::from_static(k), v)
        })
        .collect();

    // Assert
    assert_eq!(results.len(), 4);
    assert_eq!(results[0].0, Bytes::from_static(b"key000"));
    assert!(results[0].1.is_some()); // Should find key000
    assert_eq!(results[1].0, Bytes::from_static(b"key005"));
    assert!(results[1].1.is_some()); // Should find key005
    assert_eq!(results[2].0, Bytes::from_static(b"key009"));
    assert!(results[2].1.is_some()); // Should find key009
    assert_eq!(results[3].0, Bytes::from_static(b"key999"));
    assert_eq!(results[3].1, None); // Not found
}


#[test]
fn should_multi_get_mixed_memtable_and_sst() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        wal_buffer_size: 512, // WAL buffer (increased to account for WAL format v2 cf_id field)
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    // Write data and force flush
    eng.put(&cf, b"old000", b"oval000").expect("put");
    eng.put(&cf, b"old005", b"oval005").expect("put");

    // Force WAL rotation
    let big = vec![b'x'; 128];
    eng.put(&cf, b"oldlarge", big.as_slice()).expect("put");

    std::thread::sleep(std::time::Duration::from_millis(150));

    // Write new data to memtable (after rotation)
    eng.put(&cf, b"new1", b"nval1").expect("put");
    eng.put(&cf, b"new2", b"nval2").expect("put");

    // Act: Get mix of old (in SST) and new (in memtable) keys
    let keys: Vec<&[u8]> = vec![b"old000", b"new1", b"old005", b"new2", b"missing"];
    let results: Vec<(Bytes, Option<Bytes>)> = keys
        .iter()
        .map(|k| {
            let v = eng.get(&cf, k).expect("get");
            (Bytes::from_static(k), v)
        })
        .collect();

    // Assert
    assert_eq!(results.len(), 5);
    assert_eq!(results[0].0, Bytes::from_static(b"old000"));
    assert!(results[0].1.is_some()); // From SST
    assert_eq!(results[1].0, Bytes::from_static(b"new1"));
    assert_eq!(results[1].1, Some(Bytes::from_static(b"nval1"))); // From memtable
    assert_eq!(results[2].0, Bytes::from_static(b"old005"));
    assert!(results[2].1.is_some()); // From SST
    assert_eq!(results[3].0, Bytes::from_static(b"new2"));
    assert_eq!(results[3].1, Some(Bytes::from_static(b"nval2"))); // From memtable
    assert_eq!(results[4].0, Bytes::from_static(b"missing"));
    assert_eq!(results[4].1, None); // Not found
}


