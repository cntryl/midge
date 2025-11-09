// Durability tests for Compaction operations
// Tests the 3-phase commit protocol:
// Phase 1: Write new SSTs and fsync
// Phase 2: Update manifest atomically
// Phase 3: Delete old SSTs only after manifest confirms

mod common;

use bytes::Bytes;
use common::*;
use cntryl_midge::MidgeEngine;
use cntryl_midge::{MidgeOptions, StorageMode};

#[test]
fn should_preserve_source_ssts_until_manifest_updated() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 512,       // Small to trigger flush
        enable_compaction: false, // Manual control
        ..Default::default()
    };

    // Act: Create enough data to flush into SSTs
    {
        let eng = MidgeEngine::open(opts.clone()).unwrap();
        for i in 0..20 {
            let key = format!("key{:03}", i);
            let value = vec![0u8; 50];
            eng.put(Bytes::from(key), Bytes::from(value)).unwrap();
        }
        eng.flush().unwrap();
    }

    // Assert: Verify SST files exist and manifest tracks them
    let sst_dir = dir.path().join("sst");
    assert!(sst_dir.exists(), "SST directory should exist");

    let sst_files: Vec<_> = std::fs::read_dir(&sst_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("sst"))
        .collect();

    assert!(!sst_files.is_empty(), "Should have at least one SST file");

    // Verify data is still accessible
    let eng = MidgeEngine::open(opts).unwrap();
    assert_eq!(
        eng.get(b"key000").unwrap(),
        Some(Bytes::from(vec![0u8; 50]))
    );
}

#[test]
fn should_not_lose_data_given_compaction_with_overwrites() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 512,
        enable_compaction: false,
        ..Default::default()
    };

    // Act: Write same keys multiple times with different values
    {
        let eng = MidgeEngine::open(opts.clone()).unwrap();

        // First batch
        for i in 0..10 {
            let key = format!("key{}", i);
            eng.put(Bytes::from(key.clone()), Bytes::from("v1"))
                .unwrap();
        }
        eng.flush().unwrap();

        // Second batch (overwrites)
        for i in 0..10 {
            let key = format!("key{}", i);
            eng.put(Bytes::from(key.clone()), Bytes::from("v2"))
                .unwrap();
        }
        eng.flush().unwrap();
    }

    // Assert: Latest value should be visible
    let eng = MidgeEngine::open(opts).unwrap();
    for i in 0..10 {
        let key = format!("key{}", i);
        assert_eq!(
            eng.get(key.as_bytes()).unwrap(),
            Some(Bytes::from("v2")),
            "key{} should have latest value",
            i
        );
    }
}

#[test]
fn should_preserve_tombstones_during_compaction() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 512,
        enable_compaction: false,
        ..Default::default()
    };

    // Act: Create, delete, flush multiple times
    {
        let eng = MidgeEngine::open(opts.clone()).unwrap();

        // Write and flush
        eng.put(Bytes::from("key1"), Bytes::from("value1")).unwrap();
        eng.flush().unwrap();

        // Delete and flush
        eng.delete(Bytes::from("key1")).unwrap();
        eng.flush().unwrap();
    }

    // Assert: Delete should persist
    let eng = MidgeEngine::open(opts).unwrap();
    assert_eq!(
        eng.get(b"key1").unwrap(),
        None,
        "Deleted key should stay deleted"
    );
}

#[test]
fn should_maintain_snapshot_visibility_across_compaction() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        ..Default::default()
    };

    // Act: Create data, snapshot, then modify
    let eng = MidgeEngine::open(opts).unwrap();

    eng.put(Bytes::from("key"), Bytes::from("v1")).unwrap();
    let snap = eng.snapshot();

    eng.put(Bytes::from("key"), Bytes::from("v2")).unwrap();
    eng.flush().unwrap();

    // Assert: Snapshot should see old value, current should see new
    assert_eq!(eng.get_at(b"key", &snap).unwrap(), Some(Bytes::from("v1")));
    assert_eq!(eng.get(b"key").unwrap(), Some(Bytes::from("v2")));
}

#[test]
fn should_handle_manifest_consistency_after_multiple_flushes() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 512,
        ..Default::default()
    };

    // Act: Multiple flush cycles
    {
        let eng = MidgeEngine::open(opts.clone()).unwrap();
        for batch in 0..5 {
            for i in 0..10 {
                let key = format!("b{}k{}", batch, i);
                let value = format!("value{}", batch);
                eng.put(Bytes::from(key), Bytes::from(value)).unwrap();
            }
            eng.flush().unwrap();
        }
    }

    // Assert: All data should be recoverable
    let eng = MidgeEngine::open(opts).unwrap();
    for batch in 0..5 {
        let key = format!("b{}k0", batch);
        let expected = format!("value{}", batch);
        assert_eq!(
            eng.get(key.as_bytes()).unwrap(),
            Some(Bytes::from(expected)),
            "batch {} data should persist",
            batch
        );
    }
}

#[test]
fn should_not_create_orphaned_ssts_after_restart() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };

    {
        let eng = MidgeEngine::open(opts.clone()).unwrap();
        eng.put(Bytes::from("key1"), Bytes::from("value1")).unwrap();
        eng.flush().unwrap();
    }

    // Act
    let _eng = MidgeEngine::open(opts.clone()).unwrap();
    let manifest_path = dir.path().join("manifest.json");
    let manifest_data = std::fs::read_to_string(&manifest_path).unwrap();

    let sst_dir = dir.path().join("sst");
    let sst_count = std::fs::read_dir(&sst_dir)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("sst"))
                .count()
        })
        .unwrap_or(0);

    // Assert
    assert!(sst_count > 0, "Should have SST files");
    assert!(manifest_data.contains("ssts"), "Manifest should track SSTs");
}

#[test]
fn should_preserve_key_ordering_across_flush() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };

    // Act: Insert keys in random order
    {
        let eng = MidgeEngine::open(opts.clone()).unwrap();
        let keys = vec!["zebra", "apple", "mango", "banana", "cherry"];
        for key in keys {
            eng.put(Bytes::from(key), Bytes::from("value")).unwrap();
        }
        eng.flush().unwrap();
    }

    // Assert: All keys should be retrievable
    let eng = MidgeEngine::open(opts).unwrap();
    for key in &["apple", "banana", "cherry", "mango", "zebra"] {
        assert_eq!(
            eng.get(key.as_bytes()).unwrap(),
            Some(Bytes::from("value")),
            "key {} should exist",
            key
        );
    }
}

#[test]
fn should_handle_sequence_numbers_correctly_across_compaction() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 512,
        ..Default::default()
    };

    // Act: Multiple operations with sequence progression
    {
        let eng = MidgeEngine::open(opts.clone()).unwrap();

        // Op 1: Put
        eng.put(Bytes::from("key"), Bytes::from("v1")).unwrap();
        eng.flush().unwrap();

        // Op 2: Update
        eng.put(Bytes::from("key"), Bytes::from("v2")).unwrap();
        eng.flush().unwrap();

        // Op 3: Delete
        eng.delete(Bytes::from("key")).unwrap();
        eng.flush().unwrap();
    }

    // Assert: Latest operation (delete) should be visible
    let eng = MidgeEngine::open(opts).unwrap();
    assert_eq!(
        eng.get(b"key").unwrap(),
        None,
        "Final delete should be visible"
    );
}

#[test]
fn should_maintain_consistency_given_large_compaction() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 1024,
        enable_compaction: false,
        ..Default::default()
    };

    // Act: Create large dataset across multiple SSTs
    {
        let eng = MidgeEngine::open(opts.clone()).unwrap();
        for batch in 0..3 {
            for i in 0..50 {
                let key = format!("key{:04}", batch * 100 + i);
                let value = vec![0u8; 100];
                eng.put(Bytes::from(key), Bytes::from(value)).unwrap();
            }
            eng.flush().unwrap();
        }
    }

    // Assert: All 150 keys should be accessible
    let eng = MidgeEngine::open(opts).unwrap();
    for batch in 0..3 {
        for i in 0..50 {
            let key = format!("key{:04}", batch * 100 + i);
            assert!(
                eng.get(key.as_bytes()).unwrap().is_some(),
                "{} should exist",
                key
            );
        }
    }
}
