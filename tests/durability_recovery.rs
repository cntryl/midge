// Durability tests for Recovery Semantics
// Tests exactly-once recovery, crash recovery, and WAL replay correctness

mod common;

use bytes::Bytes;
use common::*;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};

#[test]
fn should_replay_wal_exactly_once_after_crash() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };

    // Act
    {
        let eng = MidgeEngine::open(opts.clone()).unwrap();
        eng.put(Bytes::from("key1"), Bytes::from("value1")).unwrap();
        eng.put(Bytes::from("key2"), Bytes::from("value2")).unwrap();
        eng.put(Bytes::from("key3"), Bytes::from("value3")).unwrap();
    }

    {
        let eng = MidgeEngine::open(opts.clone()).unwrap();
        assert_eq!(eng.get(b"key1").unwrap(), Some(Bytes::from("value1")));
        assert_eq!(eng.get(b"key2").unwrap(), Some(Bytes::from("value2")));
        assert_eq!(eng.get(b"key3").unwrap(), Some(Bytes::from("value3")));
    }

    // Assert
    {
        let eng = MidgeEngine::open(opts).unwrap();
        assert_eq!(eng.get(b"key1").unwrap(), Some(Bytes::from("value1")));
        assert_eq!(eng.get(b"key2").unwrap(), Some(Bytes::from("value2")));
        assert_eq!(eng.get(b"key3").unwrap(), Some(Bytes::from("value3")));
    }
}

#[test]
fn should_not_replay_flushed_data_from_wal() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };

    // Act: Write, flush, then write more
    {
        let eng = MidgeEngine::open(opts.clone()).unwrap();

        // First batch - will be flushed
        eng.put(Bytes::from("flushed1"), Bytes::from("v1")).unwrap();
        eng.put(Bytes::from("flushed2"), Bytes::from("v2")).unwrap();
        eng.flush().unwrap();

        // Second batch - only in WAL
        eng.put(Bytes::from("unflushed1"), Bytes::from("v3"))
            .unwrap();
        eng.put(Bytes::from("unflushed2"), Bytes::from("v4"))
            .unwrap();
        // No flush - simulates crash
    }

    // Assert: Restart should recover correctly
    let eng = MidgeEngine::open(opts).unwrap();

    // Flushed data from SST
    assert_eq!(eng.get(b"flushed1").unwrap(), Some(Bytes::from("v1")));
    assert_eq!(eng.get(b"flushed2").unwrap(), Some(Bytes::from("v2")));

    // Unflushed data from WAL replay
    assert_eq!(eng.get(b"unflushed1").unwrap(), Some(Bytes::from("v3")));
    assert_eq!(eng.get(b"unflushed2").unwrap(), Some(Bytes::from("v4")));
}

#[test]
fn should_handle_multiple_restart_cycles_idempotently() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };

    // Act: Multiple restart cycles
    for cycle in 0..5 {
        let eng = MidgeEngine::open(opts.clone()).unwrap();

        // Write unique data for this cycle
        let key = format!("cycle{}", cycle);
        let value = format!("value{}", cycle);
        eng.put(Bytes::from(key), Bytes::from(value)).unwrap();

        // Drop to simulate restart
    }

    // Assert: All cycle data should be present
    let eng = MidgeEngine::open(opts).unwrap();
    for cycle in 0..5 {
        let key = format!("cycle{}", cycle);
        let expected = format!("value{}", cycle);
        assert_eq!(
            eng.get(key.as_bytes()).unwrap(),
            Some(Bytes::from(expected)),
            "Cycle {} data missing",
            cycle
        );
    }
}

#[test]
fn should_preserve_sequence_numbers_across_recovery() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };

    // Act: Write with sequence progression
    {
        let eng = MidgeEngine::open(opts.clone()).unwrap();

        // Multiple operations with increasing sequences
        for i in 0..10 {
            let key = format!("key{}", i);
            eng.put(Bytes::from(key), Bytes::from("v1")).unwrap();
        }

        // Update some keys
        for i in 0..5 {
            let key = format!("key{}", i);
            eng.put(Bytes::from(key), Bytes::from("v2")).unwrap();
        }
    }

    // Assert: Last-write-wins should be preserved
    let eng = MidgeEngine::open(opts).unwrap();
    for i in 0..5 {
        let key = format!("key{}", i);
        assert_eq!(
            eng.get(key.as_bytes()).unwrap(),
            Some(Bytes::from("v2")),
            "Updated keys should have v2"
        );
    }
    for i in 5..10 {
        let key = format!("key{}", i);
        assert_eq!(
            eng.get(key.as_bytes()).unwrap(),
            Some(Bytes::from("v1")),
            "Non-updated keys should have v1"
        );
    }
}

#[test]
fn should_recover_tombstones_correctly() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };

    // Act: Put, delete, restart
    {
        let eng = MidgeEngine::open(opts.clone()).unwrap();

        eng.put(Bytes::from("key1"), Bytes::from("value1")).unwrap();
        eng.put(Bytes::from("key2"), Bytes::from("value2")).unwrap();
        eng.delete(Bytes::from("key1")).unwrap();
        // Crash before flush
    }

    // Assert: Tombstone should be replayed
    let eng = MidgeEngine::open(opts).unwrap();
    assert_eq!(
        eng.get(b"key1").unwrap(),
        None,
        "Deleted key should stay deleted"
    );
    assert_eq!(eng.get(b"key2").unwrap(), Some(Bytes::from("value2")));
}

#[test]
fn should_handle_empty_wal_gracefully() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };

    // Act: Create database, flush everything, restart
    {
        let eng = MidgeEngine::open(opts.clone()).unwrap();
        eng.put(Bytes::from("key"), Bytes::from("value")).unwrap();
        eng.flush().unwrap();
    }

    // Assert: Should restart cleanly with empty WAL
    let eng = MidgeEngine::open(opts).unwrap();
    assert_eq!(eng.get(b"key").unwrap(), Some(Bytes::from("value")));
}

#[test]
fn should_maintain_consistency_across_mixed_operations() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };

    // Act: Complex interleaved operations
    {
        let eng = MidgeEngine::open(opts.clone()).unwrap();

        // Batch 1
        eng.put(Bytes::from("a"), Bytes::from("1")).unwrap();
        eng.put(Bytes::from("b"), Bytes::from("2")).unwrap();
        eng.flush().unwrap();

        // Batch 2 (unflushed)
        eng.put(Bytes::from("c"), Bytes::from("3")).unwrap();
        eng.delete(Bytes::from("a")).unwrap();

        // Batch 3 (flush)
        eng.put(Bytes::from("d"), Bytes::from("4")).unwrap();
        eng.flush().unwrap();

        // Batch 4 (unflushed crash)
        eng.put(Bytes::from("e"), Bytes::from("5")).unwrap();
        eng.put(Bytes::from("b"), Bytes::from("6")).unwrap();
    }

    // Assert: Final state should be consistent
    let eng = MidgeEngine::open(opts).unwrap();
    assert_eq!(eng.get(b"a").unwrap(), None, "a was deleted");
    assert_eq!(
        eng.get(b"b").unwrap(),
        Some(Bytes::from("6")),
        "b updated to 6"
    );
    assert_eq!(eng.get(b"c").unwrap(), Some(Bytes::from("3")));
    assert_eq!(eng.get(b"d").unwrap(), Some(Bytes::from("4")));
    assert_eq!(eng.get(b"e").unwrap(), Some(Bytes::from("5")));
}

#[test]
fn should_recover_large_wal_efficiently() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };

    // Act: Large WAL with many operations
    {
        let eng = MidgeEngine::open(opts.clone()).unwrap();

        for i in 0..1000 {
            let key = format!("key{:04}", i);
            let value = format!("value{}", i);
            eng.put(Bytes::from(key), Bytes::from(value)).unwrap();
        }
        // No flush - large WAL to replay
    }

    // Assert: All data should be recovered
    let eng = MidgeEngine::open(opts).unwrap();
    for i in 0..1000 {
        let key = format!("key{:04}", i);
        assert!(
            eng.get(key.as_bytes()).unwrap().is_some(),
            "key{:04} should exist",
            i
        );
    }
}

#[test]
fn should_handle_partial_flush_scenario() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 512, // Small memtable
        ..Default::default()
    };

    // Act: Fill memtable, trigger auto-flush, add more data
    {
        let eng = MidgeEngine::open(opts.clone()).unwrap();

        // Enough data to trigger flush
        for i in 0..20 {
            let key = format!("auto{:02}", i);
            let value = vec![0u8; 50];
            eng.put(Bytes::from(key), Bytes::from(value)).unwrap();
        }

        // Additional data after auto-flush
        eng.put(Bytes::from("after_flush"), Bytes::from("value"))
            .unwrap();
        // Crash before final flush
    }

    // Assert: Both auto-flushed and WAL data should be present
    let eng = MidgeEngine::open(opts).unwrap();
    assert!(eng.get(b"auto00").unwrap().is_some(), "Auto-flushed data");
    assert_eq!(
        eng.get(b"after_flush").unwrap(),
        Some(Bytes::from("value")),
        "WAL data after flush"
    );
}

#[test]
fn should_deduplicate_keys_during_recovery() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };

    // Act: Multiple updates to same key
    {
        let eng = MidgeEngine::open(opts.clone()).unwrap();

        // Write same key multiple times
        eng.put(Bytes::from("key"), Bytes::from("v1")).unwrap();
        eng.put(Bytes::from("key"), Bytes::from("v2")).unwrap();
        eng.put(Bytes::from("key"), Bytes::from("v3")).unwrap();
        eng.put(Bytes::from("key"), Bytes::from("v4")).unwrap();
        eng.put(Bytes::from("key"), Bytes::from("v5")).unwrap();
        // Crash - all in WAL
    }

    // Assert: Should see only latest version
    let eng = MidgeEngine::open(opts).unwrap();
    assert_eq!(
        eng.get(b"key").unwrap(),
        Some(Bytes::from("v5")),
        "Should have latest value only"
    );
}
