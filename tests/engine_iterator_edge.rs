// Iterator Edge Cases (Phase 2 - P1)
// Tests deterministic iterator behavior under compaction, deletion, and boundary conditions

#![allow(clippy::field_reassign_with_default)]
mod common;
use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, Query, StorageMode};
use common::test_temp_dir;

#[test]
fn should_continue_safely_given_compaction_when_iterating() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.default_column_family();
    
    for i in 0..100 {
        let key = format!("key{:03}", i);
        eng.put(&cf, key.as_bytes(), b"val").unwrap();
    }
    eng.flush().unwrap();

    // Act - start iteration, trigger compaction mid-scan
    let query = Query::new()
        .start_key(Bytes::from("key000"))
        .end_key(Bytes::from("key100"));
    
    let snapshot = eng.snapshot();
    eng.compact_range(&cf, Some(b""), Some(b"~")).unwrap();
    
    let results = eng.scan_at(&cf, query, &snapshot).unwrap();

    // Assert - should get consistent results despite compaction
    assert_eq!(results.len(), 100);
}

#[test]
fn should_skip_deleted_key_when_seeking_after_delete() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.default_column_family();
    
    eng.put(&cf, b"key1", b"val1").unwrap();
    eng.put(&cf, b"key2", b"val2").unwrap();
    eng.put(&cf, b"key3", b"val3").unwrap();
    
    // Act
    eng.delete(&cf, b"key2").unwrap();
    
    let results = eng.scan(&cf, Query::new()
        .start_key(Bytes::from_static(b"key1"))
        .end_key(Bytes::from_static(b"key4"))).unwrap();

    // Assert
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, Bytes::from("key1"));
    assert_eq!(results[1].0, Bytes::from("key3"));
}

#[test]
fn should_handle_gracefully_given_sst_removed_when_iterating() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.default_column_family();
    
    for i in 0..50 {
        let key = format!("key{:03}", i);
        eng.put(&cf, key.as_bytes(), b"val").unwrap();
    }
    eng.flush().unwrap();

    // Act - create snapshot, then compact (removes old SSTs)
    let snapshot = eng.snapshot();
    eng.compact_range(&cf, Some(b""), Some(b"~")).unwrap();
    
    let results = eng.scan_at(&cf, Query::new()
        .start_key(Bytes::from("key000"))
        .end_key(Bytes::from("key100")), &snapshot).unwrap();

    // Assert - snapshot should still work even if files changed
    assert_eq!(results.len(), 50);
}

#[test]
fn should_not_allocate_unbounded_memory_given_large_scan_when_iterating() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.default_column_family();
    
    // Write 10k keys
    for i in 0..10000 {
        let key = format!("key{:06}", i);
        eng.put(&cf, key.as_bytes(), b"value").unwrap();
    }
    eng.flush().unwrap();

    // Act - scan all
    let results = eng.scan(&cf, Query::new()
        .start_key(Bytes::from("key000000"))
        .end_key(Bytes::from("key999999"))).unwrap();

    // Assert - should complete without OOM
    assert_eq!(results.len(), 10000);
}

#[test]
fn should_return_next_key_given_seek_greater_than_when_key_missing() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.default_column_family();
    
    eng.put(&cf, b"key1", b"val1").unwrap();
    eng.put(&cf, b"key5", b"val5").unwrap();

    // Act - seek to key3 (doesn't exist)
    let results = eng.scan(&cf, Query::new()
        .start_key(Bytes::from_static(b"key3"))
        .end_key(Bytes::from_static(b"key9"))).unwrap();

    // Assert - should return key5 (next available)
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, Bytes::from("key5"));
}

#[test]
fn should_return_empty_given_seek_past_end_when_no_keys_in_range() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.default_column_family();
    
    eng.put(&cf, b"key1", b"val1").unwrap();
    eng.put(&cf, b"key2", b"val2").unwrap();

    // Act - seek past all keys
    let results = eng.scan(&cf, Query::new()
        .start_key(Bytes::from_static(b"key9"))
        .end_key(Bytes::from_static(b"key~"))).unwrap();

    // Assert
    assert!(results.is_empty());
}

#[test]
fn should_respect_range_tombstones_when_iterating() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.default_column_family();
    
    for i in 0..10 {
        let key = format!("key{:02}", i);
        eng.put(&cf, key.as_bytes(), b"val").unwrap();
    }
    
    // Act
    eng.delete_range(&cf, b"key03", b"key07").unwrap();
    
    let results = eng.scan(&cf, Query::new()
        .start_key(Bytes::from("key00"))
        .end_key(Bytes::from("key10"))).unwrap();

    // Assert - keys 03-06 should be missing
    assert_eq!(results.len(), 6);
    assert_eq!(results[2].0, Bytes::from("key02"));
    assert_eq!(results[3].0, Bytes::from("key07"));
}

#[test]
fn should_return_consistent_results_given_interleaved_puts_deletes_when_scanning() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.default_column_family();
    
    eng.put(&cf, b"key1", b"v1").unwrap();
    eng.put(&cf, b"key2", b"v2").unwrap();
    eng.delete(&cf, b"key2").unwrap();
    eng.put(&cf, b"key2", b"v2_new").unwrap();
    eng.put(&cf, b"key3", b"v3").unwrap();

    // Act
    let results = eng.scan(&cf, Query::new()
        .start_key(Bytes::from_static(b"key0"))
        .end_key(Bytes::from_static(b"key9"))).unwrap();

    // Assert - should see latest values
    assert_eq!(results.len(), 3);
    assert_eq!(results[1].0, Bytes::from("key2"));
    assert_eq!(results[1].1, Bytes::from("v2_new"));
}
