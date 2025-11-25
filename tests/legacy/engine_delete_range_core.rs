// Delete Range Core Semantics (Phase 2 - P1)
// Tests deterministic delete range behavior across levels, compaction, and recovery

#![allow(clippy::field_reassign_with_default)]
mod common;
use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use common::test_temp_dir;

#[test]
fn should_delete_keys_across_multiple_levels_when_delete_range_applied() {
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
    eng.flush().unwrap();

    eng.put(&cf, b"key4", b"val4").unwrap();
    eng.put(&cf, b"key5", b"val5").unwrap();
    eng.flush().unwrap();

    // Act
    eng.delete_range(&cf, b"key2", b"key5").unwrap();

    // Assert
    assert!(eng.get(&cf, b"key1").unwrap().is_some());
    assert!(eng.get(&cf, b"key2").unwrap().is_none());
    assert!(eng.get(&cf, b"key3").unwrap().is_none());
    assert!(eng.get(&cf, b"key4").unwrap().is_none());
    assert!(eng.get(&cf, b"key5").unwrap().is_some());
}

#[test]
fn should_resolve_overlapping_ranges_correctly_when_multiple_delete_ranges_issued() {
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
    eng.flush().unwrap();

    // Act - overlapping ranges
    eng.delete_range(&cf, b"key02", b"key06").unwrap();
    eng.delete_range(&cf, b"key04", b"key08").unwrap();

    // Assert - keys 02-07 should be deleted (union of ranges)
    assert!(eng.get(&cf, b"key01").unwrap().is_some());
    assert!(eng.get(&cf, b"key02").unwrap().is_none());
    assert!(eng.get(&cf, b"key05").unwrap().is_none());
    assert!(eng.get(&cf, b"key07").unwrap().is_none());
    assert!(eng.get(&cf, b"key08").unwrap().is_some());
}

#[test]
fn should_handle_point_writes_and_range_deletes_correctly_when_interleaved() {
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
    eng.delete_range(&cf, b"key1", b"key3").unwrap();
    eng.put(&cf, b"key2", b"new_val2").unwrap();

    // Assert - key2 should have new value (point write after range delete)
    assert!(eng.get(&cf, b"key1").unwrap().is_none());
    assert_eq!(
        eng.get(&cf, b"key2").unwrap().unwrap(),
        Bytes::from("new_val2")
    );
    assert!(eng.get(&cf, b"key3").unwrap().is_some());
}

#[test]
fn should_apply_range_tombstones_during_compaction_when_compacting_levels() {
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

    for i in 0..20 {
        let key = format!("key{:03}", i);
        eng.put(&cf, key.as_bytes(), b"val").unwrap();
    }
    eng.flush().unwrap();

    eng.delete_range(&cf, b"key005", b"key015").unwrap();
    eng.flush().unwrap();

    // Act - force compaction
    eng.compact_range(&cf, Some(b""), Some(b"~")).unwrap();

    // Assert
    assert!(eng.get(&cf, b"key004").unwrap().is_some());
    assert!(eng.get(&cf, b"key010").unwrap().is_none());
    assert!(eng.get(&cf, b"key015").unwrap().is_some());
}

#[test]
fn should_retain_range_tombstones_when_snapshots_exist() {
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
    eng.flush().unwrap();

    let snapshot = eng.snapshot();

    // Act
    eng.delete_range(&cf, b"key1", b"key3").unwrap();
    eng.flush().unwrap();

    // Assert - snapshot should see original values
    assert_eq!(
        eng.get_at(&cf, b"key1", &snapshot).unwrap().unwrap(),
        Bytes::from("val1")
    );

    // Current view should not see deleted keys
    assert!(eng.get(&cf, b"key1").unwrap().is_none());
}

#[test]
fn should_handle_large_range_deletion_efficiently_when_spanning_many_keys() {
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

    for i in 0..1000 {
        let key = format!("key{:06}", i);
        eng.put(&cf, key.as_bytes(), b"value").unwrap();
    }
    eng.flush().unwrap();

    // Act - delete large range
    eng.delete_range(&cf, b"key000100", b"key000900").unwrap();

    // Assert - spot check
    assert!(eng.get(&cf, b"key000050").unwrap().is_some());
    assert!(eng.get(&cf, b"key000500").unwrap().is_none());
    assert!(eng.get(&cf, b"key000950").unwrap().is_some());
}

#[test]
fn should_recover_range_tombstones_after_restart_when_persisted_in_wal() {
    // Arrange
    let dir = test_temp_dir();
    {
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

        // Act - delete range without flush
        eng.delete_range(&cf, b"key1", b"key3").unwrap();
    }

    // Assert - reopen and check
    let opts2 = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng2 = MidgeEngine::open(opts2).unwrap();
    let cf2 = eng2.default_column_family();

    assert!(eng2.get(&cf2, b"key1").unwrap().is_none());
    assert!(eng2.get(&cf2, b"key2").unwrap().is_none());
    assert!(eng2.get(&cf2, b"key3").unwrap().is_some());
}

#[test]
fn should_apply_range_deletes_in_memtable_and_sst_when_querying() {
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

    eng.put(&cf, b"key1", b"sst_val1").unwrap();
    eng.put(&cf, b"key2", b"sst_val2").unwrap();
    eng.flush().unwrap();

    eng.delete_range(&cf, b"key0", b"key2").unwrap();

    // Act - new key in memtable after range delete
    eng.put(&cf, b"key1", b"mem_val1").unwrap();

    // Assert
    assert_eq!(
        eng.get(&cf, b"key1").unwrap().unwrap(),
        Bytes::from("mem_val1")
    );
    assert!(eng.get(&cf, b"key2").unwrap().is_some());
}

#[test]
fn should_prevent_key_resurrection_when_range_delete_applied_before_compaction() {
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

    eng.put(&cf, b"key5", b"old_val").unwrap();
    eng.flush().unwrap();
    eng.compact_range(&cf, Some(b""), Some(b"~")).unwrap();

    eng.delete_range(&cf, b"key0", b"key9").unwrap();
    eng.flush().unwrap();

    // Act - compact (should not resurrect key5)
    eng.compact_range(&cf, Some(b""), Some(b"~")).unwrap();

    // Assert
    assert!(eng.get(&cf, b"key5").unwrap().is_none());
}

#[test]
fn should_handle_empty_range_delete_when_start_equals_end() {
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

    // Act - empty range
    eng.delete_range(&cf, b"key1", b"key1").unwrap();

    // Assert - no keys deleted
    assert!(eng.get(&cf, b"key1").unwrap().is_some());
    assert!(eng.get(&cf, b"key2").unwrap().is_some());
}
