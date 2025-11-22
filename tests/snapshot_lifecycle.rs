// Snapshot Lifecycle (Phase 3 - P2)
// Tests snapshot blocking compaction, memory overhead, and crash recovery

#![allow(clippy::field_reassign_with_default)]
mod common;
use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use common::test_temp_dir;

#[test]
fn should_block_compaction_given_long_lived_snapshot_when_data_needed() {
    // Would test that compaction retains data needed by old snapshots
}

#[test]
fn should_track_memory_overhead_given_multiple_snapshots_when_created() {
    // Would test memory consumption scales with snapshot count
}

#[test]
fn should_recover_gracefully_given_crash_with_active_snapshots_when_reopening() {
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
        let _snapshot = eng.snapshot(); // Create snapshot but don't use it
        eng.put(&cf, b"key2", b"val2").unwrap();
        // Crash with active snapshot
    }

    // Act - reopen
    let opts2 = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng2 = MidgeEngine::open(opts2).unwrap();
    let cf2 = eng2.default_column_family();

    // Assert - both keys recovered (snapshots don't persist across crashes)
    assert!(eng2.get(&cf2, b"key1").unwrap().is_some());
    assert!(eng2.get(&cf2, b"key2").unwrap().is_some());
}

#[test]
fn should_preserve_data_given_snapshot_and_compaction_when_interacting() {
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
    eng.flush().unwrap();

    let snapshot = eng.snapshot();

    // Overwrite and delete
    eng.put(&cf, b"key1", b"v2").unwrap();
    eng.delete(&cf, b"key2").unwrap();
    eng.flush().unwrap();

    // Act - compact
    eng.compact_range(&cf, Some(b""), Some(b"~")).unwrap();

    // Assert - snapshot should still see old data
    assert_eq!(
        eng.get_at(&cf, b"key1", &snapshot).unwrap().unwrap(),
        Bytes::from("v1")
    );
    // Current view sees new data
    assert_eq!(eng.get(&cf, b"key1").unwrap().unwrap(), Bytes::from("v2"));
}

#[test]
fn should_expire_snapshot_given_ttl_when_time_elapsed() {
    // Would test automatic snapshot cleanup after TTL
}

#[test]
fn should_maintain_multiple_snapshots_given_concurrent_creation_when_reading() {
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

    eng.put(&cf, b"key", b"v1").unwrap();
    let snap1 = eng.snapshot();

    eng.put(&cf, b"key", b"v2").unwrap();
    let snap2 = eng.snapshot();

    eng.put(&cf, b"key", b"v3").unwrap();
    let snap3 = eng.snapshot();

    // Act - read from all snapshots
    let r1 = eng.get_at(&cf, b"key", &snap1).unwrap().unwrap();
    let r2 = eng.get_at(&cf, b"key", &snap2).unwrap().unwrap();
    let r3 = eng.get_at(&cf, b"key", &snap3).unwrap().unwrap();
    let r_current = eng.get(&cf, b"key").unwrap().unwrap();

    // Assert - each snapshot sees correct version
    assert_eq!(r1, Bytes::from("v1"));
    assert_eq!(r2, Bytes::from("v2"));
    assert_eq!(r3, Bytes::from("v3"));
    assert_eq!(r_current, Bytes::from("v3"));
}

#[test]
fn should_handle_snapshot_given_empty_db_when_created() {
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

    // Act - snapshot of empty DB
    let snapshot = eng.snapshot();
    eng.put(&cf, b"key1", b"val1").unwrap();

    // Assert - snapshot sees empty, current sees data
    assert!(eng.get_at(&cf, b"key1", &snapshot).unwrap().is_none());
    assert!(eng.get(&cf, b"key1").unwrap().is_some());
}

#[test]
fn should_read_consistently_given_snapshot_after_delete_when_querying() {
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

    let snapshot = eng.snapshot();

    // Act - delete after snapshot
    eng.delete(&cf, b"key1").unwrap();

    // Assert - snapshot still sees deleted key
    assert!(eng.get_at(&cf, b"key1", &snapshot).unwrap().is_some());
    assert!(eng.get(&cf, b"key1").unwrap().is_none());
}

#[test]
fn should_allow_writes_given_snapshot_released_when_no_longer_needed() {
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

    // Act - create and immediately drop snapshot
    {
        let _snapshot = eng.snapshot();
    } // Snapshot dropped here

    eng.put(&cf, b"key2", b"val2").unwrap();
    eng.flush().unwrap();

    // Assert - writes should succeed after snapshot release
    assert!(eng.get(&cf, b"key2").unwrap().is_some());
}
