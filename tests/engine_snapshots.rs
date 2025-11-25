//! Snapshot Tests
//!
//! These tests verify point-in-time consistent read views:
//! - Isolation: Snapshots hide writes that occur after creation
//! - Consistency: Snapshots see a stable view even during compaction
//! - Multiple snapshots: Multiple concurrent snapshots work correctly
//! - Lifecycle: Snapshots are released properly on drop

mod common;

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, Query, StorageMode};
use common::{all_storage_modes, create_storage_mode, test_temp_dir};

// ============================================================================
// Basic Snapshot Operations
// ============================================================================

#[test]
fn should_hide_writes_given_snapshot_created_before_write_when_get_at() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        engine.put(&cf, b"key", b"v1").expect("put");
        let snapshot = engine.snapshot();
        engine.put(&cf, b"key", b"v2").expect("put v2");

        // Act
        let at_snapshot = engine.get_at(&cf, b"key", &snapshot).expect("get_at");
        let current = engine.get(&cf, b"key").expect("get");

        // Assert
        assert_eq!(at_snapshot, Some(Bytes::from_static(b"v1")), "Failed for {}", name);
        assert_eq!(current, Some(Bytes::from_static(b"v2")), "Failed for {}", name);
    }
}

#[test]
fn should_return_none_given_snapshot_before_key_exists_when_get_at() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        let snapshot = engine.snapshot();
        engine.put(&cf, b"key", b"value").expect("put");

        // Act
        let at_snapshot = engine.get_at(&cf, b"key", &snapshot).expect("get_at");
        let current = engine.get(&cf, b"key").expect("get");

        // Assert
        assert_eq!(at_snapshot, None, "Failed for {}", name);
        assert_eq!(current, Some(Bytes::from_static(b"value")), "Failed for {}", name);
    }
}

#[test]
fn should_see_value_given_snapshot_after_write_when_get_at() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        engine.put(&cf, b"key", b"value").expect("put");
        let snapshot = engine.snapshot();

        // Act
        let at_snapshot = engine.get_at(&cf, b"key", &snapshot).expect("get_at");

        // Assert
        assert_eq!(at_snapshot, Some(Bytes::from_static(b"value")), "Failed for {}", name);
    }
}

#[test]
fn should_see_deleted_key_given_snapshot_before_delete_when_get_at() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        engine.put(&cf, b"key", b"value").expect("put");
        let snapshot = engine.snapshot();
        engine.delete(&cf, b"key").expect("delete");

        // Act
        let at_snapshot = engine.get_at(&cf, b"key", &snapshot).expect("get_at");
        let current = engine.get(&cf, b"key").expect("get");

        // Assert
        assert_eq!(at_snapshot, Some(Bytes::from_static(b"value")), "Failed for {}", name);
        assert_eq!(current, None, "Failed for {}", name);
    }
}

// ============================================================================
// Snapshot Scans
// ============================================================================

#[test]
fn should_hide_newer_writes_given_snapshot_when_scan_at() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        engine.put(&cf, b"key", b"v1").expect("put");
        let snapshot = engine.snapshot();
        engine.put(&cf, b"key", b"v2").expect("put v2");

        // Act
        let results = engine
            .scan_at(
                &cf,
                Query::new()
                    .start_key(Bytes::from_static(b"a"))
                    .end_key(Bytes::from_static(b"z")),
                &snapshot,
            )
            .expect("scan_at");

        // Assert
        assert_eq!(
            results,
            vec![(Bytes::from_static(b"key"), Bytes::from_static(b"v1"))],
            "Failed for {}",
            name
        );
    }
}

#[test]
fn should_exclude_keys_written_after_snapshot_when_scan_at() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        engine.put(&cf, b"k1", b"v1").expect("put k1");
        let snapshot = engine.snapshot();
        engine.put(&cf, b"k2", b"v2").expect("put k2");

        // Act
        let results = engine
            .scan_at(
                &cf,
                Query::new()
                    .start_key(Bytes::from_static(b"k"))
                    .end_key(Bytes::from_static(b"l")),
                &snapshot,
            )
            .expect("scan_at");

        // Assert - only k1 visible in snapshot
        assert_eq!(results.len(), 1, "Failed for {}", name);
        assert_eq!(results[0].0, Bytes::from_static(b"k1"), "Failed for {}", name);
    }
}

#[test]
fn should_include_deleted_keys_given_snapshot_before_delete_when_scan_at() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        engine.put(&cf, b"k1", b"v1").expect("put");
        engine.put(&cf, b"k2", b"v2").expect("put");
        let snapshot = engine.snapshot();
        engine.delete(&cf, b"k1").expect("delete");

        // Act
        let at_snapshot = engine
            .scan_at(
                &cf,
                Query::new()
                    .start_key(Bytes::from_static(b"k"))
                    .end_key(Bytes::from_static(b"l")),
                &snapshot,
            )
            .expect("scan_at");
        let current = engine
            .scan(
                &cf,
                Query::new()
                    .start_key(Bytes::from_static(b"k"))
                    .end_key(Bytes::from_static(b"l")),
            )
            .expect("scan");

        // Assert
        assert_eq!(at_snapshot.len(), 2, "Snapshot should see both keys for {}", name);
        assert_eq!(current.len(), 1, "Current should see only k2 for {}", name);
    }
}

// ============================================================================
// Multiple Snapshots
// ============================================================================

#[test]
fn should_maintain_separate_views_given_multiple_snapshots_when_reading() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        engine.put(&cf, b"key", b"v1").expect("put v1");
        let snap1 = engine.snapshot();

        engine.put(&cf, b"key", b"v2").expect("put v2");
        let snap2 = engine.snapshot();

        engine.put(&cf, b"key", b"v3").expect("put v3");
        let snap3 = engine.snapshot();

        // Act
        let r1 = engine.get_at(&cf, b"key", &snap1).expect("get_at 1");
        let r2 = engine.get_at(&cf, b"key", &snap2).expect("get_at 2");
        let r3 = engine.get_at(&cf, b"key", &snap3).expect("get_at 3");
        let r_current = engine.get(&cf, b"key").expect("get");

        // Assert - each snapshot sees correct version
        assert_eq!(r1, Some(Bytes::from_static(b"v1")), "snap1 failed for {}", name);
        assert_eq!(r2, Some(Bytes::from_static(b"v2")), "snap2 failed for {}", name);
        assert_eq!(r3, Some(Bytes::from_static(b"v3")), "snap3 failed for {}", name);
        assert_eq!(r_current, Some(Bytes::from_static(b"v3")), "current failed for {}", name);
    }
}

#[test]
fn should_work_correctly_given_empty_database_when_snapshot_created() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        // Act - snapshot of empty DB
        let snapshot = engine.snapshot();
        engine.put(&cf, b"key", b"value").expect("put");

        // Assert - snapshot sees empty, current sees data
        assert_eq!(engine.get_at(&cf, b"key", &snapshot).unwrap(), None, "Failed for {}", name);
        assert_eq!(
            engine.get(&cf, b"key").unwrap(),
            Some(Bytes::from_static(b"value")),
            "Failed for {}",
            name
        );
    }
}

// ============================================================================
// Snapshot Lifecycle
// ============================================================================

#[test]
fn should_not_block_writes_given_snapshot_held_when_writing() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        engine.put(&cf, b"key", b"v1").expect("put");
        let _snapshot = engine.snapshot();

        // Act - writes should still succeed while snapshot is held
        for i in 0..100 {
            engine
                .put(&cf, format!("key{}", i).as_bytes(), b"value")
                .expect("put");
        }

        // Assert - all writes succeeded
        assert_eq!(
            engine.get(&cf, b"key99").unwrap(),
            Some(Bytes::from_static(b"value")),
            "Failed for {}",
            name
        );
    }
}

#[test]
fn should_allow_writes_given_snapshot_dropped_when_continuing() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        engine.put(&cf, b"key1", b"val1").expect("put");

        // Act - create and immediately drop snapshot
        {
            let _snapshot = engine.snapshot();
        } // Snapshot dropped here

        engine.put(&cf, b"key2", b"val2").expect("put");

        // Assert - writes succeed after snapshot release
        assert_eq!(
            engine.get(&cf, b"key2").unwrap(),
            Some(Bytes::from_static(b"val2")),
            "Failed for {}",
            name
        );
    }
}

// ============================================================================
// Snapshot + Persistence (LocalDisk only)
// ============================================================================

#[test]
fn should_recover_data_given_crash_with_active_snapshot_when_reopening() {
    // Arrange - snapshots don't persist across restarts, but data should
    let dir = test_temp_dir();
    let path = dir.path().to_path_buf();

    {
        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk { db_path: path.clone() },
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        engine.put(&cf, b"key1", b"val1").expect("put");
        let _snapshot = engine.snapshot(); // Create snapshot but don't use it
        engine.put(&cf, b"key2", b"val2").expect("put");
        // Drop engine with active snapshot
    }

    // Act - reopen
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: path },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("reopen");
    let cf = engine.default_column_family();

    // Assert - both keys recovered (snapshots don't persist across crashes)
    assert_eq!(engine.get(&cf, b"key1").unwrap(), Some(Bytes::from_static(b"val1")));
    assert_eq!(engine.get(&cf, b"key2").unwrap(), Some(Bytes::from_static(b"val2")));
}

#[test]
fn should_preserve_snapshot_view_given_flush_when_reading_at_snapshot() {
    // Test that flush doesn't affect snapshot visibility
    // MVCC: snapshot should see v1 even when flush happens after snapshot
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Arrange - v1 in memtable, snapshot, then v2
    engine.put(&cf, b"key", b"v1").expect("put");
    
    // Debug: check we can read v1 before snapshot
    eprintln!("[DEBUG] Before snapshot: get = {:?}", engine.get(&cf, b"key"));
    
    let snapshot = engine.snapshot();
    eprintln!("[DEBUG] Snapshot seq = {}", snapshot.seq);
    
    // Debug: check we can read v1 at snapshot
    eprintln!("[DEBUG] After snapshot, before v2: get_at = {:?}", engine.get_at(&cf, b"key", &snapshot));
    
    engine.put(&cf, b"key", b"v2").expect("put v2");
    
    // Debug: check we can read v1 at snapshot and v2 current
    eprintln!("[DEBUG] After v2, before flush: get_at = {:?}, get = {:?}", 
        engine.get_at(&cf, b"key", &snapshot), engine.get(&cf, b"key"));
    
    // Act - flush both versions to SST
    engine.flush().expect("flush");
    
    // Debug: check state after flush
    eprintln!("[DEBUG] After flush: get_at = {:?}, get = {:?}", 
        engine.get_at(&cf, b"key", &snapshot), engine.get(&cf, b"key"));

    // Assert - snapshot still sees v1 (MVCC guarantee)
    assert_eq!(
        engine.get_at(&cf, b"key", &snapshot).unwrap(),
        Some(Bytes::from_static(b"v1"))
    );
    assert_eq!(engine.get(&cf, b"key").unwrap(), Some(Bytes::from_static(b"v2")));
}

#[test]
fn should_preserve_snapshot_view_given_compaction_when_reading_at_snapshot() {
    // Test that compaction doesn't affect snapshot visibility
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Arrange
    engine.put(&cf, b"key", b"v1").expect("put");
    engine.flush().expect("flush");

    let snapshot = engine.snapshot();

    // Overwrite and delete
    engine.put(&cf, b"key", b"v2").expect("put v2");
    engine.flush().expect("flush");

    // Act - compact
    engine.compact_range(&cf, Some(b""), Some(b"~")).expect("compact");

    // Assert - snapshot still sees v1
    assert_eq!(
        engine.get_at(&cf, b"key", &snapshot).unwrap(),
        Some(Bytes::from_static(b"v1"))
    );
    assert_eq!(engine.get(&cf, b"key").unwrap(), Some(Bytes::from_static(b"v2")));
}

// ============================================================================
// Snapshot + Delete Range
// ============================================================================

#[test]
fn should_preserve_deleted_range_given_snapshot_before_delete_range_when_scan_at() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        // Populate keys
        for i in 0..10 {
            engine
                .put(&cf, format!("k{:02}", i).as_bytes(), b"v")
                .expect("put");
        }

        let snapshot = engine.snapshot();

        // Act - delete range after snapshot
        engine.delete_range(&cf, b"k03", b"k07").expect("delete_range");

        // Assert - snapshot sees all keys, current sees only undeleted
        let at_snapshot = engine
            .scan_at(
                &cf,
                Query::new()
                    .start_key(Bytes::from_static(b"k00"))
                    .end_key(Bytes::from_static(b"k99")),
                &snapshot,
            )
            .expect("scan_at");
        let current = engine
            .scan(
                &cf,
                Query::new()
                    .start_key(Bytes::from_static(b"k00"))
                    .end_key(Bytes::from_static(b"k99")),
            )
            .expect("scan");

        assert_eq!(at_snapshot.len(), 10, "Snapshot should see all keys for {}", name);
        assert_eq!(current.len(), 6, "Current should see 6 keys for {}", name); // 0,1,2,7,8,9
    }
}
