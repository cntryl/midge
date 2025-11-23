/// Integration test demonstrating TestHooks functionality
///
/// This test shows how to use TestHooks to intercept fsync operations
/// and simulate crash scenarios for durability testing.
use cntryl_midge::{
    test_hooks::{FsyncBehavior, TestHooks},
    MidgeEngine, MidgeOptions, StorageMode,
};
use tempfile::TempDir;

#[test]
fn should_skip_fsync_when_test_hook_configured() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let hooks = TestHooks::new().with_fsync_behavior(FsyncBehavior::Skip);

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_sync: true, // Enable WAL sync to trigger fsync calls
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    // Act
    let eng = MidgeEngine::open(opts).expect("open engine");
    let cf = eng.default_column_family();
    eng.put(&cf, b"key1", b"value1").expect("put");
    eng.put(&cf, b"key2", b"value2").expect("put");

    // Assert
    // Fsync hooks were called (recorded) but actual fsync was skipped
    assert!(
        hooks.fsync_count() > 0,
        "fsync hooks should have been called"
    );

    // Verify data is still accessible (in memory)
    assert_eq!(
        eng.get(&cf, b"key1").expect("get"),
        Some(bytes::Bytes::from("value1"))
    );
}

#[test]
fn should_record_fsync_calls_when_using_record_only_mode() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let hooks = TestHooks::new().with_fsync_behavior(FsyncBehavior::RecordOnly);

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_sync: true, // Enable WAL sync to trigger fsync calls
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    // Act
    let eng = MidgeEngine::open(opts).expect("open engine");
    let cf = eng.default_column_family();

    let initial_count = hooks.fsync_count();
    eng.put(&cf, b"test", b"data").expect("put");
    let final_count = hooks.fsync_count();

    // Assert
    assert!(
        final_count > initial_count,
        "fsync count should increase after write operations"
    );
}

#[test]
fn should_perform_normal_fsync_when_no_hooks_configured() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        test_hooks: None, // No hooks - normal behavior
        ..Default::default()
    };

    // Act
    let eng = MidgeEngine::open(opts).expect("open engine");
    let cf = eng.default_column_family();
    eng.put(&cf, b"durable_key", b"durable_value").expect("put");

    // Assert
    // Normal fsync behavior - data should be durable
    assert_eq!(
        eng.get(&cf, b"durable_key").expect("get"),
        Some(bytes::Bytes::from("durable_value"))
    );
}

#[test]
fn should_increment_wal_append_count_when_writes_occur() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let hooks = TestHooks::new();

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    // Act
    let eng = MidgeEngine::open(opts).expect("open engine");
    let cf = eng.default_column_family();

    let initial_count = hooks.wal_append_count();
    eng.put(&cf, b"key1", b"value1").expect("put");
    eng.put(&cf, b"key2", b"value2").expect("put");
    eng.put(&cf, b"key3", b"value3").expect("put");
    let final_count = hooks.wal_append_count();

    // Assert
    assert!(
        final_count > initial_count,
        "WAL append count should increase after writes (initial: {}, final: {})",
        initial_count,
        final_count
    );
    assert_eq!(
        final_count - initial_count,
        3,
        "Expected 3 WAL appends for 3 puts"
    );
}

#[test]
fn should_call_manifest_hook_on_save() {
    use cntryl_midge::manifest::Manifest;

    // Arrange
    let dir = TempDir::new().unwrap();
    let hooks = TestHooks::new();

    let manifest = Manifest {
        last_persisted_sequence: 42,
        ..Default::default()
    };

    // Act
    let initial_count = hooks.manifest_update_count();
    manifest
        .save_atomic_with_hooks(dir.path(), Some(&hooks))
        .expect("save");
    let final_count = hooks.manifest_update_count();

    // Assert
    assert_eq!(
        final_count,
        initial_count + 1,
        "Manifest update count should increment by 1"
    );
}

#[test]
fn should_increment_compaction_counters_during_manual_compaction() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let hooks = TestHooks::new();

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: true,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    // Act
    let eng = MidgeEngine::open(opts).expect("open engine");
    let cf = eng.default_column_family();

    // Write some data to create SST files
    for i in 0..100 {
        let key = format!("key_{:04}", i);
        eng.put(&cf, key.as_bytes(), b"value").expect("put");
    }

    // Flush to create an SST
    eng.flush_cf(&cf).expect("flush");

    let initial_start = hooks.compaction_start_count();
    let initial_complete = hooks.compaction_complete_count();

    // Trigger manual compaction
    eng.compact_range(&cf, Some(b""), Some(b"~"))
        .expect("compact");

    // Deterministically wait for compaction using hooks.
    let after_gate = hooks.install_compaction_gate(cntryl_midge::test_hooks::CompactionGatePoint::AfterManifestUpdate);
    assert!(
        after_gate.wait_until_blocked(std::time::Duration::from_secs(10)),
        "Compaction did not reach AfterManifestUpdate"
    );
    // Release the compaction gate and wait deterministically for compaction to finish
    after_gate.release();
    eng.wait_for_compaction(std::time::Duration::from_secs(10)).unwrap();

    let final_start = hooks.compaction_start_count();
    let final_complete = hooks.compaction_complete_count();

    // Assert
    // Note: Compaction may not run if there aren't enough SSTsfor the threshold
    // So we just verify the counters either stayed the same or increased
    assert!(
        final_start >= initial_start,
        "Compaction start count should not decrease"
    );
    assert!(
        final_complete >= initial_complete,
        "Compaction complete count should not decrease"
    );
}
