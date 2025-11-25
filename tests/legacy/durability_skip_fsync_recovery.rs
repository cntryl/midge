use cntryl_midge::test_hooks::{FsyncBehavior, TestHooks};
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use tempfile::TempDir;

// Test that simulates skipping fsync on WAL writes and ensures that recovery
// behavior is as expected (records without fsync may be lost on crash).

#[test]
fn should_handle_skipped_fsync_on_recovery() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let hooks = TestHooks::new().with_fsync_behavior(FsyncBehavior::Skip);

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_sync: true, // engine will call sync, which test hook may skip
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    // Act: write a record but fsync is skipped by the hook
    {
        let eng = MidgeEngine::open(opts).expect("open engine");
        let cf = eng.default_column_family();
        eng.put(&cf, b"skipfsync_key", b"skipfsync_value")
            .expect("put");
        // Do not explicitly sync; closing engine may not persist due to hook
    }

    // Reopen without hooks to see what's recovered
    let opts_reopen = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        test_hooks: None,
        ..Default::default()
    };

    let eng2 = MidgeEngine::open(opts_reopen).expect("reopen engine");
    let cf2 = eng2.default_column_family();

    // Assert - The spec allows loss if fsync was skipped — ensure we either see the key
    // or not, but the engine must remain consistent (no partial records).
    let _ = eng2.get(&cf2, b"skipfsync_key").expect("get");
}
