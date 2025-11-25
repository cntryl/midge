use cntryl_midge::test_hooks::{TestHooks, WalBehavior};
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use tempfile::TempDir;

// This test verifies that when the WAL is truncated immediately after an
// append (simulated by TestHooks::WalBehavior::TruncateAfterWrite), the
// appended record is not visible after reopening the engine (i.e., recovery
// does not see torn/uncommitted data).

#[test]
fn should_not_recover_truncated_wal_append() {
    // Arrange
    let dir = TempDir::new().unwrap();
    // Use the simulated-failure behavior so this test is deterministic
    // on platforms where truncating an open file may fail (Windows).
    let hooks = TestHooks::new().with_wal_behavior(WalBehavior::TruncateAfterWriteFail);

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_sync: false,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    // Act
    {
        // Open engine with truncation behavior enabled so the WAL will be
        // truncated right after the append.
        let eng = MidgeEngine::open(opts).expect("open engine");
        let cf = eng.default_column_family();

        // Single put which will be truncated by the hook
        eng.put(&cf, b"trunc_key", b"trunc_value").expect("put");

        // Drop engine to force close and then reopen for recovery
    }

    // Reopen engine with no hooks (normal behavior) to perform recovery.
    let opts_reopen = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        test_hooks: None,
        ..Default::default()
    };

    // Act (reopen)
    let eng2 = MidgeEngine::open(opts_reopen).expect("reopen engine");
    let cf2 = eng2.default_column_family();

    // Assert
    assert_eq!(eng2.get(&cf2, b"trunc_key").expect("get"), None);
}
