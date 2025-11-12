use cntryl_midge::test_hooks::{TestHooks, WalBehavior};
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use tempfile::TempDir;

// End-to-end engine test: configure engine to use TestHooks that request WAL
// truncation and force the simulated failing-truncate fallback. Verify that
// after restart the torn append is not visible (recovery stops at valid tail).

#[test]
fn should_not_recover_truncated_wal_append_at_engine_level() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let hooks = TestHooks::new().with_wal_behavior(WalBehavior::TruncateAfterWriteFail);

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_sync: false,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    // Act: open engine, write a single key that will be truncated
    {
        let eng = MidgeEngine::open(opts).expect("open engine");
        let cf = eng.default_column_family();
        eng.put(&cf, b"eng_trunc_key", b"eng_trunc_value")
            .expect("put");
        // Drop engine to flush/close
    }

    // Reopen with normal options (no hooks) to recover
    let opts_reopen = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        test_hooks: None,
        ..Default::default()
    };

    let eng2 = MidgeEngine::open(opts_reopen).expect("reopen engine");
    let cf2 = eng2.default_column_family();

    // Assert: truncated record should not be present
    assert_eq!(eng2.get(&cf2, b"eng_trunc_key").expect("get"), None);
}
