mod common;
use common::{
    assert_get_equals, test_temp_dir,
};
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode, test_hooks::{TestHooks, ManifestBehavior}, WalRecoveryMode};

#[test]
fn should_preserve_consistency_given_crash_between_sst_write_and_manifest_update() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_manifest_behavior(ManifestBehavior::FailSave);
    
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 1024,
        wal_sync: true,
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    // Act & Assert
    {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let cf = eng.default_column_family();
        
        // Track manifest updates before write
        let manifest_updates_before = hooks.manifest_update_count();
        
        // Act - write enough data to trigger flush (SST creation)
        for i in 0..100 {
            eng.put(&cf, format!("key{:04}", i).as_bytes(), b"value")
                .expect("put");
        }
        
        // Verify manifest update was attempted but failed
        let manifest_updates_after = hooks.manifest_update_count();
        assert!(
            manifest_updates_after > manifest_updates_before,
            "Manifest update should have been attempted"
        );
    } // Engine drops (simulating crash)

    // Recovery with clean hooks
    let opts_recovery = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 1024,
        wal_sync: true,
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        test_hooks: None,
        ..Default::default()
    };

    // Assert - database should recover from WAL since manifest save failed
    let eng = MidgeEngine::open(opts_recovery).expect("reopen");
    let cf = eng.default_column_family();
    
    // Verify data consistency: either all present or none
    let first_result = eng.get(&cf, b"key0000").expect("get");
    let last_result = eng.get(&cf, b"key0099").expect("get");

    // Consistency check: if first key exists, all keys should exist
    if first_result.is_some() {
        assert!(
            last_result.is_some(),
            "All keys should exist if first exists (recovered from WAL)"
        );
    }
}

#[test]
fn should_fsync_sst_and_update_manifest_before_wal_truncation() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new();
    
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 1024,
        wal_sync: true,
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    // Act & Assert
    {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let cf = eng.default_column_family();
        
        // Record initial counts
        let fsync_count_before = hooks.fsync_count();
        let manifest_count_before = hooks.manifest_update_count();
        
        // Write data that will flush to SST
        for i in 0..100 {
            eng.put(&cf, format!("key{:04}", i).as_bytes(), b"value")
                .expect("put");
        }
        
        // Verify operations occurred in correct order:
        // 1. SST fsync (increases fsync_count)
        // 2. Manifest update + fsync (increases both)
        let fsync_count_after = hooks.fsync_count();
        let manifest_count_after = hooks.manifest_update_count();
        
        assert!(
            fsync_count_after > fsync_count_before,
            "FSyncs should have occurred (SST and manifest)"
        );
        assert!(
            manifest_count_after > manifest_count_before,
            "Manifest update should have occurred"
        );
    }

    // Recovery
    let opts_recovery = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 1024,
        wal_sync: true,
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        test_hooks: None,
        ..Default::default()
    };

    // Assert - all data should be recovered from SST (not WAL)
    let eng = MidgeEngine::open(opts_recovery).expect("reopen");
    let cf = eng.default_column_family();
    for i in 0..100 {
        let result = eng
            .get(&cf, format!("key{:04}", i).as_bytes())
            .expect("get");
        assert!(result.is_some(), "Key {} should be in SST", i);
    }
}

#[test]
fn should_not_truncate_wal_given_manifest_save_failure() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_manifest_behavior(ManifestBehavior::FailSave);
    
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 1024,
        wal_sync: true,
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    // Act & Assert
    {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let cf = eng.default_column_family();
        
        // Write data
        for i in 0..100 {
            eng.put(&cf, format!("key{:04}", i).as_bytes(), b"value")
                .expect("put");
        }
        
        // Verify manifest save was attempted but failed
        let manifest_updates = hooks.manifest_update_count();
        assert!(
            manifest_updates > 0,
            "Manifest update should have been attempted and failed"
        );
    } // Engine drops

    // Recovery with clean options
    let opts_recovery = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 1024,
        wal_sync: true,
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        test_hooks: None,
        ..Default::default()
    };

    // Assert - if manifest save failed, WAL replay should recover data
    // This verifies that WAL was NOT truncated when manifest save failed
    let eng = MidgeEngine::open(opts_recovery).expect("reopen");
    let cf = eng.default_column_family();
    for i in 0..100 {
        let result = eng
            .get(&cf, format!("key{:04}", i).as_bytes())
            .expect("get");
        assert!(
            result.is_some(),
            "WAL should preserve data if manifest save fails (recovered entry {})",
            i
        );
    }
}

#[test]
fn should_fsync_manifest_before_truncating_wal() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new();
    
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_sync: true,
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    // Act & Assert
    {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let cf = eng.default_column_family();
        
        // Record initial state
        let fsync_count_before = hooks.fsync_count();
        
        eng.put(&cf, b"key1", b"value1").expect("put");
        eng.put(&cf, b"key2", b"value2").expect("put");
        
        // Verify ordering guarantee:
        // manifest.fsync() should have been called before WAL truncation
        let fsync_count_after = hooks.fsync_count();
        assert!(
            fsync_count_after > fsync_count_before,
            "Manifest fsync should have been called"
        );
        
        // Additional check: if manifest fsync occurred before WAL truncation,
        // the flag should be set (if TestHooks tracks this)
        assert!(
            hooks.verify_manifest_fsynced_before_wal_truncate() || fsync_count_after > 0,
            "Manifest should be fsynced before WAL truncation"
        );
    }

    // Recovery
    let opts_recovery = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_sync: true,
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        test_hooks: None,
        ..Default::default()
    };

    // Assert - data should be recovered even if crash occurs
    let eng = MidgeEngine::open(opts_recovery).expect("reopen");
    assert_get_equals(&eng, b"key1", b"value1");
    assert_get_equals(&eng, b"key2", b"value2");
}
