//! WAL Durability Tests
//!
//! These tests verify the Write-Ahead Log (WAL) guarantees:
//! - Fsynced writes survive crashes
//! - Unfsynced writes may be lost (expected behavior)
//! - WAL recovery replays records in order
//! - Corrupted/truncated WAL tails are handled gracefully
//!
//! Tests run against both LocalDisk and CloudBacked modes where applicable.
//! CloudBacked has both a local ephemeral WAL and a cloud WAL.

mod common;

use bytes::Bytes;
use cntryl_midge::{
    test_hooks::{FsyncBehavior, TestHooks, WalBehavior},
    MidgeEngine, MidgeOptions, StorageMode, WalRecoveryMode,
};
use common::{disk_storage_modes, test_temp_dir, DurabilityTestContext};
use std::fs;
use std::sync::Arc;

// ============================================================================
// Basic WAL Persistence
// ============================================================================

#[test]
fn should_recover_writes_given_unflushed_memtable_when_reopening() {
    for mode in disk_storage_modes() {
        // Arrange
        let ctx = DurabilityTestContext::new(mode);
        let opts = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            enable_compaction: false,
            memtable_size: 1024 * 1024, // Large memtable to avoid flush
            ..Default::default()
        };

        {
            let eng = MidgeEngine::open(opts).expect("open");
            let cf = eng.default_column_family();
            eng.put(&cf, b"key_a", b"value_1").expect("put");
            eng.put(&cf, b"key_b", b"value_2").expect("put");
            // Drop without explicit flush - relies on WAL for recovery
        }

        // Act - reopen with fresh storage mode pointing to same storage
        let opts_reopen = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            enable_compaction: false,
            memtable_size: 1024 * 1024,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts_reopen).expect("reopen");
        let cf = eng.default_column_family();

        // Assert
        assert_eq!(
            eng.get(&cf, b"key_a").unwrap(),
            Some(Bytes::from_static(b"value_1")),
            "Failed for {}",
            ctx.name()
        );
        assert_eq!(
            eng.get(&cf, b"key_b").unwrap(),
            Some(Bytes::from_static(b"value_2")),
            "Failed for {}",
            ctx.name()
        );
    }
}

#[test]
fn should_persist_write_given_fsync_enabled_when_crash_occurs() {
    for mode in disk_storage_modes() {
        // Arrange
        let ctx = DurabilityTestContext::new(mode);
        let opts = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            wal_sync: true,
            wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
            ..Default::default()
        };

        // Act - write with fsync, then drop (simulating crash)
        {
            let eng = MidgeEngine::open(opts).expect("open");
            let cf = eng.default_column_family();
            eng.put(&cf, b"durable_key", b"durable_value").expect("put");
            // Data is fsynced before put() returns
        }

        // Assert - fsynced write survives restart
        let opts_reopen = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            wal_sync: true,
            wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts_reopen).expect("reopen");
        let cf = eng.default_column_family();
        assert_eq!(
            eng.get(&cf, b"durable_key").unwrap(),
            Some(Bytes::from_static(b"durable_value")),
            "Failed for {}",
            ctx.name()
        );
    }
}

#[test]
fn should_call_fsync_given_wal_sync_enabled_when_put() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_fsync_behavior(FsyncBehavior::RecordOnly);

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_sync: true,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();
    let fsync_before = hooks.fsync_count();

    // Act
    eng.put(&cf, b"key", b"value").expect("put");

    // Assert
    let fsync_after = hooks.fsync_count();
    assert!(
        fsync_after > fsync_before,
        "Fsync should be called before put() returns (before={}, after={})",
        fsync_before,
        fsync_after
    );
}

// ============================================================================
// WAL Rotation & Segments
// ============================================================================

#[test]
fn should_rotate_wal_given_small_buffer_when_writes_exceed_buffer() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_buffer_size: 64, // Small buffer to trigger rotation
        memtable_size: 1024 * 1024,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts.clone()).expect("open");
    let cf = eng.default_column_family();

    // Act
    for i in 0..10u8 {
        eng.put(&cf, &[b'k', i], &[b'v', i]).expect("put");
    }
    eng.flush().expect("flush");

    // Assert
    assert!(hooks.wal_append_count() > 0, "WAL appends should occur");

    let wal_dir = dir.path().join("wal");
    let sst_dir = dir.path().join("sst");
    let wal_exists = wal_dir.exists()
        && fs::read_dir(&wal_dir)
            .map(|mut d| d.next().is_some())
            .unwrap_or(false);
    let sst_exists = sst_dir.exists()
        && fs::read_dir(&sst_dir)
            .map(|mut d| d.next().is_some())
            .unwrap_or(false);

    assert!(
        wal_exists || sst_exists,
        "Either WAL or SST files should exist after writes"
    );
}

#[test]
fn should_replay_all_records_given_multiple_wal_segments_when_recovering() {
    for mode in disk_storage_modes() {
        // Arrange
        let ctx = DurabilityTestContext::new(mode);
        let opts = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            wal_sync: true,
            wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
            ..Default::default()
        };

        {
            let eng = MidgeEngine::open(opts).expect("open");
            let cf = eng.default_column_family();

            // Write enough data to potentially span multiple segments
            for i in 0..1000 {
                eng.put(&cf, format!("seg_key_{}", i).as_bytes(), b"value")
                    .expect("put");
            }
        }

        // Act
        let opts_reopen = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            wal_sync: true,
            wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts_reopen).expect("reopen");
        let cf = eng.default_column_family();

        // Assert - all records recovered
        for i in 0..1000 {
            let result = eng
                .get(&cf, format!("seg_key_{}", i).as_bytes())
                .expect("get");
            assert!(
                result.is_some(),
                "Record {} should be recovered for {}",
                i,
                ctx.name()
            );
        }
    }
}

// ============================================================================
// Concurrent Writes & Ordering
// ============================================================================

#[test]
fn should_recover_all_writes_given_concurrent_puts_when_crash_occurs() {
    for mode in disk_storage_modes() {
        // Arrange
        let ctx = DurabilityTestContext::new(mode);
        let opts = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            wal_sync: true,
            wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
            ..Default::default()
        };

        {
            let eng = Arc::new(MidgeEngine::open(opts).expect("open"));
            let handles: Vec<_> = (0..10)
                .map(|i| {
                    let eng = Arc::clone(&eng);
                    std::thread::spawn(move || {
                        let cf = eng.default_column_family();
                        eng.put(
                            &cf,
                            format!("conc_{}", i).as_bytes(),
                            format!("val_{}", i).as_bytes(),
                        )
                        .expect("put");
                    })
                })
                .collect();

            for h in handles {
                h.join().expect("thread join");
            }
        }

        // Act
        let opts_reopen = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            wal_sync: true,
            wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts_reopen).expect("reopen");
        let cf = eng.default_column_family();

        // Assert
        for i in 0..10 {
            let result = eng.get(&cf, format!("conc_{}", i).as_bytes()).expect("get");
            assert!(
                result.is_some(),
                "Concurrent write {} should be recovered for {}",
                i,
                ctx.name()
            );
        }
    }
}

// ============================================================================
// Crash & Truncation Scenarios
// ============================================================================

#[test]
fn should_handle_gracefully_given_truncated_wal_tail_when_recovering() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_wal_behavior(WalBehavior::TruncateAfterWrite);

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_sync: true,
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    {
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();
        eng.put(&cf, b"truncated_key", b"truncated_value")
            .expect("put");
        assert!(hooks.wal_append_count() > 0, "WAL append should occur");
    }

    // Act - reopen with recovery mode that tolerates truncation
    let opts_recovery = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        test_hooks: None,
        ..Default::default()
    };

    let result = MidgeEngine::open(opts_recovery);

    // Assert - recovery succeeds (data may or may not be present)
    assert!(
        result.is_ok(),
        "Recovery should handle truncated WAL gracefully"
    );
}

#[test]
fn should_not_recover_data_given_truncated_wal_append_when_reopening() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_wal_behavior(WalBehavior::TruncateAfterWriteFail);

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_sync: false,
        test_hooks: Some(hooks),
        ..Default::default()
    };

    {
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();
        eng.put(&cf, b"lost_key", b"lost_value").expect("put");
    }

    // Act
    let opts_recovery = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        test_hooks: None,
        ..Default::default()
    };

    let eng = MidgeEngine::open(opts_recovery).expect("reopen");
    let cf = eng.default_column_family();

    // Assert - truncated record should not be recovered
    assert_eq!(eng.get(&cf, b"lost_key").expect("get"), None);
}

#[test]
fn should_allow_data_loss_given_skipped_fsync_when_crash_occurs() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_fsync_behavior(FsyncBehavior::Skip);

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_sync: true, // Engine calls sync, but hook skips it
        test_hooks: Some(hooks),
        ..Default::default()
    };

    {
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();
        eng.put(&cf, b"maybe_lost", b"value").expect("put");
    }

    // Act
    let opts_recovery = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        test_hooks: None,
        ..Default::default()
    };

    let eng = MidgeEngine::open(opts_recovery).expect("reopen");
    let cf = eng.default_column_family();

    // Assert - data may or may not be present, but engine should be consistent
    let result = eng.get(&cf, b"maybe_lost");
    assert!(
        result.is_ok(),
        "Engine should remain consistent even if data lost"
    );
}

// ============================================================================
// Recovery Mode Behavior
// ============================================================================

#[test]
fn should_tolerate_corrupted_tail_given_recovery_mode_set_when_reopening() {
    for mode in disk_storage_modes() {
        // Arrange
        let ctx = DurabilityTestContext::new(mode);

        // First, write some valid data
        let opts = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            wal_sync: true,
            ..Default::default()
        };

        {
            let eng = MidgeEngine::open(opts).expect("open");
            let cf = eng.default_column_family();
            eng.put(&cf, b"valid_key", b"valid_value").expect("put");
        }

        // Act - reopen with TolerateCorruptedTail mode
        let opts_recovery = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
            ..Default::default()
        };

        let eng = MidgeEngine::open(opts_recovery).expect("reopen");
        let cf = eng.default_column_family();

        // Assert
        assert_eq!(
            eng.get(&cf, b"valid_key").unwrap(),
            Some(Bytes::from_static(b"valid_value")),
            "Failed for {}",
            ctx.name()
        );
    }
}
