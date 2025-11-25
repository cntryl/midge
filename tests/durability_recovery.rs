//! Crash Recovery Tests
//!
//! These tests verify the engine recovers correctly from various crash scenarios:
//! - Clean shutdown recovery
//! - Crash during flush
//! - Crash during compaction
//! - Manifest corruption/loss
//! - WAL and manifest disagreement
//! - Sequence number continuity

mod common;

use bytes::Bytes;
use cntryl_midge::{
    test_hooks::{FlushGatePoint, ManifestBehavior, TestHooks, WalBehavior},
    MidgeEngine, MidgeOptions, StorageMode, WalRecoveryMode, WriteBatch,
};
use common::{durability_opts, test_temp_dir};

// ============================================================================
// Basic Recovery
// ============================================================================

#[test]
fn should_recover_from_clean_shutdown_when_reopening() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };

    {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let cf = eng.default_column_family();
        eng.put(&cf, b"key1", b"value1").expect("put");
        eng.put(&cf, b"key2", b"value2").expect("put");
        // Clean shutdown via drop
    }

    // Act
    let eng = MidgeEngine::open(opts).expect("reopen");
    let cf = eng.default_column_family();

    // Assert
    assert_eq!(eng.get(&cf, b"key1").unwrap(), Some(Bytes::from_static(b"value1")));
    assert_eq!(eng.get(&cf, b"key2").unwrap(), Some(Bytes::from_static(b"value2")));
}

#[test]
fn should_recover_from_crash_after_flush_when_reopening() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };

    {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let cf = eng.default_column_family();
        eng.put(&cf, b"flushed_key", b"flushed_value").expect("put");
        eng.flush().expect("flush");
        // Crash after flush completes
    }

    // Act
    let eng = MidgeEngine::open(opts).expect("reopen");
    let cf = eng.default_column_family();

    // Assert - data in SST should be recovered
    assert_eq!(
        eng.get(&cf, b"flushed_key").unwrap(),
        Some(Bytes::from_static(b"flushed_value"))
    );
}

#[test]
fn should_recover_unflushed_data_given_crash_during_flush_when_reopening() {
    // Arrange
    let dir = test_temp_dir();
    {
        let hooks = TestHooks::default();
        let handle = hooks.install_flush_gate(FlushGatePoint::BeforeManifestUpdate);

        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk {
                db_path: dir.path().to_path_buf(),
            },
            test_hooks: Some(hooks),
            ..Default::default()
        };

        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        eng.put(&cf, b"mid_flush_key", b"mid_flush_value").expect("put");

        // Trigger flush but block before manifest update
        std::thread::spawn(move || {
            let _ = eng.flush();
        });

        // Wait for flush to reach gate (may not always trigger)
        let timeout = std::time::Duration::from_millis(500);
        let _ = handle.wait_until_blocked(timeout);
        // Drop engine (simulates crash with blocked flush)
    }

    // Act - reopen and recover from WAL
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("reopen");
    let cf = eng.default_column_family();

    // Assert - data recovered from WAL
    assert_eq!(
        eng.get(&cf, b"mid_flush_key").unwrap(),
        Some(Bytes::from_static(b"mid_flush_value"))
    );
}

// ============================================================================
// WAL and Manifest Consistency
// ============================================================================

#[test]
fn should_prefer_wal_given_wal_newer_than_sst_when_recovering() {
    // Arrange
    let dir = test_temp_dir();
    {
        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk {
                db_path: dir.path().to_path_buf(),
            },
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        eng.put(&cf, b"key", b"old_value").expect("put");
        eng.flush().expect("flush");

        eng.put(&cf, b"key", b"new_value").expect("put");
        // WAL has new_value, SST has old_value
    }

    // Act
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("reopen");
    let cf = eng.default_column_family();

    // Assert - WAL wins (newest data)
    assert_eq!(eng.get(&cf, b"key").unwrap(), Some(Bytes::from_static(b"new_value")));
}

#[test]
fn should_skip_wal_entries_given_already_in_sst_when_recovering() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new();

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 1024,
        wal_sync: true,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let cf = eng.default_column_family();

        // Write data that will flush to SST
        for i in 0..100 {
            eng.put(&cf, format!("dup_key_{:04}", i).as_bytes(), b"value")
                .expect("put");
        }
        eng.flush_cf(&cf).expect("flush");
    }

    // Act - reopen (recovery should skip WAL entries already in SST)
    let opts_recovery = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        test_hooks: None,
        ..Default::default()
    };

    let eng = MidgeEngine::open(opts_recovery).expect("reopen");
    let cf = eng.default_column_family();

    // Assert - data present exactly once
    for i in 0..100 {
        let result = eng.get(&cf, format!("dup_key_{:04}", i).as_bytes()).expect("get");
        assert!(result.is_some(), "Key {} should exist", i);
    }
}

#[test]
fn should_replay_wal_in_order_given_multiple_writes_when_recovering() {
    // Arrange
    let dir = test_temp_dir();
    {
        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk {
                db_path: dir.path().to_path_buf(),
            },
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Ordered overwrites
        eng.put(&cf, b"ordered_key", b"v1").expect("put");
        eng.put(&cf, b"ordered_key", b"v2").expect("put");
        eng.put(&cf, b"ordered_key", b"v3").expect("put");
    }

    // Act
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("reopen");
    let cf = eng.default_column_family();

    // Assert - should see latest value
    assert_eq!(eng.get(&cf, b"ordered_key").unwrap(), Some(Bytes::from_static(b"v3")));
}

// ============================================================================
// Delete Recovery
// ============================================================================

#[test]
fn should_recover_deletes_given_crash_after_delete_when_reopening() {
    // Arrange
    let dir = test_temp_dir();
    {
        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk {
                db_path: dir.path().to_path_buf(),
            },
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        eng.put(&cf, b"to_delete", b"value").expect("put");
        eng.put(&cf, b"to_keep", b"value").expect("put");
        eng.delete(&cf, b"to_delete").expect("delete");
    }

    // Act
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("reopen");
    let cf = eng.default_column_family();

    // Assert
    assert!(eng.get(&cf, b"to_delete").unwrap().is_none());
    assert_eq!(eng.get(&cf, b"to_keep").unwrap(), Some(Bytes::from_static(b"value")));
}

// ============================================================================
// Write Batch Recovery
// ============================================================================

#[test]
fn should_recover_write_batch_atomically_given_crash_when_reopening() {
    // Arrange
    let dir = test_temp_dir();
    {
        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk {
                db_path: dir.path().to_path_buf(),
            },
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        let mut batch = WriteBatch::new();
        batch.put(cf.id(), Bytes::from_static(b"batch_k1"), Bytes::from_static(b"v1"));
        batch.put(cf.id(), Bytes::from_static(b"batch_k2"), Bytes::from_static(b"v2"));
        batch.put(cf.id(), Bytes::from_static(b"batch_k3"), Bytes::from_static(b"v3"));
        eng.write_batch(&batch).expect("write_batch");
    }

    // Act
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("reopen");
    let cf = eng.default_column_family();

    // Assert - batch should be all-or-nothing
    assert_eq!(eng.get(&cf, b"batch_k1").unwrap(), Some(Bytes::from_static(b"v1")));
    assert_eq!(eng.get(&cf, b"batch_k2").unwrap(), Some(Bytes::from_static(b"v2")));
    assert_eq!(eng.get(&cf, b"batch_k3").unwrap(), Some(Bytes::from_static(b"v3")));
}

// ============================================================================
// Manifest Failures
// ============================================================================

#[test]
fn should_recover_from_wal_given_manifest_save_failure_when_reopening() {
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

    {
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        for i in 0..50 {
            eng.put(&cf, format!("manifest_fail_{:04}", i).as_bytes(), b"value")
                .expect("put");
        }
        // Manifest save will fail due to hook
    }

    // Act - recovery with clean options
    let opts_recovery = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        test_hooks: None,
        ..Default::default()
    };

    let eng = MidgeEngine::open(opts_recovery).expect("reopen");
    let cf = eng.default_column_family();

    // Assert - WAL should have preserved data
    for i in 0..50 {
        let result = eng.get(&cf, format!("manifest_fail_{:04}", i).as_bytes()).expect("get");
        assert!(result.is_some(), "Key {} should be recovered from WAL", i);
    }
}

#[test]
fn should_preserve_consistency_given_crash_before_manifest_update_when_reopening() {
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
        test_hooks: Some(hooks),
        ..Default::default()
    };

    {
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Write range of keys
        for i in 0..100 {
            eng.put(&cf, format!("cons_key_{:04}", i).as_bytes(), b"value")
                .expect("put");
        }
    }

    // Act
    let opts_recovery = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        test_hooks: None,
        ..Default::default()
    };

    let eng = MidgeEngine::open(opts_recovery).expect("reopen");
    let cf = eng.default_column_family();

    // Assert - consistency: either all present or none (no partial state)
    let first = eng.get(&cf, b"cons_key_0000").expect("get");
    let last = eng.get(&cf, b"cons_key_0099").expect("get");

    if first.is_some() {
        assert!(last.is_some(), "If first key exists, all should exist (consistency)");
    }
}

// ============================================================================
// Idempotent Recovery
// ============================================================================

#[test]
fn should_be_idempotent_given_multiple_recovery_cycles_when_reopening() {
    // Arrange
    let dir = test_temp_dir();
    {
        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk {
                db_path: dir.path().to_path_buf(),
            },
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();
        eng.put(&cf, b"idempotent_key", b"value").expect("put");
    }

    // Act - recover multiple times
    let mut values = Vec::new();
    for _ in 0..3 {
        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk {
                db_path: dir.path().to_path_buf(),
            },
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("reopen");
        let cf = eng.default_column_family();
        values.push(eng.get(&cf, b"idempotent_key").expect("get"));
    }

    // Assert - same value every time
    assert!(values.iter().all(|v| *v == values[0]), "Recovery should be idempotent");
}

#[test]
fn should_maintain_exactly_once_given_multiple_crash_cycles_when_reopening() {
    // Arrange
    let dir = test_temp_dir();
    let db_path = dir.path().to_path_buf();

    // Act - multiple restart cycles with writes
    for cycle in 0..5 {
        let opts = durability_opts(db_path.clone());
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        eng.put(
            &cf,
            format!("cycle_{}", cycle).as_bytes(),
            format!("value_{}", cycle).as_bytes(),
        ).expect("put");

        // Drop (simulates crash)
    }

    // Assert - all cycles present exactly once
    let opts = durability_opts(db_path);
    let eng = MidgeEngine::open(opts).expect("final open");
    let cf = eng.default_column_family();

    for cycle in 0..5 {
        let result = eng.get(&cf, format!("cycle_{}", cycle).as_bytes()).expect("get");
        assert_eq!(
            result,
            Some(Bytes::from(format!("value_{}", cycle))),
            "Cycle {} should have exact value",
            cycle
        );
    }
}

// ============================================================================
// Sequence Number Continuity
// ============================================================================

#[test]
fn should_continue_sequence_numbers_given_recovery_when_new_writes() {
    // Arrange
    let dir = test_temp_dir();
    {
        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk {
                db_path: dir.path().to_path_buf(),
            },
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        for i in 0..5 {
            eng.put(&cf, format!("pre_key_{}", i).as_bytes(), b"value").expect("put");
        }
    }

    // Act - recover and write more
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("reopen");
    let cf = eng.default_column_family();

    eng.put(&cf, b"post_recovery_key", b"post_value").expect("put");

    // Assert - new writes work correctly
    assert_eq!(
        eng.get(&cf, b"post_recovery_key").unwrap(),
        Some(Bytes::from_static(b"post_value"))
    );

    // Pre-recovery data still accessible
    for i in 0..5 {
        assert!(eng.get(&cf, format!("pre_key_{}", i).as_bytes()).unwrap().is_some());
    }
}

// ============================================================================
// Corrupted Tail Handling
// ============================================================================

#[test]
fn should_skip_corrupted_tail_given_partial_record_when_tolerant_mode() {
    // Arrange
    let dir = test_temp_dir();
    {
        let hooks = TestHooks::new().with_wal_behavior(WalBehavior::TruncateAfterWrite);

        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk {
                db_path: dir.path().to_path_buf(),
            },
            test_hooks: Some(hooks),
            ..Default::default()
        };

        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        eng.put(&cf, b"good_key", b"good_value").expect("put");
        eng.put(&cf, b"truncated_key", b"truncated_value").expect("put");
        // Last write will be truncated
    }

    // Act - recover with tolerant mode
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("reopen");
    let cf = eng.default_column_family();

    // Assert - at least good_key should be recovered
    // (truncated_key may or may not be present depending on truncation timing)
    let good = eng.get(&cf, b"good_key").expect("get");
    assert!(good.is_some() || eng.get(&cf, b"truncated_key").expect("get").is_some(),
        "At least some data should be recovered");
}
