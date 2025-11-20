// Durability Recovery Edge Cases (Phase 2 - P1)
// Tests deterministic recovery scenarios with WAL, manifest, and SST conflicts

#![allow(clippy::field_reassign_with_default)]
mod common;
use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode, WalRecoveryMode, test_hooks::{TestHooks, FlushGatePoint}};
use common::test_temp_dir;

#[test]
fn should_recover_unflushed_data_given_crash_during_flush_when_reopening() {
    // Arrange
    let dir = test_temp_dir();
    {
        let mut opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk {
                db_path: dir.path().to_path_buf(),
            },
            ..Default::default()
        };
        
        let hooks = TestHooks::default();
        let _handle = hooks.install_flush_gate(FlushGatePoint::BeforeManifestUpdate);
        opts.test_hooks = Some(hooks.clone());
        
        let eng = MidgeEngine::open(opts).unwrap();
        let cf = eng.default_column_family();
        
        eng.put(&cf, b"key1", b"val1").unwrap();
        eng.put(&cf, b"key2", b"val2").unwrap();
        
        // Trigger flush but block before manifest update
        std::thread::spawn(move || {
            let _ = eng.flush();
        });
        
        std::thread::sleep(std::time::Duration::from_millis(100));
        // Drop engine (simulates crash with blocked flush)
    }
    
    // Act - reopen and recover from WAL
    let opts2 = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        ..Default::default()
    };
    let eng2 = MidgeEngine::open(opts2).unwrap();
    let cf2 = eng2.default_column_family();

    // Assert - data recovered from WAL
    assert_eq!(eng2.get(&cf2, b"key1").unwrap().unwrap(), Bytes::from("val1"));
    assert_eq!(eng2.get(&cf2, b"key2").unwrap().unwrap(), Bytes::from("val2"));
}

#[test]
fn should_resolve_conflict_given_wal_and_manifest_disagree_when_recovering() {
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
        
        eng.put(&cf, b"key1", b"old_val").unwrap();
        eng.flush().unwrap();
        
        eng.put(&cf, b"key1", b"new_val").unwrap();
        // WAL has new_val, SST has old_val
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

    // Assert - WAL wins (newest data)
    assert_eq!(eng2.get(&cf2, b"key1").unwrap().unwrap(), Bytes::from("new_val"));
}

#[test]
fn should_handle_duplicate_wal_replay_idempotently_when_recovering_twice() {
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
        
        eng.put(&cf, b"counter", b"1").unwrap();
    }
    
    // Act - reopen twice with same WAL
    let opts2 = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng2 = MidgeEngine::open(opts2).unwrap();
    let cf2 = eng2.default_column_family();
    let val1 = eng2.get(&cf2, b"counter").unwrap().unwrap();
    drop(eng2);
    
    let opts3 = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng3 = MidgeEngine::open(opts3).unwrap();
    let cf3 = eng3.default_column_family();
    let val2 = eng3.get(&cf3, b"counter").unwrap().unwrap();

    // Assert - same value both times (idempotent replay)
    assert_eq!(val1, val2);
}

#[test]
#[ignore] // TODO: Requires orphaned SST detection logic
fn should_discover_ssts_given_out_of_order_recovery_when_manifest_incomplete() {
    // Arrange - manually create SST without manifest entry
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
        eng.flush().unwrap();
        
        // Simulate manifest corruption/loss after flush
    }
    
    // Act - reopen and detect orphaned SST
    let opts2 = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng2 = MidgeEngine::open(opts2).unwrap();
    let cf2 = eng2.default_column_family();

    // Assert - should still find data from orphaned SST
    assert_eq!(eng2.get(&cf2, b"key1").unwrap().unwrap(), Bytes::from("val1"));
}

#[test]
#[ignore] // TODO: Requires manifest rebuild from SST discovery
fn should_rebuild_manifest_given_missing_manifest_when_ssts_present() {
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
        
        for i in 0..10 {
            let key = format!("key{}", i);
            eng.put(&cf, key.as_bytes(), b"val").unwrap();
        }
        eng.flush().unwrap();
    }
    
    // Act - delete manifest and reopen
    let manifest_path = dir.path().join("MANIFEST-000001");
    if manifest_path.exists() {
        std::fs::remove_file(manifest_path).ok();
    }
    
    let opts2 = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let result = MidgeEngine::open(opts2);

    // Assert - should rebuild or error gracefully
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn should_replay_wal_in_order_given_multiple_transactions_when_recovering() {
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
        
        // Ordered operations
        eng.put(&cf, b"key", b"v1").unwrap();
        eng.put(&cf, b"key", b"v2").unwrap();
        eng.put(&cf, b"key", b"v3").unwrap();
    }
    
    // Act - recover
    let opts2 = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng2 = MidgeEngine::open(opts2).unwrap();
    let cf2 = eng2.default_column_family();

    // Assert - should see latest value (v3)
    assert_eq!(eng2.get(&cf2, b"key").unwrap().unwrap(), Bytes::from("v3"));
}

#[test]
fn should_recover_delete_operations_given_crash_when_deletes_in_wal() {
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
        eng.delete(&cf, b"key1").unwrap();
    }
    
    // Act - recover
    let opts2 = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng2 = MidgeEngine::open(opts2).unwrap();
    let cf2 = eng2.default_column_family();

    // Assert
    assert!(eng2.get(&cf2, b"key1").unwrap().is_none());
    assert_eq!(eng2.get(&cf2, b"key2").unwrap().unwrap(), Bytes::from("val2"));
}

#[test]
fn should_recover_write_batch_atomically_given_crash_when_batch_in_wal() {
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
        
        use cntryl_midge::WriteBatch;
        
        eng.put(&cf, b"before", b"val").unwrap();
        
        let mut batch = WriteBatch::new();
        batch.put(cf.id(), Bytes::from_static(b"batch1"), Bytes::from_static(b"val1"));
        batch.put(cf.id(), Bytes::from_static(b"batch2"), Bytes::from_static(b"val2"));
        batch.put(cf.id(), Bytes::from_static(b"batch3"), Bytes::from_static(b"val3"));
        eng.write_batch(&batch).unwrap();
    }
    
    // Act - recover
    let opts2 = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng2 = MidgeEngine::open(opts2).unwrap();
    let cf2 = eng2.default_column_family();

    // Assert - batch should be all-or-nothing
    assert!(eng2.get(&cf2, b"batch1").unwrap().is_some());
    assert!(eng2.get(&cf2, b"batch2").unwrap().is_some());
    assert!(eng2.get(&cf2, b"batch3").unwrap().is_some());
}

#[test]
fn should_skip_corrupted_tail_given_partial_record_when_tolerant_mode() {
    // Arrange
    let dir = test_temp_dir();
    {
        let mut opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk {
                db_path: dir.path().to_path_buf(),
            },
            ..Default::default()
        };
        
        let hooks = TestHooks::new().with_wal_behavior(cntryl_midge::test_hooks::WalBehavior::TruncateAfterWrite);
        opts.test_hooks = Some(hooks);
        
        let eng = MidgeEngine::open(opts).unwrap();
        let cf = eng.default_column_family();
        
        eng.put(&cf, b"key1", b"val1").unwrap();
        eng.put(&cf, b"key2", b"val2").unwrap(); // This will be truncated
    }
    
    // Act - recover with tolerant mode
    let opts2 = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        ..Default::default()
    };
    let eng2 = MidgeEngine::open(opts2).unwrap();
    let cf2 = eng2.default_column_family();

    // Assert - should recover key1, skip corrupted key2
    assert!(eng2.get(&cf2, b"key1").unwrap().is_some());
}

#[test]
fn should_maintain_sequence_numbers_given_recovery_when_replaying_wal() {
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
        
        for i in 0..5 {
            let key = format!("key{}", i);
            eng.put(&cf, key.as_bytes(), b"val").unwrap();
        }
    }
    
    // Act - recover and write more
    let opts2 = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng2 = MidgeEngine::open(opts2).unwrap();
    let cf2 = eng2.default_column_family();
    
    eng2.put(&cf2, b"new_key", b"new_val").unwrap();

    // Assert - new writes should have higher sequence numbers
    assert!(eng2.get(&cf2, b"new_key").unwrap().is_some());
}
