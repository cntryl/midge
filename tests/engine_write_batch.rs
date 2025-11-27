//! WriteBatch Atomicity Tests
//!
//! These tests verify WriteBatch guarantees:
//! - Atomicity: All operations commit together or none do
//! - Ordering: Operations apply in batch order
//! - Durability: Batches persist across restarts
//! - Isolation: Batches don't interleave with other operations

mod common;

use bytes::Bytes;
use cntryl_midge::{
    test_hooks::{TestHooks, WalBehavior},
    ColumnFamilyConfig, MidgeEngine, MidgeOptions, StorageMode, WalRecoveryMode, WriteBatch,
};
use common::test_temp_dir;
use std::sync::Arc;

// ============================================================================
// Basic Batch Operations
// ============================================================================

#[test]
fn should_commit_all_operations_given_batch_when_write_batch() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    let mut batch = WriteBatch::new();
    batch.put(
        cf.id(),
        Bytes::from_static(b"k1"),
        Bytes::from_static(b"v1"),
    );
    batch.put(
        cf.id(),
        Bytes::from_static(b"k2"),
        Bytes::from_static(b"v2"),
    );
    batch.put(
        cf.id(),
        Bytes::from_static(b"k3"),
        Bytes::from_static(b"v3"),
    );

    // Act
    engine.write_batch(&batch).expect("write_batch");

    // Assert
    assert_eq!(
        engine.get(&cf, b"k1").unwrap(),
        Some(Bytes::from_static(b"v1"))
    );
    assert_eq!(
        engine.get(&cf, b"k2").unwrap(),
        Some(Bytes::from_static(b"v2"))
    );
    assert_eq!(
        engine.get(&cf, b"k3").unwrap(),
        Some(Bytes::from_static(b"v3"))
    );
}

#[test]
fn should_apply_last_value_given_duplicate_keys_when_write_batch() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    let mut batch = WriteBatch::new();
    batch.put(
        cf.id(),
        Bytes::from_static(b"key"),
        Bytes::from_static(b"v1"),
    );
    batch.put(
        cf.id(),
        Bytes::from_static(b"key"),
        Bytes::from_static(b"v2"),
    );
    batch.put(
        cf.id(),
        Bytes::from_static(b"key"),
        Bytes::from_static(b"v3"),
    );

    // Act
    engine.write_batch(&batch).expect("write_batch");

    // Assert - last write wins
    assert_eq!(
        engine.get(&cf, b"key").unwrap(),
        Some(Bytes::from_static(b"v3"))
    );
}

#[test]
fn should_succeed_given_empty_batch_when_write_batch() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let batch = WriteBatch::new();

    // Act
    let result = engine.write_batch(&batch);

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_delete_key_given_delete_after_put_when_write_batch() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    let mut batch = WriteBatch::new();
    batch.put(
        cf.id(),
        Bytes::from_static(b"key"),
        Bytes::from_static(b"value"),
    );
    batch.delete(cf.id(), Bytes::from_static(b"key"));

    // Act
    engine.write_batch(&batch).expect("write_batch");

    // Assert
    assert_eq!(engine.get(&cf, b"key").unwrap(), None);
}

#[test]
fn should_delete_existing_key_given_delete_in_batch_when_write_batch() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    engine.put(&cf, b"existing", b"old_value").expect("put");

    let mut batch = WriteBatch::new();
    batch.delete(cf.id(), Bytes::from_static(b"existing"));

    // Act
    engine.write_batch(&batch).expect("write_batch");

    // Assert
    assert_eq!(engine.get(&cf, b"existing").unwrap(), None);
}

#[test]
fn should_overwrite_existing_value_given_put_in_batch_when_write_batch() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    engine.put(&cf, b"key", b"old_value").expect("put");

    let mut batch = WriteBatch::new();
    batch.put(
        cf.id(),
        Bytes::from_static(b"key"),
        Bytes::from_static(b"new_value"),
    );

    // Act
    engine.write_batch(&batch).expect("write_batch");

    // Assert
    assert_eq!(
        engine.get(&cf, b"key").unwrap(),
        Some(Bytes::from_static(b"new_value"))
    );
}

// ============================================================================
// Mixed Operations
// ============================================================================

#[test]
fn should_apply_mixed_operations_in_order_when_write_batch() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    let mut batch = WriteBatch::new();
    batch.put(
        cf.id(),
        Bytes::from_static(b"k1"),
        Bytes::from_static(b"v1"),
    );
    batch.delete(cf.id(), Bytes::from_static(b"k2"));
    batch.put(
        cf.id(),
        Bytes::from_static(b"k3"),
        Bytes::from_static(b"v3"),
    );
    batch.put(
        cf.id(),
        Bytes::from_static(b"k1"),
        Bytes::from_static(b"updated"),
    );

    // Act
    engine.write_batch(&batch).expect("write_batch");

    // Assert
    assert_eq!(
        engine.get(&cf, b"k1").unwrap(),
        Some(Bytes::from_static(b"updated"))
    );
    assert_eq!(engine.get(&cf, b"k2").unwrap(), None);
    assert_eq!(
        engine.get(&cf, b"k3").unwrap(),
        Some(Bytes::from_static(b"v3"))
    );
}

// ============================================================================
// Large Batches
// ============================================================================

#[test]
fn should_handle_large_batch_given_many_operations_when_write_batch() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    let mut batch = WriteBatch::with_capacity(1000);
    for i in 0..1000 {
        let key = format!("key{:04}", i);
        let value = format!("value{}", i);
        batch.put(cf.id(), Bytes::from(key), Bytes::from(value));
    }

    // Act
    engine.write_batch(&batch).expect("write_batch");

    // Assert
    assert_eq!(batch.len(), 1000);
    assert_eq!(
        engine.get(&cf, b"key0000").unwrap(),
        Some(Bytes::from("value0"))
    );
    assert_eq!(
        engine.get(&cf, b"key0999").unwrap(),
        Some(Bytes::from("value999"))
    );
}

// ============================================================================
// Durability
// ============================================================================

#[test]
fn should_persist_batch_given_flush_when_reopening() {
    // Arrange
    let dir = test_temp_dir();
    let path = dir.path().to_path_buf();

    {
        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk {
                db_path: path.clone(),
            },
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        let mut batch = WriteBatch::new();
        batch.put(
            cf.id(),
            Bytes::from_static(b"k1"),
            Bytes::from_static(b"v1"),
        );
        batch.put(
            cf.id(),
            Bytes::from_static(b"k2"),
            Bytes::from_static(b"v2"),
        );
        engine.write_batch(&batch).expect("write_batch");
        engine.flush().expect("flush");
    }

    // Act
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: path },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("reopen");
    let cf = engine.default_column_family();

    // Assert
    assert_eq!(
        engine.get(&cf, b"k1").unwrap(),
        Some(Bytes::from_static(b"v1"))
    );
    assert_eq!(
        engine.get(&cf, b"k2").unwrap(),
        Some(Bytes::from_static(b"v2"))
    );
}

// ============================================================================
// Column Family Support
// ============================================================================

#[test]
fn should_write_to_multiple_cfs_given_multi_cf_batch_when_write_batch() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf_default = engine.default_column_family();
    let cf1 = engine
        .create_column_family("cf1", ColumnFamilyConfig::default())
        .expect("create");
    let cf2 = engine
        .create_column_family("cf2", ColumnFamilyConfig::default())
        .expect("create");

    let mut batch = WriteBatch::new();
    batch.put(
        cf_default.id(),
        Bytes::from_static(b"k_default"),
        Bytes::from_static(b"v_default"),
    );
    batch.put(
        cf1.id(),
        Bytes::from_static(b"k_cf1"),
        Bytes::from_static(b"v_cf1"),
    );
    batch.put(
        cf2.id(),
        Bytes::from_static(b"k_cf2"),
        Bytes::from_static(b"v_cf2"),
    );

    // Act
    engine.write_batch(&batch).expect("write_batch");

    // Assert
    assert_eq!(
        engine.get(&cf_default, b"k_default").unwrap(),
        Some(Bytes::from_static(b"v_default"))
    );
    assert_eq!(
        engine.get(&cf1, b"k_cf1").unwrap(),
        Some(Bytes::from_static(b"v_cf1"))
    );
    assert_eq!(
        engine.get(&cf2, b"k_cf2").unwrap(),
        Some(Bytes::from_static(b"v_cf2"))
    );
}

#[test]
fn should_isolate_keys_given_same_key_in_different_cfs_when_write_batch() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf_default = engine.default_column_family();
    let cf1 = engine
        .create_column_family("cf1", ColumnFamilyConfig::default())
        .expect("create");

    let mut batch = WriteBatch::new();
    batch.put(
        cf1.id(),
        Bytes::from_static(b"key"),
        Bytes::from_static(b"value_cf1"),
    );

    // Act
    engine.write_batch(&batch).expect("write_batch");

    // Assert - key only in cf1, not in default
    assert_eq!(
        engine.get(&cf1, b"key").unwrap(),
        Some(Bytes::from_static(b"value_cf1"))
    );
    assert_eq!(engine.get(&cf_default, b"key").unwrap(), None);
}

// ============================================================================
// Concurrency
// ============================================================================

#[test]
fn should_not_interleave_given_concurrent_batches_when_write_batch() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = Arc::new(MidgeEngine::open(opts).expect("open"));

    // Act - concurrent batch writes from multiple threads
    let handles: Vec<_> = (0..10)
        .map(|tid| {
            let eng = Arc::clone(&engine);
            std::thread::spawn(move || {
                let cf = eng.default_column_family();
                let mut batch = WriteBatch::new();
                for i in 0..100 {
                    let key = format!("t{:02}_k{:03}", tid, i);
                    let value = format!("t{:02}_v{:03}", tid, i);
                    batch.put(cf.id(), Bytes::from(key), Bytes::from(value));
                }
                eng.write_batch(&batch).expect("write_batch");
            })
        })
        .collect();

    for h in handles {
        h.join().expect("join");
    }

    // Assert - each thread's data intact
    let cf = engine.default_column_family();
    for tid in 0..10 {
        for i in 0..100 {
            let key = format!("t{:02}_k{:03}", tid, i);
            let expected = format!("t{:02}_v{:03}", tid, i);
            assert_eq!(
                engine.get(&cf, key.as_bytes()).unwrap(),
                Some(Bytes::from(expected))
            );
        }
    }
}

// ============================================================================
// Atomicity on Crash
// ============================================================================

#[test]
fn should_be_atomic_given_crash_during_wal_write_when_recovering() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_wal_behavior(WalBehavior::TruncateAfterWrite);
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_sync: true,
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        test_hooks: Some(hooks),
        ..Default::default()
    };

    {
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        let mut batch = WriteBatch::new();
        batch.put(
            cf.id(),
            Bytes::from_static(b"atom_k1"),
            Bytes::from_static(b"v1"),
        );
        batch.put(
            cf.id(),
            Bytes::from_static(b"atom_k2"),
            Bytes::from_static(b"v2"),
        );
        batch.put(
            cf.id(),
            Bytes::from_static(b"atom_k3"),
            Bytes::from_static(b"v3"),
        );
        let _ = eng.write_batch(&batch);
    }

    // Act - reopen
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

    let k1 = eng.get(&cf, b"atom_k1").expect("get");
    let k2 = eng.get(&cf, b"atom_k2").expect("get");
    let k3 = eng.get(&cf, b"atom_k3").expect("get");

    // Assert - atomicity: all present or all absent
    if k1.is_some() {
        assert!(
            k2.is_some() && k3.is_some(),
            "Batch must be atomic: all or nothing"
        );
    } else {
        assert!(
            k2.is_none() && k3.is_none(),
            "Batch must be atomic: all or nothing"
        );
    }
}

#[test]
fn should_be_atomic_given_large_batch_crash_when_recovering() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_wal_behavior(WalBehavior::TruncateAfterWrite);
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_sync: true,
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        test_hooks: Some(hooks),
        ..Default::default()
    };

    {
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        let mut batch = WriteBatch::new();
        for i in 0..100 {
            let key = format!("large_{:03}", i);
            batch.put(
                cf.id(),
                Bytes::from(key.into_bytes()),
                Bytes::from_static(b"value"),
            );
        }
        let _ = eng.write_batch(&batch);
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

    let first = eng.get(&cf, b"large_000").expect("get");
    let last = eng.get(&cf, b"large_099").expect("get");

    // Assert - atomicity
    assert_eq!(
        first.is_some(),
        last.is_some(),
        "Large batch must be atomic"
    );
}

#[test]
fn should_support_batch_with_ttl_when_write_batch() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    let mut batch = WriteBatch::new();
    batch.put_with_ttl(
        cf.id(),
        Bytes::from_static(b"ttl_key"),
        Bytes::from_static(b"ttl_value"),
        3600,
    );
    batch.put(
        cf.id(),
        Bytes::from_static(b"regular_key"),
        Bytes::from_static(b"regular_value"),
    );

    // Act
    engine.write_batch(&batch).expect("write_batch");

    // Assert
    assert_eq!(
        engine.get(&cf, b"ttl_key").expect("get ttl"),
        Some(Bytes::from_static(b"ttl_value"))
    );
    assert_eq!(
        engine.get(&cf, b"regular_key").expect("get regular"),
        Some(Bytes::from_static(b"regular_value"))
    );
}

#[test]
fn should_maintain_atomicity_during_concurrent_reads_when_write_batch() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
    let cf = engine.default_column_family();

    let mut batch = WriteBatch::new();
    batch.put(
        cf.id(),
        Bytes::from_static(b"k1"),
        Bytes::from_static(b"v1"),
    );
    batch.put(
        cf.id(),
        Bytes::from_static(b"k2"),
        Bytes::from_static(b"v2"),
    );
    batch.put(
        cf.id(),
        Bytes::from_static(b"k3"),
        Bytes::from_static(b"v3"),
    );

    // Act - write batch while readers are active
    let reader_engine = Arc::clone(&engine);
    let reader_cf = cf.clone();
    let reader = std::thread::spawn(move || {
        for _ in 0..100 {
            let k1 = reader_engine.get(&reader_cf, b"k1").unwrap();
            let k2 = reader_engine.get(&reader_cf, b"k2").unwrap();
            let k3 = reader_engine.get(&reader_cf, b"k3").unwrap();

            // Keys should be all present or all absent (atomicity)
            if k1.is_some() {
                assert!(k2.is_some() && k3.is_some(), "Partial batch visible!");
            }
        }
    });

    engine.write_batch(&batch).expect("write_batch");
    reader.join().expect("reader join");

    // Assert
    assert_eq!(
        engine.get(&cf, b"k1").expect("get k1"),
        Some(Bytes::from_static(b"v1"))
    );
    assert_eq!(
        engine.get(&cf, b"k2").expect("get k2"),
        Some(Bytes::from_static(b"v2"))
    );
    assert_eq!(
        engine.get(&cf, b"k3").expect("get k3"),
        Some(Bytes::from_static(b"v3"))
    );
}

#[test]
fn should_increment_sequence_numbers_given_batch_operations_when_write_batch() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    let seq_before = engine.current_sequence();

    let mut batch = WriteBatch::new();
    batch.put(
        cf.id(),
        Bytes::from_static(b"k1"),
        Bytes::from_static(b"v1"),
    );
    batch.put(
        cf.id(),
        Bytes::from_static(b"k2"),
        Bytes::from_static(b"v2"),
    );
    batch.put(
        cf.id(),
        Bytes::from_static(b"k3"),
        Bytes::from_static(b"v3"),
    );

    // Act
    engine.write_batch(&batch).expect("write_batch");

    // Assert - sequence should increase
    let seq_after = engine.current_sequence();
    assert!(seq_after > seq_before);
}
