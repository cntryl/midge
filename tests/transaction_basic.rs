//! Basic Transaction Tests
//!
//! These tests verify fundamental transaction operations:
//! - Commit: Transactions apply all operations atomically
//! - Rollback: Uncommitted transactions have no effect
//! - Isolation: Transactions see their own uncommitted writes
//! - Operations: put, get, delete, delete_range, scan work in transactions

mod common;

use bytes::Bytes;
use cntryl_midge::{KvTransaction, MidgeEngine, MidgeOptions, StorageMode, WriteOptions};
use common::test_temp_dir;

// ============================================================================
// Transaction Commit
// ============================================================================

#[test]
fn should_persist_single_put_given_commit_when_reading() {
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

    // Act
    let mut txn = engine.begin_transaction(&cf).expect("begin");
    txn.put(b"key", b"value").expect("put");
    engine
        .commit_transaction(txn, WriteOptions::default())
        .expect("commit");

    // Assert
    assert_eq!(
        engine.get(&cf, b"key").expect("get"),
        Some(Bytes::from_static(b"value"))
    );
}

#[test]
fn should_persist_multiple_operations_given_commit_when_reading() {
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

    engine.put(&cf, b"to_delete", b"old_value").expect("put");

    // Act
    let mut txn = engine.begin_transaction(&cf).expect("begin");
    txn.put(b"key1", b"value1").expect("put");
    txn.put(b"key2", b"value2").expect("put");
    txn.delete(b"to_delete").expect("delete");
    engine
        .commit_transaction(txn, WriteOptions::default())
        .expect("commit");

    // Assert - all operations applied atomically
    assert_eq!(
        engine.get(&cf, b"key1").expect("get"),
        Some(Bytes::from_static(b"value1"))
    );
    assert_eq!(
        engine.get(&cf, b"key2").expect("get"),
        Some(Bytes::from_static(b"value2"))
    );
    assert_eq!(engine.get(&cf, b"to_delete").expect("get"), None);
}

#[test]
fn should_commit_empty_transaction_given_no_operations_when_commit() {
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

    // Act
    let txn = engine.begin_transaction(&cf).expect("begin");
    let result = engine.commit_transaction(txn, WriteOptions::default());

    // Assert - empty commit should succeed
    assert!(result.is_ok());
}

#[test]
fn should_apply_100_operations_given_large_transaction_when_commit() {
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

    // Act
    let mut txn = engine.begin_transaction(&cf).expect("begin");
    for i in 0..100 {
        txn.put(
            format!("key_{:03}", i).as_bytes(),
            format!("value_{:03}", i).as_bytes(),
        )
        .expect("put");
    }
    engine
        .commit_transaction(txn, WriteOptions::default())
        .expect("commit");

    // Assert - all operations applied
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        let expected = format!("value_{:03}", i);
        assert_eq!(
            engine.get(&cf, key.as_bytes()).expect("get"),
            Some(Bytes::from(expected)),
            "key {} not found",
            key
        );
    }
}

// ============================================================================
// Transaction Rollback
// ============================================================================

#[test]
fn should_discard_writes_given_drop_without_commit_when_reading() {
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

    // Act - create transaction and drop without commit
    {
        let mut txn = engine.begin_transaction(&cf).expect("begin");
        txn.put(b"key", b"value").expect("put");
        // txn dropped here
    }

    // Assert - uncommitted write not visible
    assert_eq!(engine.get(&cf, b"key").expect("get"), None);
}

#[test]
fn should_discard_writes_given_explicit_drop_when_reading() {
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

    // Act - explicitly drop transaction without commit (implicit rollback)
    let mut txn = engine.begin_transaction(&cf).expect("begin");
    txn.put(b"key", b"value").expect("put");
    drop(txn); // Explicit drop = rollback

    // Assert
    assert_eq!(engine.get(&cf, b"key").expect("get"), None);
}

#[test]
fn should_preserve_existing_data_given_rollback_with_delete_when_reading() {
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

    engine.put(&cf, b"key", b"original").expect("put");

    // Act - delete in transaction then rollback
    {
        let mut txn = engine.begin_transaction(&cf).expect("begin");
        txn.delete(b"key").expect("delete");
        // txn dropped - implicit rollback
    }

    // Assert - original data preserved
    assert_eq!(
        engine.get(&cf, b"key").expect("get"),
        Some(Bytes::from_static(b"original"))
    );
}

// ============================================================================
// Transaction Read Isolation
// ============================================================================

#[test]
fn should_see_own_write_given_put_in_transaction_when_get_in_transaction() {
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

    // Act
    let mut txn = engine.begin_transaction(&cf).expect("begin");
    txn.put(b"key", b"txn_value").expect("put");
    let txn_result = txn.get(b"key").expect("get in txn");

    // Assert - transaction sees its own write
    assert_eq!(txn_result, Some(Bytes::from_static(b"txn_value")));

    // But main engine doesn't see uncommitted write
    assert_eq!(engine.get(&cf, b"key").expect("get"), None);
}

#[test]
fn should_see_overwritten_value_given_put_over_existing_in_transaction_when_get() {
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

    engine.put(&cf, b"key", b"original").expect("put");

    // Act
    let mut txn = engine.begin_transaction(&cf).expect("begin");
    txn.put(b"key", b"updated").expect("put");
    let txn_result = txn.get(b"key").expect("get in txn");

    // Assert - transaction sees its own update
    assert_eq!(txn_result, Some(Bytes::from_static(b"updated")));

    // Main engine still sees original
    assert_eq!(
        engine.get(&cf, b"key").expect("get"),
        Some(Bytes::from_static(b"original"))
    );
}

#[test]
fn should_see_delete_given_delete_in_transaction_when_get() {
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

    engine.put(&cf, b"key", b"value").expect("put");

    // Act
    let mut txn = engine.begin_transaction(&cf).expect("begin");
    txn.delete(b"key").expect("delete");
    let txn_result = txn.get(b"key").expect("get in txn");

    // Assert - transaction sees the delete
    assert_eq!(txn_result, None);

    // Main engine still sees the value
    assert_eq!(
        engine.get(&cf, b"key").expect("get"),
        Some(Bytes::from_static(b"value"))
    );
}

// ============================================================================
// Transaction Delete Range
// ============================================================================

#[test]
fn should_delete_keys_in_range_given_delete_range_when_commit() {
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

    for i in 0..5 {
        engine
            .put(&cf, format!("key{}", i).as_bytes(), format!("val{}", i).as_bytes())
            .expect("put");
    }

    // Act
    let mut txn = engine.begin_transaction(&cf).expect("begin");
    txn.delete_range(b"key1", b"key4").expect("delete_range");
    engine
        .commit_transaction(txn, WriteOptions::default())
        .expect("commit");

    // Assert - keys in range deleted, boundaries preserved
    assert_eq!(
        engine.get(&cf, b"key0").expect("get"),
        Some(Bytes::from_static(b"val0"))
    );
    assert_eq!(engine.get(&cf, b"key1").expect("get"), None);
    assert_eq!(engine.get(&cf, b"key2").expect("get"), None);
    assert_eq!(engine.get(&cf, b"key3").expect("get"), None);
    assert_eq!(
        engine.get(&cf, b"key4").expect("get"),
        Some(Bytes::from_static(b"val4"))
    );
}

#[test]
fn should_see_delete_range_given_uncommitted_delete_range_when_scan_in_transaction() {
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

    for i in 0..5 {
        engine
            .put(&cf, format!("key{}", i).as_bytes(), format!("val{}", i).as_bytes())
            .expect("put");
    }

    // Act
    let mut txn = engine.begin_transaction(&cf).expect("begin");
    txn.delete_range(b"key1", b"key4").expect("delete_range");
    let results = txn.scan(b"key0", b"key5").expect("scan");

    // Assert - scan sees delete range
    let keys: Vec<_> = results.iter().map(|(k, _)| k.as_ref()).collect();
    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&b"key0".as_ref()));
    assert!(keys.contains(&b"key4".as_ref()));
}

// ============================================================================
// Transaction Scan
// ============================================================================

#[test]
fn should_include_uncommitted_puts_given_scan_in_transaction_when_scanning() {
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

    engine.put(&cf, b"committed1", b"val1").expect("put");
    engine.put(&cf, b"committed2", b"val2").expect("put");

    // Act
    let mut txn = engine.begin_transaction(&cf).expect("begin");
    txn.put(b"uncommitted", b"new_val").expect("put");
    let results = txn.scan(b"", b"\xFF").expect("scan");

    // Assert - scan includes uncommitted write
    let keys: Vec<_> = results.iter().map(|(k, _)| k.as_ref()).collect();
    assert!(keys.contains(&b"committed1".as_ref()));
    assert!(keys.contains(&b"committed2".as_ref()));
    assert!(keys.contains(&b"uncommitted".as_ref()));
}

#[test]
fn should_exclude_uncommitted_deletes_given_scan_in_transaction_when_scanning() {
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

    engine.put(&cf, b"key1", b"val1").expect("put");
    engine.put(&cf, b"key2", b"val2").expect("put");
    engine.put(&cf, b"key3", b"val3").expect("put");

    // Act
    let mut txn = engine.begin_transaction(&cf).expect("begin");
    txn.delete(b"key2").expect("delete");
    let results = txn.scan(b"", b"\xFF").expect("scan");

    // Assert - scan excludes deleted key
    let keys: Vec<_> = results.iter().map(|(k, _)| k.as_ref()).collect();
    assert!(keys.contains(&b"key1".as_ref()));
    assert!(!keys.contains(&b"key2".as_ref()));
    assert!(keys.contains(&b"key3".as_ref()));
}

// ============================================================================
// Transaction Insert (conditional put)
// ============================================================================

#[test]
fn should_insert_given_key_does_not_exist_when_insert_in_transaction() {
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

    // Act
    let mut txn = engine.begin_transaction(&cf).expect("begin");
    txn.insert(b"new_key", b"new_value").expect("insert");
    engine
        .commit_transaction(txn, WriteOptions::default())
        .expect("commit");

    // Assert
    assert_eq!(
        engine.get(&cf, b"new_key").expect("get"),
        Some(Bytes::from_static(b"new_value"))
    );
}

#[test]
fn should_fail_commit_given_insert_on_existing_key_when_commit() {
    // Insert validation happens at commit time, not staging time.
    // This tests that the commit correctly rejects insert for existing keys.
    
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

    engine.put(&cf, b"existing", b"original").expect("put");

    // Act - insert stages successfully, but commit should fail
    let mut txn = engine.begin_transaction(&cf).expect("begin");
    txn.insert(b"existing", b"new_value").expect("insert staging succeeds");
    let result = engine.commit_transaction(txn, WriteOptions::default());

    // Assert - commit fails with conflict
    assert!(result.is_err(), "commit should fail for insert on existing key");

    // Original value unchanged
    assert_eq!(
        engine.get(&cf, b"existing").expect("get"),
        Some(Bytes::from_static(b"original"))
    );
}

// ============================================================================
// Transaction Durability
// ============================================================================

#[test]
fn should_persist_transaction_given_sync_commit_when_reopening() {
    // Arrange
    let dir = test_temp_dir();
    let db_path = dir.path().to_path_buf();

    {
        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk {
                db_path: db_path.clone(),
            },
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        // Act - commit with sync
        let mut txn = engine.begin_transaction(&cf).expect("begin");
        txn.put(b"durable_key", b"durable_value").expect("put");
        engine
            .commit_transaction(txn, WriteOptions::sync())
            .expect("commit");
    }

    // Reopen and verify
    {
        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk { db_path },
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("reopen");
        let cf = engine.default_column_family();

        // Assert - data persisted
        assert_eq!(
            engine.get(&cf, b"durable_key").expect("get"),
            Some(Bytes::from_static(b"durable_value"))
        );
    }
}

#[test]
fn should_not_persist_uncommitted_transaction_given_crash_when_reopening() {
    // Arrange
    let dir = test_temp_dir();
    let db_path = dir.path().to_path_buf();

    // Act
    {
        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk {
                db_path: db_path.clone(),
            },
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        // Put committed data
        engine.put(&cf, b"committed", b"value").expect("put");

        // Start transaction but don't commit
        let mut txn = engine.begin_transaction(&cf).expect("begin");
        txn.put(b"uncommitted", b"txn_value").expect("put");
        // Engine dropped without commit - simulates crash
    }

    // Assert
    {
        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk { db_path },
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("reopen");
        let cf = engine.default_column_family();

        // Committed data present, uncommitted absent
        assert_eq!(
            engine.get(&cf, b"committed").expect("get"),
            Some(Bytes::from_static(b"value"))
        );
        assert_eq!(engine.get(&cf, b"uncommitted").expect("get"), None);
    }
}
