// Durability
// Extracted from transaction_acid.rs

// Transaction ACID tests - P0 Priority
// Tests document expected behavior and will fail until features are implemented

use cntryl_midge::{
    test_hooks::{IoBehavior, TestHooks},
    KvTransaction, MidgeEngine, MidgeOptions, StorageMode,
};
use std::sync::Arc;

mod common;
use common::test_temp_dir;

#[test]
fn should_persist_transaction_given_commit_when_crash_after() {
    // Arrange
    let dir = test_temp_dir();
    let db_path = dir.path().to_path_buf();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_path.clone(),
        },
        ..Default::default()
    };
    let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
    let cf = engine.default_column_family();

    let mut durable_txn = engine.begin_transaction(&cf).expect("begin_transaction");
    durable_txn.put(b"durable_key", b"durable_value").unwrap();
    engine
        .commit_transaction(durable_txn, cntryl_midge::WriteOptions::default())
        .expect("commit");

    drop(engine);

    let opts2 = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path },
        ..Default::default()
    };

    // Act
    let engine2 = MidgeEngine::open(opts2).expect("reopen");
    let cf2 = engine2.default_column_family();

    // Assert
    let result = engine2.get(&cf2, b"durable_key").expect("get");
    assert_eq!(result, Some(b"durable_value".to_vec().into()));
}

#[test]
fn should_not_persist_transaction_given_abort_when_crash_after() {
    // Arrange
    let dir = test_temp_dir();
    let db_path = dir.path().to_path_buf();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_path.clone(),
        },
        ..Default::default()
    };
    let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
    let cf = engine.default_column_family();

    let mut aborted_txn = engine.begin_transaction(&cf).expect("begin_transaction");
    aborted_txn.put(b"aborted_key", b"aborted_value").unwrap();
    drop(aborted_txn);

    drop(engine);

    let opts2 = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path },
        ..Default::default()
    };

    // Act
    let engine2 = MidgeEngine::open(opts2).expect("reopen");
    let cf2 = engine2.default_column_family();

    // Assert
    assert_eq!(engine2.get(&cf2, b"aborted_key").expect("get"), None);
}

#[test]
fn should_recover_committed_transactions_given_wal_replay_when_restart() {
    // Arrange
    let dir = test_temp_dir();
    let db_path = dir.path().to_path_buf();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_path.clone(),
        },
        ..Default::default()
    };
    let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
    let cf = engine.default_column_family();

    for i in 0..10 {
        let mut wal_txn = engine.begin_transaction(&cf).expect("begin_transaction");
        let key = format!("wal_key_{}", i);
        let value = format!("wal_val_{}", i);
        wal_txn.put(key.as_bytes(), value.as_bytes()).unwrap();
        engine
            .commit_transaction(wal_txn, cntryl_midge::WriteOptions::default())
            .expect("commit");
    }

    drop(engine);

    let opts2 = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path },
        ..Default::default()
    };

    // Act
    let engine2 = MidgeEngine::open(opts2).expect("reopen");
    let cf2 = engine2.default_column_family();

    // Assert
    for i in 0..10 {
        let key = format!("wal_key_{}", i);
        let expected = format!("wal_val_{}", i);
        let result = engine2.get(&cf2, key.as_bytes()).expect("get");
        assert_eq!(
            result,
            Some(expected.as_bytes().to_vec().into()),
            "WAL replay should recover transaction {}",
            i
        );
    }
}

#[test]
fn should_fail_transaction_commit_when_disk_full() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_io_behavior(IoBehavior::Normal);
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };
    let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
    let cf = engine.default_column_family();

    let mut txn = engine.begin_transaction(&cf).expect("begin_transaction");
    txn.put(b"txn_key", b"txn_value").unwrap();

    // Set disk full behavior
    hooks.set_io_behavior(IoBehavior::FailWithEnospc);

    // Act: attempt to commit transaction
    let result = engine.commit_transaction(txn, cntryl_midge::WriteOptions::sync());

    // Assert: commit should fail with disk full error
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("No space left on device"));

    // Reset behavior
    hooks.set_io_behavior(IoBehavior::Normal);

    // Verify engine still works after disk full error
    // Note: The transaction data may be in memtable but not durable
    engine
        .put(&cf, b"test_key", b"test_value")
        .expect("put after failed commit");
}
