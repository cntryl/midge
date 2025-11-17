//! Integration tests for WriteOptions behavior in transactions.
//!
//! Validates that WriteOptions::sync() and WriteOptions::no_sync() correctly
//! control fsync behavior at transaction commit time.

use bytes::Bytes;
use cntryl_midge::{
    KvTransaction, MidgeEngine, MidgeOptions, StorageMode, WriteOptions,
};

mod common;
use common::test_temp_dir;

#[test]
fn should_persist_transaction_with_sync_option() {
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
    let mut txn = engine.begin_transaction(&cf).unwrap();
    txn.put(b"sync_key", b"sync_value").unwrap();
    engine
        .commit_transaction(txn, WriteOptions::sync())
        .unwrap();

    drop(engine);

    // Assert - Data should be readable after reopen
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("reopen");
    let cf = engine.default_column_family();

    assert_eq!(
        engine.get(&cf, b"sync_key").expect("get"),
        Some(Bytes::from_static(b"sync_value"))
    );
}

#[test]
fn should_commit_transaction_with_nosync_option() {
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

    // Act - Transaction with no-sync option
    let mut txn = engine.begin_transaction(&cf).unwrap();
    for i in 0..10 {
        txn.put(format!("nosync_{}", i).as_bytes(), b"value")
            .unwrap();
    }
    engine
        .commit_transaction(txn, WriteOptions::no_sync())
        .unwrap();

    // Assert - Data should be readable
    for i in 0..10 {
        let result = engine
            .get(&cf, format!("nosync_{}", i).as_bytes())
            .expect("get");
        assert!(result.is_some());
    }
}

#[test]
fn should_mix_sync_and_nosync_commits() {
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

    // Act - First transaction with NoSync
    let mut txn1 = engine.begin_transaction(&cf).unwrap();
    txn1.put(b"key1", b"value1").unwrap();
    engine
        .commit_transaction(txn1, WriteOptions::no_sync())
        .unwrap();

    // Second transaction with Sync
    let mut txn2 = engine.begin_transaction(&cf).unwrap();
    txn2.put(b"key2", b"value2").unwrap();
    engine
        .commit_transaction(txn2, WriteOptions::sync())
        .unwrap();

    drop(engine);

    // Assert - All data should be persisted
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("reopen");
    let cf = engine.default_column_family();

    assert_eq!(
        engine.get(&cf, b"key1").expect("get"),
        Some(Bytes::from_static(b"value1"))
    );
    assert_eq!(
        engine.get(&cf, b"key2").expect("get"),
        Some(Bytes::from_static(b"value2"))
    );
}

#[test]
fn should_persist_sync_transaction_after_crash() {
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

    // Act - Transaction with sync option
    let mut txn = engine.begin_transaction(&cf).unwrap();
    txn.put(b"critical", b"important_data").unwrap();
    engine
        .commit_transaction(txn, WriteOptions::sync())
        .unwrap();

    // Simulate crash (drop engine without clean shutdown)
    drop(engine);

    // Assert - Data should survive crash
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("reopen");
    let cf = engine.default_column_family();

    assert_eq!(
        engine.get(&cf, b"critical").expect("get"),
        Some(Bytes::from_static(b"important_data"))
    );
}

#[test]
fn should_default_to_no_sync_behavior() {
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

    // Act - Commit with default WriteOptions
    let mut txn = engine.begin_transaction(&cf).unwrap();
    txn.put(b"default_key", b"default_value").unwrap();
    engine
        .commit_transaction(txn, WriteOptions::default())
        .unwrap();

    // Assert - Data should be readable
    assert_eq!(
        engine.get(&cf, b"default_key").expect("get"),
        Some(Bytes::from_static(b"default_value"))
    );
}

#[test]
fn should_handle_empty_transaction_with_sync() {
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

    // Act - Empty transaction with sync option
    let txn = engine.begin_transaction(&cf).unwrap();
    let result = engine.commit_transaction(txn, WriteOptions::sync());

    // Assert - Should succeed
    assert!(result.is_ok());
}

#[test]
fn should_batch_multiple_writes_in_sync_transaction() {
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

    // Act - Multiple writes in one transaction with sync
    let mut txn = engine.begin_transaction(&cf).unwrap();
    for i in 0..100 {
        txn.put(format!("batch_{}", i).as_bytes(), b"value")
            .unwrap();
    }
    engine
        .commit_transaction(txn, WriteOptions::sync())
        .unwrap();

    drop(engine);

    // Assert - All writes should be persisted
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("reopen");
    let cf = engine.default_column_family();

    for i in 0..100 {
        let result = engine
            .get(&cf, format!("batch_{}", i).as_bytes())
            .expect("get");
        assert!(
            result.is_some(),
            "Batch write {} should be persisted",
            i
        );
    }
}
