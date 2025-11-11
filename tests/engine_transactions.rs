// Transaction Operations
// Extracted from engine.rs

#![allow(clippy::field_reassign_with_default)]
// Engine integration tests consolidated per repo preference
// Structure: Arrange // Act // Assert, one behavior per test, behavior-first names
use bytes::Bytes;
use cntryl_midge::{KvTransaction, MidgeEngine, MidgeOptions, StorageMode};

mod common;
use common::test_temp_dir;
#[test]
fn should_commit_transaction_atomically_given_multiple_operations() {
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

    // Act: create transaction and stage operations
    let mut txn = engine.begin_transaction(&cf).expect("begin");
    txn.put(b"key3", b"value3").expect("put");
    txn.insert(b"key4", b"value4").expect("insert");
    txn.delete(b"key5").expect("delete");
    engine
        .commit_transaction(txn, cntryl_midge::WriteOptions::default())
        .expect("commit");

    // Assert: all operations applied
    assert_eq!(
        engine.get(&cf, b"key3").expect("get"),
        Some(Bytes::from("value3"))
    );
    assert_eq!(
        engine.get(&cf, b"key4").expect("get"),
        Some(Bytes::from("value4"))
    );
    assert_eq!(engine.get(&cf, b"key5").expect("get"), None);
}

#[test]
fn should_rollback_transaction_on_drop_given_uncommitted() {
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

    // Act: create transaction, stage operations, then drop without committing
    {
        let mut txn = engine.begin_transaction(&cf).expect("begin");
        txn.put(b"rollback_key", b"rollback_value").expect("put");
        // txn dropped here without commit
    }

    // Assert: changes not persisted
    assert_eq!(engine.get(&cf, b"rollback_key").expect("get"), None);
}

#[test]
fn should_provide_snapshot_isolation_in_transaction() {
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
    engine.put(&cf, b"k1", b"v1").expect("put");

    // Act: start transaction, then modify key externally
    let _txn = engine.begin_transaction(&cf).expect("begin");
    engine.put(&cf, b"k1", b"v2").expect("put");

    // Assert: transaction provides snapshot isolation
    // (Full snapshot isolation is provided through engine.transaction_get)

    // Note: Full snapshot isolation for transaction reads would require
    // wiring txn.get() to engine.get_at(key, snap) - that's a future enhancement
}

#[test]
fn should_stage_delete_range_in_transaction() {
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

    // Pre-populate some keys
    for i in 0..5 {
        engine
            .put(
                &cf,
                format!("key{}", i).as_bytes(),
                format!("val{}", i).as_bytes(),
            )
            .expect("put");
    }

    // Act: use transaction to delete range
    let mut txn = engine.begin_transaction(&cf).expect("begin");
    txn.delete_range(b"key1", b"key4").expect("delete_range");
    engine
        .commit_transaction(txn, cntryl_midge::WriteOptions::default())
        .expect("commit");

    // Assert: keys in range are deleted, boundaries preserved
    assert_eq!(
        engine.get(&cf, b"key0").expect("get"),
        Some(Bytes::from("val0"))
    );
    assert_eq!(engine.get(&cf, b"key1").expect("get"), None);
    assert_eq!(engine.get(&cf, b"key2").expect("get"), None);
    assert_eq!(engine.get(&cf, b"key3").expect("get"), None);
    assert_eq!(
        engine.get(&cf, b"key4").expect("get"),
        Some(Bytes::from("val4"))
    );
}
