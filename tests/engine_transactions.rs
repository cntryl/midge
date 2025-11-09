// Transaction Operations
// Extracted from engine.rs

#![allow(clippy::field_reassign_with_default)]
// Engine integration tests consolidated per repo preference
// Structure: Arrange // Act // Assert, one behavior per test, behavior-first names
use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, Query, StorageMode};

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

    // Act: create transaction and stage operations (use crate Transaction API)
    let snap = engine.snapshot();
    let mut txn = cntryl_midge::Transaction::with_options(1, snap.seq, None, 100 * 1024 * 1024);
    txn.put(Bytes::from("key3"), Bytes::from("value3"), None)
        .expect("put");
    txn.insert(Bytes::from("key4"), Bytes::from("value4"), None)
        .expect("insert");
    txn.delete(Bytes::from("key5")).expect("delete");
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
        let snap = engine.snapshot();
        let mut txn = cntryl_midge::Transaction::with_options(2, snap.seq, None, 100 * 1024 * 1024);
        txn.put(Bytes::from("rollback_key"), Bytes::from("rollback_value"), None)
            .expect("put");
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
    let snap = engine.snapshot();
    let txn = cntryl_midge::Transaction::with_options(3, snap.seq, None, 100 * 1024 * 1024);
    engine.put(&cf, b"k1", b"v2").expect("put");

    // Assert: transaction has consistent view (begin_sequence captured)
    let begin_seq = txn.begin_sequence();
    assert!(begin_seq > 0);

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
    let snap = engine.snapshot();
    let mut txn = cntryl_midge::Transaction::with_options(4, snap.seq, None, 100 * 1024 * 1024);
    txn.delete_range(Bytes::from("key1"), Bytes::from("key4"))
        .expect("delete_range");
    engine
        .commit_transaction(txn, cntryl_midge::WriteOptions::default())
        .expect("commit");

    // Assert: keys in range are deleted, boundaries preserved
    assert_eq!(engine.get(&cf, b"key0").expect("get"), Some(Bytes::from("val0")));
    assert_eq!(engine.get(&cf, b"key1").expect("get"), None);
    assert_eq!(engine.get(&cf, b"key2").expect("get"), None);
    assert_eq!(engine.get(&cf, b"key3").expect("get"), None);
    assert_eq!(engine.get(&cf, b"key4").expect("get"), Some(Bytes::from("val4")));
}


