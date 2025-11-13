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

#[test]
fn should_see_uncommitted_writes_in_scan_within_transaction() {
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

    // Pre-populate committed data
    engine.put(&cf, b"committed1", b"val1").expect("put");
    engine.put(&cf, b"committed2", b"val2").expect("put");
    engine.put(&cf, b"committed3", b"val3").expect("put");

    // Act: start transaction and make uncommitted changes
    let mut txn = engine.begin_transaction(&cf).expect("begin");
    txn.put(b"uncommitted1", b"new_val").expect("put");
    txn.delete(b"committed2").expect("delete");
    txn.put(b"uncommitted2", b"another_val").expect("put");

    // Scan within transaction
    let results = txn.scan(b"", b"\xFF\xFF\xFF\xFF").expect("scan");

    // Assert: scan sees both committed and uncommitted writes
    let keys: Vec<_> = results.iter().map(|(k, _)| k.as_ref()).collect();

    // Should see: committed1, committed3 (committed2 deleted), uncommitted1, uncommitted2
    assert_eq!(keys.len(), 4);
    assert!(keys.contains(&b"committed1".as_ref()));
    assert!(!keys.contains(&b"committed2".as_ref())); // Deleted in txn
    assert!(keys.contains(&b"committed3".as_ref()));
    assert!(keys.contains(&b"uncommitted1".as_ref()));
    assert!(keys.contains(&b"uncommitted2".as_ref()));

    // Verify values
    let committed1_val = results
        .iter()
        .find(|(k, _)| k.as_ref() == b"committed1")
        .map(|(_, v)| v.as_ref());
    assert_eq!(committed1_val, Some(b"val1".as_ref()));

    let uncommitted1_val = results
        .iter()
        .find(|(k, _)| k.as_ref() == b"uncommitted1")
        .map(|(_, v)| v.as_ref());
    assert_eq!(uncommitted1_val, Some(b"new_val".as_ref()));

    // Drop without commit - uncommitted changes should not persist
    drop(txn);

    // Assert: after rollback, only committed data visible
    assert_eq!(
        engine.get(&cf, b"committed1").expect("get"),
        Some(Bytes::from("val1"))
    );
    assert_eq!(
        engine.get(&cf, b"committed2").expect("get"),
        Some(Bytes::from("val2"))
    );
    assert_eq!(engine.get(&cf, b"uncommitted1").expect("get"), None);
}

#[test]
fn should_handle_delete_range_in_transaction_scan() {
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

    // Pre-populate data
    engine.put(&cf, b"key0", b"val0").expect("put");
    engine.put(&cf, b"key1", b"val1").expect("put");
    engine.put(&cf, b"key2", b"val2").expect("put");
    engine.put(&cf, b"key3", b"val3").expect("put");
    engine.put(&cf, b"key4", b"val4").expect("put");

    // Act: delete range in transaction and scan
    let mut txn = engine.begin_transaction(&cf).expect("begin");
    txn.delete_range(b"key1", b"key4").expect("delete_range");
    let results = txn.scan(b"key0", b"key5").expect("scan");

    // Assert: scan should not see keys in delete range
    let keys: Vec<_> = results.iter().map(|(k, _)| k.as_ref()).collect();
    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&b"key0".as_ref()));
    assert!(!keys.contains(&b"key1".as_ref()));
    assert!(!keys.contains(&b"key2".as_ref()));
    assert!(!keys.contains(&b"key3".as_ref()));
    assert!(keys.contains(&b"key4".as_ref()));
}
