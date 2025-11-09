// Atomicity
// Extracted from transaction_acid.rs

// Transaction ACID tests - P0 Priority
// Tests document expected behavior and will fail until features are implemented

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use tempfile::TempDir;

mod common;
use common::{test_temp_dir, new_engine};
/// Helper: create a new engine in a fresh temp dir and return both.
fn new_engine() -> (tempfile::TempDir, cntryl_midge::MidgeEngine) {
    let dir = test_temp_dir();
    let opts = cntryl_midge::MidgeOptions {
        storage_mode: cntryl_midge::StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = cntryl_midge::MidgeEngine::open(opts).expect("open");
    (dir, engine)
}

#[test]
fn should_commit_all_or_nothing_given_multi_key_transaction() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    let mut atomic_txn = engine.begin_transaction(&cf);
    atomic_txn
        .put(Bytes::from("k1"), Bytes::from("v1"), None)
        .unwrap();
    atomic_txn
        .put(Bytes::from("k2"), Bytes::from("v2"), None)
        .unwrap();
    atomic_txn
        .put(Bytes::from("k3"), Bytes::from("v3"), None)
        .unwrap();
    atomic_txn.delete(&cf, b"k4").unwrap();

    // Act
    engine
        .commit_transaction(atomic_txn, cntryl_midge::WriteOptions::default())
        .expect("commit");

    // Assert
    assert_eq!(
        engine.get(&cf, b"k1").expect("get"),
        Some(Bytes::from("v1"))
    );
    assert_eq!(
        engine.get(&cf, b"k2").expect("get"),
        Some(Bytes::from("v2"))
    );
    assert_eq!(
        engine.get(&cf, b"k3").expect("get"),
        Some(Bytes::from("v3"))
    );
    assert_eq!(engine.get(&cf, b"k4").expect("get"), None);
}

#[test]
fn should_be_atomic_given_transaction_with_100_operations() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    let mut batch_txn = engine.begin_transaction(&cf);
    for i in 0..100 {
        batch_txn
            .put(
                Bytes::from(format!("batch_key_{}", i)),
                Bytes::from(format!("batch_val_{}", i)),
                None,
            )
            .unwrap();
    }

    // Act
    engine
        .commit_transaction(batch_txn, cntryl_midge::WriteOptions::default())
        .expect("commit");

    // Assert
    for i in 0..100 {
        let key = format!("batch_key_{}", i);
        let expected = format!("batch_val_{}", i);
        assert_eq!(
            engine.get(&cf, key.as_bytes()).expect("get"),
            Some(Bytes::from(expected))
        );
    }
}

#[test]
fn should_rollback_all_writes_given_single_failure_when_committing() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    let mut failed_txn = engine.begin_transaction(&cf);
    failed_txn
        .put(Bytes::from("k1"), Bytes::from("v1"), None)
        .unwrap();
    failed_txn
        .put(Bytes::from("k2"), Bytes::from("v2"), None)
        .unwrap();

    // Act
    drop(failed_txn);

    // Assert
    assert_eq!(engine.get(&cf, b"k1").expect("get"), None);
    assert_eq!(engine.get(&cf, b"k2").expect("get"), None);
}

#[test]
fn should_not_expose_partial_writes_given_concurrent_readers_when_committing() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();
    let snap_before = engine.snapshot();

    let mut partial_write_txn = engine.begin_transaction(&cf);
    partial_write_txn
        .put(Bytes::from("atomic_k1"), Bytes::from("v1"), None)
        .unwrap();
    partial_write_txn
        .put(Bytes::from("atomic_k2"), Bytes::from("v2"), None)
        .unwrap();

    let read_during = engine.get(&cf, b"atomic_k1").expect("get during");

    // Act
    engine
        .commit_transaction(partial_write_txn, cntryl_midge::WriteOptions::default())
        .expect("commit");

    // Assert
    let snap_after = engine.snapshot();

    // Should not see partial writes
    assert_eq!(read_during, None, "Should not see uncommitted writes");
    assert!(snap_after.seq > snap_before.seq);
}
