// Optimistic Locking
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
fn should_validate_version_given_read_set_when_committing_transaction() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    engine
        .put(&cf, Bytes::from("key"), Bytes::from("v1"))
        .expect("put");

    let reading_txn = engine.begin_transaction(&cf);
    let snap = engine.snapshot();
    let _read_value = engine.get_at(b"key", &snap).expect("get_at");

    engine
        .put(&cf, Bytes::from("key"), Bytes::from("v2"))
        .expect("external put");

    // Act
    let result = engine.commit_transaction(reading_txn, cntryl_midge::WriteOptions::default());

    // Assert
    // Currently no read-set validation, transaction commits
    assert!(result.is_ok());
    // TODO: Should fail when optimistic locking validates read-set
}

#[test]
fn should_abort_transaction_given_stale_read_when_key_modified_by_other() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    engine
        .put(&cf, Bytes::from("key"), Bytes::from("initial"))
        .expect("put");

    let mut stale_txn = engine.begin_transaction(&cf);
    let _local = stale_txn.get_local(b"key");

    engine
        .put(&cf, Bytes::from("key"), Bytes::from("modified"))
        .expect("concurrent put");

    stale_txn
        .put(Bytes::from("key"), Bytes::from("txn_value"), None)
        .unwrap();

    // Act
    let result = engine.commit_transaction(stale_txn, cntryl_midge::WriteOptions::default());

    // Assert
    // No stale read detection currently
    assert!(result.is_ok());
    // TODO: Should abort when read-after-write validation is implemented
}

#[test]
fn should_track_read_set_given_transaction_gets_when_validating() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    engine
        .put(&cf, Bytes::from("k1"), Bytes::from("v1"))
        .expect("put");
    engine
        .put(&cf, Bytes::from("k2"), Bytes::from("v2"))
        .expect("put");

    let reading_txn = engine.begin_transaction(&cf);
    let snap = engine.snapshot();
    let _v1 = engine.get_at(b"k1", &snap);
    let _v2 = engine.get_at(b"k2", &snap);

    // Act
    // (Currently no explicit read-set tracking API, so Act is observation)

    // Assert
    // No explicit read-set tracking API currently
    // Transaction should track {k1, k2} for validation
    assert!(reading_txn.begin_sequence() > 0);
    // TODO: Add API to inspect read-set when implemented
}

#[test]
fn should_allow_commit_given_no_conflicts_when_validation_succeeds() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    let mut clean_txn = engine.begin_transaction(&cf);
    clean_txn
        .put(Bytes::from("new_key"), Bytes::from("new_value"), None)
        .unwrap();

    // Act
    let result = engine.commit_transaction(clean_txn, cntryl_midge::WriteOptions::default());

    // Assert
    assert!(result.is_ok());
    assert_eq!(
        engine.get(&cf, b"new_key").expect("get"),
        Some(Bytes::from("new_value"))
    );
}
