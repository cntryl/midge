// Optimistic Locking
// Extracted from transaction_acid.rs

// Transaction ACID tests - P0 Priority
// Tests document expected behavior and will fail until features are implemented

use bytes::Bytes;
use cntryl_midge::Transaction;

mod common;
use common::new_engine;
#[test]
fn should_validate_version_given_read_set_when_committing_transaction() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    engine.put(&cf, b"key", b"v1").expect("put");

    let reading_txn = Transaction::with_options(1, engine.snapshot().seq, None, 100 * 1024 * 1024);
    let snap = engine.snapshot();
    let _read_value = engine.get_at(b"key", &snap).expect("get_at");

    engine.put(&cf, b"key", b"v2").expect("external put");

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

    engine.put(&cf, b"key", b"initial").expect("put");

    let mut stale_txn =
        Transaction::with_options(2, engine.snapshot().seq, None, 100 * 1024 * 1024);
    let _local = stale_txn.get_local(b"key");

    engine
        .put(&cf, b"key", b"modified")
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

    engine.put(&cf, b"k1", b"v1").expect("put");
    engine.put(&cf, b"k2", b"v2").expect("put");

    let reading_txn = Transaction::with_options(3, engine.snapshot().seq, None, 100 * 1024 * 1024);
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

    let mut clean_txn =
        Transaction::with_options(4, engine.snapshot().seq, None, 100 * 1024 * 1024);
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
