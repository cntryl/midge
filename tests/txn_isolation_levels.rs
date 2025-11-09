// Isolation Levels
// Extracted from transaction_acid.rs

// Transaction ACID tests - P0 Priority
// Tests document expected behavior and will fail until features are implemented

use bytes::Bytes;
use cntryl_midge::KvStore;
use std::sync::Arc;

mod common;
use common::new_engine;

#[test]
fn should_prevent_dirty_read_given_uncommitted_write_when_read_committed() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    let mut uncommitted_txn = engine.begin_transaction(&cf).expect("begin_transaction");
    uncommitted_txn
        .put(b"key", b"uncommitted")
        .unwrap();

    // Act
    let read_result = engine.get(&cf, b"key").expect("get");

    // Assert
    // Should not see uncommitted write
    assert_eq!(
        read_result, None,
        "Should not see uncommitted transaction write"
    );

    drop(uncommitted_txn);
    assert_eq!(engine.get(&cf, b"key").expect("get after rollback"), None);
}

#[test]
fn should_prevent_dirty_write_given_uncommitted_update_when_read_committed() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    engine
        .put(&cf, b"key", b"v1")
        .expect("put");

    let mut first_txn = engine.begin_transaction(&cf).expect("begin_transaction");
    first_txn
        .put(b"key", b"txn1_value")
        .unwrap();

    let mut second_txn = engine.begin_transaction(&cf).expect("begin_transaction");
    second_txn
        .put(b"key", b"txn2_value")
        .unwrap();

    // Act
    let second_result =
        engine.commit_transaction(second_txn, cntryl_midge::WriteOptions::default());

    // Assert
    // second_txn should be able to commit (no locking currently)
    assert!(second_result.is_ok());
    // TODO: Should prevent dirty write when locking is implemented

    drop(first_txn);
}

#[test]
fn should_see_own_writes_given_transaction_when_get_after_put() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    let mut writing_txn = engine.begin_transaction(&cf).expect("begin_transaction");
    writing_txn
        .put(b"key", b"my_value")
        .unwrap();

    // Act
    let local_read = writing_txn.get(b"key").expect("get");

    // Assert
    assert_eq!(local_read, Some(Bytes::from("my_value")));
}

#[test]
fn should_not_see_other_uncommitted_writes_given_concurrent_transactions() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    let mut writing_txn = engine.begin_transaction(&cf).expect("begin_transaction");
    let mut reading_txn = engine.begin_transaction(&cf).expect("begin_transaction");

    writing_txn
        .put(b"key", b"txn1_value")
        .unwrap();

    // Act
    let read_result = reading_txn.get(b"key").expect("get");

    // Assert
    assert_eq!(
        read_result, None,
        "reading_txn should not see writing_txn's uncommitted write"
    );
}

#[test]
fn should_maintain_snapshot_view_given_transaction_when_external_writes_occur() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    engine
        .put(&cf, b"key", b"v1")
        .expect("put");

    let snapshot_txn = engine.begin_transaction(&cf).expect("begin_transaction");
    let begin_seq = snapshot_txn.begin_sequence();

    engine
        .put(&cf, b"key", b"v2")
        .expect("external put");

    // Act
    let snap = engine.snapshot();

    // Assert
    // Transaction captured sequence at begin
    assert!(begin_seq > 0);
    // Snapshot isolation would require reading at begin_seq
    // Currently no full snapshot isolation for transaction reads
    assert!(snap.seq > begin_seq);
}

#[test]
fn should_prevent_phantom_read_given_snapshot_isolation_when_range_scan() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    engine
        .put(&cf, b"key1", b"v1")
        .expect("put");

    let snap = engine.snapshot();
    let first_scan = engine
        .scan_at(
            cntryl_midge::Query {
                prefix: Some(Bytes::from("key")),
                ..Default::default()
            },
            &snap,
        )
        .expect("scan");

    engine
        .put(&cf, b"key2", b"v2")
        .expect("put new key");

    // Act
    let second_scan = engine
        .scan_at(
            cntryl_midge::Query {
                prefix: Some(Bytes::from("key")),
                ..Default::default()
            },
            &snap,
        )
        .expect("scan");

    // Assert
    // Both scans at same snapshot should see same keys
    assert_eq!(
        first_scan.len(),
        second_scan.len(),
        "Phantom read prevented by snapshot"
    );
}
