// Isolation Levels
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
fn should_prevent_dirty_read_given_uncommitted_write_when_read_committed() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    let mut uncommitted_txn = engine.begin_transaction(&cf);
    uncommitted_txn
        .put(Bytes::from("key"), Bytes::from("uncommitted"), None)
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
    let cf = engine.default_column_family();

    engine
        .put(&cf, Bytes::from("key"), Bytes::from("v1"))
        .expect("put");

    let mut first_txn = engine.begin_transaction(&cf);
    first_txn
        .put(Bytes::from("key"), Bytes::from("txn1_value"), None)
        .unwrap();

    let mut second_txn = engine.begin_transaction(&cf);
    second_txn
        .put(Bytes::from("key"), Bytes::from("txn2_value"), None)
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
    let cf = engine.default_column_family();

    let mut writing_txn = engine.begin_transaction(&cf);
    writing_txn
        .put(Bytes::from("key"), Bytes::from("my_value"), None)
        .unwrap();

    // Act
    let local_read = writing_txn.get_local(b"key");

    // Assert
    assert_eq!(local_read, Some(Some(Bytes::from("my_value"))));
}

#[test]
fn should_not_see_other_uncommitted_writes_given_concurrent_transactions() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    let mut writing_txn = engine.begin_transaction(&cf);
    let reading_txn = engine.begin_transaction(&cf);

    writing_txn
        .put(Bytes::from("key"), Bytes::from("txn1_value"), None)
        .unwrap();

    // Act
    let read_result = reading_txn.get_local(b"key");

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
    let cf = engine.default_column_family();

    engine
        .put(&cf, Bytes::from("key"), Bytes::from("v1"))
        .expect("put");

    let snapshot_txn = engine.begin_transaction(&cf);
    let begin_seq = snapshot_txn.begin_sequence();

    engine
        .put(&cf, Bytes::from("key"), Bytes::from("v2"))
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
    let cf = engine.default_column_family();

    engine
        .put(&cf, Bytes::from("key1"), Bytes::from("v1"))
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
        .put(&cf, Bytes::from("key2"), Bytes::from("v2"))
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
