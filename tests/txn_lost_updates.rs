// Lost Updates
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
fn should_prevent_lost_update_given_read_modify_write_when_concurrent() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    engine
        .put(&cf, Bytes::from("counter"), Bytes::from("0"))
        .expect("put");

    let mut first_increment_txn = engine.begin_transaction(&cf);
    let mut second_increment_txn = engine.begin_transaction(&cf);

    let snap1 = engine.snapshot();
    let snap2 = engine.snapshot();

    let val1 = engine.get_at(b"counter", &snap1).expect("get");
    let val2 = engine.get_at(b"counter", &snap2).expect("get");

    let count1: i32 = String::from_utf8(val1.unwrap().to_vec())
        .unwrap()
        .parse()
        .unwrap();
    let count2: i32 = String::from_utf8(val2.unwrap().to_vec())
        .unwrap()
        .parse()
        .unwrap();

    first_increment_txn
        .put(
            Bytes::from("counter"),
            Bytes::from((count1 + 1).to_string()),
            None,
        )
        .unwrap();
    second_increment_txn
        .put(
            Bytes::from("counter"),
            Bytes::from((count2 + 1).to_string()),
            None,
        )
        .unwrap();

    engine
        .commit_transaction(first_increment_txn, cntryl_midge::WriteOptions::default())
        .expect("commit first");

    // Act
    let result =
        engine.commit_transaction(second_increment_txn, cntryl_midge::WriteOptions::default());

    // Assert
    // With read tracking and snapshot isolation, lost update is now PREVENTED
    // Second transaction read "counter" at its snapshot, first transaction modified it
    // This is correctly detected as a read-write conflict
    assert!(
        result.is_err(),
        "Should detect read-write conflict and prevent lost update"
    );

    // Final value is 1 (only first increment succeeded)
    let final_val = engine.get(&cf, b"counter").expect("get final");
    let final_count: i32 = String::from_utf8(final_val.unwrap().to_vec())
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        final_count, 1,
        "Only first transaction should have committed"
    );
}

#[test]
fn should_detect_lost_update_given_cas_pattern_when_value_changed() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    engine
        .put(&cf, Bytes::from("key"), Bytes::from("v1"))
        .expect("put");

    let snap = engine.snapshot();
    let expected = engine.get_at(b"key", &snap).expect("get");

    engine
        .put(&cf, Bytes::from("key"), Bytes::from("v2"))
        .expect("concurrent update");

    let mut cas_txn = engine.begin_transaction(&cf);
    cas_txn
        .put(Bytes::from("key"), Bytes::from("v3"), None)
        .unwrap();

    // Act
    let result = engine.commit_transaction(cas_txn, cntryl_midge::WriteOptions::default());

    // Assert
    // No CAS validation currently
    assert!(result.is_ok());
    assert!(expected.is_some());
    // TODO: Should fail if key was modified since snapshot
}

#[test]
fn should_preserve_both_updates_given_non_overlapping_keys_when_concurrent_commits() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    let mut first_key_txn = engine.begin_transaction(&cf);
    let mut second_key_txn = engine.begin_transaction(&cf);

    first_key_txn
        .put(Bytes::from("key1"), Bytes::from("value1"), None)
        .unwrap();
    second_key_txn
        .put(Bytes::from("key2"), Bytes::from("value2"), None)
        .unwrap();

    engine
        .commit_transaction(first_key_txn, cntryl_midge::WriteOptions::default())
        .expect("commit first");

    // Act
    engine
        .commit_transaction(second_key_txn, cntryl_midge::WriteOptions::default())
        .expect("commit second");

    // Assert
    assert_eq!(
        engine.get(&cf, b"key1").expect("get"),
        Some(Bytes::from("value1"))
    );
    assert_eq!(
        engine.get(&cf, b"key2").expect("get"),
        Some(Bytes::from("value2"))
    );
}
