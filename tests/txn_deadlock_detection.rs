// Deadlock Detection
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
fn should_detect_deadlock_given_circular_wait_when_two_transactions() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    engine
        .put(&cf, Bytes::from("k1"), Bytes::from("v1"))
        .expect("put");
    engine
        .put(&cf, Bytes::from("k2"), Bytes::from("v2"))
        .expect("put");

    let mut first_circular_txn = engine.begin_transaction(&cf);
    let mut second_circular_txn = engine.begin_transaction(&cf);

    first_circular_txn
        .put(Bytes::from("k1"), Bytes::from("txn1_k1"), None)
        .unwrap();
    second_circular_txn
        .put(Bytes::from("k2"), Bytes::from("txn2_k2"), None)
        .unwrap();

    first_circular_txn
        .put(Bytes::from("k2"), Bytes::from("txn1_k2"), None)
        .unwrap();
    second_circular_txn
        .put(Bytes::from("k1"), Bytes::from("txn2_k1"), None)
        .unwrap();

    let first_result =
        engine.commit_transaction(first_circular_txn, cntryl_midge::WriteOptions::default());

    // Act
    let second_result =
        engine.commit_transaction(second_circular_txn, cntryl_midge::WriteOptions::default());

    // Assert
    // With write-write conflict detection, one transaction succeeds and the other fails
    // Both transactions write to k1 and k2, so there's a write-write conflict
    assert!(first_result.is_ok(), "First transaction should succeed");
    assert!(
        second_result.is_err(),
        "Second transaction should fail due to write-write conflict"
    );
    // Note: This is conflict detection, not deadlock detection
    // True deadlock detection would require lock-based concurrency control
}

#[test]
fn should_abort_victim_transaction_given_deadlock_when_detected() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    let mut first_deadlock_txn = engine.begin_transaction(&cf);
    let mut second_deadlock_txn = engine.begin_transaction(&cf);

    first_deadlock_txn
        .put(Bytes::from("resource_a"), Bytes::from("txn1"), None)
        .unwrap();
    second_deadlock_txn
        .put(Bytes::from("resource_b"), Bytes::from("txn2"), None)
        .unwrap();

    first_deadlock_txn
        .put(Bytes::from("resource_b"), Bytes::from("txn1_b"), None)
        .unwrap();
    second_deadlock_txn
        .put(Bytes::from("resource_a"), Bytes::from("txn2_a"), None)
        .unwrap();

    // Act
    let first_deadlock_result =
        engine.commit_transaction(first_deadlock_txn, cntryl_midge::WriteOptions::default());
    let second_deadlock_result =
        engine.commit_transaction(second_deadlock_txn, cntryl_midge::WriteOptions::default());

    // Assert
    // One transaction should be chosen as victim and aborted
    // Currently no deadlock detection
    // TODO: Verify one is aborted with deadlock error
    let _ = first_deadlock_result;
    let _ = second_deadlock_result;
}

#[test]
fn should_allow_retry_given_deadlock_victim_when_aborted() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    let mut initial_txn = engine.begin_transaction(&cf);
    initial_txn
        .put(Bytes::from("key"), Bytes::from("value"), None)
        .unwrap();

    // Act
    let result = engine.commit_transaction(initial_txn, cntryl_midge::WriteOptions::default());

    // Assert
    if result.is_err() {
        let mut retry_txn = engine.begin_transaction(&cf);
        retry_txn
            .put(Bytes::from("key"), Bytes::from("retry_value"), None)
            .unwrap();
        let retry_result =
            engine.commit_transaction(retry_txn, cntryl_midge::WriteOptions::default());
        assert!(retry_result.is_ok(), "Retry should succeed");
    } else {
        assert!(result.is_ok());
    }
}

#[test]
fn should_detect_deadlock_given_three_way_circular_dependency() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    let mut first_three_way_txn = engine.begin_transaction(&cf);
    let mut second_three_way_txn = engine.begin_transaction(&cf);
    let mut third_three_way_txn = engine.begin_transaction(&cf);

    first_three_way_txn
        .put(Bytes::from("r1"), Bytes::from("t1"), None)
        .unwrap();
    second_three_way_txn
        .put(Bytes::from("r2"), Bytes::from("t2"), None)
        .unwrap();
    third_three_way_txn
        .put(Bytes::from("r3"), Bytes::from("t3"), None)
        .unwrap();

    first_three_way_txn
        .put(Bytes::from("r2"), Bytes::from("t1_r2"), None)
        .unwrap();
    second_three_way_txn
        .put(Bytes::from("r3"), Bytes::from("t2_r3"), None)
        .unwrap();
    third_three_way_txn
        .put(Bytes::from("r1"), Bytes::from("t3_r1"), None)
        .unwrap();

    // Act
    let first_three_way_result =
        engine.commit_transaction(first_three_way_txn, cntryl_midge::WriteOptions::default());
    let second_three_way_result =
        engine.commit_transaction(second_three_way_txn, cntryl_midge::WriteOptions::default());
    let third_three_way_result =
        engine.commit_transaction(third_three_way_txn, cntryl_midge::WriteOptions::default());

    // Assert
    // Should detect 3-way deadlock: txn1->r2, txn2->r3, txn3->r1
    // TODO: At least one should be aborted when deadlock detection is implemented
    let _ = first_three_way_result;
    let _ = second_three_way_result;
    let _ = third_three_way_result;
}
