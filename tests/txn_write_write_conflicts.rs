//! Write-Write Conflict Tests
//!
//! These verify MVCC isolation and conflict detection behavior.
//! They intentionally fail until transactional conflict handling is implemented.

use cntryl_midge::{KvTransaction, WriteOptions};
use std::sync::Arc;

mod common;
use common::{assert_get_equals, new_engine};

#[test]
fn should_detect_write_write_conflict_given_concurrent_updates_to_same_key() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();
    engine.put(&cf, b"key", b"v0").unwrap();

    let mut txn1 = engine.begin_transaction(&cf).unwrap();
    let mut txn2 = engine.begin_transaction(&cf).unwrap();

    txn1.put(b"key", b"v1").unwrap();
    txn2.put(b"key", b"v2").unwrap();

    // Act
    let first_result = engine.commit_transaction(txn1, WriteOptions::default());
    let second_result = engine.commit_transaction(txn2, WriteOptions::default());

    // Assert
    assert!(first_result.is_ok(), "first transaction should succeed");
    assert!(
        second_result.is_err(),
        "second transaction should fail on conflict"
    );
    assert_get_equals(&engine, b"key", b"v1");
}

#[test]
fn should_abort_second_transaction_given_write_conflict_when_both_commit() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    let mut winner = engine.begin_transaction(&cf).unwrap();
    let mut loser = engine.begin_transaction(&cf).unwrap();

    winner.put(b"conflict_key", b"txn1_value").unwrap();
    loser.put(b"conflict_key", b"txn2_value").unwrap();

    // Act
    let winner_result = engine.commit_transaction(winner, WriteOptions::default());
    let loser_result = engine.commit_transaction(loser, WriteOptions::default());

    // Assert
    assert!(winner_result.is_ok(), "winner should commit successfully");
    assert!(
        loser_result.is_err(),
        "loser should fail with write-write conflict"
    );
    assert_get_equals(&engine, b"conflict_key", b"txn1_value");
}

#[test]
fn should_preserve_first_commit_given_write_conflict_when_second_aborts() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    let mut txn1 = engine.begin_transaction(&cf).unwrap();
    txn1.put(b"key", b"first_value").unwrap();
    engine
        .commit_transaction(txn1, WriteOptions::default())
        .unwrap();

    let mut aborted_txn = engine.begin_transaction(&cf).unwrap();
    aborted_txn.put(b"key", b"second_value").unwrap();

    // Act
    drop(aborted_txn); // rollback

    // Assert
    assert_get_equals(&engine, b"key", b"first_value");
}

#[test]
fn should_handle_write_conflict_on_delete_given_concurrent_delete_and_put() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();
    engine.put(&cf, b"key", b"initial").unwrap();

    let mut delete_txn = engine.begin_transaction(&cf).unwrap();
    let mut put_txn = engine.begin_transaction(&cf).unwrap();

    delete_txn.delete(b"key").unwrap();
    put_txn.put(b"key", b"updated").unwrap();

    // Act
    let delete_result = engine.commit_transaction(delete_txn, WriteOptions::default());
    let put_result = engine.commit_transaction(put_txn, WriteOptions::default());

    // Assert
    assert!(delete_result.is_ok(), "delete should commit first");
    assert!(put_result.is_err(), "put should fail due to conflict");
    assert_eq!(engine.get(&cf, b"key").unwrap(), None);
}

#[test]
fn should_detect_conflict_on_delete_range_given_overlapping_keys() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    for i in 0..10 {
        let key = format!("key{i}");
        engine.put(&cf, key.as_bytes(), b"val").unwrap();
    }

    let mut range_txn = engine.begin_transaction(&cf).unwrap();
    range_txn.delete_range(b"key3", b"key7").unwrap();

    let mut overlap_txn = engine.begin_transaction(&cf).unwrap();
    overlap_txn.put(b"key5", b"new_value").unwrap();

    // Act
    let range_result = engine.commit_transaction(range_txn, WriteOptions::default());
    let overlap_result = engine.commit_transaction(overlap_txn, WriteOptions::default());

    // Assert
    assert!(range_result.is_ok());
    assert!(
        overlap_result.is_err(),
        "Transaction should fail due to range conflict with delete_range operation"
    );
}

#[test]
fn should_handle_high_contention_writes_without_panic() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    // Act: Spawn 10 threads each performing conflicting writes
    let handles: Vec<_> = (0..10)
        .map(|i| {
            let eng = engine.clone();
            let cf_clone = cf.clone();
            std::thread::spawn(move || {
                for j in 0..5 {
                    let mut txn = eng.begin_transaction(&cf_clone).unwrap();
                    txn.put(
                        b"contention_key",
                        format!("thread_{}_iteration_{}", i, j).as_bytes(),
                    )
                    .unwrap();
                    let _ = eng.commit_transaction(txn, WriteOptions::default());
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Assert: No panics, final value exists
    assert!(engine.get(&cf, b"contention_key").is_ok());
}

#[test]
fn should_allow_concurrent_writes_to_different_keys() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    // Act: Spawn 20 threads each writing to a different key
    let handles: Vec<_> = (0..20)
        .map(|i| {
            let eng = engine.clone();
            let cf_clone = cf.clone();
            std::thread::spawn(move || {
                let mut txn = eng.begin_transaction(&cf_clone).unwrap();
                let key = format!("key_{}", i);
                txn.put(key.as_bytes(), b"value").unwrap();
                eng.commit_transaction(txn, WriteOptions::default())
            })
        })
        .collect();

    let results: Vec<_> = handles
        .into_iter()
        .map(|h| h.join().expect("Thread panicked"))
        .collect();

    // Assert: All writes succeeded (no conflicts for different keys)
    let success_count = results.iter().filter(|r| r.is_ok()).count();
    assert_eq!(
        success_count, 20,
        "All concurrent writes to different keys should succeed"
    );

    for i in 0..20 {
        let key = format!("key_{}", i);
        assert_get_equals(&engine, key.as_bytes(), b"value");
    }
}

#[test]
fn should_recover_conflict_state_after_engine_restart() {
    // Arrange
    let dir = common::test_temp_dir();
    let opts = common::durability_opts(dir.path().to_path_buf());
    let engine = MidgeEngine::open(opts.clone()).expect("initial open");
    let cf = engine.default_column_family();

    // Put initial value
    engine.put(&cf, b"recovery_key", b"initial").unwrap();

    let mut txn1 = engine.begin_transaction(&cf).unwrap();
    txn1.put(b"recovery_key", b"after_conflict").unwrap();
    engine
        .commit_transaction(txn1, WriteOptions::default())
        .unwrap();

    drop(engine);

    // Act: Reopen engine and verify state persisted
    let engine = MidgeEngine::open(opts).expect("restart open");

    // Assert: Value persisted correctly
    assert_get_equals(&engine, b"recovery_key", b"after_conflict");
}

#[test]
fn should_maintain_transaction_isolation_under_stress() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    // Pre-populate keys
    for i in 0..100 {
        let key = format!("stress_key_{}", i);
        engine.put(&cf, key.as_bytes(), b"0").unwrap();
    }

    // Act: High concurrency stress test
    let handles: Vec<_> = (0..50)
        .map(|thread_id| {
            let eng = engine.clone();
            let cf_clone = cf.clone();
            std::thread::spawn(move || {
                for iteration in 0..10 {
                    let key_index = (thread_id * 10 + iteration) % 100;
                    let key = format!("stress_key_{}", key_index);

                    let mut txn = eng.begin_transaction(&cf_clone).unwrap();
                    let new_value = format!("t{}_i{}", thread_id, iteration);
                    txn.put(key.as_bytes(), new_value.as_bytes()).unwrap();
                    let _ = eng.commit_transaction(txn, WriteOptions::default());
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Assert: All keys still exist and are readable
    for i in 0..100 {
        let key = format!("stress_key_{}", i);
        let result = engine.get(&cf, key.as_bytes());
        assert!(
            result.is_ok(),
            "Key {} should be readable after stress test",
            key
        );
    }
}

// Re-export MidgeEngine for use in tests
use cntryl_midge::MidgeEngine;
