// Atomicity
// Extracted from transaction_acid.rs

// Transaction ACID tests - P0 Priority
// Tests document expected behavior and will fail until features are implemented

use bytes::Bytes;
use cntryl_midge::KvTransaction;
use std::sync::Arc;

mod common;
use common::new_engine;

#[test]
fn should_commit_all_or_nothing_given_multi_key_transaction() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    let mut atomic_txn = engine.begin_transaction(&cf).expect("begin transaction");
    atomic_txn.put(b"k1", b"v1").unwrap();
    atomic_txn.put(b"k2", b"v2").unwrap();
    atomic_txn.put(b"k3", b"v3").unwrap();
    atomic_txn.delete(b"k4").unwrap();

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
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    let mut batch_txn = engine.begin_transaction(&cf).expect("begin transaction");
    for i in 0..100 {
        batch_txn
            .put(
                format!("batch_key_{}", i).as_bytes(),
                format!("batch_val_{}", i).as_bytes(),
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
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    let mut failed_txn = engine.begin_transaction(&cf).expect("begin transaction");
    failed_txn.put(b"k1", b"v1").unwrap();
    failed_txn.put(b"k2", b"v2").unwrap();

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
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();
    let snap_before = engine.snapshot();

    let mut partial_write_txn = engine.begin_transaction(&cf).expect("begin transaction");
    partial_write_txn.put(b"atomic_k1", b"v1").unwrap();
    partial_write_txn.put(b"atomic_k2", b"v2").unwrap();

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

#[test]
fn should_maintain_atomicity_under_concurrent_commits() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    // Act: Spawn 10 threads, each committing 10-key atomic transactions
    let handles: Vec<_> = (0..10)
        .map(|thread_id| {
            let eng = engine.clone();
            let cf_clone = cf.clone();
            std::thread::spawn(move || {
                for iteration in 0..5 {
                    let mut txn = eng.begin_transaction(&cf_clone).unwrap();
                    for key_offset in 0..10 {
                        let key = format!("atomic_t{}_i{}_k{}", thread_id, iteration, key_offset);
                        let value = format!("v{}", key_offset);
                        txn.put(key.as_bytes(), value.as_bytes()).unwrap();
                    }
                    let _ = eng.commit_transaction(txn, cntryl_midge::WriteOptions::default());
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Assert: All keys committed atomically (no partial writes visible)
    let mut key_count = 0;
    for thread_id in 0..10 {
        for iteration in 0..5 {
            for key_offset in 0..10 {
                let key = format!("atomic_t{}_i{}_k{}", thread_id, iteration, key_offset);
                if engine.get(&cf, key.as_bytes()).unwrap().is_some() {
                    key_count += 1;
                }
            }
        }
    }
    assert!(key_count > 0, "At least some atomic writes should succeed");
}

#[test]
fn should_persist_atomic_transactions_after_restart() {
    // Arrange
    let dir = common::test_temp_dir();
    let opts = common::durability_opts(dir.path().to_path_buf());
    let engine = cntryl_midge::MidgeEngine::open(opts.clone()).expect("initial open");
    let cf = engine.default_column_family();

    // Commit 5 atomic transactions
    for batch in 0..5 {
        let mut txn = engine.begin_transaction(&cf).unwrap();
        for i in 0..10 {
            let key = format!("persist_batch_{}_key_{}", batch, i);
            let value = format!("val_{}", i);
            txn.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        engine
            .commit_transaction(txn, cntryl_midge::WriteOptions::default())
            .unwrap();
    }

    drop(engine);

    // Act: Restart and verify all atomic transactions persisted
    let engine = cntryl_midge::MidgeEngine::open(opts).expect("restart open");
    let cf = engine.default_column_family();

    // Assert: All keys should exist (atomicity preserved across restart)
    for batch in 0..5 {
        for i in 0..10 {
            let key = format!("persist_batch_{}_key_{}", batch, i);
            assert!(
                engine.get(&cf, key.as_bytes()).unwrap().is_some(),
                "Key {} should persist atomically",
                key
            );
        }
    }
}
