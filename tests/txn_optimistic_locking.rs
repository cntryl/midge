// Optimistic Locking Tests
// Tests transaction conflict detection and optimistic concurrency control

use cntryl_midge::KvTransaction;
use std::sync::Arc;

mod common;
use common::new_engine;

#[test]
fn should_commit_transaction_given_no_conflicts() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    engine.put(&cf, b"key", b"v1").expect("put");

    // Act - Start transaction, read key, modify different key, then commit
    let mut txn = engine.begin_transaction(&cf).expect("begin");
    let _read_value = txn.get(b"key").expect("get");
    txn.put(b"other_key", b"value").expect("put");

    let result = engine.commit_transaction(txn, cntryl_midge::WriteOptions::default());

    // Assert - Should commit successfully
    assert!(
        result.is_ok(),
        "Transaction without conflicts should commit"
    );
}

#[test]
fn should_commit_transaction_given_concurrent_modifications_to_different_keys() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    engine.put(&cf, b"key", b"initial").expect("put");

    // Act - Start transaction, read one key, concurrently modify a different key
    let mut txn = engine.begin_transaction(&cf).expect("begin");
    let _value = txn.get(b"key").expect("get");

    // Concurrent modification to different key
    engine.put(&cf, b"other_key", b"modified").expect("put");

    // Transaction writes to yet another key
    txn.put(b"txn_key", b"txn_value").expect("put");

    let result = engine.commit_transaction(txn, cntryl_midge::WriteOptions::default());

    // Assert - Should succeed (no write-write conflict)
    assert!(result.is_ok(), "No conflict on different keys");
}

#[test]
fn should_read_values_within_transaction() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    engine.put(&cf, b"k1", b"v1").expect("put");
    engine.put(&cf, b"k2", b"v2").expect("put");

    // Act - Read multiple keys within transaction
    let mut txn = engine.begin_transaction(&cf).expect("begin");
    let v1 = txn.get(b"k1").expect("get");
    let v2 = txn.get(b"k2").expect("get");

    // Assert - Transaction should provide snapshot isolation
    assert!(v1.is_some(), "Should read k1");
    assert!(v2.is_some(), "Should read k2");
}

#[test]
fn should_commit_new_key_given_clean_transaction() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    // Act - Create transaction and write new key
    let mut txn = engine.begin_transaction(&cf).expect("begin");
    txn.put(b"new_key", b"new_value").expect("put");

    let result = engine.commit_transaction(txn, cntryl_midge::WriteOptions::default());

    // Assert - Should commit successfully
    assert!(result.is_ok(), "Clean transaction should commit");
    assert_eq!(
        engine.get(&cf, b"new_key").expect("get"),
        Some(bytes::Bytes::from("new_value"))
    );
}

#[test]
fn should_handle_high_concurrency_optimistic_locking() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    // Pre-populate 10 keys
    for i in 0..10 {
        let key = format!("opt_key_{}", i);
        engine.put(&cf, key.as_bytes(), b"v0").unwrap();
    }

    // Act: Spawn 20 threads with overlapping reads and writes
    let handles: Vec<_> = (0..20)
        .map(|thread_id| {
            let eng = engine.clone();
            let cf_clone = cf.clone();
            std::thread::spawn(move || {
                for iteration in 0..10 {
                    let key_index = (thread_id * iteration) % 10;
                    let key = format!("opt_key_{}", key_index);

                    let mut txn = eng.begin_transaction(&cf_clone).unwrap();
                    let _ = txn.get(key.as_bytes());
                    txn.put(
                        key.as_bytes(),
                        format!("t{}_i{}", thread_id, iteration).as_bytes(),
                    )
                    .unwrap();
                    let _ = eng.commit_transaction(txn, cntryl_midge::WriteOptions::default());
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Assert: All keys should still be readable
    for i in 0..10 {
        let key = format!("opt_key_{}", i);
        assert!(
            engine.get(&cf, key.as_bytes()).is_ok(),
            "Key {} should be readable",
            key
        );
    }
}

#[test]
fn should_maintain_optimistic_locking_under_recovery() {
    // Arrange
    let dir = common::test_temp_dir();
    let opts = common::durability_opts(dir.path().to_path_buf());
    let engine = cntryl_midge::MidgeEngine::open(opts.clone()).expect("initial open");
    let cf = engine.default_column_family();

    // Pre-populate and perform transactions
    for i in 0..5 {
        let key = format!("recovery_opt_key_{}", i);
        engine.put(&cf, key.as_bytes(), b"initial").unwrap();
    }

    for i in 0..5 {
        let key = format!("recovery_opt_key_{}", i);
        let mut txn = engine.begin_transaction(&cf).unwrap();
        let _ = txn.get(key.as_bytes());
        txn.put(key.as_bytes(), format!("updated_{}", i).as_bytes())
            .unwrap();
        engine
            .commit_transaction(txn, cntryl_midge::WriteOptions::default())
            .unwrap();
    }

    drop(engine);

    // Act: Restart and verify optimistic locking state persisted
    let engine = cntryl_midge::MidgeEngine::open(opts).expect("restart open");
    let cf = engine.default_column_family();

    // Assert: All values should persist
    for i in 0..5 {
        let key = format!("recovery_opt_key_{}", i);
        let result = engine.get(&cf, key.as_bytes()).unwrap();
        assert!(
            result.is_some(),
            "Optimistically locked key {} should persist",
            key
        );
    }
}
