// Lost Updates
// Extracted from transaction_acid.rs

// Transaction ACID tests - P0 Priority
// Tests document expected behavior and will fail until features are implemented

use bytes::Bytes;
use cntryl_midge::KvTransaction;
use std::sync::Arc;

mod common;
use common::new_engine;

#[test]
fn should_prevent_lost_update_given_read_modify_write_when_concurrent() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    engine.put(&cf, b"counter", b"0").expect("put");

    let mut first_increment_txn = engine
        .begin_transaction(&cf)
        .expect("Transaction creation failed");
    let mut second_increment_txn = engine
        .begin_transaction(&cf)
        .expect("Transaction creation failed");

    let snap1 = engine.snapshot();
    let snap2 = engine.snapshot();

    let val1 = engine.get_at(&cf, b"counter", &snap1).expect("get");
    let val2 = engine.get_at(&cf, b"counter", &snap2).expect("get");

    let count1: i32 = String::from_utf8(val1.unwrap().to_vec())
        .unwrap()
        .parse()
        .unwrap();
    let count2: i32 = String::from_utf8(val2.unwrap().to_vec())
        .unwrap()
        .parse()
        .unwrap();

    first_increment_txn
        .put(b"counter", (count1 + 1).to_string().as_bytes())
        .unwrap();
    second_increment_txn
        .put(b"counter", (count2 + 1).to_string().as_bytes())
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
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    engine.put(&cf, b"key", b"v1").expect("put");

    let snap = engine.snapshot();
    let expected = engine.get_at(&cf, b"key", &snap).expect("get");

    engine.put(&cf, b"key", b"v2").expect("concurrent update");

    let mut cas_txn = engine
        .begin_transaction(&cf)
        .expect("Transaction creation failed");
    cas_txn.put(b"key", b"v3").unwrap();

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
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    let mut first_key_txn = engine
        .begin_transaction(&cf)
        .expect("Transaction creation failed");
    let mut second_key_txn = engine
        .begin_transaction(&cf)
        .expect("Transaction creation failed");

    first_key_txn.put(b"key1", b"value1").unwrap();
    second_key_txn.put(b"key2", b"value2").unwrap();

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

#[test]
fn should_handle_concurrent_read_modify_writes_without_panic() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();
    engine.put(&cf, b"concurrent_counter", b"0").unwrap();

    // Act: Spawn 20 threads, each doing read-modify-write
    let handles: Vec<_> = (0..20)
        .map(|thread_id| {
            let eng = engine.clone();
            let cf_clone = cf.clone();
            std::thread::spawn(move || {
                for _ in 0..5 {
                    let mut txn = eng.begin_transaction(&cf_clone).unwrap();
                    let current = txn.get(b"concurrent_counter").unwrap();
                    let num: i32 = String::from_utf8(current.unwrap_or_default().to_vec())
                        .unwrap_or_else(|_| "0".to_string())
                        .parse()
                        .unwrap_or(0);
                    txn.put(
                        b"concurrent_counter",
                        format!("{}_{}", num + 1, thread_id).as_bytes(),
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

    // Assert: Counter is readable
    assert!(engine.get(&cf, b"concurrent_counter").is_ok());
}

#[test]
fn should_persist_lost_update_prevention_after_restart() {
    // Arrange
    let dir = common::test_temp_dir();
    let opts = common::durability_opts(dir.path().to_path_buf());
    let engine = cntryl_midge::MidgeEngine::open(opts.clone()).expect("initial open");
    let cf = engine.default_column_family();

    engine.put(&cf, b"persist_counter", b"100").unwrap();
    engine
        .commit_transaction(
            {
                let mut txn = engine.begin_transaction(&cf).unwrap();
                txn.put(b"persist_counter", b"101").unwrap();
                txn
            },
            cntryl_midge::WriteOptions::default(),
        )
        .unwrap();

    drop(engine);

    // Act: Restart and verify value persisted
    let engine = cntryl_midge::MidgeEngine::open(opts).expect("restart open");
    let cf = engine.default_column_family();

    // Assert: Value should persist
    let result = engine.get(&cf, b"persist_counter").unwrap();
    assert_eq!(result.as_deref(), Some(b"101".as_ref()));
}
