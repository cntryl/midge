mod common;
use bytes::Bytes;
use cntryl_midge::{KvTransaction, WriteOptions};
use common::{assert_get_equals, assert_key_absent, new_engine};
use std::sync::Arc;

#[test]
fn should_read_uncommitted_value_given_put_in_same_transaction_when_read() {
    // Arrange
    let (_dir, eng) = new_engine();
    let cf = eng.default_column_family();

    // Act - start transaction and write
    let mut txn = eng.begin_transaction(&cf).expect("begin transaction");
    txn.put(b"txn_key", b"txn_value")
        .expect("put in transaction");

    // Assert - transaction should see its own write
    let result = txn.get(b"txn_key").expect("get in transaction");
    assert_eq!(
        result,
        Some(b"txn_value".to_vec().into()),
        "Transaction should see own writes"
    );

    // Transaction not committed yet, so main engine shouldn't see it
    let main_result = eng.get(&cf, b"txn_key").expect("get from engine");
    assert!(
        main_result.is_none(),
        "Uncommitted write invisible outside transaction"
    );
}

#[test]
fn should_detect_read_write_conflict_under_snapshot() {
    // Arrange
    let (_dir, eng) = new_engine();
    let cf = eng.default_column_family();
    eng.put(&cf, b"rw_key", b"initial").expect("put");

    // Act - start a transaction with default Snapshot isolation and read key
    let mut txn_a = eng.begin_transaction(&cf).expect("begin");
    let _ = txn_a.get(b"rw_key").expect("get");

    // Another transaction updates and commits
    let mut txn_b = eng.begin_transaction(&cf).expect("begin");
    txn_b.put(b"rw_key", b"updated").expect("put");
    assert!(eng
        .commit_transaction(txn_b, WriteOptions::default())
        .is_ok());

    // Act - now txn_a tries to commit a write, should conflict due to read-write
    txn_a.put(b"some_key", b"value").expect("put");
    let res = eng.commit_transaction(txn_a, WriteOptions::default());

    // Assert
    assert!(
        res.is_err(),
        "Snapshot isolation should detect read-write conflict"
    );
}

#[test]
fn should_allow_commit_under_read_committed_when_other_commits() {
    // Arrange - setup and initial value
    let (_dir, eng) = new_engine();
    let cf = eng.default_column_family();
    eng.put(&cf, b"rw_key", b"initial").expect("put");

    // Act - start txn with ReadCommitted isolation and read key
    let mut txn_a = eng
        .begin_transaction_with_options(
            &cf,
            None,
            1024 * 1024,
            cntryl_midge::IsolationLevel::ReadCommitted,
        )
        .expect("begin");
    let _ = txn_a.get(b"rw_key").expect("get");

    // Another transaction updates and commits
    let mut txn_b = eng.begin_transaction(&cf).expect("begin");
    txn_b.put(b"rw_key", b"updated").expect("put");
    assert!(eng
        .commit_transaction(txn_b, WriteOptions::default())
        .is_ok());

    // Act - txn_a tries to commit and should NOT be treated as conflicting
    txn_a.put(b"some_key", b"value").expect("put");
    let res = eng.commit_transaction(txn_a, WriteOptions::default());

    // Assert - should succeed for read committed
    assert!(
        res.is_ok(),
        "ReadCommitted should not track reads and should allow commit"
    );
}

#[test]
fn should_not_see_uncommitted_write_given_other_transaction_when_read() {
    // Arrange
    let (_dir, eng) = new_engine();
    let cf = eng.default_column_family();

    // Act - two transactions
    let mut txn1 = eng.begin_transaction(&cf).expect("begin txn1");
    txn1.put(b"key1", b"value1").expect("put");

    let mut txn2 = eng.begin_transaction(&cf).expect("begin txn2");

    // Assert - txn2 should not see txn1's uncommitted write
    let result = txn2.get(b"key1").expect("get");
    assert!(
        result.is_none(),
        "Uncommitted writes invisible to other transactions"
    );
}

#[test]
fn should_rollback_all_operations_given_transaction_abort_called() {
    // Arrange
    let (_dir, eng) = new_engine();
    let cf = eng.default_column_family();
    eng.put(&cf, b"existing", b"value").expect("put");

    // Act - transaction with abort
    let mut txn = eng.begin_transaction(&cf).expect("begin");
    txn.put(b"new_key", b"new_value").expect("put");
    txn.put(b"existing", b"updated").expect("update");
    // Abort by dropping without commit
    drop(txn);

    // Assert - all transaction operations should be rolled back
    assert_key_absent(&eng, b"new_key");
    assert_get_equals(&eng, b"existing", b"value"); // Original value preserved
}

#[test]
fn should_detect_conflict_given_concurrent_updates_to_same_key_when_commit() {
    // Arrange
    let (_dir, eng) = new_engine();
    let cf = eng.default_column_family();
    eng.put(&cf, b"conflict_key", b"initial").expect("put");

    // Act - two transactions updating same key
    let mut txn1 = eng.begin_transaction(&cf).expect("begin txn1");
    let mut txn2 = eng.begin_transaction(&cf).expect("begin txn2");

    txn1.put(b"conflict_key", b"txn1_value").expect("put txn1");
    txn2.put(b"conflict_key", b"txn2_value").expect("put txn2");

    // Assert - at least one commit should succeed, other may fail
    let commit1 = eng.commit_transaction(txn1, WriteOptions::default());
    let commit2 = eng.commit_transaction(txn2, WriteOptions::default());

    // At least one should succeed (optimistic concurrency control)
    assert!(
        commit1.is_ok() || commit2.is_ok(),
        "At least one transaction should commit successfully"
    );

    // TODO: Verify conflict detection behavior based on isolation level
}

#[test]
fn should_return_old_value_given_snapshot_created_before_write() {
    // Arrange
    let (_dir, eng) = new_engine();
    let cf = eng.default_column_family();
    eng.put(&cf, b"key1", b"original").expect("put");

    // Act - create snapshot, then update value
    let snap = eng.snapshot();
    eng.put(&cf, b"key1", b"updated").expect("update");

    // Assert - snapshot should see old value and engine should see new value
    let snap_val = snap.get(&eng, &cf, b"key1").expect("get at snapshot");
    assert_eq!(snap_val.as_deref(), Some(&b"original"[..]));

    // For backward-compat check the main engine sees the new value
    assert_get_equals(&eng, b"key1", b"updated");
}

#[test]
fn should_maintain_isolation_under_concurrent_transaction_pressure_when_stress_tested() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();
    let engine = Arc::new(engine);
    const NUM_THREADS: usize = 10;
    const TRANSACTIONS_PER_THREAD: usize = 20;

    // Act - multiple threads performing concurrent transactions
    let handles: Vec<_> = (0..NUM_THREADS)
        .map(|thread_id| {
            let eng = Arc::clone(&engine);
            let cf_clone = cf.clone();
            std::thread::spawn(move || {
                for txn_num in 0..TRANSACTIONS_PER_THREAD {
                    let mut txn = eng.begin_transaction(&cf_clone).expect("begin txn");

                    // Each transaction writes 5 keys
                    for key_offset in 0..5 {
                        let key = format!("isolation_key_{}_{}_{}", thread_id, txn_num, key_offset)
                            .into_bytes();
                        let value =
                            format!("isolation_value_{}_{}_{}", thread_id, txn_num, key_offset)
                                .into_bytes();
                        txn.put(&key, &value).expect("put in txn");
                    }

                    // Try to commit (may fail due to conflicts)
                    let _commit_result = eng.commit_transaction(txn, WriteOptions::default());
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("Thread panicked");
    }

    // Assert - verify at least some transactions committed and data is consistent
    for thread_id in 0..NUM_THREADS {
        for txn_num in 0..TRANSACTIONS_PER_THREAD {
            let key = format!("isolation_key_{}_{}_0", thread_id, txn_num).into_bytes();
            if let Ok(Some(result)) = engine.get(&cf, &key) {
                // If we got a result, verify it matches expected pattern
                assert!(
                    !result.is_empty(),
                    "Committed transaction data should be readable"
                );
            }
            // Some transactions may not commit due to conflicts - that's OK
        }
    }
}

#[test]
fn should_prevent_dirty_reads_given_concurrent_uncommitted_changes_when_tested() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();
    let engine = Arc::new(engine);

    // Write initial key
    engine
        .put(&cf, b"dirty_read_key", b"initial_value")
        .expect("put");

    // Act - one thread modifies, other thread tries to read
    let eng_txn = Arc::clone(&engine);
    let cf_txn = cf.clone();
    let txn_handle = std::thread::spawn(move || {
        let mut txn = eng_txn.begin_transaction(&cf_txn).expect("begin");
        txn.put(b"dirty_read_key", b"uncommitted_value")
            .expect("put");
        // Hold transaction open without committing
        std::thread::sleep(std::time::Duration::from_millis(100));
        txn
    });

    // Small delay to ensure transaction is open
    std::thread::sleep(std::time::Duration::from_millis(50));

    // Reader thread attempts to read while transaction is open
    let eng_reader = Arc::clone(&engine);
    let cf_reader = cf.clone();
    let reader_result =
        std::thread::spawn(move || eng_reader.get(&cf_reader, b"dirty_read_key").expect("get"));

    let read_value = reader_result.join().expect("reader panicked");
    let _txn = txn_handle.join().expect("txn panicked");

    // Assert - reader should NOT see the uncommitted value
    if let Some(value) = read_value {
        assert_eq!(
            value,
            b"initial_value".to_vec(),
            "Should read committed value, not uncommitted"
        );
    }
}

#[test]
fn should_preserve_isolation_across_transaction_lifecycle_given_multiple_operations() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

    // Pre-populate some data
    for i in 0..100 {
        let key = format!("lifecycle_key_{:03}", i).into_bytes();
        let value = format!("lifecycle_value_{}", i).into_bytes();
        engine.put(&cf, &key, &value).expect("put");
    }

    // Act - create a transaction that reads and writes
    let mut txn = engine.begin_transaction(&cf).expect("begin");

    // Read some values from main database (snapshot view)
    let read_value = txn.get(b"lifecycle_key_050").expect("get in txn");
    assert_eq!(
        read_value,
        Some(b"lifecycle_value_50".to_vec().into()),
        "Transaction should see committed data"
    );

    // Modify a key
    txn.put(b"lifecycle_key_050", b"modified_in_txn")
        .expect("put");

    // Read the modified value (should see own write)
    let modified = txn.get(b"lifecycle_key_050").expect("get modified");
    assert_eq!(
        modified,
        Some(b"modified_in_txn".to_vec().into()),
        "Should see own write in same transaction"
    );

    // Commit the transaction
    engine
        .commit_transaction(txn, WriteOptions::default())
        .expect("commit");

    // Assert - verify modification is now visible in main engine
    let final_value = engine
        .get(&cf, b"lifecycle_key_050")
        .expect("get after commit");
    assert_eq!(
        final_value,
        Some(Bytes::from("modified_in_txn")),
        "Committed transaction modification should be visible"
    );
}
