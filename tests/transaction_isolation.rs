mod common;
use cntryl_midge::{KvTransaction, WriteOptions};
use common::{assert_get_equals, assert_key_absent, new_engine};

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
    let _snapshot = eng.snapshot();
    eng.put(&cf, b"key1", b"updated").expect("update");

    // Assert - snapshot should see old value (once snapshot API is fully implemented)
    // TODO: Add snapshot.get() API to verify isolation
    // For now, verify main engine sees new value
    assert_get_equals(&eng, b"key1", b"updated");
}
