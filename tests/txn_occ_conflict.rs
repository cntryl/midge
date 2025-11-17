use cntryl_midge::KvTransaction;
use std::sync::Arc;

mod common;
use common::new_engine;

#[test]
fn should_reject_second_committer_on_write_write_conflict() {
    // Arrange
    let (_dir, engine) = new_engine();
    let engine = Arc::new(engine);
    let cf = engine.default_column_family();

    let mut first_txn = engine
        .begin_transaction(&cf)
        .expect("Transaction creation failed");
    let mut second_txn = engine
        .begin_transaction(&cf)
        .expect("Transaction creation failed");

    first_txn.put(b"conflict_key", b"txn1_val").unwrap();
    second_txn.put(b"conflict_key", b"txn2_val").unwrap();

    // Act
    let first_result = engine.commit_transaction(first_txn, cntryl_midge::WriteOptions::default());
    let second_result =
        engine.commit_transaction(second_txn, cntryl_midge::WriteOptions::default());

    // Assert
    // With optimistic conflict detection, the second committer should be rejected due to write-write conflict.
    // If OCC isn't yet implemented, this test will fail — it should be enabled once the TransactionController
    // enforces write-set conflicts at commit time.
    assert!(
        first_result.is_ok() && second_result.is_err(),
        "Expected first commit to succeed and second to fail with transaction_conflict"
    );
}
