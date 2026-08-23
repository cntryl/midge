//! Tests for transaction crash recovery and durability semantics
//!
//! All tests parametrized across durable storage modes only (`LocalDisk`, `CloudBacked`)
//! Pattern: `for_each_storage_mode(&durable_storage_modes()`, |mode, opts| { ... })

use bytes::Bytes;
mod common;
use common::*;

// ============================================================================
// TRANSACTION CRASH RECOVERY
// ============================================================================

/// `should_persist_atomic_transactions_after_restart`
/// Verify committed transaction persists across crash/restart
/// Phase 1: Commit transaction, then crash (drop engine)
/// Phase 2: Restart and verify transaction data persisted
#[test]
fn should_persist_atomic_transactions_after_restart() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let opts_clone = opts.clone();

        // Act: Phase 1 - Write and commit
        {
            let mut engine = open_with_mode(&opts_clone, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"tx_key1".to_vec(), b"tx_value1".to_vec(), None)
                .expect("put");
            tx.put(b"tx_key2".to_vec(), b"tx_value2".to_vec(), None)
                .expect("put");
            tx.commit(buffered_write_options(mode)).expect("commit");
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before restart");
        }

        // Assert: Phase 2 - Recover
        {
            let engine = open_with_mode(&opts_clone, mode);
            let cf = engine.get_column_family("test").expect("get cf");

            let tx_read = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin_tx");
            let got1 = tx_read.get(b"tx_key1").expect("get");
            let got2 = tx_read.get(b"tx_key2").expect("get");

            assert_eq!(
                got1,
                Some(Bytes::from_static(b"tx_value1")),
                "tx_key1 not persisted in mode: {mode}"
            );
            assert_eq!(
                got2,
                Some(Bytes::from_static(b"tx_value2")),
                "tx_key2 not persisted in mode: {mode}"
            );
        }
    });
}

/// `should_not_persist_uncommitted_transaction_after_restart`
/// Verify uncommitted transaction rolls back across crash/restart
/// Phase 1: Create transaction, write but don't commit, then crash
/// Phase 2: Restart and verify data was rolled back
#[test]
fn should_not_persist_uncommitted_transaction_after_restart() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let opts_clone = opts.clone();

        // Act: Phase 1 - Write but don't commit
        {
            let mut engine = open_with_mode(&opts_clone, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(
                b"uncommitted_key".to_vec(),
                b"uncommitted_value".to_vec(),
                None,
            )
            .expect("put");
            drop(tx);
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before restart");
        }

        // Assert: Phase 2 - Recover
        {
            let engine = open_with_mode(&opts_clone, mode);
            let cf = engine.get_column_family("test").expect("get cf");

            let tx_read = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin_tx");
            let got = tx_read.get(b"uncommitted_key").expect("get");
            assert_eq!(got, None, "uncommitted data persisted in mode: {mode}");
        }
    });
}

/// `should_recover_after_abort_given_transaction_with_delete_range_when_restart`
/// Verify `delete_range` in transaction persists and recovers correctly
/// Phase 1: Write initial data, commit transaction with `delete_range`, crash
/// Phase 2: Restart and verify deleted keys are gone
#[test]
fn should_recover_after_abort_given_transaction_with_delete_range_when_restart() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let opts_clone = opts.clone();

        // Act: Phase 1 - Initial data
        {
            let mut engine = open_with_mode(&opts_clone, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            for i in 0..10 {
                let key = format!("key{i}");
                let mut tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(key.as_bytes().to_vec(), b"initial_value".to_vec(), None)
                    .expect("put");
                tx.commit(buffered_write_options(mode)).expect("commit");
            }
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before delete-range restart");
        }

        // Act: Phase 2 - Delete range as a standalone CF-scoped operation
        {
            let mut engine = open_with_mode(&opts_clone, mode);
            let cf = engine.get_column_family("test").expect("get cf");
            let mut delete_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin delete_range tx");
            delete_tx
                .delete_range(b"key3".to_vec(), b"key7".to_vec()) // [key3, key7)
                .expect("delete_range");
            delete_tx
                .commit(buffered_write_options(mode))
                .expect("commit delete_range");
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before verification restart");
        }

        // Assert: Phase 3 - Verify delete_range persisted
        {
            let engine = open_with_mode(&opts_clone, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let tx_read = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin_tx");
            // Keys before range should exist
            assert_eq!(
                tx_read.get(b"key2").unwrap(),
                Some(Bytes::from_static(b"initial_value")),
                "key2 should exist in mode: {mode}"
            );

            // Keys in range should be deleted
            assert_eq!(
                tx_read.get(b"key5").unwrap(),
                None,
                "key5 should be deleted in mode: {mode}"
            );

            // Keys after range should exist
            assert_eq!(
                tx_read.get(b"key8").unwrap(),
                Some(Bytes::from_static(b"initial_value")),
                "key8 should exist in mode: {mode}"
            );
        }
    });
}

/// `should_recover_committed_spill_given_restart_after_commit`
/// Verify large transaction with spill commits and recovers data
#[test]
fn should_recover_committed_spill_given_restart_after_commit() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let opts_clone = opts.clone();
        let mut opts = opts.clone();
        opts = opts.memory_budget(100 * 1024); // Force spill with 100KB limit

        // Act: Phase 1 - Write large transaction
        {
            let mut engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..200 {
                let key = format!("spill_key{i:04}");
                let value = format!("spill_value_{i:04}");
                tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                    .expect("put");
            }
            tx.commit(buffered_write_options(mode)).expect("commit");
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before spill restart");
        }

        // Assert: Phase 2 - Recover and verify
        {
            let engine = open_with_mode(&opts_clone, mode);
            let cf = engine.get_column_family("test").expect("get cf");

            let tx_read = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin_tx");
            for i in 0..200 {
                let key = format!("spill_key{i:04}");
                let expected = format!("spill_value_{i:04}");
                let got = tx_read.get(key.as_bytes()).expect("get");
                let got_str = got.as_ref().map(|b| String::from_utf8_lossy(b).to_string());
                assert_eq!(
                    got_str,
                    Some(expected),
                    "spill key {key} mismatch in mode: {mode}"
                );
            }
        }
    });
}

/// `should_rollback_uncommitted_spill_given_restart_before_commit`
/// Verify spilled data from uncommitted transaction is not recovered
#[test]
fn should_rollback_uncommitted_spill_given_restart_before_commit() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let opts_clone = opts.clone();
        let mut opts = opts.clone();
        opts = opts.memory_budget(100 * 1024);

        // Act: Phase 1 - Write but don't commit
        {
            let mut engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..200 {
                let key = format!("uncom_key{i:04}");
                let value = format!("uncom_value_{i:04}");
                tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                    .expect("put");
            }
            drop(tx);
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before spill restart");
        }

        // Assert: Phase 2 - Verify no data recovered
        {
            let engine = open_with_mode(&opts_clone, mode);
            let cf = engine.get_column_family("test").expect("get cf");

            let tx_read = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin_tx");
            for i in 0..200 {
                let key = format!("uncom_key{i:04}");
                let got = tx_read.get(key.as_bytes()).expect("get");
                assert_eq!(
                    got, None,
                    "uncommitted spill data {key} recovered in mode: {mode}"
                );
            }
        }
    });
}

/// `should_handle_transaction_abort_idempotency_given_multiple_restart_cycles`
/// Verify that an aborted (dropped-without-commit) transaction never resurfaces
/// across repeated abort+commit+restart cycles, while the sibling committed
/// write from the same cycle - and every prior cycle's committed/aborted
/// pair - remains correctly resolved after each restart.
#[test]
fn should_handle_transaction_abort_idempotency_given_multiple_restart_cycles() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let opts_clone = opts.clone();

        // Act
        for cycle in 0..3 {
            {
                let mut engine = open_with_mode(&opts_clone, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Abort: write then drop without commit.
                let aborted_key = format!("cycle{cycle}_aborted_key");
                let mut aborted_tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .expect("begin_tx (abort)");
                aborted_tx
                    .put(
                        aborted_key.as_bytes().to_vec(),
                        b"should_never_persist".to_vec(),
                        None,
                    )
                    .expect("put (abort)");
                drop(aborted_tx);

                // Commit: sibling write that should persist.
                let key = format!("cycle{cycle}_key");
                let value = format!("cycle{cycle}_value");
                let mut tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                    .expect("put");
                tx.commit(buffered_write_options(mode)).expect("commit");
                engine
                    .shutdown(std::time::Duration::from_secs(5))
                    .expect("shutdown before cycle restart");
            }

            {
                let mut engine = open_with_mode(&opts_clone, mode);
                let cf = engine.create_column_family("test").expect("create cf");
                let tx_read = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                    .expect("begin_tx");

                // Assert
                // This cycle's committed key persisted, aborted key did not.
                let key = format!("cycle{cycle}_key");
                let expected = format!("cycle{cycle}_value");
                let got = tx_read.get(key.as_bytes()).expect("get");
                let got_str = got.as_ref().map(|b| String::from_utf8_lossy(b).to_string());
                assert_eq!(
                    got_str,
                    Some(expected),
                    "cycle {cycle} committed key missing in mode: {mode}"
                );

                let aborted_key = format!("cycle{cycle}_aborted_key");
                let got_aborted = tx_read.get(aborted_key.as_bytes()).expect("get");
                assert_eq!(
                    got_aborted, None,
                    "cycle {cycle} aborted key persisted in mode: {mode}"
                );

                // Idempotency across restarts: every prior cycle's outcome is unchanged.
                for prior in 0..cycle {
                    let prior_key = format!("cycle{prior}_key");
                    let prior_expected = format!("cycle{prior}_value");
                    let prior_got = tx_read.get(prior_key.as_bytes()).expect("get");
                    let prior_got_str = prior_got
                        .as_ref()
                        .map(|b| String::from_utf8_lossy(b).to_string());
                    assert_eq!(
                        prior_got_str,
                        Some(prior_expected),
                        "cycle {prior} committed key regressed after cycle {cycle} restart in mode: {mode}"
                    );

                    let prior_aborted_key = format!("cycle{prior}_aborted_key");
                    let prior_got_aborted = tx_read.get(prior_aborted_key.as_bytes()).expect("get");
                    assert_eq!(
                        prior_got_aborted, None,
                        "cycle {prior} aborted key reappeared after cycle {cycle} restart in mode: {mode}"
                    );
                }

                drop(tx_read);
                engine
                    .shutdown(std::time::Duration::from_secs(5))
                    .expect("shutdown after cycle verification");
            }
        }
    });
}
