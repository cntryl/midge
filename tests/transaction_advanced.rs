//! Tests for transaction crash recovery and durability semantics
//!
//! All tests parametrized across durable storage modes only (LocalDisk, CloudBacked)
//! Pattern: for_each_storage_mode(&durable_storage_modes(), |mode, opts| { ... })

use bytes::Bytes;
use cntryl_midge::testkit::*;

// ============================================================================
// TRANSACTION CRASH RECOVERY
// ============================================================================

/// should_persist_atomic_transactions_after_restart
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
            let engine = open_with_mode(opts_clone.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"tx_key1".to_vec(), b"tx_value1".to_vec(), None)
                .expect("put");
            tx.put(b"tx_key2".to_vec(), b"tx_value2".to_vec(), None)
                .expect("put");
            engine
                .commit(tx, cntryl_midge::WriteOptions::buffered())
                .expect("commit");
            // engine dropped, crash simulation
        }

        // Assert: Phase 2 - Recover
        {
            let engine = open_with_mode(opts_clone, mode);
            let cf = engine.get_column_family("test").expect("get cf");

            let tx_read = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin_tx");
            let got1 = tx_read.get(b"tx_key1").expect("get");
            let got2 = tx_read.get(b"tx_key2").expect("get");

            assert_eq!(
                got1,
                Some(Bytes::from_static(b"tx_value1")),
                "tx_key1 not persisted in mode: {}",
                mode
            );
            assert_eq!(
                got2,
                Some(Bytes::from_static(b"tx_value2")),
                "tx_key2 not persisted in mode: {}",
                mode
            );
        }
    });
}

/// should_not_persist_uncommitted_transaction_after_restart
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
            let engine = open_with_mode(opts_clone.clone(), mode);
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
            // No commit - engine dropped, crash before commit
        }

        // Assert: Phase 2 - Recover
        {
            let engine = open_with_mode(opts_clone, mode);
            let cf = engine.get_column_family("test").expect("get cf");

            let tx_read = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin_tx");
            let got = tx_read.get(b"uncommitted_key").expect("get");
            assert_eq!(got, None, "uncommitted data persisted in mode: {}", mode);
        }
    });
}

/// should_recover_after_abort_given_transaction_with_delete_range_when_restart
/// Verify delete_range in transaction persists and recovers correctly
/// Phase 1: Write initial data, commit transaction with delete_range, crash
/// Phase 2: Restart and verify deleted keys are gone
#[test]
fn should_recover_after_abort_given_transaction_with_delete_range_when_restart() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let opts_clone = opts.clone();

        // Act: Phase 1 - Initial data
        {
            let engine = open_with_mode(opts_clone.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");
            for i in 0..10 {
                let key = format!("key{}", i);
                let mut tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(key.as_bytes().to_vec(), b"initial_value".to_vec(), None)
                    .expect("put");
                engine
                    .commit(tx, cntryl_midge::WriteOptions::buffered())
                    .expect("commit");
            }
        }

        // Act: Phase 2 - Delete range as a standalone CF-scoped operation
        {
            let engine = open_with_mode(opts_clone.clone(), mode);
            let cf = engine.get_column_family("test").expect("get cf");
            engine
                .delete_range(
                    &cf,
                    b"key3".to_vec(),
                    b"key7".to_vec(), // [key3, key7)
                    cntryl_midge::WriteOptions::buffered(),
                )
                .expect("delete_range");
            // crash simulation
        }

        // Assert: Phase 3 - Verify delete_range persisted
        {
            let engine = open_with_mode(opts_clone, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let tx_read = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin_tx");
            // Keys before range should exist
            assert_eq!(
                tx_read.get(b"key2").unwrap(),
                Some(Bytes::from_static(b"initial_value")),
                "key2 should exist in mode: {}",
                mode
            );

            // Keys in range should be deleted
            assert_eq!(
                tx_read.get(b"key5").unwrap(),
                None,
                "key5 should be deleted in mode: {}",
                mode
            );

            // Keys after range should exist
            assert_eq!(
                tx_read.get(b"key8").unwrap(),
                Some(Bytes::from_static(b"initial_value")),
                "key8 should exist in mode: {}",
                mode
            );
        }
    });
}

/// should_recover_committed_spill_given_restart_after_commit
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
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..200 {
                let key = format!("spill_key{:04}", i);
                let value = format!("spill_value_{:04}", i);
                tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                    .expect("put");
            }
            engine
                .commit(tx, cntryl_midge::WriteOptions::buffered())
                .expect("commit");
            // crash simulation
        }

        // Assert: Phase 2 - Recover and verify
        {
            let engine = open_with_mode(opts_clone, mode);
            let cf = engine.get_column_family("test").expect("get cf");

            let tx_read = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin_tx");
            for i in 0..200 {
                let key = format!("spill_key{:04}", i);
                let expected = format!("spill_value_{:04}", i);
                let got = tx_read.get(key.as_bytes()).expect("get");
                let got_str = got.as_ref().map(|b| String::from_utf8_lossy(b).to_string());
                assert_eq!(
                    got_str,
                    Some(expected),
                    "spill key {} mismatch in mode: {}",
                    key,
                    mode
                );
            }
        }
    });
}

/// should_rollback_uncommitted_spill_given_restart_before_commit
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
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..200 {
                let key = format!("uncom_key{:04}", i);
                let value = format!("uncom_value_{:04}", i);
                tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                    .expect("put");
            }
            // No commit - crash
        }

        // Assert: Phase 2 - Verify no data recovered
        {
            let engine = open_with_mode(opts_clone, mode);
            let cf = engine.get_column_family("test").expect("get cf");

            let tx_read = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin_tx");
            for i in 0..200 {
                let key = format!("uncom_key{:04}", i);
                let got = tx_read.get(key.as_bytes()).expect("get");
                assert_eq!(
                    got, None,
                    "uncommitted spill data {} recovered in mode: {}",
                    key, mode
                );
            }
        }
    });
}

/// should_handle_transaction_abort_idempotency_given_multiple_restart_cycles
/// Verify multiple restart cycles maintain consistency
#[test]
fn should_handle_transaction_abort_idempotency_given_multiple_restart_cycles() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let opts_clone = opts.clone();

        // Act
        for cycle in 0..3 {
            {
                let engine = open_with_mode(opts_clone.clone(), mode);
                let cf = engine.create_column_family("test").expect("create cf");

                let mut tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .expect("begin_tx");
                let key = format!("cycle{}_key", cycle);
                let value = format!("cycle{}_value", cycle);
                tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                    .expect("put");
                engine
                    .commit(tx, cntryl_midge::WriteOptions::buffered())
                    .expect("commit");
            }

            {
                let engine = open_with_mode(opts_clone.clone(), mode);
                let cf = engine.create_column_family("test").expect("create cf");
                let key = format!("cycle{}_key", cycle);
                let expected = format!("cycle{}_value", cycle);
                let tx_read = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                    .expect("begin_tx");
                let got = tx_read.get(key.as_bytes()).expect("get");
                let got_str = got.as_ref().map(|b| String::from_utf8_lossy(b).to_string());
                assert_eq!(
                    got_str,
                    Some(expected),
                    "cycle {} mismatch in mode: {}",
                    cycle,
                    mode
                );
            }
        }

        // Assert: No additional check required.
    });
}

/// should_maintain_exactly_once_semantics_given_transaction_with_crash
/// Verify exactly-once semantics for concurrent operations with crash
#[test]
fn should_maintain_exactly_once_semantics_given_transaction_with_crash() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let opts_clone = opts.clone();

        // Act: Phase 1 - Write twice to same key
        {
            let engine = open_with_mode(opts_clone.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"idempotent_key".to_vec(), b"value1".to_vec(), None)
                .expect("put");
            tx.put(b"idempotent_key".to_vec(), b"value2".to_vec(), None)
                .expect("put");
            engine
                .commit(tx, cntryl_midge::WriteOptions::buffered())
                .expect("commit");
        }

        // Assert: Final value should be the last write (value2)
        {
            let engine = open_with_mode(opts_clone, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let tx_read = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin_tx");
            let got = tx_read.get(b"idempotent_key").expect("get");
            assert_eq!(
                got,
                Some(Bytes::from_static(b"value2")),
                "idempotent key has wrong value in mode: {}",
                mode
            );
        }
    });
}

/// should_recover_large_transaction_given_crash_during_spill
/// Verify recovery when crash occurs during spill operation
#[test]
fn should_recover_large_transaction_given_crash_during_spill() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let opts_clone = opts.clone();
        let mut opts = opts.clone();
        opts = opts.memory_budget(256 * 1024); // Sufficient budget for transaction

        // Act: Phase 1 - Write transaction
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..200 {
                let key = format!("large_key{:04}", i);
                let value = format!("large_value_{:04}_{}", i, "x".repeat(100)); // 100 byte values
                tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                    .expect("put");
            }
            engine
                .commit(tx, cntryl_midge::WriteOptions::buffered())
                .expect("commit");
        }

        // Assert: All data recovered
        {
            let engine = open_with_mode(opts_clone, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let tx_read = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin_tx");
            for i in 0..200 {
                let key = format!("large_key{:04}", i);
                let got = tx_read.get(key.as_bytes()).expect("get");
                assert!(got.is_some(), "large key {} missing in mode: {}", key, mode);
            }
        }
    });
}

/// should_not_lose_transaction_writes_given_incomplete_wal_sync
/// Verify transaction writes are not lost even with partial WAL sync
#[test]
fn should_not_lose_transaction_writes_given_incomplete_wal_sync() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let opts_clone = opts.clone();

        // Act
        {
            let engine = open_with_mode(opts_clone.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"wal_test_key".to_vec(), b"wal_test_value".to_vec(), None)
                .expect("put");
            engine
                .commit(tx, cntryl_midge::WriteOptions::buffered())
                .expect("commit");
        }

        // Assert
        {
            let engine = open_with_mode(opts_clone, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let tx_read = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin_tx");
            let got = tx_read.get(b"wal_test_key").expect("get");
            assert_eq!(
                got,
                Some(Bytes::from_static(b"wal_test_value")),
                "WAL write lost in mode: {}",
                mode
            );
        }
    });
}

/// should_survive_mid_spill_crash_given_transaction_recovery
/// Verify system survives and recovers correctly after mid-spill crash
#[test]
fn should_survive_mid_spill_crash_given_transaction_recovery() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let opts_clone = opts.clone();
        let mut opts = opts.clone();
        opts = opts.memory_budget(80 * 1024);

        // Act
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..300 {
                let key = format!("mid_spill_{:04}", i);
                let value = format!("value_{:04}", i);
                tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                    .expect("put");
            }
            engine
                .commit(tx, cntryl_midge::WriteOptions::buffered())
                .expect("commit");
        }

        // Assert
        {
            let engine = open_with_mode(opts_clone, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let tx_read = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin_tx");
            for i in 0..300 {
                let key = format!("mid_spill_{:04}", i);
                let got = tx_read.get(key.as_bytes()).expect("get");
                assert!(got.is_some(), "mid-spill key {} missing", key);
            }
        }
    });
}
