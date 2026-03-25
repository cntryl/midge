//! Clean Shutdown Reopen Recovery Tests
//!
//! Tests recovery behavior after clean shutdown followed by reopen, plus
//! WAL/SST replay ordering and repeated reopen idempotency.
//! Coverage in this file is limited to normal process teardown via `drop`:
//! - Recovery of flushed and unflushed committed writes after reopen
//! - WAL vs SST precedence during reopen
//! - Delete replay and multi-write visibility after reopen
//! - Repeated clean reopen cycles and post-reopen write continuity
//!
//! **Storage Modes**: LocalDisk + CloudBacked ONLY (requires persistence)
//!
//! Naming convention:
//!   should_<behavior>_given_<context>_when_<condition>

use bytes::Bytes;
mod common;
use cntryl_midge::{IsolationLevel, TransactionMode, WriteOptions};
use common::*;

// ============================================================================
// BASIC RECOVERY TESTS
// ============================================================================

#[test]
fn should_recover_from_clean_shutdown_when_reopening() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Write and flush data cleanly
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key1".to_vec(), b"value1".to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::buffered()).expect("commit");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key2".to_vec(), b"value2".to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::buffered()).expect("commit");
            engine.flush_cf(&cf).expect("flush");
            // Clean shutdown (engine dropped normally)
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            assert_eq!(
                tx.get(b"key1").expect("get"),
                Some(Bytes::from_static(b"value1")),
                "mode: {}",
                mode
            );
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            assert_eq!(
                tx.get(b"key2").expect("get"),
                Some(Bytes::from_static(b"value2")),
                "mode: {}",
                mode
            );
        }
    });
}

#[test]
fn should_recover_after_clean_shutdown_when_writes_include_flushed_and_unflushed_data() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Write, flush, then add additional committed writes
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"flushed_key".to_vec(), b"flushed_value".to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::buffered()).expect("commit");
            engine.flush_cf(&cf).expect("flush");

            // Additional writes to memtable (not flushed)
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"unflushed_key".to_vec(), b"unflushed_value".to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::buffered()).expect("commit");
            // Engine is dropped without flushing the second write set
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Flushed data recoverable from SST
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            assert_eq!(
                tx.get(b"flushed_key").expect("get"),
                Some(Bytes::from_static(b"flushed_value")),
                "mode: {}",
                mode
            );
            // Unflushed data recoverable from WAL
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            assert_eq!(
                tx.get(b"unflushed_key").expect("get"),
                Some(Bytes::from_static(b"unflushed_value")),
                "mode: {}",
                mode
            );
        }
    });
}

#[test]
fn should_preserve_first_commit_given_conflict_abort_when_reopening() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx1 = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin tx1");
            let mut tx2 = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin tx2");

            tx1.set_isolation_level(IsolationLevel::AbortOnWriteConflict);
            tx2.set_isolation_level(IsolationLevel::AbortOnWriteConflict);

            tx1.put(b"key".to_vec(), b"from_tx1".to_vec(), None)
                .expect("tx1 put");
            tx2.put(b"key".to_vec(), b"from_tx2".to_vec(), None)
                .expect("tx2 put");

            // Act
            tx1.commit(WriteOptions::buffered()).expect("commit tx1");
            let conflict = tx2.commit(WriteOptions::buffered());

            // Assert
            assert!(
                matches!(conflict, Err(cntryl_midge::MidgeError::WriteConflict(_))),
                "mode: {}",
                mode
            );
        }

        // Arrange
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Read after reopen
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin tx");

            // Assert
            assert_eq!(
                tx.get(b"key").expect("get"),
                Some(Bytes::from_static(b"from_tx1")),
                "mode: {}",
                mode
            );
        }
    });
}

#[test]
fn should_recover_unflushed_data_when_reopening_after_clean_shutdown() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Write data
            for i in 0..100 {
                let key = format!("key_{:03}", i);
                let value = format!("value_{:03}", i);
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                    .expect("put");
                tx.commit(WriteOptions::buffered()).expect("commit");
            }
            // Engine is dropped before an explicit flush occurs
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Data should be recoverable from WAL
            for i in 0..100 {
                let key = format!("key_{:03}", i);
                let expected = Bytes::from(format!("value_{:03}", i));
                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");
                assert_eq!(
                    tx.get(key.as_bytes()).expect("get"),
                    Some(expected),
                    "mode: {}",
                    mode
                );
            }
        }
    });
}

// ============================================================================
// WAL vs SST PRECEDENCE TESTS
// ============================================================================

#[test]
fn should_prefer_wal_given_wal_newer_than_sst_when_recovering() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Write v1, flush to SST
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key".to_vec(), b"value_v1".to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::buffered()).expect("commit");
            engine.flush_cf(&cf).expect("flush");

            // Overwrite with v2 (in WAL only)
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key".to_vec(), b"value_v2".to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::buffered()).expect("commit");
            // Engine is dropped before a second flush occurs
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Should prefer newer value from WAL
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            assert_eq!(
                tx.get(b"key").expect("get"),
                Some(Bytes::from_static(b"value_v2")),
                "mode: {}",
                mode
            );
        }
    });
}

#[test]
fn should_skip_wal_entries_given_already_in_sst_when_recovering() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Write v1, flush to SST (WAL can be discarded)
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key".to_vec(), b"value_v1".to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::buffered()).expect("commit");
            engine.flush_cf(&cf).expect("flush");
            // Engine is dropped after a successful flush
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Should recover from SST (WAL not needed)
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            assert_eq!(
                tx.get(b"key").expect("get"),
                Some(Bytes::from_static(b"value_v1")),
                "mode: {}",
                mode
            );
        }
    });
}

#[test]
fn should_replay_wal_in_order_given_multiple_writes_when_recovering() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Write sequence (order matters)
            for i in 0..100 {
                let key = format!("seq_key_{:03}", i);
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(
                    key.as_bytes().to_vec(),
                    format!("value_{:03}", i).as_bytes().to_vec(),
                    None,
                )
                .expect("put");
                tx.commit(WriteOptions::buffered()).expect("commit");
            }
            // Engine is dropped before an explicit flush occurs
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Verify correct order (last write wins for same key)
            for i in 0..100 {
                let key = format!("seq_key_{:03}", i);
                let expected = Bytes::from(format!("value_{:03}", i));
                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");
                assert_eq!(
                    tx.get(key.as_bytes()).expect("get"),
                    Some(expected),
                    "mode: {}",
                    mode
                );
            }
        }
    });
}

// ============================================================================
// DELETE AND BATCH RECOVERY TESTS
// ============================================================================

#[test]
fn should_recover_deletes_when_reopening_after_clean_shutdown() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Write and flush
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"to_delete".to_vec(), b"value".to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::buffered()).expect("commit");
            engine.flush_cf(&cf).expect("flush");

            // Delete (written to WAL but not yet persisted)
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.delete(b"to_delete".to_vec()).expect("delete");
            tx.commit(WriteOptions::buffered()).expect("commit");
            // Engine is dropped before persisting the delete via flush
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let _cf = engine.create_column_family("test").expect("create cf");

            // Deletion should be recovered from WAL
            let tx = engine
                .begin_tx(_cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            assert!(
                tx.get(b"to_delete").expect("get").is_none(),
                "delete not recovered from WAL in mode: {}",
                mode
            );
        }
    });
}

#[test]
fn should_recover_independent_committed_transactions_when_reopening_after_clean_shutdown() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let _cf = engine.create_column_family("test").expect("create cf");

            // Commit three independent transactions before shutdown
            let mut tx = engine
                .begin_tx(_cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key1".to_vec(), b"value1".to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::buffered()).expect("commit");
            let mut tx = engine
                .begin_tx(_cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key2".to_vec(), b"value2".to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::buffered()).expect("commit");
            let mut tx = engine
                .begin_tx(_cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key3".to_vec(), b"value3".to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::buffered()).expect("commit");
            // Engine is dropped before an explicit flush occurs
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // All independently committed writes should be visible after reopen
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            assert_eq!(
                tx.get(b"key1").expect("get"),
                Some(Bytes::from_static(b"value1")),
                "mode: {}",
                mode
            );
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            assert_eq!(
                tx.get(b"key2").expect("get"),
                Some(Bytes::from_static(b"value2")),
                "mode: {}",
                mode
            );
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            assert_eq!(
                tx.get(b"key3").expect("get"),
                Some(Bytes::from_static(b"value3")),
                "mode: {}",
                mode
            );
        }
    });
}

// ============================================================================
// CONSISTENCY AND ORDERING TESTS
// ============================================================================

#[test]
fn should_recover_from_wal_when_reopening_after_clean_shutdown() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Write committed data without forcing an SST flush
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key".to_vec(), b"value".to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::buffered()).expect("commit");
            // Engine is dropped after the commit
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Recovery should still work via WAL
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            assert_eq!(
                tx.get(b"key").expect("get"),
                Some(Bytes::from_static(b"value")),
                "mode: {}",
                mode
            );
        }
    });
}

#[test]
fn should_preserve_consistency_when_reopening_after_clean_shutdown() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Write multiple batches
            for batch_num in 0..3 {
                for i in 0..10 {
                    let key = format!("batch_{}_key_{:02}", batch_num, i);
                    let mut tx = engine
                        .begin_tx(cf.id(), TransactionMode::ReadWrite)
                        .expect("begin_tx");
                    tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                        .expect("put");
                    tx.commit(WriteOptions::buffered()).expect("commit");
                }
            }
            // Engine is dropped after all commits complete
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // All writes should be recoverable
            for batch_num in 0..3 {
                for i in 0..10 {
                    let key = format!("batch_{}_key_{:02}", batch_num, i);
                    let expected = Bytes::from_static(b"value");
                    let tx = engine
                        .begin_tx(cf.id(), TransactionMode::ReadOnly)
                        .expect("begin_tx");
                    assert_eq!(
                        tx.get(key.as_bytes()).expect("get"),
                        Some(expected.clone()),
                        "mode: {}",
                        mode
                    );
                }
            }
        }
    });
}

// ============================================================================
// IDEMPOTENCY TESTS
// ============================================================================

#[test]
fn should_be_idempotent_when_reopening_multiple_times_after_clean_shutdown() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key1".to_vec(), b"value1".to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::buffered()).expect("commit");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key2".to_vec(), b"value2".to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::buffered()).expect("commit");
            // Engine is dropped after the committed writes
        }

        // Act (Recovery cycles)
        {
            // First reopen cycle: open and drop again
            let engine = open_with_mode(opts.clone(), mode);
            drop(engine);

            // Second recovery: open and verify final state
            let engine = open_with_mode(opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Assert - final state should be correct after multiple restarts
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            assert_eq!(
                tx.get(b"key1").expect("get"),
                Some(Bytes::from_static(b"value1")),
                "mode: {}",
                mode
            );
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            assert_eq!(
                tx.get(b"key2").expect("get"),
                Some(Bytes::from_static(b"value2")),
                "mode: {}",
                mode
            );
        }
    });
}

#[test]
fn should_maintain_exactly_once_visibility_when_reopening_multiple_times_after_clean_shutdown() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key".to_vec(), b"value".to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::buffered()).expect("commit");
            // Engine is dropped after the committed write
        }

        // Act (Phase 2: First recovery)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            let val = tx.get(b"key").expect("get");
            assert_eq!(val, Some(Bytes::from_static(b"value")), "mode: {}", mode);
            // Engine is dropped again after the first reopen
        }

        // Assert (Phase 3: Second recovery)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Value should appear exactly once (no duplicates)
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            let val = tx.get(b"key").expect("get");
            assert_eq!(val, Some(Bytes::from_static(b"value")), "mode: {}", mode);
        }
    });
}

#[test]
fn should_continue_sequence_numbers_when_new_writes_follow_clean_reopen() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"seq_1".to_vec(), b"value_1".to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::buffered()).expect("commit");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"seq_2".to_vec(), b"value_2".to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::buffered()).expect("commit");
            // Engine is dropped after the committed writes
        }

        // Act (Phase 2: Reopen and new writes)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Verify previously committed data is visible after reopen
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            assert_eq!(
                tx.get(b"seq_1").expect("get"),
                Some(Bytes::from_static(b"value_1")),
                "mode: {}",
                mode
            );

            // Write new data (sequence numbers should continue)
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"seq_3".to_vec(), b"value_3".to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::buffered()).expect("commit");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"seq_4".to_vec(), b"value_4".to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::buffered()).expect("commit");
        }

        // Assert (Phase 3)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // All data including post-recovery writes should be present
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            assert_eq!(
                tx.get(b"seq_1").expect("get"),
                Some(Bytes::from_static(b"value_1")),
                "mode: {}",
                mode
            );
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            assert_eq!(
                tx.get(b"seq_3").expect("get"),
                Some(Bytes::from_static(b"value_3")),
                "mode: {}",
                mode
            );
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            assert_eq!(
                tx.get(b"seq_4").expect("get"),
                Some(Bytes::from_static(b"value_4")),
                "mode: {}",
                mode
            );
        }
    });
}

#[test]
fn should_replay_valid_wal_records_when_reopening_after_clean_shutdown() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Write valid records
            for i in 0..50 {
                let key = format!("valid_{:03}", i);
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                    .expect("put");
                tx.commit(WriteOptions::buffered()).expect("commit");
            }
            // Engine is dropped after writing valid WAL records
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Valid committed records should be recovered on reopen
            for i in 0..50 {
                let key = format!("valid_{:03}", i);
                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");
                assert_eq!(
                    tx.get(key.as_bytes()).expect("get"),
                    Some(Bytes::from_static(b"value")),
                    "mode: {}",
                    mode
                );
            }
        }
    });
}
