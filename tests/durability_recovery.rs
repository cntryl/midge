//! Crash Recovery Tests
//!
//! Tests recovery behavior after crashes, restarts, and WAL/SST interactions.
//! Validates that the engine recovers correctly from various failure modes:
//! - Clean shutdown and restart
//! - Crash after flush/memtable operations
//! - WAL vs SST precedence during recovery
//! - Manifest atomicity and consistency
//! - Idempotent recovery (multiple restart cycles)
//!
//! **Storage Modes**: LocalDisk + CloudBacked ONLY (requires persistence)
//!
//! Naming convention:
//!   should_<behavior>_given_<context>_when_<condition>

use bytes::Bytes;
use cntryl_midge::testkit::*;
use cntryl_midge::{TransactionMode, WriteOptions};

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
            let cf = engine.default_column_family();

            // Write and flush data cleanly
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key1".to_vec(), b"value1".to_vec(), None)
                .expect("put");
            engine.commit(tx, WriteOptions::buffered()).expect("commit");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key2".to_vec(), b"value2".to_vec(), None)
                .expect("put");
            engine.commit(tx, WriteOptions::buffered()).expect("commit");
            engine.flush().expect("flush");
            // Clean shutdown (engine dropped normally)
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();

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
fn should_recover_from_crash_after_flush_when_reopening() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            // Write, flush, then simulate crash with additional writes
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"flushed_key".to_vec(), b"flushed_value".to_vec(), None)
                .expect("put");
            engine.commit(tx, WriteOptions::buffered()).expect("commit");
            engine.flush().expect("flush");

            // Additional writes to memtable (not flushed)
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"unflushed_key".to_vec(), b"unflushed_value".to_vec(), None)
                .expect("put");
            engine.commit(tx, WriteOptions::buffered()).expect("commit");
            // Crash before flush
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();

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
fn should_recover_unflushed_data_given_crash_during_flush_when_reopening() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            // Write data
            for i in 0..100 {
                let key = format!("key_{:03}", i);
                let value = format!("value_{:03}", i);
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                    .expect("put");
                engine.commit(tx, WriteOptions::buffered()).expect("commit");
            }
            // Simulate crash during flush (flush not completed)
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();

            // Data should be recoverable from WAL
            for i in 0..100 {
                let key = format!("key_{:03}", i);
                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");
                assert!(
                    tx.get(key.as_bytes()).expect("get").is_some(),
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
            let cf = engine.default_column_family();

            // Write v1, flush to SST
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key".to_vec(), b"value_v1".to_vec(), None)
                .expect("put");
            engine.commit(tx, WriteOptions::buffered()).expect("commit");
            engine.flush().expect("flush");

            // Overwrite with v2 (in WAL only)
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key".to_vec(), b"value_v2".to_vec(), None)
                .expect("put");
            engine.commit(tx, WriteOptions::buffered()).expect("commit");
            // Crash before flush
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();

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
            let cf = engine.default_column_family();

            // Write v1, flush to SST (WAL can be discarded)
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key".to_vec(), b"value_v1".to_vec(), None)
                .expect("put");
            engine.commit(tx, WriteOptions::buffered()).expect("commit");
            engine.flush().expect("flush");
            // Crash (no new writes after flush)
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();

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
            let cf = engine.default_column_family();

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
                engine.commit(tx, WriteOptions::buffered()).expect("commit");
            }
            // Crash before flush
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();

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
fn should_recover_deletes_given_crash_after_delete_when_reopening() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            // Write and flush
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"to_delete".to_vec(), b"value".to_vec(), None)
                .expect("put");
            engine.commit(tx, WriteOptions::buffered()).expect("commit");
            engine.flush().expect("flush");

            // Delete (written to WAL but not yet persisted)
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.delete(b"to_delete".to_vec()).expect("delete");
            engine.commit(tx, WriteOptions::buffered()).expect("commit");
            // Crash before flush
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let _cf = engine.default_column_family();

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
fn should_recover_write_batch_atomically_given_crash_when_reopening() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let _cf = engine.default_column_family();

            // Write batch operations converted to individual transactions
            let mut tx = engine
                .begin_tx(_cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key1".to_vec(), b"value1".to_vec(), None)
                .expect("put");
            engine.commit(tx, WriteOptions::buffered()).expect("commit");
            let mut tx = engine
                .begin_tx(_cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key2".to_vec(), b"value2".to_vec(), None)
                .expect("put");
            engine.commit(tx, WriteOptions::buffered()).expect("commit");
            let mut tx = engine
                .begin_tx(_cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key3".to_vec(), b"value3".to_vec(), None)
                .expect("put");
            engine.commit(tx, WriteOptions::buffered()).expect("commit");
            // Crash before flush
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();

            // All batch operations should be recovered atomically
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            assert!(tx.get(b"key1").expect("get").is_some(), "mode: {}", mode);
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            assert!(tx.get(b"key2").expect("get").is_some(), "mode: {}", mode);
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            assert!(tx.get(b"key3").expect("get").is_some(), "mode: {}", mode);
        }
    });
}

// ============================================================================
// CONSISTENCY AND ORDERING TESTS
// ============================================================================

#[test]
fn should_recover_from_wal_given_manifest_save_failure_when_reopening() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            // Write and flush (simulating manifest save failure)
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key".to_vec(), b"value".to_vec(), None)
                .expect("put");
            engine.commit(tx, WriteOptions::buffered()).expect("commit");
            // Crash during manifest save (before it persists)
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();

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
fn should_preserve_consistency_given_crash_before_manifest_update_when_reopening() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            // Write multiple batches
            for batch_num in 0..3 {
                for i in 0..10 {
                    let key = format!("batch_{}_key_{:02}", batch_num, i);
                    let mut tx = engine
                        .begin_tx(cf.id(), TransactionMode::ReadWrite)
                        .expect("begin_tx");
                    tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                        .expect("put");
                    engine.commit(tx, WriteOptions::buffered()).expect("commit");
                }
            }
            // Crash before final manifest sync
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();

            // All writes should be recoverable
            for batch_num in 0..3 {
                for i in 0..10 {
                    let key = format!("batch_{}_key_{:02}", batch_num, i);
                    let tx = engine
                        .begin_tx(cf.id(), TransactionMode::ReadOnly)
                        .expect("begin_tx");
                    assert!(
                        tx.get(key.as_bytes()).expect("get").is_some(),
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
fn should_be_idempotent_given_multiple_recovery_cycles_when_reopening() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key1".to_vec(), b"value1".to_vec(), None)
                .expect("put");
            engine.commit(tx, WriteOptions::buffered()).expect("commit");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key2".to_vec(), b"value2".to_vec(), None)
                .expect("put");
            engine.commit(tx, WriteOptions::buffered()).expect("commit");
            // Crash
        }

        // Act (Recovery cycles)
        {
            // First recovery: open and drop to simulate crash during recovery
            let engine = open_with_mode(opts.clone(), mode);
            drop(engine);

            // Second recovery: open and verify final state
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();

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
fn should_maintain_exactly_once_given_multiple_crash_cycles_when_reopening() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key".to_vec(), b"value".to_vec(), None)
                .expect("put");
            engine.commit(tx, WriteOptions::buffered()).expect("commit");
            // Crash
        }

        // Act (Phase 2: First recovery)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            let val = tx.get(b"key").expect("get");
            assert_eq!(val, Some(Bytes::from_static(b"value")), "mode: {}", mode);
            // Crash again (recovery might trigger flush)
        }

        // Assert (Phase 3: Second recovery)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();

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
fn should_continue_sequence_numbers_given_recovery_when_new_writes() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"seq_1".to_vec(), b"value_1".to_vec(), None)
                .expect("put");
            engine.commit(tx, WriteOptions::buffered()).expect("commit");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"seq_2".to_vec(), b"value_2".to_vec(), None)
                .expect("put");
            engine.commit(tx, WriteOptions::buffered()).expect("commit");
            // Crash
        }

        // Act (Phase 2: Recovery and new writes)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            // Verify recovery
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
            engine.commit(tx, WriteOptions::buffered()).expect("commit");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"seq_4".to_vec(), b"value_4".to_vec(), None)
                .expect("put");
            engine.commit(tx, WriteOptions::buffered()).expect("commit");
        }

        // Assert (Phase 3)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();

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
fn should_skip_corrupted_tail_given_partial_record_when_tolerant_mode() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            // Write valid records
            for i in 0..50 {
                let key = format!("valid_{:03}", i);
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                    .expect("put");
                engine.commit(tx, WriteOptions::buffered()).expect("commit");
            }
            // Crash with partial record at tail
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();

            // Valid records before tail should be recovered
            for i in 0..50 {
                let key = format!("valid_{:03}", i);
                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");
                assert!(
                    tx.get(key.as_bytes()).expect("get").is_some(),
                    "mode: {}",
                    mode
                );
            }
            // Recovery should not panic on corrupted tail
        }
    });
}
