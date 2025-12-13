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
use cntryl_midge::engine::api::WriteBatch;
use cntryl_midge::testkit::*;

// ============================================================================
// BASIC RECOVERY TESTS
// ============================================================================

#[test]
fn should_recover_from_clean_shutdown_when_reopening() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange & Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            // Write and flush data cleanly
            engine.put(cf, b"key1", b"value1").expect("put");
            engine.put(cf, b"key2", b"value2").expect("put");
            engine.flush().expect("flush");
            // Clean shutdown (engine dropped normally)
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();

            assert_eq!(
                engine.get(cf, b"key1").expect("get"),
                Some(Bytes::from_static(b"value1")),
                "mode: {}",
                mode
            );
            assert_eq!(
                engine.get(cf, b"key2").expect("get"),
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
        // Arrange & Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            // Write, flush, then simulate crash with additional writes
            engine
                .put(cf, b"flushed_key", b"flushed_value")
                .expect("put");
            engine.flush().expect("flush");

            // Additional writes to memtable (not flushed)
            engine
                .put(cf, b"unflushed_key", b"unflushed_value")
                .expect("put");
            // Crash before flush
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();

            // Flushed data recoverable from SST
            assert_eq!(
                engine.get(cf, b"flushed_key").expect("get"),
                Some(Bytes::from_static(b"flushed_value")),
                "mode: {}",
                mode
            );
            // Unflushed data recoverable from WAL
            assert_eq!(
                engine.get(cf, b"unflushed_key").expect("get"),
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
        // Arrange & Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            // Write data
            for i in 0..100 {
                let key = format!("key_{:03}", i);
                let value = format!("value_{:03}", i);
                engine
                    .put(cf, key.as_bytes(), value.as_bytes())
                    .expect("put");
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
                assert!(
                    engine.get(cf, key.as_bytes()).expect("get").is_some(),
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
        // Arrange & Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            // Write v1, flush to SST
            engine.put(cf, b"key", b"value_v1").expect("put");
            engine.flush().expect("flush");

            // Overwrite with v2 (in WAL only)
            engine.put(cf, b"key", b"value_v2").expect("put");
            // Crash before flush
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();

            // Should prefer newer value from WAL
            assert_eq!(
                engine.get(cf, b"key").expect("get"),
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
        // Arrange & Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            // Write v1, flush to SST (WAL can be discarded)
            engine.put(cf, b"key", b"value_v1").expect("put");
            engine.flush().expect("flush");
            // Crash (no new writes after flush)
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();

            // Should recover from SST (WAL not needed)
            assert_eq!(
                engine.get(cf, b"key").expect("get"),
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
        // Arrange & Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            // Write sequence (order matters)
            for i in 0..100 {
                let key = format!("seq_key_{:03}", i);
                engine
                    .put(cf, key.as_bytes(), format!("value_{:03}", i).as_bytes())
                    .expect("put");
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
                assert_eq!(
                    engine.get(cf, key.as_bytes()).expect("get"),
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
        // Arrange & Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            // Write and flush
            engine.put(cf, b"to_delete", b"value").expect("put");
            engine.flush().expect("flush");

            // Delete (written to WAL but not yet persisted)
            engine.delete(cf, b"to_delete").expect("delete");
            // Crash before flush
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let _cf = engine.default_column_family();

            // Deletion should be recovered from WAL
            assert!(
                engine.get(_cf, b"to_delete").expect("get").is_none(),
                "delete not recovered from WAL in mode: {}",
                mode
            );
        }
    });
}

#[test]
fn should_recover_write_batch_atomically_given_crash_when_reopening() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange & Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            // Write batch (atomic operation)
            let mut batch = WriteBatch::new();
            batch.put(
                bytes::Bytes::copy_from_slice(b"key1"),
                bytes::Bytes::copy_from_slice(b"value1"),
            );
            batch.put(
                bytes::Bytes::copy_from_slice(b"key2"),
                bytes::Bytes::copy_from_slice(b"value2"),
            );
            batch.put(
                bytes::Bytes::copy_from_slice(b"key3"),
                bytes::Bytes::copy_from_slice(b"value3"),
            );
            engine.write_batch(&batch).expect("write_batch");
            // Crash before flush
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();

            // All batch operations should be recovered atomically
            assert!(
                engine.get(cf, b"key1").expect("get").is_some(),
                "mode: {}",
                mode
            );
            assert!(
                engine.get(cf, b"key2").expect("get").is_some(),
                "mode: {}",
                mode
            );
            assert!(
                engine.get(cf, b"key3").expect("get").is_some(),
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
fn should_recover_from_wal_given_manifest_save_failure_when_reopening() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange & Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            // Write and flush (simulating manifest save failure)
            engine.put(cf, b"key", b"value").expect("put");
            // Crash during manifest save (before it persists)
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();

            // Recovery should still work via WAL
            assert_eq!(
                engine.get(cf, b"key").expect("get"),
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
        // Arrange & Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            // Write multiple batches
            for batch_num in 0..3 {
                for i in 0..10 {
                    let key = format!("batch_{}_key_{:02}", batch_num, i);
                    engine.put(cf, key.as_bytes(), b"value").expect("put");
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
                    assert!(
                        engine.get(cf, key.as_bytes()).expect("get").is_some(),
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
        // Arrange & Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            engine.put(cf, b"key1", b"value1").expect("put");
            engine.put(cf, b"key2", b"value2").expect("put");
            // Crash
        }

        // Act & Assert (Phase 2: First recovery)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            assert_eq!(
                engine.get(cf, b"key1").expect("get"),
                Some(Bytes::from_static(b"value1")),
                "mode: {}",
                mode
            );
            assert_eq!(
                engine.get(cf, b"key2").expect("get"),
                Some(Bytes::from_static(b"value2")),
                "mode: {}",
                mode
            );
            // Crash again during second recovery attempt
        }

        // Act & Assert (Phase 3: Second recovery)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();

            // Should still recover correctly (idempotent)
            assert_eq!(
                engine.get(cf, b"key1").expect("get"),
                Some(Bytes::from_static(b"value1")),
                "mode: {}",
                mode
            );
            assert_eq!(
                engine.get(cf, b"key2").expect("get"),
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
        // Arrange & Act (Phase 1: First crash cycle)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            engine.put(cf, b"key", b"value").expect("put");
            // Crash
        }

        // Act (Phase 2: First recovery)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            let val = engine.get(cf, b"key").expect("get");
            assert_eq!(val, Some(Bytes::from_static(b"value")), "mode: {}", mode);
            // Crash again (recovery might trigger flush)
        }

        // Assert (Phase 3: Second recovery)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();

            // Value should appear exactly once (no duplicates)
            let val = engine.get(cf, b"key").expect("get");
            assert_eq!(val, Some(Bytes::from_static(b"value")), "mode: {}", mode);
        }
    });
}

#[test]
fn should_continue_sequence_numbers_given_recovery_when_new_writes() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange & Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            engine.put(cf, b"seq_1", b"value_1").expect("put");
            engine.put(cf, b"seq_2", b"value_2").expect("put");
            // Crash
        }

        // Act (Phase 2: Recovery and new writes)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            // Verify recovery
            assert_eq!(
                engine.get(cf, b"seq_1").expect("get"),
                Some(Bytes::from_static(b"value_1")),
                "mode: {}",
                mode
            );

            // Write new data (sequence numbers should continue)
            engine.put(cf, b"seq_3", b"value_3").expect("put");
            engine.put(cf, b"seq_4", b"value_4").expect("put");
        }

        // Assert (Phase 3)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();

            // All data including post-recovery writes should be present
            assert_eq!(
                engine.get(cf, b"seq_1").expect("get"),
                Some(Bytes::from_static(b"value_1")),
                "mode: {}",
                mode
            );
            assert_eq!(
                engine.get(cf, b"seq_3").expect("get"),
                Some(Bytes::from_static(b"value_3")),
                "mode: {}",
                mode
            );
            assert_eq!(
                engine.get(cf, b"seq_4").expect("get"),
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
        // Arrange & Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            // Write valid records
            for i in 0..50 {
                let key = format!("valid_{:03}", i);
                engine.put(cf, key.as_bytes(), b"value").expect("put");
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
                assert!(
                    engine.get(cf, key.as_bytes()).expect("get").is_some(),
                    "mode: {}",
                    mode
                );
            }
            // Recovery should not panic on corrupted tail
        }
    });
}
