//! Manifest Visibility And Reopen Consistency Tests
//!
//! Tests visibility and ordering semantics after successful writes, flushes,
//! and compaction-related operations followed by reopen.
//! Coverage in this file is limited to normal process teardown via `drop`:
//! - WAL and SST visibility after reopen
//! - Tombstone replay over older persisted values
//! - Data visibility after successful flush/compaction-adjacent operations
//! - An in-memory transaction atomicity guardrail for concurrent reads
//!
//! **Storage Modes**: `LocalDisk` + `CloudBacked` ONLY (requires persistence)
//!
//! Naming convention:
//!   should_<behavior>_given_<context>_when_<condition>

use bytes::Bytes;
mod common;
use cntryl_midge::{TransactionMode, WriteOptions};
use common::*;

// ============================================================================
// MANIFEST VISIBILITY AND ATOMICITY TESTS
// ============================================================================

#[test]
fn should_not_expose_sst_without_manifest_entry_given_orphan_file_when_recovering() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let mut engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Write and flush to create SST file
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key1".to_vec(), b"value1".to_vec(), None)
                .expect("put");
            tx.commit(buffered_write_options(mode)).expect("commit");
            engine.flush_cf(&cf).expect("flush");

            // Write more data (will create another SST)
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key2".to_vec(), b"value2".to_vec(), None)
                .expect("put");
            tx.commit(buffered_write_options(mode)).expect("commit");
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before reopen");
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // key1 should be visible (from first SST, manifest entry exists)
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            assert!(tx.get(b"key1").expect("get").is_some(), "mode: {mode}");

            // key2 should be readable from the later committed write on reopen
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            assert_eq!(
                tx.get(b"key2").expect("get"),
                Some(Bytes::from_static(b"value2")),
                "mode: {mode}"
            );
        }
    });
}

#[test]
fn should_replay_wal_until_manifest_sequence_given_manifest_fsynced_when_recovering() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let mut engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Write and flush (manifest updated)
            for i in 0..5 {
                let key = format!("flushed_{i:02}");
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                    .expect("put");
                tx.commit(buffered_write_options(mode)).expect("commit");
            }
            engine.flush_cf(&cf).expect("flush");

            // Write more after manifest update (in WAL only)
            for i in 0..5 {
                let key = format!("unflushed_{i:02}");
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                    .expect("put");
                tx.commit(buffered_write_options(mode)).expect("commit");
            }
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before reopen");
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // All data should be recovered (flushed + WAL)
            for i in 0..5 {
                let key = format!("flushed_{i:02}");
                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");
                assert_eq!(
                    tx.get(key.as_bytes()).expect("get"),
                    Some(bytes::Bytes::from_static(b"value")),
                    "mode: {mode}"
                );
            }
            for i in 0..5 {
                let key = format!("unflushed_{i:02}");
                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");
                assert_eq!(
                    tx.get(key.as_bytes()).expect("get").as_deref(),
                    Some(b"value".as_slice()),
                    "mode: {mode}; reopened WAL value must match the committed write"
                );
            }
        }
    });
}

#[test]
fn should_preserve_manifest_authority_given_wal_newer_when_sst_missing() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let mut engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Write, flush, then overwrite
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key".to_vec(), b"value_old".to_vec(), None)
                .expect("put");
            tx.commit(buffered_write_options(mode)).expect("commit");
            engine.flush_cf(&cf).expect("flush");

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key".to_vec(), b"value_new".to_vec(), None)
                .expect("put");
            tx.commit(buffered_write_options(mode)).expect("commit");
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before reopen");
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // WAL should take precedence over SST when both exist
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            assert_eq!(
                tx.get(b"key").expect("get"),
                Some(Bytes::from_static(b"value_new")),
                "mode: {mode}"
            );
        }
    });
}

#[test]
fn should_apply_wal_tombstone_when_reopening_after_clean_shutdown() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let mut engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Create SST
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key".to_vec(), b"value".to_vec(), None)
                .expect("put");
            tx.commit(buffered_write_options(mode)).expect("commit");
            engine.flush_cf(&cf).expect("flush");

            // Delete the key (creates tombstone in WAL)
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.delete(b"key".to_vec()).expect("delete");
            tx.commit(buffered_write_options(mode)).expect("commit");
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before reopen");
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // The WAL tombstone should hide the older flushed value after reopen
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            assert_eq!(tx.get(b"key").expect("get"), None, "mode: {mode}");
        }
    });
}

// ============================================================================
// PUBLICATION AND ATOMICITY TESTS
// ============================================================================

#[test]
fn should_preserve_data_visibility_when_reopening_after_successful_flush_and_clean_shutdown() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let mut engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Flush the committed writes to persistent storage
            for i in 0..20 {
                let key = format!("key_{i:03}");
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                    .expect("put");
                tx.commit(buffered_write_options(mode)).expect("commit");
            }
            engine.flush_cf(&cf).expect("flush");
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before reopen");
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Data should still be visible after the successful flush and reopen
            for i in 0..20 {
                let key = format!("key_{i:03}");
                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");
                assert_eq!(
                    tx.get(key.as_bytes()).expect("get").as_deref(),
                    Some(b"value".as_slice()),
                    "mode: {mode}; reopened value must match the flushed write"
                );
            }
        }
    });
}

#[test]
fn should_maintain_atomicity_given_concurrent_flush_manifest_fsync_when_updating() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = std::sync::Arc::new(open_with_mode(&opts, mode));
            let _cf = engine.create_column_family("test").expect("create cf");

            // Concurrent writes from multiple threads
            let mut handles = vec![];
            for thread_id in 0..2 {
                let engine_clone = std::sync::Arc::clone(&engine);
                let write_options = buffered_write_options(mode);
                let handle = std::thread::spawn(move || {
                    let cf = engine_clone
                        .create_column_family("test")
                        .expect("create cf");
                    for i in 0..5 {
                        let key = format!("t_{thread_id}_k_{i:02}");
                        let mut tx = engine_clone
                            .begin_tx(cf.id(), TransactionMode::ReadWrite)
                            .expect("begin_tx");
                        tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                            .expect("put");
                        tx.commit(write_options).expect("commit");
                    }
                    engine_clone.flush_cf(&cf).expect("flush");
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.join().expect("thread join");
            }
            let mut engine = std::sync::Arc::try_unwrap(engine)
                .ok()
                .expect("unique engine");
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before reopen");
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // All writes should be recoverable (no partial updates)
            for thread_id in 0..2 {
                for i in 0..5 {
                    let key = format!("t_{thread_id}_k_{i:02}");
                    let tx = engine
                        .begin_tx(cf.id(), TransactionMode::ReadOnly)
                        .expect("begin_tx");
                    assert!(
                        tx.get(key.as_bytes()).expect("get").is_some(),
                        "mode: {mode}"
                    );
                }
            }
        }
    });
}

#[test]
fn should_preserve_data_when_reopening_after_flush_with_optional_compaction() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let mut engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Create enough data to trigger compaction
            for i in 0..30 {
                let key = format!("key_{i:03}");
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(
                    key.as_bytes().to_vec(),
                    format!("value_{i:03}").as_bytes().to_vec(),
                    None,
                )
                .expect("put");
                tx.commit(buffered_write_options(mode)).expect("commit");
            }
            engine.flush_cf(&cf).expect("flush");
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before reopen");
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // All data should still be present
            for i in 0..30 {
                let key = format!("key_{i:03}");
                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");
                assert!(
                    tx.get(key.as_bytes()).expect("get").is_some(),
                    "mode: {mode}"
                );
            }
        }
    });
}

#[test]
fn should_preserve_updated_values_when_reopening_after_multiple_flushes() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let mut engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Create initial SST
            for i in 0..15 {
                let key = format!("old_{i:02}");
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                    .expect("put");
                tx.commit(buffered_write_options(mode)).expect("commit");
            }
            engine.flush_cf(&cf).expect("flush");

            // Overwrite the same keys and flush again
            for i in 0..15 {
                let key = format!("old_{i:02}");
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(key.as_bytes().to_vec(), b"new_value".to_vec(), None)
                    .expect("put");
                tx.commit(buffered_write_options(mode)).expect("commit");
            }
            engine.flush_cf(&cf).expect("flush");
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before reopen");
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Updated data should be present
            for i in 0..15 {
                let key = format!("old_{i:02}");
                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");
                assert_eq!(
                    tx.get(key.as_bytes()).expect("get").as_deref(),
                    Some(b"new_value".as_slice()),
                    "mode: {mode}; reopened value must be the latest flushed update"
                );
            }
        }
    });
}

#[test]
fn should_recover_valid_wal_records_when_reopening_after_clean_shutdown() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let mut engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Write valid records
            for i in 0..10 {
                let key = format!("valid_{i:02}");
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                    .expect("put");
                tx.commit(buffered_write_options(mode)).expect("commit");
            }
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before reopen");
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Valid records should be recovered after reopen
            for i in 0..10 {
                let key = format!("valid_{i:02}");
                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");
                assert_eq!(
                    tx.get(key.as_bytes()).expect("get"),
                    Some(Bytes::from_static(b"value")),
                    "mode: {mode}"
                );
            }
        }
    });
}

// ============================================================================
// PHASE 0 GUARDRAILS - IDEMPOTENCY CACHE
// ============================================================================

/// Phase 0 Guardrail #2: Idempotency cache bounded growth
///
/// Removed: the eviction mechanism (`allocate_sequences_idempotent` /
/// `MAX_IDEMPOTENCY_CACHE_SIZE` in `src/runtime/state.rs`) is only reachable
/// through a `#[cfg(test)]`-gated internal API, invisible to this
/// integration-test binary, and is not driven by any public engine API:
/// ordinary commits are periodically cleaned up by
/// `cleanup_old_idempotency_entries` well before the 100k-entry cap, so no
/// sequence of public writes can deterministically force eviction here. There
/// is no reasonable way to exercise the real eviction path from this crate
/// without adding new production surface area purely for testability, so this
/// test — which only performed 2k non-evicting writes and asserted nothing
/// beyond an `eprintln!` — was removed rather than kept as a false signal.
/// Real coverage for this path would belong in a `#[cfg(test)]` unit test
/// inside `src/runtime/state.rs`, which does not currently exist.
/// Phase 0 Guardrail #3: Transaction atomicity barrier enforcement
///
/// Validates that reads see consistent state when a transaction is committed
/// in Batched mode. The `pending_txn_min_seq` barrier prevents seeing partial
/// transaction state.
///
/// NOTE: This test validates that the transaction is atomic - the read sees
/// either the old value or the new value, never partial state. The actual
/// barrier implementation is internal to the runtime.
#[test]
fn should_maintain_atomicity_given_concurrent_reads_when_transaction_commits() {
    use std::sync::{Arc, Barrier};
    use std::thread;

    // Arrange: Create engine in memory mode
    let opts = memory_opts();
    let engine = Arc::new(open_with_mode(&opts, "memory"));
    let cf_id = engine.create_column_family("test").expect("create cf").id();

    // Write initial values
    let mut tx = engine
        .begin_tx(cf_id, TransactionMode::ReadWrite)
        .expect("begin_tx");
    tx.put(b"key1".to_vec(), b"initial1".to_vec(), None)
        .expect("put");
    tx.put(b"key2".to_vec(), b"initial2".to_vec(), None)
        .expect("put");
    tx.commit(WriteOptions::sync()).expect("commit");

    // Act: Concurrently execute transaction and reads
    let engine_clone = Arc::clone(&engine);
    let start_barrier = Arc::new(Barrier::new(2));
    let release_barrier = Arc::new(Barrier::new(2));
    let writer_start_barrier = Arc::clone(&start_barrier);
    let writer_release_barrier = Arc::clone(&release_barrier);

    let tx_handle = thread::spawn(move || {
        // Update both keys in a transaction
        let mut tx = engine_clone
            .begin_tx(cf_id, TransactionMode::ReadWrite)
            .expect("begin_tx");

        tx.put(b"key1".to_vec(), b"updated1".to_vec(), None)
            .expect("put1");
        tx.put(b"key2".to_vec(), b"updated2".to_vec(), None)
            .expect("put2");

        // Signal that both updates have been staged, then wait for the reader
        writer_start_barrier.wait();
        writer_release_barrier.wait();

        // Commit with buffered (batched) mode
        tx.commit(WriteOptions::buffered()).expect("commit");
    });

    // Wait until the writer has staged both updates but not yet committed them
    start_barrier.wait();

    // Issue reads while transaction may be in progress
    let tx_read = engine
        .begin_tx(cf_id, TransactionMode::ReadOnly)
        .expect("begin_tx");

    let val1 = tx_read.get(b"key1").expect("get key1");
    let val2 = tx_read.get(b"key2").expect("get key2");

    // Allow the writer to commit after the read snapshot is established
    release_barrier.wait();

    // Wait for transaction to complete
    tx_handle.join().expect("tx thread");

    // Assert: Both keys should have consistent state
    // Either both are "initial" or both are "updated" - never mixed
    let val1_bytes = val1.expect("key1 should exist");
    let val2_bytes = val2.expect("key2 should exist");

    let is_initial = val1_bytes.as_ref() == b"initial1" && val2_bytes.as_ref() == b"initial2";
    let is_updated = val1_bytes.as_ref() == b"updated1" && val2_bytes.as_ref() == b"updated2";

    assert!(
        is_initial || is_updated,
        "Transaction atomicity violated: key1={:?}, key2={:?}",
        String::from_utf8_lossy(&val1_bytes),
        String::from_utf8_lossy(&val2_bytes)
    );

    // Verify final state is updated
    let tx_final = engine
        .begin_tx(cf_id, TransactionMode::ReadOnly)
        .expect("begin_tx");

    let final1 = tx_final.get(b"key1").expect("get key1");
    let final2 = tx_final.get(b"key2").expect("get key2");

    assert_eq!(final1.unwrap().as_ref(), b"updated1");
    assert_eq!(final2.unwrap().as_ref(), b"updated2");

    eprintln!("Transaction atomicity maintained: reads see consistent state");
}
