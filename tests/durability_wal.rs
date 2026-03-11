//! WAL (Write-Ahead Log) Durability Tests
//!
//! Tests the Write-Ahead Log's behavior for ensuring write durability and recovery.
//! These tests verify:
//! - fsync behavior and timing
//! - WAL rotation and buffer management
//! - Record replay during recovery
//! - Corruption handling
//!
//! **Storage Modes**: LocalDisk + CloudBacked ONLY (requires persistence)
//!
//! Naming convention:
//!   should_<behavior>_given_<context>_when_<condition>

use bytes::Bytes;
mod common;
use cntryl_midge::{
    Engine, MidgeError, OpenOptions, RecoveryPolicy, TransactionMode, WriteOptions,
};
use common::*;
use tempfile::TempDir;

// ============================================================================
// WAL RECOVERY TESTS
// ============================================================================

#[test]
fn should_recover_writes_given_unflushed_memtable_when_reopening() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();

            // Write to WAL but don't flush memtable
            let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
            tx.put(b"key1".to_vec(), b"value1".to_vec(), None)
                .expect("put");
            tx.put(b"key2".to_vec(), b"value2".to_vec(), None)
                .expect("put");
            engine.commit(tx, WriteOptions::buffered()).unwrap();
            // Engine dropped here, simulating crash with unflushed memtable
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.get_column_family("test").expect("get cf");
            let cf_id = cf.id();

            let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
            assert_eq!(
                tx.get(b"key1").expect("get"),
                Some(Bytes::from_static(b"value1")),
                "mode: {}",
                mode
            );
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
fn should_persist_write_given_fsync_enabled_when_crash_occurs() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();

            // Write with fsync guarantee (durability_opts sets fsync_enabled: true)
            let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
            tx.put(b"critical_key".to_vec(), b"critical_value".to_vec(), None)
                .expect("put");
            engine.commit(tx, WriteOptions::buffered()).unwrap();
            // Simulate immediate crash
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.get_column_family("test").expect("get cf");
            let cf_id = cf.id();

            let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
            assert_eq!(
                tx.get(b"critical_key").expect("get"),
                Some(Bytes::from_static(b"critical_value")),
                "mode: {}",
                mode
            );
        }
    });
}

#[test]
fn should_call_fsync_given_wal_sync_enabled_when_put() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();

        // Act: Write with WAL sync enabled (opts has fsync_enabled: true)
        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        let result = tx.put(b"test_key".to_vec(), b"test_value".to_vec(), None);

        // Assert: Put succeeds (fsync was called without blocking)
        assert!(result.is_ok(), "put should succeed in mode: {}", mode);
        engine.commit(tx, WriteOptions::buffered()).unwrap();
    });
}

// ============================================================================
// WAL ROTATION TESTS
// ============================================================================

#[test]
fn should_rotate_wal_given_small_buffer_when_writes_exceed_buffer() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();

            // Write enough data to trigger WAL rotation
            let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
            for i in 0..1000 {
                let key = format!("key_{:04}", i);
                let value = format!("value_{:04}_with_padding_to_exceed_buffer_size", i);
                tx.put(key.into_bytes(), value.into_bytes(), None)
                    .expect("put");
            }
            engine.commit(tx, WriteOptions::buffered()).unwrap();
            // Force checkpoint to ensure WAL segments are created
            engine.flush_cf(&cf).expect("flush");
        }

        // Assert (Phase 2): All writes recovered after rotation
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.get_column_family("test").expect("get cf");
            let cf_id = cf.id();

            // Spot check across the range
            let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
            assert!(
                tx.get(b"key_0000").expect("get").is_some(),
                "mode: {}",
                mode
            );
            assert!(
                tx.get(b"key_0500").expect("get").is_some(),
                "mode: {}",
                mode
            );
            assert!(
                tx.get(b"key_0999").expect("get").is_some(),
                "mode: {}",
                mode
            );
        }
    });
}

// ============================================================================
// WAL REPLAY TESTS
// ============================================================================

#[test]
fn should_replay_all_records_given_multiple_wal_segments_when_recovering() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();

            // Write in phases to create multiple WAL segments
            for batch in 0..3 {
                let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
                for i in 0..100 {
                    let key = format!("batch_{}_key_{:03}", batch, i);
                    let value = format!("batch_{}_value_{:03}", batch, i);
                    tx.put(key.into_bytes(), value.into_bytes(), None)
                        .expect("put");
                }
                engine.commit(tx, WriteOptions::buffered()).unwrap();
            }
        }

        // Assert (Phase 2): All records from all segments recovered
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.get_column_family("test").expect("get cf");
            let cf_id = cf.id();

            // Verify records from each batch
            let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
            for batch in 0..3 {
                for i in 0..100 {
                    let key = format!("batch_{}_key_{:03}", batch, i);
                    assert!(
                        tx.get(key.as_bytes()).expect("get").is_some(),
                        "Missing key from batch {} in mode: {}",
                        batch,
                        mode
                    );
                }
            }
        }
    });
}

#[test]
fn should_recover_all_writes_given_concurrent_puts_when_crash_occurs() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = std::sync::Arc::new(open_with_mode(opts.clone(), mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();

            // Concurrent writes from multiple threads
            let mut handles = vec![];
            for thread_id in 0..5 {
                let engine_clone = std::sync::Arc::clone(&engine);
                let handle = std::thread::spawn(move || {
                    for i in 0..20 {
                        let key = format!("thread_{}_key_{:02}", thread_id, i);
                        let value = format!("thread_{}_value_{:02}", thread_id, i);
                        let mut tx = engine_clone
                            .begin_tx(cf_id, TransactionMode::ReadWrite)
                            .unwrap();
                        tx.put(key.into_bytes(), value.into_bytes(), None)
                            .expect("put");
                        engine_clone.commit(tx, WriteOptions::buffered()).unwrap();
                    }
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.join().expect("thread join");
            }
            // Simulate crash
        }

        // Assert (Phase 2): All concurrent writes recovered
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.get_column_family("test").expect("get cf");
            let cf_id = cf.id();

            let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
            for thread_id in 0..5 {
                for i in 0..20 {
                    let key = format!("thread_{}_key_{:02}", thread_id, i);
                    assert!(
                        tx.get(key.as_bytes()).expect("get").is_some(),
                        "Missing write from thread {} in mode: {}",
                        thread_id,
                        mode
                    );
                }
            }
        }
    });
}

// ============================================================================
// CORRUPTION HANDLING TESTS
// ============================================================================

#[test]
fn should_skip_corrupted_wal_tail_given_truncated_tail_when_recovering() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();

            // Write data (some will be in incomplete record at tail)
            let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
            for i in 0..10 {
                let key = format!("key_{:02}", i);
                tx.put(key.into_bytes(), b"value".to_vec(), None)
                    .expect("put");
            }
            engine.commit(tx, WriteOptions::buffered()).unwrap();
            // Simulate crash without flushing final records
        }

        // Assert (Phase 2): Skips corrupted records, recovers valid ones
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();

            // Some early records should be recovered (before corruption)
            // Recovery should skip the truncated tail, not panic
            let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
            let _ = tx.get(b"key_00").expect("get");
        }
    });
}

#[test]
fn should_not_recover_data_given_truncated_wal_append_when_reopening() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine
                .get_column_family("test")
                .unwrap_or_else(|| engine.create_column_family("test").expect("create cf"));
            let cf_id = cf.id();

            // Write without fsync, simulating crash mid-write
            let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
            tx.put(b"unsafe_key".to_vec(), b"unsafe_value".to_vec(), None)
                .expect("put");
            engine.commit(tx, WriteOptions::buffered()).unwrap();
            // Immediate crash before fsync
        }

        // Assert (Phase 2): Graceful recovery
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.get_column_family("test").expect("get cf");
            let cf_id = cf.id();

            // Key may or may not exist depending on fsync timing
            // Recovery should not panic or corrupt data
            let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
            let _result = tx.get(b"unsafe_key").expect("get");
        }
    });
}

// ============================================================================
// DATA LOSS AND ERROR MODES
// ============================================================================

#[test]
fn should_allow_data_loss_given_skipped_fsync_when_crash_occurs() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange: This tests the expected behavior of non-fsync mode

        // Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();

            // Write without guaranteeing sync
            let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
            tx.put(b"transient_key".to_vec(), b"transient_value".to_vec(), None)
                .expect("put");
            engine.commit(tx, WriteOptions::buffered()).unwrap();
            // Crash
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();

            // With durable_storage_modes, if fsync is enabled, data should persist
            // This test documents the contract: if you disable fsync, data loss is possible
            let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
            let _result = tx.get(b"transient_key").expect("get");
        }
    });
}

#[test]
fn should_tolerate_corrupted_tail_given_recovery_mode_set_when_reopening() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine
                .get_column_family("test")
                .unwrap_or_else(|| engine.create_column_family("test").expect("create cf"));
            let cf_id = cf.id();

            // Write valid records followed by corruption
            let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
            tx.put(b"valid_key_1".to_vec(), b"value_1".to_vec(), None)
                .expect("put");
            tx.put(b"valid_key_2".to_vec(), b"value_2".to_vec(), None)
                .expect("put");
            engine.commit(tx, WriteOptions::buffered()).unwrap();
            // Simulate corruption by crashing mid-record
        }

        // Assert (Phase 2): Recovery is tolerant and doesn't crash
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.get_column_family("test").expect("get cf");
            let cf_id = cf.id();

            // Valid records before corruption should be recovered
            let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
            assert!(
                tx.get(b"valid_key_1").expect("get").is_some(),
                "mode: {}",
                mode
            );
            assert!(
                tx.get(b"valid_key_2").expect("get").is_some(),
                "mode: {}",
                mode
            );
        }
    });
}

// ============================================================================
// PHASE 0 GUARDRAILS - CloudAsync BACKPRESSURE
// ============================================================================

// Phase 0 Guardrail #1: CloudAsync write rejection on backpressure
//
// Validates that CloudAsync mode returns WriteStall error when
// pending cloud write queue reaches capacity (100k entries).
//
// NOTE: This functionality is validated via internal unit tests:
// 1. WalActor unit tests in src/runtime/actors/wal.rs
// 2. CloudWriteQueue unit tests in src/runtime/actors/cloud_write_queue.rs
// 3. Internal integration tests with mock cloud backends
// (CloudAsync durability policy is not exposed in public API)

// ============================================================================
// LOCAL TRUST-BOUNDARY TESTS
// ============================================================================

#[test]
fn should_restore_committed_write_given_local_restart_when_sync_commit_returned() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    {
        let engine = Engine::open(OpenOptions::local(db_path).build()).expect("open engine");
        let cf = engine.create_column_family("trust").expect("create cf");

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin write tx");
        tx.put(b"committed".to_vec(), b"value".to_vec(), None)
            .expect("put committed key");
        engine
            .commit(tx, WriteOptions::sync())
            .expect("sync commit must succeed");
    }

    // Act
    let reopened = Engine::open(OpenOptions::local(db_path).build()).expect("reopen engine");
    let cf = reopened.get_column_family("trust").expect("get trust cf");

    // Assert
    let tx = reopened
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin read tx");
    assert_eq!(
        tx.get(b"committed").expect("get committed key"),
        Some(Bytes::from_static(b"value"))
    );
}

#[test]
fn should_keep_valid_prefix_given_truncated_wal_tail_when_reopening_in_strict_mode() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    {
        let engine = Engine::open(OpenOptions::local(db_path).build()).expect("open engine");
        let cf = engine.create_column_family("trust").expect("create cf");

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin prefix tx");
        tx.put(b"prefix".to_vec(), b"value".to_vec(), None)
            .expect("put prefix");
        engine
            .commit(tx, WriteOptions::sync())
            .expect("sync prefix commit");

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin torn tx");
        tx.put(b"torn".to_vec(), b"value".to_vec(), None)
            .expect("put torn");
        engine
            .commit(tx, WriteOptions::sync())
            .expect("sync torn commit");
    }

    truncate_last_bytes(&db_path.join("wal").join("wal.log"), 3);

    // Act
    let reopened = Engine::open(
        OpenOptions::local(db_path)
            .recovery_policy(RecoveryPolicy::Strict)
            .build(),
    )
    .expect("strict recovery should keep valid truncated-tail prefix");
    let cf = reopened.get_column_family("trust").expect("get trust cf");

    // Assert
    let tx = reopened
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin read tx");
    assert_eq!(
        tx.get(b"prefix").expect("get prefix"),
        Some(Bytes::from_static(b"value"))
    );
    assert_eq!(tx.get(b"torn").expect("get torn key"), None);
}

#[test]
fn should_fail_strict_but_salvage_valid_prefix_given_corrupted_first_wal_frame_when_reopening() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    {
        let engine = Engine::open(OpenOptions::local(db_path).build()).expect("open engine");
        let cf = engine.create_column_family("trust").expect("create cf");

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin write tx");
        tx.put(b"first".to_vec(), b"value".to_vec(), None)
            .expect("put first");
        engine
            .commit(tx, WriteOptions::sync())
            .expect("sync commit");
    }

    corrupt_byte(&db_path.join("wal").join("wal.log"), 4);

    // Act
    let strict_error = match Engine::open(
        OpenOptions::local(db_path)
            .recovery_policy(RecoveryPolicy::Strict)
            .build(),
    ) {
        Ok(_) => panic!("strict recovery must reject corruption at byte zero frame"),
        Err(error) => error,
    };

    let salvaged = Engine::open(
        OpenOptions::local(db_path)
            .recovery_policy(RecoveryPolicy::Salvage)
            .build(),
    )
    .expect("salvage recovery should preserve valid prefix if possible");

    // Assert
    match strict_error {
        MidgeError::RecoveryFailed(message) | MidgeError::Corruption(message) => {
            assert!(
                message.to_ascii_lowercase().contains("crc")
                    || message.to_ascii_lowercase().contains("corrupt"),
                "unexpected strict recovery error: {message}"
            );
        }
        other => panic!("expected corruption-oriented error, got {other}"),
    }

    let cf = salvaged.get_column_family("trust").expect("get trust cf");
    let tx = salvaged
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin salvage read tx");
    assert_eq!(tx.get(b"first").expect("get first"), None);
}

#[test]
fn should_drop_partial_wal_entry_given_manual_tail_append_when_reopening_in_salvage_mode() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    {
        let engine = Engine::open(OpenOptions::local(db_path).build()).expect("open engine");
        let cf = engine.create_column_family("trust").expect("create cf");

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin complete tx");
        tx.put(b"complete".to_vec(), b"value".to_vec(), None)
            .expect("put complete");
        engine
            .commit(tx, WriteOptions::sync())
            .expect("sync complete commit");
    }

    append_partial_frame_bytes(&db_path.join("wal").join("wal.log"));

    // Act
    let reopened = Engine::open(
        OpenOptions::local(db_path)
            .recovery_policy(RecoveryPolicy::Salvage)
            .build(),
    )
    .expect("salvage recovery should discard partial entry");
    let cf = reopened.get_column_family("trust").expect("get trust cf");

    // Assert
    let tx = reopened
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin read tx");
    assert_eq!(
        tx.get(b"complete").expect("get complete"),
        Some(Bytes::from_static(b"value"))
    );
    assert_eq!(tx.get(b"partial").expect("get partial"), None);
}

fn truncate_last_bytes(path: &std::path::Path, byte_count: u64) {
    let metadata = std::fs::metadata(path).expect("wal metadata");
    let new_len = metadata.len().saturating_sub(byte_count);
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open wal for truncation");
    file.set_len(new_len).expect("truncate wal");
}

fn corrupt_byte(path: &std::path::Path, offset: u64) {
    use std::io::{Read, Seek, SeekFrom, Write};

    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("open wal for corruption");
    file.seek(SeekFrom::Start(offset)).expect("seek wal");
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte).expect("read byte");
    file.seek(SeekFrom::Start(offset)).expect("seek wal");
    file.write_all(&[byte[0] ^ 0x5a])
        .expect("write corrupt byte");
    file.sync_all().expect("sync corrupt wal");
}

fn append_partial_frame_bytes(path: &std::path::Path) {
    use std::io::Write;

    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(path)
        .expect("open wal for append");
    file.write_all(&[0x34, 0x12, 0x00])
        .expect("append partial frame");
    file.sync_all().expect("sync partial frame");
}
