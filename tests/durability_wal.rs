//! WAL (Write-Ahead Log) Durability Tests
//!
//! Tests the Write-Ahead Log's behavior for ensuring write durability and recovery.
//! These tests verify:
//! - fsync behavior and timing
//! - WAL rotation and buffer management
//! - Record replay during recovery
//! - Corruption handling
//!
//! **Storage Modes**: `LocalDisk` + `CloudBacked` ONLY (requires persistence)
//!
//! Naming convention:
//!   should_<behavior>_given_<context>_when_<condition>
//!
//! This suite covers public WAL durability/recovery behavior, not the internal
//! `KeyedGroupCommit` waiter primitive. Dedicated primitive tests and the real
//! runtime `CloudAck` fanout test establish join/rotate/complete semantics.

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
            let mut engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();

            // Write to WAL but don't flush memtable
            let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
            tx.put(b"key1".to_vec(), b"value1".to_vec(), None)
                .expect("put");
            tx.put(b"key2".to_vec(), b"value2".to_vec(), None)
                .expect("put");
            tx.commit(buffered_write_options(mode)).unwrap();
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before reopen");
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.get_column_family("test").expect("get cf");
            let cf_id = cf.id();

            let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
            assert_eq!(
                tx.get(b"key1").expect("get"),
                Some(Bytes::from_static(b"value1")),
                "mode: {mode}"
            );
            assert_eq!(
                tx.get(b"key2").expect("get"),
                Some(Bytes::from_static(b"value2")),
                "mode: {mode}"
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
            let mut engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();

            // Write with fsync guarantee (durability_opts sets fsync_enabled: true)
            let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
            tx.put(b"critical_key".to_vec(), b"critical_value".to_vec(), None)
                .expect("put");
            tx.commit(buffered_write_options(mode)).unwrap();
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before reopen");
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.get_column_family("test").expect("get cf");
            let cf_id = cf.id();

            let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
            assert_eq!(
                tx.get(b"critical_key").expect("get"),
                Some(Bytes::from_static(b"critical_value")),
                "mode: {mode}"
            );
        }
    });
}

// `should_call_fsync_given_wal_sync_enabled_when_put` was removed: it only
// asserted `put()` returned `Ok`, never observing whether fsync actually ran,
// and its commit used `buffered_write_options` (non-durable), so it exercised
// no fsync path at all despite its name. A real fix — commit with a durable
// `WriteOptions`, shut down, and reopen to prove the write survived — is
// exactly what `should_persist_write_given_fsync_enabled_when_crash_occurs`
// above already does, so it was pruned as a near-duplicate rather than
// rewritten into a copy of that test.

// ============================================================================
// WAL ROTATION TESTS
// ============================================================================

#[test]
fn should_rotate_wal_given_small_buffer_when_writes_exceed_buffer() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let mut engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();

            // Write enough data to trigger WAL rotation
            let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
            for i in 0..1000 {
                let key = format!("key_{i:04}");
                let value = format!("value_{i:04}_with_padding_to_exceed_buffer_size");
                tx.put(key.into_bytes(), value.into_bytes(), None)
                    .expect("put");
            }
            tx.commit(buffered_write_options(mode)).unwrap();
            // Force checkpoint to ensure WAL segments are created
            engine.flush_cf(&cf).expect("flush");
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before reopen");
        }

        // Assert (Phase 2): All writes recovered after rotation
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.get_column_family("test").expect("get cf");
            let cf_id = cf.id();

            // Spot check across the range
            let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
            assert!(tx.get(b"key_0000").expect("get").is_some(), "mode: {mode}");
            assert!(tx.get(b"key_0500").expect("get").is_some(), "mode: {mode}");
            assert!(tx.get(b"key_0999").expect("get").is_some(), "mode: {mode}");
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
            let mut engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();

            // Write in phases to create multiple WAL segments
            for batch in 0..3 {
                let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
                for i in 0..100 {
                    let key = format!("batch_{batch}_key_{i:03}");
                    let value = format!("batch_{batch}_value_{i:03}");
                    tx.put(key.into_bytes(), value.into_bytes(), None)
                        .expect("put");
                }
                tx.commit(buffered_write_options(mode)).unwrap();
            }
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before reopen");
        }

        // Assert (Phase 2): All records from all segments recovered
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.get_column_family("test").expect("get cf");
            let cf_id = cf.id();

            // Verify records from each batch
            let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
            for batch in 0..3 {
                for i in 0..100 {
                    let key = format!("batch_{batch}_key_{i:03}");
                    assert!(
                        tx.get(key.as_bytes()).expect("get").is_some(),
                        "Missing key from batch {batch} in mode: {mode}"
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
            let engine = std::sync::Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();

            // Concurrent writes from multiple threads
            let mut handles = vec![];
            for thread_id in 0..5 {
                let engine_clone = std::sync::Arc::clone(&engine);
                let write_options = buffered_write_options(mode);
                let handle = std::thread::spawn(move || {
                    for i in 0..20 {
                        let key = format!("thread_{thread_id}_key_{i:02}");
                        let value = format!("thread_{thread_id}_value_{i:02}");
                        let mut tx = engine_clone
                            .begin_tx(cf_id, TransactionMode::ReadWrite)
                            .unwrap();
                        tx.put(key.into_bytes(), value.into_bytes(), None)
                            .expect("put");
                        tx.commit(write_options).unwrap();
                    }
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

        // Assert (Phase 2): All concurrent writes recovered
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.get_column_family("test").expect("get cf");
            let cf_id = cf.id();

            let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
            for thread_id in 0..5 {
                for i in 0..20 {
                    let key = format!("thread_{thread_id}_key_{i:02}");
                    assert!(
                        tx.get(key.as_bytes()).expect("get").is_some(),
                        "Missing write from thread {thread_id} in mode: {mode}"
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
    // Arrange
    // Exercises the *default* open path (no explicit `.recovery_policy(..)` call),
    // unlike the dedicated Strict/Salvage trust-boundary tests below which always
    // set the policy explicitly. Several separately-committed frames are written so
    // truncating only the final frame's tail leaves the earlier frames intact.
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    {
        let mut engine = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
            .expect("open engine");
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();

        for i in 0..5 {
            let key = format!("key_{i:02}");
            let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
            tx.put(key.into_bytes(), b"value".to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::sync()).expect("sync commit");
        }
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown before corruption");
    }

    // Truncate a few trailing bytes so only the final commit's frame is incomplete.
    truncate_last_bytes(&db_path.join("wal").join("wal.log"), 3);

    // Act: reopen with default options (default recovery policy is Strict).
    let reopened = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
        .expect("default recovery should tolerate a truncated tail frame");
    let cf = reopened.get_column_family("test").expect("get cf");
    let cf_id = cf.id();

    // Assert: every frame before the truncated tail survives, the torn one does not.
    let tx = reopened.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
    for i in 0..4 {
        let key = format!("key_{i:02}");
        assert_eq!(
            tx.get(key.as_bytes()).expect("get"),
            Some(Bytes::from_static(b"value")),
            "key_{i:02} committed before the truncated tail must survive"
        );
    }
    assert_eq!(
        tx.get(b"key_04").expect("get"),
        None,
        "key_04's frame was truncated and must not be recovered"
    );
}

#[test]
fn should_not_recover_data_given_truncated_wal_append_when_reopening() {
    // Arrange
    // Simulates a *torn write* (the OS only wrote part of the value bytes before the
    // crash) by truncating deep into the payload of the final frame, as opposed to
    // `should_drop_partial_wal_entry_given_manual_tail_append_when_reopening_in_salvage_mode`
    // below, which appends synthetic garbage bytes *after* a clean shutdown.
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    {
        let mut engine = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
            .expect("open engine");
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();

        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        tx.put(b"safe_key".to_vec(), b"safe_value".to_vec(), None)
            .expect("put safe key");
        tx.commit(WriteOptions::sync()).expect("sync safe commit");

        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        tx.put(
            b"unsafe_key".to_vec(),
            b"unsafe_value_long_enough_to_truncate_mid_payload".to_vec(),
            None,
        )
        .expect("put unsafe key");
        tx.commit(WriteOptions::sync()).expect("sync unsafe commit");
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown before corruption");
    }

    // Cut well into the last frame's payload, simulating a write that was torn
    // mid-value rather than merely missing its trailing CRC bytes.
    truncate_last_bytes(&db_path.join("wal").join("wal.log"), 20);

    // Act
    let reopened = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
        .expect("recovery must tolerate a torn final frame, not panic");
    let cf = reopened.get_column_family("test").expect("get cf");
    let cf_id = cf.id();

    // Assert: the torn write is definitively lost; the prior committed key survives.
    let tx = reopened.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
    assert_eq!(
        tx.get(b"safe_key").expect("get"),
        Some(Bytes::from_static(b"safe_value")),
        "commit preceding the torn write must survive"
    );
    assert_eq!(
        tx.get(b"unsafe_key").expect("get"),
        None,
        "torn write must not be recovered"
    );
}

// ============================================================================
// DATA LOSS AND ERROR MODES
// ============================================================================

#[test]
fn should_allow_data_loss_given_skipped_fsync_when_crash_occurs() {
    // Arrange
    // Documents the fsync-disabled contract: a write that is committed without a
    // durability guarantee (buffered, no fsync) can be lost on crash. A real crash
    // can't be induced in-process, so the bytes that would never have made it past
    // the OS page cache are removed directly, and the loss is asserted concretely.
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();
    let wal_path = db_path.join("wal").join("wal.log");

    let len_before_unsynced_write = {
        let mut engine = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
            .expect("open engine");
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();

        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        tx.put(b"durable_key".to_vec(), b"durable_value".to_vec(), None)
            .expect("put durable key");
        tx.commit(WriteOptions::sync())
            .expect("sync durable commit");

        let len_before = std::fs::metadata(&wal_path)
            .expect("wal metadata before unsynced write")
            .len();

        // Write without a durability guarantee, simulating a commit that only ever
        // reached the OS page cache before the crash.
        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        tx.put(b"transient_key".to_vec(), b"transient_value".to_vec(), None)
            .expect("put transient key");
        tx.commit(WriteOptions::buffered())
            .expect("buffered commit");
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown before crash simulation");
        len_before
    };

    // Simulate the crash: truncate away everything written after the last fsync'd
    // commit, standing in for bytes that a real power-loss would never have
    // persisted past the OS page cache.
    truncate_last_bytes(
        &wal_path,
        std::fs::metadata(&wal_path)
            .expect("wal metadata after crash")
            .len()
            - len_before_unsynced_write,
    );

    // Act
    let reopened = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
        .expect("recovery after crash must not fail");
    let cf = reopened.get_column_family("test").expect("get cf");
    let cf_id = cf.id();

    // Assert: the fsync'd write survives, the never-durable write is gone.
    let tx = reopened.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
    assert_eq!(
        tx.get(b"durable_key").expect("get"),
        Some(Bytes::from_static(b"durable_value")),
        "fsync'd commit must survive the crash"
    );
    assert_eq!(
        tx.get(b"transient_key").expect("get"),
        None,
        "commit without fsync must be lost when the crash occurs before it is durable"
    );
}

#[test]
fn should_tolerate_corrupted_tail_given_recovery_mode_set_when_reopening() {
    // Arrange
    // Unlike `should_fail_strict_but_salvage_valid_prefix_given_corrupted_first_wal_frame_when_reopening`,
    // which corrupts the very first frame (leaving nothing valid to salvage), this
    // corrupts a *later* frame so Salvage mode must actually preserve the valid
    // records that precede the corruption rather than merely open successfully.
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();
    let wal_path = db_path.join("wal").join("wal.log");

    let offset_of_second_frame = {
        let mut engine = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
            .expect("open engine");
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();

        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        tx.put(b"valid_key_1".to_vec(), b"value_1".to_vec(), None)
            .expect("put");
        tx.commit(WriteOptions::sync()).expect("sync commit 1");

        let offset = std::fs::metadata(&wal_path)
            .expect("wal metadata after first commit")
            .len();

        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        tx.put(b"valid_key_2".to_vec(), b"value_2".to_vec(), None)
            .expect("put");
        tx.commit(WriteOptions::sync()).expect("sync commit 2");
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown before corruption");
        offset
    };

    // Flip a byte inside the second frame's header/payload region, leaving the
    // first frame untouched.
    corrupt_byte(&wal_path, offset_of_second_frame + 4);

    // Act: Salvage mode must open despite the mid-stream corruption.
    let reopened = Engine::open(
        OpenOptions::local(db_path)
            .recovery_policy(RecoveryPolicy::Salvage)
            .build()
            .expect("build options"),
    )
    .expect("salvage recovery should tolerate corruption after a valid prefix");
    let cf = reopened.get_column_family("test").expect("get cf");
    let cf_id = cf.id();

    // Assert: the valid prefix before the corruption is preserved, the corrupted
    // record is not.
    let tx = reopened.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
    assert_eq!(
        tx.get(b"valid_key_1").expect("get"),
        Some(Bytes::from_static(b"value_1")),
        "record committed before the corruption must survive salvage recovery"
    );
    assert_eq!(
        tx.get(b"valid_key_2").expect("get"),
        None,
        "corrupted record must not be recovered even in salvage mode"
    );
}

// ============================================================================
// PHASE 0 GUARDRAILS - CloudAsync BACKPRESSURE
// ============================================================================

// Phase 0 Guardrail #1: CloudAsync write rejection on backpressure
//
// Validates that CloudAsync mode returns WriteStall error when
// pending cloud write queue reaches capacity (100k entries).
//
// CloudAsync admission is validated against the production HybridStorage
// upload queue in its backend tests.

// ============================================================================
// LOCAL TRUST-BOUNDARY TESTS
// ============================================================================

#[test]
fn should_restore_committed_write_given_local_restart_when_sync_commit_returned() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    {
        let mut engine = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
            .expect("open engine");
        let cf = engine.create_column_family("trust").expect("create cf");

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin write tx");
        tx.put(b"committed".to_vec(), b"value".to_vec(), None)
            .expect("put committed key");
        tx.commit(WriteOptions::sync())
            .expect("sync commit must succeed");
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown before reopen");
    }

    // Act
    let reopened = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
        .expect("reopen engine");
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
        let mut engine = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
            .expect("open engine");
        let cf = engine.create_column_family("trust").expect("create cf");

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin prefix tx");
        tx.put(b"prefix".to_vec(), b"value".to_vec(), None)
            .expect("put prefix");
        tx.commit(WriteOptions::sync()).expect("sync prefix commit");

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin torn tx");
        tx.put(b"torn".to_vec(), b"value".to_vec(), None)
            .expect("put torn");
        tx.commit(WriteOptions::sync()).expect("sync torn commit");
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown before corruption");
    }

    truncate_last_bytes(&db_path.join("wal").join("wal.log"), 3);

    // Act
    let reopened = Engine::open(
        OpenOptions::local(db_path)
            .recovery_policy(RecoveryPolicy::Strict)
            .build()
            .expect("build options"),
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
        let mut engine = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
            .expect("open engine");
        let cf = engine.create_column_family("trust").expect("create cf");

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin write tx");
        tx.put(b"first".to_vec(), b"value".to_vec(), None)
            .expect("put first");
        tx.commit(WriteOptions::sync()).expect("sync commit");
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown before corruption");
    }

    corrupt_byte(&db_path.join("wal").join("wal.log"), 4);

    // Act
    let Err(strict_error) = Engine::open(
        OpenOptions::local(db_path)
            .recovery_policy(RecoveryPolicy::Strict)
            .build()
            .expect("build options"),
    ) else {
        panic!("strict recovery must reject corruption at byte zero frame");
    };

    let salvaged = Engine::open(
        OpenOptions::local(db_path)
            .recovery_policy(RecoveryPolicy::Salvage)
            .build()
            .expect("build options"),
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
        let mut engine = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
            .expect("open engine");
        let cf = engine.create_column_family("trust").expect("create cf");

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin complete tx");
        tx.put(b"complete".to_vec(), b"value".to_vec(), None)
            .expect("put complete");
        tx.commit(WriteOptions::sync())
            .expect("sync complete commit");
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown before corruption");
    }

    append_partial_frame_bytes(&db_path.join("wal").join("wal.log"));

    // Act
    let reopened = Engine::open(
        OpenOptions::local(db_path)
            .recovery_policy(RecoveryPolicy::Salvage)
            .build()
            .expect("build options"),
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
