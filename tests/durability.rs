//! Durability Tests
//!
//! Consolidated from: `durability_wal.rs`, `durability_recovery.rs`, `durability_atomicity.rs`, `durability_sync_count.rs`, `best_effort_durability.rs`, `concurrent_commit_durability.rs`, `recovery_policy_api.rs`

mod common;

mod durability_wal {
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
    //! should_<behavior>_given_<context>_when_<condition>
    //!
    //! This suite covers public WAL durability/recovery behavior, not the internal
    //! `KeyedGroupCommit` waiter primitive. Dedicated primitive tests and the real
    //! runtime `CloudAck` fanout test establish join/rotate/complete semantics.

    use crate::common::*;
    use bytes::Bytes;
    use cntryl_midge::{
        Engine, MidgeError, OpenOptions, RecoveryPolicy, TransactionMode, WriteOptions,
    };
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
            let mut engine =
                Engine::open(OpenOptions::local(db_path).build().expect("build options"))
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
            let mut engine =
                Engine::open(OpenOptions::local(db_path).build().expect("build options"))
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
            let mut engine =
                Engine::open(OpenOptions::local(db_path).build().expect("build options"))
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
            let mut engine =
                Engine::open(OpenOptions::local(db_path).build().expect("build options"))
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
            let mut engine =
                Engine::open(OpenOptions::local(db_path).build().expect("build options"))
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
            let mut engine =
                Engine::open(OpenOptions::local(db_path).build().expect("build options"))
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
    fn should_fail_strict_but_salvage_valid_prefix_given_corrupted_first_wal_frame_when_reopening()
    {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        {
            let mut engine =
                Engine::open(OpenOptions::local(db_path).build().expect("build options"))
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
            let mut engine =
                Engine::open(OpenOptions::local(db_path).build().expect("build options"))
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
}

mod durability_recovery {
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
    //! **Storage Modes**: `LocalDisk` + `CloudBacked` ONLY (requires persistence)
    //!
    //! Naming convention:
    //! should_<behavior>_given_<context>_when_<condition>

    use crate::common::*;
    use bytes::Bytes;
    use cntryl_midge::{ConflictPolicy, TransactionMode};

    // ============================================================================
    // BASIC RECOVERY TESTS
    // ============================================================================

    #[test]
    fn should_recover_from_clean_shutdown_when_reopening() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            // Act (Phase 1)
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Write and flush data cleanly
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(b"key1".to_vec(), b"value1".to_vec(), None)
                    .expect("put");
                tx.commit(buffered_write_options(mode)).expect("commit");
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(b"key2".to_vec(), b"value2".to_vec(), None)
                    .expect("put");
                tx.commit(buffered_write_options(mode)).expect("commit");
                engine.flush_cf(&cf).expect("flush");
                engine
                    .shutdown(std::time::Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Assert (Phase 2)
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");
                assert_eq!(
                    tx.get(b"key1").expect("get"),
                    Some(Bytes::from_static(b"value1")),
                    "mode: {mode}"
                );
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
    fn should_recover_after_clean_shutdown_when_writes_include_flushed_and_unflushed_data() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            // Act (Phase 1)
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Write, flush, then add additional committed writes
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(b"flushed_key".to_vec(), b"flushed_value".to_vec(), None)
                    .expect("put");
                tx.commit(buffered_write_options(mode)).expect("commit");
                engine.flush_cf(&cf).expect("flush");

                // Additional writes to memtable (not flushed)
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(b"unflushed_key".to_vec(), b"unflushed_value".to_vec(), None)
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

                // Flushed data recoverable from SST
                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");
                assert_eq!(
                    tx.get(b"flushed_key").expect("get"),
                    Some(Bytes::from_static(b"flushed_value")),
                    "mode: {mode}"
                );
                // Unflushed data recoverable from WAL
                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");
                assert_eq!(
                    tx.get(b"unflushed_key").expect("get"),
                    Some(Bytes::from_static(b"unflushed_value")),
                    "mode: {mode}"
                );
            }
        });
    }

    #[test]
    fn should_preserve_first_commit_given_conflict_abort_when_reopening() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                let mut tx1 = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin tx1");
                let mut tx2 = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin tx2");

                tx1.set_conflict_policy(ConflictPolicy::AbortOnWriteConflict);
                tx2.set_conflict_policy(ConflictPolicy::AbortOnWriteConflict);

                tx1.put(b"key".to_vec(), b"from_tx1".to_vec(), None)
                    .expect("tx1 put");
                tx2.put(b"key".to_vec(), b"from_tx2".to_vec(), None)
                    .expect("tx2 put");

                // Act
                tx1.commit(buffered_write_options(mode))
                    .expect("commit tx1");
                let conflict = tx2.commit(buffered_write_options(mode));

                // Assert
                assert!(
                    matches!(conflict, Err(cntryl_midge::MidgeError::WriteConflict(_))),
                    "mode: {mode}"
                );
                engine
                    .shutdown(std::time::Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Arrange
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Read after reopen
                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin tx");

                // Assert
                assert_eq!(
                    tx.get(b"key").expect("get"),
                    Some(Bytes::from_static(b"from_tx1")),
                    "mode: {mode}"
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
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Write data
                for i in 0..100 {
                    let key = format!("key_{i:03}");
                    let value = format!("value_{i:03}");
                    let mut tx = engine
                        .begin_tx(cf.id(), TransactionMode::ReadWrite)
                        .expect("begin_tx");
                    tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
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

                // Data should be recoverable from WAL
                for i in 0..100 {
                    let key = format!("key_{i:03}");
                    let expected = Bytes::from(format!("value_{i:03}"));
                    let tx = engine
                        .begin_tx(cf.id(), TransactionMode::ReadOnly)
                        .expect("begin_tx");
                    assert_eq!(
                        tx.get(key.as_bytes()).expect("get"),
                        Some(expected),
                        "mode: {mode}"
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
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Write v1, flush to SST
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(b"key".to_vec(), b"value_v1".to_vec(), None)
                    .expect("put");
                tx.commit(buffered_write_options(mode)).expect("commit");
                engine.flush_cf(&cf).expect("flush");

                // Overwrite with v2 (in WAL only)
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(b"key".to_vec(), b"value_v2".to_vec(), None)
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

                // Should prefer newer value from WAL
                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");
                assert_eq!(
                    tx.get(b"key").expect("get"),
                    Some(Bytes::from_static(b"value_v2")),
                    "mode: {mode}"
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
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Write v1, flush to SST (WAL can be discarded)
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(b"key".to_vec(), b"value_v1".to_vec(), None)
                    .expect("put");
                tx.commit(buffered_write_options(mode)).expect("commit");
                engine.flush_cf(&cf).expect("flush");
                engine
                    .shutdown(std::time::Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Assert (Phase 2)
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Should recover from SST (WAL not needed)
                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");
                assert_eq!(
                    tx.get(b"key").expect("get"),
                    Some(Bytes::from_static(b"value_v1")),
                    "mode: {mode}"
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
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Write sequence (order matters)
                for i in 0..100 {
                    let key = format!("seq_key_{i:03}");
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
                engine
                    .shutdown(std::time::Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Assert (Phase 2)
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Verify correct order (last write wins for same key)
                for i in 0..100 {
                    let key = format!("seq_key_{i:03}");
                    let expected = Bytes::from(format!("value_{i:03}"));
                    let tx = engine
                        .begin_tx(cf.id(), TransactionMode::ReadOnly)
                        .expect("begin_tx");
                    assert_eq!(
                        tx.get(key.as_bytes()).expect("get"),
                        Some(expected),
                        "mode: {mode}"
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
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Write and flush
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(b"to_delete".to_vec(), b"value".to_vec(), None)
                    .expect("put");
                tx.commit(buffered_write_options(mode)).expect("commit");
                engine.flush_cf(&cf).expect("flush");

                // Delete (written to WAL but not yet persisted)
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.delete(b"to_delete".to_vec()).expect("delete");
                tx.commit(buffered_write_options(mode)).expect("commit");
                engine
                    .shutdown(std::time::Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Assert (Phase 2)
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Deletion should be recovered from WAL
                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");
                assert!(
                    tx.get(b"to_delete").expect("get").is_none(),
                    "delete not recovered from WAL in mode: {mode}"
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
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Write committed data without forcing an SST flush
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(b"key".to_vec(), b"value".to_vec(), None)
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

                // Recovery should still work via WAL
                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");
                assert_eq!(
                    tx.get(b"key").expect("get"),
                    Some(Bytes::from_static(b"value")),
                    "mode: {mode}"
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
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Write multiple batches
                for batch_num in 0..3 {
                    for i in 0..10 {
                        let key = format!("batch_{batch_num}_key_{i:02}");
                        let mut tx = engine
                            .begin_tx(cf.id(), TransactionMode::ReadWrite)
                            .expect("begin_tx");
                        tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                            .expect("put");
                        tx.commit(buffered_write_options(mode)).expect("commit");
                    }
                }
                engine
                    .shutdown(std::time::Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Assert (Phase 2)
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // All writes should be recoverable
                for batch_num in 0..3 {
                    for i in 0..10 {
                        let key = format!("batch_{batch_num}_key_{i:02}");
                        let expected = Bytes::from_static(b"value");
                        let tx = engine
                            .begin_tx(cf.id(), TransactionMode::ReadOnly)
                            .expect("begin_tx");
                        assert_eq!(
                            tx.get(key.as_bytes()).expect("get"),
                            Some(expected.clone()),
                            "mode: {mode}"
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
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(b"key1".to_vec(), b"value1".to_vec(), None)
                    .expect("put");
                tx.commit(buffered_write_options(mode)).expect("commit");
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

            // Act (Recovery cycles)
            {
                // First reopen cycle: open and drop again
                let mut engine = open_with_mode(&opts, mode);
                engine
                    .shutdown(std::time::Duration::from_secs(5))
                    .expect("shutdown before second reopen");

                // Second recovery: open and verify final state
                let engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Assert - final state should be correct after multiple restarts
                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");
                assert_eq!(
                    tx.get(b"key1").expect("get"),
                    Some(Bytes::from_static(b"value1")),
                    "mode: {mode}"
                );
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
    fn should_continue_sequence_numbers_when_new_writes_follow_clean_reopen() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(b"seq_1".to_vec(), b"value_1".to_vec(), None)
                    .expect("put");
                tx.commit(buffered_write_options(mode)).expect("commit");
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(b"seq_2".to_vec(), b"value_2".to_vec(), None)
                    .expect("put");
                tx.commit(buffered_write_options(mode)).expect("commit");
                engine
                    .shutdown(std::time::Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Act (Phase 2: Reopen and new writes)
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Verify previously committed data is visible after reopen
                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");
                assert_eq!(
                    tx.get(b"seq_1").expect("get"),
                    Some(Bytes::from_static(b"value_1")),
                    "mode: {mode}"
                );
                drop(tx);

                // Recovery must have restored the sequence counter to at least the
                // two commits made before shutdown, not reset it to zero.
                let sequence_after_reopen = engine
                    .get_runtime_metrics()
                    .expect("runtime metrics after reopen")
                    .current_sequence;
                assert!(
                    sequence_after_reopen >= 2,
                    "sequence counter must be recovered past the 2 pre-shutdown commits, \
                     got {sequence_after_reopen}, mode: {mode}"
                );

                // Write new data (sequence numbers should continue, not restart)
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(b"seq_3".to_vec(), b"value_3".to_vec(), None)
                    .expect("put");
                tx.commit(buffered_write_options(mode)).expect("commit");
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(b"seq_4".to_vec(), b"value_4".to_vec(), None)
                    .expect("put");
                tx.commit(buffered_write_options(mode)).expect("commit");

                let sequence_after_new_writes = engine
                    .get_runtime_metrics()
                    .expect("runtime metrics after new writes")
                    .current_sequence;
                assert!(
                    sequence_after_new_writes > sequence_after_reopen,
                    "sequence counter must strictly advance past the recovered value \
                     rather than restart, before: {sequence_after_reopen}, \
                     after: {sequence_after_new_writes}, mode: {mode}"
                );

                engine
                    .shutdown(std::time::Duration::from_secs(5))
                    .expect("shutdown before final reopen");
            }

            // Assert (Phase 3)
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // All data including post-recovery writes should be present
                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");
                assert_eq!(
                    tx.get(b"seq_1").expect("get"),
                    Some(Bytes::from_static(b"value_1")),
                    "mode: {mode}"
                );
                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");
                assert_eq!(
                    tx.get(b"seq_3").expect("get"),
                    Some(Bytes::from_static(b"value_3")),
                    "mode: {mode}"
                );
                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");
                assert_eq!(
                    tx.get(b"seq_4").expect("get"),
                    Some(Bytes::from_static(b"value_4")),
                    "mode: {mode}"
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
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Write valid records
                for i in 0..50 {
                    let key = format!("valid_{i:03}");
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

                // Valid committed records should be recovered on reopen
                for i in 0..50 {
                    let key = format!("valid_{i:03}");
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
}

mod durability_atomicity {
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
    //! should_<behavior>_given_<context>_when_<condition>

    use crate::common::*;
    use bytes::Bytes;
    use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};

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

    #[test]
    fn should_not_resurrect_manifest_covered_value_from_retained_wal_after_tombstone_gc() {
        // Arrange
        let directory = tempfile::tempdir().expect("create database directory");
        let options = || {
            OpenOptions::local(directory.path())
                .build()
                .expect("build local options")
        };
        let mut engine = Engine::open(options()).expect("open database");
        let cf = engine
            .get_column_family("default")
            .expect("default column family");
        let mut seed = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin seed transaction");
        seed.put(b"a-anchor".to_vec(), b"a".to_vec(), None)
            .expect("put lower anchor");
        seed.put(b"target".to_vec(), b"stale".to_vec(), None)
            .expect("put target");
        seed.put(b"z-anchor".to_vec(), b"z".to_vec(), None)
            .expect("put upper anchor");
        seed.commit(WriteOptions::sync()).expect("commit seed");
        let retained_wal = std::fs::read(directory.path().join("wal/wal.log"))
            .expect("capture retained WAL bytes");
        engine.flush_cf(&cf).expect("flush seed values");

        let mut delete = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin delete transaction");
        delete.delete(b"target".to_vec()).expect("delete target");
        delete
            .commit(WriteOptions::sync())
            .expect("commit durable delete");
        engine.flush_cf(&cf).expect("flush delete tombstone");
        for index in 0..2 {
            let mut filler = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin filler transaction");
            filler
                .put(
                    format!("m-filler-{index}").into_bytes(),
                    b"filler".to_vec(),
                    None,
                )
                .expect("put filler");
            filler.commit(WriteOptions::sync()).expect("commit filler");
            engine.flush_cf(&cf).expect("flush filler");
        }
        engine
            .compact_all()
            .expect("compact tombstone and old value");
        let read = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin pre-reopen read");
        assert_eq!(read.get(b"target").expect("read deleted target"), None);
        drop(read);
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown database");

        // Act: model conservative WAL retention after an unrelated record prevents
        // whole-segment pruning. The manifest already incorporates this old batch.
        std::fs::write(
            directory.path().join("wal/00000000000000000000.wal"),
            retained_wal,
        )
        .expect("restore retained WAL segment");
        let reopened = Engine::open(options()).expect("reopen database");
        let cf = reopened
            .get_column_family("default")
            .expect("default column family after reopen");
        let read = reopened
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin post-reopen read");

        // Assert
        assert_eq!(
            read.get(b"target").expect("read target after reopen"),
            None,
            "manifest-covered WAL history must not resurrect a compacted tombstone"
        );
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
}

mod best_effort_durability {
    //! Test `BestEffort` durability mode - verifies WAL is skipped but data is in memtable
    //!
    //! `BestEffort` mode should:
    //! 1. Skip WAL writes entirely (no I/O overhead)
    //! 2. Update memtable immediately (data visible for reads)
    //! 3. Allow flush to SST (data becomes durably restart-safe after flush)
    //! 4. Lose data on crash before flush (documented trade-off)

    use crate::common::*;
    use cntryl_midge::{MidgeEngine, TransactionMode, WriteOptions};
    use std::time::Duration;

    #[test]
    fn should_skip_wal_when_using_best_effort() -> cntryl_midge::MidgeResult<()> {
        // Arrange
        let opts = opts_for_mode("local");
        let engine = MidgeEngine::open(opts.to_open_options())?;
        let cf = engine.create_column_family("test")?;

        // Act - Write with BestEffort (should skip WAL)
        let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
        tx.put(b"key1".to_vec(), b"value1".to_vec(), None)?;
        tx.put(b"key2".to_vec(), b"value2".to_vec(), None)?;
        tx.commit(WriteOptions::best_effort())?;

        // Assert - Data is visible in memtable
        let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
        let val1 = tx.get(b"key1")?;
        let val2 = tx.get(b"key2")?;
        assert_eq!(val1.as_deref(), Some(&b"value1"[..]));
        assert_eq!(val2.as_deref(), Some(&b"value2"[..]));

        Ok(())
    }

    #[test]
    fn should_persist_best_effort_data_when_flushed() -> cntryl_midge::MidgeResult<()> {
        // Arrange
        let opts = opts_for_mode("local");
        let mut engine = MidgeEngine::open(opts.clone().to_open_options())?;
        let cf = engine.create_column_family("test")?;
        let cf_id = cf.id();

        // Act - Write with BestEffort, then flush
        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite)?;
        for i in 0..100 {
            tx.put(
                format!("key{i}").into_bytes(),
                format!("value{i}").into_bytes(),
                None,
            )?;
        }
        tx.commit(WriteOptions::best_effort())?;

        // Flush to SST - but note: without WAL, BestEffort data relies ONLY on successful flush
        engine.flush_cf(&cf)?;

        // Write a durable marker after flush so restart exercises both WAL replay and
        // manifest-backed SST visibility on reopen.
        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite)?;
        tx.put(b"durable_marker".to_vec(), b"marker".to_vec(), None)?;
        tx.commit(WriteOptions::buffered())?;

        // Reopen engine (simulates restart)
        engine.shutdown(Duration::from_secs(2))?;
        let engine = MidgeEngine::open(opts.to_open_options())?;

        // Assert - Once flush_cf() succeeds, flushed data must be durable across restart
        let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly)?;
        let val_0 = tx.get(b"key0")?;
        let marker = tx.get(b"durable_marker")?;

        assert!(
            val_0.is_some(),
            "BestEffort data should survive restart once flush_cf() succeeds"
        );
        assert_eq!(marker.as_deref(), Some(&b"marker"[..]));

        Ok(())
    }

    #[test]
    fn should_lose_best_effort_data_when_not_flushed() -> cntryl_midge::MidgeResult<()> {
        // Arrange
        let opts = opts_for_mode("local");
        let mut engine = MidgeEngine::open(opts.clone().to_open_options())?;
        let cf = engine.create_column_family("test")?;
        let cf_id = cf.id();

        // Act - Write with BestEffort but DON'T flush
        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite)?;
        tx.put(b"ephemeral_key".to_vec(), b"ephemeral_value".to_vec(), None)?;
        tx.commit(WriteOptions::best_effort())?;

        // Verify data is in memtable before restart
        let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly)?;
        let val_before = tx.get(b"ephemeral_key")?;
        assert_eq!(val_before.as_deref(), Some(&b"ephemeral_value"[..]));
        drop(tx);

        // Simulate crash: drop engine WITHOUT flush
        engine.shutdown(Duration::from_secs(2))?;

        // Reopen engine
        let engine = MidgeEngine::open(opts.to_open_options())?;

        // Assert - Data is lost (not in WAL, not in SST)
        let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly)?;
        let val_after = tx.get(b"ephemeral_key")?;
        assert_eq!(
            val_after, None,
            "BestEffort data should be lost on crash without flush"
        );

        Ok(())
    }

    #[test]
    fn should_handle_large_batches_with_best_effort() -> cntryl_midge::MidgeResult<()> {
        // Arrange - This tests the original YCSB issue: large batches shouldn't overflow WAL queue
        let opts = opts_for_mode("local");
        let engine = MidgeEngine::open(opts.to_open_options())?;
        let cf = engine.create_column_family("test")?;

        // Act - Write 50,000 ops (same size as YCSB batch that triggered the bug)
        // This would previously fail with "WAL queue full (1000 items)"
        let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
        for i in 0..50_000 {
            tx.put(
                format!("key{i:08}").into_bytes(),
                format!("value{i:08}").into_bytes(),
                None,
            )?;
        }

        // This should NOT panic with "WAL queue full" anymore
        tx.commit(WriteOptions::best_effort())?;

        // Assert - All data is visible
        let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
        let val_first = tx.get(b"key00000000")?;
        let val_mid = tx.get(b"key00025000")?;
        let val_last = tx.get(b"key00049999")?;

        assert!(val_first.is_some(), "First key should exist");
        assert!(val_mid.is_some(), "Middle key should exist");
        assert!(val_last.is_some(), "Last key should exist");

        Ok(())
    }
}

mod concurrent_commit_durability {
    use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;

    #[test]
    fn should_apply_all_concurrent_commits_durably_when_many_threads_write_simultaneously() {
        // Arrange
        const THREADS: usize = 4;
        const COMMITS_PER_THREAD: usize = 32;

        let temp_dir = TempDir::new().expect("temp dir");
        let options = OpenOptions::local(temp_dir.path())
            .background_compaction(false)
            .build()
            .expect("build local options");
        let engine = Arc::new(Engine::open(options.clone()).expect("open engine"));
        let cf_id = engine
            .create_column_family("concurrent-commits")
            .expect("create column family")
            .id();

        // Act
        let writers = (0..THREADS)
            .map(|thread_id| {
                let engine = Arc::clone(&engine);
                std::thread::spawn(move || {
                    for commit_id in 0..COMMITS_PER_THREAD {
                        let key = format!("thread-{thread_id:02}-key-{commit_id:03}");
                        let value = format!("thread-{thread_id:02}-value-{commit_id:03}");
                        let mut tx = engine
                            .begin_tx(cf_id, TransactionMode::ReadWrite)
                            .expect("begin write transaction");
                        tx.put(key.into_bytes(), value.into_bytes(), None)
                            .expect("put unique value");
                        tx.commit(WriteOptions::sync())
                            .expect("commit unique value durably");
                    }
                })
            })
            .collect::<Vec<_>>();

        for writer in writers {
            writer.join().expect("join writer");
        }

        let mut engine = Arc::try_unwrap(engine).ok().expect("unique engine");
        engine
            .shutdown(Duration::from_secs(5))
            .expect("shutdown before recovery check");
        let mut reopened = Engine::open(options).expect("reopen engine");
        let reopened_cf = reopened
            .get_column_family("concurrent-commits")
            .expect("recover column family");
        let read_tx = reopened
            .begin_tx(reopened_cf.id(), TransactionMode::ReadOnly)
            .expect("begin recovery read");

        // Assert
        for thread_id in 0..THREADS {
            for commit_id in 0..COMMITS_PER_THREAD {
                let key = format!("thread-{thread_id:02}-key-{commit_id:03}");
                let expected = format!("thread-{thread_id:02}-value-{commit_id:03}");
                let actual = read_tx
                    .get(key.as_bytes())
                    .expect("read recovered value")
                    .expect("recovered value exists");
                assert_eq!(actual.as_ref(), expected.as_bytes(), "key: {key}");
            }
        }

        drop(read_tx);
        reopened
            .shutdown(Duration::from_secs(5))
            .expect("shutdown recovered engine");
    }
}

mod recovery_policy_api {
    use cntryl_midge::{Engine, EngineHealth, MidgeError, OpenOptions, RecoveryPolicy};
    use std::fs;
    use std::time::Duration;
    use tempfile::TempDir;

    fn initialize_format_marker(db_path: &std::path::Path) {
        let mut engine = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
            .expect("initialize engine");
        engine
            .shutdown(Duration::from_secs(2))
            .expect("shutdown initialized engine");
    }

    fn write_corrupt_wal(db_path: &std::path::Path) {
        let wal_dir = db_path.join("wal");
        fs::create_dir_all(&wal_dir).expect("create wal dir");
        // A sealed segment is not allowed to hide an incomplete/corrupt prefix;
        // the active `wal.log` tail is intentionally tolerated by strict replay.
        fs::write(
            wal_dir.join(cntryl_midge::wal::segment_file_name(1)),
            b"\x01\x02\x03",
        )
        .expect("write corrupt wal");
    }

    #[test]
    fn should_fail_strict_open_when_manifest_journal_is_corrupt() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        initialize_format_marker(db_path);

        fs::write(
            db_path.join("manifest.journal"),
            b"not-a-valid-manifest-journal",
        )
        .expect("write corrupt manifest journal");

        // Act
        let result = Engine::open(
            OpenOptions::local(db_path)
                .recovery_policy(RecoveryPolicy::Strict)
                .build()
                .expect("build options"),
        );

        // Assert
        match result {
            Err(MidgeError::RecoveryFailed(message)) => {
                assert!(
                    message.contains("manifest"),
                    "expected manifest recovery context, got: {message}"
                );
            }
            Ok(_) => panic!("expected strict recovery failure, got successful open"),
            Err(other) => panic!("expected RecoveryFailed, got: {other}"),
        }
    }

    #[test]
    fn should_open_in_salvage_mode_when_manifest_journal_is_corrupt() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        initialize_format_marker(db_path);

        fs::write(
            db_path.join("manifest.journal"),
            b"not-a-valid-manifest-journal",
        )
        .expect("write corrupt manifest journal");

        // Act
        let engine = Engine::open(
            OpenOptions::local(db_path)
                .recovery_policy(RecoveryPolicy::Salvage)
                .build()
                .expect("build options"),
        )
        .expect("salvage open");

        // Assert
        let metrics = engine.get_runtime_metrics().expect("runtime metrics");
        assert_eq!(metrics.health, EngineHealth::SalvageMode);
        assert_eq!(metrics.salvage_mode_opens, 1);
    }

    #[test]
    fn should_fail_strict_open_when_intent_log_is_corrupt() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        initialize_format_marker(db_path);

        fs::write(db_path.join("intent_log.json"), "not-json").expect("write corrupt intent log");

        // Act
        let result = Engine::open(
            OpenOptions::local(db_path)
                .recovery_policy(RecoveryPolicy::Strict)
                .build()
                .expect("build options"),
        );

        // Assert
        match result {
            Err(MidgeError::RecoveryFailed(message)) => {
                assert!(
                    message.contains("intent"),
                    "expected intent recovery context, got: {message}"
                );
            }
            Ok(_) => panic!("expected strict recovery failure, got successful open"),
            Err(other) => panic!("expected RecoveryFailed, got: {other}"),
        }
    }

    #[test]
    fn should_fail_strict_open_when_wal_is_corrupt() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        initialize_format_marker(db_path);
        write_corrupt_wal(db_path);

        // Act
        let result = Engine::open(
            OpenOptions::local(db_path)
                .recovery_policy(RecoveryPolicy::Strict)
                .build()
                .expect("build options"),
        );

        // Assert
        match result {
            Err(MidgeError::RecoveryFailed(message)) => {
                assert!(
                    message.contains("WAL"),
                    "expected WAL recovery context, got: {message}"
                );
            }
            Ok(_) => panic!("expected strict WAL recovery failure, got successful open"),
            Err(other) => panic!("expected RecoveryFailed, got: {other}"),
        }
    }

    #[test]
    fn should_open_in_salvage_mode_when_wal_is_corrupt() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        initialize_format_marker(db_path);
        write_corrupt_wal(db_path);

        // Act
        let engine = Engine::open(
            OpenOptions::local(db_path)
                .recovery_policy(RecoveryPolicy::Salvage)
                .build()
                .expect("build options"),
        )
        .expect("salvage open");

        // Assert
        let metrics = engine.get_runtime_metrics().expect("runtime metrics");
        assert_eq!(metrics.health, EngineHealth::SalvageMode);
        assert_eq!(metrics.salvage_mode_opens, 1);
    }

    #[test]
    fn should_fail_open_given_persisted_manifest_without_format_marker() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        fs::write(db_path.join("manifest.json"), "{}\n").expect("write manifest without marker");

        // Act
        let result = Engine::open(OpenOptions::local(db_path).build().expect("build options"));

        // Assert
        match result {
            Err(MidgeError::CompatibilityError(message)) => {
                assert!(
                    message.contains("FORMAT"),
                    "expected format-marker compatibility context, got: {message}"
                );
            }
            Ok(_) => panic!("expected compatibility failure, got successful open"),
            Err(other) => panic!("expected CompatibilityError, got: {other}"),
        }
    }
}
