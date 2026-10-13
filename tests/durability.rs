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

    fn wait_for_retained_wal_members(
        engine: &Engine,
        root: &std::path::Path,
        previous: u64,
        count: usize,
    ) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let current = engine
                .metrics()
                .get_runtime_metrics()
                .unwrap()
                .wal_current_segment_id;
            if current > previous && sealed_wal_segments(root).len() >= count {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "WAL rotation did not retain {count} sealed members"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    // ============================================================================
    // WAL RECOVERY TESTS
    // ============================================================================

    /// Sealed WAL segment files: every segment but the newest, which is the
    /// open one.
    fn sealed_wal_segments(db_path: &std::path::Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(db_path.join("wal"))
            .expect("read wal dir")
            .map(|entry| entry.expect("read WAL directory entry"))
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| cntryl_midge::__internal::wal::parse_segment_file_name(name).is_some())
            .collect();
        names.sort();
        names.pop();
        names
    }

    #[test]
    fn should_remove_local_wal_segment_when_flushed_segment_contains_delete() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        {
            let mut engine =
                Engine::open(OpenOptions::local(db_path).build().expect("build options"))
                    .expect("open engine");
            let cf = engine.create_column_family("deletes").expect("create cf");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin tx");
            tx.put(b"k1".to_vec(), b"v1".to_vec(), None)
                .expect("put k1");
            tx.commit(WriteOptions::sync()).expect("commit k1");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin tx");
            tx.delete(b"k1".to_vec()).expect("delete k1");
            tx.put(b"k2".to_vec(), b"v2".to_vec(), None)
                .expect("put k2");
            tx.commit(WriteOptions::sync()).expect("commit delete");

            // Act: the first flush seals the segment holding the delete, and
            // the next flush's prune can retire it once its data is published.
            engine.flush_cf(&cf).expect("flush delete");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin tx");
            tx.put(b"k3".to_vec(), b"v3".to_vec(), None)
                .expect("put k3");
            tx.commit(WriteOptions::sync()).expect("commit k3");
            engine.flush_cf(&cf).expect("flush after delete");
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown");
        }

        // Assert
        assert_eq!(
            sealed_wal_segments(db_path),
            Vec::<String>::new(),
            "flushed segments must be retired even when they contain deletes"
        );
        let reopened = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
            .expect("reopen");
        let cf = reopened.get_column_family("deletes").expect("get cf");
        let tx = reopened
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("read tx");
        assert_eq!(tx.get(b"k1").expect("get k1"), None);
        assert_eq!(
            tx.get(b"k2").expect("get k2"),
            Some(Bytes::from_static(b"v2"))
        );
        assert_eq!(
            tx.get(b"k3").expect("get k3"),
            Some(Bytes::from_static(b"v3"))
        );
    }

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
    fn should_preserve_synced_write_when_power_loss_discards_unsynced_effects() {
        use cntryl_midge::__internal::io::Fs;
        use cntryl_midge::__internal::simulation::{CrashDevice, SimulatedClock};
        use std::sync::Arc;
        use std::time::Duration;

        // Arrange
        let named = TempDir::new().expect("named directory");
        let device = CrashDevice::new();
        let clock = SimulatedClock::new();
        let options = || {
            let root: Arc<dyn Fs> = Arc::new(device.fs());
            OpenOptions::local(named.path())
                .background_compaction(false)
                .lease_ttl(Duration::from_secs(1))
                .lease_clock_skew_tolerance(Duration::ZERO)
                .lease_clock_for_testing(Arc::new(clock.clone()))
                .filesystem_for_testing(root)
                .build()
                .expect("crash-device options")
        };
        let engine = Engine::open(options()).expect("open engine");
        let cf = engine.get_column_family("default").expect("default family");
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"critical_key".to_vec(), b"critical_value".to_vec(), None)
            .expect("stage durable write");
        tx.commit(WriteOptions::sync())
            .expect("sync acknowledgement");

        // Act
        device.process_crash();
        drop(engine);
        device.power_loss_with(|_| 0);
        clock.advance(Duration::from_secs(2));
        let mut reopened = Engine::open(options()).expect("recover after power loss");

        // Assert
        let read = reopened
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .unwrap();
        assert_eq!(
            read.get(b"critical_key")
                .expect("recover acknowledged write"),
            Some(Bytes::from_static(b"critical_value"))
        );
        drop(read);
        reopened
            .shutdown(Duration::from_secs(5))
            .expect("shutdown recovery");
    }

    // ============================================================================
    // WAL ROTATION TESTS
    // ============================================================================

    #[test]
    fn should_recover_wal_only_values_when_flush_seals_a_retained_segment() {
        // Arrange
        let directory = TempDir::new().expect("database directory");
        let options = OpenOptions::local(directory.path())
            .background_compaction(false)
            .build()
            .expect("local options");
        let mut engine = Engine::open(options.clone()).expect("open engine");
        let retained = engine
            .create_column_family("retained")
            .expect("retained family");
        let flushed = engine.get_column_family("default").expect("default family");
        for cf in [&retained, &flushed] {
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key".to_vec(), cf.name().as_bytes().to_vec(), None)
                .unwrap();
            tx.commit(WriteOptions::sync()).expect("sync commit");
        }
        let original = open_wal_segment(directory.path());
        let previous = engine
            .metrics()
            .get_runtime_metrics()
            .unwrap()
            .wal_current_segment_id;

        // Act
        engine
            .flush_cf(&flushed)
            .expect("seal shared WAL by flushing default");
        wait_for_retained_wal_members(&engine, directory.path(), previous, 1);
        let active = open_wal_segment(directory.path());
        let sealed = std::fs::read(&original).expect("idle family retains original WAL");
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown");
        let reopened = Engine::open(options).expect("recover rotated history");

        // Assert
        assert_ne!(
            active, original,
            "flush must install a successor WAL member"
        );
        assert!(cntryl_midge::__internal::wal::segment::footer_link(&sealed)
            .expect("decode sealed member")
            .is_some());
        for cf in [&retained, &flushed] {
            let read = reopened
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .unwrap();
            assert_eq!(
                read.get(b"key").unwrap().as_deref(),
                Some(cf.name().as_bytes())
            );
        }
    }

    // ============================================================================
    // WAL REPLAY TESTS
    // ============================================================================

    #[test]
    fn should_replay_all_records_given_multiple_wal_segments_when_recovering() {
        for_each_storage_mode(&["local"], |mode, opts| {
            // Arrange
            // Act (Phase 1)
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");
                let cf_id = cf.id();
                let flushed = engine.get_column_family("default").expect("default family");

                // Write in phases to create multiple WAL segments
                for batch in 0..3 {
                    let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
                    for i in 0..100 {
                        let key = format!("batch_{batch}_key_{i:03}");
                        let value = format!("batch_{batch}_value_{i:03}");
                        tx.put(key.into_bytes(), value.into_bytes(), None)
                            .expect("put");
                    }
                    tx.commit(WriteOptions::sync()).unwrap();
                    let mut marker = engine
                        .begin_tx(flushed.id(), TransactionMode::ReadWrite)
                        .unwrap();
                    marker
                        .put(vec![batch], b"flushed marker".to_vec(), None)
                        .unwrap();
                    marker.commit(WriteOptions::sync()).unwrap();
                    let previous = engine
                        .metrics()
                        .get_runtime_metrics()
                        .unwrap()
                        .wal_current_segment_id;
                    engine
                        .flush_cf(&flushed)
                        .expect("rotate while test family pins each batch");
                    let StorageMode::LocalDisk { db_path } = &opts.storage_mode else {
                        panic!("local fixture");
                    };
                    wait_for_retained_wal_members(
                        &engine,
                        db_path,
                        previous,
                        usize::from(batch) + 1,
                    );
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
                        assert_eq!(
                            tx.get(key.as_bytes()).expect("get"),
                            Some(Bytes::from(format!("batch_{batch}_value_{i:03}"))),
                            "batch {batch}, mode: {mode}"
                        );
                    }
                }
            }
        });
    }

    #[test]
    fn should_recover_all_buffered_values_when_reopening_after_concurrent_commits() {
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
                        assert_eq!(
                            tx.get(key.as_bytes()).expect("get"),
                            Some(Bytes::from(format!("thread_{thread_id}_value_{i:02}"))),
                            "thread {thread_id}, mode: {mode}"
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
        // Exercises the default Strict open path with an incomplete frame beyond the
        // last acknowledged fsync boundary. Earlier sync commits remain authoritative.
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        {
            let mut engine =
                Engine::open(OpenOptions::local(db_path).build().expect("build options"))
                    .expect("open engine");
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();

            for i in 0..4 {
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

        // Append only a partial frame header after the last persisted sync boundary.
        append_partial_frame_bytes(&open_wal_segment(db_path));

        // Act: reopen with default options (default recovery policy is Strict).
        let reopened = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
            .expect("default strict recovery should discard an incomplete unacknowledged tail");
        let cf = reopened.get_column_family("test").expect("get cf");
        let cf_id = cf.id();

        // Assert: every acknowledged commit survives; the incomplete tail adds no record.
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
            "an incomplete frame beyond the durable boundary must not be recovered"
        );
    }

    #[test]
    fn should_not_recover_data_given_truncated_wal_append_when_reopening() {
        // Arrange
        // A valid frame header and partial payload beyond the durable boundary model a
        // write interrupted before its payload was complete.
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let wal_path = first_wal_segment(db_path);

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
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before interrupted append");
        }

        append_incomplete_frame_payload(&wal_path);

        // Act
        let reopened = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
            .expect("recovery must tolerate an incomplete unacknowledged frame");
        let cf = reopened.get_column_family("test").expect("get cf");
        let cf_id = cf.id();

        // Assert: no partial frame becomes a transaction; the prior commit survives.
        let tx = reopened.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
        assert_eq!(
            tx.get(b"safe_key").expect("get"),
            Some(Bytes::from_static(b"safe_value")),
            "commit preceding the torn write must survive"
        );
    }

    // ============================================================================
    // DATA LOSS AND ERROR MODES
    // ============================================================================

    #[test]
    fn should_allow_data_loss_given_skipped_fsync_when_crash_occurs() {
        use cntryl_midge::__internal::io::Fs;
        use cntryl_midge::__internal::simulation::{CrashDevice, SimulatedClock};
        use std::sync::Arc;
        use std::time::Duration;

        // Arrange
        // Documents the fsync-disabled contract: a write that is committed without a
        // durability guarantee (buffered, no fsync) can be lost on crash. Use the crash
        // device directly so a graceful engine shutdown cannot sync the buffered write.
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let device = CrashDevice::new();
        let clock = SimulatedClock::new();
        let options = || {
            let root: Arc<dyn Fs> = Arc::new(device.fs());
            OpenOptions::local(db_path)
                .background_compaction(false)
                .lease_ttl(Duration::from_secs(1))
                .lease_clock_skew_tolerance(Duration::ZERO)
                .lease_clock_for_testing(Arc::new(clock.clone()))
                .filesystem_for_testing(root)
                .build()
                .expect("crash-device options")
        };
        let engine = Engine::open(options()).expect("open engine");
        let cf = engine.get_column_family("default").expect("default family");

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin durable write");
        tx.put(b"durable_key".to_vec(), b"durable_value".to_vec(), None)
            .expect("put durable key");
        tx.commit(WriteOptions::sync())
            .expect("sync durable commit");

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin buffered write");
        tx.put(b"transient_key".to_vec(), b"transient_value".to_vec(), None)
            .expect("put transient key");
        tx.commit(WriteOptions::buffered())
            .expect("buffered commit");

        // Simulate power loss without running the engine's graceful shutdown path.
        device.process_crash();
        drop(engine);
        device.power_loss_with(|_| 0);
        clock.advance(Duration::from_secs(2));

        // Act
        let reopened = Engine::open(options()).expect("recovery after crash must not fail");
        let cf = reopened
            .get_column_family("default")
            .expect("get default family");

        // Assert: the fsync'd write survives, the never-durable write is gone.
        let tx = reopened
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin read transaction");
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
        let wal_path = first_wal_segment(db_path);

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
    fn should_fail_strict_given_missing_acknowledged_wal_tail_when_reopening() {
        // Arrange: the final commit is acknowledged, then its bytes are removed.
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

        truncate_last_bytes(&open_wal_segment(db_path), 3);

        // Act: deleting bytes below a persisted fsync boundary is data loss, not an
        // unacknowledged torn append. Strict mode must refuse to discard that commit.
        let result = Engine::open(
            OpenOptions::local(db_path)
                .recovery_policy(RecoveryPolicy::Strict)
                .build()
                .expect("build options"),
        );

        // Assert
        assert!(
            matches!(result, Err(MidgeError::Corruption(message)) if message.contains("fsync boundary")),
            "strict recovery must reject missing acknowledged WAL bytes"
        );
    }

    #[test]
    fn should_recover_post_restart_sync_commit_when_previous_crash_left_torn_wal_tail() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let wal_log = first_wal_segment(db_path);

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
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before interrupted append");
        }
        append_partial_frame_bytes(&wal_log);

        {
            let mut engine = Engine::open(
                OpenOptions::local(db_path)
                    .recovery_policy(RecoveryPolicy::Strict)
                    .build()
                    .expect("build options"),
            )
            .expect("strict recovery should tolerate the torn tail");
            let cf = engine.get_column_family("trust").expect("get trust cf");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin after tx");
            tx.put(b"after".to_vec(), b"value".to_vec(), None)
                .expect("put after");
            tx.commit(WriteOptions::sync()).expect("sync after commit");
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown after post-restart commit");
        }

        // Act
        let reopened = Engine::open(
            OpenOptions::local(db_path)
                .recovery_policy(RecoveryPolicy::Strict)
                .build()
                .expect("build options"),
        )
        .expect("strict reopen after a post-restart commit should succeed");
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
        assert_eq!(
            tx.get(b"after").expect("get after"),
            Some(Bytes::from_static(b"value"))
        );
        // The restart sealed the torn segment after its replayed prefix
        // (#843), so the post-restart commit stays in a later segment.
        let sealed = std::fs::read(&wal_log).expect("read sealed torn segment");
        assert!(cntryl_midge::__internal::wal::segment::footer_link(&sealed)
            .expect("decode sealed torn segment")
            .is_some());
        let later_records: usize = wal_segment_files(db_path)
            .into_iter()
            .filter(|(_, path)| *path != wal_log)
            .map(|(_, path)| {
                let bytes = std::fs::read(path).expect("read later segment");
                cntryl_midge::__internal::wal::segment::records(&bytes)
                    .expect("decode later segment")
                    .len()
            })
            .sum();
        assert!(
            later_records > 0,
            "post-restart commit should remain in a later WAL segment"
        );
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

        let first_frame_payload = (cntryl_midge::__internal::wal::segment::HEADER_LEN
            + cntryl_midge::__internal::wal::frame::WAL_FRAME_HEADER_LEN)
            as u64;
        corrupt_byte(&open_wal_segment(db_path), first_frame_payload);

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

        append_partial_frame_bytes(&open_wal_segment(db_path));

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
    }

    fn append_incomplete_frame_payload(path: &std::path::Path) {
        use std::io::Write;

        let mut frame = Vec::new();
        cntryl_midge::__internal::wal::frame::append_frame(&mut frame, &[0x5a; 64])
            .expect("encode frame");
        frame.truncate(frame.len() - 20);
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(path)
            .expect("open wal for append");
        file.write_all(&frame)
            .expect("append incomplete frame payload");
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
    fn should_recover_flushed_best_effort_values_without_wal_data() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            // Act (Phase 1)
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // BestEffort records reach only the memtable and flushed SST.
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(b"key".to_vec(), b"value_v1".to_vec(), None)
                    .expect("put");
                tx.commit(cntryl_midge::WriteOptions::best_effort())
                    .expect("commit");
                engine.flush_cf(&cf).expect("flush");
                engine
                    .shutdown(std::time::Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Assert (Phase 2)
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // This value has no corresponding WAL data record to replay.
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
                    let mut tx = engine
                        .begin_tx(cf.id(), TransactionMode::ReadWrite)
                        .expect("begin_tx");
                    tx.put(
                        b"ordered".to_vec(),
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

                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin read");
                assert_eq!(
                    tx.get(b"ordered").expect("read final overwrite"),
                    Some(Bytes::from_static(b"value_099")),
                    "replay must preserve last-write order in mode {mode}"
                );
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
                    .metrics()
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
                    .metrics()
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
        // Arrange: use a real SST from a separate database as an unlisted output.
        let directory = tempfile::tempdir().expect("database directory");
        let donor = tempfile::tempdir().expect("donor database");
        let donor_file = flushed_default_sst(donor.path(), b"unlisted", b"must stay absent");
        flushed_default_sst(directory.path(), b"owned", b"acknowledged");
        let orphan = directory
            .path()
            .join("sst/000000_00_00000000000000000099.sst");
        std::fs::copy(donor_file, &orphan).expect("plant valid unlisted SST");
        assert!(orphan.exists());

        // Act
        let reopened = Engine::open(OpenOptions::local(directory.path()).build().unwrap())
            .expect("reopen with orphan");
        let cf = reopened.get_column_family("default").unwrap();
        let read = reopened
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .unwrap();

        // Assert
        assert_eq!(read.get(b"unlisted").unwrap(), None);
        assert_eq!(
            read.get(b"owned").unwrap(),
            Some(Bytes::from_static(b"acknowledged"))
        );
        assert!(!orphan.exists(), "startup must remove the unowned output");
    }

    fn flushed_default_sst(root: &std::path::Path, key: &[u8], value: &[u8]) -> std::path::PathBuf {
        let mut engine = Engine::open(
            OpenOptions::local(root)
                .background_compaction(false)
                .build()
                .unwrap(),
        )
        .expect("open fixture database");
        let cf = engine.get_column_family("default").unwrap();
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(key.to_vec(), value.to_vec(), None).unwrap();
        tx.commit(WriteOptions::best_effort()).unwrap();
        engine.flush_cf(&cf).expect("publish fixture SST");
        let layout = engine.metrics().get_storage_layout().unwrap();
        let file = layout
            .levels
            .iter()
            .flat_map(|level| &level.files)
            .next()
            .expect("published SST");
        let path = root.join("sst").join(&file.name);
        engine.shutdown(std::time::Duration::from_secs(5)).unwrap();
        path
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
    fn should_report_unavailable_authority_when_a_manifest_owned_sst_is_missing() {
        // Arrange
        let directory = tempfile::tempdir().expect("database directory");
        let owned = flushed_default_sst(directory.path(), b"key", b"flushed value");
        std::fs::remove_file(owned).expect("remove manifest-owned SST");

        // Act
        let error = match Engine::open(OpenOptions::local(directory.path()).build().unwrap()) {
            Err(error) => error,
            Ok(mut engine) => {
                let family = engine.get_column_family("default").unwrap();
                let read = engine
                    .begin_tx(family.id(), TransactionMode::ReadOnly)
                    .unwrap();
                let result = read.get(b"key");
                drop(read);
                engine.shutdown(std::time::Duration::from_secs(5)).unwrap();
                result.expect_err(
                    "missing authoritative SST must not become a successful missing-key read",
                )
            }
        };

        // Assert
        assert!(
            matches!(
                &error,
                cntryl_midge::MidgeError::NotFound
                    | cntryl_midge::MidgeError::RecoveryFailed(_)
                    | cntryl_midge::MidgeError::Corruption(_)
            ) || matches!(&error, cntryl_midge::MidgeError::Io(error) if error.kind() == std::io::ErrorKind::NotFound),
            "missing SST must report unavailable or corrupt authority: {error:?}"
        );
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
        // Arrange: an idle family's unflushed write pins every WAL segment
        // from the seed on, so retention keeps WAL history the manifest
        // already covers. Segments are linked (#843): covered records stay
        // in the retained segments that wrote them rather than being spliced
        // into the open one.
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
        let idle = engine
            .create_column_family("idle")
            .expect("create idle column family");
        let mut pin = engine
            .begin_tx(idle.id(), TransactionMode::ReadWrite)
            .expect("begin idle transaction");
        pin.put(b"pin".to_vec(), b"unflushed".to_vec(), None)
            .expect("put idle pin");
        pin.commit(WriteOptions::sync()).expect("commit idle pin");
        let seed_segment = open_wal_segment(directory.path());
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
        assert!(
            seed_segment.exists(),
            "the idle family must retain the WAL segment holding the seed"
        );

        // Act: recovery replays the retained segments, whose seed and delete
        // records the manifest already covers.
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
                        assert_eq!(
                            tx.get(key.as_bytes()).expect("get").as_deref(),
                            Some(b"value".as_slice()),
                            "mode: {mode}, key: {key}"
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
                    assert_eq!(
                        tx.get(key.as_bytes()).expect("get").as_deref(),
                        Some(format!("value_{i:03}").as_bytes()),
                        "mode: {mode}, key: {key}"
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

    #[test]
    fn should_keep_read_snapshot_consistent_when_transaction_commits_between_reads() {
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

        // Establish the old snapshot and read the first member before commit.
        let tx_read = engine
            .begin_tx(cf_id, TransactionMode::ReadOnly)
            .expect("begin_tx");

        let val1 = tx_read.get(b"key1").expect("get key1");

        // Commit both replacements between reads through the same snapshot.
        release_barrier.wait();

        // Wait for transaction to complete
        tx_handle.join().expect("tx thread");
        let val2 = tx_read.get(b"key2").expect("get key2 after commit");

        // Assert: publication cannot change an already established snapshot.
        assert_eq!(val1.as_deref(), Some(b"initial1".as_slice()));
        assert_eq!(val2.as_deref(), Some(b"initial2".as_slice()));

        // Verify final state is updated
        let tx_final = engine
            .begin_tx(cf_id, TransactionMode::ReadOnly)
            .expect("begin_tx");

        let final1 = tx_final.get(b"key1").expect("get key1");
        let final2 = tx_final.get(b"key2").expect("get key2");

        assert_eq!(final1.unwrap().as_ref(), b"updated1");
        assert_eq!(final2.unwrap().as_ref(), b"updated2");
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
    use cntryl_midge::{Engine, TransactionMode, WriteOptions};
    use std::time::Duration;

    #[test]
    fn should_skip_wal_when_using_best_effort() -> cntryl_midge::MidgeResult<()> {
        // Arrange
        let opts = opts_for_mode("local");
        let engine = Engine::open(opts.to_open_options())?;
        let cf = engine.create_column_family("test")?;
        let before = engine.metrics().get_runtime_metrics()?.wal_append_count;

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
        assert_eq!(
            engine.metrics().get_runtime_metrics()?.wal_append_count,
            before
        );

        Ok(())
    }

    #[test]
    fn should_persist_best_effort_data_when_flushed() -> cntryl_midge::MidgeResult<()> {
        // Arrange
        let opts = opts_for_mode("local");
        let mut engine = Engine::open(opts.clone().to_open_options())?;
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
        let engine = Engine::open(opts.to_open_options())?;

        // Assert - Once flush_cf() succeeds, flushed data must be durable across restart
        let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly)?;
        let val_0 = tx.get(b"key0")?;
        let marker = tx.get(b"durable_marker")?;

        assert_eq!(val_0.as_deref(), Some(b"value0".as_slice()));
        for index in 1..100 {
            assert_eq!(
                tx.get(format!("key{index}").as_bytes())?.as_deref(),
                Some(format!("value{index}").as_bytes()),
                "flushed best-effort key {index}"
            );
        }
        assert_eq!(marker.as_deref(), Some(&b"marker"[..]));

        Ok(())
    }

    #[test]
    fn should_lose_unflushed_best_effort_data_when_reopening_after_shutdown(
    ) -> cntryl_midge::MidgeResult<()> {
        // Arrange
        let opts = opts_for_mode("local");
        let mut engine = Engine::open(opts.clone().to_open_options())?;
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

        // Act: close without explicitly flushing the best-effort memtable.
        engine.shutdown(Duration::from_secs(2))?;

        // Reopen engine
        let engine = Engine::open(opts.to_open_options())?;

        // Assert - Data is lost (not in WAL, not in SST)
        let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly)?;
        let val_after = tx.get(b"ephemeral_key")?;
        assert_eq!(
            val_after, None,
            "unflushed best-effort data has no persistent record"
        );

        Ok(())
    }

    #[test]
    fn should_handle_large_batches_with_best_effort() -> cntryl_midge::MidgeResult<()> {
        // Arrange - This tests the original YCSB issue: large batches shouldn't overflow WAL queue
        let opts = opts_for_mode("local");
        let engine = Engine::open(opts.to_open_options())?;
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
        for index in 0..50_000 {
            assert_eq!(
                tx.get(format!("key{index:08}").as_bytes())?.as_deref(),
                Some(format!("value{index:08}").as_bytes()),
                "acknowledged key {index}"
            );
        }

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
    use cntryl_midge::{
        Engine, EngineHealth, MidgeError, OpenOptions, RecoveryPolicy, TransactionMode,
        WriteOptions,
    };
    use std::fs;
    use std::time::Duration;
    use tempfile::TempDir;

    fn initialize_format_marker(db_path: &std::path::Path) {
        let mut engine = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
            .expect("initialize engine");
        engine
            .shutdown(Duration::from_secs(30))
            .expect("shutdown initialized engine");
    }

    /// Leave the database with a sealed WAL segment that recovery replays:
    /// an unflushed write in a second column family holds segment 1 after a
    /// flush of the default family seals it. Returns that segment's path.
    fn initialize_with_sealed_wal_segment(db_path: &std::path::Path) -> std::path::PathBuf {
        let mut engine = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
            .expect("initialize engine");
        let retained = engine
            .create_column_family("retained")
            .expect("create retaining column family");
        let default = engine
            .get_column_family("default")
            .expect("default column family");
        for cf in [&retained, &default] {
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin write");
            tx.put(b"key".to_vec(), b"value".to_vec(), None)
                .expect("stage write");
            tx.commit(WriteOptions::sync()).expect("commit write");
        }
        engine.flush_cf(&default).expect("flush default family");
        engine
            .shutdown(Duration::from_secs(30))
            .expect("shutdown initialized engine");
        let sealed = db_path
            .join("wal")
            .join(cntryl_midge::__internal::wal::segment_file_name(1));
        assert!(
            sealed.exists()
                && db_path
                    .join("wal")
                    .join(cntryl_midge::__internal::wal::segment_file_name(2))
                    .exists(),
            "the flush must seal segment 1 and the retaining family must hold it"
        );
        sealed
    }

    fn write_corrupt_wal(db_path: &std::path::Path) {
        // A sealed segment is not allowed to hide an incomplete/corrupt prefix;
        // the open segment's tail is intentionally tolerated by strict replay.
        let sealed = initialize_with_sealed_wal_segment(db_path);
        fs::write(sealed, b"\x01\x02\x03").expect("write corrupt wal");
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
        let metrics = engine
            .metrics()
            .get_runtime_metrics()
            .expect("runtime metrics");
        assert_eq!(metrics.health, EngineHealth::SalvageMode);
        assert_eq!(metrics.salvage_mode_opens, 1);
    }

    fn sst_files(db_path: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut found = Vec::new();
        let mut pending = vec![db_path.to_path_buf()];
        while let Some(dir) = pending.pop() {
            for entry in fs::read_dir(&dir).expect("read dir") {
                let path = entry.expect("dir entry").path();
                if path.is_dir() {
                    pending.push(path);
                } else if path.extension().is_some_and(|ext| ext == "sst") {
                    found.push(path);
                }
            }
        }
        found.sort();
        found
    }

    fn sst_file_names(db_path: &std::path::Path) -> std::collections::BTreeSet<String> {
        sst_files(db_path)
            .into_iter()
            .filter_map(|path| Some(path.file_name()?.to_str()?.to_string()))
            .collect()
    }

    #[test]
    fn should_retain_sst_files_when_salvage_rejects_unreadable_manifest() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        {
            let mut engine =
                Engine::open(OpenOptions::local(db_path).build().expect("build options"))
                    .expect("open engine");
            let cf = engine.create_column_family("keep").expect("create cf");
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin tx");
            tx.put(b"key".to_vec(), b"value".to_vec(), None)
                .expect("put");
            tx.commit(cntryl_midge::WriteOptions::sync())
                .expect("sync commit");
            engine.flush_cf(&cf).expect("flush to sst");
            engine
                .shutdown(Duration::from_secs(5))
                .expect("shutdown before corruption");
        }
        let before = sst_file_names(db_path);
        assert!(!before.is_empty(), "flush should have produced an SST");
        for name in [
            "manifest.json",
            "manifest.snapshot.json",
            "manifest.journal",
        ] {
            let path = db_path.join(name);
            if path.exists() {
                fs::write(&path, b"not-a-valid-manifest").expect("corrupt manifest file");
            }
        }

        // Act
        let Err(MidgeError::Corruption(message)) = Engine::open(
            OpenOptions::local(db_path)
                .recovery_policy(RecoveryPolicy::Salvage)
                .build()
                .expect("build options"),
        ) else {
            panic!("salvage must reject a database without readable manifest authority");
        };

        // Assert
        assert!(
            message.contains("manifest snapshot"),
            "unexpected error: {message}"
        );
        assert_eq!(
            sst_file_names(db_path),
            before,
            "failed salvage must leave every SST in place when manifest authority is unreadable"
        );
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
            Err(MidgeError::Corruption(message)) => {
                assert!(
                    message.contains("WAL"),
                    "expected WAL corruption context, got: {message}"
                );
            }
            Ok(_) => panic!("expected strict WAL corruption error, got successful open"),
            Err(other) => panic!("expected Corruption, got: {other}"),
        }
    }

    #[test]
    fn should_open_in_salvage_mode_when_wal_is_corrupt() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
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
        let metrics = engine
            .metrics()
            .get_runtime_metrics()
            .expect("runtime metrics");
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
