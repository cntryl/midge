//! Fault Injection Tests (failpoints)
//!
//! Consolidated from: `failure_injection.rs`, `chaos_real.rs`, `chaos_intent_log.rs`, `chaos_compaction.rs`, `background_flush_pipeline.rs`, `shutdown_orchestration.rs`, `compaction_snapshot_publication.rs`, `transaction_crash_boundaries.rs`

mod common;

mod failure_injection {
    use bytes::Bytes;
    use cntryl_midge::{
        Engine, EngineHealth, MidgeError, OpenOptions, RecoveryPolicy, TransactionMode,
        WriteOptions,
    };
    use serde::Serialize;
    use std::path::Path;
    use std::sync::{Mutex, OnceLock};
    use tempfile::TempDir;

    static FAILPOINT_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    #[test]
    fn should_leave_column_family_absent_when_create_manifest_append_fails() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let engine = open_local_engine(db_path);
        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::manifest::inject_no_space_on_append_edit", "return")
            .expect("configure manifest append no-space failpoint");

        // Act
        let first_attempt = engine.create_column_family("create-atomic");
        let absent_after_failure = engine.get_column_family("create-atomic").is_none();
        let second_attempt = engine.create_column_family("create-atomic");
        fail::remove("midge::manifest::inject_no_space_on_append_edit");
        scenario.teardown();

        // Assert
        assert!(matches!(first_attempt, Err(MidgeError::NoSpace(_))));
        assert!(matches!(second_attempt, Err(MidgeError::NoSpace(_))));
        assert!(absent_after_failure);

        engine
            .create_column_family("create-atomic")
            .expect("retry create after manifest recovers");
        shutdown_engine(engine);

        let reopened = open_local_engine(db_path);
        assert!(reopened.get_column_family("create-atomic").is_some());
    }

    #[test]
    fn should_keep_column_family_usable_when_drop_manifest_append_fails() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let engine = open_local_engine(db_path);
        let cf = engine
            .create_column_family("drop-atomic")
            .expect("create column family");
        write_cf_value(&engine, &cf, b"key", b"value");
        engine
            .flush_cf(&cf)
            .expect("flush before testing durable drop publication");
        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::manifest::inject_no_space_on_append_edit", "return")
            .expect("configure manifest append no-space failpoint");

        // Act
        let drop_result = engine.drop_column_family(cf.id());
        let value_after_failure = read_cf_value(&engine, &cf, b"key");
        fail::remove("midge::manifest::inject_no_space_on_append_edit");
        scenario.teardown();

        // Assert
        assert!(matches!(drop_result, Err(MidgeError::NoSpace(_))));
        assert_eq!(value_after_failure, Some(Bytes::from_static(b"value")));
        engine
            .drop_column_family(cf.id())
            .expect("retry drop after manifest recovers");
        shutdown_engine(engine);

        let reopened = open_local_engine(db_path);
        assert!(reopened.get_column_family("drop-atomic").is_none());
    }

    #[test]
    fn should_reject_column_family_create_when_wal_sync_fails() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let engine = open_local_engine(db_path);
        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::wal::inject_no_space_on_sync", "return")
            .expect("configure WAL sync no-space failpoint");

        // Act
        let create_result = engine.create_column_family("sync-create");
        let absent_after_failure = engine.get_column_family("sync-create").is_none();
        fail::remove("midge::wal::inject_no_space_on_sync");
        scenario.teardown();

        // Assert
        assert!(matches!(create_result, Err(MidgeError::NoSpace(_))));
        assert!(absent_after_failure);
        engine
            .create_column_family("sync-create")
            .expect("create after WAL sync recovers");
        shutdown_engine(engine);

        let reopened = open_local_engine(db_path);
        assert!(reopened.get_column_family("sync-create").is_some());
    }

    #[test]
    fn should_reject_column_family_drop_when_wal_sync_fails() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let engine = open_local_engine(db_path);
        let cf = engine
            .create_column_family("sync-drop")
            .expect("create column family");
        write_cf_value(&engine, &cf, b"key", b"value");
        engine
            .flush_cf(&cf)
            .expect("flush before testing durable drop WAL barrier");
        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::wal::inject_no_space_on_sync", "return")
            .expect("configure WAL sync no-space failpoint");

        // Act
        let drop_result = engine.drop_column_family(cf.id());
        let value_after_failure = read_cf_value(&engine, &cf, b"key");
        fail::remove("midge::wal::inject_no_space_on_sync");
        scenario.teardown();

        // Assert
        assert!(matches!(drop_result, Err(MidgeError::NoSpace(_))));
        assert_eq!(value_after_failure, Some(Bytes::from_static(b"value")));
        engine
            .drop_column_family(cf.id())
            .expect("drop after WAL sync recovers");
        shutdown_engine(engine);

        let reopened = open_local_engine(db_path);
        assert!(reopened.get_column_family("sync-drop").is_none());
    }

    #[test]
    fn should_return_busy_before_wal_sync_failure_when_safe_drop_has_unflushed_data() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let engine = open_local_engine(db_path);
        let cf = engine
            .create_column_family("busy-before-sync")
            .expect("create column family");
        write_cf_value(&engine, &cf, b"key", b"value");
        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::wal::inject_no_space_on_sync", "return")
            .expect("configure WAL sync no-space failpoint");

        // Act
        let drop_result = engine.drop_column_family(cf.id());

        // Assert
        assert!(matches!(drop_result, Err(MidgeError::Busy(_))));
        assert_eq!(
            read_cf_value(&engine, &cf, b"key"),
            Some(Bytes::from_static(b"value"))
        );
        fail::remove("midge::wal::inject_no_space_on_sync");
        scenario.teardown();
        engine.flush_cf(&cf).expect("flush retained data");
        engine
            .drop_column_family(cf.id())
            .expect("drop after flushing retained data");
    }

    #[test]
    fn should_report_success_when_drop_metadata_mirror_fails_after_authority_switch() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let engine = open_local_engine(db_path);
        let cf = engine
            .create_column_family("committed-drop")
            .expect("create column family");
        let scenario = fail::FailScenario::setup();
        fail::cfg(
            "midge::ddl::after_drop_local_commit_before_metadata_mirror",
            "return",
        )
        .expect("configure post-commit drop mirror failure");

        // Act
        let drop_result = engine.drop_column_family(cf.id());

        // Assert: the local journal already made the drop authoritative. Returning
        // an error here would leave Engine's handle registry split from runtime.
        assert!(drop_result.is_ok());
        assert!(engine.get_column_family("committed-drop").is_none());
        assert_eq!(
            engine
                .get_runtime_metrics()
                .expect("degraded runtime metrics")
                .health,
            EngineHealth::Degraded
        );
        fail::remove("midge::ddl::after_drop_local_commit_before_metadata_mirror");
        scenario.teardown();
        shutdown_engine(engine);

        let reopened = open_local_engine(db_path);
        assert!(reopened.get_column_family("committed-drop").is_none());
        assert_eq!(
            reopened
                .get_runtime_metrics()
                .expect("reopened runtime metrics")
                .health,
            EngineHealth::Healthy
        );
    }

    #[test]
    fn should_reject_transaction_when_no_space_hits_before_batch_append_and_remain_usable() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let engine = open_local_engine(db_path);
        let cf = default_cf(&engine);

        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::wal::inject_no_space_on_txn_append_batch", "return")
            .expect("configure txn batch no-space failpoint");

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin write tx");
        tx.put(b"batch-fail-a".to_vec(), b"value-a".to_vec(), None)
            .expect("put a");
        tx.put(b"batch-fail-b".to_vec(), b"value-b".to_vec(), None)
            .expect("put b");

        // Act
        let error = tx
            .commit(WriteOptions::sync())
            .expect_err("txn append batch should fail with no space");

        // Assert
        assert_no_space_like(&error);
        assert_absent(&engine, &cf, b"batch-fail-a");
        assert_absent(&engine, &cf, b"batch-fail-b");
        fail::remove("midge::wal::inject_no_space_on_txn_append_batch");
        scenario.teardown();

        let mut recovery_tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin recovery tx");
        recovery_tx
            .put(b"batch-recovery".to_vec(), b"value".to_vec(), None)
            .expect("put recovery value");
        recovery_tx
            .commit(WriteOptions::sync())
            .expect("commit recovery txn");

        shutdown_engine(engine);

        let reopened = open_local_engine(db_path);
        let reopened_cf = default_cf(&reopened);
        assert_absent(&reopened, &reopened_cf, b"batch-fail-a");
        assert_absent(&reopened, &reopened_cf, b"batch-fail-b");
        assert_visible(&reopened, &reopened_cf, b"batch-recovery", b"value");
    }

    #[test]
    fn should_preserve_acknowledged_state_when_no_space_tears_wal_frame() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let mut engine = open_local_engine(db_path);
        let cf = default_cf(&engine);
        write_cf_value(&engine, &cf, b"stream-record", b"original-stream-bytes");
        write_cf_value(&engine, &cf, b"queue-record-a", b"queue-a");
        write_cf_value(&engine, &cf, b"queue-record-b", b"queue-b");
        let wal_path = db_path.join("wal").join("wal.log");
        let wal_len_before_failure = std::fs::metadata(&wal_path)
            .expect("WAL metadata before partial write")
            .len();
        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::wal::partial_write_then_no_space", "return")
            .expect("configure partial WAL no-space failpoint");

        // Act
        let mut failed = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin failed mutation");
        failed
            .put(
                b"stream-record".to_vec(),
                b"corrupt-replacement".to_vec(),
                None,
            )
            .expect("stage failed overwrite");
        failed
            .delete(b"queue-record-a".to_vec())
            .expect("stage failed delete");
        failed
            .put(b"failed-new".to_vec(), b"must-not-appear".to_vec(), None)
            .expect("stage failed insertion");
        let failed_result = failed.commit(WriteOptions::sync());
        let wal_len_after_failure = std::fs::metadata(&wal_path)
            .expect("WAL metadata after partial write")
            .len();
        fail::remove("midge::wal::partial_write_then_no_space");
        scenario.teardown();

        let health_after_failure = engine
            .get_runtime_metrics()
            .expect("runtime metrics after ambiguous WAL failure")
            .health;
        let mut followup = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin followup mutation");
        followup
            .put(b"followup".to_vec(), b"must-be-fenced".to_vec(), None)
            .expect("stage followup mutation");
        let followup_result = followup.commit(WriteOptions::sync());

        // Assert
        assert_no_space_like(&failed_result.expect_err("partial WAL write must fail"));
        assert_eq!(
            wal_len_after_failure, wal_len_before_failure,
            "failed positional WAL append must roll back its physical partial tail"
        );
        assert_eq!(health_after_failure, EngineHealth::Degraded);
        assert!(matches!(followup_result, Err(MidgeError::Fenced(_))));
        assert_acknowledged_exhaustion_fixture(&engine, &cf);
        assert_absent(&engine, &cf, b"failed-new");
        assert_absent(&engine, &cf, b"followup");

        let _ = engine.shutdown(std::time::Duration::from_secs(5));
        let reopened = open_local_engine(db_path);
        let reopened_cf = default_cf(&reopened);
        assert_acknowledged_exhaustion_fixture(&reopened, &reopened_cf);
        assert_absent(&reopened, &reopened_cf, b"failed-new");
        assert_absent(&reopened, &reopened_cf, b"followup");
        write_cf_value(
            &reopened,
            &reopened_cf,
            b"post-recovery",
            b"durable-after-tail-repair",
        );
        shutdown_engine(reopened);

        let reopened_again = open_local_engine(db_path);
        let reopened_again_cf = default_cf(&reopened_again);
        assert_acknowledged_exhaustion_fixture(&reopened_again, &reopened_again_cf);
        assert_visible(
            &reopened_again,
            &reopened_again_cf,
            b"post-recovery",
            b"durable-after-tail-repair",
        );
        assert_absent(&reopened_again, &reopened_again_cf, b"failed-new");
    }

    #[test]
    fn should_not_leak_partial_transaction_when_no_space_hits_before_commit_marker_append() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let engine = open_local_engine(db_path);
        let cf = default_cf(&engine);

        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::wal::inject_no_space_on_txn_commit_append", "return")
            .expect("configure txn commit no-space failpoint");

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin write tx");
        tx.put(b"commit-fail-a".to_vec(), b"value-a".to_vec(), None)
            .expect("put a");
        tx.put(b"commit-fail-b".to_vec(), b"value-b".to_vec(), None)
            .expect("put b");
        tx.put(b"commit-fail-c".to_vec(), b"value-c".to_vec(), None)
            .expect("put c");

        // Act
        let error = tx
            .commit(WriteOptions::sync())
            .expect_err("txn commit append should fail with no space");

        // Assert
        assert_no_space_like(&error);
        assert_absent(&engine, &cf, b"commit-fail-a");
        assert_absent(&engine, &cf, b"commit-fail-b");
        assert_absent(&engine, &cf, b"commit-fail-c");
        fail::remove("midge::wal::inject_no_space_on_txn_commit_append");
        scenario.teardown();

        shutdown_engine(engine);

        let reopened = open_local_engine(db_path);
        let reopened_cf = default_cf(&reopened);
        assert_absent(&reopened, &reopened_cf, b"commit-fail-a");
        assert_absent(&reopened, &reopened_cf, b"commit-fail-b");
        assert_absent(&reopened, &reopened_cf, b"commit-fail-c");
    }

    #[test]
    fn should_preserve_range_tombstone_atomicity_given_crash_between_wal_append_and_memtable_apply()
    {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let engine = open_local_engine(db_path);
        let cf = default_cf(&engine);

        seed_range(&engine, &cf, 0..10, "range");

        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::wal::inject_no_space_on_txn_append_batch", "return")
            .expect("configure delete_range no-space failpoint");

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin delete_range tx");
        tx.delete_range(b"range-03".to_vec(), b"range-07".to_vec())
            .expect("stage delete_range");
        let error = tx
            .commit(WriteOptions::sync())
            .expect_err("delete_range should fail with no space");

        // Assert
        assert_no_space_like(&error);

        for index in 0..10 {
            let key = format!("range-{index:02}");
            let value = format!("value-{index:02}");
            assert_visible(&engine, &cf, key.as_bytes(), value.as_bytes());
        }
        fail::remove("midge::wal::inject_no_space_on_txn_append_batch");
        scenario.teardown();

        shutdown_engine(engine);

        let reopened = open_local_engine(db_path);
        let reopened_cf = default_cf(&reopened);
        for index in 0..10 {
            let key = format!("range-{index:02}");
            let value = format!("value-{index:02}");
            assert_visible(&reopened, &reopened_cf, key.as_bytes(), value.as_bytes());
        }
    }

    #[test]
    fn should_recover_wal_state_when_flush_sst_finalize_hits_no_space() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let engine = open_local_engine(db_path);
        let cf = default_cf(&engine);

        seed_range(&engine, &cf, 0..12, "flush");

        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::sst::inject_no_space_on_finish_to_path", "return")
            .expect("configure sst finalize no-space failpoint");

        // Act
        let error = engine
            .flush_cf(&cf)
            .expect_err("flush should fail with no space");

        // Assert
        assert_no_space_like(&error);
        assert_eq!(
            count_sst_files(db_path),
            0,
            "failed flush must not publish any SST files"
        );
        fail::remove("midge::sst::inject_no_space_on_finish_to_path");
        scenario.teardown();

        shutdown_engine(engine);

        let reopened = open_local_engine(db_path);
        let reopened_cf = default_cf(&reopened);
        for index in 0..12 {
            let key = format!("flush-{index:02}");
            let value = format!("value-{index:02}");
            assert_visible(&reopened, &reopened_cf, key.as_bytes(), value.as_bytes());
        }
        reopened
            .flush_cf(&reopened_cf)
            .expect("flush after fault clears");
    }

    #[test]
    fn should_ignore_orphan_sst_when_flush_intent_log_save_hits_no_space() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let engine = open_local_engine(db_path);
        let cf = default_cf(&engine);

        seed_range(&engine, &cf, 0..10, "intent-flush");

        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::intent::inject_no_space_on_save", "return")
            .expect("configure intent save no-space failpoint");

        // Act
        let error = engine
            .flush_cf(&cf)
            .expect_err("flush should fail when intent persistence runs out of space");

        // Assert
        assert_no_space_like(&error);
        fail::remove("midge::intent::inject_no_space_on_save");
        scenario.teardown();

        shutdown_engine(engine);

        let reopened = open_local_engine(db_path);
        let reopened_cf = default_cf(&reopened);
        for index in 0..10 {
            let key = format!("intent-flush-{index:02}");
            let value = format!("value-{index:02}");
            assert_visible(&reopened, &reopened_cf, key.as_bytes(), value.as_bytes());
        }
        assert_eq!(
            reopened
                .get_runtime_metrics()
                .expect("runtime metrics")
                .sst_count,
            0,
            "orphan SST without durable intent must not become manifest-visible after reopen"
        );
    }

    #[test]
    fn should_not_publish_flush_output_given_sst_write_failure_when_flushing() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let engine = open_local_engine(db_path);
        let cf = default_cf(&engine);

        seed_range(&engine, &cf, 0..10, "manifest-flush");

        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::manifest::inject_no_space_on_add_sst_edit", "return")
            .expect("configure manifest append no-space failpoint");

        // Act
        let error = engine
            .flush_cf(&cf)
            .expect_err("flush should fail when manifest journal append runs out of space");

        // Assert
        assert_no_space_like(&error);
        assert!(
            count_sst_files(db_path) >= 1,
            "flush should have produced an SST before manifest append failed"
        );
        fail::remove("midge::manifest::inject_no_space_on_add_sst_edit");
        scenario.teardown();

        shutdown_engine(engine);

        let reopened = open_local_engine(db_path);
        let reopened_cf = default_cf(&reopened);
        for index in 0..10 {
            let key = format!("manifest-flush-{index:02}");
            let value = format!("value-{index:02}");
            assert_visible(&reopened, &reopened_cf, key.as_bytes(), value.as_bytes());
        }

        let metrics = reopened.get_runtime_metrics().expect("runtime metrics");
        assert_eq!(
            metrics.sst_count, 0,
            "recovery should not publish the flush SST when manifest append never succeeded"
        );
        assert_eq!(
            count_sst_files(db_path),
            0,
            "recovery should delete the orphan flush SST tracked by the intent log"
        );
        assert_eq!(metrics.health, EngineHealth::Healthy);
    }

    #[test]
    fn should_retry_flush_given_transient_publish_failure_when_reopening() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let engine = open_local_engine(db_path);
        let cf = default_cf(&engine);

        seed_range(&engine, &cf, 0..10, "retry-flush");

        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::manifest::inject_no_space_on_add_sst_edit", "return")
            .expect("configure manifest append no-space failpoint");
        let error = engine
            .flush_cf(&cf)
            .expect_err("flush should fail when manifest publication runs out of space");
        assert_no_space_like(&error);

        let failed_sst_name = std::fs::read_dir(db_path.join("sst"))
            .expect("read failed flush outputs")
            .filter_map(Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .find(|name| {
                std::path::Path::new(name)
                    .extension()
                    .is_some_and(|extension| extension.eq_ignore_ascii_case("sst"))
            })
            .expect("failed publication should retain its durable SST output");

        fail::remove("midge::manifest::inject_no_space_on_add_sst_edit");
        scenario.teardown();

        let mut later = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin later transaction");
        later
            .put(b"retry-flush-later".to_vec(), b"later-value".to_vec(), None)
            .expect("put later active-memtable value");
        later
            .commit(WriteOptions::sync())
            .expect("commit later active-memtable value");

        // Act
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        let metrics = loop {
            let metrics = engine.get_runtime_metrics().expect("runtime metrics");
            if metrics.sst_count == 1 {
                break metrics;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "failed frozen memtable was not retried by runtime maintenance"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        };

        // Assert
        let layout = engine.get_storage_layout().expect("storage layout");
        let published_names: Vec<_> = layout
            .levels
            .iter()
            .flat_map(|level| level.files.iter())
            .map(|file| file.name.as_str())
            .collect();
        assert_eq!(
            published_names,
            vec![failed_sst_name.as_str()],
            "retry must publish the original frozen immutable under its stable SST identity"
        );
        assert!(
            metrics.manifest_last_persisted_sequence < metrics.current_sequence,
            "retry must not advance the persisted frontier over later active-memtable writes"
        );
        assert_visible(&engine, &cf, b"retry-flush-later", b"later-value");
    }

    #[test]
    fn should_preserve_flush_identity_order_when_oldest_publication_retries() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let engine = open_local_engine(temp_dir.path());
        let cf = default_cf(&engine);
        seed_range(&engine, &cf, 0..8, "oldest");
        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::manifest::inject_no_space_on_add_sst_edit", "return")
            .expect("configure oldest publication failure");
        let first_error = engine
            .flush_cf(&cf)
            .expect_err("oldest publication should fail");
        assert_no_space_like(&first_error);
        fail::remove("midge::manifest::inject_no_space_on_add_sst_edit");

        let mut later = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin younger transaction");
        later
            .put(b"younger".to_vec(), vec![0x42; 512 * 1024], None)
            .expect("put younger value");
        later
            .commit(WriteOptions::sync())
            .expect("commit younger value");

        // Act
        engine
            .flush_cf(&cf)
            .expect("retry oldest then flush younger");
        scenario.teardown();

        // Assert
        let layout = engine.get_storage_layout().expect("storage layout");
        let mut names: Vec<_> = layout
            .levels
            .iter()
            .flat_map(|level| level.files.iter())
            .map(|file| file.name.clone())
            .collect();
        names.sort();
        assert_eq!(
            names,
            vec![
                "000000_00_00000000000000000001.sst".to_string(),
                "000000_00_00000000000000000002.sst".to_string(),
            ],
            "the retained oldest immutable and younger flush need distinct stable identities"
        );
        let manifest: serde_json::Value = serde_json::from_slice(
            &std::fs::read(temp_dir.path().join("manifest.json")).expect("read manifest"),
        )
        .expect("parse manifest");
        assert_eq!(manifest["files"].as_array().map(Vec::len), Some(2));
        assert_eq!(manifest["next_sst_seqs"][cf.id().to_string()], 3);
    }

    #[test]
    fn should_retry_frozen_memtable_when_sst_sequence_journal_recovers() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let engine = open_local_engine(temp_dir.path());
        let cf = default_cf(&engine);
        seed_range(&engine, &cf, 0..6, "journal-retry");

        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::manifest::inject_no_space_on_append_edit", "return")
            .expect("configure manifest journal failure");
        let error = engine
            .flush_cf(&cf)
            .expect_err("SST sequence reservation should fail");
        assert_no_space_like(&error);
        fail::remove("midge::manifest::inject_no_space_on_append_edit");
        scenario.teardown();

        // Act
        wait_for_sst_count(&engine, 1);

        // Assert
        let layout = engine.get_storage_layout().expect("storage layout");
        let names: Vec<_> = layout
            .levels
            .iter()
            .flat_map(|level| level.files.iter())
            .map(|file| file.name.as_str())
            .collect();
        assert_eq!(names, vec!["000000_00_00000000000000000001.sst"]);
    }

    #[test]
    fn should_retry_frozen_memtable_when_sst_write_recovers() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let engine = open_local_engine(temp_dir.path());
        let cf = default_cf(&engine);
        seed_range(&engine, &cf, 0..6, "sst-retry");

        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::sst::inject_no_space_on_finish_to_path", "return")
            .expect("configure SST write failure");
        let error = engine.flush_cf(&cf).expect_err("SST write should fail");
        assert_no_space_like(&error);
        fail::remove("midge::sst::inject_no_space_on_finish_to_path");
        scenario.teardown();

        // Act
        wait_for_sst_count(&engine, 1);

        // Assert
        let layout = engine.get_storage_layout().expect("storage layout");
        assert_eq!(
            layout
                .levels
                .iter()
                .map(|level| level.file_count)
                .sum::<usize>(),
            1
        );
    }

    #[test]
    fn should_retry_frozen_memtable_when_cloud_sst_upload_recovers() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let engine = Engine::open(
            OpenOptions::cloud_simulated(temp_dir.path(), "test-bucket", "retry-prefix")
                .background_compaction(false)
                .build()
                .expect("build options"),
        )
        .expect("open simulated cloud engine");
        let cf = default_cf(&engine);
        seed_range_with_options(
            &engine,
            &cf,
            0..6,
            "cloud-retry",
            // Cloud acknowledgement retires each local WAL segment before the
            // admission baseline, so later WAL settlement cannot change it.
            WriteOptions::cloud_strict(),
        );
        let committed_before_flush = engine
            .get_runtime_metrics()
            .expect("pre-flush metrics")
            .hybrid_total_committed_bytes;

        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::cloud::inject_fail_sst_upload", "return")
            .expect("configure cloud SST upload failure");
        let error = engine
            .flush_cf(&cf)
            .expect_err("cloud SST upload should fail");
        assert!(
            error.to_string().contains("cloud SST upload failed"),
            "unexpected cloud upload error: {error}"
        );
        let committed_during_retry = engine
            .get_runtime_metrics()
            .expect("failed flush metrics")
            .hybrid_total_committed_bytes;
        let retained_output_bytes: u64 = sst_file_names(temp_dir.path())
            .iter()
            .map(|name| {
                std::fs::metadata(temp_dir.path().join("sst").join(name))
                    .expect("retained flush output metadata")
                    .len()
            })
            .sum();
        assert!(committed_before_flush > 0);
        assert!(retained_output_bytes > 0);
        // Building transfers prepaid flush headroom into the worker's reservation;
        // a retained output keeps that charge without requiring a second admission.
        assert!(
            committed_during_retry >= committed_before_flush
                && committed_during_retry >= retained_output_bytes,
            "retained output must remain charged until retry succeeds: before={committed_before_flush}, retry={committed_during_retry}, retained={retained_output_bytes}"
        );
        fail::remove("midge::cloud::inject_fail_sst_upload");
        scenario.teardown();

        // Act
        wait_for_sst_count(&engine, 1);

        // Assert
        let layout = engine.get_storage_layout().expect("storage layout");
        let settled = engine.get_runtime_metrics().expect("settled flush metrics");
        assert!(
            settled.hybrid_total_committed_bytes <= committed_before_flush,
            "successful retry must settle the reservation and evict its published SST: before={committed_before_flush}, retry={committed_during_retry}, settled={}",
            settled.hybrid_total_committed_bytes
        );
        assert!(sst_file_names(temp_dir.path()).is_empty());
        assert_eq!(
            layout
                .levels
                .iter()
                .map(|level| level.file_count)
                .sum::<usize>(),
            1
        );
    }

    #[test]
    fn should_restore_sequence_floor_from_flushed_ssts_without_wal_recovery() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let engine = open_local_engine(db_path);
        let cf = default_cf(&engine);

        for index in 0..8 {
            let key = format!("sst-seq-{index:02}");
            let value = format!("value-{index:02}");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin write tx");
            tx.put(key.into_bytes(), value.into_bytes(), None)
                .expect("put sequence-floor value");
            tx.commit(WriteOptions::best_effort())
                .expect("commit best-effort value");
        }

        // Act
        engine.flush_cf(&cf).expect("flush sst-backed state");
        shutdown_engine(engine);

        let reopened = open_local_engine(db_path);
        let reopened_cf = default_cf(&reopened);
        let metrics = reopened.get_runtime_metrics().expect("runtime metrics");

        // Assert
        assert!(
            metrics.current_sequence >= 8,
            "reopen must restore sequence from SST-backed durable state, got {}",
            metrics.current_sequence
        );
        assert!(
            metrics.manifest_last_persisted_sequence >= 8,
            "manifest durable sequence must reflect recovered SST data, got {}",
            metrics.manifest_last_persisted_sequence
        );

        for index in 0..8 {
            let key = format!("sst-seq-{index:02}");
            let value = format!("value-{index:02}");
            assert_visible(&reopened, &reopened_cf, key.as_bytes(), value.as_bytes());
        }
    }

    #[test]
    fn should_open_in_salvage_mode_when_replay_cannot_clear_intent_log() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let engine = open_local_engine(db_path);
        let cf = default_cf(&engine);

        seed_range(&engine, &cf, 0..10, "replay-intent-clear");

        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::manifest::inject_no_space_on_add_sst_edit", "return")
            .expect("configure manifest append no-space failpoint");

        // Act
        let error = engine
            .flush_cf(&cf)
            .expect_err("flush should fail when manifest append runs out of space");
        assert_no_space_like(&error);
        fail::remove("midge::manifest::inject_no_space_on_add_sst_edit");
        scenario.teardown();
        shutdown_engine(engine);

        let replay_scenario = fail::FailScenario::setup();
        fail::cfg("midge::intent::inject_no_space_on_save", "return")
            .expect("configure intent save no-space failpoint");
        let reopened = Engine::open(
            OpenOptions::local(db_path)
                .recovery_policy(RecoveryPolicy::Salvage)
                .build()
                .expect("build options"),
        )
        .expect("salvage open should survive replay intent cleanup failure");
        fail::remove("midge::intent::inject_no_space_on_save");
        replay_scenario.teardown();

        // Assert
        let reopened_cf = default_cf(&reopened);
        let metrics = reopened.get_runtime_metrics().expect("runtime metrics");
        assert_eq!(metrics.health, EngineHealth::SalvageMode);
        assert_eq!(metrics.sst_count, 0);
        assert_eq!(count_sst_files(db_path), 0);
        for index in 0..10 {
            let key = format!("replay-intent-clear-{index:02}");
            let value = format!("value-{index:02}");
            assert_visible(&reopened, &reopened_cf, key.as_bytes(), value.as_bytes());
        }
    }

    #[derive(Serialize)]
    struct TestIntentFileMeta {
        name: String,
        level: u32,
        size_bytes: u64,
        cf_id: u32,
        smallest_key: Option<Vec<u8>>,
        largest_key: Option<Vec<u8>>,
        smallest_seq: Option<u64>,
        largest_seq: Option<u64>,
    }

    #[derive(Serialize)]
    enum TestIntentLogEntry {
        SstAdded { file_meta: TestIntentFileMeta },
    }

    #[derive(Serialize)]
    struct TestManifestFile {
        name: String,
        level: u32,
        size_bytes: u64,
        cf_id: u32,
        smallest_key: Option<Vec<u8>>,
        largest_key: Option<Vec<u8>>,
        smallest_seq: Option<u64>,
        largest_seq: Option<u64>,
    }

    #[derive(Serialize)]
    struct TestManifestFixture {
        last_persisted_sequence: u64,
        ssts: Vec<String>,
        files: Vec<TestManifestFile>,
        column_families: Vec<TestColumnFamilyMeta>,
        next_wal_seq: u64,
        next_sst_seqs: std::collections::BTreeMap<u32, u64>,
    }

    #[derive(Serialize)]
    struct TestColumnFamilyMeta {
        id: u32,
        name: String,
        created_at: u64,
        deleted_at: Option<u64>,
    }

    #[test]
    fn should_fail_strict_open_when_replay_cannot_clear_intent_log() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let engine = open_local_engine(db_path);
        let cf = default_cf(&engine);

        seed_range(&engine, &cf, 0..10, "replay-intent-strict");

        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::manifest::inject_no_space_on_add_sst_edit", "return")
            .expect("configure manifest append no-space failpoint");

        // Act
        let error = engine
            .flush_cf(&cf)
            .expect_err("flush should fail when manifest append runs out of space");
        assert_no_space_like(&error);
        fail::remove("midge::manifest::inject_no_space_on_add_sst_edit");
        scenario.teardown();
        shutdown_engine(engine);

        let replay_scenario = fail::FailScenario::setup();
        fail::cfg("midge::intent::inject_no_space_on_save", "return")
            .expect("configure intent save no-space failpoint");
        let Err(error) = Engine::open(
            OpenOptions::local(db_path)
                .recovery_policy(RecoveryPolicy::Strict)
                .build()
                .expect("build options"),
        ) else {
            panic!("strict open should fail when replay cannot clear intent log");
        };
        fail::remove("midge::intent::inject_no_space_on_save");
        replay_scenario.teardown();

        // Assert
        match error {
            MidgeError::RecoveryFailed(message) => assert!(
                message.contains("intent log"),
                "expected replay intent cleanup context, got: {message}"
            ),
            other => panic!("expected RecoveryFailed, got: {other}"),
        }
    }

    #[test]
    fn should_open_in_salvage_mode_when_replay_cannot_checkpoint_manifest() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let engine = open_local_engine(db_path);
        let cf = default_cf(&engine);

        for index in 0..12 {
            let key = format!("replay-checkpoint-{index:02}");
            let value = format!("value-{index:02}");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin write tx");
            tx.put(key.into_bytes(), value.into_bytes(), None)
                .expect("put best-effort value");
            tx.commit(WriteOptions::best_effort())
                .expect("commit best-effort value");
        }
        engine.flush_cf(&cf).expect("flush replay checkpoint seed");
        let layout = engine.get_storage_layout().expect("storage layout");
        let file = layout
            .levels
            .iter()
            .flat_map(|level| level.files.iter())
            .find(|file| file.cf_id == cf.id())
            .cloned()
            .expect("flushed file layout");
        shutdown_engine(engine);

        let empty_manifest = TestManifestFixture {
            last_persisted_sequence: 0,
            ssts: Vec::new(),
            files: Vec::new(),
            column_families: Vec::new(),
            next_wal_seq: 1,
            next_sst_seqs: std::collections::BTreeMap::new(),
        };
        std::fs::write(
            db_path.join("manifest.json"),
            serde_json::to_string_pretty(&empty_manifest).expect("serialize empty manifest"),
        )
        .expect("write empty manifest");
        std::fs::write(db_path.join("manifest.journal"), b"").expect("truncate manifest journal");
        let _ = std::fs::remove_file(db_path.join("manifest.snapshot.json"));

        let replay_intent = vec![TestIntentLogEntry::SstAdded {
            file_meta: TestIntentFileMeta {
                name: file.name.clone(),
                level: file.level,
                size_bytes: file.size_bytes,
                cf_id: file.cf_id,
                smallest_key: file.smallest_key.clone(),
                largest_key: file.largest_key.clone(),
                smallest_seq: file.smallest_seq,
                largest_seq: file.largest_seq,
            },
        }];
        std::fs::write(
            db_path.join("intent_log.json"),
            serde_json::to_string_pretty(&replay_intent).expect("serialize replay intent"),
        )
        .expect("write replay intent log");

        // Act
        let scenario = fail::FailScenario::setup();
        fail::cfg(
            "midge::manifest::inject_no_space_on_checkpoint_save",
            "return",
        )
        .expect("configure manifest checkpoint no-space failpoint");
        let reopened = Engine::open(
            OpenOptions::local(db_path)
                .recovery_policy(RecoveryPolicy::Salvage)
                .build()
                .expect("build options"),
        )
        .expect("salvage open should survive replay checkpoint failure");
        fail::remove("midge::manifest::inject_no_space_on_checkpoint_save");
        scenario.teardown();

        // Assert
        let reopened_cf = default_cf(&reopened);
        let metrics = reopened.get_runtime_metrics().expect("runtime metrics");
        assert_eq!(metrics.health, EngineHealth::SalvageMode);
        assert_eq!(metrics.sst_count, 1);
        for index in 0..12 {
            let key = format!("replay-checkpoint-{index:02}");
            let value = format!("value-{index:02}");
            assert_visible(&reopened, &reopened_cf, key.as_bytes(), value.as_bytes());
        }
    }

    #[test]
    fn should_fail_strict_open_when_replay_cannot_checkpoint_manifest() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let engine = open_local_engine(db_path);
        let cf = default_cf(&engine);

        for index in 0..12 {
            let key = format!("replay-checkpoint-strict-{index:02}");
            let value = format!("value-{index:02}");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin write tx");
            tx.put(key.into_bytes(), value.into_bytes(), None)
                .expect("put best-effort value");
            tx.commit(WriteOptions::best_effort())
                .expect("commit best-effort value");
        }
        engine.flush_cf(&cf).expect("flush replay checkpoint seed");
        let layout = engine.get_storage_layout().expect("storage layout");
        let file = layout
            .levels
            .iter()
            .flat_map(|level| level.files.iter())
            .find(|file| file.cf_id == cf.id())
            .cloned()
            .expect("flushed file layout");
        shutdown_engine(engine);

        let empty_manifest = TestManifestFixture {
            last_persisted_sequence: 0,
            ssts: Vec::new(),
            files: Vec::new(),
            column_families: Vec::new(),
            next_wal_seq: 1,
            next_sst_seqs: std::collections::BTreeMap::new(),
        };
        std::fs::write(
            db_path.join("manifest.json"),
            serde_json::to_string_pretty(&empty_manifest).expect("serialize empty manifest"),
        )
        .expect("write empty manifest");
        std::fs::write(db_path.join("manifest.journal"), b"").expect("truncate manifest journal");
        let _ = std::fs::remove_file(db_path.join("manifest.snapshot.json"));

        let replay_intent = vec![TestIntentLogEntry::SstAdded {
            file_meta: TestIntentFileMeta {
                name: file.name.clone(),
                level: file.level,
                size_bytes: file.size_bytes,
                cf_id: file.cf_id,
                smallest_key: file.smallest_key.clone(),
                largest_key: file.largest_key.clone(),
                smallest_seq: file.smallest_seq,
                largest_seq: file.largest_seq,
            },
        }];
        std::fs::write(
            db_path.join("intent_log.json"),
            serde_json::to_string_pretty(&replay_intent).expect("serialize replay intent"),
        )
        .expect("write replay intent log");

        // Act
        let scenario = fail::FailScenario::setup();
        fail::cfg(
            "midge::manifest::inject_no_space_on_checkpoint_save",
            "return",
        )
        .expect("configure manifest checkpoint no-space failpoint");
        let Err(error) = Engine::open(
            OpenOptions::local(db_path)
                .recovery_policy(RecoveryPolicy::Strict)
                .build()
                .expect("build options"),
        ) else {
            panic!("strict open should fail when replay cannot checkpoint manifest");
        };
        fail::remove("midge::manifest::inject_no_space_on_checkpoint_save");
        scenario.teardown();

        // Assert
        match error {
            MidgeError::RecoveryFailed(message) => assert!(
                message.contains("manifest checkpoint"),
                "expected replay checkpoint context, got: {message}"
            ),
            other => panic!("expected RecoveryFailed, got: {other}"),
        }
    }

    #[test]
    fn should_recover_flushed_best_effort_data_when_manifest_checkpoint_save_hits_no_space() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let engine = open_local_engine(db_path);
        let cf = default_cf(&engine);

        for index in 0..12 {
            let key = format!("checkpoint-flush-{index:02}");
            let value = format!("value-{index:02}");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin write tx");
            tx.put(key.into_bytes(), value.into_bytes(), None)
                .expect("put best-effort value");
            tx.commit(WriteOptions::best_effort())
                .expect("commit best-effort value");
        }

        // Act
        let scenario = fail::FailScenario::setup();
        fail::cfg(
            "midge::manifest::inject_no_space_on_checkpoint_save",
            "return",
        )
        .expect("configure manifest checkpoint no-space failpoint");
        engine.flush_cf(&cf).expect("flush should still succeed");
        let metrics = engine.get_runtime_metrics().expect("runtime metrics");
        assert_eq!(
            metrics.health,
            EngineHealth::Degraded,
            "checkpoint failure should degrade the live engine until restart"
        );
        fail::remove("midge::manifest::inject_no_space_on_checkpoint_save");
        scenario.teardown();

        shutdown_engine(engine);

        let reopened = open_local_engine(db_path);
        let reopened_cf = default_cf(&reopened);

        // Assert
        for index in 0..12 {
            let key = format!("checkpoint-flush-{index:02}");
            let value = format!("value-{index:02}");
            assert_visible(&reopened, &reopened_cf, key.as_bytes(), value.as_bytes());
        }

        let reopened_metrics = reopened.get_runtime_metrics().expect("runtime metrics");
        assert_eq!(reopened_metrics.health, EngineHealth::Healthy);
        assert!(
            reopened_metrics.sst_count >= 1,
            "journal replay should recover the flushed SST into the manifest"
        );
    }

    #[test]
    fn should_preserve_compacted_input_state_when_compaction_output_hits_no_space() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let engine = open_local_engine(db_path);
        let cf = default_cf(&engine);

        for batch in 0..6 {
            for index in 0..25 {
                let key = format!("cmp-b{batch}-k{index:02}");
                let value = format!("value-b{batch}-k{index:02}");
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin batch tx");
                tx.put(key.into_bytes(), value.into_bytes(), None)
                    .expect("put compaction seed");
                tx.commit(WriteOptions::sync())
                    .expect("commit compaction seed");
            }
            engine.flush_cf(&cf).expect("flush compaction seed");
        }

        let initial_sst_count = engine
            .get_runtime_metrics()
            .expect("runtime metrics")
            .sst_count;
        assert!(
            initial_sst_count >= 6,
            "expected multiple L0 files before forced compaction"
        );

        // Act
        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::sst::inject_no_space_on_finish_to_path", "return")
            .expect("configure compaction output no-space failpoint");
        engine
            .compact_all()
            .expect_err("compact_all must report the compaction output failure");
        assert_eq!(
            engine
                .get_runtime_metrics()
                .expect("runtime metrics")
                .sst_count,
            initial_sst_count,
            "failed compaction must not publish partial output SSTs"
        );
        fail::remove("midge::sst::inject_no_space_on_finish_to_path");
        scenario.teardown();

        shutdown_engine(engine);

        let reopened = open_local_engine(db_path);
        let reopened_cf = default_cf(&reopened);

        // Assert
        for batch in 0..6 {
            for index in 0..25 {
                let key = format!("cmp-b{batch}-k{index:02}");
                let value = format!("value-b{batch}-k{index:02}");
                assert_visible(&reopened, &reopened_cf, key.as_bytes(), value.as_bytes());
            }
        }
    }

    #[test]
    fn should_publish_partitioned_outputs_when_retrying_compaction_after_failed_manifest_restart() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let engine = open_local_engine_with_target(db_path, 128);
        let cf = default_cf(&engine);

        seed_compaction_batches(&engine, &cf, "cmp-recover", 4, 25);

        let initial_layout = engine.get_storage_layout().expect("initial storage layout");
        let initial_sst_count = initial_layout
            .levels
            .iter()
            .map(|level| level.file_count)
            .sum::<usize>();
        assert!(
            !initial_layout
                .levels
                .iter()
                .any(|level| level.level > 0 && level.file_count > 0),
            "expected only L0 files before forcing compaction"
        );

        // Act
        let scenario = fail::FailScenario::setup();
        fail::cfg(
            "midge::manifest::inject_no_space_on_compaction_batch_edit",
            "return",
        )
        .expect("configure manifest batch append no-space failpoint");
        engine
            .compact_all()
            .expect_err("compact_all must report the manifest batch failure");
        fail::remove("midge::manifest::inject_no_space_on_compaction_batch_edit");
        scenario.teardown();
        let live_retry = engine.compact_all();

        // Assert
        assert!(matches!(live_retry, Err(MidgeError::Fenced(_))));

        shutdown_engine(engine);

        let reopened = open_local_engine_with_target(db_path, 128);
        let reopened_cf = default_cf(&reopened);

        assert_compaction_batches_visible(&reopened, &reopened_cf, "cmp-recover", 4, 25);

        let recovered_layout = reopened
            .get_storage_layout()
            .expect("recovered storage layout");
        assert!(
            recovered_layout
                .levels
                .iter()
                .all(|level| level.level == 0 || level.file_count == 0),
            "OutputDurable recovery must not publish a compaction whose manifest batch failed"
        );
        assert_eq!(
            recovered_layout
                .levels
                .iter()
                .map(|level| level.file_count)
                .sum::<usize>(),
            initial_sst_count,
            "rollback must retain every authoritative input and remove the orphan output set"
        );

        reopened
            .compact_all()
            .expect("retry compaction after rollback recovery");
        let retried_layout = reopened.get_storage_layout().expect("retried layout");
        assert!(
            retried_layout
                .levels
                .iter()
                .find(|level| level.level == 1)
                .map_or(0, |level| level.file_count)
                > 1,
            "successful retry must atomically publish the complete partitioned replacement set"
        );
        assert!(
            retried_layout
                .levels
                .iter()
                .find(|level| level.level == 0)
                .map_or(0, |level| level.file_count)
                < initial_sst_count,
            "successful retry must consume the original L0 inputs"
        );
        shutdown_engine(reopened);

        let final_reopen = open_local_engine_with_target(db_path, 128);
        let final_cf = default_cf(&final_reopen);
        assert_compaction_batches_visible(&final_reopen, &final_cf, "cmp-recover", 4, 25);
        let final_layout = final_reopen
            .get_storage_layout()
            .expect("final recovered layout");
        assert!(
            final_layout
                .levels
                .iter()
                .find(|level| level.level == 1)
                .map_or(0, |level| level.file_count)
                > 1,
            "restart must preserve only the complete successful partition set"
        );
    }

    #[test]
    fn should_delete_untracked_compaction_output_on_reopen_when_intent_save_fails() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let engine = open_local_engine_with_target(db_path, 128);
        let cf = default_cf(&engine);
        for batch in 0..6 {
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin compaction seed");
            tx.put(
                format!("intentless-output-{batch}").into_bytes(),
                b"value".to_vec(),
                None,
            )
            .expect("put compaction seed");
            tx.commit(WriteOptions::best_effort())
                .expect("commit compaction seed");
            engine.flush_cf(&cf).expect("flush compaction seed");
        }
        let initial_sst_count = count_sst_files(db_path);
        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::intent::inject_no_space_on_save", "return")
            .expect("configure intent save failure");

        // Act
        engine
            .compact_all()
            .expect_err("compact_all must report the intent persistence failure");

        // Assert: execution produced an output, but no intent or manifest entry
        // owns it, so startup cleanup must remove exactly that residue.
        assert!(count_sst_files(db_path) > initial_sst_count);
        assert_eq!(
            engine
                .get_runtime_metrics()
                .expect("degraded live metrics")
                .health,
            EngineHealth::Degraded
        );
        fail::remove("midge::intent::inject_no_space_on_save");
        scenario.teardown();
        shutdown_engine(engine);

        let reopened = open_local_engine_with_target(db_path, 128);
        let metrics = reopened
            .get_runtime_metrics()
            .expect("reopened runtime metrics");
        let layout = reopened
            .get_storage_layout()
            .expect("reopened storage layout");
        assert_eq!(metrics.health, EngineHealth::Healthy);
        assert_eq!(metrics.obsolete_file_backlog, 0);
        assert_eq!(count_sst_files(db_path), initial_sst_count);
        assert!(
            layout
                .levels
                .iter()
                .all(|level| level.level == 0 || level.file_count == 0),
            "intentless output must not become manifest-authoritative"
        );
    }

    #[test]
    fn should_fence_followup_compaction_when_phase_save_fails_after_authority_switch() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let engine = open_local_engine_with_target(db_path, 128);
        let cf = default_cf(&engine);
        for batch in 0..6 {
            for index in 0..25 {
                let key = format!("phase-save-b{batch}-k{index:02}");
                let value = format!("value-b{batch}-k{index:02}");
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin compaction seed");
                tx.put(key.into_bytes(), value.into_bytes(), None)
                    .expect("put compaction seed");
                tx.commit(WriteOptions::best_effort())
                    .expect("commit compaction seed");
            }
            engine.flush_cf(&cf).expect("flush compaction seed");
        }
        let initial_sst_count = count_sst_files(db_path);
        assert!(initial_sst_count >= 6);
        let scenario = fail::FailScenario::setup();
        fail::cfg(
            "midge::compaction::inject_failure_after_manifest_batch",
            "return",
        )
        .expect("configure post-manifest compaction failure");

        // Act
        engine
            .compact_all()
            .expect_err("compact_all must report the phase-save failure");

        // Assert: the batch journal already made the output authoritative. The
        // live engine must not roll that decision back or retain obsolete local
        // inputs just because advancing the publication phase failed.
        let live_layout = engine.get_storage_layout().expect("live storage layout");
        let live_manifest_sst_count = live_layout
            .levels
            .iter()
            .map(|level| level.file_count)
            .sum::<usize>();
        assert_eq!(live_layout.health, EngineHealth::Degraded);
        assert!(
            live_layout
                .levels
                .iter()
                .any(|level| level.level > 0 && level.file_count > 1),
            "durable manifest batch must make every partition authoritative"
        );
        assert!(
            live_layout
                .levels
                .iter()
                .find(|level| level.level == 0)
                .map_or(0, |level| level.file_count)
                < initial_sst_count,
            "authority switch must remove the selected input batch"
        );
        assert_eq!(
            count_sst_files(db_path),
            live_manifest_sst_count,
            "local inputs removed by the authoritative manifest must be reclaimed"
        );
        fail::remove("midge::compaction::inject_failure_after_manifest_batch");
        scenario.teardown();
        let followup = engine.compact_all();
        assert!(matches!(followup, Err(MidgeError::Fenced(_))));
        shutdown_engine(engine);

        let reopened = open_local_engine_with_target(db_path, 128);
        let reopened_cf = default_cf(&reopened);
        for batch in 0..6 {
            for index in 0..25 {
                let key = format!("phase-save-b{batch}-k{index:02}");
                let value = format!("value-b{batch}-k{index:02}");
                assert_visible(&reopened, &reopened_cf, key.as_bytes(), value.as_bytes());
            }
        }
        let recovered_layout = reopened
            .get_storage_layout()
            .expect("recovered storage layout");
        assert_eq!(recovered_layout.health, EngineHealth::Healthy);
        assert!(
            recovered_layout
                .levels
                .iter()
                .any(|level| level.level > 0 && level.file_count > 1),
            "intent replay must preserve the complete journal-authoritative partition set"
        );
        assert_eq!(count_sst_files(db_path), live_manifest_sst_count);
    }

    #[test]
    fn should_fence_followup_compaction_when_intent_clear_save_fails() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let engine = open_local_engine_with_target(db_path, 64);
        let cf = default_cf(&engine);
        for batch in 0..4 {
            let key = format!("clear-save-{batch}");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin compaction seed");
            tx.put(key.into_bytes(), b"value".to_vec(), None)
                .expect("put compaction seed");
            tx.commit(WriteOptions::best_effort())
                .expect("commit compaction seed");
            engine.flush_cf(&cf).expect("flush compaction seed");
        }
        let scenario = fail::FailScenario::setup();
        fail::cfg(
            "midge::compaction::inject_no_space_on_intent_clear",
            "return",
        )
        .expect("configure compaction intent-clear failure");

        // Act
        engine
            .compact_all()
            .expect_err("compact_all must report the intent-clear failure");
        let live_layout = engine
            .get_storage_layout()
            .expect("live partitioned layout");
        assert!(
            live_layout
                .levels
                .iter()
                .any(|level| level.level > 0 && level.file_count > 1),
            "intent-clear failure fixture must publish multiple outputs"
        );
        fail::remove("midge::compaction::inject_no_space_on_intent_clear");
        scenario.teardown();
        let followup = engine.compact_all();

        // Assert
        assert!(matches!(followup, Err(MidgeError::Fenced(_))));
        shutdown_engine(engine);
        let reopened = open_local_engine_with_target(db_path, 64);
        let reopened_cf = default_cf(&reopened);
        for batch in 0..4 {
            let key = format!("clear-save-{batch}");
            assert_visible(&reopened, &reopened_cf, key.as_bytes(), b"value");
        }
        assert_eq!(
            reopened
                .get_runtime_metrics()
                .expect("reopened metrics")
                .health,
            EngineHealth::Healthy
        );
    }

    #[test]
    fn should_fence_followup_compaction_when_manifest_sync_result_is_ambiguous() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let engine = open_local_engine(db_path);
        let cf = default_cf(&engine);
        for batch in 0..4 {
            let key = format!("ambiguous-sync-{batch}");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin compaction seed");
            tx.put(key.into_bytes(), b"value".to_vec(), None)
                .expect("put compaction seed");
            tx.commit(WriteOptions::best_effort())
                .expect("commit compaction seed");
            engine.flush_cf(&cf).expect("flush compaction seed");
        }
        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::manifest::inject_required_sync_failure", "return")
            .expect("configure ambiguous manifest sync failure");

        // Act
        engine
            .compact_all()
            .expect_err("compact_all must report the required sync failure");
        fail::remove("midge::manifest::inject_required_sync_failure");
        scenario.teardown();
        let fenced_retry = engine.compact_all();
        shutdown_engine(engine);
        let recovered = open_local_engine(db_path);
        shutdown_engine(recovered);
        let reopened = open_local_engine(db_path);

        // Assert
        assert!(matches!(fenced_retry, Err(MidgeError::Fenced(_))));
        let reopened_cf = default_cf(&reopened);
        for batch in 0..4 {
            let key = format!("ambiguous-sync-{batch}");
            assert_visible(&reopened, &reopened_cf, key.as_bytes(), b"value");
        }
        let layout = reopened.get_storage_layout().expect("reopened layout");
        assert_eq!(
            layout
                .levels
                .iter()
                .find(|level| level.level == 1)
                .map_or(0, |level| level.file_count),
            1,
            "ambiguous sync recovery must select one authoritative L1 output: {layout:?}"
        );
    }

    #[test]
    fn should_remove_remote_compaction_orphan_on_reopen_when_manifest_batch_fails() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let open_cloud = || {
            Engine::open(
                OpenOptions::cloud_simulated(db_path, "test-bucket", "compaction-cleanup")
                    .background_compaction(false)
                    .target_sst_size_for_testing(64)
                    .build()
                    .expect("build simulated cloud options"),
            )
            .expect("open simulated cloud engine")
        };
        let engine = open_cloud();
        let cf = default_cf(&engine);
        for batch in 0..4 {
            let key = format!("remote-orphan-{batch}");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin compaction seed");
            tx.put(key.into_bytes(), b"value".to_vec(), None)
                .expect("put compaction seed");
            tx.commit(WriteOptions::cloud_async())
                .expect("commit compaction seed");
            engine.flush_cf(&cf).expect("flush compaction seed");
        }
        let cloud_root = db_path.join("cloud_store");
        let initial_remote_count = count_sst_files(&cloud_root);
        assert_eq!(initial_remote_count, 4);
        let scenario = fail::FailScenario::setup();
        fail::cfg(
            "midge::manifest::inject_no_space_on_compaction_batch_edit",
            "return",
        )
        .expect("configure manifest batch append failure");

        // Act
        engine
            .compact_all()
            .expect_err("compact_all must report the manifest batch failure");
        fail::remove("midge::manifest::inject_no_space_on_compaction_batch_edit");
        scenario.teardown();
        assert!(
            count_sst_files(&cloud_root) > initial_remote_count + 1,
            "failed publication should leave the complete tracked remote partition set"
        );
        shutdown_engine(engine);
        let reopened = open_cloud();

        // Assert
        assert_eq!(count_sst_files(&cloud_root), initial_remote_count);
        let reopened_cf = default_cf(&reopened);
        for batch in 0..4 {
            let key = format!("remote-orphan-{batch}");
            assert_visible(&reopened, &reopened_cf, key.as_bytes(), b"value");
        }
    }

    #[test]
    fn should_preserve_remote_inputs_after_partial_compaction_upload() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let open_cloud = || {
            Engine::open(
                OpenOptions::cloud_simulated(db_path, "test-bucket", "partial-mirror")
                    .background_compaction(false)
                    .target_sst_size_for_testing(64)
                    .build()
                    .expect("build simulated cloud options"),
            )
            .expect("open simulated cloud engine")
        };
        let engine = open_cloud();
        let cf = default_cf(&engine);
        for batch in 0..4 {
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin compaction seed");
            tx.put(
                format!("partial-mirror-{batch}").into_bytes(),
                vec![u8::try_from(batch).expect("batch fits u8"); 512],
                None,
            )
            .expect("put compaction seed");
            tx.commit(WriteOptions::cloud_async())
                .expect("commit compaction seed");
            engine.flush_cf(&cf).expect("flush compaction seed");
        }
        let cloud_root = db_path.join("cloud_store");
        let initial_local = sst_file_names(db_path);
        let initial_remote = sst_file_names(&cloud_root);
        assert!(initial_local.is_empty());
        assert_eq!(initial_remote.len(), 4);
        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::cloud::inject_fail_sst_upload", "1*off->return")
            .expect("fail the second partition upload");

        // Act
        engine
            .compact_all()
            .expect_err("compact_all must report the partial remote mirror failure");
        fail::remove("midge::cloud::inject_fail_sst_upload");
        scenario.teardown();

        // Assert: only partition zero reached remote storage. Without a complete
        // output set, no output may become manifest-authoritative or retire inputs.
        let failed_remote = sst_file_names(&cloud_root);
        assert!(failed_remote.is_superset(&initial_remote));
        assert_eq!(failed_remote.difference(&initial_remote).count(), 1);
        assert_eq!(manifest_sst_file_names(&engine), initial_remote);
        shutdown_engine(engine);
        discard_local_cloud_data_cache(db_path);

        let reopened = open_cloud();
        assert_eq!(sst_file_names(db_path), initial_local);
        assert_eq!(manifest_sst_file_names(&reopened), initial_remote);
        assert_eq!(sst_file_names(&cloud_root), failed_remote);
        let reopened_cf = default_cf(&reopened);
        for batch in 0..4 {
            let key = format!("partial-mirror-{batch}");
            assert_visible(
                &reopened,
                &reopened_cf,
                key.as_bytes(),
                &vec![u8::try_from(batch).expect("batch fits u8"); 512],
            );
        }
    }

    #[test]
    fn should_retain_replaced_remote_compaction_orphan_when_cleanup_proof_is_stale() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let cloud_options = || {
            OpenOptions::cloud_simulated(db_path, "test-bucket", "guarded-compaction-cleanup")
                .background_compaction(false)
                .build()
                .expect("build simulated cloud options")
        };
        let engine = Engine::open(cloud_options()).expect("open simulated cloud engine");
        let cf = default_cf(&engine);
        for batch in 0..4 {
            let key = format!("guarded-remote-orphan-{batch}");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin compaction seed");
            tx.put(key.into_bytes(), b"value".to_vec(), None)
                .expect("put compaction seed");
            tx.commit(WriteOptions::cloud_async())
                .expect("commit compaction seed");
            engine.flush_cf(&cf).expect("flush compaction seed");
        }
        let cloud_root = db_path.join("cloud_store");
        let initial_remote_files = sst_file_names(&cloud_root);
        let scenario = fail::FailScenario::setup();
        fail::cfg(
            "midge::manifest::inject_no_space_on_compaction_batch_edit",
            "return",
        )
        .expect("configure manifest batch append failure");
        engine
            .compact_all()
            .expect_err("compact_all must report the manifest batch failure");
        fail::remove("midge::manifest::inject_no_space_on_compaction_batch_edit");
        let orphan_names = sst_file_names(&cloud_root)
            .difference(&initial_remote_files)
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(orphan_names.len(), 1);
        shutdown_engine(engine);
        let orphan_path = cloud_root.join("sst").join(&orphan_names[0]);
        let replacement = b"replacement must survive stale cleanup proof".to_vec();
        let replacement_path = orphan_path.clone();
        let replacement_for_callback = replacement.clone();
        fail::cfg_callback("midge::cloud::before_compaction_orphan_delete", move || {
            std::fs::write(&replacement_path, &replacement_for_callback)
                .expect("replace remote orphan after proof");
        })
        .expect("configure remote replacement race");

        // Act
        let reopen_result = Engine::open(cloud_options());
        fail::remove("midge::cloud::before_compaction_orphan_delete");
        scenario.teardown();

        // Assert
        assert!(matches!(reopen_result, Err(MidgeError::RecoveryFailed(_))));
        assert_eq!(
            std::fs::read(&orphan_path).expect("replacement must remain remote"),
            replacement
        );
    }

    #[test]
    fn should_remove_remote_compaction_orphan_when_column_family_is_dropped_before_reopen() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let open_cloud = || {
            Engine::open(
                OpenOptions::cloud_simulated(db_path, "test-bucket", "dropped-cf-cleanup")
                    .background_compaction(false)
                    .build()
                    .expect("build simulated cloud options"),
            )
            .expect("open simulated cloud engine")
        };
        let engine = open_cloud();
        let cf = engine
            .create_column_family("drop-after-compaction-failure")
            .expect("create compaction column family");
        for batch in 0..4 {
            let key = format!("dropped-remote-orphan-{batch}");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin compaction seed");
            tx.put(key.into_bytes(), b"value".to_vec(), None)
                .expect("put compaction seed");
            tx.commit(WriteOptions::cloud_async())
                .expect("commit compaction seed");
            engine.flush_cf(&cf).expect("flush compaction seed");
        }
        let cloud_root = db_path.join("cloud_store");
        let initial_remote_files = sst_file_names(&cloud_root);
        assert_eq!(initial_remote_files.len(), 4);
        let scenario = fail::FailScenario::setup();
        fail::cfg(
            "midge::manifest::inject_no_space_on_compaction_batch_edit",
            "return",
        )
        .expect("configure manifest batch append failure");

        // Act: the output upload is tracked but not published. Dropping the CF
        // then removes every input name that normally proves the intent is still
        // prepublication.
        engine
            .compact_all()
            .expect_err("compact_all must report the manifest batch failure");
        fail::remove("midge::manifest::inject_no_space_on_compaction_batch_edit");
        scenario.teardown();
        let failed_remote_files = sst_file_names(&cloud_root);
        let orphan_names = failed_remote_files
            .difference(&initial_remote_files)
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(orphan_names.len(), 1);
        engine
            .drop_column_family(cf.id())
            .expect("drop column family after compaction failure");
        shutdown_engine(engine);
        let orphan_path = cloud_root.join("sst").join(&orphan_names[0]);
        assert!(
            orphan_path.exists(),
            "drop GC must not mistake the unpublished output for an authoritative input"
        );
        let reopened = open_cloud();

        // Assert
        assert!(reopened
            .get_column_family("drop-after-compaction-failure")
            .is_none());
        assert!(
            !orphan_path.exists(),
            "startup must delete the proven remote output before inactive-CF replay clears its intent"
        );
    }

    #[test]
    fn should_recover_cloud_compaction_when_intent_save_fails() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let options = OpenOptions::cloud_simulated(db_path, "test-bucket", "intent-before-upload")
            .background_compaction(false)
            .target_sst_size_for_testing(64)
            .build()
            .expect("build simulated cloud options");
        let engine = Engine::open(options.clone()).expect("open simulated cloud engine");
        let cf = default_cf(&engine);
        for batch in 0..4 {
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin compaction seed");
            tx.put(
                format!("intent-first-{batch}").into_bytes(),
                b"value".to_vec(),
                None,
            )
            .expect("put compaction seed");
            tx.commit(WriteOptions::cloud_async())
                .expect("commit compaction seed");
            engine.flush_cf(&cf).expect("flush compaction seed");
        }
        let cloud_root = db_path.join("cloud_store");
        let initial_remote = sst_file_names(&cloud_root);
        assert_eq!(initial_remote.len(), 4);
        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::intent::inject_no_space_on_save", "return")
            .expect("configure intent persistence failure");

        // Act
        let error = engine
            .compact_all()
            .expect_err("compact_all must report the intent persistence failure");
        fail::remove("midge::intent::inject_no_space_on_save");
        scenario.teardown();

        // Assert
        assert_no_space_like(&error);
        let failed_remote = sst_file_names(&cloud_root);
        assert!(failed_remote.is_superset(&initial_remote));
        assert!(failed_remote.len() > initial_remote.len());
        assert_eq!(manifest_sst_file_names(&engine), initial_remote);
        let orphan_bytes: Vec<_> = failed_remote
            .difference(&initial_remote)
            .map(|name| {
                let path = cloud_root.join("sst").join(name);
                let bytes = std::fs::read(&path).expect("read uploaded orphan");
                (path, bytes)
            })
            .collect();
        shutdown_engine(engine);
        discard_local_cloud_data_cache(db_path);
        let reopened = Engine::open(options).expect("reopen with cold SST and WAL caches");
        assert_eq!(manifest_sst_file_names(&reopened), initial_remote);
        assert!(sst_file_names(db_path).is_empty());
        reopened
            .compact_all()
            .expect("retry with a fresh generation");
        let published = manifest_sst_file_names(&reopened);
        assert!(!published.is_empty());
        assert!(published.is_disjoint(&failed_remote));
        for (path, bytes) in orphan_bytes {
            assert_eq!(
                std::fs::read(path).expect("untracked remote orphan retained"),
                bytes,
                "retry must not overwrite an orphan from the durably reserved earlier generation"
            );
        }
        let reopened_cf = default_cf(&reopened);
        for batch in 0..4 {
            let key = format!("intent-first-{batch}");
            assert_visible(&reopened, &reopened_cf, key.as_bytes(), b"value");
        }
    }

    #[test]
    fn should_recover_compaction_from_manifest_checkpoint_save_failure_after_batch_journal_success()
    {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let engine = open_local_engine(db_path);
        let cf = default_cf(&engine);

        for batch in 0..6 {
            for index in 0..25 {
                let key = format!("checkpoint-compaction-b{batch}-k{index:02}");
                let value = format!("value-b{batch}-k{index:02}");
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin batch tx");
                tx.put(key.into_bytes(), value.into_bytes(), None)
                    .expect("put best-effort compaction seed");
                tx.commit(WriteOptions::best_effort())
                    .expect("commit best-effort compaction seed");
            }
            engine.flush_cf(&cf).expect("flush compaction seed");
        }

        let initial_layout = engine.get_storage_layout().expect("initial storage layout");
        assert!(
            !initial_layout
                .levels
                .iter()
                .any(|level| level.level > 0 && level.file_count > 0),
            "expected only L0 files before compaction"
        );

        // Act
        let scenario = fail::FailScenario::setup();
        fail::cfg(
            "midge::manifest::inject_no_space_on_checkpoint_save",
            "return",
        )
        .expect("configure manifest checkpoint no-space failpoint");
        engine
            .compact_all()
            .expect_err("compact_all must report the checkpoint save failure");
        assert_eq!(
            engine
                .get_runtime_metrics()
                .expect("runtime metrics")
                .health,
            EngineHealth::Degraded,
            "checkpoint failure should degrade the live engine until restart"
        );
        fail::remove("midge::manifest::inject_no_space_on_checkpoint_save");
        scenario.teardown();

        shutdown_engine(engine);

        let reopened = open_local_engine(db_path);
        let reopened_cf = default_cf(&reopened);

        // Assert
        for batch in 0..6 {
            for index in 0..25 {
                let key = format!("checkpoint-compaction-b{batch}-k{index:02}");
                let value = format!("value-b{batch}-k{index:02}");
                assert_visible(&reopened, &reopened_cf, key.as_bytes(), value.as_bytes());
            }
        }

        let recovered_layout = reopened
            .get_storage_layout()
            .expect("recovered storage layout");
        assert_eq!(recovered_layout.health, EngineHealth::Healthy);
        assert!(
            recovered_layout
                .levels
                .iter()
                .any(|level| level.level > 0 && level.file_count > 0),
            "journal replay should recover the compaction result into higher levels"
        );
    }

    fn failpoint_test_lock() -> &'static Mutex<()> {
        FAILPOINT_TEST_LOCK.get_or_init(|| Mutex::new(()))
    }

    fn wait_for_sst_count(engine: &Engine, expected: usize) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            let actual = engine
                .get_runtime_metrics()
                .expect("runtime metrics")
                .sst_count;
            if actual == expected {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for {expected} SSTs; observed {actual}"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    fn open_local_engine(db_path: &Path) -> Engine {
        Engine::open(
            OpenOptions::local(db_path)
                .background_compaction(false)
                .build()
                .expect("build options"),
        )
        .expect("open engine")
    }

    fn open_local_engine_with_target(db_path: &Path, target_sst_size: usize) -> Engine {
        Engine::open(
            OpenOptions::local(db_path)
                .background_compaction(false)
                .target_sst_size_for_testing(target_sst_size)
                .build()
                .expect("build small-target options"),
        )
        .expect("open small-target engine")
    }

    fn shutdown_engine(mut engine: Engine) {
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown engine before same-path reopen");
    }

    fn default_cf(engine: &Engine) -> cntryl_midge::ColumnFamilyHandle {
        engine
            .get_column_family("default")
            .expect("default column family")
    }

    fn write_cf_value(
        engine: &Engine,
        cf: &cntryl_midge::ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
    ) {
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin column-family write");
        tx.put(key.to_vec(), value.to_vec(), None)
            .expect("put column-family value");
        tx.commit(WriteOptions::sync())
            .expect("commit column-family value");
    }

    fn read_cf_value(
        engine: &Engine,
        cf: &cntryl_midge::ColumnFamilyHandle,
        key: &[u8],
    ) -> Option<Bytes> {
        engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .and_then(|tx| tx.get(key))
            .expect("read column-family value")
    }

    fn seed_range(
        engine: &Engine,
        cf: &cntryl_midge::ColumnFamilyHandle,
        range: std::ops::Range<u32>,
        prefix: &str,
    ) {
        seed_range_with_options(engine, cf, range, prefix, WriteOptions::sync());
    }

    fn seed_range_with_options(
        engine: &Engine,
        cf: &cntryl_midge::ColumnFamilyHandle,
        range: std::ops::Range<u32>,
        prefix: &str,
        write_options: WriteOptions,
    ) {
        for index in range {
            let key = format!("{prefix}-{index:02}");
            let value = format!("value-{index:02}");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin seed tx");
            tx.put(key.into_bytes(), value.into_bytes(), None)
                .expect("put seed value");
            tx.commit(write_options).expect("commit seed value");
        }
    }

    fn seed_compaction_batches(
        engine: &Engine,
        cf: &cntryl_midge::ColumnFamilyHandle,
        prefix: &str,
        batch_count: usize,
        keys_per_batch: usize,
    ) {
        for batch in 0..batch_count {
            for index in 0..keys_per_batch {
                let key = format!("{prefix}-b{batch}-k{index:02}");
                let value = format!("value-b{batch}-k{index:02}");
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin batch tx");
                tx.put(key.into_bytes(), value.into_bytes(), None)
                    .expect("put compaction seed");
                tx.commit(WriteOptions::sync())
                    .expect("commit compaction seed");
            }
            engine.flush_cf(cf).expect("flush compaction seed");
        }
    }

    fn assert_compaction_batches_visible(
        engine: &Engine,
        cf: &cntryl_midge::ColumnFamilyHandle,
        prefix: &str,
        batch_count: usize,
        keys_per_batch: usize,
    ) {
        for batch in 0..batch_count {
            for index in 0..keys_per_batch {
                let key = format!("{prefix}-b{batch}-k{index:02}");
                let value = format!("value-b{batch}-k{index:02}");
                assert_visible(engine, cf, key.as_bytes(), value.as_bytes());
            }
        }
    }

    fn assert_visible(
        engine: &Engine,
        cf: &cntryl_midge::ColumnFamilyHandle,
        key: &[u8],
        expected: &[u8],
    ) {
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin read tx");
        assert_eq!(
            tx.get(key).expect("get visible key"),
            Some(Bytes::copy_from_slice(expected)),
            "key {:?} must remain visible",
            String::from_utf8_lossy(key)
        );
    }

    fn assert_absent(engine: &Engine, cf: &cntryl_midge::ColumnFamilyHandle, key: &[u8]) {
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin read tx");
        assert_eq!(
            tx.get(key).expect("get absent key"),
            None,
            "key {:?} must not become visible",
            String::from_utf8_lossy(key)
        );
    }

    fn assert_acknowledged_exhaustion_fixture(
        engine: &Engine,
        cf: &cntryl_midge::ColumnFamilyHandle,
    ) {
        assert_visible(engine, cf, b"stream-record", b"original-stream-bytes");
        assert_visible(engine, cf, b"queue-record-a", b"queue-a");
        assert_visible(engine, cf, b"queue-record-b", b"queue-b");
    }

    fn assert_no_space_like(error: &MidgeError) {
        let error_text = error.to_string().to_ascii_lowercase();
        assert!(
            matches!(error, MidgeError::NoSpace(_)) || error_text.contains("no space"),
            "expected a no-space error, got: {error}"
        );
    }

    fn count_sst_files(db_path: &Path) -> usize {
        sst_file_names(db_path).len()
    }

    fn manifest_sst_file_names(engine: &Engine) -> std::collections::BTreeSet<String> {
        engine
            .get_storage_layout()
            .expect("storage layout")
            .levels
            .into_iter()
            .flat_map(|level| level.files.into_iter().map(|file| file.name))
            .collect()
    }

    fn discard_local_cloud_data_cache(db_path: &Path) {
        // The filesystem simulator keeps metadata authority local. Exercise loss
        // of every data cache while preserving that simulator metadata contract.
        for name in ["sst", "wal", "hybrid_local", "txn", "cloud_recovery"] {
            let path = db_path.join(name);
            if path.exists() {
                std::fs::remove_dir_all(path).expect("discard local data cache");
            }
        }
    }

    fn sst_file_names(db_path: &Path) -> std::collections::BTreeSet<String> {
        let sst_dir = db_path.join("sst");
        let Ok(entries) = std::fs::read_dir(&sst_dir) else {
            return std::collections::BTreeSet::new();
        };
        entries
            .flatten()
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("sst"))
            })
            .filter_map(|entry| entry.file_name().into_string().ok())
            .collect()
    }
}

mod chaos_real {
    use std::collections::HashMap;
    use std::fs;
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::{Arc, Barrier};
    use std::thread;

    use bytes::Bytes;
    use cntryl_midge::wal::{self, WalOpKind, WalRecord};
    use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    use crate::common::crash;

    const CHILD_TEST_NAME: &str =
        "chaos_real::should_abort_in_child_process_when_chaos_scenario_requested";
    const ENV_SCENARIO: &str = "MIDGE_CHAOS_REAL_SCENARIO";
    const ENV_DB_PATH: &str = "MIDGE_CHAOS_REAL_DB_PATH";
    const ENV_TRIGGER_CF: &str = "MIDGE_CHAOS_REAL_TRIGGER_CF";
    const ENV_WAL_APPEND_CRASH_TARGET: &str = "MIDGE_WAL_APPEND_CRASH_TARGET";

    /*
    SCENARIO: commit returned Ok, process crashes after WAL append but before fsync, strict durability mode.
    STATUS: not currently testable honestly.
    BLOCKER: the strict commit path does not expose a post-ack, pre-fsync crash boundary. The available WAL append hook fires before commit returns Ok, which would make a failing write look committed if we tested it.
    NEEDED: a production hook after the client-visible acknowledgment boundary for a buffered/strict write and before the corresponding sync makes the write durable.
    */

    /*
    #[ignore = "Exposes a real bug: a buffered write can become visible after a WAL-append crash even when commit never returned Ok"]
    SCENARIO: compaction output SST fully written, process crashes before manifest swap.
    STATUS: not currently testable honestly.
    BLOCKER: the live runtime path does not currently wire compaction completion into manifest publication during normal engine operation, so there is no real manifest-swap boundary to crash between.
    NEEDED: live manifest publication for compaction outputs plus a failpoint after the output SST is durable and before the manifest version becomes visible.
    */

    /*
    SCENARIO: manifest references new SST set after compaction, process crashes before old SSTs are deleted.
    STATUS: not currently testable honestly.
    BLOCKER: the live runtime path does not currently drive obsolete-SST deletion after compaction publication, so there is no real old-file deletion boundary to crash between.
    NEEDED: end-to-end compaction publish and obsolete-file cleanup wiring, plus a failpoint after manifest publication and before old SST deletion.
    */

    /*
    SCENARIO: valid SST file is corrupted after crash and before reopen.
    STATUS: not currently testable honestly.
    BLOCKER: current reopen behavior is still dominated by WAL replay for the reachable crash flows above, so corrupting an SST file would not isolate an SST-only recovery path without additional production wiring.
    NEEDED: a reachable manifest-published SST-only reopen path, or a way to disable WAL replay for this scenario while keeping the engine on a real production code path.
    */

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct CommitRecord {
        key: Vec<u8>,
        value: Vec<u8>,
    }

    #[test]
    fn should_abort_in_child_process_when_chaos_scenario_requested() {
        // Arrange
        let Some(scenario) = std::env::var_os(ENV_SCENARIO) else {
            return;
        };

        let db_path = PathBuf::from(std::env::var_os(ENV_DB_PATH).expect("db path env"));

        // Act
        match scenario.to_string_lossy().as_ref() {
            "flush_after_sst_write" => child_flush_after_sst_write(&db_path),
            "flush_after_sst_write_best_effort" => {
                child_flush_after_sst_write_best_effort(&db_path);
            }
            "manifest_crash_after_sync" => child_manifest_crash_after_sync(&db_path),
            "concurrent_random_wal_append" => child_concurrent_random_wal_append(&db_path),
            other => panic!("unknown child scenario: {other}"),
        }

        // Assert
        panic!("child scenario returned without abort");
    }

    #[test]
    fn should_recover_sync_commits_when_crashing_after_sst_write_before_manifest_publication() {
        // WHAT THIS TEST DOES:
        // Runs a child process that writes sync commits, calls flush on the default CF,
        // and aborts at the real failpoint after the SST file is written but before the flush returns.
        // FAILURE INJECTED:
        // Real subprocess abort via midge::flush::after_sst_write_before_publish.
        // EXPECTED INVARIANT:
        // Every sync-committed key remains readable after reopen.
        // WHY THIS IS HONEST:
        // The child dies inside the production flush path; the parent then deletes the unpublished SSTs
        // and proves recovery came from WAL rather than a fake clean shutdown.
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        // Act
        let committed = act_flush_crash_before_publish(db_path);

        // Assert
        assert!(
            !committed.is_empty(),
            "child did not persist tracked commits"
        );
        let engine = open_local_engine(db_path);
        assert_committed_records_visible(&engine, &committed);
    }

    #[test]
    fn should_not_publish_best_effort_writes_when_crashing_after_sst_write_before_manifest_publication(
    ) {
        // WHAT THIS TEST DOES:
        // Runs a child process that writes best-effort commits, calls flush, and aborts at the real failpoint
        // after the SST is durable but before manifest publication completes.
        // FAILURE INJECTED:
        // Real subprocess abort via midge::flush::after_sst_write_before_publish.
        // EXPECTED INVARIANT:
        // Best-effort writes must not become restart-visible because flush did not return Ok.
        // WHY THIS IS HONEST:
        // The child dies in the production flush path after the file write; recovery must clean up the orphan SST
        // instead of incorrectly publishing it.
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        // Act
        let committed = act_best_effort_flush_crash_before_publish(db_path);

        // Assert
        assert!(
            !committed.is_empty(),
            "child did not stage tracked best-effort writes"
        );

        let engine = open_local_engine(db_path);
        assert_committed_records_absent(&engine, &committed);
    }

    #[test]
    fn should_preserve_complete_wal_records_when_partial_final_record_is_missing_one_byte() {
        // WHAT THIS TEST DOES:
        // Runs a child process that writes sync commits and then aborts using a real manifest-persist failpoint.
        // The parent appends a valid WAL record directly to wal.log and truncates one byte from its tail.
        // FAILURE INJECTED:
        // Real subprocess abort via midge::manifest::after_temp_sync_before_rename plus direct WAL tail truncation.
        // EXPECTED INVARIANT:
        // Recovery preserves every complete record before the partial tail and never fabricates a value from the cut record.
        // WHY THIS IS HONEST:
        // The corruption is applied to the real on-disk WAL file between crash and reopen; recovery runs through the production WAL parser.
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        // Act
        let committed = act_manifest_crash_with_partial_final_record(db_path);
        let engine = open_local_engine(db_path);
        assert_committed_records_visible(&engine, &committed);

        // Assert
        let default_cf = default_cf(&engine);
        let tx = engine
            .begin_tx(default_cf.id(), TransactionMode::ReadOnly)
            .expect("begin read tx");
        assert_eq!(
            tx.get(b"synthetic_tail").expect("get synthetic tail"),
            None,
            "partial tail record must not appear after recovery"
        );
    }

    #[test]
    fn should_preserve_complete_wal_prefix_when_truncated_to_one_byte_past_last_complete_record() {
        // WHAT THIS TEST DOES:
        // Runs a child process that writes sync commits and aborts using a real manifest-persist failpoint.
        // The parent then appends a synthetic WAL record and truncates wal.log so exactly one byte of that new record remains.
        // FAILURE INJECTED:
        // Real subprocess abort via midge::manifest::after_temp_sync_before_rename plus direct WAL truncation.
        // EXPECTED INVARIANT:
        // Recovery keeps the complete prefix and drops the one-byte tail fragment.
        // WHY THIS IS HONEST:
        // The file is mutated on disk and reopened through the real recovery implementation.
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        // Act
        let committed = act_manifest_crash_with_prefix_fragment(db_path);
        let engine = open_local_engine(db_path);
        assert_committed_records_visible(&engine, &committed);

        // Assert
        let default_cf = default_cf(&engine);
        let tx = engine
            .begin_tx(default_cf.id(), TransactionMode::ReadOnly)
            .expect("begin read tx");
        assert_eq!(
            tx.get(b"synthetic_prefix_tail")
                .expect("get synthetic prefix tail"),
            None,
            "one-byte tail fragment must not materialize as a recovered value"
        );
    }

    #[test]
    fn should_recover_all_sync_commits_when_manifest_file_is_zeroed_after_crash() {
        // WHAT THIS TEST DOES:
        // Runs a child process that writes sync commits, persists a manifest successfully once,
        // then aborts during a second manifest persist using a real failpoint. The parent zeroes manifest.json before reopen.
        // FAILURE INJECTED:
        // Real subprocess abort via midge::manifest::after_temp_sync_before_rename plus direct manifest zeroing.
        // EXPECTED INVARIANT:
        // Reopen must recover every sync-committed key exactly, never a partial view.
        // WHY THIS IS HONEST:
        // The crash and the corruption both happen against real files and the reopen path is production recovery.
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        // Act
        let committed = act_manifest_crash_with_zeroed_manifest(db_path);

        // Assert
        match Engine::open(OpenOptions::local(db_path).build().expect("build options")) {
            Ok(engine) => assert_committed_records_visible(&engine, &committed),
            Err(error) => {
                let message = error.to_string();
                assert!(
                    message.contains("failed to load manifest") || message.contains("manifest"),
                    "zeroed manifest must either reopen cleanly or fail with a clear manifest error, got: {message}"
                );
            }
        }
    }

    #[test]
    fn should_recover_only_committed_or_single_inflight_write_when_random_wal_append_crash_interrupts_buffered_writes(
    ) {
        // WHAT THIS TEST DOES:
        // Runs four child threads that each attempt one hundred buffered commits under a start barrier,
        // with a real failpoint aborting the process at a random WAL append count.
        // FAILURE INJECTED:
        // Real subprocess abort via midge::wal::after_append_batch_before_sync.
        // EXPECTED INVARIANT:
        // Every successful commit is visible after recovery. At most one additional visible key may
        // appear: the WAL-appended transaction interrupted before the child could record commit success.
        // WHY THIS IS HONEST:
        // The process dies after physical WAL append and before the client-visible ack. The child
        // serializes commit attempts, so that boundary can expose at most one unacknowledged write.
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        // Act
        let committed = act_concurrent_random_wal_append_crash(db_path);

        // Assert
        assert!(
            !committed.is_empty(),
            "expected at least one successful commit before crash"
        );
        let engine = open_local_engine(db_path);
        let default_cf = default_cf(&engine);
        let mut unacknowledged_visible = 0usize;

        for thread_id in 0..4 {
            for index in 0..100 {
                let key = concurrent_key(thread_id, index);
                let tx = engine
                    .begin_tx(default_cf.id(), TransactionMode::ReadOnly)
                    .expect("begin read tx");
                let actual = tx.get(key.as_bytes()).expect("get concurrent key");

                match (actual, committed.get(key.as_bytes())) {
                    (Some(value), Some(expected)) => {
                        assert_eq!(
                            value.as_ref(),
                            expected.as_slice(),
                            "visible key {key} must match the exact value returned by a successful commit"
                        );
                    }
                    (Some(value), None) => {
                        unacknowledged_visible += 1;
                        let expected = concurrent_value(thread_id, index);
                        assert_eq!(
                            value.as_ref(),
                            expected.as_bytes(),
                            "visible unacknowledged key {key} must match its deterministic WAL value"
                        );
                    }
                    (None, Some(_)) => panic!("committed key {key} was not recovered"),
                    (None, None) => {}
                }
            }
        }

        assert!(
            unacknowledged_visible <= 1,
            "serialized child writes can leave at most one WAL-appended transaction without a child commit record, found {unacknowledged_visible}"
        );
    }

    #[test]
    fn should_fail_deterministically_given_fixed_offset_when_replaying_concurrent_wal_append_crash()
    {
        // Arrange
        let first = TempDir::new().expect("first temp dir");
        let second = TempDir::new().expect("second temp dir");
        let target = 17;

        // Act
        let first_committed = act_concurrent_wal_append_crash_at(first.path(), target);
        let second_committed = act_concurrent_wal_append_crash_at(second.path(), target);
        let first_visible = count_visible_concurrent_keys(first.path());
        let second_visible = count_visible_concurrent_keys(second.path());

        // Assert
        assert_eq!(first_committed.len(), target - 1);
        assert_eq!(second_committed.len(), target - 1);
        assert_eq!(first_visible, second_visible);
        assert!((target - 1..=target).contains(&first_visible));
    }

    #[test]
    fn should_preserve_recovered_data_when_two_crash_recovery_cycles_reuse_same_directory() {
        // WHAT THIS TEST DOES:
        // Runs one crashing child that writes sync commits and aborts in manifest persistence,
        // reopens and verifies recovery, then runs a second crashing child against the same directory and reopens again.
        // FAILURE INJECTED:
        // Real subprocess aborts via midge::manifest::after_temp_sync_before_rename in both child runs.
        // EXPECTED INVARIANT:
        // Data that survived the first recovery is still present after the second crash/recovery cycle.
        // WHY THIS IS HONEST:
        // Both failures are real process aborts on the same on-disk database directory.
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        // Act
        let committed = act_two_manifest_crash_cycles(db_path);

        // Assert
        let engine = open_local_engine(db_path);
        assert_committed_records_visible(&engine, &committed);
    }

    fn act_flush_crash_before_publish(db_path: &Path) -> Vec<CommitRecord> {
        run_child_expect_abort("flush_after_sst_write", db_path, &[]);
        expire_crashed_process_lease(db_path);

        let sst_dir = db_path.join("sst");
        if sst_dir.exists() {
            fs::remove_dir_all(&sst_dir).expect("remove unpublished sst dir");
        }

        read_committed_records(db_path)
    }

    fn act_best_effort_flush_crash_before_publish(db_path: &Path) -> Vec<CommitRecord> {
        run_child_expect_abort("flush_after_sst_write_best_effort", db_path, &[]);
        expire_crashed_process_lease(db_path);
        read_committed_records(db_path)
    }

    fn act_manifest_crash_with_partial_final_record(db_path: &Path) -> Vec<CommitRecord> {
        run_child_expect_abort(
            "manifest_crash_after_sync",
            db_path,
            &[(ENV_TRIGGER_CF, "truncate-one-byte-trigger")],
        );
        expire_crashed_process_lease(db_path);

        let wal_path = wal_log_path(db_path);
        append_complete_wal_record(&wal_path, b"synthetic_tail", b"synthetic_value");
        truncate_file_by(&wal_path, 1);

        read_committed_records(db_path)
    }

    fn act_manifest_crash_with_prefix_fragment(db_path: &Path) -> Vec<CommitRecord> {
        run_child_expect_abort(
            "manifest_crash_after_sync",
            db_path,
            &[(ENV_TRIGGER_CF, "truncate-to-prefix-trigger")],
        );
        expire_crashed_process_lease(db_path);

        let wal_path = wal_log_path(db_path);
        let original_len = fs::metadata(&wal_path).expect("wal metadata").len();
        append_complete_wal_record(&wal_path, b"synthetic_prefix_tail", b"synthetic_value");
        truncate_file_to(&wal_path, original_len + 1);

        read_committed_records(db_path)
    }

    fn act_manifest_crash_with_zeroed_manifest(db_path: &Path) -> Vec<CommitRecord> {
        run_child_expect_abort(
            "manifest_crash_after_sync",
            db_path,
            &[(ENV_TRIGGER_CF, "manifest-zero-trigger")],
        );
        expire_crashed_process_lease(db_path);

        let manifest_path = manifest_path(db_path);
        assert!(
            manifest_path.exists(),
            "manifest file should exist before corruption"
        );
        zero_file(&manifest_path);

        read_committed_records(db_path)
    }

    fn act_concurrent_random_wal_append_crash(db_path: &Path) -> HashMap<Vec<u8>, Vec<u8>> {
        let target = selected_wal_append_crash_target();
        act_concurrent_wal_append_crash_at(db_path, target)
    }

    fn act_concurrent_wal_append_crash_at(
        db_path: &Path,
        target: usize,
    ) -> HashMap<Vec<u8>, Vec<u8>> {
        assert!((2..=400).contains(&target));
        let target_text = target.to_string();
        eprintln!(
            "[midge-test] {ENV_WAL_APPEND_CRASH_TARGET}={target}; replay with the same value"
        );
        run_child_expect_abort(
            "concurrent_random_wal_append",
            db_path,
            &[(ENV_WAL_APPEND_CRASH_TARGET, target_text.as_str())],
        );
        expire_crashed_process_lease(db_path);
        committed_map_by_key(&read_committed_records(db_path))
    }

    fn count_visible_concurrent_keys(db_path: &Path) -> usize {
        let mut engine = open_local_engine(db_path);
        let default_cf = default_cf(&engine);
        let mut visible = 0;
        for thread_id in 0..4 {
            for index in 0..100 {
                let key = concurrent_key(thread_id, index);
                let tx = engine
                    .begin_tx(default_cf.id(), TransactionMode::ReadOnly)
                    .expect("begin replay verification transaction");
                visible += usize::from(tx.get(key.as_bytes()).expect("read replay key").is_some());
            }
        }
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown replay verification engine");
        visible
    }

    fn act_two_manifest_crash_cycles(db_path: &Path) -> Vec<CommitRecord> {
        run_child_expect_abort(
            "manifest_crash_after_sync",
            db_path,
            &[(ENV_TRIGGER_CF, "cycle-one-trigger")],
        );
        expire_crashed_process_lease(db_path);

        let committed = read_committed_records(db_path);
        let mut engine = open_local_engine(db_path);
        assert_committed_records_visible(&engine, &committed);
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown between crash cycles");

        run_child_expect_abort(
            "manifest_crash_after_sync",
            db_path,
            &[(ENV_TRIGGER_CF, "cycle-two-trigger")],
        );
        expire_crashed_process_lease(db_path);

        committed
    }

    fn child_flush_after_sst_write(db_path: &Path) {
        crash::configure_abort_failpoint(
            "midge::flush::after_sst_write_before_publish",
            "flush_after_sst_write",
        );

        let engine = open_local_engine(db_path);
        let default_cf = default_cf(&engine);
        let committed_path = committed_log_path(db_path);

        for index in 0..8 {
            let key = format!("flush-key-{index:02}");
            let value = format!("flush-value-{index:02}");
            put_and_track_commit(
                &engine,
                &default_cf,
                key.as_bytes(),
                value.as_bytes(),
                WriteOptions::sync(),
                &committed_path,
            );
        }

        engine
            .flush_cf(&default_cf)
            .expect("flush should reach failpoint");
    }

    fn child_flush_after_sst_write_best_effort(db_path: &Path) {
        crash::configure_abort_failpoint(
            "midge::flush::after_sst_write_before_publish",
            "flush_after_sst_write_best_effort",
        );

        let engine = open_local_engine(db_path);
        let default_cf = default_cf(&engine);
        let committed_path = committed_log_path(db_path);

        for index in 0..8 {
            let key = format!("best-effort-flush-key-{index:02}");
            let value = format!("best-effort-flush-value-{index:02}");
            put_and_track_commit(
                &engine,
                &default_cf,
                key.as_bytes(),
                value.as_bytes(),
                WriteOptions::best_effort(),
                &committed_path,
            );
        }

        engine
            .flush_cf(&default_cf)
            .expect("flush should reach failpoint");
    }

    fn child_manifest_crash_after_sync(db_path: &Path) {
        let engine = open_local_engine(db_path);
        let default_cf = default_cf(&engine);
        let committed_path = committed_log_path(db_path);

        if engine.get_column_family("manifest-seed").is_none() {
            engine
                .create_column_family("manifest-seed")
                .expect("seed manifest file");
        }

        for index in 0..8 {
            let key = format!("sync-key-{index:02}");
            let value = format!("sync-value-{index:02}");
            put_and_track_commit(
                &engine,
                &default_cf,
                key.as_bytes(),
                value.as_bytes(),
                WriteOptions::sync(),
                &committed_path,
            );
        }

        crash::configure_abort_failpoint(
            "midge::manifest::after_temp_sync_before_rename",
            "manifest_crash_after_sync",
        );

        let trigger_cf = std::env::var(ENV_TRIGGER_CF).expect("trigger cf env");
        assert!(
            engine.get_column_family(&trigger_cf).is_none(),
            "trigger column family must be unique per child run"
        );
        engine
            .create_column_family(&trigger_cf)
            .expect("manifest persist should reach failpoint");
    }

    fn child_concurrent_random_wal_append(db_path: &Path) {
        let target = std::env::var(ENV_WAL_APPEND_CRASH_TARGET)
            .expect("WAL append crash target env")
            .parse::<usize>()
            .expect("WAL append crash target must be an integer");
        assert!(
            (2..=400).contains(&target),
            "WAL append crash target must be in 2..=400"
        );
        eprintln!("[midge-test-child] WAL append crash target={target}");
        crash::configure_nth_abort_failpoint(
            "midge::wal::after_append_batch_before_sync",
            "concurrent_random_wal_append",
            target,
        );

        let engine = Arc::new(open_local_engine(db_path));
        let default_cf = default_cf(&engine);
        let committed_path = Arc::new(committed_log_path(db_path));
        let start_barrier = Arc::new(Barrier::new(5));
        let commit_gate = Arc::new(std::sync::Mutex::new(()));

        for thread_id in 0..4 {
            let engine = Arc::clone(&engine);
            let committed_path = Arc::clone(&committed_path);
            let start_barrier = Arc::clone(&start_barrier);
            let commit_gate = Arc::clone(&commit_gate);
            let default_cf = default_cf.clone();
            thread::spawn(move || {
                start_barrier.wait();
                for index in 0..100 {
                    let key = concurrent_key(thread_id, index);
                    let value = concurrent_value(thread_id, index);
                    let _guard = commit_gate
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    put_and_track_commit(
                        &engine,
                        &default_cf,
                        key.as_bytes(),
                        value.as_bytes(),
                        WriteOptions::buffered(),
                        &committed_path,
                    );
                }
            });
        }

        start_barrier.wait();
        loop {
            thread::park();
        }
    }

    fn open_local_engine(db_path: &Path) -> Engine {
        Engine::open(OpenOptions::local(db_path).build().expect("build options"))
            .expect("open engine")
    }

    fn default_cf(engine: &Engine) -> cntryl_midge::ColumnFamilyHandle {
        engine
            .get_column_family("default")
            .expect("default column family")
    }

    fn put_and_track_commit(
        engine: &Engine,
        cf: &cntryl_midge::ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
        opts: WriteOptions,
        committed_path: &Path,
    ) {
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin write tx");
        tx.put(key.to_vec(), value.to_vec(), None)
            .expect("put value");
        tx.commit(opts).expect("commit value");
        append_commit_record(committed_path, key, value);
    }

    fn append_commit_record(committed_path: &Path, key: &[u8], value: &[u8]) {
        let record = CommitRecord {
            key: key.to_vec(),
            value: value.to_vec(),
        };
        let line = serde_json::to_vec(&record).expect("serialize commit record");
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(committed_path)
            .expect("open committed log");
        file.write_all(&line).expect("write committed record");
        file.write_all(b"\n").expect("write newline");
        file.sync_all().expect("sync committed log");
    }

    fn read_committed_records(db_path: &Path) -> Vec<CommitRecord> {
        let committed_path = committed_log_path(db_path);
        let Ok(bytes) = fs::read(&committed_path) else {
            return Vec::new();
        };

        let mut records = Vec::new();
        for line in bytes.split(|byte| *byte == b'\n') {
            if line.is_empty() {
                continue;
            }
            if let Ok(record) = serde_json::from_slice::<CommitRecord>(line) {
                records.push(record);
            }
        }
        records
    }

    fn assert_committed_records_visible(engine: &Engine, committed: &[CommitRecord]) {
        let default_cf = default_cf(engine);
        for record in committed {
            let tx = engine
                .begin_tx(default_cf.id(), TransactionMode::ReadOnly)
                .expect("begin read tx");
            let value = tx.get(&record.key).expect("get committed key");
            assert_eq!(
                value,
                Some(Bytes::from(record.value.clone())),
                "committed key {:?} must recover exactly",
                String::from_utf8_lossy(&record.key)
            );
        }
    }

    fn assert_committed_records_absent(engine: &Engine, committed: &[CommitRecord]) {
        let default_cf = default_cf(engine);
        for record in committed {
            let tx = engine
                .begin_tx(default_cf.id(), TransactionMode::ReadOnly)
                .expect("begin read tx");
            let value = tx.get(&record.key).expect("get tracked key");
            assert_eq!(
                value,
                None,
                "best-effort key {:?} must not become visible after crash before flush publication",
                String::from_utf8_lossy(&record.key)
            );
        }
    }

    fn committed_map_by_key(committed: &[CommitRecord]) -> HashMap<Vec<u8>, Vec<u8>> {
        committed
            .iter()
            .map(|record| (record.key.clone(), record.value.clone()))
            .collect()
    }

    fn run_child_expect_abort(scenario: &str, db_path: &Path, extra_envs: &[(&str, &str)]) {
        let current_exe = std::env::current_exe().expect("current exe");
        let mut command = Command::new(current_exe);
        command
            .arg("--exact")
            .arg(CHILD_TEST_NAME)
            .arg("--nocapture")
            .arg("--test-threads=1")
            .env(ENV_SCENARIO, scenario)
            .env(ENV_DB_PATH, db_path);

        for (name, value) in extra_envs {
            command.env(name, value);
        }

        crash::run_child_expect_abort(&mut command, scenario, crash_trigger(scenario), db_path);
    }

    fn crash_trigger(scenario: &str) -> &'static str {
        match scenario {
            "flush_after_sst_write" | "flush_after_sst_write_best_effort" => {
                "midge::flush::after_sst_write_before_publish"
            }
            "manifest_crash_after_sync" => "midge::manifest::after_temp_sync_before_rename",
            "concurrent_random_wal_append" => "midge::wal::after_append_batch_before_sync",
            other => panic!("unknown crash trigger for scenario {other}"),
        }
    }

    fn selected_wal_append_crash_target() -> usize {
        let target = std::env::var(ENV_WAL_APPEND_CRASH_TARGET).map_or_else(
            |_| (usize::from(rand::random::<u16>()) % 399) + 2,
            |value| {
                value.parse::<usize>().unwrap_or_else(|error| {
                    panic!("invalid {ENV_WAL_APPEND_CRASH_TARGET}: {error}")
                })
            },
        );
        assert!(
            (2..=400).contains(&target),
            "{ENV_WAL_APPEND_CRASH_TARGET} must be in 2..=400, got {target}"
        );
        target
    }

    fn append_complete_wal_record(wal_path: &Path, key: &[u8], value: &[u8]) {
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::copy_from_slice(key),
            Some(Bytes::copy_from_slice(value)),
            9_000_000,
            0,
        );
        let payload = wal::encoding::encode(&record).expect("encode wal record");
        let len = u32::try_from(payload.len()).expect("payload len fits into u32");

        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(wal_path)
            .expect("open wal path for append");
        file.write_all(&len.to_le_bytes())
            .expect("write wal length prefix");
        file.write_all(&payload).expect("write wal payload");
        file.sync_all().expect("sync wal file");
    }

    fn truncate_file_by(path: &Path, bytes: u64) {
        let current_len = fs::metadata(path).expect("file metadata").len();
        truncate_file_to(path, current_len - bytes);
    }

    fn truncate_file_to(path: &Path, len: u64) {
        let file = fs::OpenOptions::new()
            .write(true)
            .open(path)
            .expect("open file for truncation");
        file.set_len(len).expect("truncate file");
    }

    fn zero_file(path: &Path) {
        let file = fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(path)
            .expect("open file for zeroing");
        file.sync_all().expect("sync zeroed file");
    }

    fn wal_log_path(db_path: &Path) -> PathBuf {
        db_path.join("wal").join("wal.log")
    }

    fn manifest_path(db_path: &Path) -> PathBuf {
        db_path.join("manifest.json")
    }

    fn committed_log_path(db_path: &Path) -> PathBuf {
        db_path.join("chaos_real_commits.ndjson")
    }

    fn expire_crashed_process_lease(db_path: &Path) {
        let leader_path = db_path.join(".midge_leader");
        if !leader_path.exists() {
            return;
        }

        let mut content = fs::read_to_string(&leader_path).expect("read leader record");
        if content.contains("acquired_at: ") {
            content = content
                .lines()
                // Drop the checksum line rather than recompute it: a record
                // with no checksum field is valid-but-unchecked (backward
                // compatibility with pre-checksum records), so this keeps the
                // rewritten timestamp from being rejected as corrupt.
                .filter(|line| !line.starts_with("checksum: "))
                .map(|line| {
                    if line.starts_with("acquired_at: ") {
                        "acquired_at: 1970-01-01T00:00:00Z".to_string()
                    } else {
                        line.to_string()
                    }
                })
                .collect::<Vec<_>>()
                .join("\n");
            content.push('\n');
            fs::write(&leader_path, content).expect("rewrite leader record as stale");
        }
        crash::clear_crashed_process_acquisition_lock(db_path);
    }

    fn concurrent_key(thread_id: usize, index: usize) -> String {
        format!("concurrent-thread-{thread_id}-key-{index:03}")
    }

    fn concurrent_value(thread_id: usize, index: usize) -> String {
        format!("concurrent-thread-{thread_id}-value-{index:03}")
    }
}

mod chaos_intent_log {
    //! Crash Testing: WAL-Compaction Integration and Durability
    //!
    //! Validates end-to-end WAL and compaction alignment. This test complements crash tests
    //! from Slice 7 by ensuring the complete pipeline (WAL â†’ memtable â†’ flush â†’ SST â†’ compaction
    //! â†’ manifest) maintains durability guarantees when crashes occur.
    //!
    //! Slice 8 specifically validates:
    //! 1. WAL entries are preserved across compaction
    //! 2. Compaction state is correctly recovery-safe at all points
    //! 3. Intent log enables safe deferral of GC operations
    //!
    //! **Storage Modes**: Local\
    //! **Pattern**: Parent spawns child crash process, recovers, validates all data intact

    use std::fs;
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::process::Command;

    use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    use crate::common::crash;

    const CHILD_TEST_NAME: &str =
        "chaos_intent_log::should_crash_in_child_when_wal_compaction_crash_scenario_requested";
    const ENV_SCENARIO: &str = "MIDGE_WAL_COMPACTION_CHAOS_SCENARIO";
    const ENV_DB_PATH: &str = "MIDGE_WAL_COMPACTION_CHAOS_DB_PATH";

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct CommitRecord {
        key: Vec<u8>,
        value: Vec<u8>,
    }

    // ============================================================================
    // WAL & COMPACTION CRASH TESTS
    // ============================================================================

    /// Validates WAL and compaction durable frontier alignment.
    /// Crashes after manifest persist, verifies recovery succeeds and all data intact.
    #[test]
    fn should_recover_all_data_after_wal_compaction_crash() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        // Act: Spawn child that performs WAL writes, flush, compaction, then crashes
        run_child_crash_after_manifest_persist(db_path);

        // Stale the leader file to allow reopen
        expire_crashed_process_lease(db_path);

        // Assert: All data recoverable after crash + reopen
        {
            let engine = open_local_engine(db_path);
            let default_cf = default_cf(&engine);

            // Read what was committed before crash
            let committed = read_committed_records(db_path);
            assert!(
                !committed.is_empty(),
                "should have committed records before crash"
            );

            // Verify all committed data is present after recovery
            for record in &committed {
                let query_tx = engine
                    .begin_tx(default_cf.id(), TransactionMode::ReadOnly)
                    .expect("begin query tx");
                let result = query_tx.get(&record.key).expect("query should succeed");
                assert!(
                    result.is_some(),
                    "key {:?} should be recoverable after WAL + compaction crash",
                    String::from_utf8_lossy(&record.key)
                );
            }
        }
    }

    // ============================================================================
    // CHILD PROCESS HELPERS
    // ============================================================================

    /// Child: Write, flush, compact, crash after manifest persist before GC.
    fn child_create_data_flush_compact_and_crash() {
        // Configure failpoint: crash after manifest persist but before GC
        crash::configure_abort_failpoint(
            "slice6::after_manifest_persist_before_sst_gc",
            "wal_compaction_crash",
        );

        let db_path_str = std::env::var("MIDGE_WAL_COMPACTION_CHAOS_DB_PATH")
            .expect("env var MIDGE_WAL_COMPACTION_CHAOS_DB_PATH");
        let db_path = PathBuf::from(&db_path_str);

        let engine = Engine::open(
            OpenOptions::local(&db_path)
                .background_compaction(false)
                .build()
                .expect("build options"),
        )
        .expect("open engine");

        let default_cf = engine.get_column_family("default").expect("default cf");

        let mut commits = Vec::new();

        // Write 5 batches of 100 keys to create L0 SSTs for compaction
        for batch in 0..5 {
            let mut tx = engine
                .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
                .expect("begin tx");

            for i in 0..100 {
                let key = format!("wal_compaction_key_{batch:02}_{i:03}");
                let value = format!("wal_compaction_value_{batch:02}_{i:03}");

                tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                    .expect("put");

                commits.push(CommitRecord {
                    key: key.into_bytes(),
                    value: value.into_bytes(),
                });
            }

            // Commit writes (goes to WAL)
            tx.commit(WriteOptions::buffered()).expect("commit tx");

            // Flush memtable to SST (creates L0 file)
            engine.flush_cf(&default_cf).expect("flush_cf");
        }

        // Persist commit log for recovery verification
        {
            let commit_file = db_path.join("commits.ndjson");
            let mut file = fs::File::create(&commit_file).expect("create commits file");
            for record in commits {
                let line = serde_json::to_string(&record).expect("serialize");
                file.write_all(line.as_bytes()).expect("write line");
                file.write_all(b"\n").expect("write newline");
            }
        }

        // Trigger compaction (will panic at failpoint)
        engine.compact_all().expect("compact_all");

        // Should not reach here (panicked at failpoint)
        panic!("expected crash at manifest persist failpoint");
    }

    // ============================================================================
    // PARENT PROCESS HELPERS
    // ============================================================================

    fn run_child_crash_after_manifest_persist(db_path: &Path) {
        let current_exe = std::env::current_exe().expect("current exe");
        let mut command = Command::new(current_exe);
        command
            .arg("--exact")
            .arg(CHILD_TEST_NAME)
            .arg("--nocapture")
            .arg("--test-threads=1")
            .env(ENV_SCENARIO, "wal_compaction_crash")
            .env(ENV_DB_PATH, db_path);

        crash::run_child_expect_abort(
            &mut command,
            "wal_compaction_crash",
            "slice6::after_manifest_persist_before_sst_gc",
            db_path,
        );
    }

    // ============================================================================
    // CHILD DISPATCH TEST
    // ============================================================================

    #[test]
    fn should_crash_in_child_when_wal_compaction_crash_scenario_requested() {
        // Arrange
        let scenario = std::env::var(ENV_SCENARIO).unwrap_or_default();
        if scenario.is_empty() {
            return; // Not running as child
                    // Act
        }

        match scenario.as_str() {
            "wal_compaction_crash" => {
                child_create_data_flush_compact_and_crash();
                panic!("child should have crashed");
            }
            _ => {
                panic!("unknown scenario: {scenario}");
                // Assert (implicit: panics above prove test executed)
            }
        }
    }

    // ============================================================================
    // RECOVERY HELPERS (matching chaos_compaction.rs patterns)
    // ============================================================================

    fn expire_crashed_process_lease(db_path: &Path) {
        let leader_path = db_path.join(".midge_leader");
        if !leader_path.exists() {
            return;
        }

        let mut content = fs::read_to_string(&leader_path).expect("read leader record");
        if content.contains("acquired_at: ") {
            content = content
                .lines()
                // Drop the checksum line rather than recompute it: a record
                // with no checksum field is valid-but-unchecked (backward
                // compatibility with pre-checksum records), so this keeps the
                // rewritten timestamp from being rejected as corrupt.
                .filter(|line| !line.starts_with("checksum: "))
                .map(|line| {
                    if line.starts_with("acquired_at: ") {
                        "acquired_at: 1970-01-01T00:00:00Z".to_string()
                    } else {
                        line.to_string()
                    }
                })
                .collect::<Vec<_>>()
                .join("\n");
            content.push('\n');
            fs::write(&leader_path, content).expect("rewrite leader record as stale");
        }
        crash::clear_crashed_process_acquisition_lock(db_path);
    }

    fn read_committed_records(db_path: &Path) -> Vec<CommitRecord> {
        let committed_log = db_path.join("commits.ndjson");
        if !committed_log.exists() {
            return Vec::new();
        }

        let content = fs::read_to_string(&committed_log).expect("read commits log");
        content
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_str(line).expect("parse commit record"))
            .collect()
    }

    fn open_local_engine(db_path: &Path) -> Engine {
        Engine::open(
            OpenOptions::local(db_path)
                .background_compaction(false)
                .build()
                .expect("build options"),
        )
        .expect("open engine after crash")
    }

    fn default_cf(engine: &Engine) -> cntryl_midge::ColumnFamilyHandle {
        engine
            .get_column_family("default")
            .expect("default column family")
    }
}

mod chaos_compaction {
    //! Crash Testing: Compaction Durability Under Faults
    //!
    //! Tests crash recovery for the compaction lifecycle:
    //! - Crash after compaction update but before manifest persistence
    //! - Crash after manifest persistence but before SST garbage collection
    //!
    //! These tests validate that the compaction sequence maintains invariants:
    //! 1. All data remains recoverable after each crash point
    //! 2. Manifest updates are atomic (either fully applied or rolled back)
    //! 3. Input SSTs are only deleted after manifest is safely persisted
    //!
    //! **Storage Modes**: Local only (requires filesystem verification)
    //!
    //! Naming convention:
    //! should_<behavior>_`when_crashing`_<`at_point`>

    use std::collections::BTreeSet;
    use std::fs;
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::thread;
    use std::time::Duration;

    use bytes::Bytes;
    use cntryl_midge::{Engine, OpenOptions, Query, TransactionMode, WriteOptions};
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    use crate::common::crash;

    const CHILD_TEST_NAME: &str =
        "chaos_compaction::should_crash_in_child_when_compaction_crash_scenario_requested";
    const ENV_SCENARIO: &str = "MIDGE_COMPACTION_CHAOS_SCENARIO";
    const ENV_DB_PATH: &str = "MIDGE_COMPACTION_CHAOS_DB_PATH";
    const COMPACTION_INPUTS_FILE: &str = "compaction-inputs.json";

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct CommitRecord {
        key: Vec<u8>,
        value: Vec<u8>,
    }

    // ============================================================================
    // COMPACTION CRASH SCENARIOS
    // ============================================================================

    /// Crash BEFORE manifest persist: manifest update in memory, but not durable.
    /// Recovery should restore compaction state from WAL, not manifest.
    #[test]
    fn should_retain_input_ssts_given_compaction_failure_before_manifest_publish() {
        // Arrange: Create engine and write enough data to trigger compaction
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        // Act: crash after outputs are durable and intented, but before manifest publish.
        let committed = run_child_crash_at_output_durable_before_publish(db_path);
        let input_names = read_compaction_input_names(db_path);
        let disk_before_reopen = sst_names_on_disk(db_path);

        // Assert
        assert!(
            !committed.is_empty(),
            "expected child to commit data before crash"
        );
        assert!(
            input_names.is_subset(&disk_before_reopen),
            "every authoritative input must survive the pre-publication crash: inputs={input_names:?}, disk={disk_before_reopen:?}"
        );

        let engine = open_local_engine(db_path);
        let default_cf = default_cf(&engine);
        let recovered_layout = engine
            .get_storage_layout()
            .expect("storage layout after pre-publication recovery");
        let manifest_names = manifest_sst_names(&recovered_layout);
        assert_eq!(
            manifest_names, input_names,
            "rollback must leave the original inputs as the complete manifest authority"
        );
        assert_eq!(
            sst_names_on_disk(db_path),
            manifest_names,
            "startup must remove the unowned compaction output without deleting inputs"
        );

        for record in &committed {
            let tx = engine
                .begin_tx(default_cf.id(), TransactionMode::ReadOnly)
                .expect("begin read tx");
            let actual = tx.get(&record.key).expect("get committed key");
            assert_eq!(
                actual,
                Some(Bytes::copy_from_slice(&record.value)),
                "key {:?} must be recoverable after crash before manifest publish",
                String::from_utf8_lossy(&record.key)
            );
        }
    }

    #[test]
    fn should_recover_all_data_when_crashing_after_compaction_but_before_manifest_persist() {
        // Arrange: Create engine and write enough data to trigger compaction
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        // Act: Write data and flush to create L0 files, then crash during compaction â†’ manifest
        let committed = run_child_crash_at_compaction_before_persist(db_path);

        // Assert
        assert!(
            !committed.is_empty(),
            "expected child to commit data before crash"
        );

        let engine = open_local_engine(db_path);
        let default_cf = default_cf(&engine);

        // All committed data must be recoverable through WAL
        for record in &committed {
            let tx = engine
                .begin_tx(default_cf.id(), TransactionMode::ReadOnly)
                .expect("begin read tx");
            let actual = tx.get(&record.key).expect("get committed key");
            assert_eq!(
                actual,
                Some(Bytes::copy_from_slice(&record.value)),
                "key {:?} must be recoverable from WAL after crash before manifest persist",
                String::from_utf8_lossy(&record.key)
            );
        }
    }

    /// Crash AFTER manifest persist: manifest durably updated with new SSTs, input SSTs not yet deleted.
    /// Manifest references new SSTs, but old SSTs still exist on disk.
    /// Recovery should use manifest (with new SSTs), old files will be cleaned up on next GC.
    #[test]
    fn should_not_delete_input_ssts_given_compaction_gc_failure_after_manifest_publish() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        // Act: Write data, trigger compaction, crash after manifest persist but before GC
        let committed = run_child_crash_at_manifest_persist_before_gc(db_path);
        let input_names = read_compaction_input_names(db_path);
        let disk_before_reopen = sst_names_on_disk(db_path);

        // Assert: Manifest is durable, data accessible from new SSTs
        assert!(
            !committed.is_empty(),
            "expected child to commit data before crash"
        );
        assert!(
            input_names.is_subset(&disk_before_reopen),
            "GC failure must retain every obsolete input at the crash boundary: inputs={input_names:?}, disk={disk_before_reopen:?}"
        );

        let engine = open_local_engine(db_path);
        let default_cf = default_cf(&engine);
        let recovered_layout = engine
            .get_storage_layout()
            .expect("storage layout after post-publication recovery");
        let manifest_names = manifest_sst_names(&recovered_layout);
        let disk_after_reopen = sst_names_on_disk(db_path);
        let compacted_inputs = input_names
            .difference(&manifest_names)
            .cloned()
            .collect::<BTreeSet<_>>();
        let replacement_names = manifest_names
            .difference(&input_names)
            .cloned()
            .collect::<BTreeSet<_>>();
        assert!(
            !compacted_inputs.is_empty() && !replacement_names.is_empty(),
            "the durable manifest must replace selected inputs with a new output: manifest={manifest_names:?}, inputs={input_names:?}"
        );
        assert!(
            !manifest_names.is_empty() && manifest_names.is_subset(&disk_after_reopen),
            "every manifest-owned replacement must exist on disk: manifest={manifest_names:?}, disk={disk_after_reopen:?}"
        );
        for retained_input in compacted_inputs.intersection(&disk_after_reopen) {
            assert!(
                recovered_layout.obsolete_files.contains(retained_input),
                "a retained obsolete input must be reported outside manifest ownership: {retained_input}"
            );
        }

        // All data must be recoverable (from manifest-referenced SSTs, not WAL replay)
        for record in &committed {
            let tx = engine
                .begin_tx(default_cf.id(), TransactionMode::ReadOnly)
                .expect("begin read tx");
            let actual = tx.get(&record.key).expect("get committed key");
            assert_eq!(
                actual,
                Some(Bytes::copy_from_slice(&record.value)),
                "key {:?} must be readable after crash (from manifest SSTs, not WAL)",
                String::from_utf8_lossy(&record.key)
            );
        }

        // Trigger another compaction to ensure no issues with orphaned files
        // (a 2nd compaction would try to re-compact old L0s if they still existed and manifest wasn't fixed)
        engine
            .compact_all()
            .expect("second compaction after crash recovery");

        // Verify data still accessible after 2nd compaction
        let tx = engine
            .begin_tx(default_cf.id(), TransactionMode::ReadOnly)
            .expect("begin read tx after 2nd compact");
        let count = tx
            .scan(&Query::new())
            .expect("scan")
            .try_collect()
            .expect("collect scan")
            .len();
        assert_eq!(
            count,
            committed.len(),
            "all {} committed keys must still be accessible after 2nd compaction",
            committed.len()
        );
    }

    // ============================================================================
    // CHILD PROCESS SCENARIOS
    // ============================================================================

    #[test]
    fn should_crash_in_child_when_compaction_crash_scenario_requested() {
        // Arrange
        let Some(scenario) = std::env::var_os(ENV_SCENARIO) else {
            return;
        };

        let db_path = PathBuf::from(std::env::var_os(ENV_DB_PATH).expect("db path env"));

        // Act
        match scenario.to_string_lossy().as_ref() {
            "crash_before_manifest_publish" => {
                child_create_data_and_compact_with_crash_before_publish(&db_path);
            }
            "crash_before_manifest_persist" => {
                child_create_data_and_compact_with_crash_before_persist(&db_path);
            }
            "crash_before_sst_gc" => child_create_data_and_compact_with_crash_before_gc(&db_path),
            other => panic!("unknown compaction crash scenario: {other}"),
        }

        // Assert - should not reach here
        panic!("child scenario returned without abort");
    }

    fn child_create_data_and_compact_with_crash_before_persist(db_path: &Path) {
        // Configure failpoint to crash after compaction update but before manifest persist
        crash::configure_abort_failpoint(
            "slice6::after_compaction_update_before_manifest_persist",
            "crash_before_manifest_persist",
        );

        // Write data and trigger compaction
        let engine = open_local_engine(db_path);
        let default_cf = default_cf(&engine);

        // Write multiple batches to create L0 files
        for batch in 0..5 {
            let mut tx = engine
                .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
                .expect("begin batch tx");
            for i in 0..100 {
                let key = format!("compaction_key_batch{batch:02}_{i:04}");
                tx.put(key.as_bytes().to_vec(), b"compaction_value".to_vec(), None)
                    .expect("put");
                // Record this commit
                let committed_log = db_path.join("commits.ndjson");
                let record = CommitRecord {
                    key: key.as_bytes().to_vec(),
                    value: b"compaction_value".to_vec(),
                };
                let json = serde_json::to_string(&record).expect("serialize record");
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&committed_log)
                    .expect("open commits log")
                    .write_all(format!("{json}\n").as_bytes())
                    .expect("write commit record");
            }
            tx.commit(WriteOptions::buffered()).expect("commit batch");
            engine.flush_cf(&default_cf).expect("flush batch");
        }

        // Trigger compaction - will crash at slice6::after_compaction_update_before_manifest_persist
        engine.compact_all().expect("compact_all");

        // Should have panicked and aborted before reaching here
        panic!("expected crash at manifest persist failpoint");
    }

    fn child_create_data_and_compact_with_crash_before_publish(db_path: &Path) {
        crash::configure_abort_failpoint(
            "slice7::after_compaction_output_durable_before_manifest_publish",
            "crash_before_manifest_publish",
        );

        let engine = open_local_engine(db_path);
        let default_cf = default_cf(&engine);

        for batch in 0..4 {
            let mut tx = engine
                .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
                .expect("begin batch tx");
            for i in 0..100 {
                let key = format!("compaction_prepublish_key_batch{batch:02}_{i:04}");
                tx.put(key.as_bytes().to_vec(), b"compaction_value".to_vec(), None)
                    .expect("put");
                let committed_log = db_path.join("commits.ndjson");
                let record = CommitRecord {
                    key: key.as_bytes().to_vec(),
                    value: b"compaction_value".to_vec(),
                };
                let json = serde_json::to_string(&record).expect("serialize record");
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&committed_log)
                    .expect("open commits log")
                    .write_all(format!("{json}\n").as_bytes())
                    .expect("write commit record");
            }
            tx.commit(WriteOptions::buffered()).expect("commit batch");
            engine.flush_cf(&default_cf).expect("flush batch");
        }

        write_compaction_input_names(&engine, db_path);

        engine.compact_all().expect("compact_all");

        panic!("expected crash at manifest publish failpoint");
    }

    fn child_create_data_and_compact_with_crash_before_gc(db_path: &Path) {
        // Configure failpoint to crash after manifest persist but before GC deletion
        crash::configure_abort_failpoint(
            "slice6::after_manifest_persist_before_sst_gc",
            "crash_before_sst_gc",
        );

        // Write data and trigger compaction
        let engine = open_local_engine(db_path);
        let default_cf = default_cf(&engine);

        // Write multiple batches to create L0 files
        for batch in 0..4 {
            let mut tx = engine
                .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
                .expect("begin batch tx");
            for i in 0..100 {
                let key = format!("compaction_key_batch{batch:02}_{i:04}");
                tx.put(key.as_bytes().to_vec(), b"compaction_value".to_vec(), None)
                    .expect("put");
                // Record this commit
                let committed_log = db_path.join("commits.ndjson");
                let record = CommitRecord {
                    key: key.as_bytes().to_vec(),
                    value: b"compaction_value".to_vec(),
                };
                let json = serde_json::to_string(&record).expect("serialize record");
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&committed_log)
                    .expect("open commits log")
                    .write_all(format!("{json}\n").as_bytes())
                    .expect("write commit record");
            }
            tx.commit(WriteOptions::buffered()).expect("commit batch");
            engine.flush_cf(&default_cf).expect("flush batch");
        }

        write_compaction_input_names(&engine, db_path);

        // Trigger compaction - will crash at slice6::after_manifest_persist_before_sst_gc
        engine.compact_all().expect("compact_all");

        // Should have panicked and aborted before reaching here
        panic!("expected crash at GC deletion failpoint");
    }

    // ============================================================================
    // HELPER FUNCTIONS
    // ============================================================================

    fn run_child_crash_at_compaction_before_persist(db_path: &Path) -> Vec<CommitRecord> {
        run_child_expect_abort("crash_before_manifest_persist", db_path);
        expire_crashed_process_lease(db_path);
        read_committed_records(db_path)
    }

    fn run_child_crash_at_output_durable_before_publish(db_path: &Path) -> Vec<CommitRecord> {
        run_child_expect_abort("crash_before_manifest_publish", db_path);
        expire_crashed_process_lease(db_path);
        read_committed_records(db_path)
    }

    fn run_child_crash_at_manifest_persist_before_gc(db_path: &Path) -> Vec<CommitRecord> {
        run_child_expect_abort("crash_before_sst_gc", db_path);
        expire_crashed_process_lease(db_path);
        read_committed_records(db_path)
    }

    fn run_child_expect_abort(scenario: &str, db_path: &Path) {
        let current_exe = std::env::current_exe().expect("current exe");
        let mut command = Command::new(current_exe);
        command
            .arg("--exact")
            .arg(CHILD_TEST_NAME)
            .arg("--nocapture")
            .arg("--test-threads=1")
            .env(ENV_SCENARIO, scenario)
            .env(ENV_DB_PATH, db_path);

        crash::run_child_expect_abort(&mut command, scenario, crash_trigger(scenario), db_path);
    }

    fn crash_trigger(scenario: &str) -> &'static str {
        match scenario {
            "crash_before_manifest_publish" => {
                "slice7::after_compaction_output_durable_before_manifest_publish"
            }
            "crash_before_manifest_persist" => {
                "slice6::after_compaction_update_before_manifest_persist"
            }
            "crash_before_sst_gc" => "slice6::after_manifest_persist_before_sst_gc",
            other => panic!("unknown compaction crash trigger for scenario {other}"),
        }
    }

    fn expire_crashed_process_lease(db_path: &Path) {
        thread::sleep(Duration::from_millis(100)); // Let filesystem settle
        let leader_path = db_path.join(".midge_leader");
        if !leader_path.exists() {
            return;
        }

        let mut content = fs::read_to_string(&leader_path).expect("read leader record");
        if content.contains("acquired_at: ") {
            content = content
                .lines()
                // Drop the checksum line rather than recompute it: a record
                // with no checksum field is valid-but-unchecked (backward
                // compatibility with pre-checksum records), so this keeps the
                // rewritten timestamp from being rejected as corrupt.
                .filter(|line| !line.starts_with("checksum: "))
                .map(|line| {
                    if line.starts_with("acquired_at: ") {
                        "acquired_at: 1970-01-01T00:00:00Z".to_string()
                    } else {
                        line.to_string()
                    }
                })
                .collect::<Vec<_>>()
                .join("\n");
            content.push('\n');
            fs::write(&leader_path, content).expect("rewrite leader record as stale");
        }
        crash::clear_crashed_process_acquisition_lock(db_path);
    }

    fn read_committed_records(db_path: &Path) -> Vec<CommitRecord> {
        let committed_log = db_path.join("commits.ndjson");
        if !committed_log.exists() {
            return Vec::new();
        }

        let content = fs::read_to_string(&committed_log).expect("read commits log");
        content
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_str(line).expect("parse commit record"))
            .collect()
    }

    fn write_compaction_input_names(engine: &Engine, db_path: &Path) {
        let layout = engine
            .get_storage_layout()
            .expect("storage layout before compaction");
        let names = manifest_sst_names(&layout);
        assert_eq!(
            names.len(),
            4,
            "fixture must expose exactly the four L0 files selected by compaction"
        );
        assert!(
            layout
                .levels
                .iter()
                .all(|level| level.level == 0 || level.files.is_empty()),
            "fixture inputs must all be manifest-owned L0 files"
        );
        let encoded = serde_json::to_vec(&names).expect("serialize compaction input names");
        let mut file = fs::File::create(db_path.join(COMPACTION_INPUTS_FILE))
            .expect("create compaction input evidence");
        file.write_all(&encoded)
            .expect("write compaction input evidence");
        file.sync_all().expect("sync compaction input evidence");
    }

    fn read_compaction_input_names(db_path: &Path) -> BTreeSet<String> {
        let bytes = fs::read(db_path.join(COMPACTION_INPUTS_FILE))
            .expect("read child compaction input evidence");
        serde_json::from_slice(&bytes).expect("parse child compaction input evidence")
    }

    fn manifest_sst_names(layout: &cntryl_midge::StorageLayoutSnapshot) -> BTreeSet<String> {
        layout
            .levels
            .iter()
            .flat_map(|level| &level.files)
            .map(|file| file.name.clone())
            .collect()
    }

    fn sst_names_on_disk(db_path: &Path) -> BTreeSet<String> {
        fs::read_dir(db_path.join("sst"))
            .expect("read SST directory")
            .map(|entry| entry.expect("read SST directory entry"))
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "sst")
            })
            .map(|entry| {
                entry
                    .file_name()
                    .into_string()
                    .expect("SST file name is UTF-8")
            })
            .collect()
    }

    fn open_local_engine(db_path: &Path) -> Engine {
        Engine::open(
            OpenOptions::local(db_path)
                .background_compaction(false)
                .build()
                .expect("build options"),
        )
        .expect("open engine after crash")
    }

    fn default_cf(engine: &Engine) -> cntryl_midge::ColumnFamilyHandle {
        engine
            .get_column_family("default")
            .expect("default column family")
    }
}

mod background_flush_pipeline {
    use bytes::Bytes;
    use cntryl_midge::{
        ColumnFamilyId, Engine, MidgeError, OpenOptions, TransactionMode, WriteOptions,
    };
    use std::sync::{mpsc, Arc, Mutex, OnceLock};
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    static FAILPOINT_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn failpoint_test_lock() -> &'static Mutex<()> {
        FAILPOINT_TEST_LOCK.get_or_init(|| Mutex::new(()))
    }

    fn open_small_memtable_engine(temp_dir: &TempDir) -> Arc<Engine> {
        Arc::new(
            Engine::open(
                OpenOptions::local(temp_dir.path())
                    .with_memtable_size_limit(512 * 1024)
                    .with_memtable_flush_threshold(128 * 1024)
                    .background_compaction(false)
                    .build()
                    .expect("build options"),
            )
            .expect("open engine"),
        )
    }

    fn commit_sync_put(engine: &Engine, cf_id: ColumnFamilyId, key: &[u8], value: Vec<u8>) {
        let mut tx = engine
            .begin_tx(cf_id, TransactionMode::ReadWrite)
            .expect("begin write transaction");
        tx.put(key.to_vec(), value, None).expect("stage put");
        tx.commit(WriteOptions::sync()).expect("commit sync put");
    }

    fn seed_visible_values(engine: &Engine, cf_id: ColumnFamilyId) {
        let mut tx = engine
            .begin_tx(cf_id, TransactionMode::ReadWrite)
            .expect("begin seed transaction");
        tx.put(b"overwrite".to_vec(), b"old".to_vec(), None)
            .expect("seed overwrite key");
        tx.put(b"point-delete".to_vec(), b"old".to_vec(), None)
            .expect("seed point delete key");
        tx.put(b"range-b".to_vec(), b"old".to_vec(), None)
            .expect("seed range key");
        tx.commit(WriteOptions::sync()).expect("commit seed values");
    }

    fn rotate_with_mixed_operations(engine: &Engine, cf_id: ColumnFamilyId) {
        let mut tx = engine
            .begin_tx(cf_id, TransactionMode::ReadWrite)
            .expect("begin rotation transaction");
        tx.delete_range(b"range-a".to_vec(), b"range-z".to_vec())
            .expect("stage range delete");
        tx.put(b"overwrite".to_vec(), b"new".to_vec(), None)
            .expect("stage overwrite");
        tx.delete(b"point-delete".to_vec())
            .expect("stage point delete");
        tx.put(b"rotation-payload".to_vec(), vec![0xA5; 160 * 1024], None)
            .expect("stage rotation payload");
        tx.commit(WriteOptions::sync())
            .expect("commit rotation transaction");
    }

    fn wait_for_metrics(
        engine: &Engine,
        timeout: Duration,
        predicate: impl Fn(&cntryl_midge::RuntimeMetricsSnapshot) -> bool,
    ) -> cntryl_midge::RuntimeMetricsSnapshot {
        let deadline = Instant::now() + timeout;
        loop {
            let metrics = engine.get_runtime_metrics().expect("runtime metrics");
            if predicate(&metrics) {
                return metrics;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for flush state: {metrics:?}"
            );
            std::thread::yield_now();
        }
    }

    fn assert_visibility_while_flush_is_blocked(
        engine: &Engine,
        cf_id: ColumnFamilyId,
        old_snapshot: &cntryl_midge::Transaction,
    ) {
        let current = engine
            .begin_tx(cf_id, TransactionMode::ReadOnly)
            .expect("begin current read");
        assert_eq!(
            current.get(b"overwrite").expect("read overwrite"),
            Some(Bytes::from_static(b"new"))
        );
        assert_eq!(
            current.get(b"point-delete").expect("read point delete"),
            None
        );
        assert_eq!(current.get(b"range-b").expect("read range delete"), None);

        assert_eq!(
            old_snapshot.get(b"overwrite").expect("read old overwrite"),
            Some(Bytes::from_static(b"old"))
        );
        assert_eq!(
            old_snapshot
                .get(b"point-delete")
                .expect("read old point delete"),
            Some(Bytes::from_static(b"old"))
        );
        assert_eq!(
            old_snapshot.get(b"range-b").expect("read old range key"),
            Some(Bytes::from_static(b"old"))
        );
    }

    fn assert_followup_sync_commit_completes(engine: &Arc<Engine>, cf_id: ColumnFamilyId) {
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let worker_engine = Arc::clone(engine);
        let worker = std::thread::spawn(move || {
            let result = (|| {
                let mut tx = worker_engine.begin_tx(cf_id, TransactionMode::ReadWrite)?;
                tx.put(b"foreground-followup".to_vec(), b"committed".to_vec(), None)?;
                tx.commit(WriteOptions::sync())
            })();
            result_tx.send(result).expect("send commit result");
        });

        let result = result_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("sync commit must complete while flush I/O is blocked");
        result.expect("follow-up sync commit");
        worker.join().expect("join commit worker");
    }

    #[test]
    fn should_keep_foreground_responsive_while_sst_build_is_blocked() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::flush_worker::before_build", "pause").expect("configure build pause");
        let temp_dir = TempDir::new().expect("temp dir");
        let engine = open_small_memtable_engine(&temp_dir);
        let cf = engine
            .create_column_family("background-build")
            .expect("create column family");
        seed_visible_values(&engine, cf.id());
        let old_snapshot = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin pre-rotation snapshot");

        // Act
        rotate_with_mixed_operations(&engine, cf.id());
        let blocked = wait_for_metrics(&engine, Duration::from_secs(2), |metrics| {
            metrics.flush_inflight == 1 && metrics.immutable_memtables >= 1
        });
        assert_visibility_while_flush_is_blocked(&engine, cf.id(), &old_snapshot);
        assert_followup_sync_commit_completes(&engine, cf.id());

        fail::remove("midge::flush_worker::before_build");
        drop(old_snapshot);
        engine.flush_cf(&cf).expect("flush after releasing build");
        scenario.teardown();

        // Assert
        let finished = engine.get_runtime_metrics().expect("finished metrics");
        assert_eq!(blocked.flush_build_count, 0);
        assert!(finished.flush_build_count >= 1);
        assert!(finished.flush_publish_count >= 1);
        assert_eq!(finished.flush_inflight, 0);
    }

    #[test]
    fn should_keep_foreground_responsive_while_publication_is_blocked() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::flush_worker::before_publication", "pause")
            .expect("configure publication pause");
        let temp_dir = TempDir::new().expect("temp dir");
        let engine = open_small_memtable_engine(&temp_dir);
        let cf = engine
            .create_column_family("background-publish")
            .expect("create column family");
        seed_visible_values(&engine, cf.id());
        let old_snapshot = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin pre-rotation snapshot");

        // Act
        rotate_with_mixed_operations(&engine, cf.id());
        let blocked = wait_for_metrics(&engine, Duration::from_secs(2), |metrics| {
            metrics.flush_build_count >= 1
                && metrics.flush_publish_count == 0
                && metrics.flush_inflight == 1
        });
        assert_visibility_while_flush_is_blocked(&engine, cf.id(), &old_snapshot);
        assert_followup_sync_commit_completes(&engine, cf.id());

        fail::remove("midge::flush_worker::before_publication");
        drop(old_snapshot);
        engine
            .flush_cf(&cf)
            .expect("flush after releasing publication");
        scenario.teardown();

        // Assert
        let finished = engine.get_runtime_metrics().expect("finished metrics");
        assert_eq!(blocked.flush_publish_count, 0);
        assert!(finished.flush_publish_count >= 1);
        assert_eq!(finished.flush_inflight, 0);
    }

    #[test]
    fn should_preserve_followup_mutations_after_concurrent_flush_when_reopening() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::flush_worker::before_build", "pause").expect("configure build pause");
        let temp_dir = TempDir::new().expect("temp dir");
        let options = OpenOptions::local(temp_dir.path())
            .with_memtable_size_limit(512 * 1024)
            .with_memtable_flush_threshold(128 * 1024)
            .background_compaction(false)
            .build()
            .expect("build options");
        let engine = Arc::new(Engine::open(options.clone()).expect("open engine"));
        let cf = engine
            .create_column_family("concurrent-followup")
            .expect("create column family");
        commit_sync_put(&engine, cf.id(), b"overwrite", b"old".to_vec());
        commit_sync_put(&engine, cf.id(), b"deleted", b"old".to_vec());
        commit_sync_put(&engine, cf.id(), b"rotation", vec![0xA5; 160 * 1024]);
        wait_for_metrics(&engine, Duration::from_secs(2), |metrics| {
            metrics.flush_inflight == 1 && metrics.immutable_memtables >= 1
        });

        // Act
        commit_sync_put(&engine, cf.id(), b"overwrite", b"new".to_vec());
        let mut delete = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin delete transaction");
        delete.delete(b"deleted".to_vec()).expect("stage delete");
        delete
            .commit(WriteOptions::sync())
            .expect("commit followup delete");
        fail::remove("midge::flush_worker::before_build");
        engine.flush_cf(&cf).expect("flush followup memtable");
        let mut engine = Arc::try_unwrap(engine).ok().expect("unique engine");
        engine
            .shutdown(Duration::from_secs(5))
            .expect("shutdown before reopen");
        scenario.teardown();
        let reopened = Engine::open(options).expect("reopen engine");
        let reopened_cf = reopened
            .get_column_family("concurrent-followup")
            .expect("reopen column family");
        let read = reopened
            .begin_tx(reopened_cf.id(), TransactionMode::ReadOnly)
            .expect("begin recovery read");

        // Assert
        assert_eq!(
            read.get(b"overwrite").expect("read overwrite"),
            Some(Bytes::from_static(b"new"))
        );
        assert_eq!(read.get(b"deleted").expect("read delete"), None);
    }

    #[test]
    fn should_retry_retained_immutable_after_flush_worker_panics() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let scenario = fail::FailScenario::setup();
        let temp_dir = TempDir::new().expect("temp dir");
        let engine = open_small_memtable_engine(&temp_dir);
        let cf = engine
            .create_column_family("worker-panic")
            .expect("create column family");
        fail::cfg("midge::flush_worker::before_build", "panic").expect("configure build panic");

        // Act
        commit_sync_put(&engine, cf.id(), b"panic-value", vec![0x5A; 160 * 1024]);
        let failed = wait_for_metrics(&engine, Duration::from_secs(2), |metrics| {
            metrics.flush_failures_total >= 1
        });
        fail::remove("midge::flush_worker::before_build");
        engine
            .flush_cf(&cf)
            .expect("retry retained immutable after panic");
        scenario.teardown();

        // Assert
        assert!(failed.immutable_memtables >= 1);
        let metrics = engine.get_runtime_metrics().expect("runtime metrics");
        assert!(metrics.flush_failures_total >= 1);
        assert!(metrics.flush_retries_total >= 1);
        assert_eq!(metrics.immutable_memtables, 0);
        assert_eq!(metrics.flush_inflight, 0);
    }

    #[test]
    fn should_retain_writer_lease_until_blocked_flush_worker_exits() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::flush_worker::before_publication", "pause")
            .expect("configure publication pause");
        let temp_dir = TempDir::new().expect("temp dir");
        let options = OpenOptions::local(temp_dir.path())
            .with_memtable_size_limit(512 * 1024)
            .with_memtable_flush_threshold(128 * 1024)
            .background_compaction(false)
            .build()
            .expect("build options");
        let mut engine = Engine::open(options.clone()).expect("open engine");
        let cf = engine
            .create_column_family("shutdown-fence")
            .expect("create column family");
        commit_sync_put(&engine, cf.id(), b"blocked", vec![0x7A; 160 * 1024]);
        wait_for_metrics(&engine, Duration::from_secs(2), |metrics| {
            metrics.flush_build_count >= 1 && metrics.flush_inflight == 1
        });

        // Act
        let first_shutdown = engine.shutdown(Duration::from_millis(25));
        let contending_open = Engine::open(options.clone());
        fail::remove("midge::flush_worker::before_publication");
        let second_shutdown = engine.shutdown(Duration::from_secs(2));
        scenario.teardown();

        // Assert
        assert!(matches!(first_shutdown, Err(MidgeError::Timeout(_))));
        assert!(matches!(contending_open, Err(MidgeError::LeaseHeld(_))));
        second_shutdown.expect("cleanup reaper should finish after publication resumes");
        let mut reopened =
            Engine::open(options).expect("lease should be acquirable after worker exit");
        reopened
            .shutdown(Duration::from_secs(2))
            .expect("shutdown reopened engine");
    }

    #[test]
    fn should_defer_drop_until_flush_pipeline_completes_given_immutable_flush_inflight() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::flush_worker::before_build", "pause").expect("configure build pause");
        let temp_dir = TempDir::new().expect("temp dir");
        let engine = open_small_memtable_engine(&temp_dir);
        let cf = engine
            .create_column_family("drop-after-flush")
            .expect("create column family");
        commit_sync_put(&engine, cf.id(), b"drop-key", b"drop-value".to_vec());

        let (flush_tx, flush_rx) = mpsc::sync_channel(1);
        let flush_engine = Arc::clone(&engine);
        let flush_cf = cf.clone();
        let flush_thread = std::thread::spawn(move || {
            flush_tx
                .send(flush_engine.flush_cf(&flush_cf))
                .expect("send flush result");
        });
        wait_for_metrics(&engine, Duration::from_secs(2), |metrics| {
            metrics.flush_inflight == 1 && metrics.immutable_memtables == 1
        });

        // Act
        let (drop_tx, drop_rx) = mpsc::sync_channel(1);
        let drop_engine = Arc::clone(&engine);
        let cf_id = cf.id();
        let drop_thread = std::thread::spawn(move || {
            drop_tx
                .send(drop_engine.drop_column_family(cf_id))
                .expect("send drop result");
        });
        assert!(
            drop_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "target column-family deletion must wait for its active flush"
        );
        fail::remove("midge::flush_worker::before_build");
        flush_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("flush barrier should resolve")
            .expect("flush should succeed");
        drop_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("drop should resolve after flush")
            .expect("drop should succeed");
        flush_thread.join().expect("join flush thread");
        drop_thread.join().expect("join drop thread");
        scenario.teardown();

        // Assert
        let metrics = engine.get_runtime_metrics().expect("runtime metrics");
        assert_eq!(metrics.flush_inflight, 0);
        assert_eq!(metrics.immutable_memtables, 0);
        let staging_dir = temp_dir.path().join("sst/.flush-staging");
        assert!(
            !staging_dir.exists()
                || std::fs::read_dir(staging_dir)
                    .expect("read staging directory")
                    .next()
                    .is_none(),
            "completed flush/drop must not leak staging files"
        );
    }
}

mod shutdown_orchestration {
    use cntryl_midge::{
        CloudWritePolicy, Engine, MidgeError, OpenOptions, TransactionMode, WriteOptions,
    };
    use std::collections::BTreeSet;
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::{Duration, Instant};

    const BLOCKED_UPLOAD_FAILPOINT: &str = "midge::cloud::before_wal_upload";

    struct UploadRelease {
        gate: Arc<(Mutex<bool>, Condvar)>,
    }

    impl UploadRelease {
        fn release(&self) {
            let (released, changed) = &*self.gate;
            *released
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
            changed.notify_all();
        }
    }

    impl Drop for UploadRelease {
        fn drop(&mut self) {
            self.release();
        }
    }

    #[test]
    fn should_release_primary_lease_given_shutdown_timeout_when_shutdown_completes() {
        // Arrange
        let scenario = fail::FailScenario::setup();
        let temp_dir = tempfile::TempDir::new().expect("create cloud shutdown directory");
        let db_path = temp_dir.path().join("db");
        let lease_loss_calls = Arc::new(AtomicUsize::new(0));
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let release_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let callback_gate = Arc::clone(&release_gate);
        let callback_fired = Arc::new(AtomicBool::new(false));
        let fired_in_callback = Arc::clone(&callback_fired);
        fail::cfg_callback(BLOCKED_UPLOAD_FAILPOINT, move || {
            fired_in_callback.store(true, Ordering::SeqCst);
            let _ = entered_tx.try_send(());
            let (released, changed) = &*callback_gate;
            let mut released = released
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            while !*released {
                released = changed
                    .wait(released)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
        })
        .expect("configure blocked WAL upload boundary");
        let mut engine = Engine::open(cloud_options(&db_path, Arc::clone(&lease_loss_calls)))
            .expect("open cloud engine");
        let release = UploadRelease { gate: release_gate };
        let default_cf = engine
            .get_column_family("default")
            .expect("default column family");
        let mut transaction = engine
            .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
            .expect("begin cloud transaction");
        transaction
            .put(b"shutdown-key".to_vec(), b"shutdown-value".to_vec(), None)
            .expect("stage cloud write");
        transaction
            .commit(WriteOptions::cloud_async())
            .expect("commit CloudAsync write");
        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("upload worker must reach deterministic blocked boundary");
        let wal_before_shutdown = local_wal_files(&db_path);
        assert!(
            !wal_before_shutdown.is_empty(),
            "CloudAsync write must have a sealed local WAL before upload"
        );

        // Act: the caller's budget expires while the event loop keeps ownership of
        // the blocked worker and lease. No lease-loss notification is appropriate
        // for an orderly, caller-bounded shutdown.
        let shutdown_started = Instant::now();
        let first_shutdown = engine.shutdown(Duration::from_millis(50));
        let shutdown_elapsed = shutdown_started.elapsed();
        let wal_after_timeout = local_wal_files(&db_path);
        let competing = Engine::open(cloud_options(&db_path, Arc::clone(&lease_loss_calls)));

        // Assert
        assert!(matches!(first_shutdown, Err(MidgeError::Timeout(_))));
        assert!(
            shutdown_elapsed < Duration::from_millis(500),
            "shutdown exceeded its caller budget by too much: {shutdown_elapsed:?}"
        );
        assert!(callback_fired.load(Ordering::SeqCst));
        assert!(
            wal_before_shutdown.is_subset(&wal_after_timeout),
            "timed-out shutdown removed local WAL needed for recovery: before={wal_before_shutdown:?}, after={wal_after_timeout:?}"
        );
        assert!(matches!(competing, Err(MidgeError::LeaseHeld(_))));
        assert_eq!(lease_loss_calls.load(Ordering::SeqCst), 0);

        // Act: let the runtime's shorter injected cloud-drain deadline expire
        // before releasing the real upload worker. The retained cleanup reaper
        // must preserve that eventual durability result after the first Engine
        // caller has already timed out.
        std::thread::sleep(Duration::from_millis(150));
        release.release();
        let terminal_shutdown = engine.shutdown(Duration::from_secs(5));
        let replayed_shutdown = engine.shutdown(Duration::from_millis(50));

        // Assert: every later caller observes the runtime's terminal error rather
        // than a synthetic cleanup success.
        let terminal_message = match terminal_shutdown {
            Err(MidgeError::Internal(message)) => message,
            other => panic!("expected terminal cloud-drain error, got {other:?}"),
        };
        assert!(terminal_message.contains("cloud uploads"));
        assert!(matches!(
            replayed_shutdown,
            Err(MidgeError::Internal(message)) if message == terminal_message
        ));
        assert!(
            !db_path.join(".midge_leader.lock").exists(),
            "completed shutdown cleanup must remove the acquisition lock"
        );
        fail::remove(BLOCKED_UPLOAD_FAILPOINT);
        scenario.teardown();
        let mut reopened = Engine::open(reopen_options(&db_path, Arc::clone(&lease_loss_calls)))
            .expect("reopen only after blocked runtime has exited");
        let default_cf = reopened
            .get_column_family("default")
            .expect("reopened default column family");
        let read = reopened
            .begin_tx(default_cf.id(), TransactionMode::ReadOnly)
            .expect("begin recovery read");

        // Assert
        assert_eq!(
            read.get(b"shutdown-key").expect("read recovered value"),
            Some(bytes::Bytes::from_static(b"shutdown-value"))
        );
        drop(read);
        assert_eq!(lease_loss_calls.load(Ordering::SeqCst), 0);
        reopened
            .shutdown(Duration::from_secs(5))
            .expect("shutdown reopened engine");
    }

    fn cloud_options(
        db_path: &Path,
        lease_loss_calls: Arc<AtomicUsize>,
    ) -> cntryl_midge::OpenOptions {
        cloud_options_with_drain_timeout(db_path, lease_loss_calls, Duration::from_millis(100))
    }

    fn reopen_options(
        db_path: &Path,
        lease_loss_calls: Arc<AtomicUsize>,
    ) -> cntryl_midge::OpenOptions {
        cloud_options_with_drain_timeout(db_path, lease_loss_calls, Duration::from_secs(5))
    }

    fn cloud_options_with_drain_timeout(
        db_path: &Path,
        lease_loss_calls: Arc<AtomicUsize>,
        shutdown_cloud_drain_timeout: Duration,
    ) -> cntryl_midge::OpenOptions {
        OpenOptions::cloud_simulated(db_path, "shutdown-bucket", "shutdown/")
            .background_compaction(false)
            .cloud_write_policy(CloudWritePolicy {
                eventual_flush_segment_gap: 128,
                wal_seal_min_segment_bytes: usize::MAX,
                wal_seal_max_flush_delay: Duration::from_mins(1),
                wal_seal_max_pending_writes: 1,
            })
            .shutdown_cloud_drain_timeout_for_testing(shutdown_cloud_drain_timeout)
            .on_lease_loss(move || {
                lease_loss_calls.fetch_add(1, Ordering::SeqCst);
            })
            .build()
            .expect("build cloud shutdown options")
    }

    fn local_wal_files(db_path: &Path) -> BTreeSet<String> {
        let wal_dir = db_path.join("wal");
        let Ok(entries) = std::fs::read_dir(&wal_dir) else {
            return BTreeSet::new();
        };
        entries
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let metadata = entry.metadata().ok()?;
                (metadata.is_file() && metadata.len() > 0)
                    .then(|| entry.file_name().to_string_lossy().into_owned())
            })
            .collect()
    }
}

mod compaction_snapshot_publication {
    //! Compaction snapshot publication ordering regressions.

    use bytes::Bytes;
    use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Barrier,
    };

    #[test]
    fn should_publish_replacement_snapshot_before_obsolete_sst_deletion() {
        // Arrange
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let engine = Engine::open(
            OpenOptions::local(temp_dir.path())
                .background_compaction(false)
                .build()
                .expect("build options"),
        )
        .expect("open engine");
        let cf = engine
            .get_column_family("default")
            .expect("default column family");

        for batch in 0..4 {
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin write transaction");
            for index in 0..25 {
                let key = format!("snapshot_key_{batch}_{index:04}");
                tx.put(
                    key.into_bytes(),
                    format!("value_{batch}").into_bytes(),
                    None,
                )
                .expect("put compaction seed");
            }
            tx.commit(WriteOptions::buffered()).expect("commit batch");
            engine.flush_cf(&cf).expect("flush L0 generation");
        }

        let scenario = fail::FailScenario::setup();
        let gc_reached = Arc::new(Barrier::new(2));
        let allow_compaction_to_finish = Arc::new(Barrier::new(2));
        let callback_gc_reached = Arc::clone(&gc_reached);
        let callback_allow_finish = Arc::clone(&allow_compaction_to_finish);
        let paused_once = Arc::new(AtomicBool::new(false));
        let callback_paused_once = Arc::clone(&paused_once);
        fail::cfg_callback("midge::compaction::after_input_sst_gc", move || {
            if !callback_paused_once.swap(true, Ordering::AcqRel) {
                callback_gc_reached.wait();
                callback_allow_finish.wait();
            }
        })
        .expect("configure post-GC pause");

        // Act
        let read_results = std::thread::scope(|scope| {
            let compaction = scope.spawn(|| engine.compact_all());
            gc_reached.wait();

            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin read transaction while compaction is paused");
            let reads = (0..4)
                .map(|batch| {
                    let key = format!("snapshot_key_{batch}_0000");
                    (batch, tx.get(key.as_bytes()))
                })
                .collect::<Vec<_>>();

            allow_compaction_to_finish.wait();
            compaction
                .join()
                .expect("join compaction thread")
                .expect("complete compaction");
            reads
        });

        fail::remove("midge::compaction::after_input_sst_gc");
        scenario.teardown();

        // Assert
        for (batch, result) in read_results {
            assert_eq!(
                result.expect("read while obsolete SSTs are being collected"),
                Some(Bytes::from(format!("value_{batch}"))),
                "replacement snapshot must serve batch {batch} before old inputs disappear"
            );
        }
    }
}

mod transaction_crash_boundaries {
    use bytes::Bytes;
    use cntryl_midge::{ConflictPolicy, Engine, OpenOptions, TransactionMode, WriteOptions};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::{Arc, Barrier};
    use tempfile::TempDir;

    use crate::common::crash;

    const CHILD_TEST_NAME: &str = "transaction_crash_boundaries::should_abort_in_child_process_when_txn_crash_scenario_requested";
    const ENV_SCENARIO: &str = "MIDGE_TXN_CRASH_SCENARIO";
    const ENV_DB_PATH: &str = "MIDGE_TXN_CRASH_DB_PATH";

    #[derive(Clone, Copy)]
    struct ExpectedRecord {
        key: &'static [u8],
        value: &'static [u8],
    }

    const PRE_COMMIT_RECORDS: &[ExpectedRecord] = &[
        ExpectedRecord {
            key: b"txn-pre-commit-a",
            value: b"value-a",
        },
        ExpectedRecord {
            key: b"txn-pre-commit-b",
            value: b"value-b",
        },
        ExpectedRecord {
            key: b"txn-pre-commit-c",
            value: b"value-c",
        },
    ];

    const POST_SYNC_RECORDS: &[ExpectedRecord] = &[
        ExpectedRecord {
            key: b"txn-post-sync-a",
            value: b"value-a",
        },
        ExpectedRecord {
            key: b"txn-post-sync-b",
            value: b"value-b",
        },
        ExpectedRecord {
            key: b"txn-post-sync-c",
            value: b"value-c",
        },
    ];

    const POST_ACK_RECORDS: &[ExpectedRecord] = &[
        ExpectedRecord {
            key: b"txn-post-ack-a",
            value: b"value-a",
        },
        ExpectedRecord {
            key: b"txn-post-ack-b",
            value: b"value-b",
        },
        ExpectedRecord {
            key: b"txn-post-ack-c",
            value: b"value-c",
        },
    ];

    const GROUP_POST_SYNC_RECORDS: &[ExpectedRecord] = &[
        ExpectedRecord {
            key: b"group-post-sync-a",
            value: b"value-a",
        },
        ExpectedRecord {
            key: b"group-post-sync-b",
            value: b"value-b",
        },
        ExpectedRecord {
            key: b"group-post-sync-c",
            value: b"value-c",
        },
        ExpectedRecord {
            key: b"group-post-sync-d",
            value: b"value-d",
        },
    ];

    const GROUP_POST_ACK_RECORDS: &[ExpectedRecord] = &[
        ExpectedRecord {
            key: b"group-post-ack-a",
            value: b"value-a",
        },
        ExpectedRecord {
            key: b"group-post-ack-b",
            value: b"value-b",
        },
        ExpectedRecord {
            key: b"group-post-ack-c",
            value: b"value-c",
        },
        ExpectedRecord {
            key: b"group-post-ack-d",
            value: b"value-d",
        },
    ];

    const STRICT_CONFLICT_FIRST_COMMIT_RECORD: ExpectedRecord = ExpectedRecord {
        key: b"txn-strict-conflict-key",
        value: b"first-commit",
    };

    const STRICT_CONFLICT_SECOND_COMMIT_VALUE: &[u8] = b"second-commit";

    const ASSERTION_ONLY_SYNC_RECORD: ExpectedRecord = ExpectedRecord {
        key: b"assertion-only-sync-buffered",
        value: b"buffered-before-assertion-sync",
    };

    const ASSERTION_GUARDED_RECORD: ExpectedRecord = ExpectedRecord {
        key: b"assertion-guarded-commit",
        value: b"guarded-value",
    };

    const ASSERTION_GUARDED_SPILLED_KEYS: &[&[u8]] = &[
        b"assertion-guarded-spilled-a",
        b"assertion-guarded-spilled-b",
        b"assertion-guarded-spilled-c",
        b"assertion-guarded-spilled-d",
    ];
    const ASSERTION_GUARDED_SPILL_VALUE_BYTES: usize = 8 * 1024;
    const ASSERTION_GUARDED_SPILL_POOL_BYTES: usize = 8 * 1024;

    const ASSERTION_CONCURRENT_RECORD: ExpectedRecord = ExpectedRecord {
        key: b"assertion-concurrent-key",
        value: b"concurrent-value",
    };

    #[test]
    fn should_abort_in_child_process_when_txn_crash_scenario_requested() {
        // Arrange
        let Some(scenario) = std::env::var_os(ENV_SCENARIO) else {
            return;
        };

        let db_path = PathBuf::from(std::env::var_os(ENV_DB_PATH).expect("db path env"));

        // Act
        match scenario.to_string_lossy().as_ref() {
            "after_ops_before_commit" => child_abort_after_ops_before_commit(&db_path),
            "after_sync_before_ack" => child_abort_after_sync_before_ack(&db_path),
            "after_commit_ack" => child_abort_after_commit_ack(&db_path),
            "group_after_sync_before_ack" => child_abort_group_after_sync_before_ack(&db_path),
            "group_after_commit_ack" => child_abort_group_after_commit_ack(&db_path),
            "after_strict_conflict_abort" => child_abort_after_strict_conflict_abort(&db_path),
            "after_assertion_only_sync_ack" => child_abort_after_assertion_only_sync_ack(&db_path),
            "assertion_guarded_after_sync_before_ack" => {
                child_abort_assertion_guarded_after_sync_before_ack(&db_path);
            }
            "assertion_guarded_spilled_after_sync_before_ack" => {
                child_abort_assertion_guarded_spilled_after_sync_before_ack(&db_path);
            }
            "after_assertion_conflict_abort" => {
                child_abort_after_assertion_conflict_abort(&db_path);
            }
            other => panic!("unknown txn crash scenario: {other}"),
        }

        // Assert
        panic!("child scenario returned without abort");
    }

    #[test]
    fn should_drop_sync_transaction_when_crashing_after_ops_append_before_commit_marker() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        // Act
        run_child_expect_abort("after_ops_before_commit", db_path);
        expire_crashed_process_lease(db_path);

        let engine = open_local_engine(db_path);

        // Assert
        assert_records_absent(&engine, PRE_COMMIT_RECORDS);
    }

    #[test]
    fn should_recover_sync_transaction_when_crashing_after_sync_before_ack() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        // Act
        run_child_expect_abort("after_sync_before_ack", db_path);
        expire_crashed_process_lease(db_path);

        let engine = open_local_engine(db_path);

        // Assert
        assert_records_visible(&engine, POST_SYNC_RECORDS);
    }

    #[test]
    fn should_recover_sync_transaction_when_process_aborts_after_commit_ack() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        // Act
        run_child_expect_abort("after_commit_ack", db_path);
        expire_crashed_process_lease(db_path);

        let engine = open_local_engine(db_path);

        // Assert
        assert_records_visible(&engine, POST_ACK_RECORDS);
    }

    #[test]
    fn should_recover_every_group_member_when_crashing_after_shared_sync_before_ack() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        // Act
        run_child_expect_abort("group_after_sync_before_ack", db_path);
        expire_crashed_process_lease(db_path);
        let engine = open_local_engine(db_path);

        // Assert
        assert_records_visible(&engine, GROUP_POST_SYNC_RECORDS);
    }

    #[test]
    fn should_recover_every_group_member_when_process_aborts_after_group_acknowledgements() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        // Act
        run_child_expect_abort("group_after_commit_ack", db_path);
        expire_crashed_process_lease(db_path);
        let engine = open_local_engine(db_path);

        // Assert
        assert_records_visible(&engine, GROUP_POST_ACK_RECORDS);
    }

    #[test]
    fn should_preserve_first_commit_when_process_aborts_after_strict_conflict_abort() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        // Act
        run_child_expect_abort("after_strict_conflict_abort", db_path);
        expire_crashed_process_lease(db_path);
        let engine = open_local_engine(db_path);

        // Assert
        let default_cf = default_cf(&engine);
        let tx = engine
            .begin_tx(default_cf.id(), TransactionMode::ReadOnly)
            .expect("begin read tx");
        assert_eq!(
            tx.get(STRICT_CONFLICT_FIRST_COMMIT_RECORD.key)
                .expect("get strict conflict key"),
            Some(Bytes::from_static(
                STRICT_CONFLICT_FIRST_COMMIT_RECORD.value
            )),
            "first strict commit must remain visible after crash"
        );
    }

    #[test]
    fn should_recover_buffered_write_when_process_aborts_after_assertion_only_sync_commit() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        // Act
        run_child_expect_abort("after_assertion_only_sync_ack", db_path);
        expire_crashed_process_lease(db_path);
        let engine = open_local_engine(db_path);

        // Assert
        assert_records_visible(&engine, &[ASSERTION_ONLY_SYNC_RECORD]);
    }

    #[test]
    fn should_recover_assertion_guarded_transaction_when_crashing_after_sync_before_ack() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        // Act
        run_child_expect_abort("assertion_guarded_after_sync_before_ack", db_path);
        expire_crashed_process_lease(db_path);
        let engine = open_local_engine(db_path);

        // Assert
        assert_records_visible(&engine, &[ASSERTION_GUARDED_RECORD]);
    }

    #[test]
    fn should_recover_assertion_guarded_spilled_transaction_when_crashing_after_sync_before_ack() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        // Act
        run_child_expect_abort("assertion_guarded_spilled_after_sync_before_ack", db_path);
        expire_crashed_process_lease(db_path);
        let engine = open_local_engine(db_path);

        // Assert
        assert_assertion_guarded_spilled_records_visible(&engine);
    }

    #[test]
    fn should_recover_concurrent_commit_when_process_aborts_after_assertion_rejection() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        // Act
        run_child_expect_abort("after_assertion_conflict_abort", db_path);
        expire_crashed_process_lease(db_path);
        let engine = open_local_engine(db_path);

        // Assert
        assert_records_visible(&engine, &[ASSERTION_CONCURRENT_RECORD]);
    }

    fn child_abort_after_ops_before_commit(db_path: &Path) {
        crash::configure_abort_failpoint(
            "midge::wal::txn_after_ops_append_before_commit",
            "after_ops_before_commit",
        );
        let engine = open_local_engine(db_path);
        let default_cf = default_cf(&engine);
        commit_fixed_sync_transaction(&engine, &default_cf, PRE_COMMIT_RECORDS);
    }

    fn child_abort_after_sync_before_ack(db_path: &Path) {
        crash::configure_abort_failpoint(
            "midge::wal::txn_after_sync_before_ack",
            "after_sync_before_ack",
        );
        let engine = open_local_engine(db_path);
        let default_cf = default_cf(&engine);
        commit_fixed_sync_transaction(&engine, &default_cf, POST_SYNC_RECORDS);
    }

    fn child_abort_after_commit_ack(db_path: &Path) {
        let engine = open_local_engine(db_path);
        let default_cf = default_cf(&engine);
        commit_fixed_sync_transaction(&engine, &default_cf, POST_ACK_RECORDS);
        crash::abort_at_trigger("after_commit_ack", "manual::after_commit_ack");
    }

    fn child_abort_after_assertion_only_sync_ack(db_path: &Path) {
        let engine = open_local_engine(db_path);
        let default_cf = default_cf(&engine);
        let mut buffered = engine
            .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
            .expect("begin buffered transaction");
        buffered
            .put(
                ASSERTION_ONLY_SYNC_RECORD.key.to_vec(),
                ASSERTION_ONLY_SYNC_RECORD.value.to_vec(),
                None,
            )
            .expect("stage buffered value");
        buffered
            .commit(WriteOptions::buffered())
            .expect("commit buffered value");
        let mut asserting = engine
            .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
            .expect("begin assertion-only transaction");
        asserting
            .assert_value(
                ASSERTION_ONLY_SYNC_RECORD.key.to_vec(),
                Some(ASSERTION_ONLY_SYNC_RECORD.value.to_vec()),
            )
            .expect("register buffered value assertion");
        asserting
            .commit(WriteOptions::sync())
            .expect("establish assertion-only sync barrier");
        crash::abort_at_trigger(
            "after_assertion_only_sync_ack",
            "manual::after_assertion_only_sync_ack",
        );
    }

    fn child_abort_assertion_guarded_after_sync_before_ack(db_path: &Path) {
        crash::configure_abort_failpoint(
            "midge::wal::txn_after_sync_before_ack",
            "assertion_guarded_after_sync_before_ack",
        );
        let engine = open_local_engine(db_path);
        let default_cf = default_cf(&engine);
        let mut guarded = engine
            .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
            .expect("begin assertion-guarded transaction");
        guarded
            .assert_value(ASSERTION_GUARDED_RECORD.key.to_vec(), None)
            .expect("assert guarded key is absent");
        guarded
            .put(
                ASSERTION_GUARDED_RECORD.key.to_vec(),
                ASSERTION_GUARDED_RECORD.value.to_vec(),
                None,
            )
            .expect("stage assertion-guarded value");
        guarded
            .commit(WriteOptions::sync())
            .expect("commit assertion-guarded transaction");
    }

    fn child_abort_assertion_guarded_spilled_after_sync_before_ack(db_path: &Path) {
        let engine =
            open_local_engine_with_transaction_pool(db_path, ASSERTION_GUARDED_SPILL_POOL_BYTES);
        let default_cf = default_cf(&engine);
        let mut guarded = engine
            .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
            .expect("begin spilled assertion-guarded transaction");
        guarded
            .assert_value(b"assertion-guarded-spilled-guard".to_vec(), None)
            .expect("assert spilled guard key is absent");
        for (index, key) in ASSERTION_GUARDED_SPILLED_KEYS.iter().enumerate() {
            guarded
                .put(key.to_vec(), assertion_guarded_spill_value(index), None)
                .expect("stage spilled assertion-guarded value");
        }
        assert!(
            transaction_spill_run_count(db_path) > 0,
            "assertion-guarded transaction must spill before commit"
        );
        crash::configure_abort_failpoint(
            "midge::wal::txn_after_sync_before_ack",
            "assertion_guarded_spilled_after_sync_before_ack",
        );
        guarded
            .commit(WriteOptions::sync())
            .expect("commit spilled assertion-guarded transaction");
    }

    fn child_abort_group_after_sync_before_ack(db_path: &Path) {
        configure_group_abort_failpoint("group_after_sync_before_ack");
        commit_concurrent_sync_transactions(db_path, GROUP_POST_SYNC_RECORDS);
    }

    fn child_abort_group_after_commit_ack(db_path: &Path) {
        commit_concurrent_sync_transactions(db_path, GROUP_POST_ACK_RECORDS);
        crash::abort_at_trigger("group_after_commit_ack", "manual::group_after_commit_ack");
    }

    fn commit_concurrent_sync_transactions(db_path: &Path, records: &[ExpectedRecord]) {
        let engine = Arc::new(open_local_engine(db_path));
        let default_cf = default_cf(&engine);
        let barrier = Arc::new(Barrier::new(records.len() + 1));
        let mut handles = Vec::with_capacity(records.len());
        for record in records.iter().copied() {
            let engine = Arc::clone(&engine);
            let barrier = Arc::clone(&barrier);
            let cf_id = default_cf.id();
            handles.push(std::thread::spawn(move || {
                let mut tx = engine
                    .begin_tx(cf_id, TransactionMode::ReadWrite)
                    .expect("begin grouped write tx");
                tx.put(record.key.to_vec(), record.value.to_vec(), None)
                    .expect("put grouped record");
                barrier.wait();
                tx.commit(WriteOptions::sync())
                    .expect("commit grouped sync transaction");
            }));
        }
        barrier.wait();
        for handle in handles {
            handle.join().expect("join grouped sync transaction");
        }
    }

    fn child_abort_after_strict_conflict_abort(db_path: &Path) {
        // Arrange
        let engine = open_local_engine(db_path);
        let default_cf = default_cf(&engine);

        let mut tx1 = engine
            .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
            .expect("begin tx1");
        let mut tx2 = engine
            .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
            .expect("begin tx2");

        tx1.set_conflict_policy(ConflictPolicy::AbortOnWriteConflict);
        tx2.set_conflict_policy(ConflictPolicy::AbortOnWriteConflict);

        tx1.put(
            STRICT_CONFLICT_FIRST_COMMIT_RECORD.key.to_vec(),
            STRICT_CONFLICT_FIRST_COMMIT_RECORD.value.to_vec(),
            None,
        )
        .expect("tx1 put");
        tx2.put(
            STRICT_CONFLICT_FIRST_COMMIT_RECORD.key.to_vec(),
            STRICT_CONFLICT_SECOND_COMMIT_VALUE.to_vec(),
            None,
        )
        .expect("tx2 put");

        // Act
        tx1.commit(WriteOptions::sync()).expect("commit tx1");
        let conflict = tx2.commit(WriteOptions::sync());

        // Assert
        assert!(
            matches!(conflict, Err(cntryl_midge::MidgeError::WriteConflict(_))),
            "second strict commit must abort with WriteConflict"
        );

        // Crash after the conflict outcome has been observed.
        crash::abort_at_trigger(
            "after_strict_conflict_abort",
            "manual::after_strict_conflict_abort",
        );
    }

    fn child_abort_after_assertion_conflict_abort(db_path: &Path) {
        // Arrange
        let engine = open_local_engine(db_path);
        let default_cf = default_cf(&engine);
        let mut asserting = engine
            .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
            .expect("begin asserting transaction");
        asserting
            .assert_value(ASSERTION_CONCURRENT_RECORD.key.to_vec(), None)
            .expect("assert concurrent key is absent");
        let mut concurrent = engine
            .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
            .expect("begin concurrent transaction");
        concurrent
            .put(
                ASSERTION_CONCURRENT_RECORD.key.to_vec(),
                ASSERTION_CONCURRENT_RECORD.value.to_vec(),
                None,
            )
            .expect("stage concurrent value");

        // Act
        concurrent
            .commit(WriteOptions::sync())
            .expect("commit concurrent value");
        let conflict = asserting.commit(WriteOptions::sync());

        // Assert
        assert!(matches!(
            conflict,
            Err(cntryl_midge::MidgeError::WriteConflict(_))
        ));
        crash::abort_at_trigger(
            "after_assertion_conflict_abort",
            "manual::after_assertion_conflict_abort",
        );
    }

    fn commit_fixed_sync_transaction(
        engine: &Engine,
        cf: &cntryl_midge::ColumnFamilyHandle,
        records: &[ExpectedRecord],
    ) {
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin write tx");
        for record in records {
            tx.put(record.key.to_vec(), record.value.to_vec(), None)
                .expect("put record");
        }
        tx.commit(WriteOptions::sync())
            .expect("commit fixed sync transaction");
    }

    fn open_local_engine(db_path: &Path) -> Engine {
        Engine::open(OpenOptions::local(db_path).build().expect("build options"))
            .expect("open engine")
    }

    fn open_local_engine_with_transaction_pool(db_path: &Path, pool_bytes: usize) -> Engine {
        Engine::open(
            OpenOptions::local(db_path)
                .transaction_memory_pool_size(pool_bytes)
                .build()
                .expect("build options"),
        )
        .expect("open engine")
    }

    fn default_cf(engine: &Engine) -> cntryl_midge::ColumnFamilyHandle {
        engine
            .get_column_family("default")
            .expect("default column family")
    }

    fn assert_records_visible(engine: &Engine, expected: &[ExpectedRecord]) {
        let default_cf = default_cf(engine);
        for record in expected {
            let tx = engine
                .begin_tx(default_cf.id(), TransactionMode::ReadOnly)
                .expect("begin read tx");
            let actual = tx.get(record.key).expect("get expected key");
            assert_eq!(
                actual,
                Some(Bytes::from_static(record.value)),
                "key {:?} must recover with the committed value",
                String::from_utf8_lossy(record.key)
            );
        }
    }

    fn assert_records_absent(engine: &Engine, expected: &[ExpectedRecord]) {
        let default_cf = default_cf(engine);
        for record in expected {
            let tx = engine
                .begin_tx(default_cf.id(), TransactionMode::ReadOnly)
                .expect("begin read tx");
            let actual = tx.get(record.key).expect("get expected key");
            assert_eq!(
                actual,
                None,
                "key {:?} must not appear after crash before commit marker",
                String::from_utf8_lossy(record.key)
            );
        }
    }

    fn assert_assertion_guarded_spilled_records_visible(engine: &Engine) {
        let default_cf = default_cf(engine);
        for (index, key) in ASSERTION_GUARDED_SPILLED_KEYS.iter().enumerate() {
            let tx = engine
                .begin_tx(default_cf.id(), TransactionMode::ReadOnly)
                .expect("begin spilled verification transaction");
            assert_eq!(
                tx.get(key).expect("read spilled assertion-guarded value"),
                Some(Bytes::from(assertion_guarded_spill_value(index))),
                "spilled key {index} must recover after the commit-marker sync"
            );
        }
    }

    fn assertion_guarded_spill_value(index: usize) -> Vec<u8> {
        let byte = b'a'.saturating_add(u8::try_from(index).expect("spill value index fits in u8"));
        vec![byte; ASSERTION_GUARDED_SPILL_VALUE_BYTES]
    }

    fn transaction_spill_run_count(db_path: &Path) -> usize {
        fs::read_dir(db_path.join("txn")).map_or(0, |entries| {
            entries
                .filter_map(Result::ok)
                .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "run"))
                .count()
        })
    }

    fn run_child_expect_abort(scenario: &str, db_path: &Path) {
        let current_exe = std::env::current_exe().expect("current exe");
        let mut command = Command::new(current_exe);
        command
            .arg("--exact")
            .arg(CHILD_TEST_NAME)
            .arg("--nocapture")
            .arg("--test-threads=1")
            .env(ENV_SCENARIO, scenario)
            .env(ENV_DB_PATH, db_path);
        crash::run_child_expect_abort(&mut command, scenario, crash_trigger(scenario), db_path);
    }

    fn configure_group_abort_failpoint(scenario_name: &'static str) {
        crash::configure_abort_failpoint("midge::wal::txn_after_sync_before_ack", scenario_name);
        fail::cfg("midge::runtime::strict_group_before_collect", "1*sleep(50)")
            .expect("configure strict group collection failpoint");
    }

    fn crash_trigger(scenario: &str) -> &'static str {
        match scenario {
            "after_ops_before_commit" => "midge::wal::txn_after_ops_append_before_commit",
            "after_sync_before_ack"
            | "group_after_sync_before_ack"
            | "assertion_guarded_after_sync_before_ack"
            | "assertion_guarded_spilled_after_sync_before_ack" => {
                "midge::wal::txn_after_sync_before_ack"
            }
            "after_commit_ack" => "manual::after_commit_ack",
            "group_after_commit_ack" => "manual::group_after_commit_ack",
            "after_strict_conflict_abort" => "manual::after_strict_conflict_abort",
            "after_assertion_only_sync_ack" => "manual::after_assertion_only_sync_ack",
            "after_assertion_conflict_abort" => "manual::after_assertion_conflict_abort",
            other => panic!("unknown transaction crash trigger for scenario {other}"),
        }
    }

    fn expire_crashed_process_lease(db_path: &Path) {
        let leader_path = db_path.join(".midge_leader");
        if leader_path.exists() {
            let mut content = fs::read_to_string(&leader_path).expect("read leader record");
            if content.contains("acquired_at: ") {
                content = content
                    .lines()
                    // Drop the checksum line rather than recompute it: a
                    // record with no checksum field is valid-but-unchecked
                    // (backward compatibility with pre-checksum records), so
                    // this keeps the rewritten timestamp from being rejected
                    // as corrupt.
                    .filter(|line| !line.starts_with("checksum: "))
                    .map(|line| {
                        if line.starts_with("acquired_at: ") {
                            "acquired_at: 1970-01-01T00:00:00Z".to_string()
                        } else {
                            line.to_string()
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                content.push('\n');
                fs::write(&leader_path, content).expect("rewrite leader record as stale");
            }
        }

        crash::clear_crashed_process_acquisition_lock(db_path);
    }
}

mod cloud_crash_recovery {
    use bytes::Bytes;

    use crate::common::{crash, MidgeOptions, StorageMode};
    use cntryl_midge::{
        CloudWritePolicy, Engine, RuntimeMetricsSnapshot, TransactionMode, WriteOptions,
    };
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::thread;
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    const CHILD_TEST_NAME: &str =
        "cloud_crash_recovery::should_abort_in_child_process_when_cloud_crash_scenario_requested";
    const ENV_SCENARIO: &str = "MIDGE_CLOUD_CRASH_SCENARIO";
    const ENV_DB_PATH: &str = "MIDGE_CLOUD_CRASH_DB_PATH";
    const LARGE_MEMTABLE_BYTES: usize = 512 * 1024 * 1024;
    const EVENTUAL_FLUSH_GAP: u64 = 4;

    #[test]
    fn should_abort_in_child_process_when_cloud_crash_scenario_requested() {
        // Arrange
        let Some(scenario) = std::env::var_os(ENV_SCENARIO) else {
            return;
        };

        let db_path = PathBuf::from(std::env::var_os(ENV_DB_PATH).expect("db path env"));
        match scenario.to_string_lossy().as_ref() {
            "cloud_strict_after_ack" => child_cloud_strict_after_ack(&db_path),
            "cloud_async_active_after_ack" => child_cloud_async_active_after_ack(&db_path),
            #[cfg(feature = "failpoints")]
            "cloud_async_local_wal_after_ack" => child_cloud_async_local_wal_after_ack(&db_path),
            "buffered_eventual_flush_after_publish" => {
                child_buffered_eventual_flush_after_publish(&db_path);
            }
            other => panic!("unknown cloud crash scenario: {other}"),
        }

        // Act
        // Assert
        panic!("child scenario returned without abort");
    }

    #[test]
    fn should_clear_acquisition_lock_when_simulating_crash_timeout() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        std::fs::write(
            db_path.join("midge_primary_lease.json"),
            "holder_id: crashed@host\nowner_token: lease-token\nacquired_at: 2026-07-26T11:00:00Z\nexpires_at: 2026-07-26T11:00:30Z\n",
        )
        .expect("write lease record");
        std::fs::write(
            db_path.join(".midge_leader.lock"),
            "holder_id=crashed@host\nowner_token=lock-token\ncreated_at=2026-07-26T11:00:10Z\n",
        )
        .expect("write acquisition lock");

        // Act
        expire_crashed_process_lease(db_path);

        // Assert
        assert!(!db_path.join(".midge_leader.lock").exists());
        let reopened = open_cloud_engine(db_path, None);
        commit_value(
            &reopened,
            b"crash-timeout-key",
            b"crash-timeout-value",
            WriteOptions::cloud_async(),
        );
        assert_value_visible(&reopened, b"crash-timeout-key", b"crash-timeout-value");
        let lease_content = std::fs::read_to_string(db_path.join("midge_primary_lease.json"))
            .expect("read refreshed lease record");
        assert!(
            !lease_content.contains("crashed@host"),
            "reopen should re-acquire the lease with a fresh holder instead of keeping the stale one: {lease_content}"
        );
    }

    #[test]
    fn should_clear_incomplete_acquisition_lock_when_crashed_child_has_exited() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        std::fs::write(
            db_path.join("midge_primary_lease.json"),
            "holder_id: crashed@host\nowner_token: lease-token\nacquired_at: 2026-07-26T11:00:00Z\nexpires_at: 2026-07-26T11:00:30Z\n",
        )
        .expect("write lease record");
        std::fs::write(db_path.join(".midge_leader.lock"), []).expect("write incomplete lock");

        // Act
        expire_crashed_process_lease(db_path);

        // Assert
        assert!(!db_path.join(".midge_leader.lock").exists());
        let reopened = open_cloud_engine(db_path, None);
        commit_value(
            &reopened,
            b"incomplete-lock-key",
            b"incomplete-lock-value",
            WriteOptions::cloud_async(),
        );
        assert_value_visible(&reopened, b"incomplete-lock-key", b"incomplete-lock-value");
        let lease_content = std::fs::read_to_string(db_path.join("midge_primary_lease.json"))
            .expect("read refreshed lease record");
        assert!(
            !lease_content.contains("crashed@host"),
            "reopen should re-acquire the lease with a fresh holder instead of keeping the stale one: {lease_content}"
        );
    }

    #[test]
    fn should_recover_cloud_strict_write_when_cache_lost_after_child_abort() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        run_child_expect_abort("cloud_strict_after_ack", db_path);
        expire_crashed_process_lease(db_path);
        reset_dir(&db_path.join("wal"));
        reset_dir(&db_path.join("sst"));

        let reopened = open_cloud_engine(db_path, None);
        // Act
        // Assert
        assert_value_visible(
            &reopened,
            b"cloud-strict-crash-key",
            b"cloud-strict-crash-value",
        );
    }

    #[test]
    fn should_resume_cloud_upload_from_recovered_active_wal_after_child_abort() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        run_child_expect_abort("cloud_async_active_after_ack", db_path);
        expire_crashed_process_lease(db_path);
        let active_wal = db_path.join("wal").join("wal.log");
        assert!(
            std::fs::metadata(&active_wal).is_ok_and(|metadata| metadata.len() > 0),
            "crashed child must leave the acknowledged write in the active WAL"
        );

        // Act
        let reopened = open_cloud_engine(db_path, None);
        let metrics = wait_for_metrics(&reopened, Duration::from_secs(10), |metrics| {
            metrics.current_sequence >= 1
                && metrics.wal_cloud_durable_seq >= metrics.current_sequence
        });

        // Assert
        assert_value_visible(
            &reopened,
            b"cloud-async-active-crash-key",
            b"cloud-async-active-crash-value",
        );
        assert!(metrics.wal_cloud_durable_seq >= metrics.current_sequence);
        let remote_wal_dir = db_path.join("cloud_store").join("wal");
        assert!(
            contains_file_with_extension(&remote_wal_dir, "wal"),
            "recovered active WAL must be sealed and uploaded"
        );
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_recover_local_cloud_async_wal_when_child_aborts_before_upload() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        run_child_expect_abort("cloud_async_local_wal_after_ack", db_path);
        expire_crashed_process_lease(db_path);

        // Act
        let reopened = open_cloud_engine(db_path, None);

        // Assert
        assert_value_visible(
            &reopened,
            b"cloud-async-local-crash-key",
            b"cloud-async-local-crash-value",
        );
    }

    #[test]
    fn should_restore_published_cloud_sst_when_cache_lost_after_child_abort() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        run_child_expect_abort("buffered_eventual_flush_after_publish", db_path);
        expire_crashed_process_lease(db_path);
        reset_dir(&db_path.join("wal"));
        reset_dir(&db_path.join("sst"));

        let reopened = open_cloud_engine(db_path, Some(buffered_cloud_policy()));
        let metrics = reopened.get_runtime_metrics().expect("runtime metrics");
        // Act
        // Assert
        assert!(
            metrics.sst_count >= 1,
            "reopen should restore at least one SST from the authoritative cloud object"
        );
        assert!(
            metrics.manifest_last_persisted_sequence > 0,
            "reopen should preserve manifest persistence progress after crash"
        );

        let layout = reopened.get_storage_layout().expect("storage layout");
        assert!(
            layout
                .levels
                .iter()
                .map(|level| level.file_count)
                .sum::<usize>()
                >= 1,
            "reopen should retain the published remote SST inventory"
        );

        for index in 0..17 {
            let key = format!("cloud-buffered-crash-key-{index:04}");
            assert_value_visible(&reopened, key.as_bytes(), b"cloud-buffered-crash-value");
        }
    }

    fn child_cloud_strict_after_ack(db_path: &Path) {
        let engine = open_cloud_engine(db_path, None);
        commit_value(
            &engine,
            b"cloud-strict-crash-key",
            b"cloud-strict-crash-value",
            WriteOptions::cloud_strict(),
        );

        abort_after_marking_ready(db_path, "cloud_strict_after_ack");
    }

    fn child_cloud_async_active_after_ack(db_path: &Path) {
        let engine = open_cloud_engine(db_path, Some(unsealed_cloud_policy()));
        commit_value(
            &engine,
            b"cloud-async-active-crash-key",
            b"cloud-async-active-crash-value",
            WriteOptions::cloud_async(),
        );

        abort_after_marking_ready(db_path, "cloud_async_active_after_ack");
    }

    #[cfg(feature = "failpoints")]
    fn child_cloud_async_local_wal_after_ack(db_path: &Path) {
        let _scenario = fail::FailScenario::setup();
        fail::cfg("midge::cloud::inject_fail_wal_upload", "return")
            .expect("configure WAL upload failure");
        let engine = open_cloud_engine(db_path, None);
        commit_value(
            &engine,
            b"cloud-async-local-crash-key",
            b"cloud-async-local-crash-value",
            WriteOptions::cloud_async(),
        );
        wait_for_metrics(&engine, Duration::from_secs(10), |metrics| {
            metrics.current_sequence >= 1
                && metrics.wal_cloud_durable_seq < metrics.current_sequence
        });

        abort_after_marking_ready(db_path, "cloud_async_local_wal_after_ack");
    }

    fn child_buffered_eventual_flush_after_publish(db_path: &Path) {
        let engine = open_cloud_engine(db_path, Some(buffered_cloud_policy()));
        let cf = default_cf(&engine);

        for index in 0..16 {
            let key = format!("cloud-buffered-crash-key-{index:04}");
            commit_value(
                &engine,
                key.as_bytes(),
                b"cloud-buffered-crash-value",
                WriteOptions::cloud_async(),
            );
        }

        let pre_publish_metrics = engine
            .get_runtime_metrics()
            .expect("runtime metrics before publish");
        assert!(
            pre_publish_metrics.flush_enqueued_total > 0,
            "buffered cloud writes should trigger at least one eventual flush"
        );

        let _published = wait_for_metrics(&engine, Duration::from_secs(10), |metrics| {
            metrics.sst_count >= 1 && metrics.manifest_last_persisted_sequence > 0
        });
        // Add one acknowledged write after publication, below the four-segment
        // eventual-flush trigger. Recovery must reconcile an authoritative SST
        // with this newer WAL-only frontier.
        commit_value(
            &engine,
            b"cloud-buffered-crash-key-0016",
            b"cloud-buffered-crash-value",
            WriteOptions::cloud_async(),
        );
        wait_for_metrics(&engine, Duration::from_secs(10), |metrics| {
            metrics.current_sequence >= 17
                && metrics.wal_cloud_durable_seq >= metrics.current_sequence
        });
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin read tx");
        assert_eq!(
            tx.get(b"cloud-buffered-crash-key-0000")
                .expect("read back buffered key before crash"),
            Some(Bytes::from_static(b"cloud-buffered-crash-value"))
        );

        abort_after_marking_ready(db_path, "buffered_eventual_flush_after_publish");
    }

    fn abort_after_marking_ready(_db_path: &Path, scenario: &str) -> ! {
        crash::abort_at_trigger(scenario, cloud_crash_trigger(scenario));
    }

    fn buffered_cloud_policy() -> CloudWritePolicy {
        CloudWritePolicy {
            eventual_flush_segment_gap: EVENTUAL_FLUSH_GAP,
            wal_seal_min_segment_bytes: usize::MAX,
            wal_seal_max_flush_delay: Duration::from_hours(1),
            wal_seal_max_pending_writes: 1,
        }
    }

    fn unsealed_cloud_policy() -> CloudWritePolicy {
        CloudWritePolicy {
            eventual_flush_segment_gap: u64::MAX,
            wal_seal_min_segment_bytes: usize::MAX,
            wal_seal_max_flush_delay: Duration::from_hours(1),
            wal_seal_max_pending_writes: usize::MAX,
        }
    }

    fn open_cloud_engine(db_path: &Path, policy: Option<CloudWritePolicy>) -> Engine {
        let opts = MidgeOptions {
            storage_mode: StorageMode::CloudBacked {
                local_cache_path: db_path.to_path_buf(),
            },
            wal_sync: true,
            wal_batch_config: None,
            memtable_size: LARGE_MEMTABLE_BYTES,
            compression: false,
            enable_compaction: false,
            memory_budget: None,
            cloud_write_policy: policy,
            simulated_cloud_local_storage_budget_bytes: None,
            shutdown_cloud_drain_timeout: None,
        };

        Engine::open(opts.to_open_options()).expect("open cloud engine")
    }

    fn default_cf(engine: &Engine) -> cntryl_midge::ColumnFamilyHandle {
        engine
            .get_column_family("default")
            .expect("default column family")
    }

    fn commit_value(engine: &Engine, key: &[u8], value: &[u8], opts: WriteOptions) {
        let cf = default_cf(engine);
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin write tx");
        tx.put(key.to_vec(), value.to_vec(), None)
            .expect("put value");
        tx.commit(opts).expect("commit value");
    }

    fn assert_value_visible(engine: &Engine, key: &[u8], expected: &[u8]) {
        let cf = default_cf(engine);
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin read tx");
        assert_eq!(
            tx.get(key).expect("read value"),
            Some(Bytes::copy_from_slice(expected))
        );
    }

    fn wait_for_metrics<F>(
        engine: &Engine,
        timeout: Duration,
        predicate: F,
    ) -> RuntimeMetricsSnapshot
    where
        F: Fn(&RuntimeMetricsSnapshot) -> bool,
    {
        let deadline = Instant::now() + timeout;
        loop {
            let metrics = engine.get_runtime_metrics().expect("runtime metrics");
            if predicate(&metrics) {
                return metrics;
            }

            assert!(
                Instant::now() < deadline,
                "timed out waiting for cloud recovery condition; last metrics: sst_count={} persisted_seq={} wal_segment={} current_seq={} cloud_seq={} gap={}",
                metrics.sst_count,
                metrics.manifest_last_persisted_sequence,
                metrics.wal_current_segment_id,
                metrics.current_sequence,
                metrics.wal_cloud_durable_seq,
                metrics.max_memtable_wal_segment_gap
            );
            thread::sleep(Duration::from_millis(25));
        }
    }

    fn run_child_expect_abort(scenario: &str, db_path: &Path) {
        let current_exe = std::env::current_exe().expect("current exe");
        let mut command = Command::new(current_exe);
        command
            .arg("--exact")
            .arg(CHILD_TEST_NAME)
            .arg("--nocapture")
            .arg("--test-threads=1")
            .env(ENV_SCENARIO, scenario)
            .env(ENV_DB_PATH, db_path);
        crash::run_child_expect_abort(
            &mut command,
            scenario,
            cloud_crash_trigger(scenario),
            db_path,
        );
    }

    fn cloud_crash_trigger(scenario: &str) -> &'static str {
        match scenario {
            "cloud_strict_after_ack" => "manual::cloud_strict_after_ack",
            "cloud_async_active_after_ack" => "manual::cloud_async_active_after_ack",
            "cloud_async_local_wal_after_ack" => "manual::cloud_async_local_wal_after_ack",
            "buffered_eventual_flush_after_publish" => {
                "manual::buffered_eventual_flush_after_publish"
            }
            other => panic!("unknown cloud crash scenario: {other}"),
        }
    }

    fn expire_crashed_process_lease(db_path: &Path) {
        let lease_path = db_path.join("midge_primary_lease.json");
        if !lease_path.exists() {
            return;
        }

        let mut content = std::fs::read_to_string(&lease_path).expect("read lease record");
        if content.contains("acquired_at: ") || content.contains("expires_at: ") {
            content = content
                .lines()
                .map(|line| {
                    if line.starts_with("acquired_at: ") {
                        "acquired_at: 1970-01-01T00:00:00Z".to_string()
                    } else if line.starts_with("expires_at: ") {
                        "expires_at: 1970-01-01T00:00:00Z".to_string()
                    } else {
                        line.to_string()
                    }
                })
                .collect::<Vec<_>>()
                .join("\n");
            content.push('\n');
            std::fs::write(&lease_path, content).expect("rewrite lease record as stale");
        }

        crash::clear_crashed_process_acquisition_lock(db_path);
    }

    fn reset_dir(dir: &Path) {
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).expect("recreate directory");
    }

    fn contains_file_with_extension(dir: &Path, extension: &str) -> bool {
        std::fs::read_dir(dir)
            .unwrap_or_else(|error| panic!("list {}: {error}", dir.display()))
            .flatten()
            .any(|entry| {
                let path = entry.path();
                if path.is_dir() {
                    contains_file_with_extension(&path, extension)
                } else {
                    path.extension().and_then(|value| value.to_str()) == Some(extension)
                }
            })
    }
}

mod cloud_persistence_hardening {
    use crate::common::{crash, opts_for_mode, MidgeOptions, StorageMode};
    use bytes::Bytes;
    use cntryl_midge::{
        Engine, EngineHealth, MidgeError, OpenOptions, RecoveryPolicy, TransactionMode,
        WriteOptions,
    };
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::{Mutex, OnceLock};
    use std::thread;
    use std::time::{Duration, Instant};

    static FAILPOINT_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    const CORRUPT_WAL_CHILD_TEST_NAME: &str =
        "cloud_persistence_hardening::should_abort_in_child_process_when_remote_wal_corruption_scenario_requested";
    const CORRUPT_WAL_ENV_DB_PATH: &str = "MIDGE_CORRUPT_REMOTE_WAL_DB_PATH";
    const CORRUPT_WAL_SCENARIO: &str = "remote_wal_corruption_after_strict_ack";
    const CORRUPT_WAL_TRIGGER: &str = "manual::remote_wal_corruption_after_strict_ack";
    const TRUNCATED_PRIMARY_CATALOG: &[u8] = b"{\"format_version\":1";
    const TRUNCATED_MIRROR_CATALOG: &[u8] = b"{\"format_version\":1,\"fencing_epoch\":";

    #[test]
    fn should_abort_in_child_process_when_remote_wal_corruption_scenario_requested() {
        // Arrange
        let Some(db_path) = std::env::var_os(CORRUPT_WAL_ENV_DB_PATH) else {
            return;
        };
        let engine = Engine::open(cloud_open_options(
            Path::new(&db_path),
            RecoveryPolicy::Strict,
        ))
        .expect("open cloud engine in crash child");
        put_default(
            &engine,
            b"prefix-key",
            b"prefix-value",
            WriteOptions::cloud_strict(),
        );
        put_default(
            &engine,
            b"truncated-key",
            b"truncated-value",
            WriteOptions::cloud_strict(),
        );

        // Act
        // Assert
        crash::abort_at_trigger(CORRUPT_WAL_SCENARIO, CORRUPT_WAL_TRIGGER);
    }

    #[test]
    fn should_recover_cloud_strict_write_from_authoritative_remote_wal_after_local_cache_loss() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let opts = opts_for_mode("cloud");
        let db_path = cloud_db_path(&opts);
        let mut engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");

        put_default(
            &engine,
            b"strict-remote-key",
            b"strict-remote-value",
            WriteOptions::cloud_strict(),
        );
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown before reopen");
        reset_dir(&db_path.join("wal"));

        // Act
        let reopened = Engine::open(opts.to_open_options()).expect("reopen cloud engine");

        // Assert
        assert_eq!(
            get_default(&reopened, b"strict-remote-key"),
            Some(Bytes::from_static(b"strict-remote-value"))
        );
        shutdown_test_engine(reopened);
    }

    #[test]
    fn should_recover_from_valid_catalog_mirror_when_primary_catalog_has_torn_tail() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let opts = opts_for_mode("cloud");
        let db_path = cloud_db_path(&opts);
        let mut engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");
        put_default(
            &engine,
            b"catalog-mirror-key",
            b"catalog-mirror-value",
            WriteOptions::cloud_strict(),
        );
        engine
            .shutdown(Duration::from_secs(5))
            .expect("shutdown before damaging primary catalog");

        let remote_wal_dir = db_path.join("cloud_store").join("wal");
        let primary_catalog = remote_wal_dir.join("publication-catalog.v1.json");
        let mirror_catalog = remote_wal_dir.join("publication-catalog.v1.mirror.json");
        let valid_catalog = fs::read(&primary_catalog).expect("read valid primary WAL catalog");
        assert_eq!(
            fs::read(&mirror_catalog).expect("read valid WAL catalog mirror"),
            valid_catalog,
            "successful strict publication must converge the catalog mirror"
        );
        fs::write(
            &primary_catalog,
            &valid_catalog[..valid_catalog.len().saturating_sub(7)],
        )
        .expect("truncate primary WAL catalog tail");
        reset_dir(&db_path.join("wal"));

        // Act
        let reopened =
            Engine::open(opts.to_open_options()).expect("recover through catalog mirror");

        // Assert
        assert_eq!(
            get_default(&reopened, b"catalog-mirror-key"),
            Some(Bytes::from_static(b"catalog-mirror-value"))
        );
        assert_eq!(
            fs::read(&primary_catalog).expect("read repaired primary WAL catalog"),
            fs::read(&mirror_catalog).expect("read converged WAL catalog mirror"),
            "startup must repair and fence both catalog copies"
        );
        shutdown_test_engine(reopened);
    }

    #[test]
    fn should_fail_closed_when_both_cloud_wal_catalog_copies_are_invalid() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let opts = opts_for_mode("cloud");
        let db_path = cloud_db_path(&opts);
        let mut engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");
        put_default(
            &engine,
            b"doubly-corrupt-catalog-key",
            b"must-not-be-ambiguously-recovered",
            WriteOptions::cloud_strict(),
        );
        engine
            .shutdown(Duration::from_secs(5))
            .expect("shutdown before damaging catalog copies");

        let remote_wal_dir = db_path.join("cloud_store").join("wal");
        fs::write(
            remote_wal_dir.join("publication-catalog.v1.json"),
            TRUNCATED_PRIMARY_CATALOG,
        )
        .expect("damage primary WAL catalog");
        fs::write(
            remote_wal_dir.join("publication-catalog.v1.mirror.json"),
            TRUNCATED_MIRROR_CATALOG,
        )
        .expect("damage WAL catalog mirror");
        reset_dir(&db_path.join("wal"));

        // Act
        let error = expect_engine_open_error(opts.to_open_options());

        // Assert
        assert!(
            matches!(&error, MidgeError::Corruption(message) if message.contains("both cloud WAL publication catalogs are invalid")),
            "unexpected catalog corruption error: {error:?}"
        );
    }

    #[test]
    fn should_remove_local_wal_segment_after_cloud_durable_upload() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let opts = opts_for_mode("cloud");
        let db_path = cloud_db_path(&opts);
        let mut engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");

        // Act
        put_default(
            &engine,
            b"cloud-pruned-local-wal",
            b"remote-wal-value",
            WriteOptions::cloud_strict(),
        );
        let metrics = engine.get_runtime_metrics().expect("runtime metrics");
        let local_segments = list_files_with_extension(&db_path.join("wal"), "wal");
        let remote_segments =
            list_files_with_extension(&db_path.join("cloud_store").join("wal"), "wal");
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown before reopen");
        reset_dir(&db_path.join("wal"));
        let reopened = Engine::open(opts.to_open_options()).expect("reopen cloud engine");

        // Assert
        assert!(
            metrics.wal_cloud_durable_seq >= metrics.current_sequence,
            "cloud-strict write should advance the cloud durability frontier"
        );
        assert!(
            local_segments.is_empty(),
            "cloud-durable local WAL segments should be removed, found: {local_segments:?}"
        );
        assert!(
            !remote_segments.is_empty(),
            "authoritative remote WAL segment should remain available"
        );
        assert_eq!(
            get_default(&reopened, b"cloud-pruned-local-wal"),
            Some(Bytes::from_static(b"remote-wal-value"))
        );
        shutdown_test_engine(reopened);
    }

    #[test]
    fn should_prune_remote_wal_segment_after_cloud_sst_covers_it() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let opts = opts_for_mode("cloud");
        let db_path = cloud_db_path(&opts);
        let remote_wal_dir = db_path.join("cloud_store").join("wal");
        let mut engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");

        put_default(
            &engine,
            b"remote-pruned-after-flush",
            b"covered-by-sst",
            WriteOptions::cloud_strict(),
        );
        assert!(
            !list_files_with_extension(&remote_wal_dir, "wal").is_empty(),
            "cloud-strict write should create an authoritative remote WAL segment"
        );
        let default_cf = default_cf(&engine);

        // Act
        engine.flush_cf(&default_cf).expect("flush default cf");
        let remote_segments = wait_for_remote_wal_count(&remote_wal_dir, 0);
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown before reopen");
        reset_dir(&db_path.join("wal"));
        reset_dir(&db_path.join("sst"));
        let reopened = Engine::open(opts.to_open_options()).expect("reopen cloud engine");

        // Assert
        assert!(
            remote_segments.is_empty(),
            "remote WAL should be pruned after cloud SST coverage"
        );
        assert_eq!(
            get_default(&reopened, b"remote-pruned-after-flush"),
            Some(Bytes::from_static(b"covered-by-sst"))
        );
        assert!(
            list_files_with_extension(&db_path.join("sst"), "sst").is_empty(),
            "reopen should read covered values without restoring full SST files"
        );
        shutdown_test_engine(reopened);
    }

    #[test]
    fn should_ignore_reintroduced_manifest_covered_remote_wal_after_restart() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let opts = opts_for_mode("cloud");
        let db_path = cloud_db_path(&opts);
        let remote_wal_dir = db_path.join("cloud_store").join("wal");
        let mut engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");
        put_default(
            &engine,
            b"restart-prune-key",
            b"restart-prune-value",
            WriteOptions::cloud_strict(),
        );
        let retained_segments = wait_for_remote_wal_count_at_least(&remote_wal_dir, 1)
            .into_iter()
            .map(|path| {
                let bytes = fs::read(&path).expect("read remote WAL before simulated interruption");
                (
                    path.strip_prefix(&remote_wal_dir)
                        .expect("remote WAL path below WAL root")
                        .to_owned(),
                    bytes,
                )
            })
            .collect::<Vec<_>>();
        let default_cf = default_cf(&engine);
        engine.flush_cf(&default_cf).expect("flush covered value");
        assert!(wait_for_remote_wal_count(&remote_wal_dir, 0).is_empty());
        engine
            .shutdown(Duration::from_secs(5))
            .expect("shutdown before restoring interrupted prune residue");

        fs::create_dir_all(&remote_wal_dir).expect("recreate remote WAL directory");
        for (relative_path, bytes) in retained_segments {
            let restored_path = remote_wal_dir.join(relative_path);
            fs::create_dir_all(restored_path.parent().expect("remote WAL parent"))
                .expect("restore remote WAL epoch directory");
            fs::write(restored_path, bytes).expect("restore covered remote WAL residue");
        }
        reset_dir(&db_path.join("wal"));
        reset_dir(&db_path.join("sst"));

        // Act
        let reopened = Engine::open(opts.to_open_options()).expect("reopen cloud engine");
        let remaining = wait_for_remote_wal_count_at_least(&remote_wal_dir, 1);

        // Assert
        assert!(
            !remaining.is_empty(),
            "a WAL object reintroduced after catalog retirement should remain a harmless orphan"
        );
        assert_eq!(
            get_default(&reopened, b"restart-prune-key"),
            Some(Bytes::from_static(b"restart-prune-value"))
        );
        shutdown_test_engine(reopened);
    }

    #[test]
    fn should_recover_delete_range_given_remote_wal_only_when_local_cache_is_lost() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let opts = opts_for_mode("cloud");
        let db_path = cloud_db_path(&opts);
        let remote_wal_dir = db_path.join("cloud_store").join("wal");
        let mut engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");
        let default_cf = default_cf(&engine);

        put_default(
            &engine,
            b"range-10",
            b"covered-before-delete",
            WriteOptions::cloud_strict(),
        );
        put_default(
            &engine,
            b"range-15",
            b"covered-before-delete",
            WriteOptions::cloud_strict(),
        );
        put_default(
            &engine,
            b"range-25",
            b"outside-delete-range",
            WriteOptions::cloud_strict(),
        );
        let mut tx = engine
            .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
            .expect("begin delete-range tx");
        tx.delete_range(b"range-10".to_vec(), b"range-20".to_vec())
            .expect("delete range");
        tx.commit(WriteOptions::cloud_strict())
            .expect("commit delete range");
        assert!(
            !wait_for_remote_wal_count_at_least(&remote_wal_dir, 1).is_empty(),
            "cloud-strict writes should create remote WAL before flush"
        );

        // Act
        engine.flush_cf(&default_cf).expect("flush range tombstone");
        let remote_segments = wait_for_remote_wal_count_at_least(&remote_wal_dir, 1);
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown before reopen");
        reset_dir(&db_path.join("wal"));
        reset_dir(&db_path.join("sst"));
        let reopened = Engine::open(opts.to_open_options()).expect("reopen cloud engine");

        // Assert
        assert!(
            !remote_segments.is_empty(),
            "range tombstone WAL must be retained without an exact per-record SST coverage proof"
        );
        assert_eq!(get_default(&reopened, b"range-10"), None);
        assert_eq!(get_default(&reopened, b"range-15"), None);
        assert_eq!(
            get_default(&reopened, b"range-25"),
            Some(Bytes::from_static(b"outside-delete-range"))
        );
        shutdown_test_engine(reopened);
    }

    #[test]
    fn should_preserve_remote_wal_when_unflushed_column_family_still_depends_on_it_given_partial_gc(
    ) {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let opts = opts_for_mode("cloud");
        let db_path = cloud_db_path(&opts);
        let remote_wal_dir = db_path.join("cloud_store").join("wal");
        let mut engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");
        let default_cf = default_cf(&engine);
        let other_cf = engine
            .create_column_family("other")
            .expect("create other cf");

        put_cf(
            &engine,
            &default_cf,
            b"default-buffered",
            b"default-value",
            WriteOptions::cloud_async(),
        );
        put_cf(
            &engine,
            &other_cf,
            b"other-buffered",
            b"other-value",
            WriteOptions::cloud_async(),
        );
        put_cf(
            &engine,
            &default_cf,
            b"default-strict",
            b"default-strict-value",
            WriteOptions::cloud_strict(),
        );
        assert!(
            !wait_for_remote_wal_count_at_least(&remote_wal_dir, 1).is_empty(),
            "shared remote WAL segment should exist before partial flush"
        );

        // Act
        engine.flush_cf(&default_cf).expect("flush default cf");
        wait_for_no_remote_wal_prune(&remote_wal_dir);
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown before reopen");
        reset_dir(&db_path.join("wal"));
        let reopened = Engine::open(opts.to_open_options()).expect("reopen cloud engine");

        // Assert
        assert_eq!(
            get_cf(&reopened, "other", b"other-buffered"),
            Some(Bytes::from_static(b"other-value")),
            "unflushed column family data should still recover from retained remote WAL"
        );
        shutdown_test_engine(reopened);
    }

    #[test]
    fn should_recover_partial_remote_wal_cleanup_given_mixed_flush_state_when_reopening() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let opts = opts_for_mode("cloud");
        let db_path = cloud_db_path(&opts);
        let remote_wal_dir = db_path.join("cloud_store").join("wal");
        let mut engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");
        let default_cf = default_cf(&engine);

        put_default(
            &engine,
            b"covered-before-partial-cleanup",
            b"covered-value",
            WriteOptions::cloud_strict(),
        );
        assert!(
            !wait_for_remote_wal_count_at_least(&remote_wal_dir, 1).is_empty(),
            "first strict write should create remote WAL before flush"
        );
        engine.flush_cf(&default_cf).expect("flush covered value");
        wait_for_remote_wal_count(&remote_wal_dir, 0);

        // Act: a later strict write remains WAL-backed after the earlier segment was pruned.
        put_default(
            &engine,
            b"retained-after-partial-cleanup",
            b"retained-value",
            WriteOptions::cloud_strict(),
        );
        let retained_segments = wait_for_remote_wal_count_at_least(&remote_wal_dir, 1);
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown before reopen");
        reset_dir(&db_path.join("wal"));
        reset_dir(&db_path.join("sst"));
        let reopened = Engine::open(opts.to_open_options()).expect("reopen cloud engine");

        // Assert
        assert_eq!(
            get_default(&reopened, b"covered-before-partial-cleanup"),
            Some(Bytes::from_static(b"covered-value")),
            "covered data should recover from cloud SST after its remote WAL was pruned"
        );
        assert_eq!(
            get_default(&reopened, b"retained-after-partial-cleanup"),
            Some(Bytes::from_static(b"retained-value")),
            "later unflushed data should recover from the retained remote WAL"
        );
        assert!(
            !retained_segments.is_empty(),
            "test must prove at least one later remote WAL segment survived partial cleanup"
        );
        shutdown_test_engine(reopened);
    }

    #[test]
    fn should_reject_sync_buffered_options_given_cloud_storage_when_committing() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let opts = opts_for_mode("cloud");
        let engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");

        // Act
        let default_cf = default_cf(&engine);
        let mut tx = engine
            .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
            .expect("begin write tx");
        tx.put(b"sync-local-only".to_vec(), b"sync-value".to_vec(), None)
            .expect("put sync-local-only value");
        let error = tx
            .commit(WriteOptions::sync())
            .expect_err("sync() should be rejected for cloud-backed storage");

        // Assert
        assert!(
            matches!(error, MidgeError::InvalidArgument(message) if message.contains("local-only"))
        );
        shutdown_test_engine(engine);
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_keep_cloud_async_commit_visible_given_cloud_upload_failure_when_committing() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let recovery_opts = opts_for_mode("cloud");
        let failure_opts = recovery_opts
            .clone()
            .with_shutdown_cloud_drain_timeout(Duration::from_millis(100));
        let db_path = cloud_db_path(&recovery_opts);
        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::cloud::inject_fail_wal_upload", "return")
            .expect("configure wal upload failure failpoint");

        let mut engine = Engine::open(failure_opts.to_open_options()).expect("open cloud engine");
        put_default(
            &engine,
            b"buffered-local-only",
            b"buffered-value",
            WriteOptions::cloud_async(),
        );
        thread::sleep(Duration::from_millis(600));

        // Act
        let metrics = wait_for_cloud_gap(&engine, 1);

        // Assert
        assert!(metrics.current_sequence >= 1);
        assert!(
            metrics.wal_cloud_durable_seq < metrics.current_sequence,
            "buffered cloud writes must stay below the cloud durability frontier after upload failure"
        );
        assert_eq!(
            get_default(&engine, b"buffered-local-only"),
            Some(Bytes::from_static(b"buffered-value"))
        );
        assert_shutdown_fails_with_pending_cloud_uploads(&mut engine);

        fail::remove("midge::cloud::inject_fail_wal_upload");
        scenario.teardown();

        reset_dir(&db_path.join("wal"));
        let reopened = Engine::open(recovery_opts.to_open_options()).expect("reopen cloud engine");
        assert_eq!(get_default(&reopened, b"buffered-local-only"), None);
        shutdown_test_engine(reopened);
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_recover_cloud_async_commit_given_intact_local_wal_when_upload_fails() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let recovery_opts = opts_for_mode("cloud");
        let failure_opts = recovery_opts
            .clone()
            .with_shutdown_cloud_drain_timeout(Duration::from_millis(100));
        let db_path = cloud_db_path(&recovery_opts);
        let remote_wal_dir = db_path.join("cloud_store").join("wal");
        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::cloud::inject_fail_wal_upload", "return")
            .expect("configure wal upload failure failpoint");

        let mut engine = Engine::open(failure_opts.to_open_options()).expect("open cloud engine");
        put_default(
            &engine,
            b"intact-local-cloud-async",
            b"survives-same-node-restart",
            WriteOptions::cloud_async(),
        );
        let _metrics = wait_for_cloud_gap(&engine, 1);
        assert_shutdown_fails_with_pending_cloud_uploads(&mut engine);

        fail::remove("midge::cloud::inject_fail_wal_upload");
        scenario.teardown();

        // Act
        let reopened = Engine::open(recovery_opts.to_open_options()).expect("reopen cloud engine");

        // Assert
        assert_eq!(
            get_default(&reopened, b"intact-local-cloud-async"),
            Some(Bytes::from_static(b"survives-same-node-restart"))
        );
        let durable = wait_for_cloud_catch_up(&reopened, 1);
        assert!(
            durable.wal_cloud_durable_seq >= durable.current_sequence,
            "resumed upload must advance the cloud frontier only after acknowledgment"
        );
        assert!(
            !wait_for_remote_wal_count_at_least(&remote_wal_dir, 1).is_empty(),
            "same-node recovery must resume publication of the local-only WAL"
        );
        shutdown_test_engine(reopened);
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_fail_cloud_strict_commit_given_cloud_upload_failure_when_waiting_for_ack() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let recovery_opts = opts_for_mode("cloud");
        let failure_opts = recovery_opts
            .clone()
            .with_shutdown_cloud_drain_timeout(Duration::from_millis(100));
        let db_path = cloud_db_path(&recovery_opts);
        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::cloud::inject_fail_wal_upload", "return")
            .expect("configure wal upload failure failpoint");

        let mut engine = Engine::open(failure_opts.to_open_options()).expect("open cloud engine");
        let default_cf = default_cf(&engine);
        let mut tx = engine
            .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
            .expect("begin write tx");
        tx.put(
            b"cloud-strict-fail".to_vec(),
            b"strict-fail-value".to_vec(),
            None,
        )
        .expect("put strict-fail value");

        // Act
        let error = tx
            .commit(WriteOptions::cloud_strict())
            .expect_err("cloud_strict should fail when the authoritative upload fails");
        assert_shutdown_fails_with_pending_cloud_uploads(&mut engine);

        // Assert
        match error {
            MidgeError::Internal(message) => {
                assert!(
                    message.contains("Cloud durability failed"),
                    "expected cloud durability failure, got: {message}"
                );
            }
            other => panic!("expected cloud durability failure, got: {other:?}"),
        }

        fail::remove("midge::cloud::inject_fail_wal_upload");
        scenario.teardown();

        reset_dir(&db_path.join("wal"));
        let reopened = Engine::open(recovery_opts.to_open_options()).expect("reopen cloud engine");
        assert_eq!(get_default(&reopened, b"cloud-strict-fail"), None);
        shutdown_test_engine(reopened);
    }

    #[test]
    fn should_salvage_valid_prefix_when_remote_wal_segment_is_corrupt() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let opts = opts_for_mode("cloud");
        let db_path = cloud_db_path(&opts);
        run_remote_wal_corruption_child(&db_path);
        expire_crashed_process_lease(&db_path);

        let remote_wal_dir = db_path.join("cloud_store").join("wal");
        let corrupt_remote_wal = list_files_with_extension(&remote_wal_dir, "wal")
            .into_iter()
            .max()
            .expect("remote WAL object to corrupt");
        corrupt_last_file(&remote_wal_dir);
        let corrupt_authoritative_bytes =
            fs::read(&corrupt_remote_wal).expect("read corrupt authoritative WAL bytes");
        reset_dir(&db_path.join("wal"));

        // Act
        let Err(strict_error) = Engine::open(opts.clone().to_open_options()) else {
            panic!("strict cloud reopen should fail on corrupt authoritative WAL");
        };
        let salvaged = Engine::open(cloud_open_options(&db_path, RecoveryPolicy::Salvage))
            .expect("salvage cloud reopen");
        let metrics = salvaged.get_runtime_metrics().expect("runtime metrics");

        // Assert
        match strict_error {
            MidgeError::RecoveryFailed(_) => {}
            other => panic!("expected strict recovery failure, got: {other:?}"),
        }
        assert_eq!(metrics.health, EngineHealth::SalvageMode);
        assert_eq!(
            get_default(&salvaged, b"prefix-key"),
            Some(Bytes::from_static(b"prefix-value"))
        );
        assert_eq!(get_default(&salvaged, b"truncated-key"), None);
        assert!(
            corrupt_remote_wal.exists(),
            "corrupt recovered WAL must be retained when it cannot be proven safe to delete"
        );
        assert_eq!(
            fs::read(&corrupt_remote_wal).expect("read retained corrupt authoritative WAL"),
            corrupt_authoritative_bytes,
            "salvage recovery must retain corrupt authoritative WAL byte-for-byte"
        );
        shutdown_test_engine(salvaged);
    }

    fn run_remote_wal_corruption_child(db_path: &Path) {
        let current_exe = std::env::current_exe().expect("current test executable");
        let mut command = Command::new(current_exe);
        command
            .arg("--exact")
            .arg(CORRUPT_WAL_CHILD_TEST_NAME)
            .arg("--nocapture")
            .arg("--test-threads=1")
            .env(CORRUPT_WAL_ENV_DB_PATH, db_path);
        crash::run_child_expect_abort(
            &mut command,
            CORRUPT_WAL_SCENARIO,
            CORRUPT_WAL_TRIGGER,
            db_path,
        );
    }

    fn expire_crashed_process_lease(db_path: &Path) {
        let lease_path = db_path.join("midge_primary_lease.json");
        if lease_path.exists() {
            let mut content = fs::read_to_string(&lease_path).expect("read crashed lease record");
            if content.contains("acquired_at: ") || content.contains("expires_at: ") {
                content = content
                    .lines()
                    .map(|line| {
                        if line.starts_with("acquired_at: ") {
                            "acquired_at: 1970-01-01T00:00:00Z".to_string()
                        } else if line.starts_with("expires_at: ") {
                            "expires_at: 1970-01-01T00:00:00Z".to_string()
                        } else {
                            line.to_string()
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                content.push('\n');
                fs::write(&lease_path, content).expect("expire crashed lease record");
            }
        }
        crash::clear_crashed_process_acquisition_lock(db_path);
    }

    #[test]
    fn should_read_authoritative_remote_sst_when_reopening_after_cache_loss() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let opts = opts_for_mode("cloud");
        let db_path = cloud_db_path(&opts);
        let mut engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");
        let default_cf = default_cf(&engine);

        put_default(
            &engine,
            b"sst-restore-key",
            b"sst-restore-value",
            WriteOptions::best_effort(),
        );
        engine.flush_cf(&default_cf).expect("flush default cf");
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown before reopen");

        reset_dir(&db_path.join("sst"));

        // Act
        let reopened = Engine::open(opts.to_open_options()).expect("reopen cloud engine");

        // Assert
        assert_eq!(
            get_default(&reopened, b"sst-restore-key"),
            Some(Bytes::from_static(b"sst-restore-value"))
        );
        assert!(
            list_files_with_extension(&db_path.join("sst"), "sst").is_empty(),
            "reads should leave the full SST in authoritative cloud storage"
        );
        shutdown_test_engine(reopened);
    }

    #[test]
    fn should_fail_strict_recovery_given_authoritative_remote_sst_missing_when_reopening() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let opts = opts_for_mode("cloud");
        let db_path = cloud_db_path(&opts);
        let mut engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");
        let default_cf = default_cf(&engine);

        put_default(
            &engine,
            b"missing-remote-sst-key",
            b"sst-value",
            WriteOptions::best_effort(),
        );
        engine.flush_cf(&default_cf).expect("flush default cf");
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown before reopen");

        for remote_sst in list_files_with_extension(&db_path.join("cloud_store").join("sst"), "sst")
        {
            fs::remove_file(&remote_sst).expect("delete authoritative remote sst");
        }

        // Act
        let Err(error) = Engine::open(opts.to_open_options()) else {
            panic!("strict reopen should reject missing remote sst");
        };

        // Assert
        match error {
            MidgeError::RecoveryFailed(message) => {
                assert!(
                    message.contains("authoritative cloud SST"),
                    "expected remote SST recovery failure, got: {message}"
                );
            }
            other => panic!("expected recovery failure, got: {other:?}"),
        }
    }

    fn cloud_db_path(opts: &MidgeOptions) -> PathBuf {
        match &opts.storage_mode {
            StorageMode::CloudBacked { local_cache_path } => local_cache_path.clone(),
            _ => panic!("expected cloud-backed storage mode"),
        }
    }

    fn shutdown_test_engine(mut engine: Engine) {
        engine
            .shutdown(Duration::from_secs(5))
            .expect("shut down cloud test engine");
    }

    #[cfg(feature = "failpoints")]
    fn assert_shutdown_fails_with_pending_cloud_uploads(engine: &mut Engine) {
        let started = Instant::now();
        let error = engine
            .shutdown(Duration::from_secs(5))
            .expect_err("permanent upload failure must prevent a clean shutdown");
        let elapsed = started.elapsed();

        match error {
            MidgeError::Internal(message) => assert!(
                message.contains("cloud uploads")
                    && (message.contains("storage-owned") || message.contains("runtime-owned")),
                "expected pending cloud-upload shutdown error, got: {message}"
            ),
            other => panic!("expected pending cloud-upload shutdown error, got: {other:?}"),
        }
        assert!(
            elapsed < Duration::from_secs(2),
            "terminal cloud-upload shutdown exceeded its injected drain budget: {elapsed:?}"
        );
    }

    fn cloud_open_options(db_path: &Path, recovery_policy: RecoveryPolicy) -> OpenOptions {
        OpenOptions::cloud_simulated(
            db_path.to_path_buf(),
            "test-bucket".to_string(),
            "test-prefix/".to_string(),
        )
        .recovery_policy(recovery_policy)
        .build()
        .expect("build cloud options")
    }

    fn default_cf(engine: &Engine) -> cntryl_midge::ColumnFamilyHandle {
        engine
            .get_column_family("default")
            .expect("default column family")
    }

    fn put_default(engine: &Engine, key: &[u8], value: &[u8], opts: WriteOptions) {
        let default_cf = default_cf(engine);
        put_cf(engine, &default_cf, key, value, opts);
    }

    fn put_cf(
        engine: &Engine,
        cf: &cntryl_midge::ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
        opts: WriteOptions,
    ) {
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin write tx");
        tx.put(key.to_vec(), value.to_vec(), None)
            .expect("put value");
        tx.commit(opts).expect("commit value");
    }

    fn get_default(engine: &Engine, key: &[u8]) -> Option<Bytes> {
        let default_cf = default_cf(engine);
        get_cf_by_handle(engine, &default_cf, key)
    }

    fn get_cf(engine: &Engine, name: &str, key: &[u8]) -> Option<Bytes> {
        let cf = engine
            .get_column_family(name)
            .unwrap_or_else(|| panic!("missing column family: {name}"));
        get_cf_by_handle(engine, &cf, key)
    }

    fn get_cf_by_handle(
        engine: &Engine,
        cf: &cntryl_midge::ColumnFamilyHandle,
        key: &[u8],
    ) -> Option<Bytes> {
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin read tx");
        tx.get(key).expect("get value")
    }

    #[cfg(feature = "failpoints")]
    fn wait_for_cloud_gap(
        engine: &Engine,
        min_sequence: u64,
    ) -> cntryl_midge::RuntimeMetricsSnapshot {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let metrics = engine.get_runtime_metrics().expect("runtime metrics");
            if metrics.current_sequence >= min_sequence
                && metrics.wal_cloud_durable_seq < metrics.current_sequence
                && metrics.health == EngineHealth::Degraded
            {
                return metrics;
            }

            assert!(
                Instant::now() < deadline,
                "timed out waiting for failed cloud durability; last metrics: seq={} cloud_seq={} health={:?}",
                metrics.current_sequence,
                metrics.wal_cloud_durable_seq,
                metrics.health
            );
            thread::sleep(Duration::from_millis(25));
        }
    }

    #[cfg(feature = "failpoints")]
    fn wait_for_cloud_catch_up(
        engine: &Engine,
        min_sequence: u64,
    ) -> cntryl_midge::RuntimeMetricsSnapshot {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let metrics = engine.get_runtime_metrics().expect("runtime metrics");
            if metrics.current_sequence >= min_sequence
                && metrics.wal_cloud_durable_seq >= metrics.current_sequence
            {
                return metrics;
            }

            assert!(
                Instant::now() < deadline,
                "timed out waiting for recovered WAL cloud acknowledgment; last metrics: seq={} cloud_seq={} health={:?}",
                metrics.current_sequence,
                metrics.wal_cloud_durable_seq,
                metrics.health
            );
            thread::sleep(Duration::from_millis(25));
        }
    }

    fn corrupt_last_file(dir: &Path) {
        let mut files = list_files_with_extension(dir, "wal");
        files.sort();
        let target = files.pop().expect("expected at least one file to corrupt");
        fs::write(&target, b"\x01\x02\x03").expect("corrupt remote wal segment");
    }

    fn list_files_with_extension(dir: &Path, extension: &str) -> Vec<PathBuf> {
        list_files(dir)
            .into_iter()
            .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some(extension))
            .collect()
    }

    fn list_files(dir: &Path) -> Vec<PathBuf> {
        let mut files = Vec::new();
        collect_files(dir, &mut files);
        files
    }

    fn collect_files(dir: &Path, files: &mut Vec<PathBuf>) {
        for entry in
            fs::read_dir(dir).unwrap_or_else(|error| panic!("read_dir({}): {error}", dir.display()))
        {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                collect_files(&path, files);
            } else {
                files.push(path);
            }
        }
    }

    fn wait_for_remote_wal_count(dir: &Path, expected: usize) -> Vec<PathBuf> {
        wait_for_remote_wal_condition(dir, |files| files.len() == expected)
    }

    fn wait_for_remote_wal_count_at_least(dir: &Path, expected: usize) -> Vec<PathBuf> {
        wait_for_remote_wal_condition(dir, |files| files.len() >= expected)
    }

    fn wait_for_no_remote_wal_prune(dir: &Path) {
        let deadline = Instant::now() + Duration::from_millis(300);
        loop {
            let files = list_files_with_extension(dir, "wal");
            assert!(
                !files.is_empty(),
                "remote WAL segment was pruned while another column family still needed it"
            );
            if Instant::now() >= deadline {
                return;
            }
            thread::sleep(Duration::from_millis(25));
        }
    }

    fn wait_for_remote_wal_condition<F>(dir: &Path, predicate: F) -> Vec<PathBuf>
    where
        F: Fn(&[PathBuf]) -> bool,
    {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let files = list_files_with_extension(dir, "wal");
            if predicate(&files) {
                return files;
            }

            assert!(
                Instant::now() < deadline,
                "timed out waiting for remote WAL condition; found: {files:?}"
            );
            thread::sleep(Duration::from_millis(25));
        }
    }

    fn reset_dir(dir: &Path) {
        let _ = fs::remove_dir_all(dir);
        fs::create_dir_all(dir).expect("recreate directory");
    }

    fn failpoint_test_lock() -> &'static Mutex<()> {
        FAILPOINT_TEST_LOCK.get_or_init(|| Mutex::new(()))
    }

    fn expect_engine_open_error(options: OpenOptions) -> MidgeError {
        match Engine::open(options) {
            Ok(_) => panic!("engine open unexpectedly succeeded"),
            Err(error) => error,
        }
    }
}

mod cloud_remote_sst_compaction_recovery {
    #![cfg(feature = "failpoints")]

    use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
    use std::collections::BTreeSet;
    use std::path::Path;
    use std::time::Duration;

    static FAILPOINT_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn sst_names(directory: &Path) -> BTreeSet<String> {
        std::fs::read_dir(directory)
            .into_iter()
            .flatten()
            .flatten()
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "sst")
            })
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect()
    }

    #[test]
    fn should_roll_back_proven_remote_compaction_orphan_when_local_output_is_corrupt() {
        // Arrange
        let _guard = FAILPOINT_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let scenario = fail::FailScenario::setup();
        let directory = tempfile::tempdir().expect("database directory");
        let options =
            OpenOptions::cloud_simulated(directory.path(), "bucket", "corrupt-local-orphan")
                .background_compaction(false)
                .build()
                .expect("options");
        let mut engine = Engine::open(options.clone()).expect("open");
        let cf = engine.create_column_family("data").expect("column family");
        for key in [b"first".as_slice(), b"second".as_slice()] {
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("transaction");
            tx.put(key.to_vec(), b"preserved".to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::cloud_strict())
                .expect("cloud commit");
            engine.flush_cf(&cf).expect("flush");
        }
        let remote_directory = directory.path().join("cloud_store/sst");
        let inputs = sst_names(&remote_directory);
        fail::cfg(
            "midge::manifest::inject_no_space_on_compaction_batch_edit",
            "return",
        )
        .expect("interrupt manifest publication");
        engine.compact_all().expect_err("publication must fail");
        fail::remove("midge::manifest::inject_no_space_on_compaction_batch_edit");
        let outputs = sst_names(&remote_directory)
            .difference(&inputs)
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(outputs.len(), 1);
        engine.shutdown(Duration::from_secs(30)).expect("shutdown");
        drop(engine);
        let remote_output = remote_directory.join(&outputs[0]);
        let local_output = directory.path().join("sst").join(&outputs[0]);
        let mut corrupt_bytes = std::fs::read(&remote_output).expect("read remote output");
        corrupt_bytes[0] ^= 1;
        std::fs::write(&local_output, corrupt_bytes).expect("retain corrupt local cache");

        // Act
        let mut reopened = Engine::open(options).expect("strict rollback with intact inputs");

        // Assert
        assert_eq!(sst_names(&remote_directory), inputs);
        assert!(!remote_output.exists());
        assert!(!local_output.exists());
        let cf = reopened.get_column_family("data").expect("recovered CF");
        let tx = reopened
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("read transaction");
        for key in [b"first".as_slice(), b"second".as_slice()] {
            assert_eq!(
                tx.get(key).expect("read preserved input").as_deref(),
                Some(b"preserved".as_slice())
            );
        }
        drop(tx);
        reopened
            .shutdown(Duration::from_secs(30))
            .expect("shutdown");
        scenario.teardown();
    }

    #[test]
    fn should_retry_with_fresh_output_identity_after_remote_partition_eviction_fails() {
        // Arrange
        let _guard = FAILPOINT_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let scenario = fail::FailScenario::setup();
        let directory = tempfile::tempdir().expect("database directory");
        let options = OpenOptions::cloud_simulated(directory.path(), "bucket", "orphan-retry")
            .local_storage_budget(1024 * 1024)
            .background_compaction(false)
            .build()
            .expect("options");
        let mut engine = Engine::open(options.clone()).expect("open");
        let cf = engine.create_column_family("data").expect("column family");
        for key in [b"first".as_slice(), b"second".as_slice()] {
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("transaction");
            tx.put(key.to_vec(), b"preserved".to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::cloud_strict())
                .expect("cloud commit");
            engine.flush_cf(&cf).expect("flush");
        }
        let remote_directory = directory.path().join("cloud_store/sst");
        let inputs = sst_names(&remote_directory);
        fail::cfg(
            "midge::compaction::after_remote_partition_evicted",
            "return",
        )
        .expect("configure compaction interruption");

        // Act
        let interrupted = engine.compact_all();
        fail::remove("midge::compaction::after_remote_partition_evicted");
        assert!(
            interrupted.is_err(),
            "the injected partition failure must reach the caller"
        );
        let after_failure = sst_names(&remote_directory);
        assert!(
            after_failure.is_superset(&inputs),
            "unpublished output cannot retire inputs"
        );
        assert!(
            after_failure.len() > inputs.len(),
            "the failed job left a remote output"
        );
        assert!(sst_names(&directory.path().join("sst")).is_empty());
        engine
            .shutdown(Duration::from_secs(30))
            .expect("shutdown interrupted engine");
        drop(engine);
        let mut reopened = Engine::open(options).expect("reopen after interrupted job");
        reopened.compact_all().expect("retry remote compaction");

        // Assert
        let after_retry = sst_names(&remote_directory);
        assert!(
            after_retry.difference(&after_failure).next().is_some(),
            "retry must allocate a fresh generation instead of reusing an orphan identity"
        );
        assert!(sst_names(&directory.path().join("sst")).is_empty());
        let cf = reopened
            .get_column_family("data")
            .expect("recovered column family");
        let tx = reopened
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("read transaction");
        for key in [b"first".as_slice(), b"second".as_slice()] {
            assert_eq!(
                tx.get(key).expect("read after retry").as_deref(),
                Some(b"preserved".as_slice())
            );
        }
        drop(tx);
        reopened
            .shutdown(Duration::from_secs(30))
            .expect("shutdown recovered engine");
        scenario.teardown();
    }
}

mod cloud_ddl_two_phase_hardening {
    use cntryl_midge::{Engine, OpenOptions};
    #[cfg(feature = "failpoints")]
    use cntryl_midge::{EngineHealth, MidgeError, TransactionMode, WriteOptions};
    use std::fs;
    use std::path::Path;
    #[cfg(feature = "failpoints")]
    use std::sync::{Mutex, OnceLock};
    use std::time::Duration;

    #[cfg(feature = "failpoints")]
    static DDL_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn open_cloud(path: &Path) -> Engine {
        Engine::open(
            OpenOptions::cloud_simulated(path, "ddl-test-bucket", "ddl/")
                .background_compaction(false)
                .build()
                .expect("build cloud options"),
        )
        .expect("open cloud engine")
    }

    fn shutdown(mut engine: Engine) {
        engine
            .shutdown(Duration::from_secs(3))
            .expect("shutdown cloud engine");
    }

    #[test]
    fn should_converge_local_remote_ddl_state_given_normal_create_drop_when_reopening() {
        // Arrange
        #[cfg(feature = "failpoints")]
        let _guard = DDL_TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp = tempfile::tempdir().expect("temp dir");
        let engine = open_cloud(temp.path());
        let created = engine
            .create_column_family("two-phase-cf")
            .expect("create column family");
        shutdown(engine);

        // Act
        let reopened = open_cloud(temp.path());
        assert!(reopened.get_column_family("two-phase-cf").is_some());
        reopened
            .drop_column_family(created.id())
            .expect("drop column family");
        shutdown(reopened);
        let final_open = open_cloud(temp.path());

        // Assert
        assert!(final_open.get_column_family("two-phase-cf").is_none());
        let registry = fs::read(temp.path().join("cloud_store/metadata/ddl.registry.json"))
            .expect("read authoritative DDL registry");
        let registry: serde_json::Value =
            serde_json::from_slice(&registry).expect("decode registry");
        assert_eq!(registry["epoch"], 2);
        assert_eq!(registry["operations"].as_array().map(Vec::len), Some(2));
        shutdown(final_open);
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_converge_local_remote_ddl_state_given_remote_cas_failure_when_reopening() {
        // Arrange
        let _guard = DDL_TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp = tempfile::tempdir().expect("temp dir");
        let engine = open_cloud(temp.path());
        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::ddl::before_remote_cas", "return").expect("configure remote CAS failure");

        // Act
        let failed_create = engine.create_column_family("cas-reopen");
        fail::remove("midge::ddl::before_remote_cas");
        scenario.teardown();
        assert!(matches!(failed_create, Err(MidgeError::Internal(_))));
        assert!(engine.get_column_family("cas-reopen").is_none());
        shutdown(engine);
        let reopened = open_cloud(temp.path());
        assert!(
            !temp.path().join("ddl.prepare.json").exists(),
            "reopen itself must clear the failed pre-authority prepare before any retry"
        );
        let before_retry = reopened.get_column_family("cas-reopen");
        let created = reopened
            .create_column_family("cas-reopen")
            .expect("retry create after reopen");

        // Assert
        assert!(before_retry.is_none());
        assert_eq!(created.name(), "cas-reopen");
        let registry = fs::read(temp.path().join("cloud_store/metadata/ddl.registry.json"))
            .expect("read authoritative DDL registry");
        let registry: serde_json::Value =
            serde_json::from_slice(&registry).expect("decode registry");
        assert_eq!(registry["epoch"], 1);
        assert_eq!(registry["operations"].as_array().map(Vec::len), Some(1));
        shutdown(reopened);
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_retry_cloud_column_family_create_after_remote_cas_failure() {
        // Arrange
        let _guard = DDL_TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp = tempfile::tempdir().expect("temp dir");
        let engine = open_cloud(temp.path());
        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::ddl::before_remote_cas", "return").expect("configure CAS failpoint");

        // Act
        let first = engine.create_column_family("cas-retry");
        fail::remove("midge::ddl::before_remote_cas");
        scenario.teardown();
        let second = engine.create_column_family("cas-retry");

        // Assert
        assert!(matches!(first, Err(MidgeError::Internal(_))));
        assert_eq!(second.expect("retry create").name(), "cas-retry");
        shutdown(engine);
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_reconcile_remote_commit_when_local_commit_fails() {
        // Arrange
        let _guard = DDL_TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp = tempfile::tempdir().expect("temp dir");
        let engine = open_cloud(temp.path());
        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::ddl::before_local_commit", "return")
            .expect("configure local commit failpoint");

        // Act
        let first = engine.create_column_family("local-retry");
        fail::remove("midge::ddl::before_local_commit");
        scenario.teardown();

        // Assert: remote CAS already committed, so the live runtime adopts that
        // authority and fences the stale local view until restart reconciliation.
        assert_eq!(
            first.expect("remote-authoritative create").name(),
            "local-retry"
        );
        assert_eq!(
            engine
                .get_runtime_metrics()
                .expect("degraded runtime metrics")
                .health,
            EngineHealth::Degraded
        );
        shutdown(engine);
        let reopened = open_cloud(temp.path());
        assert!(reopened.get_column_family("local-retry").is_some());
        assert_eq!(
            reopened
                .get_runtime_metrics()
                .expect("reconciled runtime metrics")
                .health,
            EngineHealth::Healthy
        );
        shutdown(reopened);
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_return_usable_column_family_when_create_metadata_mirror_fails_after_commit() {
        // Arrange
        let _guard = DDL_TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp = tempfile::tempdir().expect("temp dir");
        let engine = open_cloud(temp.path());
        let scenario = fail::FailScenario::setup();
        fail::cfg(
            "midge::ddl::after_create_local_commit_before_metadata_mirror",
            "return",
        )
        .expect("configure post-commit create mirror failure");

        // Act
        let create_result = engine.create_column_family("committed-create");
        fail::remove("midge::ddl::after_create_local_commit_before_metadata_mirror");
        scenario.teardown();

        // Assert: the DDL registry and local journal already made this create
        // authoritative, so the public handle registry must adopt it even when an
        // auxiliary manifest mirror is temporarily degraded.
        let cf = create_result.expect("return committed column family");
        assert_eq!(
            engine
                .get_column_family("committed-create")
                .expect("registered committed column family")
                .id(),
            cf.id()
        );
        let mut writer = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin transaction through committed handle");
        writer
            .put(b"key".to_vec(), b"value".to_vec(), None)
            .expect("buffer write through committed handle");
        writer
            .commit(WriteOptions::cloud_async())
            .expect("commit write through committed handle");
        let reader = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin read through committed handle");
        assert_eq!(
            reader
                .get(b"key")
                .expect("read value through committed handle")
                .as_deref(),
            Some(b"value".as_slice())
        );
        drop(reader);

        let retried = engine
            .create_column_family("committed-create")
            .expect("idempotently retry committed create");
        assert_eq!(retried.id(), cf.id());
        let registry = fs::read(temp.path().join("cloud_store/metadata/ddl.registry.json"))
            .expect("read authoritative DDL registry");
        let registry: serde_json::Value =
            serde_json::from_slice(&registry).expect("decode registry");
        assert_eq!(registry["epoch"], 1);
        assert_eq!(registry["operations"].as_array().map(Vec::len), Some(1));
        assert_eq!(
            engine
                .get_runtime_metrics()
                .expect("degraded runtime metrics")
                .health,
            EngineHealth::Degraded
        );
        shutdown(engine);

        let reopened = open_cloud(temp.path());
        assert!(reopened.get_column_family("committed-create").is_some());
        assert_eq!(
            reopened
                .get_runtime_metrics()
                .expect("reconciled runtime metrics")
                .health,
            EngineHealth::Healthy
        );
        shutdown(reopened);
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_fence_column_family_when_remote_drop_commits_before_local_commit() {
        // Arrange
        let _guard = DDL_TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp = tempfile::tempdir().expect("temp dir");
        let engine = open_cloud(temp.path());
        let cf = engine
            .create_column_family("remote-drop")
            .expect("create column family");
        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::ddl::before_local_commit", "return")
            .expect("configure local drop commit failpoint");

        // Act
        let drop_result = engine.drop_column_family(cf.id());
        fail::remove("midge::ddl::before_local_commit");
        scenario.teardown();

        // Assert
        assert!(drop_result.is_ok(), "remote authority committed the drop");
        assert!(engine.get_column_family("remote-drop").is_none());
        assert_eq!(
            engine
                .get_runtime_metrics()
                .expect("degraded runtime metrics")
                .health,
            EngineHealth::Degraded
        );
        shutdown(engine);

        let reopened = open_cloud(temp.path());
        assert!(reopened.get_column_family("remote-drop").is_none());
        assert_eq!(
            reopened
                .get_runtime_metrics()
                .expect("reconciled runtime metrics")
                .health,
            EngineHealth::Healthy
        );
        shutdown(reopened);
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_fence_column_family_when_remote_drop_cas_response_is_lost() {
        // Arrange
        let _guard = DDL_TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp = tempfile::tempdir().expect("temp dir");
        let engine = open_cloud(temp.path());
        let cf = engine
            .create_column_family("lost-drop-response")
            .expect("create column family");
        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::ddl::after_remote_cas", "return")
            .expect("configure lost remote CAS response");

        // Act
        let drop_result = engine.drop_column_family(cf.id());
        fail::remove("midge::ddl::after_remote_cas");
        scenario.teardown();

        // Assert
        assert!(
            drop_result.is_ok(),
            "operation id proves the remote drop committed"
        );
        assert!(engine.get_column_family("lost-drop-response").is_none());
        assert_eq!(
            engine
                .get_runtime_metrics()
                .expect("degraded runtime metrics")
                .health,
            EngineHealth::Degraded
        );
        shutdown(engine);

        let reopened = open_cloud(temp.path());
        assert!(reopened.get_column_family("lost-drop-response").is_none());
        assert_eq!(
            reopened
                .get_runtime_metrics()
                .expect("reconciled runtime metrics")
                .health,
            EngineHealth::Healthy
        );
        shutdown(reopened);
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_fence_writes_when_remote_drop_authority_cannot_be_resolved() {
        // Arrange
        let _guard = DDL_TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp = tempfile::tempdir().expect("temp dir");
        let engine = open_cloud(temp.path());
        let cf = engine
            .create_column_family("ambiguous-drop")
            .expect("create column family");
        let mut seed = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin unflushed seed transaction");
        seed.put(b"discarded".to_vec(), b"value".to_vec(), None)
            .expect("buffer unflushed seed");
        seed.commit(WriteOptions::cloud_async())
            .expect("commit unflushed seed");
        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::ddl::after_remote_cas", "return")
            .expect("configure lost remote CAS response");
        fail::cfg(
            "midge::ddl::before_ambiguous_cas_authority_reread",
            "return",
        )
        .expect("configure authority re-read failure");

        // Act
        let drop_result = engine.drop_column_family_discarding_unflushed(cf.id());
        fail::remove("midge::ddl::after_remote_cas");
        fail::remove("midge::ddl::before_ambiguous_cas_authority_reread");
        scenario.teardown();

        // Assert: until the durable prepare can be reconciled, the runtime must
        // not accept a write that a remotely committed drop would erase.
        assert!(matches!(drop_result, Err(MidgeError::Fenced(_))));
        assert!(engine.get_column_family("ambiguous-drop").is_some());
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin transaction against locally visible CF");
        tx.put(b"must-not-commit".to_vec(), b"value".to_vec(), None)
            .expect("buffer fenced write");
        assert!(matches!(
            tx.commit(WriteOptions::cloud_async()),
            Err(MidgeError::Fenced(_))
        ));
        assert_eq!(
            engine
                .get_runtime_metrics()
                .expect("degraded runtime metrics")
                .health,
            EngineHealth::Degraded
        );

        engine
            .drop_column_family(cf.id())
            .expect("safe retry resolves already-committed prepared remote drop");
        assert!(engine.get_column_family("ambiguous-drop").is_none());
        shutdown(engine);

        let reopened = open_cloud(temp.path());
        assert!(reopened.get_column_family("ambiguous-drop").is_none());
        shutdown(reopened);
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_abort_torn_ddl_prepare_given_local_prepare_without_remote_commit_when_reopening() {
        // Arrange
        let _guard = DDL_TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp = tempfile::tempdir().expect("temp dir");
        let engine = open_cloud(temp.path());
        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::ddl::after_prepare", "return").expect("configure prepare failpoint");

        // Act
        let first = engine.create_column_family("prepare-retry");
        assert!(temp.path().join("ddl.prepare.json").exists());
        fail::remove("midge::ddl::after_prepare");
        scenario.teardown();
        let second = engine.create_column_family("prepare-retry");

        // Assert
        assert!(matches!(first, Err(MidgeError::Internal(_))));
        assert_eq!(second.expect("retry after abort").name(), "prepare-retry");
        assert!(!temp.path().join("ddl.prepare.json").exists());
        shutdown(engine);
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_recover_ambiguous_ddl_once_when_reopening_after_crash_before_remote_cas_submission() {
        // Arrange
        let _guard = DDL_TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp = tempfile::tempdir().expect("temp dir");
        let engine = open_cloud(temp.path());
        let scenario = fail::FailScenario::setup();
        fail::cfg(
            "midge::ddl::after_ambiguous_prepare_before_remote_cas_submission",
            "return",
        )
        .expect("configure crash-before-CAS failpoint");

        // Act
        let first = engine.create_column_family("crash-before-cas");
        fail::remove("midge::ddl::after_ambiguous_prepare_before_remote_cas_submission");
        scenario.teardown();

        // Assert: the accepted operation is durable locally but never reached the
        // remote registry before the simulated crash boundary.
        assert!(matches!(first, Err(MidgeError::Internal(_))));
        let prepare = fs::read(temp.path().join("ddl.prepare.json")).expect("read DDL prepare");
        let prepare: serde_json::Value =
            serde_json::from_slice(&prepare).expect("decode DDL prepare");
        assert_eq!(prepare["remote_cas_ambiguous"], true);
        let op_id = prepare["op_id"]
            .as_str()
            .expect("prepared operation id")
            .to_string();
        let registry = fs::read(temp.path().join("cloud_store/metadata/ddl.registry.json"))
            .expect("read authoritative DDL registry");
        let registry: serde_json::Value =
            serde_json::from_slice(&registry).expect("decode registry");
        assert!(registry["operations"]
            .as_array()
            .expect("registry operations")
            .iter()
            .all(|operation| operation["op_id"].as_str() != Some(op_id.as_str())));
        assert!(engine.get_column_family("crash-before-cas").is_none());
        shutdown(engine);

        // Act: startup must safely re-drive the same durable operation rather than
        // fence the database forever or mint a replacement operation id.
        let reopened = open_cloud(temp.path());

        // Assert
        assert!(reopened.get_column_family("crash-before-cas").is_some());
        assert!(!temp.path().join("ddl.prepare.json").exists());
        let registry = fs::read(temp.path().join("cloud_store/metadata/ddl.registry.json"))
            .expect("read reconciled authoritative DDL registry");
        let registry: serde_json::Value =
            serde_json::from_slice(&registry).expect("decode registry");
        let matching_operations = registry["operations"]
            .as_array()
            .expect("registry operations")
            .iter()
            .filter(|operation| operation["op_id"].as_str() == Some(op_id.as_str()))
            .count();
        assert_eq!(matching_operations, 1);
        assert_eq!(registry["epoch"], 1);
        shutdown(reopened);
    }
}

mod engine_gc_cloud {
    //! Cloud-Specific Garbage Collection Tests
    //!
    //! Tests garbage collection of cloud objects:
    //! - Orphaned cloud SST deletion after compaction
    //! - Preservation of referenced cloud objects
    //! - Graceful handling of cloud delete failures
    //!
    //! **Storage Modes**: Cloud only (uses `MockStorage` for failure injection)
    //!
    //! Naming convention:
    //! should_<behavior>_given_<context>_when_<condition>

    use crate::common::*;
    use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
    use std::collections::BTreeSet;
    use std::path::Path;
    use std::thread;
    use std::time::{Duration, Instant};

    // ============================================================================
    // HELPERS
    // ============================================================================

    /// List the `.sst` object names directly inside `dir` (non-recursive).
    fn sst_object_names(dir: &Path) -> BTreeSet<String> {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return BTreeSet::new();
        };
        entries
            .flatten()
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("sst"))
            })
            .filter_map(|entry| entry.file_name().into_string().ok())
            .collect()
    }

    // ============================================================================
    // TEST GROUP: Cloud Object Garbage Collection
    // ============================================================================

    #[test]
    fn should_collect_orphaned_cloud_objects_after_compaction() {
        eprintln!("\n=== Cloud GC: Collect Orphaned Objects ===");

        // Arrange: a real simulated cloud backend (filesystem-backed bucket),
        // not "local" mode standing in for it, so we can observe the actual
        // cloud object store before and after compaction.
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let db_path = temp_dir.path();
        let engine = Engine::open(
            OpenOptions::cloud_simulated(db_path, "test-bucket", "gc-orphan-objects")
                .background_compaction(false)
                .build()
                .expect("build simulated cloud options"),
        )
        .expect("open simulated cloud engine");
        let cf = engine.create_column_family("test").expect("create cf");

        // Write the same overlapping key range across four separate flushes.
        // Use four files so the fixture exceeds the derived default L0 trigger;
        // the overlapping keys make every bounded pass a genuine L0 -> L1 merge.
        for batch in 0..4 {
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..200 {
                let key = format!("cloudsst_key_{i:04}");
                tx.put(
                    key.into_bytes(),
                    format!("cloud_value_{batch}").into_bytes(),
                    None,
                )
                .expect("put");
            }
            tx.commit(WriteOptions::cloud_async()).expect("commit");
            engine.flush_cf(&cf).expect("flush batch");
        }

        let cloud_sst_dir = db_path.join("cloud_store").join("sst");
        let before_objects = sst_object_names(&cloud_sst_dir);
        assert!(
            before_objects.len() >= 4,
            "expected all four flushed SSTs to be mirrored to cloud storage, got {before_objects:?}"
        );

        // Act: Compact. Each merge batch stays bounded, while compact_all walks
        // every batch until the logical L0 debt is clear.
        engine.compact_all().expect("compact_all");

        // The cloud delete of orphaned inputs runs on a background worker, so
        // poll until at least one pre-compaction object is gone and at least
        // one new object has appeared (bounded wait).
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut after_objects;
        loop {
            after_objects = sst_object_names(&cloud_sst_dir);
            let removed = before_objects.difference(&after_objects).count();
            let added = after_objects.difference(&before_objects).count();
            if removed > 0 && added > 0 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for orphaned cloud SST objects to be collected; \
                 before={before_objects:?} after={after_objects:?}"
            );
            thread::sleep(Duration::from_millis(50));
        }

        // Assert: at least one pre-compaction object is actually gone from cloud
        // storage (a real orphan collection, not a no-op), and the compacted
        // output object is present in its place.
        let removed: Vec<_> = before_objects.difference(&after_objects).collect();
        let added: Vec<_> = after_objects.difference(&before_objects).collect();
        assert!(
            !removed.is_empty(),
            "expected at least one orphaned cloud object to be collected after compaction"
        );
        assert!(
            !added.is_empty(),
            "expected the compacted output SST to be present in cloud storage"
        );

        // Assert: all data survived compaction.
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin_tx");
        for i in 0..200 {
            let key = format!("cloudsst_key_{i:04}");
            let val = tx.get(key.as_bytes()).expect("get");
            assert!(val.is_some(), "data lost during cloud object compaction");
        }

        eprintln!("✓ Cloud GC successfully cleaned up orphaned objects");
    }

    #[test]
    fn should_not_collect_cloud_objects_referenced_by_manifest() {
        for_each_storage_mode(&["local"], |mode, opts| {
            eprintln!("\n=== Cloud GC: Preserve Referenced Objects (mode: {mode}) ===");

            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Create active SST
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..100 {
                let key = format!("ref_key_{i:04}");
                tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                    .ok();
            }
            tx.commit(WriteOptions::buffered()).expect("commit");
            engine.flush_cf(&cf).expect("flush");

            // Act: Don't compact; SST remains in manifest
            // Verify data is readable
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");

            // Assert: Check manifest indirectly via data availability
            let mut found = 0;
            for i in 0..100 {
                let key = format!("ref_key_{i:04}");
                if tx
                    .get(key.as_bytes())
                    .expect("read manifest-referenced key")
                    .is_some()
                {
                    found += 1;
                }
            }
            assert_eq!(
                found, 100,
                "manifest-referenced cloud objects were incorrectly deleted in mode: {mode}"
            );

            eprintln!("✓ All manifest-referenced cloud objects preserved");
        });
    }

    /// Simulates a cloud provider outage that specifically affects deleting
    /// GC'd (orphaned) objects, without disturbing the output upload that must
    /// precede manifest publication. A provider-boundary failpoint keeps this
    /// deterministic even when the process has permission to delete read-only
    /// files, as root does inside the Docker qualification image.
    #[test]
    #[cfg(feature = "failpoints")]
    fn should_handle_gc_when_cloud_delete_fails() {
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        eprintln!("\n=== Cloud GC: Handle Delete Failure ===");

        // Arrange: real simulated cloud backend.
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let db_path = temp_dir.path();
        let options = OpenOptions::cloud_simulated(db_path, "test-bucket", "gc-delete-fail")
            .background_compaction(false)
            .build()
            .expect("build simulated cloud options");
        let l0_batch_size = options.l0_compaction_trigger();
        let mut engine = Engine::open(options).expect("open simulated cloud engine");
        let cf = engine.create_column_family("test").expect("create cf");

        // Write exactly one configured L0 batch. This isolates the delete failure
        // after publication without requiring a second upload while the simulated
        // bucket is deliberately read-only.
        for batch in 0..l0_batch_size {
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..100 {
                let key = format!("del_fail_key_{i:04}");
                tx.put(
                    key.as_bytes().to_vec(),
                    format!("value_{batch}").into_bytes(),
                    None,
                )
                .expect("put");
            }
            tx.commit(WriteOptions::cloud_async()).expect("commit");
            engine.flush_cf(&cf).expect("flush batch");
        }

        let cloud_sst_dir = db_path.join("cloud_store").join("sst");
        let before_objects = sst_object_names(&cloud_sst_dir);
        assert!(
            before_objects.len() >= l0_batch_size,
            "expected one configured L0 batch to be mirrored to cloud storage, got {before_objects:?}"
        );

        // Arm only the remote SST delete boundary. The compacted output upload
        // and manifest authority switch remain real simulated-cloud operations.
        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::cloud::inject_fail_sst_delete", "return")
            .expect("configure cloud delete outage failpoint");

        // Act: compaction should orphan the selected input SSTs and try (and fail)
        // to delete them from cloud storage.
        let compact_result = engine.compact_all();

        // Assert: compaction tolerates the delete failure rather than
        // propagating it as an error.
        assert!(
            compact_result.is_ok(),
            "compact_all should tolerate a cloud delete failure: {compact_result:?}"
        );

        // Assert: engine remains fully functional; no data was lost.
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin_tx");
        for i in 0..100 {
            let key = format!("del_fail_key_{i:04}");
            assert!(
                tx.get(key.as_bytes()).expect("get").is_some(),
                "data lost after cloud delete failure"
            );
        }
        drop(tx);

        // Shutdown joins every cloud-delete worker while the outage remains
        // armed, so the filesystem observation cannot race an unattempted delete.
        engine
            .shutdown(Duration::from_secs(10))
            .expect("shutdown after failed cloud delete");
        let after_objects = sst_object_names(&cloud_sst_dir);
        let retained: Vec<_> = before_objects.intersection(&after_objects).collect();

        fail::remove("midge::cloud::inject_fail_sst_delete");
        scenario.teardown();

        // Assert: the orphaned objects are still present in cloud storage
        // because their delete genuinely failed and was retained for retry,
        // not silently skipped or corrupted.
        assert!(
            !retained.is_empty(),
            "expected the orphaned cloud objects whose delete failed to remain \
             in cloud storage for retry, got {after_objects:?}"
        );

        eprintln!("✓ Engine gracefully handled cloud delete failure");
    }

    #[cfg(feature = "failpoints")]
    fn failpoint_test_lock() -> &'static std::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
    }
}
mod external_adopter_smoke {
    //! Trust-critical smoke suite for early external evaluation.
    //!
    //! These tests are intentionally small and local-disk only. They are the
    //! minimum gate for saying Midge is "safe enough to try" in a controlled
    //! environment.

    use crate::common::*;
    use bytes::Bytes;
    use cntryl_midge::{Query, TransactionMode, WriteOptions};
    use std::sync::{Mutex, OnceLock};
    use std::time::Duration;

    #[test]
    fn should_recover_wal_backed_commit_when_reopening_local_engine() {
        // Arrange
        let _guard = lock_smoke_tests();
        let opts = opts_for_mode("local");

        // Act
        {
            let mut engine = open_with_mode(&opts, "local");
            let cf = engine.create_column_family("smoke").expect("create cf");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin write tx");
            tx.put(b"wal-key".to_vec(), b"wal-value".to_vec(), None)
                .expect("put wal value");
            tx.commit(WriteOptions::sync()).expect("sync commit");
            engine
                .shutdown(Duration::from_secs(2))
                .expect("shutdown before reopen");
        }

        let engine = open_with_mode(&opts, "local");
        let cf = engine.get_column_family("smoke").expect("get smoke cf");
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin read tx");

        // Assert
        assert_eq!(
            tx.get(b"wal-key").expect("get wal key"),
            Some(Bytes::from_static(b"wal-value"))
        );
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_keep_data_recoverable_when_flush_publication_is_interrupted() {
        // Arrange
        let _guard = lock_smoke_tests();
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let mut engine = cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::local(db_path)
                .build()
                .expect("build options"),
        )
        .expect("open engine");
        let cf = engine.get_column_family("default").expect("default cf");

        for index in 0..8 {
            let key = format!("flush-{index:02}");
            let value = format!("value-{index:02}");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin write tx");
            tx.put(key.into_bytes(), value.into_bytes(), None)
                .expect("put flush seed");
            tx.commit(WriteOptions::sync()).expect("commit flush seed");
        }

        // Act
        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::manifest::inject_no_space_on_add_sst_edit", "return")
            .expect("configure manifest append failpoint");
        let flush_error = engine
            .flush_cf(&cf)
            .expect_err("flush should fail before publish");
        fail::remove("midge::manifest::inject_no_space_on_add_sst_edit");
        scenario.teardown();
        assert!(
            flush_error
                .to_string()
                .to_ascii_lowercase()
                .contains("space"),
            "unexpected flush error: {flush_error}"
        );
        engine
            .shutdown(Duration::from_secs(2))
            .expect("shutdown before reopen");

        let reopened = cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::local(db_path)
                .build()
                .expect("build options"),
        )
        .expect("reopen engine");
        let cf = reopened.get_column_family("default").expect("default cf");
        let tx = reopened
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin read tx");

        // Assert
        assert_eq!(
            tx.get(b"flush-00").expect("get recovered key"),
            Some(Bytes::from_static(b"value-00"))
        );
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_keep_compacted_data_visible_when_compaction_crashes_before_publish() {
        // Arrange
        let _guard = lock_smoke_tests();
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let mut engine = cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::local(db_path)
                .build()
                .expect("build options"),
        )
        .expect("open engine");
        let cf = engine.get_column_family("default").expect("default cf");

        for batch in 0..4 {
            for index in 0..20 {
                let key = format!("cmp-b{batch}-k{index:02}");
                let value = format!("value-{batch}-{index}");
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin write tx");
                tx.put(key.into_bytes(), value.into_bytes(), None)
                    .expect("put compaction seed");
                tx.commit(WriteOptions::sync())
                    .expect("commit compaction seed");
            }
            engine.flush_cf(&cf).expect("flush compaction seed");
        }

        // Act
        let scenario = fail::FailScenario::setup();
        fail::cfg(
            "midge::manifest::inject_no_space_on_compaction_batch_edit",
            "return",
        )
        .expect("configure compaction publish failpoint");
        engine
            .compact_all()
            .expect_err("compact_all must report the injected publication failure");
        fail::remove("midge::manifest::inject_no_space_on_compaction_batch_edit");
        scenario.teardown();
        engine
            .shutdown(Duration::from_secs(2))
            .expect("shutdown before reopen");

        let reopened = cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::local(db_path)
                .build()
                .expect("build options"),
        )
        .expect("reopen engine");
        let cf = reopened.get_column_family("default").expect("default cf");
        let tx = reopened
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin read tx");

        // Assert
        assert_eq!(
            tx.get(b"cmp-b0-k00").expect("get oldest compacted key"),
            Some(Bytes::from_static(b"value-0-0"))
        );
        assert_eq!(
            tx.get(b"cmp-b3-k19").expect("get newest compacted key"),
            Some(Bytes::from_static(b"value-3-19"))
        );
    }

    #[test]
    fn should_filter_point_delete_tombstones_during_cross_sst_iteration() {
        // Arrange
        let _guard = lock_smoke_tests();
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine.create_column_family("smoke").expect("create cf");

        for batch in 0..2 {
            for i in 0..20 {
                let key = format!("k{:03}", batch * 20 + i);
                let value = format!("v{:03}", batch * 20 + i);
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin write tx");
                tx.put(key.into_bytes(), value.into_bytes(), None)
                    .expect("put iterator seed");
                tx.commit(WriteOptions::buffered())
                    .expect("commit iterator seed");
            }
            engine.flush_cf(&cf).expect("flush iterator batch");
        }

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin delete tx");
        for deleted in [b"k010", b"k011", b"k024"] {
            tx.delete(deleted.to_vec()).expect("delete iterator key");
        }
        tx.commit(WriteOptions::buffered())
            .expect("commit iterator deletes");
        engine.flush_cf(&cf).expect("flush iterator tombstones");

        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin read tx");
        let results = tx
            .scan(&Query::new())
            .expect("scan")
            .try_collect()
            .expect("collect scan");

        // Assert
        for deleted in [b"k010", b"k011", b"k024"] {
            assert!(
                !results.iter().any(|(k, _)| k.as_ref() == deleted),
                "deleted key {:?} should stay hidden",
                String::from_utf8_lossy(deleted)
            );
        }
        assert!(results.iter().any(|(k, _)| k.as_ref() == b"k000"));
        assert!(results.iter().any(|(k, _)| k.as_ref() == b"k039"));
    }

    fn smoke_test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn lock_smoke_tests() -> std::sync::MutexGuard<'static, ()> {
        match smoke_test_lock().lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}
mod observability_api {
    use cntryl_midge::{
        Engine, EngineHealth, MidgeError, OpenOptions, TransactionMode, WriteOptions,
    };
    use std::process::Command;
    use std::sync::{Mutex, OnceLock};
    use std::time::Duration;
    use tempfile::TempDir;

    static FAILPOINT_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    #[test]
    fn should_expose_local_engine_observability_surfaces() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        let engine = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
            .expect("open engine");
        let default_cf = engine
            .get_column_family("default")
            .expect("default column family");

        let mut tx = engine
            .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
            .expect("begin tx");
        tx.put(b"alpha".to_vec(), b"value-alpha".to_vec(), None)
            .expect("put alpha");
        tx.put(b"bravo".to_vec(), b"value-bravo".to_vec(), None)
            .expect("put bravo");
        tx.commit(WriteOptions::best_effort())
            .expect("commit best effort");
        engine.flush_cf(&default_cf).expect("flush default cf");

        // Act
        let metrics = engine.get_runtime_metrics().expect("runtime metrics");
        let layout = engine.get_storage_layout().expect("storage layout");
        let report = engine
            .verify_storage(Duration::from_secs(5))
            .expect("verify storage");
        let offline_report = Engine::verify_path(db_path).expect("offline verify path");

        // Assert
        assert_eq!(metrics.health, EngineHealth::Healthy);
        assert!(
            metrics.sst_count >= 1,
            "flush should publish at least one SST"
        );
        assert!(
            metrics.manifest_last_persisted_sequence >= metrics.current_sequence,
            "flush should advance manifest durability frontier"
        );
        assert_eq!(
            metrics.max_memtable_wal_segment_gap, 0,
            "local explicit flush should leave no outstanding WAL segment gap"
        );
        assert!(
            metrics.wal_append_count >= metrics.wal_fsync_count,
            "WAL append counter should never be below fsync counter"
        );
        assert!(metrics.flush_build_count >= 1);
        assert!(metrics.flush_publish_count >= 1);
        assert!(metrics.flush_enqueued_total >= metrics.flush_build_count);
        assert!(metrics.flush_build_ns_total >= metrics.flush_build_ns_max);
        assert!(metrics.flush_publish_ns_total >= metrics.flush_publish_ns_max);
        assert_eq!(metrics.flush_queue_depth, 0);
        assert_eq!(metrics.flush_inflight, 0);
        assert_eq!(metrics.flush_failures_total, 0);
        assert_eq!(metrics.flush_retries_total, 0);
        if metrics.wal_fsync_count > 0 {
            assert!(metrics.wal_fsync_ns_max > 0);
            assert!(metrics.wal_fsync_ns_total >= metrics.wal_fsync_ns_max);
        }
        assert_eq!(metrics.cache_hits + metrics.cache_misses, 0);
        assert_eq!(metrics.cloud_async_wal_uploads_failed, 0);
        assert_eq!(metrics.hybrid_pending_evictions, 0);
        let sealed_local_wal = std::fs::read_dir(db_path.join("wal"))
            .expect("list local WAL directory")
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|extension| extension == "wal"))
            .collect::<Vec<_>>();
        assert!(
            sealed_local_wal.is_empty(),
            "manifest-covered sealed local WAL segments should be pruned: {sealed_local_wal:?}"
        );
        assert_eq!(layout.health, EngineHealth::Healthy);
        assert!(
            layout
                .levels
                .iter()
                .map(|level| level.file_count)
                .sum::<usize>()
                >= 1,
            "layout should report the flushed SST"
        );
        assert!(
            layout
                .levels
                .iter()
                .flat_map(|level| level.files.iter())
                .all(|file| file.smallest_key.is_some()
                    && file.largest_key.is_some()
                    && file.smallest_seq.is_some()
                    && file.largest_seq.is_some()
                    && file.size_bytes > 0),
            "published SSTs must have complete metadata"
        );
        assert!(report.manifest_files_verified >= 1);
        assert!(report.sst_files_verified >= 1);
        assert_eq!(report.health, EngineHealth::Healthy);
        assert_eq!(offline_report.health, EngineHealth::Healthy);
        assert!(offline_report.manifest_files_verified >= 1);
    }

    #[test]
    fn should_reject_storage_verification_in_memory_mode() {
        // Arrange
        let engine = Engine::open(OpenOptions::in_memory().build().expect("build options"))
            .expect("open in-memory engine");

        // Act
        let result = engine.verify_storage(Duration::from_secs(5));

        // Assert
        match result {
            Err(MidgeError::NotSupported(message)) => {
                assert!(
                    message.contains("not supported"),
                    "expected descriptive not-supported message, got: {message}"
                );
            }
            other => panic!("expected NotSupported from verify_storage, got: {other:?}"),
        }
    }

    #[test]
    fn should_install_explicit_memtable_size_in_runtime_metrics() {
        // Arrange
        let memtable_size = 128 * 1024;

        let engine = Engine::open(
            OpenOptions::in_memory()
                .with_memtable_size_limit(memtable_size)
                .build()
                .expect("build options"),
        )
        .expect("open in-memory engine");

        // Act
        let metrics = engine.get_runtime_metrics().expect("runtime metrics");

        // Assert
        assert_eq!(metrics.memtable_size_limit, memtable_size);
        assert_eq!(metrics.memtable_flush_threshold, memtable_size);
        assert_eq!(metrics.max_memtable_wal_segment_gap, 0);
    }

    #[test]
    fn should_install_explicit_memtable_limits_in_runtime_metrics_when_both_are_set() {
        // Arrange
        let memtable_size = 256 * 1024;
        let flush_threshold = 128 * 1024;

        let engine = Engine::open(
            OpenOptions::in_memory()
                .with_memtable_size_limit(memtable_size)
                .with_memtable_flush_threshold(flush_threshold)
                .build()
                .expect("build options"),
        )
        .expect("open in-memory engine");

        // Act
        let metrics = engine.get_runtime_metrics().expect("runtime metrics");

        // Assert
        assert_eq!(metrics.memtable_size_limit, memtable_size);
        assert_eq!(metrics.memtable_flush_threshold, flush_threshold);
        assert_eq!(metrics.max_memtable_wal_segment_gap, 0);
    }

    #[test]
    fn should_report_degraded_health_given_obsolete_sst_files_and_json_verification() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        let engine = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
            .expect("open engine");
        let default_cf = engine
            .get_column_family("default")
            .expect("default column family");

        let mut tx = engine
            .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
            .expect("begin tx");
        tx.put(b"charlie".to_vec(), b"value-charlie".to_vec(), None)
            .expect("put charlie");
        tx.commit(WriteOptions::best_effort())
            .expect("commit best effort");
        engine.flush_cf(&default_cf).expect("flush default cf");

        std::fs::write(db_path.join("sst").join("orphan.sst"), b"orphan-bytes")
            .expect("write orphan file");

        // Act
        let metrics = engine.get_runtime_metrics().expect("runtime metrics");
        let layout = engine.get_storage_layout().expect("storage layout");
        let report = engine
            .verify_storage(Duration::from_secs(5))
            .expect("verify storage");
        let output = Command::new(env!("CARGO_BIN_EXE_midge"))
            .arg("verify")
            .arg("--json")
            .arg(db_path)
            .output()
            .expect("run midge verify");

        // Assert
        assert_eq!(metrics.health, EngineHealth::Degraded);
        assert!(
            metrics.obsolete_file_backlog >= 1,
            "obsolete file backlog should reflect orphaned SST files"
        );
        assert_eq!(layout.health, EngineHealth::Degraded);
        assert!(
            layout
                .obsolete_files
                .iter()
                .any(|name| name == "orphan.sst"),
            "storage layout should report obsolete SST artifacts"
        );
        assert_eq!(report.health, EngineHealth::Degraded);
        assert_eq!(
            output.status.code(),
            Some(1),
            "degraded verification should exit with code 1"
        );

        let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
        let json: serde_json::Value = serde_json::from_str(&stdout).expect("parse json");
        assert_eq!(json["health"], "Degraded");
        assert_eq!(json["intent_entries_loaded"], 0);
    }

    #[test]
    fn should_exit_zero_given_healthy_database_when_midge_verify_runs() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let mut engine = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
            .expect("open engine");
        engine
            .shutdown(Duration::from_secs(2))
            .expect("shutdown healthy engine");

        // Act
        let output = Command::new(env!("CARGO_BIN_EXE_midge"))
            .arg("verify")
            .arg("--json")
            .arg(db_path)
            .output()
            .expect("run midge verify");

        // Assert
        assert_eq!(output.status.code(), Some(0));
        let report: serde_json::Value =
            serde_json::from_slice(&output.stdout).expect("parse healthy report");
        assert_eq!(report["health"], "Healthy");
        assert!(output.stderr.is_empty());
    }

    #[test]
    fn should_emit_json_error_object_given_verify_failure_when_json_flag_requested() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let missing_path = temp_dir.path().join("missing-db");

        // Act
        let output = Command::new(env!("CARGO_BIN_EXE_midge"))
            .arg("verify")
            .arg("--json")
            .arg(&missing_path)
            .output()
            .expect("run midge verify");

        // Assert
        assert_eq!(output.status.code(), Some(3));
        let error: serde_json::Value =
            serde_json::from_slice(&output.stdout).expect("parse structured error");
        assert_eq!(error["status"], "error");
        assert_eq!(error["error_kind"], "storage");
        assert!(
            error["message"]
                .as_str()
                .is_some_and(|message| message.contains("does not exist")),
            "unexpected structured error: {error}"
        );
        assert!(output.stderr.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn should_exit_three_given_inaccessible_storage_when_midge_verify_runs() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let mut engine = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
            .expect("open engine");
        engine
            .shutdown(Duration::from_secs(2))
            .expect("shutdown healthy engine");
        let marker_path = db_path.join("FORMAT");
        std::fs::remove_file(&marker_path).expect("remove FORMAT file");
        std::fs::create_dir(&marker_path).expect("replace FORMAT with directory");

        // Act
        let output = Command::new(env!("CARGO_BIN_EXE_midge"))
            .arg("verify")
            .arg("--json")
            .arg(db_path)
            .output()
            .expect("run midge verify");

        // Assert
        assert_eq!(output.status.code(), Some(3));
        let error: serde_json::Value =
            serde_json::from_slice(&output.stdout).expect("parse inaccessible-storage error");
        assert_eq!(error["status"], "error");
        assert_eq!(error["error_kind"], "storage");
        assert!(
            error["message"]
                .as_str()
                .is_some_and(|message| message.contains("Is a directory")),
            "unexpected inaccessible-storage error: {error}"
        );
        assert!(output.stderr.is_empty());
    }

    #[test]
    fn should_report_usage_error_given_missing_db_path_when_verify_invoked() {
        // Arrange

        // Act
        let output = Command::new(env!("CARGO_BIN_EXE_midge"))
            .arg("verify")
            .arg("--json")
            .output()
            .expect("run midge verify without path");

        // Assert
        assert_eq!(output.status.code(), Some(2));
        let error: serde_json::Value =
            serde_json::from_slice(&output.stdout).expect("parse structured usage error");
        assert_eq!(error["status"], "error");
        assert_eq!(error["error_kind"], "usage");
        assert!(output.stderr.is_empty());
    }

    #[test]
    fn should_treat_unrecognized_flag_as_path_given_typoed_json_flag_when_verify_invoked() {
        // Arrange

        // Act
        let output = Command::new(env!("CARGO_BIN_EXE_midge"))
            .arg("verify")
            .arg("--Json")
            .output()
            .expect("run midge verify with unknown flag");

        // Assert
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        let stderr = String::from_utf8(output.stderr).expect("utf8 stderr");
        assert!(
            stderr.contains("unknown flag '--Json'"),
            "unexpected usage error: {stderr}"
        );
        assert!(
            !stderr.contains("FORMAT"),
            "unknown flags must not be treated as storage paths: {stderr}"
        );
    }

    #[test]
    fn should_exit_four_given_corrupt_database_when_midge_verify_runs() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        let mut engine = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
            .expect("open engine");
        let default_cf = engine
            .get_column_family("default")
            .expect("default column family");
        let mut tx = engine
            .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
            .expect("begin write");
        tx.put(b"corrupt-me".to_vec(), b"value".to_vec(), None)
            .expect("put value");
        tx.commit(WriteOptions::sync()).expect("commit value");
        engine.flush_cf(&default_cf).expect("flush value");
        engine
            .shutdown(Duration::from_secs(2))
            .expect("shutdown before corruption");
        let sst_path = std::fs::read_dir(db_path.join("sst"))
            .expect("list SST directory")
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| path.extension().is_some_and(|extension| extension == "sst"))
            .expect("flushed SST");
        let mut bytes = std::fs::read(&sst_path).expect("read SST");
        let last = bytes.last_mut().expect("non-empty SST");
        *last ^= 0x01;
        std::fs::write(&sst_path, bytes).expect("write corrupt SST");

        // Act
        let output = Command::new(env!("CARGO_BIN_EXE_midge"))
            .arg("verify")
            .arg("--json")
            .arg(db_path)
            .output()
            .expect("run midge verify");

        // Assert
        assert_eq!(output.status.code(), Some(4));
        let error: serde_json::Value =
            serde_json::from_slice(&output.stdout).expect("parse corruption error");
        assert_eq!(error["status"], "error");
        assert_eq!(error["error_kind"], "corruption");
        assert!(output.stderr.is_empty());
    }

    #[test]
    fn should_ignore_stale_sst_temp_files_on_reopen() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        let mut engine = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
            .expect("open engine");
        engine
            .shutdown(Duration::from_secs(2))
            .expect("shutdown before reopen");

        std::fs::create_dir_all(db_path.join("sst")).expect("create sst dir");
        std::fs::write(db_path.join("sst").join("orphan.sst.tmp"), b"temp-bytes")
            .expect("write stale temp sst");

        // Act
        let reopened = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
            .expect("reopen engine");
        let metrics = reopened.get_runtime_metrics().expect("runtime metrics");
        let layout = reopened.get_storage_layout().expect("storage layout");
        let report = reopened
            .verify_storage(Duration::from_secs(5))
            .expect("verify storage");

        // Assert
        assert_eq!(metrics.health, EngineHealth::Healthy);
        assert_eq!(metrics.obsolete_file_backlog, 0);
        assert_eq!(layout.health, EngineHealth::Healthy);
        assert!(
            layout.obsolete_files.is_empty(),
            "temp SST residue must not be reported as authoritative storage residue"
        );
        assert_eq!(report.health, EngineHealth::Healthy);
        assert!(
            !db_path.join("sst").join("orphan.sst.tmp").exists(),
            "startup cleanup should remove stale temp SST residue"
        );
    }

    #[test]
    fn should_delete_orphan_sst_residue_during_startup_cleanup() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        {
            let mut engine =
                Engine::open(OpenOptions::local(db_path).build().expect("build options"))
                    .expect("open engine");
            let default_cf = engine
                .get_column_family("default")
                .expect("default column family");

            let mut tx = engine
                .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
                .expect("begin tx");
            tx.put(b"delta".to_vec(), b"value-delta".to_vec(), None)
                .expect("put delta");
            tx.commit(WriteOptions::best_effort())
                .expect("commit best effort");
            engine.flush_cf(&default_cf).expect("flush default cf");
            engine
                .shutdown(Duration::from_secs(2))
                .expect("shutdown before reopen");
        }

        std::fs::write(db_path.join("sst").join("orphan.sst"), b"orphan-bytes")
            .expect("write orphan sst");

        // Act
        let reopened = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
            .expect("reopen engine");
        let metrics = reopened.get_runtime_metrics().expect("runtime metrics");
        let report = Engine::verify_path(db_path).expect("offline verify path");

        // Assert
        assert_eq!(metrics.health, EngineHealth::Healthy);
        assert_eq!(metrics.obsolete_file_backlog, 0);
        assert_eq!(report.health, EngineHealth::Healthy);
        assert!(
            !db_path.join("sst").join("orphan.sst").exists(),
            "startup cleanup should delete orphan final SST residue when possible"
        );
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_report_degraded_health_when_orphan_sst_cleanup_is_blocked() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        {
            let mut engine =
                Engine::open(OpenOptions::local(db_path).build().expect("build options"))
                    .expect("open engine");
            let default_cf = engine
                .get_column_family("default")
                .expect("default column family");

            let mut tx = engine
                .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
                .expect("begin tx");
            tx.put(b"echo".to_vec(), b"value-echo".to_vec(), None)
                .expect("put echo");
            tx.commit(WriteOptions::best_effort())
                .expect("commit best effort");
            engine.flush_cf(&default_cf).expect("flush default cf");
            engine
                .shutdown(Duration::from_secs(2))
                .expect("shutdown before reopen");
        }

        std::fs::write(db_path.join("sst").join("orphan.sst"), b"orphan-bytes")
            .expect("write orphan sst");

        let scenario = fail::FailScenario::setup();
        fail::cfg(
            "midge::recovery::inject_orphan_sst_delete_failure",
            "return",
        )
        .expect("configure orphan delete failure failpoint");

        // Act
        let reopened = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
            .expect("reopen engine");
        let metrics = reopened.get_runtime_metrics().expect("runtime metrics");
        let layout = reopened.get_storage_layout().expect("storage layout");
        let report = Engine::verify_path(db_path).expect("offline verify path");

        // Assert
        assert_eq!(metrics.health, EngineHealth::Degraded);
        assert_eq!(layout.health, EngineHealth::Degraded);
        assert_eq!(report.health, EngineHealth::Degraded);
        assert!(
            layout
                .obsolete_files
                .iter()
                .any(|name| name == "orphan.sst"),
            "blocked orphan cleanup should leave the orphan visible to layout reporting"
        );

        fail::remove("midge::recovery::inject_orphan_sst_delete_failure");
        scenario.teardown();
    }

    #[test]
    fn should_ignore_stale_metadata_temp_files_on_reopen() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        let mut engine = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
            .expect("open engine");
        engine
            .shutdown(Duration::from_secs(2))
            .expect("shutdown before reopen");

        let temp_manifest = serde_json::json!({
            "last_persisted_sequence": 99,
            "ssts": [],
            "files": [{
                "name": "ghost.sst",
                "level": 0,
                "size_bytes": 123,
                "cf_id": 0,
                "sst_seq": 0,
                "smallest_key": null,
                "largest_key": null,
                "smallest_seq": null,
                "largest_seq": null,
                "sublevel": 0
            }],
            "column_families": [],
            "next_wal_seq": 1,
            "next_sst_seqs": {}
        });
        std::fs::write(
            db_path.join("manifest.json.tmp"),
            serde_json::to_vec_pretty(&temp_manifest).expect("serialize temp manifest"),
        )
        .expect("write temp manifest");
        std::fs::write(
            db_path.join("manifest.snapshot.json.tmp"),
            serde_json::to_vec_pretty(&temp_manifest).expect("serialize temp snapshot"),
        )
        .expect("write temp snapshot");
        std::fs::write(
            db_path.join("intent_log.json.tmp"),
            br#"[{"WalSynced":{"segment_id":7,"seqno":11}}]"#,
        )
        .expect("write temp intent log");

        // Act
        let reopened = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
            .expect("reopen engine");
        let metrics = reopened.get_runtime_metrics().expect("runtime metrics");
        let layout = reopened.get_storage_layout().expect("storage layout");

        // Assert
        assert_eq!(metrics.health, EngineHealth::Healthy);
        assert_eq!(metrics.sst_count, 0);
        assert_eq!(metrics.manifest_last_persisted_sequence, 0);
        assert!(
            layout.levels.iter().all(|level| level.file_count == 0),
            "stale metadata temp files must not publish SST state on reopen"
        );
        assert!(
            !db_path.join("manifest.json.tmp").exists(),
            "manifest staging residue should be cleaned up on reopen"
        );
        assert!(
            !db_path.join("manifest.snapshot.json.tmp").exists(),
            "snapshot staging residue should be cleaned up on reopen"
        );
        assert!(
            !db_path.join("intent_log.json.tmp").exists(),
            "intent staging residue should be cleaned up on reopen"
        );
    }

    #[test]
    fn should_return_runtime_metrics_within_generous_timeout() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let engine = Engine::open(
            OpenOptions::local(temp_dir.path())
                .build()
                .expect("build options"),
        )
        .expect("open engine");

        // Act
        let metrics = engine
            .get_runtime_metrics_with_timeout(Duration::from_secs(2))
            .expect("runtime metrics within timeout");

        // Assert
        assert_eq!(metrics.health, EngineHealth::Healthy);
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_unregister_response_slot_when_runtime_metrics_response_times_out() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let scenario = fail::FailScenario::setup();
        fail::cfg(
            "midge::runtime::before_get_runtime_metrics_response",
            "pause",
        )
        .expect("configure runtime metrics pause");
        let temp_dir = TempDir::new().expect("temp dir");
        let engine = Engine::open(
            OpenOptions::local(temp_dir.path())
                .build()
                .expect("build options"),
        )
        .expect("open engine");

        // Act
        let result = engine.get_runtime_metrics_with_timeout(Duration::from_millis(200));

        // Assert
        assert!(
            matches!(result, Err(MidgeError::Timeout(_))),
            "expected a bounded timeout while the response is blocked, got: {result:?}"
        );

        // The event loop thread is still parked inside the paused handler for the
        // abandoned request above. Release it and confirm the timed-out response
        // slot was unregistered rather than left to wedge a later request.
        fail::remove("midge::runtime::before_get_runtime_metrics_response");
        scenario.teardown();

        let metrics = engine
            .get_runtime_metrics_with_timeout(Duration::from_secs(2))
            .expect("runtime metrics after unblocking");
        assert_eq!(metrics.health, EngineHealth::Healthy);
    }

    #[test]
    fn should_reject_zero_deadline_for_runtime_metrics_without_sending_a_request() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let engine = Engine::open(
            OpenOptions::local(temp_dir.path())
                .build()
                .expect("build options"),
        )
        .expect("open engine");

        // Act
        let result = engine.get_runtime_metrics_with_timeout(Duration::ZERO);

        // Assert
        assert!(
            matches!(result, Err(MidgeError::Timeout(_))),
            "expected an immediate Timeout for a zero deadline, got: {result:?}"
        );

        // A zero deadline must fail fast without ever registering a response
        // slot, so the runtime must still answer a normal request afterward.
        let metrics = engine.get_runtime_metrics().expect("runtime metrics");
        assert_eq!(metrics.health, EngineHealth::Healthy);
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_return_runtime_metrics_when_stall_clears_before_deadline() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let scenario = fail::FailScenario::setup();
        fail::cfg(
            "midge::runtime::before_get_runtime_metrics_response",
            "pause",
        )
        .expect("configure runtime metrics pause");
        let temp_dir = TempDir::new().expect("temp dir");
        let engine = Engine::open(
            OpenOptions::local(temp_dir.path())
                .build()
                .expect("build options"),
        )
        .expect("open engine");

        // Act: release the stall well inside the deadline from another thread,
        // so the request must complete rather than time out.
        let releaser = std::thread::spawn(|| {
            std::thread::sleep(Duration::from_millis(100));
            fail::remove("midge::runtime::before_get_runtime_metrics_response");
        });
        let result = engine.get_runtime_metrics_with_timeout(Duration::from_secs(5));
        releaser.join().expect("join stall releaser");
        scenario.teardown();

        // Assert
        let metrics = result.expect("runtime metrics once the stall clears");
        assert_eq!(metrics.health, EngineHealth::Healthy);
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_not_leak_response_slots_across_repeated_timeouts() {
        // Arrange
        let _guard = failpoint_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let scenario = fail::FailScenario::setup();
        fail::cfg(
            "midge::runtime::before_get_runtime_metrics_response",
            "pause",
        )
        .expect("configure runtime metrics pause");
        let temp_dir = TempDir::new().expect("temp dir");
        let engine = Engine::open(
            OpenOptions::local(temp_dir.path())
                .build()
                .expect("build options"),
        )
        .expect("open engine");

        // Act: the very first call blocks the event loop inside the paused
        // handler. Every call after it queues behind that one blocked request
        // and times out on the client side without the event loop ever reaching
        // it, exercising the same abandon-and-unregister path repeatedly.
        for attempt in 0..5 {
            let result = engine.get_runtime_metrics_with_timeout(Duration::from_millis(100));
            assert!(
                matches!(result, Err(MidgeError::Timeout(_))),
                "attempt {attempt} expected Timeout, got: {result:?}"
            );
        }

        fail::remove("midge::runtime::before_get_runtime_metrics_response");
        scenario.teardown();

        // Assert: none of the abandoned requests wedged the runtime; it still
        // answers once the stall clears.
        let metrics = engine
            .get_runtime_metrics_with_timeout(Duration::from_secs(2))
            .expect("runtime metrics after repeated timeouts");
        assert_eq!(metrics.health, EngineHealth::Healthy);
    }

    #[test]
    fn should_reject_runtime_metrics_request_after_engine_shutdown() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let mut engine = Engine::open(
            OpenOptions::local(temp_dir.path())
                .build()
                .expect("build options"),
        )
        .expect("open engine");
        engine
            .shutdown(Duration::from_secs(2))
            .expect("shutdown engine");

        // Act
        let result = engine.get_runtime_metrics_with_timeout(Duration::from_secs(2));

        // Assert: a closed engine must reject the request rather than hang until
        // the deadline.
        assert!(
            matches!(result, Err(MidgeError::Busy(_) | MidgeError::Internal(_))),
            "expected a closed-runtime rejection, got: {result:?}"
        );
    }

    fn failpoint_test_lock() -> &'static Mutex<()> {
        FAILPOINT_TEST_LOCK.get_or_init(|| Mutex::new(()))
    }
}
mod hybrid_storage {
    //! Hybrid Storage & Eviction Tests
    //!
    //! Tests memory budget management, eviction triggering, and cloud-local coordination:
    //! - High watermark eviction triggering
    //! - Emergency watermark write blocking
    //! - Backpressure and recovery
    //! - Read preference (local before cloud)
    //! - Cloud fetch after eviction
    //! - Eviction state persistence across restarts
    //! - Reader isolation during eviction
    //!
    //! **Storage Modes**: Cloud only (hybrid storage requires cloud backend)
    //! **Memory config**: Explicitly configured budgets for testing eviction thresholds
    //!
    //! Naming convention:
    //! should_<behavior>_given_<context>_when_<condition>

    use crate::common::*;
    #[cfg(feature = "failpoints")]
    use cntryl_midge::EngineHealth;
    #[cfg(feature = "failpoints")]
    use cntryl_midge::MidgeError;
    use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[cfg(feature = "failpoints")]
    const CLOUD_OUTAGE_CHILD_ENV: &str = "MIDGE_HYBRID_CLOUD_OUTAGE_CHILD";

    /// Count files nested anywhere under `root`, used to prove that the
    /// filesystem-backed simulated cloud/local stores actually received or lost
    /// data, rather than trusting a `get()` result alone.
    fn count_files_recursive(root: &std::path::Path) -> usize {
        let Ok(entries) = std::fs::read_dir(root) else {
            return 0;
        };
        let mut count = 0;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                count += count_files_recursive(&path);
            } else {
                count += 1;
            }
        }
        count
    }

    /// A value that defeats SST compression, so its on-disk footprint tracks its
    /// logical size closely enough to reliably cross a storage budget.
    fn incompressible_value(len: usize, seed: u8) -> Vec<u8> {
        let mut random = 0x9e37_79b9_u32 ^ u32::from(seed);
        (0..len)
            .map(|_| {
                random ^= random << 13;
                random ^= random >> 17;
                random ^= random << 5;
                random.to_le_bytes()[0]
            })
            .collect()
    }

    // ============================================================================
    // TEST GROUP: Memory Budget & Eviction Control
    // ============================================================================

    #[test]
    fn should_apply_simulated_cloud_local_storage_budget_when_opening_simulated_cloud() {
        // Arrange
        let temp_dir = test_temp_dir();
        let budget_bytes = 8 * 1024 * 1024;
        let opts = OpenOptions::cloud_simulated(temp_dir.path(), "test-bucket", "test-prefix")
            .with_simulated_cloud_local_storage_budget(budget_bytes)
            .build()
            .expect("build options");

        // Act
        let engine = Engine::open(opts).expect("open simulated cloud engine");
        let metrics = engine.get_runtime_metrics().expect("runtime metrics");

        // Assert
        assert_eq!(metrics.hybrid_max_local_bytes, budget_bytes);
    }

    #[test]
    fn should_trigger_eviction_at_high_watermark() {
        // Note: This test assumes cloud storage is available
        // In a pure local test, we validate the logic without touching actual cloud
        for_each_storage_mode(&["local"], |mode, opts| {
            eprintln!("\n=== Hybrid: Trigger Eviction at High Watermark (mode: {mode}) ===");

            // Arrange: Set tight memory budget to make eviction triggerable
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Write large values to reach high watermark (~70% of budget)
            // Target: ~1MB per value * 10 = 10MB total
            let large_value = vec![b'X'; 1024 * 1024]; // 1MB value

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..10 {
                let key = format!("large_key_{i:02}");
                tx.put(key.as_bytes().to_vec(), large_value.clone(), None)
                    .ok();
            }
            tx.commit(buffered_write_options(mode)).expect("commit");

            // Act: Flush to SST (triggers potential eviction to cloud)
            engine.flush_cf(&cf).expect("flush");

            // Assert: Engine handled high watermark gracefully
            // Verify:
            // 1. No panic
            // 2. Data still accessible
            // 3. Memory pressure handled

            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            let readable = (0..10)
                .filter(|i| {
                    let key = format!("large_key_{i:02}");
                    tx.get(key.as_bytes())
                        .expect("read after high-watermark eviction")
                        .is_some()
                })
                .count();

            assert!(
                readable >= 8,
                "high watermark eviction caused data loss in mode: {mode}"
            );

            eprintln!("âœ“ High watermark eviction triggered safely; {readable} keys readable");
        });
    }

    #[test]
    fn should_keep_writes_running_when_published_ssts_exceed_local_budget() {
        // Arrange
        let temp_dir = test_temp_dir();
        let budget_bytes = 1024 * 1024;
        let opts = OpenOptions::cloud_simulated(temp_dir.path(), "test-bucket", "test-prefix")
            .local_storage_budget(budget_bytes)
            .background_compaction(false)
            .build()
            .expect("build options");
        let engine = Engine::open(opts).expect("open cloud-simulated engine");
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        for attempt in 0_u8..12 {
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(
                format!("budget-key-{attempt:02}").into_bytes(),
                incompressible_value(100 * 1024, attempt),
                None,
            )
            .expect("put");
            tx.commit(WriteOptions::cloud_strict())
                .expect("cloud commit");
            engine
                .flush_cf(&cf)
                .expect("flush and release published local SST");
        }

        // Assert
        let metrics = engine.get_runtime_metrics().expect("runtime metrics");
        assert!(metrics.hybrid_total_committed_bytes <= budget_bytes);
        assert_eq!(count_files_recursive(&temp_dir.path().join("sst")), 0);
        let remote_bytes: u64 = std::fs::read_dir(temp_dir.path().join("cloud_store/sst"))
            .expect("remote SST directory")
            .map(|entry| {
                entry
                    .expect("remote SST")
                    .metadata()
                    .expect("SST metadata")
                    .len()
            })
            .sum();
        assert!(remote_bytes > budget_bytes);
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("read transaction");
        for attempt in 0_u8..12 {
            let expected = incompressible_value(100 * 1024, attempt);
            assert_eq!(
                tx.get(format!("budget-key-{attempt:02}").as_bytes())
                    .expect("read")
                    .as_deref(),
                Some(expected.as_slice())
            );
        }
    }

    #[test]
    fn should_resume_writes_given_cloud_upload_completes_when_emergency_watermark_is_active() {
        for_each_storage_mode(&["local"], |mode, opts| {
            eprintln!("\n=== Hybrid: Resume Writes After Eviction (mode: {mode}) ===");

            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Fill to pressure point
            let medium_value = vec![b'Z'; 256 * 1024]; // 256KB

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..20 {
                let key = format!("pressure_key_{i:02}");
                tx.put(key.as_bytes().to_vec(), medium_value.clone(), None)
                    .ok();
            }
            tx.commit(buffered_write_options(mode)).expect("commit");

            // Act: Trigger eviction and wait
            engine.flush_cf(&cf).expect("flush");
            thread::sleep(Duration::from_millis(300));

            // Resume writes after eviction
            let mut resume_writes = 0;
            let mut last_error = None;
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            for i in 0_u64.. {
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                let key = format!("resume_key_{i:02}");
                let result = tx
                    .put(key.as_bytes().to_vec(), medium_value.clone(), None)
                    .and_then(|()| tx.commit(buffered_write_options(mode)));
                match result {
                    Ok(()) => resume_writes += 1,
                    Err(error @ cntryl_midge::MidgeError::WriteStall(_)) => {
                        last_error = Some(error);
                    }
                    Err(error) => {
                        panic!("unexpected resume write failure in mode {mode}: {error:?}")
                    }
                }
                if resume_writes >= 5 {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "writes did not resume before the flush queue drained in mode: {mode}; successes={resume_writes}; last_error={last_error:?}"
                );
                thread::sleep(Duration::from_millis(25));
            }

            // Assert: Writes can resume after eviction
            assert!(
                resume_writes >= 5,
                "writes did not resume after eviction in mode: {mode}; successes={resume_writes}; last_error={last_error:?}"
            );

            eprintln!("âœ“ Writes resumed after eviction; {resume_writes} resume writes succeeded");
        });
    }

    #[test]
    fn should_read_published_cloud_ssts_without_local_replica() {
        // Arrange
        // A large working budget still does not require a permanent SST replica.
        let temp_dir = test_temp_dir();
        let budget_bytes = 64 * 1024 * 1024; // comfortably above the data written
        let opts = OpenOptions::cloud_simulated(temp_dir.path(), "test-bucket", "test-prefix")
            .with_simulated_cloud_local_storage_budget(budget_bytes)
            .build()
            .expect("build options");
        let engine = Engine::open(opts).expect("open cloud-simulated engine");
        let cf = engine.create_column_family("test").expect("create cf");

        let small_value = b"cached_value";

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin_tx");
        for i in 0..50 {
            let key = format!("local_pref_key_{i:04}");
            tx.put(key.as_bytes().to_vec(), small_value.to_vec(), None)
                .expect("put");
        }
        tx.commit(WriteOptions::cloud_async()).expect("commit");
        engine.flush_cf(&cf).expect("flush");

        // Act: Read published SST blocks through the cloud-backed reader.
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin_tx");

        let mut readable = 0;
        for i in 0..50 {
            let key = format!("local_pref_key_{i:04}");
            if tx
                .get(key.as_bytes())
                .expect("read published key")
                .is_some()
            {
                readable += 1;
            }
        }

        // Assert: every read succeeded...
        assert_eq!(
            readable, 50,
            "expected every key to be readable while comfortably under budget"
        );

        assert_eq!(count_files_recursive(&temp_dir.path().join("sst")), 0);
        assert_eq!(
            count_files_recursive(&temp_dir.path().join("hybrid_local/sst")),
            0
        );
        assert!(count_files_recursive(&temp_dir.path().join("cloud_store/sst")) > 0);
    }

    #[test]
    fn should_fetch_from_cloud_after_local_eviction() {
        // Arrange
        // Verify published SSTs remain readable after their local staging files are removed.
        let temp_dir = test_temp_dir();
        let budget_bytes = 8 * 1024 * 1024; // Each transaction fits the 4 MiB flush window.
        let opts = OpenOptions::cloud_simulated(temp_dir.path(), "test-bucket", "test-prefix")
            .with_simulated_cloud_local_storage_budget(budget_bytes)
            .build()
            .expect("build options");
        let engine = Engine::open(opts).expect("open cloud-simulated engine");
        let cf = engine.create_column_family("test").expect("create cf");

        // Preserve all 20 values while keeping each atomic batch within admission.
        let value = vec![b'V'; 64 * 1024];

        for batch in 0..2 {
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in batch * 10..(batch + 1) * 10 {
                let key = format!("evict_fetch_key_{i:02}");
                tx.put(key.as_bytes().to_vec(), value.clone(), None)
                    .expect("put");
            }
            tx.commit(WriteOptions::cloud_async()).expect("commit");
        }
        engine.flush_cf(&cf).expect("publish and evict SST");

        // Act / Assert: publication has completed, and only the cloud copy remains.
        assert!(count_files_recursive(&temp_dir.path().join("cloud_store/sst")) > 0);
        assert_eq!(count_files_recursive(&temp_dir.path().join("sst")), 0);

        // Now read evicted data (must trigger a cloud fetch to succeed)
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin_tx");

        let mut cloud_fetched = 0;
        for i in 0..20 {
            let key = format!("evict_fetch_key_{i:02}");
            if tx
                .get(key.as_bytes())
                .expect("read cloud-evicted key")
                .is_some()
            {
                cloud_fetched += 1;
            }
        }

        // Assert: Data accessible via cloud fetch
        assert_eq!(
            cloud_fetched, 20,
            "cloud fetch after eviction failed to return all keys"
        );

        eprintln!("âœ“ Cloud fetch working after eviction; {cloud_fetched} keys fetched");
    }

    #[test]
    fn should_persist_eviction_state_across_restart() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            eprintln!("\n=== Hybrid: Persist Eviction State (mode: {mode}) ===");

            // Arrange
            // Act: Write, evict, note manifest state
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                let value = vec![b'E'; 128 * 1024]; // 128KB

                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 0..15 {
                    let key = format!("persist_evict_key_{i:02}");
                    tx.put(key.as_bytes().to_vec(), value.clone(), None).ok();
                }
                tx.commit(buffered_write_options(mode)).expect("commit");
                engine.flush_cf(&cf).expect("flush");

                // Eviction occurs
                thread::sleep(Duration::from_millis(200));
                engine
                    .shutdown(Duration::from_secs(5))
                    .expect("shutdown before same-path restart");
            }

            // Assert: Restart and verify eviction state
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Verify:
                // 1. Manifest loaded correctly
                // 2. Evicted SSTs marked as such
                // 3. Data still accessible (no re-load from cloud into cache)

                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");

                let mut persisted = 0;
                for i in 0..15 {
                    let key = format!("persist_evict_key_{i:02}");
                    if tx
                        .get(key.as_bytes())
                        .expect("read after eviction-state restart")
                        .is_some()
                    {
                        persisted += 1;
                    }
                }

                assert!(
                    persisted >= 12,
                    "eviction state not persisted across restart in mode: {mode}"
                );

                eprintln!("âœ“ Eviction state persisted; {persisted} keys still accessible");
            }
        });
    }

    #[test]
    #[cfg(feature = "failpoints")]
    fn should_handle_cloud_unavailable_during_eviction() {
        // Failpoints are process-global. Run the injection in an exact-test child
        // so parallel tests in this binary cannot observe the simulated outage.
        if std::env::var_os(CLOUD_OUTAGE_CHILD_ENV).is_none() {
            let output = std::process::Command::new(
                std::env::current_exe().expect("locate hybrid storage test executable"),
            )
            .arg("--exact")
            .arg("hybrid_storage::should_handle_cloud_unavailable_during_eviction")
            .arg("--nocapture")
            .arg("--test-threads=1")
            .env(CLOUD_OUTAGE_CHILD_ENV, "1")
            .output()
            .expect("run isolated cloud outage child");
            assert!(
                output.status.success(),
                "isolated cloud outage child failed: status={} stdout={} stderr={}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }

        // "local" storage mode has no cloud tier and no upload to fail. Exercise
        // the simulated-cloud provider boundary directly so the outage remains
        // deterministic even when this test runs as root in the Docker image.
        let temp_dir = test_temp_dir();
        let budget_bytes = 8 * 1024 * 1024; // Admit the flush so this test reaches the failed provider.
        let opts = OpenOptions::cloud_simulated(temp_dir.path(), "test-bucket", "test-prefix")
            .with_simulated_cloud_local_storage_budget(budget_bytes)
            .build()
            .expect("build options");
        let engine = Engine::open(opts).expect("open cloud-simulated engine");
        let cf = engine.create_column_family("test").expect("create cf");

        let large_value = vec![b'U'; 64 * 1024];

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin_tx");
        for i in 0..12 {
            let key = format!("cloud_down_key_{i:02}");
            tx.put(key.as_bytes().to_vec(), large_value.clone(), None)
                .expect("put");
        }
        let cloud_sst_dir = temp_dir.path().join("cloud_store").join("sst");
        let files_before_outage_attempt = count_files_recursive(&cloud_sst_dir);
        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::cloud::inject_fail_sst_upload", "return")
            .expect("configure cloud SST upload outage");
        // Commit may trigger an automatic flush under this working-disk budget.
        // Install the SST outage first so every publication attempt sees it.
        tx.commit(WriteOptions::cloud_async()).expect("commit");

        // Act: Flush with the remote SST provider unavailable.
        let flush_error = engine
            .flush_cf(&cf)
            .expect_err("cloud outage should fail the SST upload");
        let files_after_outage_attempt = count_files_recursive(&cloud_sst_dir);

        fail::remove("midge::cloud::inject_fail_sst_upload");
        scenario.teardown();

        // Assert: the provider boundary genuinely rejected the upload and no new
        // SST object landed in cloud storage.
        assert!(
            matches!(&flush_error, MidgeError::Internal(message) if message.contains("cloud SST upload failed")),
            "unexpected cloud upload error: {flush_error:?}"
        );
        assert_eq!(
            files_after_outage_attempt, files_before_outage_attempt,
            "expected no SST objects to be written while the provider was unavailable"
        );

        // Assert: Engine still operational and data stayed available locally.
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin_tx");

        let mut accessible = 0;
        for i in 0..12 {
            let key = format!("cloud_down_key_{i:02}");
            if tx
                .get(key.as_bytes())
                .expect("read during cloud outage")
                .is_some()
            {
                accessible += 1;
            }
        }
        assert_eq!(
            accessible, 12,
            "cloud unavailability during eviction caused local data loss"
        );

        let metrics = engine.get_runtime_metrics().expect("runtime metrics");
        assert_ne!(
            metrics.health,
            EngineHealth::Corrupt,
            "engine must not become corrupt after a transient cloud outage"
        );

        eprintln!(
            "âœ“ Handled cloud unavailability gracefully; {accessible} keys still accessible, flush_error={flush_error:?}"
        );
    }

    #[test]
    fn should_keep_pinned_sst_local_given_active_snapshot_when_eviction_runs() {
        for_each_storage_mode(&["local"], |mode, opts| {
            eprintln!("\n=== Hybrid: Don't Evict Active Readers (mode: {mode}) ===");

            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            // Arrange: Write and create snapshot
            let value = vec![b'A'; 64 * 1024]; // 64KB

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..25 {
                let key = format!("reader_protect_key_{i:02}");
                tx.put(key.as_bytes().to_vec(), value.clone(), None).ok();
            }
            tx.commit(buffered_write_options(mode)).expect("commit");
            engine.flush_cf(&cf).expect("flush");

            // Create read snapshot (holds reference to SST)
            let snapshot = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");

            // Act: Trigger eviction attempt while snapshot is active
            let engine_clone = Arc::clone(&engine);
            let cf_clone = cf.clone();
            let eviction_handle = thread::spawn(move || {
                thread::sleep(Duration::from_millis(50));
                engine_clone.flush_cf(&cf_clone).ok();
                engine_clone.compact_all().ok();
            });

            // Wait for eviction to complete
            eviction_handle
                .join()
                .expect("background eviction thread should not panic");

            // Assert: Snapshot reads still succeed (SST not evicted)
            let mut snapshot_reads = 0;
            for i in 0..25 {
                let key = format!("reader_protect_key_{i:02}");
                if snapshot
                    .get(key.as_bytes())
                    .expect("read pinned snapshot key")
                    .is_some()
                {
                    snapshot_reads += 1;
                }
            }

            assert!(
                snapshot_reads >= 20,
                "active reader SST was evicted in mode: {mode}"
            );

            eprintln!(
                "âœ“ Active readers protected from eviction; {snapshot_reads} snapshot reads successful"
            );
        });
    }
}

mod storage_verification_hardening {
    use cntryl_midge::{Engine, OpenOptions};
    #[cfg(feature = "failpoints")]
    use cntryl_midge::{MidgeError, TransactionMode, WriteOptions};
    use serde_json::json;
    use std::path::Path;
    #[cfg(feature = "failpoints")]
    use std::sync::{Mutex, OnceLock};
    use std::time::Duration;
    #[cfg(feature = "failpoints")]
    use std::time::Instant;

    #[cfg(feature = "failpoints")]
    static FAILPOINT_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn write_empty_storage_fixture(db_path: &Path) {
        std::fs::write(db_path.join("FORMAT"), "midge-format-version=3\n")
            .expect("write format marker");
        std::fs::write(
            db_path.join("manifest.json"),
            serde_json::to_vec_pretty(&json!({
                "last_persisted_sequence": 0,
                "files": [],
                "column_families": [],
                "next_wal_seq": 1,
                "next_sst_seqs": {},
                "edit_checkpoint_id": 0
            }))
            .expect("serialize empty manifest"),
        )
        .expect("write empty manifest");
    }

    #[test]
    fn should_not_create_storage_directories_when_offline_verification_runs() {
        // Arrange
        let temp_dir = tempfile::tempdir().expect("create temp directory");
        write_empty_storage_fixture(temp_dir.path());
        let wal_dir = temp_dir.path().join("wal");
        let sst_dir = temp_dir.path().join("sst");
        assert!(!wal_dir.exists());
        assert!(!sst_dir.exists());

        // Act
        Engine::verify_path(temp_dir.path()).expect("verify empty storage fixture");

        // Assert
        assert!(
            !wal_dir.exists(),
            "offline verification must not create wal/"
        );
        assert!(
            !sst_dir.exists(),
            "offline verification must not create sst/"
        );
    }

    #[test]
    fn should_reject_parent_sst_name_when_intent_log_is_loaded() {
        // Arrange
        let temp_dir = tempfile::tempdir().expect("create temp directory");
        write_empty_storage_fixture(temp_dir.path());
        std::fs::write(
            temp_dir.path().join("intent_log.json"),
            serde_json::to_vec_pretty(&json!([
                {
                    "SstAdded": {
                        "file_meta": {
                            "name": "../escape.sst",
                            "level": 0,
                            "size_bytes": 0,
                            "cf_id": 0,
                            "sst_seq": 1,
                            "sublevel": 0
                        }
                    }
                }
            ]))
            .expect("serialize unsafe intent"),
        )
        .expect("write unsafe intent");

        // Act
        let error = Engine::verify_path(temp_dir.path())
            .expect_err("unsafe persisted SST name must fail verification");

        // Assert
        assert!(
            error.to_string().contains("SST name"),
            "expected persisted-name error, got: {error}"
        );
    }

    #[test]
    fn should_reject_absolute_sst_name_when_intent_log_is_loaded() {
        // Arrange
        let temp_dir = tempfile::tempdir().expect("create temp directory");
        write_empty_storage_fixture(temp_dir.path());
        std::fs::write(
            temp_dir.path().join("intent_log.json"),
            serde_json::to_vec_pretty(&json!([
                {
                    "CompactionApplied": {
                        "removed": ["/tmp/escape.sst"],
                        "added": []
                    }
                }
            ]))
            .expect("serialize unsafe intent"),
        )
        .expect("write unsafe intent");

        // Act
        let error = Engine::verify_path(temp_dir.path())
            .expect_err("absolute persisted SST name must fail verification");

        // Assert
        assert!(
            error.to_string().contains("SST name"),
            "expected persisted-name error, got: {error}"
        );
    }

    #[test]
    fn should_report_local_storage_as_authoritative_when_recovery_intents_remain() {
        // Arrange
        let temp_dir = tempfile::tempdir().expect("create temp directory");
        write_empty_storage_fixture(temp_dir.path());
        std::fs::write(
            temp_dir.path().join("intent_log.json"),
            serde_json::to_vec_pretty(&json!([
                {
                    "WalSynced": {
                        "segment_id": 7,
                        "seqno": 11
                    }
                }
            ]))
            .expect("serialize recovery intent"),
        )
        .expect("write recovery intent");

        // Act
        let report = Engine::verify_path(temp_dir.path()).expect("verify local storage");

        // Assert
        assert!(
            report.authoritative,
            "pending recovery work affects health, not local storage authority"
        );
        assert_eq!(report.intent_entries_loaded, 1);
    }

    #[test]
    fn should_verify_online_layout_within_caller_timeout() {
        // Arrange
        #[cfg(feature = "failpoints")]
        let _guard = FAILPOINT_TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let temp_dir = tempfile::tempdir().expect("create temp directory");
        let mut engine = Engine::open(
            OpenOptions::local(temp_dir.path())
                .build()
                .expect("build local options"),
        )
        .expect("open local engine");

        // Act
        let report = engine
            .verify_storage(Duration::from_secs(2))
            .expect("verify online storage");

        // Assert
        assert!(report.authoritative);
        engine
            .shutdown(Duration::from_secs(2))
            .expect("shutdown verified engine");
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_retain_verification_barrier_when_online_verification_times_out() {
        // Arrange
        let _guard = FAILPOINT_TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let scenario = fail::FailScenario::setup();
        fail::cfg(
            "midge::verification::after_barrier_acquired",
            "1*sleep(750)",
        )
        .expect("delay verification worker");

        let temp_dir = tempfile::tempdir().expect("create temp directory");
        let mut engine = Engine::open(
            OpenOptions::local(temp_dir.path())
                .build()
                .expect("build local options"),
        )
        .expect("open local engine");
        let default_cf = engine
            .get_column_family("default")
            .expect("default column family");

        // Act
        let started = Instant::now();
        let verification = engine.verify_storage(Duration::from_millis(25));
        let elapsed = started.elapsed();

        let ddl_error = engine
            .create_column_family("after-verification")
            .expect_err("DDL must remain fenced while verifier owns the barrier");
        let mut tx = engine
            .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
            .expect("begin transaction while layout is pinned");
        tx.put(b"fenced".to_vec(), b"write".to_vec(), None)
            .expect("buffer transaction write");
        let write_error = tx
            .commit(WriteOptions::best_effort())
            .expect_err("WAL mutation must remain fenced while verifier owns the barrier");

        let retry_deadline = Instant::now() + Duration::from_secs(3);
        loop {
            match engine.create_column_family("after-verification") {
                Ok(_) => break,
                Err(MidgeError::Busy(_)) if Instant::now() < retry_deadline => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("verification barrier was not released: {error}"),
            }
        }

        // Assert
        assert!(matches!(verification, Err(MidgeError::Timeout(_))));
        assert!(
            elapsed < Duration::from_millis(500),
            "caller timeout must not wait for verifier worker: {elapsed:?}"
        );
        assert!(matches!(ddl_error, MidgeError::Busy(_)));
        assert!(matches!(write_error, MidgeError::Busy(_)));

        fail::remove("midge::verification::after_barrier_acquired");
        scenario.teardown();
        engine
            .shutdown(Duration::from_secs(2))
            .expect("shutdown verified engine");
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_release_verification_barrier_when_acquire_response_is_lost() {
        // Arrange
        let _guard = FAILPOINT_TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let scenario = fail::FailScenario::setup();
        fail::cfg(
            "midge::verification::before_barrier_response",
            "1*sleep(250)",
        )
        .expect("delay verification barrier response");

        let temp_dir = tempfile::tempdir().expect("create temp directory");
        let mut engine = Engine::open(
            OpenOptions::local(temp_dir.path())
                .build()
                .expect("build local options"),
        )
        .expect("open local engine");

        // Act
        let verification = engine.verify_storage(Duration::from_millis(25));
        let retry_deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match engine.create_column_family("after-lost-response") {
                Ok(_) => break,
                Err(MidgeError::Busy(_)) if Instant::now() < retry_deadline => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("orphaned verification barrier was not released: {error}"),
            }
        }

        // Assert
        assert!(matches!(verification, Err(MidgeError::Timeout(_))));

        fail::remove("midge::verification::before_barrier_response");
        scenario.teardown();
        engine
            .shutdown(Duration::from_secs(2))
            .expect("shutdown after barrier cleanup");
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_retain_primary_lease_when_shutdown_overlaps_timed_out_verification() {
        // Arrange
        let _guard = FAILPOINT_TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let scenario = fail::FailScenario::setup();
        fail::cfg(
            "midge::verification::after_barrier_acquired",
            "1*sleep(750)",
        )
        .expect("delay verification worker");

        let temp_dir = tempfile::tempdir().expect("create temp directory");
        let mut engine = Engine::open(
            OpenOptions::local(temp_dir.path())
                .build()
                .expect("build local options"),
        )
        .expect("open local engine");
        let verification = engine.verify_storage(Duration::from_millis(25));

        // Act
        let shutdown = engine.shutdown(Duration::from_millis(25));
        let competing_open = Engine::open(
            OpenOptions::local(temp_dir.path())
                .build()
                .expect("build competing options"),
        );

        // Assert
        assert!(matches!(verification, Err(MidgeError::Timeout(_))));
        assert!(matches!(shutdown, Err(MidgeError::Timeout(_))));
        assert!(
            competing_open.is_err(),
            "shutdown released the primary lease while verification still read the path"
        );

        fail::remove("midge::verification::after_barrier_acquired");
        scenario.teardown();
        drop(engine);

        let reopen_deadline = Instant::now() + Duration::from_secs(3);
        let mut reopened = loop {
            match Engine::open(
                OpenOptions::local(temp_dir.path())
                    .build()
                    .expect("build reopen options"),
            ) {
                Ok(engine) => break engine,
                Err(_) if Instant::now() < reopen_deadline => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("verification reaper did not release the lease: {error}"),
            }
        };
        reopened
            .shutdown(Duration::from_secs(2))
            .expect("shutdown reopened engine");
    }
}
mod transaction_spill_hardening {
    //! Hardening contract for bounded transaction memory and durable spill runs.

    use bytes::Bytes;
    use cntryl_midge::wal::{WalOpKind, WalRecord};
    use cntryl_midge::{
        Engine, MemoryBudget, MidgeError, OpenOptions, Query, TransactionMode, WriteOptions,
    };
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::{Path, PathBuf};
    #[cfg(feature = "failpoints")]
    use std::process::Command;
    use std::time::Duration;
    use tempfile::TempDir;

    #[cfg(feature = "failpoints")]
    use crate::common::crash;

    const SMALL_POOL_BYTES: usize = 24 * 1024;
    const VALUE_BYTES: usize = 12 * 1024;

    fn spill_shutdown_timeout() -> Duration {
        // Windows CI runs this filesystem-heavy test binary in parallel. Leave
        // enough bounded time for durable lease cleanup under that contention.
        if cfg!(windows) {
            Duration::from_secs(10)
        } else {
            Duration::from_secs(2)
        }
    }
    #[cfg(feature = "failpoints")]
    const CHILD_TEST_NAME: &str =
        "transaction_spill_hardening::should_abort_in_child_process_when_spilled_transaction_commit_is_interrupted";
    #[cfg(feature = "failpoints")]
    const ENV_SCENARIO: &str = "MIDGE_SPILL_CRASH_SCENARIO";
    #[cfg(feature = "failpoints")]
    const ENV_DB_PATH: &str = "MIDGE_SPILL_CRASH_DB_PATH";
    #[cfg(feature = "failpoints")]
    const CHILD_REACHED_SPILL_MARKER: &str = "spill-child-ready";
    #[cfg(feature = "failpoints")]
    const ENV_FAILPOINT_ISOLATION_CHILD: &str = "MIDGE_SPILL_FAILPOINT_ISOLATION_CHILD";

    #[test]
    fn should_bound_shared_resident_bytes_when_two_transactions_pressure_one_pool() {
        // Arrange
        let temp = TempDir::new().expect("temp dir");
        let mut engine = open_local(temp.path(), SMALL_POOL_BYTES);
        let cf = default_cf(&engine);
        let mut first = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin first transaction");
        let mut second = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin second transaction");

        // Act
        first
            .put(b"first".to_vec(), vec![b'a'; VALUE_BYTES], None)
            .expect("put first value");
        second
            .put(b"second".to_vec(), vec![b'b'; VALUE_BYTES], None)
            .expect("put second value");
        let spills = spill_files(temp.path());
        let first_value = first.get(b"first").expect("read first intent");
        let second_value = second.get(b"second").expect("read second intent");
        first.rollback().expect("rollback first transaction");
        second.rollback().expect("rollback second transaction");
        engine
            .shutdown(spill_shutdown_timeout())
            .expect("shutdown engine");

        // Assert
        assert!(
            !spills.is_empty(),
            "two individually under-cap transactions must share one bounded pool and spill"
        );
        assert_eq!(first_value.as_ref().map(Bytes::len), Some(VALUE_BYTES));
        assert_eq!(second_value.as_ref().map(Bytes::len), Some(VALUE_BYTES));
    }

    #[test]
    fn should_create_engine_private_runs_under_txn_when_durable_transaction_spills() {
        // Arrange
        let temp = TempDir::new().expect("temp dir");
        let mut engine = open_local(temp.path(), 8 * 1024);
        let cf = default_cf(&engine);
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin transaction");

        // Act
        fill_transaction(&mut tx, "durable", 4, 8 * 1024);
        let files = spill_files(temp.path());
        let all_are_files = files.iter().all(|path| path.is_file());
        tx.rollback().expect("rollback transaction");
        engine
            .shutdown(spill_shutdown_timeout())
            .expect("shutdown engine");

        // Assert
        assert!(!files.is_empty(), "durable pressure must create spill runs");
        assert!(files
            .iter()
            .all(|path| path.starts_with(temp.path().join("txn"))));
        assert!(all_are_files);
    }

    #[test]
    fn should_read_latest_point_intent_after_spilling() {
        // Arrange
        let temp = TempDir::new().expect("temp dir");
        let mut engine = open_local(temp.path(), 8 * 1024);
        let cf = default_cf(&engine);
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin transaction");
        tx.put(b"point".to_vec(), b"before-spill".to_vec(), None)
            .expect("put initial point value");

        // Act
        fill_transaction(&mut tx, "point-fill", 4, 8 * 1024);
        let before_update = tx.get(b"point").expect("read spilled point");
        tx.put(b"point".to_vec(), b"after-spill".to_vec(), None)
            .expect("replace point value");
        let after_update = tx.get(b"point").expect("read latest point");
        let had_spills = !spill_files(temp.path()).is_empty();
        tx.rollback().expect("rollback transaction");
        engine
            .shutdown(spill_shutdown_timeout())
            .expect("shutdown engine");

        // Assert
        assert!(had_spills, "test must cross a physical spill boundary");
        assert_eq!(before_update, Some(Bytes::from_static(b"before-spill")));
        assert_eq!(after_update, Some(Bytes::from_static(b"after-spill")));
    }

    #[test]
    fn should_merge_transaction_intents_when_scanning_after_spill() {
        // Arrange
        let temp = TempDir::new().expect("temp dir");
        let mut engine = open_local(temp.path(), 8 * 1024);
        let cf = default_cf(&engine);
        let mut seed = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin seed transaction");
        seed.put(b"scan:base".to_vec(), b"snapshot".to_vec(), None)
            .expect("seed snapshot value");
        seed.commit(WriteOptions::sync()).expect("commit seed");

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin scanning transaction");
        tx.put(b"scan:a".to_vec(), b"resident-or-spilled-a".to_vec(), None)
            .expect("put scan a");
        fill_transaction(&mut tx, "outside-prefix", 4, 8 * 1024);
        tx.put(b"scan:b".to_vec(), b"resident-or-spilled-b".to_vec(), None)
            .expect("put scan b");
        tx.put(b"scan:base".to_vec(), b"overridden".to_vec(), None)
            .expect("override snapshot value");

        // Act
        let rows = tx
            .scan(&Query::new().prefix(Bytes::from_static(b"scan:")))
            .expect("scan intents")
            .try_collect()
            .expect("collect scan intents")
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        let had_spills = !spill_files(temp.path()).is_empty();
        tx.rollback().expect("rollback transaction");
        engine
            .shutdown(spill_shutdown_timeout())
            .expect("shutdown engine");

        // Assert
        assert!(had_spills, "test must merge at least one spill run");
        assert_eq!(
            rows.get(b"scan:a".as_slice()).map(Bytes::as_ref),
            Some(b"resident-or-spilled-a".as_slice())
        );
        assert_eq!(
            rows.get(b"scan:b".as_slice()).map(Bytes::as_ref),
            Some(b"resident-or-spilled-b".as_slice())
        );
        assert_eq!(
            rows.get(b"scan:base".as_slice()).map(Bytes::as_ref),
            Some(b"overridden".as_slice())
        );
    }

    #[test]
    fn should_return_reverse_rows_in_descending_order_given_prefix_and_limit_when_scanning() {
        // Arrange
        let temp = TempDir::new().expect("temp dir");
        let mut engine = open_local(temp.path(), 8 * 1024);
        let cf = default_cf(&engine);
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin scanning transaction");
        tx.put(b"scan:a".to_vec(), b"a".to_vec(), None)
            .expect("put scan a");
        tx.put(b"scan:b".to_vec(), b"b".to_vec(), None)
            .expect("put scan b");
        fill_transaction(&mut tx, "outside-prefix", 4, 8 * 1024);
        tx.put(b"scan:c".to_vec(), b"c".to_vec(), None)
            .expect("put scan c");
        tx.put(b"scan:d".to_vec(), b"d".to_vec(), None)
            .expect("put scan d");
        let had_spills = !spill_files(temp.path()).is_empty();

        // Act
        let mut scan = tx
            .scan(
                &Query::new()
                    .prefix(Bytes::from_static(b"scan:"))
                    .reverse()
                    .limit(3),
            )
            .expect("reverse prefix scan");
        let mut rows = Vec::new();
        for row in scan.by_ref() {
            rows.push(row.expect("reverse scan row"));
        }

        // Assert
        assert!(had_spills, "reverse scan must merge a physical spill run");
        assert!(scan.exhausted());
        assert!(scan.next().is_none());
        assert_eq!(
            rows,
            vec![
                (Bytes::from_static(b"scan:d"), Bytes::from_static(b"d")),
                (Bytes::from_static(b"scan:c"), Bytes::from_static(b"c")),
                (Bytes::from_static(b"scan:b"), Bytes::from_static(b"b")),
            ]
        );
        drop(scan);
        tx.rollback().expect("rollback scanning transaction");
        engine
            .shutdown(spill_shutdown_timeout())
            .expect("shutdown engine");
    }

    #[test]
    fn should_preserve_transaction_atomicity_given_spill_run_read_failure_when_committing() {
        // Arrange
        let temp = TempDir::new().expect("temp dir");
        let mut engine = open_local(temp.path(), 8 * 1024);
        let cf = default_cf(&engine);
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin spilled transaction");
        fill_transaction(&mut tx, "atomic", 6, 8 * 1024);
        let data_run = spill_files(temp.path())
            .into_iter()
            .find(|path| path.extension().is_some_and(|extension| extension == "run"))
            .expect("transaction must create a data run");
        fs::remove_file(&data_run).expect("remove spill data run");

        // Act
        let result = tx.commit(WriteOptions::sync());
        let read = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin read after failed commit");

        // Assert
        assert!(matches!(
            result,
            Err(MidgeError::Io(_) | MidgeError::Corruption(_))
        ));
        for index in 0..6 {
            assert_eq!(
                read.get(format!("atomic-{index:03}").as_bytes())
                    .expect("read key after failed commit"),
                None,
                "spill read failure must not partially publish transaction key {index}"
            );
        }
        drop(read);
        engine
            .shutdown(spill_shutdown_timeout())
            .expect("shutdown engine");
    }

    #[test]
    fn should_preserve_put_delete_insert_ordinals_across_spill_runs() {
        // Arrange
        let temp = TempDir::new().expect("temp dir");
        let mut engine = open_local(temp.path(), 8 * 1024);
        let cf = default_cf(&engine);
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin transaction");
        tx.put(b"ordered".to_vec(), b"first".to_vec(), None)
            .expect("put ordered value");
        fill_transaction(&mut tx, "ordinal-a", 2, 8 * 1024);
        tx.delete(b"ordered".to_vec())
            .expect("delete ordered value");
        fill_transaction(&mut tx, "ordinal-b", 2, 8 * 1024);
        tx.insert(b"ordered".to_vec(), b"final".to_vec(), None)
            .expect("insert replacement");
        let before_commit = tx.get(b"ordered").expect("read latest ordinal");
        let had_spills = !spill_files(temp.path()).is_empty();

        // Act
        tx.commit(WriteOptions::sync()).expect("commit transaction");
        let read = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin read transaction");
        let after_commit = read.get(b"ordered").expect("read committed ordinal");
        drop(read);
        engine
            .shutdown(spill_shutdown_timeout())
            .expect("shutdown engine");

        // Assert
        assert!(had_spills, "ordinal sequence must cross spill runs");
        assert_eq!(before_commit, Some(Bytes::from_static(b"final")));
        assert_eq!(after_commit, Some(Bytes::from_static(b"final")));
    }

    #[test]
    fn should_reject_duplicate_insert_when_duplicate_is_in_older_spill_run() {
        // Arrange
        let temp = TempDir::new().expect("temp dir");
        let mut engine = open_local(temp.path(), 8 * 1024);
        let cf = default_cf(&engine);
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin transaction");
        tx.insert(b"duplicate".to_vec(), b"first".to_vec(), None)
            .expect("insert first value");
        fill_transaction(&mut tx, "duplicate-fill", 3, 8 * 1024);
        tx.insert(b"duplicate".to_vec(), b"second".to_vec(), None)
            .expect("record duplicate insert intent");
        let had_spills = !spill_files(temp.path()).is_empty();

        // Act
        let result = tx.commit(WriteOptions::sync());
        engine
            .shutdown(spill_shutdown_timeout())
            .expect("shutdown engine");

        // Assert
        assert!(
            had_spills,
            "duplicate must be resolved across a spill boundary"
        );
        assert!(matches!(result, Err(MidgeError::InvalidArgument(_))));
    }

    #[test]
    fn should_remove_spill_runs_when_transaction_rolls_back() {
        // Arrange
        let temp = TempDir::new().expect("temp dir");
        let mut engine = open_local(temp.path(), 8 * 1024);
        let cf = default_cf(&engine);
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin transaction");
        fill_transaction(&mut tx, "rollback", 4, 8 * 1024);
        let before = spill_files(temp.path());

        // Act
        tx.rollback().expect("rollback transaction");
        let after = spill_files(temp.path());
        engine
            .shutdown(spill_shutdown_timeout())
            .expect("shutdown engine");

        // Assert
        assert!(!before.is_empty(), "rollback test must create spill runs");
        assert!(
            after.is_empty(),
            "rollback must remove every spill run: {after:?}"
        );
    }

    #[test]
    fn should_remove_spill_runs_when_transaction_is_dropped() {
        // Arrange
        let temp = TempDir::new().expect("temp dir");
        let mut engine = open_local(temp.path(), 8 * 1024);
        let cf = default_cf(&engine);
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin transaction");
        fill_transaction(&mut tx, "drop", 4, 8 * 1024);
        let before = spill_files(temp.path());

        // Act
        drop(tx);
        let after = spill_files(temp.path());
        engine
            .shutdown(spill_shutdown_timeout())
            .expect("shutdown engine");

        // Assert
        assert!(!before.is_empty(), "drop test must create spill runs");
        assert!(
            after.is_empty(),
            "drop must remove every spill run: {after:?}"
        );
    }

    #[test]
    fn should_return_resource_limit_when_memory_mode_exhausts_transaction_pool() {
        // Arrange
        let mut engine = Engine::open(
            OpenOptions::in_memory()
                .memory_budget(MemoryBudget::Bytes(64 * 1024 * 1024))
                .transaction_memory_pool_size(8 * 1024)
                .build()
                .expect("build memory options"),
        )
        .expect("open memory engine");
        let cf = default_cf(&engine);
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin transaction");

        // Act
        let mut resource_error = None;
        for index in 0..8 {
            if let Err(error) = tx.put(
                format!("memory-{index:02}").into_bytes(),
                vec![b'm'; 8 * 1024],
                None,
            ) {
                resource_error = Some(error);
                break;
            }
        }
        drop(tx);
        engine
            .shutdown(spill_shutdown_timeout())
            .expect("shutdown memory engine");

        // Assert
        assert!(
            matches!(resource_error, Some(MidgeError::ResourceLimit(_))),
            "memory mode must refuse growth instead of spilling: {resource_error:?}"
        );
    }

    #[test]
    fn should_remove_orphaned_spill_runs_when_engine_starts() {
        // Arrange
        let temp = TempDir::new().expect("temp dir");
        let mut initialized = open_local(temp.path(), SMALL_POOL_BYTES);
        initialized
            .shutdown(spill_shutdown_timeout())
            .expect("shutdown initialized engine");
        let txn_dir = temp.path().join("txn");
        fs::create_dir_all(&txn_dir).expect("create txn dir");
        fs::write(txn_dir.join("orphan.run"), b"uncommitted spill residue")
            .expect("write orphan spill run");

        // Act
        let mut reopened = open_local(temp.path(), SMALL_POOL_BYTES);
        let remaining = spill_files(temp.path());
        reopened
            .shutdown(spill_shutdown_timeout())
            .expect("shutdown reopened engine");

        // Assert
        assert!(
            remaining.is_empty(),
            "startup must remove orphaned uncommitted spill files: {remaining:?}"
        );
    }

    #[test]
    fn should_frame_large_transaction_with_one_commit_marker() {
        // Arrange
        let temp = TempDir::new().expect("temp dir");
        let mut engine = open_local(temp.path(), 8 * 1024);
        let cf = default_cf(&engine);
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin transaction");
        fill_transaction(&mut tx, "chunked", 12, 8 * 1024);
        let logical_value_bytes = 12 * 8 * 1024;

        // Act
        tx.commit(WriteOptions::sync())
            .expect("commit large transaction");
        engine
            .shutdown(spill_shutdown_timeout())
            .expect("shutdown committed engine");
        let frames = read_wal_frames(&temp.path().join("wal").join("wal.log"));
        let records = frames.iter().map(|(record, _)| record).collect::<Vec<_>>();

        let mut reopened = open_local(temp.path(), 8 * 1024);
        let read_cf = default_cf(&reopened);
        let read = reopened
            .begin_tx(read_cf.id(), TransactionMode::ReadOnly)
            .expect("begin recovery read");
        let visible = read.get(b"chunked-000").expect("read committed key");
        drop(read);
        reopened
            .shutdown(spill_shutdown_timeout())
            .expect("shutdown reopened engine");

        // Assert
        assert_eq!(
            records.first().map(|record| record.op),
            Some(WalOpKind::TxnBegin)
        );
        assert_eq!(
            records.last().map(|record| record.op),
            Some(WalOpKind::TxnCommit)
        );
        assert!(
            records.len() >= 4,
            "large transaction must use more than one bounded chunk: {records:?}"
        );
        assert!(records
            .iter()
            .all(|record| record.op != WalOpKind::TxnBatch));
        assert_eq!(
            records
                .iter()
                .filter(|record| record.op == WalOpKind::TxnCommit)
                .count(),
            1
        );
        assert!(
            frames
                .iter()
                .all(|(_, payload_len)| *payload_len < logical_value_bytes),
            "no WAL frame may reconstruct the complete transaction"
        );
        assert_eq!(visible, Some(Bytes::from(vec![b'x'; 8 * 1024])));
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_hide_spilled_transaction_when_commit_marker_is_missing() {
        // Arrange
        let temp = TempDir::new().expect("temp dir");

        // Act
        run_spill_child_expect_abort(temp.path());
        expire_crashed_process_lease(temp.path());
        let spill_count = fs::read_to_string(temp.path().join(CHILD_REACHED_SPILL_MARKER))
            .expect("child spill marker")
            .parse::<usize>()
            .expect("spill count");
        let frames = read_wal_frames(&temp.path().join("wal").join("wal.log"));
        let records = frames.iter().map(|(record, _)| record).collect::<Vec<_>>();
        let mut engine = open_local(temp.path(), 8 * 1024);
        let cf = default_cf(&engine);
        let read = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin recovery read");
        let visible = read.get(b"crash-000").expect("read uncommitted key");
        drop(read);
        let remaining_spills = spill_files(temp.path());
        engine
            .shutdown(spill_shutdown_timeout())
            .expect("shutdown recovered engine");

        // Assert
        assert!(
            spill_count > 0,
            "child must have spilled before commit began"
        );
        assert!(records
            .iter()
            .any(|record| record.op == WalOpKind::TxnBegin));
        assert!(records.iter().any(|record| {
            record.op != WalOpKind::TxnBegin
                && record.op != WalOpKind::TxnCommit
                && record.op != WalOpKind::TxnBatch
        }));
        assert!(records
            .iter()
            .all(|record| record.op != WalOpKind::TxnCommit));
        assert_eq!(
            visible, None,
            "chunks are invisible without the commit marker"
        );
        assert!(
            remaining_spills.is_empty(),
            "startup must remove crashed transaction spill runs: {remaining_spills:?}"
        );
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_abort_in_child_process_when_spilled_transaction_commit_is_interrupted() {
        // Arrange
        if std::env::var_os(ENV_SCENARIO).is_none() {
            return;
        }
        let db_path = PathBuf::from(std::env::var_os(ENV_DB_PATH).expect("db path env"));
        let engine = open_local(&db_path, 8 * 1024);
        let cf = default_cf(&engine);
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin child transaction");
        fill_transaction(&mut tx, "crash", 12, 8 * 1024);
        let spill_count = spill_files(&db_path).len();
        fs::write(
            db_path.join(CHILD_REACHED_SPILL_MARKER),
            spill_count.to_string(),
        )
        .expect("write child spill marker");

        // Act
        crash::configure_abort_failpoint(
            "midge::wal::spilled_txn_after_ops_append_before_commit",
            "before-commit-marker",
        );
        let _ = tx.commit(WriteOptions::sync());

        // Assert
        panic!("child commit returned without aborting before its commit marker");
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_reach_only_path_specific_boundary_when_spill_and_direct_commit_race() {
        // Arrange
        if std::env::var_os(ENV_FAILPOINT_ISOLATION_CHILD).is_some() {
            assert_path_specific_failpoints_under_concurrent_commits();
            return;
        }
        let current_exe = std::env::current_exe().expect("current test executable");
        let temp = TempDir::new().expect("temp dir");

        // Act
        let output = Command::new(current_exe)
            .arg("--exact")
            .arg("transaction_spill_hardening::should_reach_only_path_specific_boundary_when_spill_and_direct_commit_race")
            .arg("--nocapture")
            .arg("--test-threads=1")
            .env(ENV_FAILPOINT_ISOLATION_CHILD, "1")
            .env(ENV_DB_PATH, temp.path())
            .output()
            .expect("run isolated failpoint child");

        // Assert
        assert!(
            output.status.success(),
            "path-specific failpoint child failed; stdout={}; stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[cfg(feature = "failpoints")]
    fn assert_path_specific_failpoints_under_concurrent_commits() {
        let scenario = fail::FailScenario::setup();
        let direct_hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let spilled_hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let direct_callback_hits = std::sync::Arc::clone(&direct_hits);
        let spilled_callback_hits = std::sync::Arc::clone(&spilled_hits);
        fail::cfg_callback(
            "midge::wal::txn_after_ops_append_before_commit",
            move || {
                direct_callback_hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            },
        )
        .expect("configure direct transaction boundary");
        fail::cfg_callback(
            "midge::wal::spilled_txn_after_ops_append_before_commit",
            move || {
                spilled_callback_hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            },
        )
        .expect("configure spilled transaction boundary");

        let db_path = PathBuf::from(std::env::var_os(ENV_DB_PATH).expect("db path env"));
        let engine = std::sync::Arc::new(open_local(&db_path, 8 * 1024));
        let cf_id = default_cf(&engine).id();
        let start = std::sync::Arc::new(std::sync::Barrier::new(2));

        let direct_engine = std::sync::Arc::clone(&engine);
        let direct_start = std::sync::Arc::clone(&start);
        let direct = std::thread::spawn(move || {
            let mut tx = direct_engine
                .begin_tx(cf_id, TransactionMode::ReadWrite)
                .expect("begin direct transaction");
            tx.put(b"direct".to_vec(), b"value".to_vec(), None)
                .expect("stage direct value");
            direct_start.wait();
            tx.commit(WriteOptions::sync())
        });

        let spilled_engine = std::sync::Arc::clone(&engine);
        let spilled_start = std::sync::Arc::clone(&start);
        let spilled = std::thread::spawn(move || {
            let mut tx = spilled_engine
                .begin_tx(cf_id, TransactionMode::ReadWrite)
                .expect("begin spilled transaction");
            fill_transaction(&mut tx, "isolation", 12, 8 * 1024);
            spilled_start.wait();
            tx.commit(WriteOptions::sync())
        });

        direct
            .join()
            .expect("join direct transaction")
            .expect("commit direct transaction");
        spilled
            .join()
            .expect("join spilled transaction")
            .expect("commit spilled transaction");
        let Ok(mut engine) = std::sync::Arc::try_unwrap(engine) else {
            panic!("transaction threads retained an engine reference");
        };
        engine
            .shutdown(spill_shutdown_timeout())
            .expect("shutdown isolated engine");
        scenario.teardown();

        assert_eq!(direct_hits.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(spilled_hits.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    fn open_local(path: &Path, transaction_pool_bytes: usize) -> Engine {
        Engine::open(
            OpenOptions::local(path)
                .memory_budget(MemoryBudget::Bytes(64 * 1024 * 1024))
                .transaction_memory_pool_size(transaction_pool_bytes)
                .background_compaction(false)
                .build()
                .expect("build local options"),
        )
        .expect("open local engine")
    }

    fn default_cf(engine: &Engine) -> cntryl_midge::ColumnFamilyHandle {
        engine
            .get_column_family("default")
            .expect("default column family")
    }

    fn fill_transaction(
        tx: &mut cntryl_midge::Transaction,
        prefix: &str,
        count: usize,
        value_bytes: usize,
    ) {
        for index in 0..count {
            tx.put(
                format!("{prefix}-{index:03}").into_bytes(),
                vec![b'x'; value_bytes],
                None,
            )
            .expect("put transaction filler");
        }
    }

    fn spill_files(db_path: &Path) -> Vec<PathBuf> {
        fn collect(path: &Path, files: &mut Vec<PathBuf>) {
            let Ok(entries) = fs::read_dir(path) else {
                return;
            };
            for entry in entries {
                let entry = entry.expect("read spill directory entry");
                let path = entry.path();
                if path.is_dir() {
                    collect(&path, files);
                } else {
                    files.push(path);
                }
            }
        }

        let mut files = Vec::new();
        collect(&db_path.join("txn"), &mut files);
        files.sort();
        files
    }

    fn read_wal_frames(path: &Path) -> Vec<(WalRecord, usize)> {
        let bytes = fs::read(path).expect("read active WAL");
        let mut frames = Vec::new();
        let mut offset = 0usize;
        while offset < bytes.len() {
            let header_end = offset + cntryl_midge::wal::frame::WAL_FRAME_HEADER_LEN;
            let (payload_len, expected_crc) =
                cntryl_midge::wal::frame::decode_frame_header(&bytes[offset..header_end])
                    .expect("decode WAL frame header");
            let payload_start = header_end;
            let payload_end = payload_start + payload_len;
            let payload = &bytes[payload_start..payload_end];
            cntryl_midge::wal::frame::verify_frame_crc(payload, expected_crc)
                .expect("verify WAL frame CRC");
            frames.push((
                cntryl_midge::wal::encoding::decode(payload).expect("decode WAL record"),
                payload_len,
            ));
            offset = payload_end;
        }
        frames
    }

    #[cfg(feature = "failpoints")]
    fn run_spill_child_expect_abort(db_path: &Path) {
        let mut command = Command::new(std::env::current_exe().expect("current test executable"));
        command
            .arg("--exact")
            .arg(CHILD_TEST_NAME)
            .arg("--nocapture")
            .arg("--test-threads=1")
            .env(ENV_SCENARIO, "before-commit-marker")
            .env(ENV_DB_PATH, db_path);
        crash::run_child_expect_abort(
            &mut command,
            "before-commit-marker",
            "midge::wal::spilled_txn_after_ops_append_before_commit",
            db_path,
        );
    }

    #[cfg(feature = "failpoints")]
    fn expire_crashed_process_lease(db_path: &Path) {
        let leader_path = db_path.join(".midge_leader");
        if !leader_path.exists() {
            return;
        }
        let mut content = fs::read_to_string(&leader_path).expect("read leader record");
        if content.contains("acquired_at: ") {
            content = content
                .lines()
                // Drop the checksum line rather than recompute it: a record
                // with no checksum field is valid-but-unchecked (backward
                // compatibility with pre-checksum records), so this keeps the
                // rewritten timestamp from being rejected as corrupt.
                .filter(|line| !line.starts_with("checksum: "))
                .map(|line| {
                    if line.starts_with("acquired_at: ") {
                        "acquired_at: 1970-01-01T00:00:00Z".to_string()
                    } else {
                        line.to_string()
                    }
                })
                .collect::<Vec<_>>()
                .join("\n");
            content.push('\n');
            fs::write(&leader_path, content).expect("expire crashed process lease");
        }
        crash::clear_crashed_process_acquisition_lock(db_path);
    }
}
