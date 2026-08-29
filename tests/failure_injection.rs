use bytes::Bytes;
use cntryl_midge::{
    Engine, EngineHealth, MidgeError, OpenOptions, RecoveryPolicy, TransactionMode, WriteOptions,
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
fn should_preserve_range_tombstone_atomicity_given_crash_between_wal_append_and_memtable_apply() {
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
        WriteOptions::cloud_async(),
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
    assert_eq!(
        engine
            .get_runtime_metrics()
            .expect("failed flush metrics")
            .hybrid_total_committed_bytes,
        committed_before_flush,
        "failed cloud attempt must release its exact storage reservation"
    );
    fail::remove("midge::cloud::inject_fail_sst_upload");
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
        .expect("compact_all returns after failure");
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
fn should_publish_single_output_when_retrying_compaction_after_failed_manifest_restart() {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();
    let engine = open_local_engine(db_path);
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
        .expect("compact_all returns after manifest batch failure");
    fail::remove("midge::manifest::inject_no_space_on_compaction_batch_edit");
    scenario.teardown();
    let live_retry = engine.compact_all();

    // Assert
    assert!(matches!(live_retry, Err(MidgeError::Fenced(_))));

    shutdown_engine(engine);

    let reopened = open_local_engine(db_path);
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
        "rollback must retain every authoritative input and remove only the orphan output"
    );

    reopened
        .compact_all()
        .expect("retry compaction after rollback recovery");
    let retried_layout = reopened.get_storage_layout().expect("retried layout");
    assert_eq!(
        retried_layout
            .levels
            .iter()
            .find(|level| level.level == 1)
            .map_or(0, |level| level.file_count),
        1,
        "successful retry must publish exactly one replacement output"
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

    let final_reopen = open_local_engine(db_path);
    let final_cf = default_cf(&final_reopen);
    assert_compaction_batches_visible(&final_reopen, &final_cf, "cmp-recover", 4, 25);
    let final_layout = final_reopen
        .get_storage_layout()
        .expect("final recovered layout");
    assert_eq!(
        final_layout
            .levels
            .iter()
            .find(|level| level.level == 1)
            .map_or(0, |level| level.file_count),
        1,
        "restart must not resurrect the failed output beside the retry output"
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
    let engine = open_local_engine(db_path);
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
        .expect("compact_all returns after intent persistence failure");

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

    let reopened = open_local_engine(db_path);
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
    let engine = open_local_engine(db_path);
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
        .expect("compact_all returns after phase-save failure");

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
            .any(|level| level.level > 0 && level.file_count > 0),
        "durable manifest batch must remain authoritative"
    );
    assert!(live_manifest_sst_count < initial_sst_count);
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

    let reopened = open_local_engine(db_path);
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
            .any(|level| level.level > 0 && level.file_count > 0),
        "intent replay must preserve the journal-authoritative compaction"
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
    let engine = open_local_engine(db_path);
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
        .expect("authoritative compaction completes in degraded state");
    fail::remove("midge::compaction::inject_no_space_on_intent_clear");
    scenario.teardown();
    let followup = engine.compact_all();

    // Assert
    assert!(matches!(followup, Err(MidgeError::Fenced(_))));
    shutdown_engine(engine);
    let reopened = open_local_engine(db_path);
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
        .expect("compact_all returns after required sync failure");
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
        .expect("compact_all returns after manifest batch failure");
    fail::remove("midge::manifest::inject_no_space_on_compaction_batch_edit");
    scenario.teardown();
    assert_eq!(
        count_sst_files(&cloud_root),
        initial_remote_count + 1,
        "failed publication should leave one tracked remote cleanup candidate"
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
        .expect("compact_all returns after manifest batch failure");
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
        .expect("compact_all returns after manifest batch failure");
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
fn should_not_upload_remote_compaction_output_when_intent_save_fails() {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();
    let engine = Engine::open(
        OpenOptions::cloud_simulated(db_path, "test-bucket", "intent-before-upload")
            .background_compaction(false)
            .build()
            .expect("build simulated cloud options"),
    )
    .expect("open simulated cloud engine");
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
    let initial_remote_count = count_sst_files(&cloud_root);
    assert_eq!(initial_remote_count, 4);
    let scenario = fail::FailScenario::setup();
    fail::cfg("midge::intent::inject_no_space_on_save", "return")
        .expect("configure intent persistence failure");

    // Act
    engine
        .compact_all()
        .expect("compact_all returns after intent persistence failure");
    fail::remove("midge::intent::inject_no_space_on_save");
    scenario.teardown();

    // Assert
    assert_eq!(
        count_sst_files(&cloud_root),
        initial_remote_count,
        "remote upload must not precede its durable cleanup obligation"
    );
}

#[test]
fn should_recover_compaction_from_manifest_checkpoint_save_failure_after_batch_journal_success() {
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
    engine.compact_all().expect("compact_all returns");
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

fn assert_acknowledged_exhaustion_fixture(engine: &Engine, cf: &cntryl_midge::ColumnFamilyHandle) {
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
