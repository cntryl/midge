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
fn should_reject_transaction_when_no_space_hits_before_batch_append_and_remain_usable() {
    // Arrange
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
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

    drop(engine);

    let reopened = open_local_engine(db_path);
    let reopened_cf = default_cf(&reopened);
    assert_absent(&reopened, &reopened_cf, b"batch-fail-a");
    assert_absent(&reopened, &reopened_cf, b"batch-fail-b");
    assert_visible(&reopened, &reopened_cf, b"batch-recovery", b"value");
}

#[test]
fn should_not_leak_partial_transaction_when_no_space_hits_before_commit_marker_append() {
    // Arrange
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
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

    drop(engine);

    let reopened = open_local_engine(db_path);
    let reopened_cf = default_cf(&reopened);
    assert_absent(&reopened, &reopened_cf, b"commit-fail-a");
    assert_absent(&reopened, &reopened_cf, b"commit-fail-b");
    assert_absent(&reopened, &reopened_cf, b"commit-fail-c");
}

#[test]
fn should_preserve_existing_keys_when_delete_range_wal_append_hits_no_space() {
    // Arrange
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
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

    drop(engine);

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
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
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

    drop(engine);

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
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
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

    drop(engine);

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
fn should_delete_orphan_flush_sst_on_reopen_when_manifest_append_hits_no_space() {
    // Arrange
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
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

    drop(engine);

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
fn should_restore_sequence_floor_from_flushed_ssts_without_wal_recovery() {
    // Arrange
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
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
    drop(engine);

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
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
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
    drop(engine);

    let replay_scenario = fail::FailScenario::setup();
    fail::cfg("midge::intent::inject_no_space_on_save", "return")
        .expect("configure intent save no-space failpoint");
    let reopened = Engine::open(
        OpenOptions::local(db_path)
            .recovery_policy(RecoveryPolicy::Salvage)
            .build(),
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
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
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
    drop(engine);

    let replay_scenario = fail::FailScenario::setup();
    fail::cfg("midge::intent::inject_no_space_on_save", "return")
        .expect("configure intent save no-space failpoint");
    let error = match Engine::open(
        OpenOptions::local(db_path)
            .recovery_policy(RecoveryPolicy::Strict)
            .build(),
    ) {
        Ok(_) => panic!("strict open should fail when replay cannot clear intent log"),
        Err(error) => error,
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
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
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
    drop(engine);

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
            .build(),
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
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
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
    drop(engine);

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
    let error = match Engine::open(
        OpenOptions::local(db_path)
            .recovery_policy(RecoveryPolicy::Strict)
            .build(),
    ) {
        Ok(_) => panic!("strict open should fail when replay cannot checkpoint manifest"),
        Err(error) => error,
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
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
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

    drop(engine);

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
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();
    let engine = open_local_engine(db_path);
    cntryl_midge::testkit::bench::set_runtime_compaction_enabled(&engine, false)
        .expect("disable automatic compaction during failure-injection setup");
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
    cntryl_midge::testkit::bench::set_runtime_compaction_enabled(&engine, true)
        .expect("enable manual compaction for failure injection");
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

    drop(engine);

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
fn should_publish_compaction_output_on_reopen_when_manifest_batch_append_hits_no_space() {
    // Arrange
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();
    let engine = open_local_engine(db_path);
    cntryl_midge::testkit::bench::set_runtime_compaction_enabled(&engine, false)
        .expect("disable automatic compaction during failure-injection setup");
    let cf = default_cf(&engine);

    for batch in 0..6 {
        for index in 0..25 {
            let key = format!("cmp-recover-b{batch}-k{index:02}");
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

    let initial_layout = engine.get_storage_layout().expect("initial storage layout");
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
    cntryl_midge::testkit::bench::set_runtime_compaction_enabled(&engine, true)
        .expect("enable manual compaction for failure injection");
    engine
        .compact_all()
        .expect("compact_all returns after manifest batch failure");
    fail::remove("midge::manifest::inject_no_space_on_compaction_batch_edit");
    scenario.teardown();

    drop(engine);

    let reopened = open_local_engine(db_path);
    let reopened_cf = default_cf(&reopened);

    // Assert
    for batch in 0..6 {
        for index in 0..25 {
            let key = format!("cmp-recover-b{batch}-k{index:02}");
            let value = format!("value-b{batch}-k{index:02}");
            assert_visible(&reopened, &reopened_cf, key.as_bytes(), value.as_bytes());
        }
    }

    let recovered_layout = reopened
        .get_storage_layout()
        .expect("recovered storage layout");
    assert!(
        recovered_layout
            .levels
            .iter()
            .any(|level| level.level > 0 && level.file_count > 0),
        "recovery should publish the durable compaction output into the manifest"
    );
}

#[test]
fn should_recover_compaction_from_manifest_checkpoint_save_failure_after_batch_journal_success() {
    // Arrange
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();
    let engine = open_local_engine(db_path);
    cntryl_midge::testkit::bench::set_runtime_compaction_enabled(&engine, false)
        .expect("disable automatic compaction during failure-injection setup");
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
    cntryl_midge::testkit::bench::set_runtime_compaction_enabled(&engine, true)
        .expect("enable manual compaction for failure injection");
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

    drop(engine);

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

fn open_local_engine(db_path: &Path) -> Engine {
    Engine::open(OpenOptions::local(db_path).build()).expect("open engine")
}

fn default_cf(engine: &Engine) -> cntryl_midge::ColumnFamilyHandle {
    engine
        .get_column_family("default")
        .expect("default column family")
}

fn seed_range(
    engine: &Engine,
    cf: &cntryl_midge::ColumnFamilyHandle,
    range: std::ops::Range<u32>,
    prefix: &str,
) {
    for index in range {
        let key = format!("{prefix}-{index:02}");
        let value = format!("value-{index:02}");
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin seed tx");
        tx.put(key.into_bytes(), value.into_bytes(), None)
            .expect("put seed value");
        tx.commit(WriteOptions::sync()).expect("commit seed value");
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

fn assert_no_space_like(error: &MidgeError) {
    let error_text = error.to_string().to_ascii_lowercase();
    assert!(
        matches!(error, MidgeError::NoSpace(_)) || error_text.contains("no space"),
        "expected a no-space error, got: {error}"
    );
}

fn count_sst_files(db_path: &Path) -> usize {
    let sst_dir = db_path.join("sst");
    let Ok(entries) = std::fs::read_dir(&sst_dir) else {
        return 0;
    };
    entries
        .flatten()
        .filter(|entry| {
            entry
                .path()
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext.eq_ignore_ascii_case("sst"))
                .unwrap_or(false)
        })
        .count()
}
