use cntryl_midge::{Engine, EngineHealth, MidgeError, OpenOptions, TransactionMode, WriteOptions};
use std::process::Command;
use std::sync::{Mutex, OnceLock};
use tempfile::TempDir;

static FAILPOINT_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

#[test]
fn should_expose_local_engine_observability_surfaces() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    let engine = Engine::open(OpenOptions::local(db_path).build()).expect("open engine");
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
    let report = engine.verify_storage().expect("verify storage");
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
    assert_eq!(metrics.cache_hits + metrics.cache_misses, 0);
    assert_eq!(metrics.cloud_async_wal_uploads_failed, 0);
    assert_eq!(metrics.hybrid_pending_evictions, 0);
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
    let engine = Engine::open(OpenOptions::in_memory().build()).expect("open in-memory engine");

    // Act
    let result = engine.verify_storage();

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
            .build(),
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
            .build(),
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

    let engine = Engine::open(OpenOptions::local(db_path).build()).expect("open engine");
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
    let report = engine.verify_storage().expect("verify storage");
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
fn should_ignore_stale_sst_temp_files_on_reopen() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    let engine = Engine::open(OpenOptions::local(db_path).build()).expect("open engine");
    drop(engine);

    std::fs::create_dir_all(db_path.join("sst")).expect("create sst dir");
    std::fs::write(db_path.join("sst").join("orphan.sst.tmp"), b"temp-bytes")
        .expect("write stale temp sst");

    // Act
    let reopened = Engine::open(OpenOptions::local(db_path).build()).expect("reopen engine");
    let metrics = reopened.get_runtime_metrics().expect("runtime metrics");
    let layout = reopened.get_storage_layout().expect("storage layout");
    let report = reopened.verify_storage().expect("verify storage");

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
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    {
        let engine = Engine::open(OpenOptions::local(db_path).build()).expect("open engine");
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
    }

    std::fs::write(db_path.join("sst").join("orphan.sst"), b"orphan-bytes")
        .expect("write orphan sst");

    // Act
    let reopened = Engine::open(OpenOptions::local(db_path).build()).expect("reopen engine");
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

#[test]
fn should_report_degraded_health_when_orphan_sst_cleanup_is_blocked() {
    // Arrange
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    {
        let engine = Engine::open(OpenOptions::local(db_path).build()).expect("open engine");
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
    let reopened = Engine::open(OpenOptions::local(db_path).build()).expect("reopen engine");
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

    let engine = Engine::open(OpenOptions::local(db_path).build()).expect("open engine");
    drop(engine);

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
    let reopened = Engine::open(OpenOptions::local(db_path).build()).expect("reopen engine");
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

fn failpoint_test_lock() -> &'static Mutex<()> {
    FAILPOINT_TEST_LOCK.get_or_init(|| Mutex::new(()))
}
