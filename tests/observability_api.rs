use cntryl_midge::{Engine, EngineHealth, MidgeError, OpenOptions, TransactionMode, WriteOptions};
use std::process::Command;
use tempfile::TempDir;

#[test]
fn should_expose_runtime_metrics_storage_layout_and_verification_for_local_engine() {
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
    engine
        .commit(tx, WriteOptions::best_effort())
        .expect("commit best effort");
    engine.flush_cf(&default_cf).expect("flush default cf");

    let metrics = engine.get_runtime_metrics().expect("runtime metrics");
    assert_eq!(metrics.health, EngineHealth::Healthy);
    assert!(
        metrics.sst_count >= 1,
        "flush should publish at least one SST"
    );
    assert!(
        metrics.manifest_last_persisted_sequence >= metrics.current_sequence,
        "flush should advance manifest durability frontier"
    );
    assert!(
        metrics.wal_append_count >= metrics.wal_fsync_count,
        "WAL append counter should never be below fsync counter"
    );

    let layout = engine.get_storage_layout().expect("storage layout");
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

    let report = engine.verify_storage().expect("verify storage");
    assert!(report.manifest_files_verified >= 1);
    assert!(report.sst_files_verified >= 1);
    assert_eq!(report.health, EngineHealth::Healthy);

    let offline_report = Engine::verify_path(db_path).expect("offline verify path");
    assert_eq!(offline_report.health, EngineHealth::Healthy);
    assert!(offline_report.manifest_files_verified >= 1);
}

#[test]
fn should_reject_storage_verification_in_memory_mode() {
    let engine = Engine::open(OpenOptions::in_memory().build()).expect("open in-memory engine");

    let result = engine.verify_storage();

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
fn should_report_degraded_health_given_obsolete_sst_files_and_json_verification() {
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
    engine
        .commit(tx, WriteOptions::best_effort())
        .expect("commit best effort");
    engine.flush_cf(&default_cf).expect("flush default cf");

    std::fs::write(db_path.join("sst").join("orphan.sst.tmp"), b"orphan-bytes")
        .expect("write orphan file");

    let metrics = engine.get_runtime_metrics().expect("runtime metrics");
    assert_eq!(metrics.health, EngineHealth::Degraded);
    assert!(
        metrics.obsolete_file_backlog >= 1,
        "obsolete file backlog should reflect orphaned SST files"
    );

    let layout = engine.get_storage_layout().expect("storage layout");
    assert_eq!(layout.health, EngineHealth::Degraded);
    assert!(
        layout
            .obsolete_files
            .iter()
            .any(|name| name == "orphan.sst.tmp"),
        "storage layout should report obsolete SST artifacts"
    );

    let report = engine.verify_storage().expect("verify storage");
    assert_eq!(report.health, EngineHealth::Degraded);

    let output = Command::new(env!("CARGO_BIN_EXE_midge"))
        .arg("verify")
        .arg("--json")
        .arg(db_path)
        .output()
        .expect("run midge verify");

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
