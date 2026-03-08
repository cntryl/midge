use cntryl_midge::{Engine, EngineHealth, MidgeError, OpenOptions, TransactionMode, WriteOptions};
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
