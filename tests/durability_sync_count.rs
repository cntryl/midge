use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};

fn open_engine() -> (tempfile::TempDir, Engine, cntryl_midge::ColumnFamilyHandle) {
    cntryl_midge::init_benchmark_telemetry().expect("enable test-visible WAL metrics");
    let temp_dir = tempfile::tempdir().expect("create database directory");
    let engine = Engine::open(
        OpenOptions::local(temp_dir.path())
            .build()
            .expect("build options"),
    )
    .expect("open engine");
    let cf = engine
        .get_column_family("default")
        .expect("default column family");
    (temp_dir, engine, cf)
}

fn wal_fsync_count(engine: &Engine) -> u64 {
    engine
        .get_runtime_metrics()
        .expect("read runtime metrics")
        .wal_fsync_count
}

#[test]
fn should_issue_one_physical_wal_sync_when_non_empty_sync_transaction_commits() {
    // Arrange
    let (_temp_dir, engine, cf) = open_engine();
    let before = wal_fsync_count(&engine);
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin write transaction");
    tx.put(b"key".to_vec(), b"value".to_vec(), None)
        .expect("stage value");

    // Act
    tx.commit(WriteOptions::sync())
        .expect("commit synchronously");
    let after = wal_fsync_count(&engine);

    // Assert
    assert_eq!(
        after.saturating_sub(before),
        1,
        "strict WAL append must own the sole physical sync boundary"
    );
}

#[test]
fn should_issue_one_physical_wal_sync_when_empty_sync_transaction_commits() {
    // Arrange
    let (_temp_dir, engine, cf) = open_engine();
    let before = wal_fsync_count(&engine);
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin empty write transaction");

    // Act
    tx.commit(WriteOptions::sync())
        .expect("commit empty transaction synchronously");
    let after = wal_fsync_count(&engine);

    // Assert
    assert_eq!(
        after.saturating_sub(before),
        1,
        "an explicit empty synchronous commit must perform exactly one barrier"
    );
}

#[test]
fn should_not_issue_physical_wal_sync_before_buffered_commit_returns() {
    // Arrange
    let (_temp_dir, engine, cf) = open_engine();
    let before = wal_fsync_count(&engine);
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin buffered transaction");
    tx.put(b"buffered-key".to_vec(), b"buffered-value".to_vec(), None)
        .expect("stage buffered value");

    // Act
    tx.commit(WriteOptions::buffered())
        .expect("commit to the WAL buffer");
    let after = wal_fsync_count(&engine);

    // Assert
    assert_eq!(
        after.saturating_sub(before),
        0,
        "buffered commit must not introduce a synchronous physical barrier"
    );
}
