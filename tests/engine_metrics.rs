//! Per-engine runtime metrics.
//!
//! This binary never initializes global telemetry: the documented runtime
//! metrics snapshot must work without it, and must describe one engine.

use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
use std::time::Duration;

fn open(path: &std::path::Path) -> Engine {
    Engine::open(
        OpenOptions::local(path)
            .background_compaction(false)
            .build()
            .expect("options"),
    )
    .expect("open engine")
}

fn write_and_flush(engine: &Engine, round: u32) {
    let cf = engine.get_column_family("default").expect("default cf");
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin write");
    tx.put(format!("key-{round}").into_bytes(), b"value".to_vec(), None)
        .expect("put");
    tx.commit(WriteOptions::sync()).expect("commit");
    engine.flush_cf(&cf).expect("flush");
}

#[test]
fn should_report_compactions_run_per_engine_when_telemetry_not_initialized() {
    // Arrange
    let a_dir = tempfile::tempdir().expect("engine A directory");
    let b_dir = tempfile::tempdir().expect("engine B directory");
    let mut a = open(a_dir.path());
    let mut b = open(b_dir.path());
    for round in 0..3 {
        write_and_flush(&a, round);
        write_and_flush(&b, round);
    }

    // Act
    a.compact_all().expect("compact engine A");
    let a_metrics = a.get_runtime_metrics().expect("engine A metrics");
    let b_metrics = b.get_runtime_metrics().expect("engine B metrics");

    // Assert
    assert!(a_metrics.compactions_run > 0, "{a_metrics:?}");
    assert_eq!(b_metrics.compactions_run, 0, "engine B did not compact");
    assert!(a_metrics.wal_fsync_count > 0, "sync commits fsync the WAL");
    a.shutdown(Duration::from_secs(10)).expect("shutdown A");
    b.shutdown(Duration::from_secs(10)).expect("shutdown B");
}
