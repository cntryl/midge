//! Integration tests for recovery metrics API.
//!
//! Validates engine-level visibility into startup recovery work.

use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
use tempfile::TempDir;

#[test]
fn should_report_wal_recovery_metrics_after_reopen_when_wal_replay_occurs() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    {
        let engine = Engine::open(OpenOptions::local(db_path).build()).expect("open engine");
        let cf = engine
            .get_column_family("default")
            .expect("default column family exists");

        // Commit without explicit flush to force WAL replay on reopen.
        for i in 0..20 {
            let key = format!("recovery-key-{i}").into_bytes();
            let value = format!("recovery-value-{i}").into_bytes();
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin tx");
            tx.put(key, value, None).expect("put");
            engine.commit(tx, WriteOptions::buffered()).expect("commit");
        }
    }

    // Act
    let reopened = Engine::open(OpenOptions::local(db_path).build()).expect("reopen engine");
    let recovery = reopened.get_recovery_metrics().expect("get recovery metrics");

    // Assert
    assert!(
        recovery.wal_recovery_records_replayed > 0,
        "expected WAL recovery to replay at least one record"
    );
    assert!(
        recovery.wal_recovery_bytes_replayed > 0,
        "expected WAL recovery to replay at least one byte"
    );
    assert!(
        recovery.intent_log_replay_runs <= 1,
        "startup performs at most one intent replay run"
    );
}

#[test]
fn should_expose_zero_recovery_metrics_on_fresh_engine_when_no_replay_needed() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    // Act
    let engine = Engine::open(OpenOptions::local(db_path).build()).expect("open engine");
    let recovery = engine.get_recovery_metrics().expect("get recovery metrics");

    // Assert
    assert_eq!(recovery.wal_recovery_records_replayed, 0);
    assert_eq!(recovery.wal_recovery_bytes_replayed, 0);
    assert_eq!(recovery.intent_log_replay_runs, 0);
    assert_eq!(recovery.intent_log_entries_replayed, 0);
}
