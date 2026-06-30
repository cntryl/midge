//! Integration tests for recovery metrics API.
//!
//! Validates engine-level visibility into startup recovery work.

use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
use serde::Serialize;
use std::fs;
use tempfile::TempDir;

#[derive(Serialize)]
enum TestIntentLogEntry {
    WalSynced { segment_id: u64, seqno: u64 },
}

fn initialize_format_marker(db_path: &std::path::Path) {
    let engine = Engine::open(OpenOptions::local(db_path).build()).expect("initialize engine");
    drop(engine);
}

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
            tx.commit(WriteOptions::buffered()).expect("commit");
        }
    }

    // Act
    let reopened = Engine::open(OpenOptions::local(db_path).build()).expect("reopen engine");
    let recovery = reopened
        .get_recovery_metrics()
        .expect("get recovery metrics");

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

#[test]
fn should_report_intent_log_replay_metrics_after_reopen_when_manifest_intents_persisted() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();
    initialize_format_marker(db_path);

    let intents = vec![
        TestIntentLogEntry::WalSynced {
            segment_id: 7,
            seqno: 11,
        },
        TestIntentLogEntry::WalSynced {
            segment_id: 8,
            seqno: 12,
        },
    ];

    let intent_json = serde_json::to_string_pretty(&intents).expect("serialize intent fixture");
    fs::write(db_path.join("intent_log.json"), intent_json).expect("write intent fixture");

    // Act
    let engine = Engine::open(OpenOptions::local(db_path).build()).expect("open engine");
    let recovery = engine.get_recovery_metrics().expect("get recovery metrics");

    // Assert
    assert_eq!(
        recovery.intent_log_replay_runs, 1,
        "expected exactly one intent replay run during startup"
    );
    assert!(
        recovery.intent_log_entries_replayed >= 2,
        "expected fixture intent entries to be replayed"
    );
}
