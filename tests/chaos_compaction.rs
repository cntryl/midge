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
//!   should_<behavior>_`when_crashing`_<`at_point`>

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

const CHILD_TEST_NAME: &str = "should_crash_in_child_when_compaction_crash_scenario_requested";
const ENV_SCENARIO: &str = "MIDGE_COMPACTION_CHAOS_SCENARIO";
const ENV_DB_PATH: &str = "MIDGE_COMPACTION_CHAOS_DB_PATH";

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
fn should_recover_all_data_when_crashing_after_compaction_output_is_durable_but_before_manifest_publish(
) {
    // Arrange: Create engine and write enough data to trigger compaction
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    // Act: crash after outputs are durable and intented, but before manifest publish.
    let committed = run_child_crash_at_output_durable_before_publish(db_path);

    // Assert
    assert!(
        !committed.is_empty(),
        "expected child to commit data before crash"
    );

    let engine = open_local_engine(db_path);
    let default_cf = default_cf(&engine);

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
fn should_use_manifest_ssts_when_crashing_after_manifest_persist_before_gc() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    // Act: Write data, trigger compaction, crash after manifest persist but before GC
    let committed = run_child_crash_at_manifest_persist_before_gc(db_path);

    // Assert: Manifest is durable, data accessible from new SSTs
    assert!(
        !committed.is_empty(),
        "expected child to commit data before crash"
    );

    let engine = open_local_engine(db_path);
    let default_cf = default_cf(&engine);

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
    let count = tx.scan(&Query::new()).expect("scan").remaining();
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
    let scenario = fail::FailScenario::setup();
    std::panic::set_hook(Box::new(|_| std::process::abort()));

    // Configure failpoint to crash after compaction update but before manifest persist
    fail::cfg(
        "slice6::after_compaction_update_before_manifest_persist",
        "panic",
    )
    .expect("configure failpoint");

    std::mem::forget(scenario);

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
    let scenario = fail::FailScenario::setup();
    std::panic::set_hook(Box::new(|_| std::process::abort()));

    fail::cfg(
        "slice7::after_compaction_output_durable_before_manifest_publish",
        "panic",
    )
    .expect("configure failpoint");

    std::mem::forget(scenario);

    let engine = open_local_engine(db_path);
    let default_cf = default_cf(&engine);

    for batch in 0..5 {
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

    engine.compact_all().expect("compact_all");

    panic!("expected crash at manifest publish failpoint");
}

fn child_create_data_and_compact_with_crash_before_gc(db_path: &Path) {
    let scenario = fail::FailScenario::setup();
    std::panic::set_hook(Box::new(|_| std::process::abort()));

    // Configure failpoint to crash after manifest persist but before GC deletion
    fail::cfg("slice6::after_manifest_persist_before_sst_gc", "panic")
        .expect("configure failpoint");

    std::mem::forget(scenario);

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

    let output = command.output().expect("run child test binary");
    assert!(
        !output.status.success(),
        "child scenario {scenario} unexpectedly exited successfully"
    );
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
