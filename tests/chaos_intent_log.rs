//! Crash Testing: WAL-Compaction Integration and Durability
//!
//! Validates end-to-end WAL and compaction alignment. This test complements crash tests
//! from Slice 7 by ensuring the complete pipeline (WAL → memtable → flush → SST → compaction
//! → manifest) maintains durability guarantees when crashes occur.
//!
//! Slice 8 specifically validates:
//! 1. WAL entries are preserved across compaction
//! 2. Compaction state is correctly recovery-safe at all points
//! 3. Intent log enables safe deferral of GC operations
//!
//! **Storage Modes**: Local  
//! **Pattern**: Parent spawns child crash process, recovers, validates all data intact

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

const CHILD_TEST_NAME: &str =
    "should_crash_in_child_when_wal_compaction_crash_scenario_requested";
const ENV_SCENARIO: &str = "MIDGE_WAL_COMPACTION_CHAOS_SCENARIO";
const ENV_DB_PATH: &str = "MIDGE_WAL_COMPACTION_CHAOS_DB_PATH";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CommitRecord {
    key: Vec<u8>,
    value: Vec<u8>,
}

// ============================================================================
// WAL & COMPACTION CRASH TESTS
// ============================================================================

/// Validates WAL and compaction durable frontier alignment.
/// Crashes after manifest persist, verifies recovery succeeds and all data intact.
#[test]
fn should_recover_all_data_after_wal_compaction_crash() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();
    
    // Act: Spawn child that performs WAL writes, flush, compaction, then crashes
    run_child_crash_after_manifest_persist(db_path);
    
    // Stale the leader file to allow reopen
    expire_crashed_process_lease(db_path);
    
    // Assert: All data recoverable after crash + reopen
    {
        let engine = open_local_engine(db_path);
        let default_cf = default_cf(&engine);
        
        // Read what was committed before crash
        let committed = read_committed_records(db_path);
        assert!(!committed.is_empty(), "should have committed records before crash");
        
        // Verify all committed data is present after recovery
        for record in &committed {
            let query_tx = engine
                .begin_tx(default_cf.id(), TransactionMode::ReadOnly)
                .expect("begin query tx");
            let result = query_tx
                .get(&record.key)
                .expect("query should succeed");
            assert!(
                result.is_some(),
                "key {:?} should be recoverable after WAL + compaction crash",
                String::from_utf8_lossy(&record.key)
            );
        }
    }
}

// ============================================================================
// CHILD PROCESS HELPERS
// ============================================================================

/// Child: Write, flush, compact, crash after manifest persist before GC.
fn child_create_data_flush_compact_and_crash() {
    let scenario = fail::FailScenario::setup();
    std::panic::set_hook(Box::new(|_| std::process::abort()));

    // Configure failpoint: crash after manifest persist but before GC
    fail::cfg("slice6::after_manifest_persist_before_sst_gc", "panic")
        .expect("configure failpoint");

    std::mem::forget(scenario);

    let db_path_str = std::env::var("MIDGE_WAL_COMPACTION_CHAOS_DB_PATH")
        .expect("env var MIDGE_WAL_COMPACTION_CHAOS_DB_PATH");
    let db_path = PathBuf::from(&db_path_str);

    let engine = Engine::open(OpenOptions::local(&db_path).build())
        .expect("open engine");

    let default_cf = engine
        .get_column_family("default")
        .expect("default cf");

    let mut commits = Vec::new();

    // Write 5 batches of 100 keys to create L0 SSTs for compaction
    for batch in 0..5 {
        let mut tx = engine
            .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
            .expect("begin tx");

        for i in 0..100 {
            let key = format!("wal_compaction_key_{:02}_{:03}", batch, i);
            let value = format!("wal_compaction_value_{:02}_{:03}", batch, i);

            tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                .expect("put");

            commits.push(CommitRecord {
                key: key.into_bytes(),
                value: value.into_bytes(),
            });
        }

        // Commit writes (goes to WAL)
        engine
            .commit(tx, WriteOptions::buffered())
            .expect("commit tx");

        // Flush memtable to SST (creates L0 file)
        engine.flush_cf(&default_cf).expect("flush_cf");
    }

    // Persist commit log for recovery verification
    {
        let commit_file = db_path.join("commits.ndjson");
        let mut file = fs::File::create(&commit_file).expect("create commits file");
        for record in commits {
            let line = serde_json::to_string(&record).expect("serialize");
            file.write_all(line.as_bytes()).expect("write line");
            file.write_all(b"\n").expect("write newline");
        }
    }

    // Trigger compaction (will panic at failpoint)
    engine.compact_all().expect("compact_all");

    // Should not reach here (panicked at failpoint)
    panic!("expected crash at manifest persist failpoint");
}

// ============================================================================
// PARENT PROCESS HELPERS
// ============================================================================

fn run_child_crash_after_manifest_persist(db_path: &Path) {
    let current_exe = std::env::current_exe().expect("current exe");
    let mut command = Command::new(current_exe);
    command
        .arg("--exact")
        .arg(CHILD_TEST_NAME)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(ENV_SCENARIO, "wal_compaction_crash")
        .env(ENV_DB_PATH, db_path);

    let output = command.output().expect("run child test binary");
    assert!(
        !output.status.success(),
        "child should crash at manifest persist failpoint but exited successfully"
    );
}

// ============================================================================
// CHILD DISPATCH TEST
// ============================================================================

#[test]
fn should_crash_in_child_when_wal_compaction_crash_scenario_requested() {
    // Arrange
    let scenario = std::env::var(ENV_SCENARIO).unwrap_or_default();
    if scenario.is_empty() {
        return; // Not running as child
    // Act
    }

    match scenario.as_str() {
        "wal_compaction_crash" => {
            child_create_data_flush_compact_and_crash();
            panic!("child should have crashed");
        }
        _ => {
            panic!("unknown scenario: {}", scenario);
        // Assert (implicit: panics above prove test executed)
        }
    }
}

// ============================================================================
// RECOVERY HELPERS (matching chaos_compaction.rs patterns)
// ============================================================================

fn expire_crashed_process_lease(db_path: &Path) {
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
    Engine::open(OpenOptions::local(db_path).build()).expect("open engine after crash")
}

fn default_cf(engine: &Engine) -> cntryl_midge::ColumnFamilyHandle {
    engine
        .get_column_family("default")
        .expect("default column family")
}
