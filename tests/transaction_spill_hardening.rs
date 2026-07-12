//! Hardening contract for bounded transaction memory and durable spill runs.

use bytes::Bytes;
use cntryl_midge::wal::{WalOpKind, WalRecord};
use cntryl_midge::{
    Engine, MemoryBudget, MidgeError, OpenOptions, Query, TransactionMode, WriteOptions,
};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
#[cfg(feature = "failpoints")]
use std::process::Command;
use std::time::Duration;
use tempfile::TempDir;

const SMALL_POOL_BYTES: usize = 24 * 1024;
const VALUE_BYTES: usize = 12 * 1024;

fn spill_shutdown_timeout() -> Duration {
    // Windows CI runs this filesystem-heavy test binary in parallel. Leave
    // enough bounded time for durable lease cleanup under that contention.
    if cfg!(windows) {
        Duration::from_secs(10)
    } else {
        Duration::from_secs(2)
    }
}
#[cfg(feature = "failpoints")]
const CHILD_TEST_NAME: &str =
    "should_abort_in_child_process_when_spilled_transaction_commit_is_interrupted";
#[cfg(feature = "failpoints")]
const ENV_SCENARIO: &str = "MIDGE_SPILL_CRASH_SCENARIO";
#[cfg(feature = "failpoints")]
const ENV_DB_PATH: &str = "MIDGE_SPILL_CRASH_DB_PATH";
#[cfg(feature = "failpoints")]
const CHILD_REACHED_SPILL_MARKER: &str = "spill-child-ready";

#[test]
fn should_bound_shared_resident_bytes_when_two_transactions_pressure_one_pool() {
    // Arrange
    let temp = TempDir::new().expect("temp dir");
    let mut engine = open_local(temp.path(), SMALL_POOL_BYTES);
    let cf = default_cf(&engine);
    let mut first = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin first transaction");
    let mut second = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin second transaction");

    // Act
    first
        .put(b"first".to_vec(), vec![b'a'; VALUE_BYTES], None)
        .expect("put first value");
    second
        .put(b"second".to_vec(), vec![b'b'; VALUE_BYTES], None)
        .expect("put second value");
    let spills = spill_files(temp.path());
    let first_value = first.get(b"first").expect("read first intent");
    let second_value = second.get(b"second").expect("read second intent");
    first.rollback().expect("rollback first transaction");
    second.rollback().expect("rollback second transaction");
    engine
        .shutdown(spill_shutdown_timeout())
        .expect("shutdown engine");

    // Assert
    assert!(
        !spills.is_empty(),
        "two individually under-cap transactions must share one bounded pool and spill"
    );
    assert_eq!(first_value.as_ref().map(Bytes::len), Some(VALUE_BYTES));
    assert_eq!(second_value.as_ref().map(Bytes::len), Some(VALUE_BYTES));
}

#[test]
fn should_create_engine_private_runs_under_txn_when_durable_transaction_spills() {
    // Arrange
    let temp = TempDir::new().expect("temp dir");
    let mut engine = open_local(temp.path(), 8 * 1024);
    let cf = default_cf(&engine);
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin transaction");

    // Act
    fill_transaction(&mut tx, "durable", 4, 8 * 1024);
    let files = spill_files(temp.path());
    let all_are_files = files.iter().all(|path| path.is_file());
    tx.rollback().expect("rollback transaction");
    engine
        .shutdown(spill_shutdown_timeout())
        .expect("shutdown engine");

    // Assert
    assert!(!files.is_empty(), "durable pressure must create spill runs");
    assert!(files
        .iter()
        .all(|path| path.starts_with(temp.path().join("txn"))));
    assert!(all_are_files);
}

#[test]
fn should_read_latest_point_intent_after_spilling() {
    // Arrange
    let temp = TempDir::new().expect("temp dir");
    let mut engine = open_local(temp.path(), 8 * 1024);
    let cf = default_cf(&engine);
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin transaction");
    tx.put(b"point".to_vec(), b"before-spill".to_vec(), None)
        .expect("put initial point value");

    // Act
    fill_transaction(&mut tx, "point-fill", 4, 8 * 1024);
    let before_update = tx.get(b"point").expect("read spilled point");
    tx.put(b"point".to_vec(), b"after-spill".to_vec(), None)
        .expect("replace point value");
    let after_update = tx.get(b"point").expect("read latest point");
    let had_spills = !spill_files(temp.path()).is_empty();
    tx.rollback().expect("rollback transaction");
    engine
        .shutdown(spill_shutdown_timeout())
        .expect("shutdown engine");

    // Assert
    assert!(had_spills, "test must cross a physical spill boundary");
    assert_eq!(before_update, Some(Bytes::from_static(b"before-spill")));
    assert_eq!(after_update, Some(Bytes::from_static(b"after-spill")));
}

#[test]
fn should_merge_transaction_intents_when_scanning_after_spill() {
    // Arrange
    let temp = TempDir::new().expect("temp dir");
    let mut engine = open_local(temp.path(), 8 * 1024);
    let cf = default_cf(&engine);
    let mut seed = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin seed transaction");
    seed.put(b"scan:base".to_vec(), b"snapshot".to_vec(), None)
        .expect("seed snapshot value");
    seed.commit(WriteOptions::sync()).expect("commit seed");

    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin scanning transaction");
    tx.put(b"scan:a".to_vec(), b"resident-or-spilled-a".to_vec(), None)
        .expect("put scan a");
    fill_transaction(&mut tx, "outside-prefix", 4, 8 * 1024);
    tx.put(b"scan:b".to_vec(), b"resident-or-spilled-b".to_vec(), None)
        .expect("put scan b");
    tx.put(b"scan:base".to_vec(), b"overridden".to_vec(), None)
        .expect("override snapshot value");

    // Act
    let rows = tx
        .scan(&Query::new().prefix(Bytes::from_static(b"scan:")))
        .expect("scan intents")
        .try_collect()
        .expect("collect scan intents")
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let had_spills = !spill_files(temp.path()).is_empty();
    tx.rollback().expect("rollback transaction");
    engine
        .shutdown(spill_shutdown_timeout())
        .expect("shutdown engine");

    // Assert
    assert!(had_spills, "test must merge at least one spill run");
    assert_eq!(
        rows.get(b"scan:a".as_slice()).map(Bytes::as_ref),
        Some(b"resident-or-spilled-a".as_slice())
    );
    assert_eq!(
        rows.get(b"scan:b".as_slice()).map(Bytes::as_ref),
        Some(b"resident-or-spilled-b".as_slice())
    );
    assert_eq!(
        rows.get(b"scan:base".as_slice()).map(Bytes::as_ref),
        Some(b"overridden".as_slice())
    );
}

#[test]
fn should_preserve_put_delete_insert_ordinals_across_spill_runs() {
    // Arrange
    let temp = TempDir::new().expect("temp dir");
    let mut engine = open_local(temp.path(), 8 * 1024);
    let cf = default_cf(&engine);
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin transaction");
    tx.put(b"ordered".to_vec(), b"first".to_vec(), None)
        .expect("put ordered value");
    fill_transaction(&mut tx, "ordinal-a", 2, 8 * 1024);
    tx.delete(b"ordered".to_vec())
        .expect("delete ordered value");
    fill_transaction(&mut tx, "ordinal-b", 2, 8 * 1024);
    tx.insert(b"ordered".to_vec(), b"final".to_vec(), None)
        .expect("insert replacement");
    let before_commit = tx.get(b"ordered").expect("read latest ordinal");
    let had_spills = !spill_files(temp.path()).is_empty();

    // Act
    tx.commit(WriteOptions::sync()).expect("commit transaction");
    let read = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin read transaction");
    let after_commit = read.get(b"ordered").expect("read committed ordinal");
    drop(read);
    engine
        .shutdown(spill_shutdown_timeout())
        .expect("shutdown engine");

    // Assert
    assert!(had_spills, "ordinal sequence must cross spill runs");
    assert_eq!(before_commit, Some(Bytes::from_static(b"final")));
    assert_eq!(after_commit, Some(Bytes::from_static(b"final")));
}

#[test]
fn should_reject_duplicate_insert_when_duplicate_is_in_older_spill_run() {
    // Arrange
    let temp = TempDir::new().expect("temp dir");
    let mut engine = open_local(temp.path(), 8 * 1024);
    let cf = default_cf(&engine);
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin transaction");
    tx.insert(b"duplicate".to_vec(), b"first".to_vec(), None)
        .expect("insert first value");
    fill_transaction(&mut tx, "duplicate-fill", 3, 8 * 1024);
    tx.insert(b"duplicate".to_vec(), b"second".to_vec(), None)
        .expect("record duplicate insert intent");
    let had_spills = !spill_files(temp.path()).is_empty();

    // Act
    let result = tx.commit(WriteOptions::sync());
    engine
        .shutdown(spill_shutdown_timeout())
        .expect("shutdown engine");

    // Assert
    assert!(
        had_spills,
        "duplicate must be resolved across a spill boundary"
    );
    assert!(matches!(result, Err(MidgeError::InvalidArgument(_))));
}

#[test]
fn should_remove_spill_runs_when_transaction_rolls_back() {
    // Arrange
    let temp = TempDir::new().expect("temp dir");
    let mut engine = open_local(temp.path(), 8 * 1024);
    let cf = default_cf(&engine);
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin transaction");
    fill_transaction(&mut tx, "rollback", 4, 8 * 1024);
    let before = spill_files(temp.path());

    // Act
    tx.rollback().expect("rollback transaction");
    let after = spill_files(temp.path());
    engine
        .shutdown(spill_shutdown_timeout())
        .expect("shutdown engine");

    // Assert
    assert!(!before.is_empty(), "rollback test must create spill runs");
    assert!(
        after.is_empty(),
        "rollback must remove every spill run: {after:?}"
    );
}

#[test]
fn should_remove_spill_runs_when_transaction_is_dropped() {
    // Arrange
    let temp = TempDir::new().expect("temp dir");
    let mut engine = open_local(temp.path(), 8 * 1024);
    let cf = default_cf(&engine);
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin transaction");
    fill_transaction(&mut tx, "drop", 4, 8 * 1024);
    let before = spill_files(temp.path());

    // Act
    drop(tx);
    let after = spill_files(temp.path());
    engine
        .shutdown(spill_shutdown_timeout())
        .expect("shutdown engine");

    // Assert
    assert!(!before.is_empty(), "drop test must create spill runs");
    assert!(
        after.is_empty(),
        "drop must remove every spill run: {after:?}"
    );
}

#[test]
fn should_return_resource_limit_when_memory_mode_exhausts_transaction_pool() {
    // Arrange
    let mut engine = Engine::open(
        OpenOptions::in_memory()
            .memory_budget(MemoryBudget::Bytes(64 * 1024 * 1024))
            .transaction_memory_pool_size(8 * 1024)
            .build()
            .expect("build memory options"),
    )
    .expect("open memory engine");
    let cf = default_cf(&engine);
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin transaction");

    // Act
    let mut resource_error = None;
    for index in 0..8 {
        if let Err(error) = tx.put(
            format!("memory-{index:02}").into_bytes(),
            vec![b'm'; 8 * 1024],
            None,
        ) {
            resource_error = Some(error);
            break;
        }
    }
    drop(tx);
    engine
        .shutdown(spill_shutdown_timeout())
        .expect("shutdown memory engine");

    // Assert
    assert!(
        matches!(resource_error, Some(MidgeError::ResourceLimit(_))),
        "memory mode must refuse growth instead of spilling: {resource_error:?}"
    );
}

#[test]
fn should_remove_orphaned_spill_runs_when_engine_starts() {
    // Arrange
    let temp = TempDir::new().expect("temp dir");
    let mut initialized = open_local(temp.path(), SMALL_POOL_BYTES);
    initialized
        .shutdown(spill_shutdown_timeout())
        .expect("shutdown initialized engine");
    let txn_dir = temp.path().join("txn");
    fs::create_dir_all(&txn_dir).expect("create txn dir");
    fs::write(txn_dir.join("orphan.run"), b"uncommitted spill residue")
        .expect("write orphan spill run");

    // Act
    let mut reopened = open_local(temp.path(), SMALL_POOL_BYTES);
    let remaining = spill_files(temp.path());
    reopened
        .shutdown(spill_shutdown_timeout())
        .expect("shutdown reopened engine");

    // Assert
    assert!(
        remaining.is_empty(),
        "startup must remove orphaned uncommitted spill files: {remaining:?}"
    );
}

#[test]
fn should_frame_large_transaction_with_one_commit_marker() {
    // Arrange
    let temp = TempDir::new().expect("temp dir");
    let mut engine = open_local(temp.path(), 8 * 1024);
    let cf = default_cf(&engine);
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin transaction");
    fill_transaction(&mut tx, "chunked", 12, 8 * 1024);
    let logical_value_bytes = 12 * 8 * 1024;

    // Act
    tx.commit(WriteOptions::sync())
        .expect("commit large transaction");
    engine
        .shutdown(spill_shutdown_timeout())
        .expect("shutdown committed engine");
    let frames = read_wal_frames(&temp.path().join("wal").join("wal.log"));
    let records = frames.iter().map(|(record, _)| record).collect::<Vec<_>>();

    let mut reopened = open_local(temp.path(), 8 * 1024);
    let read_cf = default_cf(&reopened);
    let read = reopened
        .begin_tx(read_cf.id(), TransactionMode::ReadOnly)
        .expect("begin recovery read");
    let visible = read.get(b"chunked-000").expect("read committed key");
    drop(read);
    reopened
        .shutdown(spill_shutdown_timeout())
        .expect("shutdown reopened engine");

    // Assert
    assert_eq!(
        records.first().map(|record| record.op),
        Some(WalOpKind::TxnBegin)
    );
    assert_eq!(
        records.last().map(|record| record.op),
        Some(WalOpKind::TxnCommit)
    );
    assert!(
        records.len() >= 4,
        "large transaction must use more than one bounded chunk: {records:?}"
    );
    assert!(records
        .iter()
        .all(|record| record.op != WalOpKind::TxnBatch));
    assert_eq!(
        records
            .iter()
            .filter(|record| record.op == WalOpKind::TxnCommit)
            .count(),
        1
    );
    assert!(
        frames
            .iter()
            .all(|(_, payload_len)| *payload_len < logical_value_bytes),
        "no WAL frame may reconstruct the complete transaction"
    );
    assert_eq!(visible, Some(Bytes::from(vec![b'x'; 8 * 1024])));
}

#[cfg(feature = "failpoints")]
#[test]
fn should_hide_spilled_transaction_when_commit_marker_is_missing() {
    // Arrange
    let temp = TempDir::new().expect("temp dir");

    // Act
    run_spill_child_expect_abort(temp.path());
    expire_crashed_process_lease(temp.path());
    let spill_count = fs::read_to_string(temp.path().join(CHILD_REACHED_SPILL_MARKER))
        .expect("child spill marker")
        .parse::<usize>()
        .expect("spill count");
    let frames = read_wal_frames(&temp.path().join("wal").join("wal.log"));
    let records = frames.iter().map(|(record, _)| record).collect::<Vec<_>>();
    let mut engine = open_local(temp.path(), 8 * 1024);
    let cf = default_cf(&engine);
    let read = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin recovery read");
    let visible = read.get(b"crash-000").expect("read uncommitted key");
    drop(read);
    let remaining_spills = spill_files(temp.path());
    engine
        .shutdown(spill_shutdown_timeout())
        .expect("shutdown recovered engine");

    // Assert
    assert!(
        spill_count > 0,
        "child must have spilled before commit began"
    );
    assert!(records
        .iter()
        .any(|record| record.op == WalOpKind::TxnBegin));
    assert!(records.iter().any(|record| {
        record.op != WalOpKind::TxnBegin
            && record.op != WalOpKind::TxnCommit
            && record.op != WalOpKind::TxnBatch
    }));
    assert!(records
        .iter()
        .all(|record| record.op != WalOpKind::TxnCommit));
    assert_eq!(
        visible, None,
        "chunks are invisible without the commit marker"
    );
    assert!(
        remaining_spills.is_empty(),
        "startup must remove crashed transaction spill runs: {remaining_spills:?}"
    );
}

#[cfg(feature = "failpoints")]
#[test]
fn should_abort_in_child_process_when_spilled_transaction_commit_is_interrupted() {
    // Arrange
    if std::env::var_os(ENV_SCENARIO).is_none() {
        return;
    }
    let db_path = PathBuf::from(std::env::var_os(ENV_DB_PATH).expect("db path env"));
    let engine = open_local(&db_path, 8 * 1024);
    let cf = default_cf(&engine);
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin child transaction");
    fill_transaction(&mut tx, "crash", 12, 8 * 1024);
    let spill_count = spill_files(&db_path).len();
    fs::write(
        db_path.join(CHILD_REACHED_SPILL_MARKER),
        spill_count.to_string(),
    )
    .expect("write child spill marker");

    // Act
    let scenario = fail::FailScenario::setup();
    std::panic::set_hook(Box::new(|_| std::process::abort()));
    fail::cfg("midge::wal::txn_after_ops_append_before_commit", "panic")
        .expect("configure crash failpoint");
    std::mem::forget(scenario);
    let _ = tx.commit(WriteOptions::sync());

    // Assert
    panic!("child commit returned without aborting before its commit marker");
}

fn open_local(path: &Path, transaction_pool_bytes: usize) -> Engine {
    Engine::open(
        OpenOptions::local(path)
            .memory_budget(MemoryBudget::Bytes(64 * 1024 * 1024))
            .transaction_memory_pool_size(transaction_pool_bytes)
            .background_compaction(false)
            .build()
            .expect("build local options"),
    )
    .expect("open local engine")
}

fn default_cf(engine: &Engine) -> cntryl_midge::ColumnFamilyHandle {
    engine
        .get_column_family("default")
        .expect("default column family")
}

fn fill_transaction(
    tx: &mut cntryl_midge::Transaction,
    prefix: &str,
    count: usize,
    value_bytes: usize,
) {
    for index in 0..count {
        tx.put(
            format!("{prefix}-{index:03}").into_bytes(),
            vec![b'x'; value_bytes],
            None,
        )
        .expect("put transaction filler");
    }
}

fn spill_files(db_path: &Path) -> Vec<PathBuf> {
    fn collect(path: &Path, files: &mut Vec<PathBuf>) {
        let Ok(entries) = fs::read_dir(path) else {
            return;
        };
        for entry in entries {
            let entry = entry.expect("read spill directory entry");
            let path = entry.path();
            if path.is_dir() {
                collect(&path, files);
            } else {
                files.push(path);
            }
        }
    }

    let mut files = Vec::new();
    collect(&db_path.join("txn"), &mut files);
    files.sort();
    files
}

fn read_wal_frames(path: &Path) -> Vec<(WalRecord, usize)> {
    let bytes = fs::read(path).expect("read active WAL");
    let mut frames = Vec::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        let header_end = offset + cntryl_midge::wal::frame::WAL_FRAME_HEADER_LEN;
        let (payload_len, expected_crc) =
            cntryl_midge::wal::frame::decode_frame_header(&bytes[offset..header_end])
                .expect("decode WAL frame header");
        let payload_start = header_end;
        let payload_end = payload_start + payload_len;
        let payload = &bytes[payload_start..payload_end];
        cntryl_midge::wal::frame::verify_frame_crc(payload, expected_crc)
            .expect("verify WAL frame CRC");
        frames.push((
            cntryl_midge::wal::encoding::decode(payload).expect("decode WAL record"),
            payload_len,
        ));
        offset = payload_end;
    }
    frames
}

#[cfg(feature = "failpoints")]
fn run_spill_child_expect_abort(db_path: &Path) {
    let output = Command::new(std::env::current_exe().expect("current test executable"))
        .arg("--exact")
        .arg(CHILD_TEST_NAME)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(ENV_SCENARIO, "before-commit-marker")
        .env(ENV_DB_PATH, db_path)
        .output()
        .expect("run spill crash child");
    assert!(
        !output.status.success(),
        "spill crash child unexpectedly succeeded: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(feature = "failpoints")]
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
        fs::write(&leader_path, content).expect("expire crashed process lease");
    }
}
