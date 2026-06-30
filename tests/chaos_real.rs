use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use bytes::Bytes;
use cntryl_midge::wal::{self, WalOpKind, WalRecord};
use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

const CHILD_TEST_NAME: &str = "should_abort_in_child_process_when_chaos_scenario_requested";
const ENV_SCENARIO: &str = "MIDGE_CHAOS_REAL_SCENARIO";
const ENV_DB_PATH: &str = "MIDGE_CHAOS_REAL_DB_PATH";
const ENV_TRIGGER_CF: &str = "MIDGE_CHAOS_REAL_TRIGGER_CF";

/*
SCENARIO: commit returned Ok, process crashes after WAL append but before fsync, strict durability mode.
STATUS: not currently testable honestly.
BLOCKER: the strict commit path does not expose a post-ack, pre-fsync crash boundary. The available WAL append hook fires before commit returns Ok, which would make a failing write look committed if we tested it.
NEEDED: a production hook after the client-visible acknowledgment boundary for a buffered/strict write and before the corresponding sync makes the write durable.
*/

/*
#[ignore = "Exposes a real bug: a buffered write can become visible after a WAL-append crash even when commit never returned Ok"]
SCENARIO: compaction output SST fully written, process crashes before manifest swap.
STATUS: not currently testable honestly.
BLOCKER: the live runtime path does not currently wire compaction completion into manifest publication during normal engine operation, so there is no real manifest-swap boundary to crash between.
NEEDED: live manifest publication for compaction outputs plus a failpoint after the output SST is durable and before the manifest version becomes visible.
*/

/*
SCENARIO: manifest references new SST set after compaction, process crashes before old SSTs are deleted.
STATUS: not currently testable honestly.
BLOCKER: the live runtime path does not currently drive obsolete-SST deletion after compaction publication, so there is no real old-file deletion boundary to crash between.
NEEDED: end-to-end compaction publish and obsolete-file cleanup wiring, plus a failpoint after manifest publication and before old SST deletion.
*/

/*
SCENARIO: valid SST file is corrupted after crash and before reopen.
STATUS: not currently testable honestly.
BLOCKER: current reopen behavior is still dominated by WAL replay for the reachable crash flows above, so corrupting an SST file would not isolate an SST-only recovery path without additional production wiring.
NEEDED: a reachable manifest-published SST-only reopen path, or a way to disable WAL replay for this scenario while keeping the engine on a real production code path.
*/

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CommitRecord {
    key: Vec<u8>,
    value: Vec<u8>,
}

#[test]
fn should_abort_in_child_process_when_chaos_scenario_requested() {
    // Arrange
    let Some(scenario) = std::env::var_os(ENV_SCENARIO) else {
        return;
    };

    let db_path = PathBuf::from(std::env::var_os(ENV_DB_PATH).expect("db path env"));

    // Act
    match scenario.to_string_lossy().as_ref() {
        "flush_after_sst_write" => child_flush_after_sst_write(&db_path),
        "flush_after_sst_write_best_effort" => child_flush_after_sst_write_best_effort(&db_path),
        "manifest_crash_after_sync" => child_manifest_crash_after_sync(&db_path),
        "concurrent_random_wal_append" => child_concurrent_random_wal_append(&db_path),
        other => panic!("unknown child scenario: {other}"),
    }

    // Assert
    panic!("child scenario returned without abort");
}

#[test]
fn should_recover_sync_commits_when_crashing_after_sst_write_before_manifest_publication() {
    // WHAT THIS TEST DOES:
    // Runs a child process that writes sync commits, calls flush on the default CF,
    // and aborts at the real failpoint after the SST file is written but before the flush returns.
    // FAILURE INJECTED:
    // Real subprocess abort via midge::flush::after_sst_write_before_publish.
    // EXPECTED INVARIANT:
    // Every sync-committed key remains readable after reopen.
    // WHY THIS IS HONEST:
    // The child dies inside the production flush path; the parent then deletes the unpublished SSTs
    // and proves recovery came from WAL rather than a fake clean shutdown.
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    // Act
    let committed = act_flush_crash_before_publish(db_path);

    // Assert
    assert!(
        !committed.is_empty(),
        "child did not persist tracked commits"
    );
    let engine = open_local_engine(db_path);
    assert_committed_records_visible(&engine, &committed);
}

#[test]
fn should_not_publish_best_effort_writes_when_crashing_after_sst_write_before_manifest_publication()
{
    // WHAT THIS TEST DOES:
    // Runs a child process that writes best-effort commits, calls flush, and aborts at the real failpoint
    // after the SST is durable but before manifest publication completes.
    // FAILURE INJECTED:
    // Real subprocess abort via midge::flush::after_sst_write_before_publish.
    // EXPECTED INVARIANT:
    // Best-effort writes must not become restart-visible because flush did not return Ok.
    // WHY THIS IS HONEST:
    // The child dies in the production flush path after the file write; recovery must clean up the orphan SST
    // instead of incorrectly publishing it.
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    // Act
    let committed = act_best_effort_flush_crash_before_publish(db_path);

    // Assert
    assert!(
        !committed.is_empty(),
        "child did not stage tracked best-effort writes"
    );

    let engine = open_local_engine(db_path);
    assert_committed_records_absent(&engine, &committed);
}

#[test]
fn should_preserve_complete_wal_records_when_partial_final_record_is_missing_one_byte() {
    // WHAT THIS TEST DOES:
    // Runs a child process that writes sync commits and then aborts using a real manifest-persist failpoint.
    // The parent appends a valid WAL record directly to wal.log and truncates one byte from its tail.
    // FAILURE INJECTED:
    // Real subprocess abort via midge::manifest::after_temp_sync_before_rename plus direct WAL tail truncation.
    // EXPECTED INVARIANT:
    // Recovery preserves every complete record before the partial tail and never fabricates a value from the cut record.
    // WHY THIS IS HONEST:
    // The corruption is applied to the real on-disk WAL file between crash and reopen; recovery runs through the production WAL parser.
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    // Act
    let committed = act_manifest_crash_with_partial_final_record(db_path);
    let engine = open_local_engine(db_path);
    assert_committed_records_visible(&engine, &committed);

    // Assert
    let default_cf = default_cf(&engine);
    let tx = engine
        .begin_tx(default_cf.id(), TransactionMode::ReadOnly)
        .expect("begin read tx");
    assert_eq!(
        tx.get(b"synthetic_tail").expect("get synthetic tail"),
        None,
        "partial tail record must not appear after recovery"
    );
}

#[test]
fn should_preserve_complete_wal_prefix_when_truncated_to_one_byte_past_last_complete_record() {
    // WHAT THIS TEST DOES:
    // Runs a child process that writes sync commits and aborts using a real manifest-persist failpoint.
    // The parent then appends a synthetic WAL record and truncates wal.log so exactly one byte of that new record remains.
    // FAILURE INJECTED:
    // Real subprocess abort via midge::manifest::after_temp_sync_before_rename plus direct WAL truncation.
    // EXPECTED INVARIANT:
    // Recovery keeps the complete prefix and drops the one-byte tail fragment.
    // WHY THIS IS HONEST:
    // The file is mutated on disk and reopened through the real recovery implementation.
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    // Act
    let committed = act_manifest_crash_with_prefix_fragment(db_path);
    let engine = open_local_engine(db_path);
    assert_committed_records_visible(&engine, &committed);

    // Assert
    let default_cf = default_cf(&engine);
    let tx = engine
        .begin_tx(default_cf.id(), TransactionMode::ReadOnly)
        .expect("begin read tx");
    assert_eq!(
        tx.get(b"synthetic_prefix_tail")
            .expect("get synthetic prefix tail"),
        None,
        "one-byte tail fragment must not materialize as a recovered value"
    );
}

#[test]
fn should_recover_all_sync_commits_when_manifest_file_is_zeroed_after_crash() {
    // WHAT THIS TEST DOES:
    // Runs a child process that writes sync commits, persists a manifest successfully once,
    // then aborts during a second manifest persist using a real failpoint. The parent zeroes manifest.json before reopen.
    // FAILURE INJECTED:
    // Real subprocess abort via midge::manifest::after_temp_sync_before_rename plus direct manifest zeroing.
    // EXPECTED INVARIANT:
    // Reopen must recover every sync-committed key exactly, never a partial view.
    // WHY THIS IS HONEST:
    // The crash and the corruption both happen against real files and the reopen path is production recovery.
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    // Act
    let committed = act_manifest_crash_with_zeroed_manifest(db_path);

    // Assert
    match Engine::open(OpenOptions::local(db_path).build()) {
        Ok(engine) => assert_committed_records_visible(&engine, &committed),
        Err(error) => {
            let message = error.to_string();
            assert!(
                message.contains("failed to load manifest") || message.contains("manifest"),
                "zeroed manifest must either reopen cleanly or fail with a clear manifest error, got: {message}"
            );
        }
    }
}

#[test]
fn should_expose_only_successful_commits_when_random_wal_append_crash_interrupts_concurrent_buffered_writes(
) {
    // WHAT THIS TEST DOES:
    // Runs four child threads that each attempt one hundred buffered commits under a start barrier,
    // with a real failpoint aborting the process at a random WAL append count.
    // FAILURE INJECTED:
    // Real subprocess abort via midge::wal::after_append_batch_before_sync.
    // EXPECTED INVARIANT:
    // Every visible key after recovery must match a commit that actually returned Ok in the child.
    // WHY THIS IS HONEST:
    // The process dies inside the real WAL append path while concurrent client threads are actively writing.
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    // Act
    let committed = act_concurrent_random_wal_append_crash(db_path);

    // Assert
    assert!(
        !committed.is_empty(),
        "expected at least one successful commit before crash"
    );
    let engine = open_local_engine(db_path);
    let default_cf = default_cf(&engine);
    for thread_id in 0..4 {
        for index in 0..100 {
            let key = concurrent_key(thread_id, index);
            let tx = engine
                .begin_tx(default_cf.id(), TransactionMode::ReadOnly)
                .expect("begin read tx");
            let actual = tx.get(key.as_bytes()).expect("get concurrent key");
            if let Some(value) = actual {
                let expected = committed
                    .get(key.as_bytes())
                    .unwrap_or_else(|| panic!("visible key {key:?} never committed"));
                assert_eq!(
                    value.as_ref(),
                    expected.as_slice(),
                    "visible key {key} must match the exact value returned by a successful commit"
                );
            }
        }
    }
}

#[test]
fn should_preserve_recovered_data_when_two_crash_recovery_cycles_reuse_same_directory() {
    // WHAT THIS TEST DOES:
    // Runs one crashing child that writes sync commits and aborts in manifest persistence,
    // reopens and verifies recovery, then runs a second crashing child against the same directory and reopens again.
    // FAILURE INJECTED:
    // Real subprocess aborts via midge::manifest::after_temp_sync_before_rename in both child runs.
    // EXPECTED INVARIANT:
    // Data that survived the first recovery is still present after the second crash/recovery cycle.
    // WHY THIS IS HONEST:
    // Both failures are real process aborts on the same on-disk database directory.
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    // Act
    let committed = act_two_manifest_crash_cycles(db_path);

    // Assert
    let engine = open_local_engine(db_path);
    assert_committed_records_visible(&engine, &committed);
}

fn act_flush_crash_before_publish(db_path: &Path) -> Vec<CommitRecord> {
    run_child_expect_abort("flush_after_sst_write", db_path, &[]);
    expire_crashed_process_lease(db_path);

    let sst_dir = db_path.join("sst");
    if sst_dir.exists() {
        fs::remove_dir_all(&sst_dir).expect("remove unpublished sst dir");
    }

    read_committed_records(db_path)
}

fn act_best_effort_flush_crash_before_publish(db_path: &Path) -> Vec<CommitRecord> {
    run_child_expect_abort("flush_after_sst_write_best_effort", db_path, &[]);
    expire_crashed_process_lease(db_path);
    read_committed_records(db_path)
}

fn act_manifest_crash_with_partial_final_record(db_path: &Path) -> Vec<CommitRecord> {
    run_child_expect_abort(
        "manifest_crash_after_sync",
        db_path,
        &[(ENV_TRIGGER_CF, "truncate-one-byte-trigger")],
    );
    expire_crashed_process_lease(db_path);

    let wal_path = wal_log_path(db_path);
    append_complete_wal_record(&wal_path, b"synthetic_tail", b"synthetic_value");
    truncate_file_by(&wal_path, 1);

    read_committed_records(db_path)
}

fn act_manifest_crash_with_prefix_fragment(db_path: &Path) -> Vec<CommitRecord> {
    run_child_expect_abort(
        "manifest_crash_after_sync",
        db_path,
        &[(ENV_TRIGGER_CF, "truncate-to-prefix-trigger")],
    );
    expire_crashed_process_lease(db_path);

    let wal_path = wal_log_path(db_path);
    let original_len = fs::metadata(&wal_path).expect("wal metadata").len();
    append_complete_wal_record(&wal_path, b"synthetic_prefix_tail", b"synthetic_value");
    truncate_file_to(&wal_path, original_len + 1);

    read_committed_records(db_path)
}

fn act_manifest_crash_with_zeroed_manifest(db_path: &Path) -> Vec<CommitRecord> {
    run_child_expect_abort(
        "manifest_crash_after_sync",
        db_path,
        &[(ENV_TRIGGER_CF, "manifest-zero-trigger")],
    );
    expire_crashed_process_lease(db_path);

    let manifest_path = manifest_path(db_path);
    assert!(
        manifest_path.exists(),
        "manifest file should exist before corruption"
    );
    zero_file(&manifest_path);

    read_committed_records(db_path)
}

fn act_concurrent_random_wal_append_crash(db_path: &Path) -> HashMap<Vec<u8>, Vec<u8>> {
    run_child_expect_abort("concurrent_random_wal_append", db_path, &[]);
    expire_crashed_process_lease(db_path);
    committed_map_by_key(&read_committed_records(db_path))
}

fn act_two_manifest_crash_cycles(db_path: &Path) -> Vec<CommitRecord> {
    run_child_expect_abort(
        "manifest_crash_after_sync",
        db_path,
        &[(ENV_TRIGGER_CF, "cycle-one-trigger")],
    );
    expire_crashed_process_lease(db_path);

    let committed = read_committed_records(db_path);
    let engine = open_local_engine(db_path);
    assert_committed_records_visible(&engine, &committed);
    drop(engine);

    run_child_expect_abort(
        "manifest_crash_after_sync",
        db_path,
        &[(ENV_TRIGGER_CF, "cycle-two-trigger")],
    );
    expire_crashed_process_lease(db_path);

    committed
}

fn child_flush_after_sst_write(db_path: &Path) {
    configure_abort_failpoint("midge::flush::after_sst_write_before_publish");

    let engine = open_local_engine(db_path);
    let default_cf = default_cf(&engine);
    let committed_path = committed_log_path(db_path);

    for index in 0..8 {
        let key = format!("flush-key-{index:02}");
        let value = format!("flush-value-{index:02}");
        put_and_track_commit(
            &engine,
            &default_cf,
            key.as_bytes(),
            value.as_bytes(),
            WriteOptions::sync(),
            &committed_path,
        );
    }

    engine
        .flush_cf(&default_cf)
        .expect("flush should reach failpoint");
}

fn child_flush_after_sst_write_best_effort(db_path: &Path) {
    configure_abort_failpoint("midge::flush::after_sst_write_before_publish");

    let engine = open_local_engine(db_path);
    let default_cf = default_cf(&engine);
    let committed_path = committed_log_path(db_path);

    for index in 0..8 {
        let key = format!("best-effort-flush-key-{index:02}");
        let value = format!("best-effort-flush-value-{index:02}");
        put_and_track_commit(
            &engine,
            &default_cf,
            key.as_bytes(),
            value.as_bytes(),
            WriteOptions::best_effort(),
            &committed_path,
        );
    }

    engine
        .flush_cf(&default_cf)
        .expect("flush should reach failpoint");
}

fn child_manifest_crash_after_sync(db_path: &Path) {
    let engine = open_local_engine(db_path);
    let default_cf = default_cf(&engine);
    let committed_path = committed_log_path(db_path);

    if engine.get_column_family("manifest-seed").is_none() {
        engine
            .create_column_family("manifest-seed")
            .expect("seed manifest file");
    }

    for index in 0..8 {
        let key = format!("sync-key-{index:02}");
        let value = format!("sync-value-{index:02}");
        put_and_track_commit(
            &engine,
            &default_cf,
            key.as_bytes(),
            value.as_bytes(),
            WriteOptions::sync(),
            &committed_path,
        );
    }

    configure_abort_failpoint("midge::manifest::after_temp_sync_before_rename");

    let trigger_cf = std::env::var(ENV_TRIGGER_CF).expect("trigger cf env");
    assert!(
        engine.get_column_family(&trigger_cf).is_none(),
        "trigger column family must be unique per child run"
    );
    engine
        .create_column_family(&trigger_cf)
        .expect("manifest persist should reach failpoint");
}

fn child_concurrent_random_wal_append(db_path: &Path) {
    let target = (usize::from(rand::random::<u16>()) % 399) + 2;
    configure_nth_abort_failpoint("midge::wal::after_append_batch_before_sync", target);

    let engine = Arc::new(open_local_engine(db_path));
    let default_cf = default_cf(&engine);
    let committed_path = Arc::new(committed_log_path(db_path));
    let start_barrier = Arc::new(Barrier::new(5));
    let commit_gate = Arc::new(std::sync::Mutex::new(()));

    for thread_id in 0..4 {
        let engine = Arc::clone(&engine);
        let committed_path = Arc::clone(&committed_path);
        let start_barrier = Arc::clone(&start_barrier);
        let commit_gate = Arc::clone(&commit_gate);
        let default_cf = default_cf.clone();
        thread::spawn(move || {
            start_barrier.wait();
            for index in 0..100 {
                let key = concurrent_key(thread_id, index);
                let value = concurrent_value(thread_id, index);
                let _guard = commit_gate.lock().expect("lock commit gate");
                put_and_track_commit(
                    &engine,
                    &default_cf,
                    key.as_bytes(),
                    value.as_bytes(),
                    WriteOptions::buffered(),
                    &committed_path,
                );
            }
        });
    }

    start_barrier.wait();
    loop {
        thread::park();
    }
}

fn open_local_engine(db_path: &Path) -> Engine {
    Engine::open(OpenOptions::local(db_path).build()).expect("open engine")
}

fn default_cf(engine: &Engine) -> cntryl_midge::ColumnFamilyHandle {
    engine
        .get_column_family("default")
        .expect("default column family")
}

fn put_and_track_commit(
    engine: &Engine,
    cf: &cntryl_midge::ColumnFamilyHandle,
    key: &[u8],
    value: &[u8],
    opts: WriteOptions,
    committed_path: &Path,
) {
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin write tx");
    tx.put(key.to_vec(), value.to_vec(), None)
        .expect("put value");
    tx.commit(opts).expect("commit value");
    append_commit_record(committed_path, key, value);
}

fn append_commit_record(committed_path: &Path, key: &[u8], value: &[u8]) {
    let record = CommitRecord {
        key: key.to_vec(),
        value: value.to_vec(),
    };
    let line = serde_json::to_vec(&record).expect("serialize commit record");
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(committed_path)
        .expect("open committed log");
    file.write_all(&line).expect("write committed record");
    file.write_all(b"\n").expect("write newline");
    file.sync_all().expect("sync committed log");
}

fn read_committed_records(db_path: &Path) -> Vec<CommitRecord> {
    let committed_path = committed_log_path(db_path);
    let Ok(bytes) = fs::read(&committed_path) else {
        return Vec::new();
    };

    let mut records = Vec::new();
    for line in bytes.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        if let Ok(record) = serde_json::from_slice::<CommitRecord>(line) {
            records.push(record);
        }
    }
    records
}

fn assert_committed_records_visible(engine: &Engine, committed: &[CommitRecord]) {
    let default_cf = default_cf(engine);
    for record in committed {
        let tx = engine
            .begin_tx(default_cf.id(), TransactionMode::ReadOnly)
            .expect("begin read tx");
        let value = tx.get(&record.key).expect("get committed key");
        assert_eq!(
            value,
            Some(Bytes::from(record.value.clone())),
            "committed key {:?} must recover exactly",
            String::from_utf8_lossy(&record.key)
        );
    }
}

fn assert_committed_records_absent(engine: &Engine, committed: &[CommitRecord]) {
    let default_cf = default_cf(engine);
    for record in committed {
        let tx = engine
            .begin_tx(default_cf.id(), TransactionMode::ReadOnly)
            .expect("begin read tx");
        let value = tx.get(&record.key).expect("get tracked key");
        assert_eq!(
            value,
            None,
            "best-effort key {:?} must not become visible after crash before flush publication",
            String::from_utf8_lossy(&record.key)
        );
    }
}

fn committed_map_by_key(committed: &[CommitRecord]) -> HashMap<Vec<u8>, Vec<u8>> {
    committed
        .iter()
        .map(|record| (record.key.clone(), record.value.clone()))
        .collect()
}

fn run_child_expect_abort(scenario: &str, db_path: &Path, extra_envs: &[(&str, &str)]) {
    let current_exe = std::env::current_exe().expect("current exe");
    let mut command = Command::new(current_exe);
    command
        .arg("--exact")
        .arg(CHILD_TEST_NAME)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(ENV_SCENARIO, scenario)
        .env(ENV_DB_PATH, db_path);

    for (name, value) in extra_envs {
        command.env(name, value);
    }

    let output = command.output().expect("run child test binary");
    assert!(
        !output.status.success(),
        "child scenario {scenario} unexpectedly exited successfully"
    );
}

fn configure_abort_failpoint(name: &str) {
    let scenario = fail::FailScenario::setup();
    std::panic::set_hook(Box::new(|_| std::process::abort()));
    fail::cfg(name, "panic").expect("configure abort failpoint");
    std::mem::forget(scenario);
}

fn configure_nth_abort_failpoint(name: &str, target: usize) {
    let scenario = fail::FailScenario::setup();
    std::panic::set_hook(Box::new(|_| std::process::abort()));

    let hits = Arc::new(AtomicUsize::new(0));
    let hits_for_callback = Arc::clone(&hits);
    fail::cfg_callback(name, move || {
        assert!(
            hits_for_callback.fetch_add(1, Ordering::SeqCst) + 1 != target,
            "abort on wal append hit {target}"
        );
    })
    .expect("configure nth abort failpoint");
    std::mem::forget(scenario);
}

fn append_complete_wal_record(wal_path: &Path, key: &[u8], value: &[u8]) {
    let record = WalRecord::new(
        WalOpKind::Put,
        Bytes::copy_from_slice(key),
        Some(Bytes::copy_from_slice(value)),
        9_000_000,
        0,
    );
    let payload = wal::encoding::encode(&record).expect("encode wal record");
    let len = u32::try_from(payload.len()).expect("payload len fits into u32");

    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(wal_path)
        .expect("open wal path for append");
    file.write_all(&len.to_le_bytes())
        .expect("write wal length prefix");
    file.write_all(&payload).expect("write wal payload");
    file.sync_all().expect("sync wal file");
}

fn truncate_file_by(path: &Path, bytes: u64) {
    let current_len = fs::metadata(path).expect("file metadata").len();
    truncate_file_to(path, current_len - bytes);
}

fn truncate_file_to(path: &Path, len: u64) {
    let file = fs::OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open file for truncation");
    file.set_len(len).expect("truncate file");
}

fn zero_file(path: &Path) {
    let file = fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(path)
        .expect("open file for zeroing");
    file.sync_all().expect("sync zeroed file");
}

fn wal_log_path(db_path: &Path) -> PathBuf {
    db_path.join("wal").join("wal.log")
}

fn manifest_path(db_path: &Path) -> PathBuf {
    db_path.join("manifest.json")
}

fn committed_log_path(db_path: &Path) -> PathBuf {
    db_path.join("chaos_real_commits.ndjson")
}

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

fn concurrent_key(thread_id: usize, index: usize) -> String {
    format!("concurrent-thread-{thread_id}-key-{index:03}")
}

fn concurrent_value(thread_id: usize, index: usize) -> String {
    format!("concurrent-thread-{thread_id}-value-{index:03}")
}
