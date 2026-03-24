use bytes::Bytes;
use cntryl_midge::{Engine, IsolationLevel, OpenOptions, TransactionMode, WriteOptions};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::TempDir;

const CHILD_TEST_NAME: &str = "should_abort_in_child_process_when_txn_crash_scenario_requested";
const ENV_SCENARIO: &str = "MIDGE_TXN_CRASH_SCENARIO";
const ENV_DB_PATH: &str = "MIDGE_TXN_CRASH_DB_PATH";

#[derive(Clone, Copy)]
struct ExpectedRecord {
    key: &'static [u8],
    value: &'static [u8],
}

const PRE_COMMIT_RECORDS: &[ExpectedRecord] = &[
    ExpectedRecord {
        key: b"txn-pre-commit-a",
        value: b"value-a",
    },
    ExpectedRecord {
        key: b"txn-pre-commit-b",
        value: b"value-b",
    },
    ExpectedRecord {
        key: b"txn-pre-commit-c",
        value: b"value-c",
    },
];

const POST_SYNC_RECORDS: &[ExpectedRecord] = &[
    ExpectedRecord {
        key: b"txn-post-sync-a",
        value: b"value-a",
    },
    ExpectedRecord {
        key: b"txn-post-sync-b",
        value: b"value-b",
    },
    ExpectedRecord {
        key: b"txn-post-sync-c",
        value: b"value-c",
    },
];

const POST_ACK_RECORDS: &[ExpectedRecord] = &[
    ExpectedRecord {
        key: b"txn-post-ack-a",
        value: b"value-a",
    },
    ExpectedRecord {
        key: b"txn-post-ack-b",
        value: b"value-b",
    },
    ExpectedRecord {
        key: b"txn-post-ack-c",
        value: b"value-c",
    },
];

const STRICT_CONFLICT_FIRST_COMMIT_RECORD: ExpectedRecord = ExpectedRecord {
    key: b"txn-strict-conflict-key",
    value: b"first-commit",
};

const STRICT_CONFLICT_SECOND_COMMIT_VALUE: &[u8] = b"second-commit";

#[test]
fn should_abort_in_child_process_when_txn_crash_scenario_requested() {
    // Arrange
    let Some(scenario) = std::env::var_os(ENV_SCENARIO) else {
        return;
    };

    let db_path = PathBuf::from(std::env::var_os(ENV_DB_PATH).expect("db path env"));

    // Act
    match scenario.to_string_lossy().as_ref() {
        "after_ops_before_commit" => child_abort_after_ops_before_commit(&db_path),
        "after_sync_before_ack" => child_abort_after_sync_before_ack(&db_path),
        "after_commit_ack" => child_abort_after_commit_ack(&db_path),
        "after_strict_conflict_abort" => child_abort_after_strict_conflict_abort(&db_path),
        other => panic!("unknown txn crash scenario: {other}"),
    }

    // Assert
    panic!("child scenario returned without abort");
}

#[test]
fn should_drop_sync_transaction_when_crashing_after_ops_append_before_commit_marker() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    // Act
    run_child_expect_abort("after_ops_before_commit", db_path);
    expire_crashed_process_lease(db_path);

    let engine = open_local_engine(db_path);

    // Assert
    assert_records_absent(&engine, PRE_COMMIT_RECORDS);
}

#[test]
fn should_recover_sync_transaction_when_crashing_after_sync_before_ack() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    // Act
    run_child_expect_abort("after_sync_before_ack", db_path);
    expire_crashed_process_lease(db_path);

    let engine = open_local_engine(db_path);

    // Assert
    assert_records_visible(&engine, POST_SYNC_RECORDS);
}

#[test]
fn should_recover_sync_transaction_when_process_aborts_after_commit_ack() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    // Act
    run_child_expect_abort("after_commit_ack", db_path);
    expire_crashed_process_lease(db_path);

    let engine = open_local_engine(db_path);

    // Assert
    assert_records_visible(&engine, POST_ACK_RECORDS);
}

#[test]
fn should_preserve_first_commit_when_process_aborts_after_strict_conflict_abort() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    // Act
    run_child_expect_abort("after_strict_conflict_abort", db_path);
    expire_crashed_process_lease(db_path);
    let engine = open_local_engine(db_path);

    // Assert
    let default_cf = default_cf(&engine);
    let tx = engine
        .begin_tx(default_cf.id(), TransactionMode::ReadOnly)
        .expect("begin read tx");
    assert_eq!(
        tx.get(STRICT_CONFLICT_FIRST_COMMIT_RECORD.key)
            .expect("get strict conflict key"),
        Some(Bytes::from_static(
            STRICT_CONFLICT_FIRST_COMMIT_RECORD.value
        )),
        "first strict commit must remain visible after crash"
    );
}

fn child_abort_after_ops_before_commit(db_path: &Path) {
    configure_abort_failpoint("midge::wal::txn_after_ops_append_before_commit");
    let engine = open_local_engine(db_path);
    let default_cf = default_cf(&engine);
    commit_fixed_sync_transaction(&engine, &default_cf, PRE_COMMIT_RECORDS);
}

fn child_abort_after_sync_before_ack(db_path: &Path) {
    configure_abort_failpoint("midge::wal::txn_after_sync_before_ack");
    let engine = open_local_engine(db_path);
    let default_cf = default_cf(&engine);
    commit_fixed_sync_transaction(&engine, &default_cf, POST_SYNC_RECORDS);
}

fn child_abort_after_commit_ack(db_path: &Path) {
    let engine = open_local_engine(db_path);
    let default_cf = default_cf(&engine);
    commit_fixed_sync_transaction(&engine, &default_cf, POST_ACK_RECORDS);
    std::process::abort();
}

fn child_abort_after_strict_conflict_abort(db_path: &Path) {
    // Arrange
    let engine = open_local_engine(db_path);
    let default_cf = default_cf(&engine);

    let mut tx1 = engine
        .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
        .expect("begin tx1");
    let mut tx2 = engine
        .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
        .expect("begin tx2");

    tx1.set_isolation_level(IsolationLevel::AbortOnWriteConflict);
    tx2.set_isolation_level(IsolationLevel::AbortOnWriteConflict);

    tx1.put(
        STRICT_CONFLICT_FIRST_COMMIT_RECORD.key.to_vec(),
        STRICT_CONFLICT_FIRST_COMMIT_RECORD.value.to_vec(),
        None,
    )
    .expect("tx1 put");
    tx2.put(
        STRICT_CONFLICT_FIRST_COMMIT_RECORD.key.to_vec(),
        STRICT_CONFLICT_SECOND_COMMIT_VALUE.to_vec(),
        None,
    )
    .expect("tx2 put");

    // Act
    engine
        .commit(tx1, WriteOptions::sync())
        .expect("commit tx1");
    let conflict = engine.commit(tx2, WriteOptions::sync());

    // Assert
    assert!(
        matches!(conflict, Err(cntryl_midge::MidgeError::WriteConflict(_))),
        "second strict commit must abort with WriteConflict"
    );

    // Crash after the conflict outcome has been observed.
    std::process::abort();
}

fn commit_fixed_sync_transaction(
    engine: &Engine,
    cf: &cntryl_midge::ColumnFamilyHandle,
    records: &[ExpectedRecord],
) {
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin write tx");
    for record in records {
        tx.put(record.key.to_vec(), record.value.to_vec(), None)
            .expect("put record");
    }
    engine
        .commit(tx, WriteOptions::sync())
        .expect("commit fixed sync transaction");
}

fn open_local_engine(db_path: &Path) -> Engine {
    Engine::open(OpenOptions::local(db_path).build()).expect("open engine")
}

fn default_cf(engine: &Engine) -> cntryl_midge::ColumnFamilyHandle {
    engine
        .get_column_family("default")
        .expect("default column family")
}

fn assert_records_visible(engine: &Engine, expected: &[ExpectedRecord]) {
    let default_cf = default_cf(engine);
    for record in expected {
        let tx = engine
            .begin_tx(default_cf.id(), TransactionMode::ReadOnly)
            .expect("begin read tx");
        let actual = tx.get(record.key).expect("get expected key");
        assert_eq!(
            actual,
            Some(Bytes::from_static(record.value)),
            "key {:?} must recover with the committed value",
            String::from_utf8_lossy(record.key)
        );
    }
}

fn assert_records_absent(engine: &Engine, expected: &[ExpectedRecord]) {
    let default_cf = default_cf(engine);
    for record in expected {
        let tx = engine
            .begin_tx(default_cf.id(), TransactionMode::ReadOnly)
            .expect("begin read tx");
        let actual = tx.get(record.key).expect("get expected key");
        assert_eq!(
            actual,
            None,
            "key {:?} must not appear after crash before commit marker",
            String::from_utf8_lossy(record.key)
        );
    }
}

fn run_child_expect_abort(scenario: &str, db_path: &Path) {
    let current_exe = std::env::current_exe().expect("current exe");
    let output = Command::new(current_exe)
        .arg("--exact")
        .arg(CHILD_TEST_NAME)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(ENV_SCENARIO, scenario)
        .env(ENV_DB_PATH, db_path)
        .output()
        .expect("run child test binary");

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
