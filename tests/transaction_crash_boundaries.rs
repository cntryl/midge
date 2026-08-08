mod common;

use bytes::Bytes;
use cntryl_midge::{ConflictPolicy, Engine, OpenOptions, TransactionMode, WriteOptions};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Barrier};
use tempfile::TempDir;

use common::crash;

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

const GROUP_POST_SYNC_RECORDS: &[ExpectedRecord] = &[
    ExpectedRecord {
        key: b"group-post-sync-a",
        value: b"value-a",
    },
    ExpectedRecord {
        key: b"group-post-sync-b",
        value: b"value-b",
    },
    ExpectedRecord {
        key: b"group-post-sync-c",
        value: b"value-c",
    },
    ExpectedRecord {
        key: b"group-post-sync-d",
        value: b"value-d",
    },
];

const GROUP_POST_ACK_RECORDS: &[ExpectedRecord] = &[
    ExpectedRecord {
        key: b"group-post-ack-a",
        value: b"value-a",
    },
    ExpectedRecord {
        key: b"group-post-ack-b",
        value: b"value-b",
    },
    ExpectedRecord {
        key: b"group-post-ack-c",
        value: b"value-c",
    },
    ExpectedRecord {
        key: b"group-post-ack-d",
        value: b"value-d",
    },
];

const STRICT_CONFLICT_FIRST_COMMIT_RECORD: ExpectedRecord = ExpectedRecord {
    key: b"txn-strict-conflict-key",
    value: b"first-commit",
};

const STRICT_CONFLICT_SECOND_COMMIT_VALUE: &[u8] = b"second-commit";

const ASSERTION_ONLY_SYNC_RECORD: ExpectedRecord = ExpectedRecord {
    key: b"assertion-only-sync-buffered",
    value: b"buffered-before-assertion-sync",
};

const ASSERTION_GUARDED_RECORD: ExpectedRecord = ExpectedRecord {
    key: b"assertion-guarded-commit",
    value: b"guarded-value",
};

const ASSERTION_GUARDED_SPILLED_KEYS: &[&[u8]] = &[
    b"assertion-guarded-spilled-a",
    b"assertion-guarded-spilled-b",
    b"assertion-guarded-spilled-c",
    b"assertion-guarded-spilled-d",
];
const ASSERTION_GUARDED_SPILL_VALUE_BYTES: usize = 8 * 1024;
const ASSERTION_GUARDED_SPILL_POOL_BYTES: usize = 8 * 1024;

const ASSERTION_CONCURRENT_RECORD: ExpectedRecord = ExpectedRecord {
    key: b"assertion-concurrent-key",
    value: b"concurrent-value",
};

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
        "group_after_sync_before_ack" => child_abort_group_after_sync_before_ack(&db_path),
        "group_after_commit_ack" => child_abort_group_after_commit_ack(&db_path),
        "after_strict_conflict_abort" => child_abort_after_strict_conflict_abort(&db_path),
        "after_assertion_only_sync_ack" => child_abort_after_assertion_only_sync_ack(&db_path),
        "assertion_guarded_after_sync_before_ack" => {
            child_abort_assertion_guarded_after_sync_before_ack(&db_path);
        }
        "assertion_guarded_spilled_after_sync_before_ack" => {
            child_abort_assertion_guarded_spilled_after_sync_before_ack(&db_path);
        }
        "after_assertion_conflict_abort" => child_abort_after_assertion_conflict_abort(&db_path),
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
fn should_recover_every_group_member_when_crashing_after_shared_sync_before_ack() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    // Act
    run_child_expect_abort("group_after_sync_before_ack", db_path);
    expire_crashed_process_lease(db_path);
    let engine = open_local_engine(db_path);

    // Assert
    assert_records_visible(&engine, GROUP_POST_SYNC_RECORDS);
}

#[test]
fn should_recover_every_group_member_when_process_aborts_after_group_acknowledgements() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    // Act
    run_child_expect_abort("group_after_commit_ack", db_path);
    expire_crashed_process_lease(db_path);
    let engine = open_local_engine(db_path);

    // Assert
    assert_records_visible(&engine, GROUP_POST_ACK_RECORDS);
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

#[test]
fn should_recover_buffered_write_when_process_aborts_after_assertion_only_sync_commit() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    // Act
    run_child_expect_abort("after_assertion_only_sync_ack", db_path);
    expire_crashed_process_lease(db_path);
    let engine = open_local_engine(db_path);

    // Assert
    assert_records_visible(&engine, &[ASSERTION_ONLY_SYNC_RECORD]);
}

#[test]
fn should_recover_assertion_guarded_transaction_when_crashing_after_sync_before_ack() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    // Act
    run_child_expect_abort("assertion_guarded_after_sync_before_ack", db_path);
    expire_crashed_process_lease(db_path);
    let engine = open_local_engine(db_path);

    // Assert
    assert_records_visible(&engine, &[ASSERTION_GUARDED_RECORD]);
}

#[test]
fn should_recover_assertion_guarded_spilled_transaction_when_crashing_after_sync_before_ack() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    // Act
    run_child_expect_abort("assertion_guarded_spilled_after_sync_before_ack", db_path);
    expire_crashed_process_lease(db_path);
    let engine = open_local_engine(db_path);

    // Assert
    assert_assertion_guarded_spilled_records_visible(&engine);
}

#[test]
fn should_recover_concurrent_commit_when_process_aborts_after_assertion_rejection() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    // Act
    run_child_expect_abort("after_assertion_conflict_abort", db_path);
    expire_crashed_process_lease(db_path);
    let engine = open_local_engine(db_path);

    // Assert
    assert_records_visible(&engine, &[ASSERTION_CONCURRENT_RECORD]);
}

fn child_abort_after_ops_before_commit(db_path: &Path) {
    crash::configure_abort_failpoint(
        "midge::wal::txn_after_ops_append_before_commit",
        "after_ops_before_commit",
    );
    let engine = open_local_engine(db_path);
    let default_cf = default_cf(&engine);
    commit_fixed_sync_transaction(&engine, &default_cf, PRE_COMMIT_RECORDS);
}

fn child_abort_after_sync_before_ack(db_path: &Path) {
    crash::configure_abort_failpoint(
        "midge::wal::txn_after_sync_before_ack",
        "after_sync_before_ack",
    );
    let engine = open_local_engine(db_path);
    let default_cf = default_cf(&engine);
    commit_fixed_sync_transaction(&engine, &default_cf, POST_SYNC_RECORDS);
}

fn child_abort_after_commit_ack(db_path: &Path) {
    let engine = open_local_engine(db_path);
    let default_cf = default_cf(&engine);
    commit_fixed_sync_transaction(&engine, &default_cf, POST_ACK_RECORDS);
    crash::abort_at_trigger("after_commit_ack", "manual::after_commit_ack");
}

fn child_abort_after_assertion_only_sync_ack(db_path: &Path) {
    let engine = open_local_engine(db_path);
    let default_cf = default_cf(&engine);
    let mut buffered = engine
        .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
        .expect("begin buffered transaction");
    buffered
        .put(
            ASSERTION_ONLY_SYNC_RECORD.key.to_vec(),
            ASSERTION_ONLY_SYNC_RECORD.value.to_vec(),
            None,
        )
        .expect("stage buffered value");
    buffered
        .commit(WriteOptions::buffered())
        .expect("commit buffered value");
    let mut asserting = engine
        .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
        .expect("begin assertion-only transaction");
    asserting
        .assert_value(
            ASSERTION_ONLY_SYNC_RECORD.key.to_vec(),
            Some(ASSERTION_ONLY_SYNC_RECORD.value.to_vec()),
        )
        .expect("register buffered value assertion");
    asserting
        .commit(WriteOptions::sync())
        .expect("establish assertion-only sync barrier");
    crash::abort_at_trigger(
        "after_assertion_only_sync_ack",
        "manual::after_assertion_only_sync_ack",
    );
}

fn child_abort_assertion_guarded_after_sync_before_ack(db_path: &Path) {
    crash::configure_abort_failpoint(
        "midge::wal::txn_after_sync_before_ack",
        "assertion_guarded_after_sync_before_ack",
    );
    let engine = open_local_engine(db_path);
    let default_cf = default_cf(&engine);
    let mut guarded = engine
        .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
        .expect("begin assertion-guarded transaction");
    guarded
        .assert_value(ASSERTION_GUARDED_RECORD.key.to_vec(), None)
        .expect("assert guarded key is absent");
    guarded
        .put(
            ASSERTION_GUARDED_RECORD.key.to_vec(),
            ASSERTION_GUARDED_RECORD.value.to_vec(),
            None,
        )
        .expect("stage assertion-guarded value");
    guarded
        .commit(WriteOptions::sync())
        .expect("commit assertion-guarded transaction");
}

fn child_abort_assertion_guarded_spilled_after_sync_before_ack(db_path: &Path) {
    let engine =
        open_local_engine_with_transaction_pool(db_path, ASSERTION_GUARDED_SPILL_POOL_BYTES);
    let default_cf = default_cf(&engine);
    let mut guarded = engine
        .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
        .expect("begin spilled assertion-guarded transaction");
    guarded
        .assert_value(b"assertion-guarded-spilled-guard".to_vec(), None)
        .expect("assert spilled guard key is absent");
    for (index, key) in ASSERTION_GUARDED_SPILLED_KEYS.iter().enumerate() {
        guarded
            .put(key.to_vec(), assertion_guarded_spill_value(index), None)
            .expect("stage spilled assertion-guarded value");
    }
    assert!(
        transaction_spill_run_count(db_path) > 0,
        "assertion-guarded transaction must spill before commit"
    );
    crash::configure_abort_failpoint(
        "midge::wal::txn_after_sync_before_ack",
        "assertion_guarded_spilled_after_sync_before_ack",
    );
    guarded
        .commit(WriteOptions::sync())
        .expect("commit spilled assertion-guarded transaction");
}

fn child_abort_group_after_sync_before_ack(db_path: &Path) {
    configure_group_abort_failpoint("group_after_sync_before_ack");
    commit_concurrent_sync_transactions(db_path, GROUP_POST_SYNC_RECORDS);
}

fn child_abort_group_after_commit_ack(db_path: &Path) {
    commit_concurrent_sync_transactions(db_path, GROUP_POST_ACK_RECORDS);
    crash::abort_at_trigger("group_after_commit_ack", "manual::group_after_commit_ack");
}

fn commit_concurrent_sync_transactions(db_path: &Path, records: &[ExpectedRecord]) {
    let engine = Arc::new(open_local_engine(db_path));
    let default_cf = default_cf(&engine);
    let barrier = Arc::new(Barrier::new(records.len() + 1));
    let mut handles = Vec::with_capacity(records.len());
    for record in records.iter().copied() {
        let engine = Arc::clone(&engine);
        let barrier = Arc::clone(&barrier);
        let cf_id = default_cf.id();
        handles.push(std::thread::spawn(move || {
            let mut tx = engine
                .begin_tx(cf_id, TransactionMode::ReadWrite)
                .expect("begin grouped write tx");
            tx.put(record.key.to_vec(), record.value.to_vec(), None)
                .expect("put grouped record");
            barrier.wait();
            tx.commit(WriteOptions::sync())
                .expect("commit grouped sync transaction");
        }));
    }
    barrier.wait();
    for handle in handles {
        handle.join().expect("join grouped sync transaction");
    }
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

    tx1.set_conflict_policy(ConflictPolicy::AbortOnWriteConflict);
    tx2.set_conflict_policy(ConflictPolicy::AbortOnWriteConflict);

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
    tx1.commit(WriteOptions::sync()).expect("commit tx1");
    let conflict = tx2.commit(WriteOptions::sync());

    // Assert
    assert!(
        matches!(conflict, Err(cntryl_midge::MidgeError::WriteConflict(_))),
        "second strict commit must abort with WriteConflict"
    );

    // Crash after the conflict outcome has been observed.
    crash::abort_at_trigger(
        "after_strict_conflict_abort",
        "manual::after_strict_conflict_abort",
    );
}

fn child_abort_after_assertion_conflict_abort(db_path: &Path) {
    // Arrange
    let engine = open_local_engine(db_path);
    let default_cf = default_cf(&engine);
    let mut asserting = engine
        .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
        .expect("begin asserting transaction");
    asserting
        .assert_value(ASSERTION_CONCURRENT_RECORD.key.to_vec(), None)
        .expect("assert concurrent key is absent");
    let mut concurrent = engine
        .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
        .expect("begin concurrent transaction");
    concurrent
        .put(
            ASSERTION_CONCURRENT_RECORD.key.to_vec(),
            ASSERTION_CONCURRENT_RECORD.value.to_vec(),
            None,
        )
        .expect("stage concurrent value");

    // Act
    concurrent
        .commit(WriteOptions::sync())
        .expect("commit concurrent value");
    let conflict = asserting.commit(WriteOptions::sync());

    // Assert
    assert!(matches!(
        conflict,
        Err(cntryl_midge::MidgeError::WriteConflict(_))
    ));
    crash::abort_at_trigger(
        "after_assertion_conflict_abort",
        "manual::after_assertion_conflict_abort",
    );
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
    tx.commit(WriteOptions::sync())
        .expect("commit fixed sync transaction");
}

fn open_local_engine(db_path: &Path) -> Engine {
    Engine::open(OpenOptions::local(db_path).build().expect("build options")).expect("open engine")
}

fn open_local_engine_with_transaction_pool(db_path: &Path, pool_bytes: usize) -> Engine {
    Engine::open(
        OpenOptions::local(db_path)
            .transaction_memory_pool_size(pool_bytes)
            .build()
            .expect("build options"),
    )
    .expect("open engine")
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

fn assert_assertion_guarded_spilled_records_visible(engine: &Engine) {
    let default_cf = default_cf(engine);
    for (index, key) in ASSERTION_GUARDED_SPILLED_KEYS.iter().enumerate() {
        let tx = engine
            .begin_tx(default_cf.id(), TransactionMode::ReadOnly)
            .expect("begin spilled verification transaction");
        assert_eq!(
            tx.get(key).expect("read spilled assertion-guarded value"),
            Some(Bytes::from(assertion_guarded_spill_value(index))),
            "spilled key {index} must recover after the commit-marker sync"
        );
    }
}

fn assertion_guarded_spill_value(index: usize) -> Vec<u8> {
    let byte = b'a'.saturating_add(u8::try_from(index).expect("spill value index fits in u8"));
    vec![byte; ASSERTION_GUARDED_SPILL_VALUE_BYTES]
}

fn transaction_spill_run_count(db_path: &Path) -> usize {
    fs::read_dir(db_path.join("txn")).map_or(0, |entries| {
        entries
            .filter_map(Result::ok)
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "run"))
            .count()
    })
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
    crash::run_child_expect_abort(&mut command, scenario, crash_trigger(scenario), db_path);
}

fn configure_group_abort_failpoint(scenario_name: &'static str) {
    crash::configure_abort_failpoint("midge::wal::txn_after_sync_before_ack", scenario_name);
    fail::cfg("midge::runtime::strict_group_before_collect", "1*sleep(50)")
        .expect("configure strict group collection failpoint");
}

fn crash_trigger(scenario: &str) -> &'static str {
    match scenario {
        "after_ops_before_commit" => "midge::wal::txn_after_ops_append_before_commit",
        "after_sync_before_ack"
        | "group_after_sync_before_ack"
        | "assertion_guarded_after_sync_before_ack"
        | "assertion_guarded_spilled_after_sync_before_ack" => {
            "midge::wal::txn_after_sync_before_ack"
        }
        "after_commit_ack" => "manual::after_commit_ack",
        "group_after_commit_ack" => "manual::group_after_commit_ack",
        "after_strict_conflict_abort" => "manual::after_strict_conflict_abort",
        "after_assertion_only_sync_ack" => "manual::after_assertion_only_sync_ack",
        "after_assertion_conflict_abort" => "manual::after_assertion_conflict_abort",
        other => panic!("unknown transaction crash trigger for scenario {other}"),
    }
}

fn expire_crashed_process_lease(db_path: &Path) {
    let leader_path = db_path.join(".midge_leader");
    if leader_path.exists() {
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

    crash::clear_crashed_process_acquisition_lock(db_path);
}
