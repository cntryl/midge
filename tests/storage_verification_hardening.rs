use cntryl_midge::{Engine, OpenOptions};
#[cfg(feature = "failpoints")]
use cntryl_midge::{MidgeError, TransactionMode, WriteOptions};
use serde_json::json;
use std::path::Path;
#[cfg(feature = "failpoints")]
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
#[cfg(feature = "failpoints")]
use std::time::Instant;

#[cfg(feature = "failpoints")]
static FAILPOINT_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn write_empty_storage_fixture(db_path: &Path) {
    std::fs::write(db_path.join("FORMAT"), "midge-format-version=2\n")
        .expect("write format marker");
    std::fs::write(
        db_path.join("manifest.json"),
        serde_json::to_vec_pretty(&json!({
            "last_persisted_sequence": 0,
            "files": [],
            "column_families": [],
            "next_wal_seq": 1,
            "next_sst_seqs": {},
            "edit_checkpoint_id": 0
        }))
        .expect("serialize empty manifest"),
    )
    .expect("write empty manifest");
}

#[test]
fn should_not_create_storage_directories_when_offline_verification_runs() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp directory");
    write_empty_storage_fixture(temp_dir.path());
    let wal_dir = temp_dir.path().join("wal");
    let sst_dir = temp_dir.path().join("sst");
    assert!(!wal_dir.exists());
    assert!(!sst_dir.exists());

    // Act
    Engine::verify_path(temp_dir.path()).expect("verify empty storage fixture");

    // Assert
    assert!(
        !wal_dir.exists(),
        "offline verification must not create wal/"
    );
    assert!(
        !sst_dir.exists(),
        "offline verification must not create sst/"
    );
}

#[test]
fn should_reject_parent_sst_name_when_intent_log_is_loaded() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp directory");
    write_empty_storage_fixture(temp_dir.path());
    std::fs::write(
        temp_dir.path().join("intent_log.json"),
        serde_json::to_vec_pretty(&json!([
            {
                "SstAdded": {
                    "file_meta": {
                        "name": "../escape.sst",
                        "level": 0,
                        "size_bytes": 0,
                        "cf_id": 0,
                        "sst_seq": 1,
                        "sublevel": 0
                    }
                }
            }
        ]))
        .expect("serialize unsafe intent"),
    )
    .expect("write unsafe intent");

    // Act
    let error = Engine::verify_path(temp_dir.path())
        .expect_err("unsafe persisted SST name must fail verification");

    // Assert
    assert!(
        error.to_string().contains("SST name"),
        "expected persisted-name error, got: {error}"
    );
}

#[test]
fn should_reject_absolute_sst_name_when_intent_log_is_loaded() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp directory");
    write_empty_storage_fixture(temp_dir.path());
    std::fs::write(
        temp_dir.path().join("intent_log.json"),
        serde_json::to_vec_pretty(&json!([
            {
                "CompactionApplied": {
                    "removed": ["/tmp/escape.sst"],
                    "added": []
                }
            }
        ]))
        .expect("serialize unsafe intent"),
    )
    .expect("write unsafe intent");

    // Act
    let error = Engine::verify_path(temp_dir.path())
        .expect_err("absolute persisted SST name must fail verification");

    // Assert
    assert!(
        error.to_string().contains("SST name"),
        "expected persisted-name error, got: {error}"
    );
}

#[test]
fn should_report_local_storage_as_authoritative_when_recovery_intents_remain() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp directory");
    write_empty_storage_fixture(temp_dir.path());
    std::fs::write(
        temp_dir.path().join("intent_log.json"),
        serde_json::to_vec_pretty(&json!([
            {
                "WalSynced": {
                    "segment_id": 7,
                    "seqno": 11
                }
            }
        ]))
        .expect("serialize recovery intent"),
    )
    .expect("write recovery intent");

    // Act
    let report = Engine::verify_path(temp_dir.path()).expect("verify local storage");

    // Assert
    assert!(
        report.authoritative,
        "pending recovery work affects health, not local storage authority"
    );
    assert_eq!(report.intent_entries_loaded, 1);
}

#[test]
fn should_verify_online_layout_within_caller_timeout() {
    // Arrange
    #[cfg(feature = "failpoints")]
    let _guard = FAILPOINT_TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let temp_dir = tempfile::tempdir().expect("create temp directory");
    let mut engine = Engine::open(
        OpenOptions::local(temp_dir.path())
            .build()
            .expect("build local options"),
    )
    .expect("open local engine");

    // Act
    let report = engine
        .verify_storage(Duration::from_secs(2))
        .expect("verify online storage");

    // Assert
    assert!(report.authoritative);
    engine
        .shutdown(Duration::from_secs(2))
        .expect("shutdown verified engine");
}

#[cfg(feature = "failpoints")]
#[test]
fn should_retain_verification_barrier_when_online_verification_times_out() {
    // Arrange
    let _guard = FAILPOINT_TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let scenario = fail::FailScenario::setup();
    fail::cfg(
        "midge::verification::after_barrier_acquired",
        "1*sleep(750)",
    )
    .expect("delay verification worker");

    let temp_dir = tempfile::tempdir().expect("create temp directory");
    let mut engine = Engine::open(
        OpenOptions::local(temp_dir.path())
            .build()
            .expect("build local options"),
    )
    .expect("open local engine");
    let default_cf = engine
        .get_column_family("default")
        .expect("default column family");

    // Act
    let started = Instant::now();
    let verification = engine.verify_storage(Duration::from_millis(25));
    let elapsed = started.elapsed();

    let ddl_error = engine
        .create_column_family("after-verification")
        .expect_err("DDL must remain fenced while verifier owns the barrier");
    let mut tx = engine
        .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
        .expect("begin transaction while layout is pinned");
    tx.put(b"fenced".to_vec(), b"write".to_vec(), None)
        .expect("buffer transaction write");
    let write_error = tx
        .commit(WriteOptions::best_effort())
        .expect_err("WAL mutation must remain fenced while verifier owns the barrier");

    let retry_deadline = Instant::now() + Duration::from_secs(3);
    loop {
        match engine.create_column_family("after-verification") {
            Ok(_) => break,
            Err(MidgeError::Busy(_)) if Instant::now() < retry_deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("verification barrier was not released: {error}"),
        }
    }

    // Assert
    assert!(matches!(verification, Err(MidgeError::Timeout(_))));
    assert!(
        elapsed < Duration::from_millis(500),
        "caller timeout must not wait for verifier worker: {elapsed:?}"
    );
    assert!(matches!(ddl_error, MidgeError::Busy(_)));
    assert!(matches!(write_error, MidgeError::Busy(_)));

    fail::remove("midge::verification::after_barrier_acquired");
    scenario.teardown();
    engine
        .shutdown(Duration::from_secs(2))
        .expect("shutdown verified engine");
}

#[cfg(feature = "failpoints")]
#[test]
fn should_release_verification_barrier_when_acquire_response_is_lost() {
    // Arrange
    let _guard = FAILPOINT_TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let scenario = fail::FailScenario::setup();
    fail::cfg(
        "midge::verification::before_barrier_response",
        "1*sleep(250)",
    )
    .expect("delay verification barrier response");

    let temp_dir = tempfile::tempdir().expect("create temp directory");
    let mut engine = Engine::open(
        OpenOptions::local(temp_dir.path())
            .build()
            .expect("build local options"),
    )
    .expect("open local engine");

    // Act
    let verification = engine.verify_storage(Duration::from_millis(25));
    let retry_deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match engine.create_column_family("after-lost-response") {
            Ok(_) => break,
            Err(MidgeError::Busy(_)) if Instant::now() < retry_deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("orphaned verification barrier was not released: {error}"),
        }
    }

    // Assert
    assert!(matches!(verification, Err(MidgeError::Timeout(_))));

    fail::remove("midge::verification::before_barrier_response");
    scenario.teardown();
    engine
        .shutdown(Duration::from_secs(2))
        .expect("shutdown after barrier cleanup");
}

#[cfg(feature = "failpoints")]
#[test]
fn should_retain_primary_lease_when_shutdown_overlaps_timed_out_verification() {
    // Arrange
    let _guard = FAILPOINT_TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let scenario = fail::FailScenario::setup();
    fail::cfg(
        "midge::verification::after_barrier_acquired",
        "1*sleep(750)",
    )
    .expect("delay verification worker");

    let temp_dir = tempfile::tempdir().expect("create temp directory");
    let mut engine = Engine::open(
        OpenOptions::local(temp_dir.path())
            .build()
            .expect("build local options"),
    )
    .expect("open local engine");
    let verification = engine.verify_storage(Duration::from_millis(25));

    // Act
    let shutdown = engine.shutdown(Duration::from_millis(25));
    let competing_open = Engine::open(
        OpenOptions::local(temp_dir.path())
            .build()
            .expect("build competing options"),
    );

    // Assert
    assert!(matches!(verification, Err(MidgeError::Timeout(_))));
    assert!(matches!(shutdown, Err(MidgeError::Timeout(_))));
    assert!(
        competing_open.is_err(),
        "shutdown released the primary lease while verification still read the path"
    );

    fail::remove("midge::verification::after_barrier_acquired");
    scenario.teardown();
    drop(engine);

    let reopen_deadline = Instant::now() + Duration::from_secs(3);
    let mut reopened = loop {
        match Engine::open(
            OpenOptions::local(temp_dir.path())
                .build()
                .expect("build reopen options"),
        ) {
            Ok(engine) => break engine,
            Err(_) if Instant::now() < reopen_deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("verification reaper did not release the lease: {error}"),
        }
    };
    reopened
        .shutdown(Duration::from_secs(2))
        .expect("shutdown reopened engine");
}
