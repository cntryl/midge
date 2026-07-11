#[cfg(feature = "failpoints")]
use cntryl_midge::MidgeError;
use cntryl_midge::{Engine, OpenOptions};
use std::fs;
use std::path::Path;
#[cfg(feature = "failpoints")]
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

#[cfg(feature = "failpoints")]
static DDL_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn open_cloud(path: &Path) -> Engine {
    Engine::open(
        OpenOptions::cloud_simulated(path, "ddl-test-bucket", "ddl/")
            .background_compaction(false)
            .build()
            .expect("build cloud options"),
    )
    .expect("open cloud engine")
}

fn shutdown(mut engine: Engine) {
    engine
        .shutdown(Duration::from_secs(3))
        .expect("shutdown cloud engine");
}

#[test]
fn should_converge_cloud_column_family_lifecycle_after_restart() {
    // Arrange
    let temp = tempfile::tempdir().expect("temp dir");
    let engine = open_cloud(temp.path());
    let created = engine
        .create_column_family("two-phase-cf")
        .expect("create column family");
    shutdown(engine);

    // Act
    let reopened = open_cloud(temp.path());
    assert!(reopened.get_column_family("two-phase-cf").is_some());
    reopened
        .drop_column_family(created.id())
        .expect("drop column family");
    shutdown(reopened);
    let final_open = open_cloud(temp.path());

    // Assert
    assert!(final_open.get_column_family("two-phase-cf").is_none());
    let registry = fs::read(temp.path().join("cloud_store/metadata/ddl.registry.json"))
        .expect("read authoritative DDL registry");
    let registry: serde_json::Value = serde_json::from_slice(&registry).expect("decode registry");
    assert_eq!(registry["epoch"], 2);
    assert_eq!(registry["operations"].as_array().map(Vec::len), Some(2));
    shutdown(final_open);
}

#[cfg(feature = "failpoints")]
#[test]
fn should_retry_cloud_column_family_create_after_remote_cas_failure() {
    // Arrange
    let _guard = DDL_TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("lock");
    let temp = tempfile::tempdir().expect("temp dir");
    let engine = open_cloud(temp.path());
    let scenario = fail::FailScenario::setup();
    fail::cfg("midge::ddl::before_remote_cas", "return").expect("configure CAS failpoint");

    // Act
    let first = engine.create_column_family("cas-retry");
    fail::remove("midge::ddl::before_remote_cas");
    scenario.teardown();
    let second = engine.create_column_family("cas-retry");

    // Assert
    assert!(matches!(first, Err(MidgeError::Internal(_))));
    assert_eq!(second.expect("retry create").name(), "cas-retry");
    shutdown(engine);
}

#[cfg(feature = "failpoints")]
#[test]
fn should_reconcile_remote_commit_when_local_commit_fails() {
    // Arrange
    let _guard = DDL_TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("lock");
    let temp = tempfile::tempdir().expect("temp dir");
    let engine = open_cloud(temp.path());
    let scenario = fail::FailScenario::setup();
    fail::cfg("midge::ddl::before_local_commit", "return")
        .expect("configure local commit failpoint");

    // Act
    let first = engine.create_column_family("local-retry");
    fail::remove("midge::ddl::before_local_commit");
    scenario.teardown();
    let second = engine.create_column_family("local-retry");

    // Assert
    assert!(matches!(first, Err(MidgeError::Internal(_))));
    assert_eq!(second.expect("reconciled create").name(), "local-retry");
    shutdown(engine);
}

#[cfg(feature = "failpoints")]
#[test]
fn should_abort_torn_prepare_when_remote_cas_never_started() {
    // Arrange
    let _guard = DDL_TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("lock");
    let temp = tempfile::tempdir().expect("temp dir");
    let engine = open_cloud(temp.path());
    let scenario = fail::FailScenario::setup();
    fail::cfg("midge::ddl::after_prepare", "return").expect("configure prepare failpoint");

    // Act
    let first = engine.create_column_family("prepare-retry");
    assert!(temp.path().join("ddl.prepare.json").exists());
    fail::remove("midge::ddl::after_prepare");
    scenario.teardown();
    let second = engine.create_column_family("prepare-retry");

    // Assert
    assert!(matches!(first, Err(MidgeError::Internal(_))));
    assert_eq!(second.expect("retry after abort").name(), "prepare-retry");
    assert!(!temp.path().join("ddl.prepare.json").exists());
    shutdown(engine);
}
