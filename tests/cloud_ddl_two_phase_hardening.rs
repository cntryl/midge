use cntryl_midge::{Engine, OpenOptions};
#[cfg(feature = "failpoints")]
use cntryl_midge::{EngineHealth, MidgeError, TransactionMode, WriteOptions};
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
fn should_converge_local_remote_ddl_state_given_normal_create_drop_when_reopening() {
    // Arrange
    #[cfg(feature = "failpoints")]
    let _guard = DDL_TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
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
fn should_converge_local_remote_ddl_state_given_remote_cas_failure_when_reopening() {
    // Arrange
    let _guard = DDL_TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let temp = tempfile::tempdir().expect("temp dir");
    let engine = open_cloud(temp.path());
    let scenario = fail::FailScenario::setup();
    fail::cfg("midge::ddl::before_remote_cas", "return").expect("configure remote CAS failure");

    // Act
    let failed_create = engine.create_column_family("cas-reopen");
    fail::remove("midge::ddl::before_remote_cas");
    scenario.teardown();
    assert!(matches!(failed_create, Err(MidgeError::Internal(_))));
    assert!(engine.get_column_family("cas-reopen").is_none());
    shutdown(engine);
    let reopened = open_cloud(temp.path());
    assert!(
        !temp.path().join("ddl.prepare.json").exists(),
        "reopen itself must clear the failed pre-authority prepare before any retry"
    );
    let before_retry = reopened.get_column_family("cas-reopen");
    let created = reopened
        .create_column_family("cas-reopen")
        .expect("retry create after reopen");

    // Assert
    assert!(before_retry.is_none());
    assert_eq!(created.name(), "cas-reopen");
    let registry = fs::read(temp.path().join("cloud_store/metadata/ddl.registry.json"))
        .expect("read authoritative DDL registry");
    let registry: serde_json::Value = serde_json::from_slice(&registry).expect("decode registry");
    assert_eq!(registry["epoch"], 1);
    assert_eq!(registry["operations"].as_array().map(Vec::len), Some(1));
    shutdown(reopened);
}

#[cfg(feature = "failpoints")]
#[test]
fn should_retry_cloud_column_family_create_after_remote_cas_failure() {
    // Arrange
    let _guard = DDL_TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
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
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let temp = tempfile::tempdir().expect("temp dir");
    let engine = open_cloud(temp.path());
    let scenario = fail::FailScenario::setup();
    fail::cfg("midge::ddl::before_local_commit", "return")
        .expect("configure local commit failpoint");

    // Act
    let first = engine.create_column_family("local-retry");
    fail::remove("midge::ddl::before_local_commit");
    scenario.teardown();

    // Assert: remote CAS already committed, so the live runtime adopts that
    // authority and fences the stale local view until restart reconciliation.
    assert_eq!(
        first.expect("remote-authoritative create").name(),
        "local-retry"
    );
    assert_eq!(
        engine
            .get_runtime_metrics()
            .expect("degraded runtime metrics")
            .health,
        EngineHealth::Degraded
    );
    shutdown(engine);
    let reopened = open_cloud(temp.path());
    assert!(reopened.get_column_family("local-retry").is_some());
    assert_eq!(
        reopened
            .get_runtime_metrics()
            .expect("reconciled runtime metrics")
            .health,
        EngineHealth::Healthy
    );
    shutdown(reopened);
}

#[cfg(feature = "failpoints")]
#[test]
fn should_return_usable_column_family_when_create_metadata_mirror_fails_after_commit() {
    // Arrange
    let _guard = DDL_TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let temp = tempfile::tempdir().expect("temp dir");
    let engine = open_cloud(temp.path());
    let scenario = fail::FailScenario::setup();
    fail::cfg(
        "midge::ddl::after_create_local_commit_before_metadata_mirror",
        "return",
    )
    .expect("configure post-commit create mirror failure");

    // Act
    let create_result = engine.create_column_family("committed-create");
    fail::remove("midge::ddl::after_create_local_commit_before_metadata_mirror");
    scenario.teardown();

    // Assert: the DDL registry and local journal already made this create
    // authoritative, so the public handle registry must adopt it even when an
    // auxiliary manifest mirror is temporarily degraded.
    let cf = create_result.expect("return committed column family");
    assert_eq!(
        engine
            .get_column_family("committed-create")
            .expect("registered committed column family")
            .id(),
        cf.id()
    );
    let mut writer = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin transaction through committed handle");
    writer
        .put(b"key".to_vec(), b"value".to_vec(), None)
        .expect("buffer write through committed handle");
    writer
        .commit(WriteOptions::cloud_async())
        .expect("commit write through committed handle");
    let reader = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin read through committed handle");
    assert_eq!(
        reader
            .get(b"key")
            .expect("read value through committed handle")
            .as_deref(),
        Some(b"value".as_slice())
    );
    drop(reader);

    let retried = engine
        .create_column_family("committed-create")
        .expect("idempotently retry committed create");
    assert_eq!(retried.id(), cf.id());
    let registry = fs::read(temp.path().join("cloud_store/metadata/ddl.registry.json"))
        .expect("read authoritative DDL registry");
    let registry: serde_json::Value = serde_json::from_slice(&registry).expect("decode registry");
    assert_eq!(registry["epoch"], 1);
    assert_eq!(registry["operations"].as_array().map(Vec::len), Some(1));
    assert_eq!(
        engine
            .get_runtime_metrics()
            .expect("degraded runtime metrics")
            .health,
        EngineHealth::Degraded
    );
    shutdown(engine);

    let reopened = open_cloud(temp.path());
    assert!(reopened.get_column_family("committed-create").is_some());
    assert_eq!(
        reopened
            .get_runtime_metrics()
            .expect("reconciled runtime metrics")
            .health,
        EngineHealth::Healthy
    );
    shutdown(reopened);
}

#[cfg(feature = "failpoints")]
#[test]
fn should_fence_column_family_when_remote_drop_commits_before_local_commit() {
    // Arrange
    let _guard = DDL_TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let temp = tempfile::tempdir().expect("temp dir");
    let engine = open_cloud(temp.path());
    let cf = engine
        .create_column_family("remote-drop")
        .expect("create column family");
    let scenario = fail::FailScenario::setup();
    fail::cfg("midge::ddl::before_local_commit", "return")
        .expect("configure local drop commit failpoint");

    // Act
    let drop_result = engine.drop_column_family(cf.id());
    fail::remove("midge::ddl::before_local_commit");
    scenario.teardown();

    // Assert
    assert!(drop_result.is_ok(), "remote authority committed the drop");
    assert!(engine.get_column_family("remote-drop").is_none());
    assert_eq!(
        engine
            .get_runtime_metrics()
            .expect("degraded runtime metrics")
            .health,
        EngineHealth::Degraded
    );
    shutdown(engine);

    let reopened = open_cloud(temp.path());
    assert!(reopened.get_column_family("remote-drop").is_none());
    assert_eq!(
        reopened
            .get_runtime_metrics()
            .expect("reconciled runtime metrics")
            .health,
        EngineHealth::Healthy
    );
    shutdown(reopened);
}

#[cfg(feature = "failpoints")]
#[test]
fn should_fence_column_family_when_remote_drop_cas_response_is_lost() {
    // Arrange
    let _guard = DDL_TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let temp = tempfile::tempdir().expect("temp dir");
    let engine = open_cloud(temp.path());
    let cf = engine
        .create_column_family("lost-drop-response")
        .expect("create column family");
    let scenario = fail::FailScenario::setup();
    fail::cfg("midge::ddl::after_remote_cas", "return")
        .expect("configure lost remote CAS response");

    // Act
    let drop_result = engine.drop_column_family(cf.id());
    fail::remove("midge::ddl::after_remote_cas");
    scenario.teardown();

    // Assert
    assert!(
        drop_result.is_ok(),
        "operation id proves the remote drop committed"
    );
    assert!(engine.get_column_family("lost-drop-response").is_none());
    assert_eq!(
        engine
            .get_runtime_metrics()
            .expect("degraded runtime metrics")
            .health,
        EngineHealth::Degraded
    );
    shutdown(engine);

    let reopened = open_cloud(temp.path());
    assert!(reopened.get_column_family("lost-drop-response").is_none());
    assert_eq!(
        reopened
            .get_runtime_metrics()
            .expect("reconciled runtime metrics")
            .health,
        EngineHealth::Healthy
    );
    shutdown(reopened);
}

#[cfg(feature = "failpoints")]
#[test]
fn should_fence_writes_when_remote_drop_authority_cannot_be_resolved() {
    // Arrange
    let _guard = DDL_TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let temp = tempfile::tempdir().expect("temp dir");
    let engine = open_cloud(temp.path());
    let cf = engine
        .create_column_family("ambiguous-drop")
        .expect("create column family");
    let mut seed = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin unflushed seed transaction");
    seed.put(b"discarded".to_vec(), b"value".to_vec(), None)
        .expect("buffer unflushed seed");
    seed.commit(WriteOptions::cloud_async())
        .expect("commit unflushed seed");
    let scenario = fail::FailScenario::setup();
    fail::cfg("midge::ddl::after_remote_cas", "return")
        .expect("configure lost remote CAS response");
    fail::cfg(
        "midge::ddl::before_ambiguous_cas_authority_reread",
        "return",
    )
    .expect("configure authority re-read failure");

    // Act
    let drop_result = engine.drop_column_family_discarding_unflushed(cf.id());
    fail::remove("midge::ddl::after_remote_cas");
    fail::remove("midge::ddl::before_ambiguous_cas_authority_reread");
    scenario.teardown();

    // Assert: until the durable prepare can be reconciled, the runtime must
    // not accept a write that a remotely committed drop would erase.
    assert!(matches!(drop_result, Err(MidgeError::Fenced(_))));
    assert!(engine.get_column_family("ambiguous-drop").is_some());
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin transaction against locally visible CF");
    tx.put(b"must-not-commit".to_vec(), b"value".to_vec(), None)
        .expect("buffer fenced write");
    assert!(matches!(
        tx.commit(WriteOptions::cloud_async()),
        Err(MidgeError::Fenced(_))
    ));
    assert_eq!(
        engine
            .get_runtime_metrics()
            .expect("degraded runtime metrics")
            .health,
        EngineHealth::Degraded
    );

    engine
        .drop_column_family(cf.id())
        .expect("safe retry resolves already-committed prepared remote drop");
    assert!(engine.get_column_family("ambiguous-drop").is_none());
    shutdown(engine);

    let reopened = open_cloud(temp.path());
    assert!(reopened.get_column_family("ambiguous-drop").is_none());
    shutdown(reopened);
}

#[cfg(feature = "failpoints")]
#[test]
fn should_abort_torn_ddl_prepare_given_local_prepare_without_remote_commit_when_reopening() {
    // Arrange
    let _guard = DDL_TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
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

#[cfg(feature = "failpoints")]
#[test]
fn should_recover_ambiguous_ddl_once_when_reopening_after_crash_before_remote_cas_submission() {
    // Arrange
    let _guard = DDL_TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let temp = tempfile::tempdir().expect("temp dir");
    let engine = open_cloud(temp.path());
    let scenario = fail::FailScenario::setup();
    fail::cfg(
        "midge::ddl::after_ambiguous_prepare_before_remote_cas_submission",
        "return",
    )
    .expect("configure crash-before-CAS failpoint");

    // Act
    let first = engine.create_column_family("crash-before-cas");
    fail::remove("midge::ddl::after_ambiguous_prepare_before_remote_cas_submission");
    scenario.teardown();

    // Assert: the accepted operation is durable locally but never reached the
    // remote registry before the simulated crash boundary.
    assert!(matches!(first, Err(MidgeError::Internal(_))));
    let prepare = fs::read(temp.path().join("ddl.prepare.json")).expect("read DDL prepare");
    let prepare: serde_json::Value = serde_json::from_slice(&prepare).expect("decode DDL prepare");
    assert_eq!(prepare["remote_cas_ambiguous"], true);
    let op_id = prepare["op_id"]
        .as_str()
        .expect("prepared operation id")
        .to_string();
    let registry = fs::read(temp.path().join("cloud_store/metadata/ddl.registry.json"))
        .expect("read authoritative DDL registry");
    let registry: serde_json::Value = serde_json::from_slice(&registry).expect("decode registry");
    assert!(registry["operations"]
        .as_array()
        .expect("registry operations")
        .iter()
        .all(|operation| operation["op_id"].as_str() != Some(op_id.as_str())));
    assert!(engine.get_column_family("crash-before-cas").is_none());
    shutdown(engine);

    // Act: startup must safely re-drive the same durable operation rather than
    // fence the database forever or mint a replacement operation id.
    let reopened = open_cloud(temp.path());

    // Assert
    assert!(reopened.get_column_family("crash-before-cas").is_some());
    assert!(!temp.path().join("ddl.prepare.json").exists());
    let registry = fs::read(temp.path().join("cloud_store/metadata/ddl.registry.json"))
        .expect("read reconciled authoritative DDL registry");
    let registry: serde_json::Value = serde_json::from_slice(&registry).expect("decode registry");
    let matching_operations = registry["operations"]
        .as_array()
        .expect("registry operations")
        .iter()
        .filter(|operation| operation["op_id"].as_str() == Some(op_id.as_str()))
        .count();
    assert_eq!(matching_operations, 1);
    assert_eq!(registry["epoch"], 1);
    shutdown(reopened);
}
