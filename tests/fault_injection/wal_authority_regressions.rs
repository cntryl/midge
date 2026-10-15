//! #808 WAL authority regressions: the native-provider predecessor cache, and
//! salvage substituting an intact local segment for a damaged cloud copy.
use crate::common::crash;
use cntryl_midge::__internal::simulation::SimulatedClock;
use cntryl_midge::__internal::wal::catalog::WalPublicationCatalog;
#[cfg(feature = "sqrzl-tests")]
use cntryl_midge::{CloudProviderConfig, CloudStorageLocation};
use cntryl_midge::{Engine, OpenOptions, RecoveryPolicy, TransactionMode, WriteOptions};
#[cfg(feature = "sqrzl-tests")]
use serde::{Deserialize, Serialize};
use std::path::Path;
#[cfg(feature = "sqrzl-tests")]
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
#[cfg(feature = "sqrzl-tests")]
use std::time::Instant;

#[cfg(feature = "sqrzl-tests")]
const CHILD: &str =
    "wal_authority_regressions::should_abort_predecessor_when_audit_child_is_requested";
#[cfg(feature = "sqrzl-tests")]
const CHILD_ENV: &str = "MIDGE_808_PREDECESSOR_CASE";
#[cfg(feature = "sqrzl-tests")]
const SCENARIO: &str = "predecessor-cache";
#[cfg(feature = "sqrzl-tests")]
const TRIGGER: &str = "audit::after-unpublished-commits";

#[cfg(feature = "sqrzl-tests")]
#[derive(Deserialize, Serialize)]
struct Case {
    root: PathBuf,
    bucket: String,
    prefix: String,
}

#[cfg(feature = "sqrzl-tests")]
fn options(case: &Case, root: &Path) -> OpenOptions {
    OpenOptions::cloud(
        root,
        CloudStorageLocation::new(CloudProviderConfig::sqrzl_s3(&case.bucket), &case.prefix),
    )
    .background_compaction(false)
    .lease_ttl(Duration::from_secs(18))
    .lease_clock_skew_tolerance(Duration::ZERO)
    .build()
    .unwrap()
}

fn get(engine: &Engine, key: &[u8]) -> Option<Vec<u8>> {
    let cf = engine.get_column_family("default").unwrap();
    let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
    tx.get(key).unwrap().map(|value| value.to_vec())
}

#[test]
#[cfg(feature = "sqrzl-tests")]
fn should_abort_predecessor_when_audit_child_is_requested() {
    // Arrange
    let Some(path) = std::env::var_os(CHILD_ENV) else {
        return;
    };
    let case: Case = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let _scenario = fail::FailScenario::setup();
    fail::cfg("midge::cloud::inject_fail_wal_upload", "return").unwrap();
    let engine = Engine::open(options(&case, &case.root)).unwrap();
    let cf = engine.get_column_family("default").unwrap();
    // Act
    let mut put = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .unwrap();
    put.put(b"J".to_vec(), b"predecessor-only".to_vec(), None)
        .unwrap();
    put.commit(WriteOptions::cloud_async()).unwrap();
    let mut delete = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .unwrap();
    delete.delete(b"K".to_vec()).unwrap();
    delete.commit(WriteOptions::cloud_async()).unwrap();
    // Assert: the parent requires this exact synced marker and process abort.
    assert_eq!(get(&engine, b"J"), Some(b"predecessor-only".to_vec()));
    crash::abort_at_trigger(SCENARIO, TRIGGER);
}

#[test]
#[cfg(feature = "sqrzl-tests")]
fn should_preserve_successor_strict_commits_when_predecessor_reopens_old_cache() {
    // Arrange: the predecessor dies with acknowledged but unpublished local WAL.
    let dir = tempfile::tempdir().unwrap();
    let case = Case {
        root: dir.path().join("predecessor"),
        bucket: "midge-808-audit".to_string(),
        prefix: format!("lineage/{}/", uuid::Uuid::new_v4()),
    };
    crate::common::prepare_sqrzl_namespace(&CloudProviderConfig::sqrzl_s3(&case.bucket)).unwrap();
    let case_path = dir.path().join("case.json");
    std::fs::write(&case_path, serde_json::to_vec(&case).unwrap()).unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap());
    child
        .args(["--exact", CHILD, "--nocapture"])
        .env(CHILD_ENV, case_path);
    crash::run_child_expect_abort(&mut child, SCENARIO, TRIGGER, &case.root);
    let successor = dir.path().join("successor");
    let deadline = Instant::now() + Duration::from_secs(35);
    let mut engine = loop {
        match Engine::open(options(&case, &successor)) {
            Ok(engine) => break engine,
            Err(error) => {
                assert!(
                    Instant::now() < deadline,
                    "successor could not acquire expired lease: {error}"
                );
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    };
    let cf = engine.get_column_family("default").unwrap();
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .unwrap();
    tx.put(b"K".to_vec(), b"successor-K".to_vec(), None)
        .unwrap();
    tx.put(b"Q".to_vec(), b"successor-Q".to_vec(), None)
        .unwrap();
    tx.commit(WriteOptions::cloud_strict()).unwrap();
    engine.flush_cf(&cf).unwrap();
    assert_eq!(get(&engine, b"K"), Some(b"successor-K".to_vec()));
    assert_eq!(get(&engine, b"Q"), Some(b"successor-Q".to_vec()));
    engine.shutdown(Duration::from_secs(10)).unwrap();
    drop(engine);
    crash::clear_crashed_process_acquisition_lock(&case.root);

    // Act: production startup hydrates shared metadata but sees A's old tail.
    let mut reopened = Engine::open(options(&case, &case.root)).unwrap();
    let k = get(&reopened, b"K");
    let q = get(&reopened, b"Q");
    let j = get(&reopened, b"J");
    reopened.shutdown(Duration::from_secs(10)).unwrap();

    // Assert: foreign takeover permanently disqualifies that unpublished tail.
    assert_eq!(
        (k, q, j),
        (
            Some(b"successor-K".to_vec()),
            Some(b"successor-Q".to_vec()),
            None
        ),
        "foreign predecessor cache overrode successor authority"
    );
}

/// A substitute child: the process that acknowledged segment 1 dies before
/// that segment's local copy retires.
const SUBSTITUTE_CHILD: &str =
    "wal_authority_regressions::should_abort_before_local_retirement_when_substitute_child_is_requested";
const SUBSTITUTE_ENV: &str = "MIDGE_808_SUBSTITUTE_ROOT";
const SUBSTITUTE_SCENARIO: &str = "substitute-upload-collision";
/// Cloud-mode local retirement of an acknowledged segment, before the ledger
/// records it.
const LOCAL_RETIREMENT: &str = "midge::cloud::before_local_wal_retirement";
const SUBSTITUTE_LEASE_TTL: Duration = Duration::from_secs(30);
const SUBSTITUTE_REPAIR_CHILD: &str =
    "wal_authority_regressions::should_abort_during_substitute_repair_child_when_requested";
const SUBSTITUTE_REPAIR_ENV: &str = "MIDGE_808_SUBSTITUTE_REPAIR_TRIGGER";
const SUBSTITUTE_REPAIR_SCENARIO: &str = "substitute-repair";
const AFTER_SUBSTITUTE_CHECKPOINT: &str =
    "midge::startup::substituted_wal_after_checkpoint_before_catalog_retire";
const AFTER_SUBSTITUTE_CATALOG_RETIRE: &str =
    "midge::startup::substituted_wal_after_catalog_retire_before_local_rebase";

fn substitute_options(root: &Path, clock: SimulatedClock) -> OpenOptions {
    OpenOptions::cloud_simulated(root, "audit", "substitute")
        .background_compaction(false)
        .runtime_response_timeout(Duration::from_secs(5))
        .storage_io_timeout(Duration::from_secs(2))
        .recovery_policy(RecoveryPolicy::Salvage)
        .lease_ttl(SUBSTITUTE_LEASE_TTL)
        .lease_clock_for_testing(Arc::new(clock))
        .build()
        .unwrap()
}

fn prepare_damaged_substituted_segment(root: &Path) -> (std::path::PathBuf, Vec<u8>) {
    // Arrange: a predecessor leaves its cataloged local WAL before retiring
    // it, then the test damages only the immutable cloud object.
    let mut child = std::process::Command::new(std::env::current_exe().unwrap());
    child
        .args(["--exact", SUBSTITUTE_CHILD, "--nocapture"])
        .env(SUBSTITUTE_ENV, root);
    crash::run_child_expect_abort(&mut child, SUBSTITUTE_SCENARIO, LOCAL_RETIREMENT, root);
    let catalog = WalPublicationCatalog::decode(
        &std::fs::read(root.join("cloud_store/wal/publication-catalog.v1.json")).unwrap(),
    )
    .unwrap();
    let entry = catalog
        .entries()
        .first()
        .expect("acknowledged segment cataloged");
    let id = entry.id();
    let remote = root.join("cloud_store").join(entry.object_key());
    let original = std::fs::read(&remote).unwrap();
    let local = std::fs::read(
        root.join("wal")
            .join(cntryl_midge::__internal::wal::segment_file_name(id)),
    )
    .expect("the crash kept the acknowledged segment's local copy");
    assert_eq!(local, original, "the local member is the cataloged bytes");
    let mut corrupt = original;
    corrupt[8] ^= 1;
    std::fs::write(&remote, &corrupt).unwrap();
    (remote, corrupt)
}

#[test]
fn should_abort_before_local_retirement_when_substitute_child_is_requested() {
    // Arrange
    let Some(root) = std::env::var_os(SUBSTITUTE_ENV) else {
        return;
    };
    let engine = Engine::open(substitute_options(Path::new(&root), SimulatedClock::new())).unwrap();
    let cf = engine.get_column_family("default").unwrap();
    crash::configure_abort_failpoint(LOCAL_RETIREMENT, SUBSTITUTE_SCENARIO);

    // Act: segment 1 is cataloged and acknowledged; retiring its local copy
    // aborts the process.
    let mut seed = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .unwrap();
    seed.put(b"seed".to_vec(), b"value".to_vec(), None).unwrap();
    seed.commit(WriteOptions::cloud_strict()).unwrap();
    std::thread::sleep(Duration::from_secs(10));

    // Assert: the parent requires this exact synced marker and process abort.
    panic!("the acknowledged segment's local retirement never ran");
}

#[test]
fn should_abort_during_substitute_repair_child_when_requested() {
    // Arrange
    let Some(root) = std::env::var_os(SUBSTITUTE_ENV) else {
        return;
    };
    let Some(trigger) = std::env::var_os(SUBSTITUTE_REPAIR_ENV) else {
        return;
    };
    let trigger = trigger.to_string_lossy().into_owned();
    crash::configure_abort_failpoint(&trigger, SUBSTITUTE_REPAIR_SCENARIO);
    let clock = SimulatedClock::new();
    clock.advance(SUBSTITUTE_LEASE_TTL * 2);

    // Act: startup must reach the selected repair boundary.
    let _engine = Engine::open(substitute_options(Path::new(&root), clock)).unwrap();

    // Assert: the parent requires an exact abort marker from the selected boundary.
    panic!("substitute repair did not reach failpoint {trigger}");
}

#[test]
fn should_allow_strict_writes_when_salvage_substitutes_intact_local_segment() {
    // Arrange: a process dies after segment 1 is cataloged and acknowledged
    // but before its local copy retires, so the WAL ledger still names an
    // intact local member. Then the cataloged remote object is damaged.
    let dir = tempfile::tempdir().unwrap();
    let (remote, corrupt) = prepare_damaged_substituted_segment(dir.path());
    // The restarted process's clock runs past the dead one's lease.
    let clock = SimulatedClock::new();
    clock.advance(SUBSTITUTE_LEASE_TTL * 2);

    // Act: the real startup planner substitutes the verified local bytes.
    let mut recovered = Engine::open(substitute_options(dir.path(), clock)).unwrap();
    let cf = recovered.get_column_family("default").unwrap();
    assert_eq!(get(&recovered, b"seed"), Some(b"value".to_vec()));
    let mut tx = recovered
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .unwrap();
    tx.put(b"after-salvage".to_vec(), b"new".to_vec(), None)
        .unwrap();
    let commit = tx.commit(WriteOptions::cloud_strict());
    let shutdown = recovered.shutdown(Duration::from_secs(10));

    // Assert: the damaged immutable key is not overwritten by the substitute.
    assert!(
        commit.is_ok(),
        "substituted old identity blocks new strict writes: {commit:?}"
    );
    assert!(
        shutdown.is_ok(),
        "recovered engine shutdown failed: {shutdown:?}"
    );
    assert_eq!(std::fs::read(&remote).unwrap(), corrupt);

    // Arrange: reopen after the repair's SST and catalog authority are durable.
    let reopen_clock = SimulatedClock::new();
    reopen_clock.advance(SUBSTITUTE_LEASE_TTL * 2);

    // Act
    let mut reopened = Engine::open(substitute_options(dir.path(), reopen_clock)).unwrap();

    // Assert: both the substituted row and successor commit survive the reopen.
    assert_eq!(get(&reopened, b"seed"), Some(b"value".to_vec()));
    assert_eq!(get(&reopened, b"after-salvage"), Some(b"new".to_vec()));
    assert!(reopened.shutdown(Duration::from_secs(10)).is_ok());
}

#[test]
fn should_recover_substitute_repair_when_crash_precedes_catalog_retirement() {
    recover_substitute_repair_after_crash_at(AFTER_SUBSTITUTE_CHECKPOINT);
}

#[test]
fn should_recover_substitute_repair_when_crash_follows_catalog_retirement() {
    recover_substitute_repair_after_crash_at(AFTER_SUBSTITUTE_CATALOG_RETIRE);
}

fn recover_substitute_repair_after_crash_at(trigger: &str) {
    // Arrange: preserve the acknowledged cache and damage the remote copy.
    let dir = tempfile::tempdir().unwrap();
    let (remote, corrupt) = prepare_damaged_substituted_segment(dir.path());
    let mut child = std::process::Command::new(std::env::current_exe().unwrap());
    child
        .args(["--exact", SUBSTITUTE_REPAIR_CHILD, "--nocapture"])
        .env(SUBSTITUTE_ENV, dir.path())
        .env(SUBSTITUTE_REPAIR_ENV, trigger);

    // Act: crash at the selected durable authority boundary, then recover normally.
    crash::run_child_expect_abort(&mut child, SUBSTITUTE_REPAIR_SCENARIO, trigger, dir.path());
    let clock = SimulatedClock::new();
    clock.advance(SUBSTITUTE_LEASE_TTL * 4);
    let mut recovered = Engine::open(substitute_options(dir.path(), clock)).unwrap();

    // Assert: acknowledged data remains available, the damaged key is not
    // overwritten, and the interrupted repair can finish on the next open.
    assert_eq!(get(&recovered, b"seed"), Some(b"value".to_vec()));
    assert_eq!(std::fs::read(&remote).unwrap(), corrupt);
    assert!(recovered.shutdown(Duration::from_secs(10)).is_ok());
}
