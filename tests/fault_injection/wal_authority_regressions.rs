//! #808 native-provider predecessor-cache regression.
use crate::common::crash;
use cntryl_midge::{
    CloudProviderConfig, CloudStorageLocation, Engine, OpenOptions, TransactionMode, WriteOptions,
};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const CHILD: &str =
    "wal_authority_regressions::should_abort_predecessor_when_audit_child_is_requested";
const CHILD_ENV: &str = "MIDGE_808_PREDECESSOR_CASE";
const SCENARIO: &str = "predecessor-cache";
const TRIGGER: &str = "audit::after-unpublished-commits";

#[derive(Deserialize, Serialize)]
struct Case {
    root: PathBuf,
    bucket: String,
    prefix: String,
}

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
#[ignore = "#808 PR 2: predecessor-cache"]
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
        )
    );
}

#[test]
#[ignore = "#808 PR 5: substitute-upload-collision"]
fn should_allow_strict_writes_when_salvage_substitutes_intact_local_segment() {
    // Arrange: a cataloged remote segment is damaged while its local copy is intact.
    let dir = tempfile::tempdir().unwrap();
    let options = cntryl_midge::OpenOptions::cloud_simulated(dir.path(), "audit", "substitute")
        .background_compaction(false)
        .runtime_response_timeout(Duration::from_secs(5))
        .storage_io_timeout(Duration::from_secs(2))
        .recovery_policy(cntryl_midge::RecoveryPolicy::Salvage)
        .build()
        .unwrap();
    let mut engine = Engine::open(options.clone()).unwrap();
    let cf = engine.get_column_family("default").unwrap();
    let mut seed = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .unwrap();
    seed.put(b"seed".to_vec(), b"value".to_vec(), None).unwrap();
    seed.commit(WriteOptions::cloud_strict()).unwrap();
    engine.shutdown(Duration::from_secs(10)).unwrap();
    drop(engine);
    let catalog: serde_json::Value = serde_json::from_slice(
        &std::fs::read(
            dir.path()
                .join("cloud_store/wal/publication-catalog.v1.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let (id, entry) = catalog["segments"]
        .as_object()
        .unwrap()
        .iter()
        .next()
        .expect("unflushed segment retained");
    let id = id.parse::<u64>().unwrap();
    let remote = dir
        .path()
        .join("cloud_store")
        .join(entry["object_key"].as_str().unwrap());
    let original = std::fs::read(&remote).unwrap();
    std::fs::write(
        dir.path()
            .join("wal")
            .join(cntryl_midge::__internal::wal::segment_file_name(id)),
        &original,
    )
    .unwrap();
    let mut corrupt = original;
    corrupt[8] ^= 1;
    std::fs::write(remote, corrupt).unwrap();

    // Act: the real startup planner substitutes the verified local bytes.
    let mut recovered = Engine::open(options).unwrap();
    let cf = recovered.get_column_family("default").unwrap();
    assert_eq!(get(&recovered, b"seed"), Some(b"value".to_vec()));
    let mut tx = recovered
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .unwrap();
    tx.put(b"after-salvage".to_vec(), b"new".to_vec(), None)
        .unwrap();
    let commit = tx.commit(WriteOptions::cloud_strict());
    let _ = recovered.shutdown(Duration::from_secs(10));

    // Assert: recovered bytes must be resealed under a fresh immutable identity.
    assert!(
        commit.is_ok(),
        "substituted old identity blocks new strict writes: {commit:?}"
    );
}
