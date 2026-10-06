//! Composed native writer takeover, late WAL completion, cleanup and recovery.
//! These three explicit schedules are not an exhaustive simulation campaign.
#[path = "authority_publication/proxy.rs"]
mod proxy;
#[path = "authority_publication/read_fencing.rs"]
mod read_fencing;

use super::*;
use cntryl_midge::{MidgeError, Query};
use proxy::{Boundary, Proxy};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::{mpsc, Arc};
use std::time::Instant;

const CHILD_ENV: &str = "MIDGE_AUTHORITY_RECOVERY_CONFIG";
const CHILD_TEST: &str =
    "authority_publication::should_verify_acknowledged_state_in_fresh_recovery_process";
const WAIT: Duration = Duration::from_secs(60);

#[derive(Serialize, Deserialize)]
struct RecoveryConfig {
    bucket: String,
    prefix: String,
    cache: PathBuf,
    expected: BTreeMap<String, String>,
}

struct Evidence {
    path: PathBuf,
    predecessor: Arc<proxy::Control>,
    successor: Arc<proxy::Control>,
    details: serde_json::Value,
}

impl Drop for Evidence {
    fn drop(&mut self) {
        self.details["predecessor_requests"] =
            serde_json::to_value(self.predecessor.observations()).unwrap();
        self.details["successor_requests"] =
            serde_json::to_value(self.successor.observations()).unwrap();
        let result = serde_json::to_vec_pretty(&self.details)
            .map_err(std::io::Error::other)
            .and_then(|bytes| std::fs::write(&self.path, bytes));
        if let Err(error) = result {
            eprintln!("could not retain authority fixture evidence: {error}");
        }
    }
}

#[test]
#[ignore = "requires Sqrzl; run cloud-integration.yml"]
fn should_preserve_acknowledged_history_when_predecessor_wal_resumes_after_takeover_and_cleanup() {
    // Arrange
    require_sqrzl("composed-authority-publication");
    for boundary in [
        Boundary::BeforePut,
        Boundary::AfterPut,
        Boundary::LostResponse,
    ] {
        run_schedule(boundary);
    }
    // Act / Assert: each schedule exercises actual native Engine operations,
    // checks the frontier/catalog, and verifies two independent recoveries.
}

fn options(
    cache: &Path,
    bucket: &str,
    prefix: &str,
    endpoint: &str,
) -> cntryl_midge::OpenOptionsBuilder {
    let secret = std::env::var("SQRZL_SECRET_ACCESS_KEY").expect("local Sqrzl credential");
    OpenOptions::cloud(
        cache,
        CloudStorageLocation::new(
            CloudProviderConfig::s3_compatible_static(bucket, endpoint, "admin", secret),
            prefix,
        ),
    )
    .background_compaction(false)
    .storage_io_timeout(Duration::from_secs(40))
    .runtime_response_timeout(WAIT)
    .lease_clock_skew_tolerance(Duration::ZERO)
    .with_memtable_size_limit(1024 * 1024)
    .with_memtable_flush_threshold(1024 * 1024)
}

fn write_rows(engine: &Engine, rows: &[(&str, Option<&str>)]) -> Result<(), MidgeError> {
    let cf = default_cf(engine);
    let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    for (key, value) in rows {
        if let Some(value) = value {
            tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)?;
        } else {
            tx.delete(key.as_bytes().to_vec())?;
        }
    }
    tx.commit(WriteOptions::cloud_strict())
}

fn assert_exact(engine: &Engine, expected: &BTreeMap<String, String>) {
    let cf = default_cf(engine);
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("verification snapshot");
    let actual = tx
        .scan(&Query::new())
        .expect("exact inventory")
        .try_collect()
        .expect("scan must not hide errors");
    let actual: BTreeMap<_, _> = actual
        .into_iter()
        .map(|(key, value)| {
            (
                String::from_utf8(key.to_vec()).unwrap(),
                String::from_utf8(value.to_vec()).unwrap(),
            )
        })
        .collect();
    assert_eq!(
        &actual, expected,
        "exact state must include every acknowledged effect and exclude uncatalogued history"
    );
    for (key, value) in expected {
        assert_eq!(
            tx.get(key.as_bytes()).unwrap(),
            Some(Bytes::copy_from_slice(value.as_bytes()))
        );
    }
    for key in ["deleted", "uncertain-a", "uncertain-b", "rejected"] {
        assert_eq!(
            tx.get(key.as_bytes()).unwrap(),
            None,
            "deleted and uncatalogued values must not appear"
        );
    }
}

fn catalog(bucket: &str, prefix: &str) -> Vec<u8> {
    signed_s3_request(
        "GET",
        &format!("/{bucket}/{prefix}/wal/publication-catalog.v1.json"),
        &[],
    )
    .expect("read actual authoritative catalog")
}

fn expected_state() -> BTreeMap<String, String> {
    [
        ("prefix", "durable"),
        ("overwritten", "new"),
        ("atomic-a", "final-a"),
        ("atomic-b", "final-b"),
        ("successor", "two"),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_string(), value.to_string()))
    .collect()
}

// Keep the ordered public operations and their assertions together so the
// happens-before relationship is reviewable without hidden scenario helpers.
#[allow(clippy::too_many_lines)]
fn run_schedule(boundary: Boundary) {
    let directory = tempfile::tempdir().expect("isolated caches");
    let case = uuid::Uuid::new_v4().to_string();
    let bucket = format!("midge-authority-{case}");
    let prefix = "db";
    ensure_sqrzl_s3_bucket(&bucket).expect("isolated remote namespace");
    let artifacts = std::env::var_os("MIDGE_QUALIFICATION_ARTIFACT_DIR")
        .map_or_else(
            || PathBuf::from("target/authority-publication"),
            PathBuf::from,
        )
        .join(&case);
    std::fs::create_dir_all(&artifacts).unwrap();
    let predecessor_proxy = Proxy::start("127.0.0.1:9000".parse().unwrap(), boundary);
    let successor_proxy = Proxy::start("127.0.0.1:9000".parse().unwrap(), Boundary::BeforePut);
    let commit = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap();
    assert!(commit.status.success());
    let mut evidence = Evidence {
        path: artifacts.join("receipt.json"),
        predecessor: Arc::clone(&predecessor_proxy.control),
        successor: Arc::clone(&successor_proxy.control),
        details: json!({
            "schema_version": 1, "boundary": boundary,
            "git_commit": String::from_utf8(commit.stdout).unwrap().trim(),
            "crate_version": env!("CARGO_PKG_VERSION"),
            "fixture_sha256": hex::encode(Sha256::digest(concat!(
                include_str!("authority_publication.rs"),
                include_str!("authority_publication/proxy.rs"),
                include_str!("native_startup_proxy.rs"),
            ))),
            "test_binary_sha256": hex::encode(Sha256::digest(
                std::fs::read(std::env::current_exe().unwrap()).unwrap()
            )),
            "passed": false, "phase": "predecessor_open", "fresh_process_recoveries": 0,
            "expected_recovery_state": expected_state(),
            "acknowledged_transactions": [], "accepted_unacknowledged": [], "rejected": [],
        }),
    };
    eprintln!("authority fixture {boundary:?}: {}", artifacts.display());
    let (loss_tx, loss_rx) = mpsc::sync_channel(1);
    let predecessor = Arc::new(
        Engine::open(
            options(
                &directory.path().join("predecessor"),
                &bucket,
                prefix,
                &predecessor_proxy.endpoint,
            )
            .lease_ttl(Duration::from_secs(18))
            .shutdown_cloud_drain_timeout_for_testing(Duration::from_millis(100))
            .on_lease_loss(move || {
                let _ = loss_tx.try_send(());
            })
            .build()
            .unwrap(),
        )
        .expect("predecessor native engine"),
    );
    write_rows(
        &predecessor,
        &[
            ("prefix", Some("durable")),
            ("overwritten", Some("old")),
            ("deleted", Some("old")),
            ("atomic-a", Some("old")),
            ("atomic-b", Some("old")),
        ],
    )
    .expect("durable prefix ACK");
    evidence.details["acknowledged_transactions"] = json!(["predecessor_prefix"]);
    let prefix_frontier = predecessor
        .metrics()
        .get_runtime_metrics()
        .unwrap()
        .wal_cloud_durable_seq;
    assert!(prefix_frontier > 0);
    let initial_catalog: serde_json::Value =
        serde_json::from_slice(&catalog(&bucket, prefix)).unwrap();
    predecessor_proxy.control.arm();
    let writer = Arc::clone(&predecessor);
    let pending = std::thread::spawn(move || {
        write_rows(
            &writer,
            &[
                ("uncertain-a", Some("pending")),
                ("uncertain-b", Some("pending")),
            ],
        )
    });
    let held = predecessor_proxy.control.reached();
    evidence.details["accepted_unacknowledged"] = json!(["uncertain-a", "uncertain-b"]);
    evidence.details["held"] = serde_json::to_value(&held).unwrap();
    evidence.details["prefix_frontier"] = json!(prefix_frontier);
    evidence.details["phase"] = json!("takeover");
    assert_eq!(held.forwarded, !matches!(boundary, Boundary::BeforePut));
    if held.forwarded {
        assert_eq!(
            held.status,
            Some(200),
            "post-PUT gates require actual upstream success"
        );
    }
    predecessor_proxy.control.stop_renewals();
    loss_rx
        .recv_timeout(WAIT)
        .expect("real predecessor authority loss");
    let rejected = write_rows(&predecessor, &[("rejected", Some("must-not-appear"))]);
    assert!(
        matches!(rejected, Err(MidgeError::Fenced(_))),
        "new mutation after authority loss must be rejected: {rejected:?}"
    );
    evidence.details["rejected"] = json!(["rejected"]);

    // Act: acquisition succeeds only after the persisted lease expires. No
    // fixture rewrites a lease, forces an epoch, or fabricates provider success.
    let deadline = Instant::now() + WAIT;
    let mut successor = loop {
        match Engine::open(
            options(
                &directory.path().join("successor"),
                &bucket,
                prefix,
                &successor_proxy.endpoint,
            )
            .build()
            .unwrap(),
        ) {
            Ok(engine) => break engine,
            Err(MidgeError::LeaseHeld(_)) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(25));
            }
            Err(error) => {
                panic!("successor acquisition must succeed after genuine expiry: {error}")
            }
        }
    };
    write_rows(
        &successor,
        &[
            ("overwritten", Some("new")),
            ("deleted", None),
            ("atomic-a", Some("new-a")),
            ("atomic-b", Some("new-b")),
            ("successor", Some("one")),
        ],
    )
    .expect("successor strict ACK");
    let cf = default_cf(&successor);
    successor
        .flush_cf(&cf)
        .expect("first complete SST publication");
    write_rows(
        &successor,
        &[
            ("atomic-a", Some("final-a")),
            ("atomic-b", Some("final-b")),
            ("successor", Some("two")),
        ],
    )
    .expect("second successor strict ACK");
    evidence.details["acknowledged_transactions"] =
        json!(["predecessor_prefix", "successor_first", "successor_second"]);
    successor
        .flush_cf(&cf)
        .expect("second complete SST publication");
    successor
        .compact_all()
        .expect("complete replacement publication");
    wait_for_reclamation(&successor_proxy);
    let expected = expected_state();
    assert_exact(&successor, &expected);
    successor.shutdown(WAIT).expect("successor owned shutdown");
    drop(successor);

    let committed_catalog = catalog(&bucket, prefix);
    let successor_catalog: serde_json::Value = serde_json::from_slice(&committed_catalog).unwrap();
    assert!(
        successor_catalog["fencing_epoch"].as_u64().unwrap()
            > initial_catalog["fencing_epoch"].as_u64().unwrap()
    );
    evidence.details["predecessor_epoch"] = initial_catalog["fencing_epoch"].clone();
    evidence.details["successor_epoch"] = successor_catalog["fencing_epoch"].clone();
    std::fs::write(artifacts.join("successor-catalog.json"), &committed_catalog).unwrap();
    evidence.details["phase"] = json!("late_predecessor_completion");
    let predecessor_release_index = predecessor_proxy.control.observations().len();
    predecessor_proxy.control.release();
    predecessor_proxy.control.wait_finished();
    assert!(
        predecessor_proxy
            .control
            .observations()
            .iter()
            .any(|event| event.path == held.path
                && event.method == "PUT"
                && event.status == Some(200)),
        "held WAL PUT must actually succeed upstream after release"
    );
    let result = pending.join().expect("predecessor client thread");
    evidence.details["predecessor_result"] = json!(format!("{result:?}"));
    assert!(
        matches!(result, Err(MidgeError::Fenced(_))),
        "stale strict waiter must be fenced, got {result:?}"
    );
    let deadline = Instant::now() + WAIT;
    let settled = loop {
        let metrics = predecessor.metrics().get_runtime_metrics().unwrap();
        if metrics.pending_cloud_uploads == 0 {
            break metrics;
        }
        assert!(
            Instant::now() < deadline,
            "late upload must settle in the fenced runtime"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(
        settled.wal_cloud_durable_seq, prefix_frontier,
        "late completion must not advance stale durability frontier"
    );
    let mut predecessor = Arc::try_unwrap(predecessor)
        .unwrap_or_else(|_| panic!("predecessor client must release engine"));
    // Fencing preserves the accepted, unpublished WAL obligation. Shutdown
    // must report that incomplete drain rather than erase it or claim success.
    let predecessor_shutdown = predecessor.shutdown(WAIT);
    evidence.details["predecessor_shutdown"] = json!(format!("{predecessor_shutdown:?}"));
    evidence.details["settled_frontier"] = json!(settled.wal_cloud_durable_seq);
    assert!(
        matches!(&predecessor_shutdown, Err(MidgeError::Timeout(message))
            if message.contains("0 storage-owned and 1 runtime-owned cloud uploads")),
        "fenced dirty runtime must retain its unpublished obligation: {predecessor_shutdown:?}"
    );
    drop(predecessor);
    assert!(predecessor_proxy.control.observations()[predecessor_release_index..]
        .iter().all(|event| !event.forwarded
            || !matches!(event.method.as_str(), "PUT" | "DELETE")
            || (event.method == "PUT" && event.path == held.path)),
        "stale predecessor may complete the held immutable PUT but must not mutate control authority or delete objects");
    signed_s3_request("GET", &held.path, &[])
        .expect("successful late immutable WAL must remain an uncatalogued orphan");
    assert_eq!(
        catalog(&bucket, prefix),
        committed_catalog,
        "late completion and predecessor cleanup must preserve successor catalog bytes"
    );

    // Assert: each recovery starts in a new process with an entirely absent
    // cache. The ledger lives in the parent and contains no provider credentials.
    let config = RecoveryConfig {
        bucket,
        prefix: prefix.into(),
        cache: directory.path().join("fresh-recovery"),
        expected,
    };
    let config_path = artifacts.join("recovery-config.json");
    std::fs::write(&config_path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();
    evidence.details["phase"] = json!("fresh_process_recovery");
    for attempt in 0..2 {
        if config.cache.exists() {
            std::fs::remove_dir_all(&config.cache).unwrap();
        }
        run_recovery_child(&config_path, &artifacts, attempt);
        evidence.details["fresh_process_recoveries"] = json!(attempt + 1);
    }
    let observations = successor_proxy.control.observations();
    assert!(
        observations.iter().any(|event| event.method == "DELETE"
            && proxy::is_wal_path(&event.path)
            && matches!(event.status, Some(200 | 204))),
        "schedule must exercise actual successful remote WAL reclamation"
    );
    evidence.details["phase"] = json!("complete");
    evidence.details["passed"] = json!(true);
}

fn wait_for_reclamation(proxy: &Proxy) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let observations = proxy.control.observations();
        let successful_delete = |extension: &str| {
            observations.iter().any(|event| {
                event.method == "DELETE"
                    && Path::new(&event.path)
                        .extension()
                        .is_some_and(|actual| actual == extension)
                    && matches!(event.status, Some(200 | 204))
            })
        };
        if successful_delete("wal") && successful_delete("sst") {
            return;
        }
        assert!(Instant::now() < deadline,
            "replacement publication must reclaim an actual remote WAL and obsolete SST before predecessor release");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn run_recovery_child(config: &Path, artifacts: &Path, attempt: usize) {
    let receipt = artifacts.join(format!("recovery-{attempt}.json"));
    let output_path = artifacts.join(format!("recovery-{attempt}.log"));
    let output = std::fs::File::create(output_path).unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", CHILD_TEST, "--ignored", "--nocapture"])
        .env(CHILD_ENV, config)
        .env("MIDGE_AUTHORITY_RECOVERY_RECEIPT", &receipt)
        .stdout(Stdio::from(output.try_clone().unwrap()))
        .stderr(Stdio::from(output))
        .spawn()
        .unwrap();
    let deadline = Instant::now() + WAIT;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(
                status.success(),
                "fresh-process exact recovery failed: {status}; see retained log"
            );
            break;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("fresh-process recovery exceeded fixture deadline");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let actual: serde_json::Value =
        serde_json::from_slice(&std::fs::read(receipt).expect("child must execute verification"))
            .unwrap();
    assert_eq!(actual, json!({"verified": true, "shutdown": true}));
}

#[test]
#[ignore = "child entry point for composed authority qualification"]
fn should_verify_acknowledged_state_in_fresh_recovery_process() {
    // Arrange
    let Some(path) = std::env::var_os(CHILD_ENV) else {
        return;
    };
    let config: RecoveryConfig = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert!(
        !config.cache.exists(),
        "recovery cannot inherit local bytes"
    );
    // Act
    let mut engine = Engine::open(
        options(
            &config.cache,
            &config.bucket,
            &config.prefix,
            "http://127.0.0.1:9000",
        )
        .build()
        .unwrap(),
    )
    .expect("strict cloud recovery");
    // Assert
    assert_exact(&engine, &config.expected);
    engine.shutdown(WAIT).expect("recovery shutdown");
    std::fs::write(
        std::env::var_os("MIDGE_AUTHORITY_RECOVERY_RECEIPT").expect("child receipt destination"),
        serde_json::to_vec(&json!({"verified": true, "shutdown": true})).unwrap(),
    )
    .unwrap();
}
