//! Composed native writer takeover, late WAL completion, cleanup and recovery.
//! These three explicit schedules are not an exhaustive simulation campaign.
#[path = "authority_publication/proxy.rs"]
mod proxy;
#[path = "authority_publication/read_fencing.rs"]
mod read_fencing;

use super::*;
use cntryl_midge::__internal::wal::catalog::WalPublicationCatalog;
use cntryl_midge::{MidgeError, Query};
use proxy::{Boundary, Proxy};
use serde::{Deserialize, Serialize};
use serde_json::json;
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
fn should_preserve_acknowledged_history_when_predecessor_wal_resumes_after_takeover_and_cleanup() {
    // Arrange
    require_sqrzl("composed-authority-publication");
    // Act
    for boundary in [
        Boundary::BeforePut,
        Boundary::AfterPut,
        Boundary::LostResponse,
    ] {
        // Assert: native operations preserve the frontier/catalog and exact
        // acknowledged history through two independent recoveries.
        run_schedule(boundary);
    }
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

fn schedule_evidence(
    artifacts: &Path,
    boundary: Boundary,
    predecessor_proxy: &Proxy,
    successor_proxy: &Proxy,
) -> Evidence {
    Evidence {
        path: artifacts.join("receipt.json"),
        predecessor: Arc::clone(&predecessor_proxy.control),
        successor: Arc::clone(&successor_proxy.control),
        details: json!({
            "schema_version": 1, "boundary": boundary,
            "crate_version": env!("CARGO_PKG_VERSION"),
            "passed": false, "phase": "predecessor_open", "fresh_process_recoveries": 0,
            "expected_recovery_state": expected_state(),
            "acknowledged_transactions": [], "accepted_unacknowledged": [], "rejected": [],
        }),
    }
}

fn open_predecessor(
    cache: &Path,
    bucket: &str,
    prefix: &str,
    proxy: &Proxy,
) -> (Arc<Engine>, mpsc::Receiver<()>) {
    let (loss_tx, loss_rx) = mpsc::sync_channel(1);
    let predecessor = Arc::new(
        Engine::open(
            options(cache, bucket, prefix, &proxy.endpoint)
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
    (predecessor, loss_rx)
}

fn write_predecessor_prefix(predecessor: &Engine, evidence: &mut Evidence) -> u64 {
    write_rows(
        predecessor,
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
    prefix_frontier
}

fn assert_predecessor_fenced(
    predecessor: &Engine,
    proxy: &Proxy,
    loss_rx: &mpsc::Receiver<()>,
    evidence: &mut Evidence,
) {
    proxy.control.stop_renewals();
    loss_rx
        .recv_timeout(WAIT)
        .expect("real predecessor authority loss");
    let rejected = write_rows(predecessor, &[("rejected", Some("must-not-appear"))]);
    assert!(
        matches!(rejected, Err(MidgeError::Fenced(_))),
        "new mutation after authority loss must be rejected: {rejected:?}"
    );
    evidence.details["rejected"] = json!(["rejected"]);
}

fn open_successor(options: &cntryl_midge::OpenOptions) -> Engine {
    let deadline = Instant::now() + WAIT;
    loop {
        match Engine::open(options.clone()) {
            Ok(engine) => return engine,
            Err(MidgeError::LeaseHeld(_)) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(25));
            }
            Err(error) => {
                panic!("successor acquisition must succeed after genuine expiry: {error}")
            }
        }
    }
}

fn publish_successor_history(
    cache: &Path,
    bucket: &str,
    prefix: &str,
    proxy: &Proxy,
    evidence: &mut Evidence,
) -> BTreeMap<String, String> {
    let successor_options = options(cache, bucket, prefix, &proxy.endpoint)
        .build()
        .unwrap();
    let mut successor = open_successor(&successor_options);
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
    wait_for_reclamation(proxy);
    let expected = expected_state();
    assert_exact(&successor, &expected);
    successor.shutdown(WAIT).expect("successor owned shutdown");
    drop(successor);
    expected
}

fn assert_stale_frontier_retained(
    predecessor: Arc<Engine>,
    prefix_frontier: u64,
    held_local_wal: &Path,
    evidence: &mut Evidence,
) {
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
    let retained_wal = std::fs::read(held_local_wal).expect("accepted local WAL is retained");
    assert!(
        !retained_wal.is_empty(),
        "held immutable PUT has real WAL bytes"
    );
    // Fencing preserves the accepted, unpublished WAL obligation. Shutdown
    // may first exhaust its budget draining an accepted immutable flush.
    // Prove the retained obligation itself instead of one phase's wording.
    let predecessor_shutdown = predecessor.shutdown(WAIT);
    evidence.details["predecessor_shutdown"] = json!(format!("{predecessor_shutdown:?}"));
    evidence.details["settled_frontier"] = json!(settled.wal_cloud_durable_seq);
    assert!(
        matches!(&predecessor_shutdown, Err(MidgeError::Timeout(_))),
        "fenced dirty runtime must retain its unpublished obligation: {predecessor_shutdown:?}"
    );
    assert_eq!(
        std::fs::read(held_local_wal).expect("failed shutdown retains accepted local WAL"),
        retained_wal
    );
    evidence.details["retained_unpublished_local_wal_bytes"] = json!(retained_wal.len());
    drop(predecessor);
}

fn assert_fresh_process_recoveries(
    directory: &Path,
    bucket: String,
    prefix: &str,
    artifacts: &Path,
    expected: BTreeMap<String, String>,
    evidence: &mut Evidence,
) {
    let config = RecoveryConfig {
        bucket,
        prefix: prefix.into(),
        cache: directory.join("fresh-recovery"),
        expected,
    };
    let config_path = artifacts.join("recovery-config.json");
    std::fs::write(&config_path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();
    evidence.details["phase"] = json!("fresh_process_recovery");
    for attempt in 0..2 {
        if config.cache.exists() {
            std::fs::remove_dir_all(&config.cache).unwrap();
        }
        run_recovery_child(&config_path, artifacts, attempt);
        evidence.details["fresh_process_recoveries"] = json!(attempt + 1);
    }
}

fn hold_predecessor_write(
    predecessor: &Arc<Engine>,
    proxy: &Proxy,
    boundary: Boundary,
    prefix_frontier: u64,
    evidence: &mut Evidence,
) -> (
    std::thread::JoinHandle<Result<(), MidgeError>>,
    proxy::Observation,
) {
    proxy.control.arm();
    let writer = Arc::clone(predecessor);
    let pending = std::thread::spawn(move || {
        write_rows(
            &writer,
            &[
                ("uncertain-a", Some("pending")),
                ("uncertain-b", Some("pending")),
            ],
        )
    });
    let held = proxy.control.reached();
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
    (pending, held)
}

fn release_predecessor_write(
    proxy: &Proxy,
    held: &proxy::Observation,
    pending: std::thread::JoinHandle<Result<(), MidgeError>>,
    evidence: &mut Evidence,
) -> usize {
    let release_index = proxy.control.observations().len();
    proxy.control.release();
    proxy.control.wait_finished();
    assert!(
        proxy
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
        matches!(&result, Err(MidgeError::OutcomeUnknown(_))),
        "a stale writer fenced after applying a WAL-backed transaction must report an unknown outcome, got {result:?}"
    );
    release_index
}

fn run_schedule(boundary: Boundary) {
    // Arrange
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
    let mut evidence =
        schedule_evidence(&artifacts, boundary, &predecessor_proxy, &successor_proxy);
    eprintln!("authority fixture {boundary:?}: {}", artifacts.display());
    let (predecessor, loss_rx) = open_predecessor(
        &directory.path().join("predecessor"),
        &bucket,
        prefix,
        &predecessor_proxy,
    );
    let prefix_frontier = write_predecessor_prefix(&predecessor, &mut evidence);
    let initial_catalog = WalPublicationCatalog::decode(&catalog(&bucket, prefix)).unwrap();
    let (pending, held) = hold_predecessor_write(
        &predecessor,
        &predecessor_proxy,
        boundary,
        prefix_frontier,
        &mut evidence,
    );
    assert_predecessor_fenced(&predecessor, &predecessor_proxy, &loss_rx, &mut evidence);

    // Act: acquire only after persisted lease expiry, then publish replacements.
    let expected = publish_successor_history(
        &directory.path().join("successor"),
        &bucket,
        prefix,
        &successor_proxy,
        &mut evidence,
    );
    let committed_catalog = catalog(&bucket, prefix);
    let successor_catalog = WalPublicationCatalog::decode(&committed_catalog).unwrap();
    assert!(successor_catalog.fencing_epoch() > initial_catalog.fencing_epoch());
    evidence.details["predecessor_epoch"] = json!(initial_catalog.fencing_epoch());
    evidence.details["successor_epoch"] = json!(successor_catalog.fencing_epoch());
    std::fs::write(artifacts.join("successor-catalog.json"), &committed_catalog).unwrap();
    evidence.details["phase"] = json!("late_predecessor_completion");
    let predecessor_release_index =
        release_predecessor_write(&predecessor_proxy, &held, pending, &mut evidence);
    let held_local_wal = directory.path().join("predecessor/wal").join(
        Path::new(&held.path)
            .file_name()
            .expect("held WAL file name"),
    );
    assert_stale_frontier_retained(predecessor, prefix_frontier, &held_local_wal, &mut evidence);
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

    // Assert: both recoveries use new processes and absent caches.
    assert_fresh_process_recoveries(
        directory.path(),
        bucket,
        prefix,
        &artifacts,
        expected,
        &mut evidence,
    );
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
        .args(["--exact", CHILD_TEST, "--nocapture"])
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
