//! Replayable serial process-abort tuples, independent of logical shrinking.

use super::driver::{self, Backend, Fixture, ReadPath};
use cntryl_midge::{Engine, Query, TransactionMode, WriteOptions};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

const CHILD_ENV: &str = "MIDGE_DISCOVERY_CRASH_CASE";
const CHILD_TEST: &str = "physical::should_abort_physical_history_when_child_requested";
const DEFER_WAL_PRUNE: &str = "midge::cloud::defer_wal_prune_admission";
const RESTORE_TRIGGER: &str = "midge::backup::after_restore_object_copy";
const COMPACTION_TRIGGERS: [&str; 3] = [
    "slice7::after_compaction_output_durable_before_manifest_publish",
    "slice6::after_compaction_update_before_manifest_persist",
    "slice6::after_manifest_persist_before_sst_gc",
];

#[derive(Debug, Deserialize, Serialize)]
struct CrashCase {
    root: PathBuf,
    backend: Backend,
    scenario: String,
    failpoint: String,
    ordinal: usize,
}

fn options(path: &Path, backend: Backend) -> cntryl_midge::MidgeResult<cntryl_midge::OpenOptions> {
    driver::options(
        path,
        Fixture {
            backend,
            read_path: ReadPath::Resident,
        },
    )
}

fn write_options(backend: Backend) -> WriteOptions {
    if matches!(backend, Backend::Local) {
        WriteOptions::sync()
    } else {
        WriteOptions::cloud_strict()
    }
}

fn seed(path: &Path, backend: Backend) {
    let mut engine = Engine::open(options(path, backend).unwrap()).unwrap();
    let cf = engine.get_column_family("default").unwrap();
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .unwrap();
    for index in 0..16 {
        tx.put(format!("key-{index:02}").into_bytes(), vec![42; 1024], None)
            .unwrap();
    }
    tx.commit(write_options(backend)).unwrap();
    engine.flush_cf(&cf).unwrap();
    engine.shutdown(Duration::from_secs(10)).unwrap();
}

#[derive(Default, Serialize)]
struct Counters {
    validated_aborts: usize,
    successful_reopens: usize,
    validated_scans: usize,
}

fn assert_seed_rows(rows: &[(bytes::Bytes, bytes::Bytes)]) {
    assert_eq!(rows.len(), 16);
    for (index, (key, value)) in rows.iter().enumerate() {
        assert_eq!(key.as_ref(), format!("key-{index:02}").as_bytes());
        assert_eq!(value.as_ref(), [42; 1024]);
    }
}

#[test]
fn should_reject_substituted_key_when_validating_restore() {
    // Arrange
    let mut rows: Vec<_> = (0..16)
        .map(|index| {
            (
                bytes::Bytes::from(format!("key-{index:02}")),
                bytes::Bytes::from(vec![42; 1024]),
            )
        })
        .collect();
    rows[0].0 = bytes::Bytes::from_static(b"foreign");

    // Act
    let result = std::panic::catch_unwind(|| assert_seed_rows(&rows));

    // Assert
    assert!(result.is_err());
}

fn assert_restored(case: &CrashCase, counters: &mut Counters) {
    let artifact = case.root.join("backup");
    let target = case.root.join("restored");
    assert!(
        !target.exists(),
        "interrupted restore must not publish its target"
    );
    Engine::restore_backup(&artifact, options(&target, case.backend).unwrap())
        .expect("same artifact must restore after an actual process abort");
    let mut engine = Engine::open(options(&target, case.backend).unwrap()).unwrap();
    counters.successful_reopens += 1;
    let cf = engine.get_column_family("default").unwrap();
    {
        let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        let rows = tx.scan(&Query::new()).unwrap().try_collect().unwrap();
        assert_seed_rows(&rows);
    }
    counters.validated_scans += 1;
    engine.shutdown(Duration::from_secs(10)).unwrap();
}

fn assert_remove_only_intent(path: &Path) {
    let bytes = std::fs::read(path.join("intent_log.json")).expect("durable compaction intent");
    let intents: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(
        intents.as_array().unwrap().iter().any(|intent| {
            intent.get("CompactionPublish").is_some_and(|entry| {
                entry["added"].as_array().is_some_and(Vec::is_empty)
                    && entry["removed"]
                        .as_array()
                        .is_some_and(|files| !files.is_empty())
            })
        }),
        "named abort must follow actual filtering to empty output: {intents}"
    );
}

fn cloud_wal_catalogs(path: &Path) -> [serde_json::Value; 2] {
    [
        "publication-catalog.v1.json",
        "publication-catalog.v1.mirror.json",
    ]
    .map(|name| {
        serde_json::from_slice(&std::fs::read(path.join("cloud_store/wal").join(name)).unwrap())
            .unwrap()
    })
}

fn catalogs_are_retired(catalogs: &[serde_json::Value; 2]) -> bool {
    catalogs.iter().all(|catalog| {
        catalog["segments"]
            .as_object()
            .is_some_and(serde_json::Map::is_empty)
    })
}

fn retire_cloud_wal_before_compaction(mut engine: Engine, case: &CrashCase) -> Engine {
    let path = case.root.join("db");
    let retained = cloud_wal_catalogs(&path);
    assert!(
        retained.iter().all(|catalog| {
            catalog["segments"]
                .as_object()
                .is_some_and(|segments| !segments.is_empty())
        }),
        "the regression must start with authoritative WAL still retained"
    );
    fail::remove(DEFER_WAL_PRUNE);
    let family = engine.get_column_family("default").unwrap();
    engine.flush_cf(&family).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if catalogs_are_retired(&cloud_wal_catalogs(&path)) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "cloud WAL retirement did not settle"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    // Catalog publication precedes delivery of the prune completion to the
    // event loop. Reopen captures the retired authority instead of racing that
    // delivery or guessing when the runtime's tombstone horizon moved.
    engine.shutdown(Duration::from_secs(10)).unwrap();
    drop(engine);
    let engine = Engine::open(options(&path, case.backend).unwrap()).unwrap();
    let retired = cloud_wal_catalogs(&path);
    assert!(catalogs_are_retired(&retired));
    std::fs::write(
        case.root.join("wal-retirement.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "retained_before": retained,
            "retired_after_reopen": retired,
        }))
        .unwrap(),
    )
    .unwrap();
    engine
}

fn expire_aborted_local_owner(path: &Path) {
    let local = path.join(".midge_leader");
    if local.exists() {
        let text = std::fs::read_to_string(&local).unwrap();
        let body = text
            .lines()
            .filter(|line| !line.starts_with("checksum: "))
            .map(|line| {
                if line.starts_with("acquired_at: ") {
                    "acquired_at: 1970-01-01T00:00:00Z"
                } else {
                    line
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        let checksum = crc32c::crc32c(body.as_bytes());
        std::fs::write(local, format!("{body}checksum: {checksum}\n")).unwrap();
    }
    let lease = path.join("midge_primary_lease.json");
    if lease.exists() {
        let text = std::fs::read_to_string(&lease).unwrap();
        let mut expired = text
            .lines()
            .map(|line| {
                if line.starts_with("acquired_at: ") {
                    "acquired_at: 1970-01-01T00:00:00Z"
                } else if line.starts_with("expires_at: ") {
                    "expires_at: 1970-01-01T00:00:00Z"
                } else {
                    line
                }
            })
            .collect::<Vec<_>>()
            .join("\n");
        expired.push('\n');
        std::fs::write(lease, expired).unwrap();
    }
    crate::common::crash::clear_crashed_process_acquisition_lock(path);
}

fn assert_empty_compaction_recovery(case: &CrashCase, counters: &mut Counters) {
    let path = case.root.join("db");
    if matches!(case.backend, Backend::CloudSimulated) {
        let evidence: serde_json::Value =
            serde_json::from_slice(&std::fs::read(case.root.join("wal-retirement.json")).unwrap())
                .unwrap();
        assert!(evidence["retained_before"]
            .as_array()
            .unwrap()
            .iter()
            .all(|catalog| { !catalog["segments"].as_object().unwrap().is_empty() }));
        assert!(evidence["retired_after_reopen"]
            .as_array()
            .unwrap()
            .iter()
            .all(|catalog| { catalog["segments"].as_object().unwrap().is_empty() }));
    }
    assert_remove_only_intent(&path);
    expire_aborted_local_owner(&path);
    // Repeat reopen to detect publication/cleanup state that succeeds only once.
    for _ in 0..2 {
        let mut engine = Engine::open(options(&path, case.backend).unwrap()).unwrap();
        counters.successful_reopens += 1;
        let cf = engine.get_column_family("default").unwrap();
        {
            let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
            let rows = tx.scan(&Query::new()).unwrap().try_collect().unwrap();
            assert_eq!(
                rows.len(),
                0,
                "obsolete values must remain deleted across recovery"
            );
        }
        counters.validated_scans += 1;
        engine.shutdown(Duration::from_secs(10)).unwrap();
    }
}

fn run_case(case: &CrashCase, counters: &mut Counters) {
    std::fs::create_dir(&case.root).unwrap();
    let path = case.root.join("case.json");
    std::fs::write(&path, serde_json::to_vec_pretty(case).unwrap()).unwrap();
    seed(&case.root.join("db"), case.backend);
    if case.scenario == "restore_retry" {
        let mut engine =
            Engine::open(options(&case.root.join("db"), case.backend).unwrap()).unwrap();
        engine
            .backup_to(case.root.join("backup"), Duration::from_secs(10))
            .unwrap();
        engine.shutdown(Duration::from_secs(10)).unwrap();
    }
    let mut child = Command::new(std::env::current_exe().unwrap());
    child
        .args(["--exact", CHILD_TEST, "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, &path);
    crate::common::crash::run_child_expect_abort(
        &mut child,
        &case.scenario,
        &case.failpoint,
        &case.root,
    );
    counters.validated_aborts += 1;
    if case.scenario == "restore_retry" {
        assert_restored(case, counters);
    } else {
        assert_empty_compaction_recovery(case, counters);
    }
}

#[test]
fn should_execute_named_abort_histories_when_physical_discovery_is_requested() {
    // Arrange
    if std::env::var("MIDGE_PHYSICAL_DISCOVERY").as_deref() != Ok("1") {
        return;
    }
    let status = Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .unwrap();
    assert!(status.status.success());
    assert!(
        status.stdout.is_empty(),
        "physical discovery requires a clean committed revision"
    );
    let revision = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap();
    assert!(revision.status.success());
    let revision = String::from_utf8(revision.stdout).unwrap();
    let root = std::env::var_os("MIDGE_DISCOVERY_ARTIFACT_DIR")
        .map_or_else(
            || PathBuf::from("target/discovery-031/physical"),
            PathBuf::from,
        )
        .join(uuid::Uuid::new_v4().to_string());
    std::fs::create_dir_all(&root).unwrap();
    let mut cases = Vec::new();
    for backend in [Backend::Local, Backend::CloudSimulated] {
        for (scenario, failpoint) in std::iter::once(("restore_retry", RESTORE_TRIGGER))
            .chain(COMPACTION_TRIGGERS.map(|trigger| ("empty_compaction", trigger)))
        {
            cases.push(CrashCase {
                root: root.join(format!("case-{}", cases.len())),
                backend,
                scenario: scenario.into(),
                failpoint: failpoint.into(),
                ordinal: 1,
            });
        }
    }
    std::fs::write(
        root.join("corpus.json"),
        serde_json::to_vec_pretty(&cases).unwrap(),
    )
    .unwrap();
    std::fs::write(root.join("revision.txt"), revision).unwrap();
    let mut failures = 0;

    // Act
    for case in cases {
        let mut counters = Counters::default();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_case(&case, &mut counters);
        }));
        let failure = result.as_ref().err().map(|payload| {
            payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| {
                    payload
                        .downcast_ref::<&str>()
                        .map(|message| (*message).to_owned())
                })
                .unwrap_or_else(|| "non-string panic".into())
        });
        if result.is_err() {
            failures += 1;
        }
        std::fs::write(case.root.join("outcome.json"), serde_json::to_vec_pretty(&serde_json::json!({
            "passed": result.is_ok(), "seed": super::histories::SEED,
            "backend": case.backend, "failpoint_ordinal": case.ordinal,
            "durability": if matches!(case.backend, Backend::Local) {"sync"} else {"cloud_strict"},
            "planned_reopens": if case.scenario == "empty_compaction" {2} else {1},
            "counters": counters, "failure": failure,
        })).unwrap()).unwrap();
    }

    // Assert
    assert_eq!(
        failures,
        0,
        "physical counterexamples retained under {}",
        root.display()
    );
}

#[test]
fn should_abort_physical_history_when_child_requested() {
    // Arrange
    let Some(path) = std::env::var_os(CHILD_ENV) else {
        return;
    };
    let case: CrashCase = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();

    // Act
    if case.scenario == "restore_retry" {
        crate::common::crash::configure_nth_abort_failpoint(
            &case.failpoint,
            &case.scenario,
            case.ordinal,
        );
        Engine::restore_backup(
            case.root.join("backup"),
            options(&case.root.join("restored"), case.backend).unwrap(),
        )
        .unwrap();
    } else {
        let path = case.root.join("db");
        // Hold WAL through deletion flush, then prove catalog retirement and
        // reopen before testing the named remove-only publication abort.
        crate::common::crash::configure_nth_abort_failpoint(
            &case.failpoint,
            &case.scenario,
            case.ordinal,
        );
        if matches!(case.backend, Backend::CloudSimulated) {
            fail::cfg(DEFER_WAL_PRUNE, "return").unwrap();
        }
        let engine = Engine::open(options(&path, case.backend).unwrap()).unwrap();
        let cf = engine.get_column_family("default").unwrap();
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.delete_range(b"key-".to_vec(), b"key.".to_vec()).unwrap();
        tx.commit(write_options(case.backend)).unwrap();
        engine.flush_cf(&cf).unwrap();
        let engine = if matches!(case.backend, Backend::CloudSimulated) {
            retire_cloud_wal_before_compaction(engine, &case)
        } else {
            engine
        };
        engine.compact_all().unwrap();
    }

    // Assert
    panic!("named crash boundary was not reached");
}

#[test]
fn should_collect_tombstone_when_live_runtime_receives_cloud_wal_retirement() {
    // Arrange: both puts and the deletion retain WAL until the explicit release.
    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    fail::cfg(DEFER_WAL_PRUNE, "return").unwrap();
    let mut engine = Engine::open(options(path, Backend::CloudSimulated).unwrap()).unwrap();
    let cf = engine.get_column_family("default").unwrap();
    let mut put = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .unwrap();
    put.put(b"key-00".to_vec(), b"value".to_vec(), None)
        .unwrap();
    put.commit(WriteOptions::cloud_strict()).unwrap();
    engine.flush_cf(&cf).unwrap();
    let mut delete = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .unwrap();
    delete
        .delete_range(b"key-".to_vec(), b"key.".to_vec())
        .unwrap();
    delete.commit(WriteOptions::cloud_strict()).unwrap();
    engine.flush_cf(&cf).unwrap();
    let retained = cloud_wal_catalogs(path);
    let expected = retained[0]["segments"].as_object().unwrap().len();
    assert!(expected >= 2);
    assert!(!catalogs_are_retired(&retained));
    let delivered = "midge::cloud::after_wal_prune_complete";
    let (tx, rx) = std::sync::mpsc::channel();
    fail::cfg_callback(delivered, move || {
        tx.send(()).unwrap();
    })
    .unwrap();

    // Act: every completion is observed after event-loop delivery. No reopen or
    // catalog polling substitutes for the live runtime moving its horizon.
    fail::remove(DEFER_WAL_PRUNE);
    engine.flush_cf(&cf).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    for _ in 0..expected {
        rx.recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .expect("live event loop must deliver every admitted prune completion");
    }
    fail::remove(delivered);
    assert!(catalogs_are_retired(&cloud_wal_catalogs(path)));
    engine.compact_all().unwrap();

    // Assert: real compaction collects the tombstone in this same Engine.
    assert!(engine
        .metrics()
        .get_storage_layout()
        .unwrap()
        .levels
        .iter()
        .flat_map(|level| &level.files)
        .all(|file| file.cf_id != cf.id()));
    let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
    assert!(read
        .scan(&Query::new())
        .unwrap()
        .try_collect()
        .unwrap()
        .is_empty());
    drop(read);
    engine.shutdown(Duration::from_secs(10)).unwrap();
    drop(engine);
    scenario.teardown();
    let mut reopened = Engine::open(options(path, Backend::CloudSimulated).unwrap()).unwrap();
    let cf = reopened.get_column_family("default").unwrap();
    let read = reopened
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .unwrap();
    assert!(read
        .scan(&Query::new())
        .unwrap()
        .try_collect()
        .unwrap()
        .is_empty());
    drop(read);
    reopened.shutdown(Duration::from_secs(10)).unwrap();
}
