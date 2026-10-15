//! Actual aborts at salvage floor, ledger set-aside, and catalog boundaries.
use super::*;
use crate::runtime::hybrid_persistence::CloudPersistence;
use crate::storage::HybridStorage;
use crate::wal::cloud_catalog::CatalogEntry;
use std::path::PathBuf;

#[allow(dead_code)]
#[path = "../../../../../tests/common/crash.rs"]
mod crash;

const CHILD: &str = "runtime::cloud_startup::streaming_wal_plan::tests::crashes::should_abort_salvage_when_child_requested";
const CHILD_ENV: &str = "MIDGE_SALVAGE_CRASH_CASE";

/// Make `recovered`'s plan durable as startup does, with no authority
/// check of its own.
fn commit(
    recovered: &StreamingCloudWalRecovery,
    persistence: &CloudPersistence,
    catalog: &WalPublicationCatalog,
    history: crate::runtime::wal_history::WalHistory,
) -> MidgeResult<crate::runtime::wal_history::WalHistory> {
    crate::runtime::cloud_startup::commit_recovery_plan_within(
        &recovered.plan,
        persistence,
        9,
        catalog,
        crate::runtime::cloud_startup::CacheHistory::Existing(history),
        None,
        &|| Ok(()),
        &crate::common::DeadlineScope::new(crate::common::OperationDeadline::unbounded()),
    )
}

fn persistence(fixture: &Fixture) -> CloudPersistence {
    CloudPersistence::new(Arc::new(HybridStorage::with_policy(
        Arc::clone(&fixture.cloud),
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )))
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Case {
    root: PathBuf,
    trigger: String,
    ordinal: usize,
}

#[test]
fn should_abort_salvage_when_child_requested() -> MidgeResult<()> {
    // Arrange
    let Some(path) = std::env::var_os(CHILD_ENV) else {
        return Ok(());
    };
    let case: Case = serde_json::from_slice(&std::fs::read(path)?).unwrap();
    let cloud: Arc<dyn StorageBackend> = Arc::new(crate::storage::filesystem::FileSystem::new(
        case.root.join("cloud"),
    )?);
    let catalog = WalPublicationCatalog::decode(&std::fs::read(
        case.root
            .join("cloud")
            .join(crate::wal::cloud_catalog::OBJECT_KEY),
    )?)
    .unwrap();
    let persistence = CloudPersistence::new(Arc::new(HybridStorage::with_policy(
        Arc::clone(&cloud),
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )));
    let wal: Arc<dyn Fs> =
        Arc::new(crate::io::RealFs::new(case.root.join("local/wal")).map_err(FsError::into_midge)?);
    // Startup opens the ledger under its lease, epoch 9.
    let history = crate::runtime::wal_history::WalHistory::open(wal, 1, 9)?;
    let recovered = StreamingCloudWalRecovery::build(
        &crate::io::real::test_root(case.root.join("local")),
        Some(history.ledger()),
        &cloud,
        &catalog,
        RecoveryPolicy::Salvage,
        Duration::from_secs(5),
        127,
        limits(),
    )?;
    crash::configure_nth_abort_failpoint(&case.trigger, "salvage-prefix", case.ordinal);
    // Act
    commit(&recovered, &persistence, &catalog, history)?;
    // Assert
    panic!("salvage crash boundary was not reached");
}

#[test]
fn should_preserve_floor_when_salvage_aborts_at_each_publication_boundary() -> MidgeResult<()> {
    // Arrange
    let tuples = [
        ("midge::recovery::after_salvage_floor_before_quarantine", 1),
        ("midge::recovery::after_salvage_member_moved", 1),
        ("midge::recovery::after_salvage_member_moved", 2),
    ];
    let artifacts = PathBuf::from(
        std::env::var_os("MIDGE_DISCOVERY_ARTIFACT_DIR")
            .unwrap_or_else(|| "target/discovery-031".into()),
    )
    .join(format!("salvage-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&artifacts)?;
    let revision = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()?;
    let status = std::process::Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=no"])
        .output()?;
    std::fs::write(artifacts.join("run.json"), serde_json::to_vec_pretty(&serde_json::json!({
        "revision": String::from_utf8_lossy(&revision.stdout).trim(), "dirty": !status.stdout.is_empty(),
        "seed": 0x4d49_4447_4530_3331_u64, "backend": "CloudSimulated", "durability": "durable_catalog",
        "tuples": tuples, "published_epochs": [8,8,9], "published_sequences": [1,2,3], "local_epoch": 9, "local_sequence": 4, "active_epoch": 9, "active_sequence": 5
    })).unwrap())?;
    for (case_index, (trigger, ordinal)) in tuples.into_iter().enumerate() {
        let (fixture, _catalog_path, mut command) = prepare_case(trigger, ordinal)?;
        let aborted = std::cell::Cell::new(0);
        let reopens = std::cell::Cell::new(0);
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> MidgeResult<()> {
                // Act
                crash::run_child_expect_abort(
                    &mut command,
                    "salvage-prefix",
                    trigger,
                    fixture.directory.path(),
                );
                aborted.set(1);
                let recovered = fixture.plan_only(RecoveryPolicy::Salvage, limits())?;
                commit(
                    &recovered,
                    &persistence(&fixture),
                    &fixture.catalog(),
                    fixture.history()?,
                )?;
                let reopened = fixture.plan_only(RecoveryPolicy::Strict, limits())?;
                reopens.set(1);
                // Assert
                assert_eq!(fixture.catalog().seq_floor(), 5);
                assert_eq!(
                    fixture
                        .catalog()
                        .entries()
                        .iter()
                        .map(CatalogEntry::id)
                        .collect::<Vec<_>>(),
                    vec![1]
                );
                assert_eq!(
                    reopened
                        .plan
                        .remote_segments
                        .keys()
                        .copied()
                        .collect::<Vec<_>>(),
                    vec![1]
                );
                assert!(reopened.segments.open_segment().is_none());
                assert_eq!(reopened.plan.max_unreplayed_sequence, 5);
                Ok(())
            }));
        let passed = matches!(&result, Ok(Ok(())));
        let failure = match &result {
            Ok(Err(error)) => Some(error.to_string()),
            Err(panic) => Some(
                panic
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| panic.downcast_ref::<&str>().map(ToString::to_string))
                    .unwrap_or_else(|| "non-string panic".into()),
            ),
            Ok(Ok(())) => None,
        };
        std::fs::write(artifacts.join(format!("outcome-{case_index}.json")), serde_json::to_vec_pretty(&serde_json::json!({
            "trigger": trigger, "ordinal": ordinal, "passed": passed, "failure": failure,
            "counters": { "validated_aborts": aborted.get(), "successful_recovery_replans": reopens.get() }
        })).unwrap())?;
        match result {
            Ok(result) => result?,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }
    Ok(())
}

fn prepare_case(
    trigger: &str,
    ordinal: usize,
) -> MidgeResult<(Fixture, PathBuf, std::process::Command)> {
    // Cataloged object 2 is damaged, the hole; the cache holds local-only
    // segment 4 and the open segment 5, which salvage both keeps aside.
    let mut fixture = Fixture::new()?;
    for (id, epoch) in [(1, 8), (2, 8), (3, 9)] {
        fixture.publish(id, id, epoch, &framed_wal(id, b"value"))?;
    }
    corrupt_publication(&mut fixture, 2);
    record_case(fixture, trigger, ordinal)
}

/// Write the cache's local segment 4 and open segment 5 after `fixture`'s
/// catalog, record its ledger and catalog, and the child command that
/// salvages it and aborts at `trigger`'s `ordinal`th hit.
fn record_case(
    mut fixture: Fixture,
    trigger: &str,
    ordinal: usize,
) -> MidgeResult<(Fixture, PathBuf, std::process::Command)> {
    fixture.local_in_epoch(4, 9, &framed_wal(4, b"local"))?;
    fixture.active_in_epoch(9, &framed_wal(5, b"active"))?;
    // The child process opens the ledger this records.
    drop(fixture.history()?);
    let catalog_path = fixture
        .directory
        .path()
        .join("cloud")
        .join(crate::wal::cloud_catalog::OBJECT_KEY);
    // The child reads the catalog the fixture keeps in the cloud store.
    let case = Case {
        root: fixture.directory.path().to_path_buf(),
        trigger: trigger.into(),
        ordinal,
    };
    let case_path = fixture.directory.path().join("case.json");
    std::fs::write(&case_path, serde_json::to_vec(&case).unwrap())?;
    let mut command = std::process::Command::new(std::env::current_exe()?);
    command
        .arg("--exact")
        .arg(CHILD)
        .arg("--nocapture")
        .env(CHILD_ENV, case_path);
    Ok((fixture, catalog_path, command))
}

#[test]
fn should_preserve_successor_active_wal_when_validity_expires_after_salvage_floor(
) -> MidgeResult<()> {
    // Arrange: salvage's one catalog change lands, then the writer's validity
    // expires and a successor writes into the open segment's path.
    let _guard = crate::failpoints::test_failpoint_guard();
    let _scenario = fail::FailScenario::setup();
    let (fixture, catalog_path, _command) = prepare_case("unused", 1)?;
    let recovered = fixture.plan_only(RecoveryPolicy::Salvage, limits())?;
    let validity = Arc::new(crate::lease::LeaseValidity::new());
    validity
        .activate(9, std::time::Instant::now() + Duration::from_mins(1))
        .unwrap();
    let check = Arc::clone(&validity);
    let active = fixture.segment(5);
    let successor = framed_wal(6, b"successor");
    let changed_active = active.clone();
    let changed_bytes = successor.clone();
    fail::cfg_callback(
        "midge::recovery::after_salvage_floor_before_quarantine",
        move || {
            validity.expire_for_test();
            std::fs::write(&changed_active, &changed_bytes).unwrap();
        },
    )
    .unwrap();
    let history = fixture.history()?;
    let catalog = fixture.catalog();

    // Act
    let result = crate::runtime::cloud_startup::commit_recovery_plan_within(
        &recovered.plan,
        &persistence(&fixture),
        9,
        &catalog,
        crate::runtime::cloud_startup::CacheHistory::Existing(history),
        None,
        &|| {
            check
                .remaining(9)
                .map(|_| ())
                .map_err(|error| error.into_validation_error("salvage writer validity"))
        },
        &crate::common::DeadlineScope::new(crate::common::OperationDeadline::unbounded()),
    );

    // Assert: the expired writer stops before touching its ledger, so the
    // successor's bytes survive where it wrote them, and the catalog the
    // successor sees is consistent: the suffix set aside behind its floor.
    assert!(matches!(result, Err(MidgeError::Fenced(_))), "{result:?}");
    assert_eq!(std::fs::read(&active)?, successor);
    assert!(fixture.salvaged(5).is_none());
    let catalog = WalPublicationCatalog::decode(&std::fs::read(&catalog_path)?).unwrap();
    assert_eq!(catalog.seq_floor(), 5);
    assert_eq!(
        catalog
            .entries()
            .iter()
            .map(CatalogEntry::id)
            .collect::<Vec<_>>(),
        vec![1]
    );
    Ok(())
}

#[test]
fn should_open_strictly_without_set_aside_segments_when_salvage_stops_after_its_catalog_commit(
) -> MidgeResult<()> {
    // Arrange: cataloged object 2 is damaged and the cache holds no copy of
    // it; local segment 4 and open segment 5 follow. Salvage stops after its
    // catalog commit, before the ledger records the set-aside.
    let trigger = "midge::recovery::after_salvage_floor_before_quarantine";
    let (fixture, _catalog_path, mut command) = prepare_case(trigger, 1)?;
    let head = published_link(&fixture, 1)?;
    crash::run_child_expect_abort(
        &mut command,
        "salvage-prefix",
        trigger,
        fixture.directory.path(),
    );

    // Act: a strict open after the crash.
    let reopened = fixture.plan_only(RecoveryPolicy::Strict, limits())?;
    let history = commit(
        &reopened,
        &persistence(&fixture),
        &fixture.catalog(),
        fixture.history()?,
    )?;

    // Assert: only the history before the hole replays, nothing salvage set
    // aside comes back, and new writes start above all of it.
    assert_eq!(
        reopened
            .plan
            .remote_segments
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![1]
    );
    assert!(reopened.plan.local_segments.is_empty());
    assert!(reopened.segments.open_segment().is_none());
    assert_eq!(reopened.plan.max_unreplayed_sequence, 5);
    assert_eq!(history.ledger().open_member().open.prev, head);
    Ok(())
}
