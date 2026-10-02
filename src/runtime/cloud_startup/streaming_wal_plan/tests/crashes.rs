//! Actual aborts at salvage floor, quarantine, and catalog boundaries.
use super::*;
use crate::runtime::hybrid_persistence::CloudPersistence;
use crate::storage::HybridStorage;

#[allow(dead_code)]
#[path = "../../../../../tests/common/crash.rs"]
mod crash;

const CHILD: &str = "runtime::cloud_startup::streaming_wal_plan::tests::crashes::should_abort_salvage_when_child_requested";
const CHILD_ENV: &str = "MIDGE_SALVAGE_CRASH_CASE";

fn persistence(fixture: &Fixture) -> MidgeResult<CloudPersistence> {
    let local = Arc::new(crate::storage::filesystem::FileSystem::new(
        fixture.directory.path().join("hybrid"),
    )?);
    Ok(CloudPersistence::new(Arc::new(HybridStorage::with_policy(
        local,
        Arc::clone(&fixture.cloud),
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    ))))
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
    let local = Arc::new(crate::storage::filesystem::FileSystem::new(
        case.root.join("hybrid"),
    )?);
    let persistence = CloudPersistence::new(Arc::new(HybridStorage::with_policy(
        local,
        Arc::clone(&cloud),
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )));
    let recovered = StreamingCloudWalRecovery::build(
        &case.root.join("local"),
        &cloud,
        &catalog,
        RecoveryPolicy::Salvage,
        Duration::from_secs(5),
        127,
        limits(),
    )?;
    crash::configure_nth_abort_failpoint(&case.trigger, "salvage-prefix", case.ordinal);
    // Act
    recovered
        .plan
        .commit_set_aside(&persistence, 9, &catalog, &case.root.join("local"))?;
    // Assert
    panic!("salvage crash boundary was not reached");
}

#[test]
fn should_preserve_floor_when_salvage_aborts_at_each_publication_boundary() -> MidgeResult<()> {
    // Arrange
    let tuples = [
        ("midge::recovery::after_salvage_floor_before_quarantine", 1),
        ("midge::recovery::after_salvage_quarantine_rename", 1),
        ("midge::recovery::after_salvage_quarantine_rename", 2),
        ("midge::recovery::after_salvage_quarantine_rename", 3),
        ("midge::recovery::before_salvage_catalog_retirement", 1),
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
    std::fs::write(artifacts.join("run.json"), serde_json::to_vec_pretty(&serde_json::json!({
        "revision": String::from_utf8_lossy(&revision.stdout).trim(), "dirty": !status.stdout.is_empty(),
        "seed": 0x4d49_4447_4530_3331_u64, "backend": "CloudSimulated", "durability": "durable_catalog",
        "tuples": tuples, "sealed_epochs": [8,7,9], "sealed_sequences": [1,2,3], "active_epoch": 7, "active_sequence": 4
    })).unwrap())?;
    for (case_index, (trigger, ordinal)) in tuples.into_iter().enumerate() {
        let (mut fixture, catalog_path, mut command) = prepare_case(trigger, ordinal)?;
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
                fixture.catalog =
                    WalPublicationCatalog::decode(&std::fs::read(&catalog_path)?).unwrap();
                let recovered = fixture.plan_only(RecoveryPolicy::Salvage, limits())?;
                recovered.plan.commit_set_aside(
                    &persistence(&fixture)?,
                    9,
                    &fixture.catalog,
                    &fixture.directory.path().join("local"),
                )?;
                fixture.catalog =
                    WalPublicationCatalog::decode(&std::fs::read(&catalog_path)?).unwrap();
                let reopened = fixture.plan_only(RecoveryPolicy::Strict, limits())?;
                reopens.set(1);
                // Assert
                assert_eq!(fixture.catalog.sequence_floor, 4);
                assert_eq!(
                    fixture.catalog.segments.keys().copied().collect::<Vec<_>>(),
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
                assert!(reopened.plan.active_wal.is_none());
                assert_eq!(reopened.plan.max_unreplayed_sequence, 4);
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
    let mut fixture = Fixture::new()?;
    for (id, epoch) in [(1, 8), (2, 7), (3, 9)] {
        let bytes = framed_wal(id, epoch, b"value");
        fixture.publish(id, id, epoch, &bytes)?;
        fixture.local(&crate::wal::segment_file_name(id), &bytes)?;
    }
    fixture.local(crate::wal::ACTIVE_FILE_NAME, &framed_wal(4, 7, b"active"))?;
    let catalog_path = fixture
        .directory
        .path()
        .join("cloud")
        .join(crate::wal::cloud_catalog::OBJECT_KEY);
    std::fs::create_dir_all(catalog_path.parent().unwrap())?;
    std::fs::write(&catalog_path, fixture.catalog.encode().unwrap())?;
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
