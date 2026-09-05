use super::*;
use crate::common::MidgeError;

struct DelayedRecoveryBackend {
    objects: std::sync::Mutex<std::collections::BTreeMap<String, Vec<u8>>>,
    stalled: std::sync::Mutex<std::collections::BTreeSet<String>>,
    submitted: std::sync::Mutex<std::collections::BTreeSet<String>>,
    in_flight: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    max_in_flight: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl DelayedRecoveryBackend {
    fn new() -> Self {
        Self {
            objects: std::sync::Mutex::new(std::collections::BTreeMap::new()),
            stalled: std::sync::Mutex::new(std::collections::BTreeSet::new()),
            submitted: std::sync::Mutex::new(std::collections::BTreeSet::new()),
            in_flight: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            max_in_flight: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    fn full_key(key: &str) -> String {
        format!("midge/{key}")
    }

    fn insert(&self, key: &str, bytes: Vec<u8>) {
        self.objects
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(Self::full_key(key), bytes);
    }

    fn remove(&self, key: &str) {
        self.objects
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&Self::full_key(key));
    }

    fn stall(&self, key: &str) {
        self.stalled
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(Self::full_key(key));
    }

    fn maximum_in_flight(&self) -> usize {
        self.max_in_flight
            .load(std::sync::atomic::Ordering::Acquire)
    }

    fn was_submitted(&self, key: &str) -> bool {
        self.submitted
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(&Self::full_key(key))
    }
}

impl crate::storage::cloud::CloudBackend for DelayedRecoveryBackend {
    fn submit_put(
        &self,
        key: &str,
        _data: Vec<u8>,
        _headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        let _ = callback.send(crate::storage::cloud::CloudEvent::Put {
            key: key.to_string(),
            result: crate::storage::cloud::CloudOutcome::Err(
                crate::storage::cloud::CloudError::Protocol(
                    "delayed recovery backend is read-only".to_string(),
                ),
            ),
        });
    }

    fn submit_get(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        self.submitted
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key.to_string());
        let active = self
            .in_flight
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            .saturating_add(1);
        self.max_in_flight
            .fetch_max(active, std::sync::atomic::Ordering::AcqRel);

        if self
            .stalled
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(key)
        {
            return;
        }

        let key = key.to_string();
        let bytes = self
            .objects
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&key)
            .cloned();
        let in_flight = std::sync::Arc::clone(&self.in_flight);
        std::thread::spawn(move || {
            let delay = 2 + u64::from(
                key.as_bytes()
                    .iter()
                    .fold(0_u8, |sum, byte| sum.wrapping_add(*byte))
                    % 7,
            );
            std::thread::sleep(std::time::Duration::from_millis(delay));
            let result = bytes.map_or_else(
                || {
                    crate::storage::cloud::CloudOutcome::Err(
                        crate::storage::cloud::CloudError::NotFound(key.clone()),
                    )
                },
                crate::storage::cloud::CloudOutcome::Ok,
            );
            // Completion transfers ownership back to the recovery caller. End
            // the measured in-flight interval before publishing the callback,
            // which may immediately admit the next bounded batch.
            in_flight.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            let _ = callback.send(crate::storage::cloud::CloudEvent::Get { key, result });
        });
    }

    fn submit_get_range(
        &self,
        key: &str,
        start: u64,
        end: Option<u64>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        let _ = callback.send(crate::storage::cloud::CloudEvent::GetRange {
            key: key.to_string(),
            start,
            end,
            result: crate::storage::cloud::CloudOutcome::Err(
                crate::storage::cloud::CloudError::Protocol(
                    "ranged GET is unsupported".to_string(),
                ),
            ),
        });
    }
}

fn recovery_catalog(
    backend: &DelayedRecoveryBackend,
    segment_count: u64,
) -> crate::wal::cloud_catalog::WalPublicationCatalog {
    let mut catalog = crate::wal::cloud_catalog::WalPublicationCatalog::empty(7)
        .expect("create recovery catalog");
    for segment_id in 1..=segment_count {
        let record = crate::wal::WalRecord::new(
            crate::wal::WalOpKind::Put,
            bytes::Bytes::from(format!("key-{segment_id}")),
            Some(bytes::Bytes::from(format!("value-{segment_id}"))),
            segment_id,
            7,
        );
        let payload = crate::wal::encoding::encode(&record).expect("encode recovery WAL record");
        let mut bytes = Vec::new();
        crate::wal::frame::append_frame(&mut bytes, &payload).expect("frame recovery WAL record");
        let publication = crate::wal::cloud_catalog::PublishedWalSegment::from_validated_bytes(
            segment_id, segment_id, 7, &bytes,
        );
        backend.insert(&publication.object_key, bytes);
        catalog.segments.insert(segment_id, publication);
    }
    catalog
}

#[test]
fn should_not_require_durable_directory_sync_when_staging_ephemeral_cloud_wal() -> MidgeResult<()> {
    // Arrange: the recovery directory is discarded and rebuilt from
    // authoritative cloud WAL on every open, so its temporary materialization
    // must not pay or depend on a durable directory barrier per segment.
    let fs = Arc::new(crate::io::MockFs::new());
    fs.set_sync_dir_failure(true);
    let staging_fs: Arc<dyn crate::io::Fs> = fs.clone();
    let file_name = crate::wal::cloud_segment_file_name(17);

    // Act
    CloudStartupRecovery::stage_recovery_wal_bytes(
        &staging_fs,
        &file_name,
        b"authoritative-cloud-wal",
    )?;

    // Assert
    assert_eq!(
        fs.get_file(&format!("cloud_recovery/wal/{file_name}")),
        Some(b"authoritative-cloud-wal".to_vec())
    );
    assert!(fs.sync_dir_calls().is_empty());
    Ok(())
}

#[test]
fn should_bound_parallel_cloud_wal_hydration_to_eight_requests() -> MidgeResult<()> {
    // Arrange
    let db = tempfile::tempdir()?;
    let backend = std::sync::Arc::new(DelayedRecoveryBackend::new());
    let catalog = recovery_catalog(&backend, 17);
    let cloud = crate::storage::cloud::CloudStorage::new(backend.clone(), "midge".to_string());

    // Act
    let plan = CloudStartupRecovery::materialize_cloud_wal_recovery_dir(
        &cloud,
        db.path(),
        crate::config::RecoveryPolicy::Strict,
        &catalog,
    )?;

    // Assert
    assert_eq!(plan.remote_segments.len(), 17);
    assert!(backend.maximum_in_flight() > 1);
    assert!(backend.maximum_in_flight() <= 8);
    assert_eq!(
        plan.remote_segments.keys().copied().collect::<Vec<_>>(),
        (1..=17).collect::<Vec<_>>()
    );
    Ok(())
}

#[test]
fn should_reject_cloud_wal_recovery_before_get_or_staging_when_catalog_exceeds_disk_budget(
) -> MidgeResult<()> {
    // Arrange
    let db = tempfile::tempdir()?;
    let backend = Arc::new(DelayedRecoveryBackend::new());
    let catalog = recovery_catalog(&backend, 2);
    let required = catalog
        .segments
        .values()
        .map(|segment| segment.size_bytes)
        .sum::<u64>();
    let cloud = crate::storage::cloud::CloudStorage::new(backend.clone(), "midge".into());

    // Act
    let result = CloudStartupRecovery::materialize_cloud_wal_recovery_dir_with_budget(
        &cloud,
        db.path(),
        crate::config::RecoveryPolicy::Strict,
        &catalog,
        required - 1,
    );

    // Assert
    assert!(matches!(result, Err(MidgeError::NoSpace(_))));
    assert_eq!(backend.maximum_in_flight(), 0);
    assert!(!db.path().join("cloud_recovery").exists());
    Ok(())
}

#[test]
fn should_account_for_total_working_space_before_wal_hydration() -> MidgeResult<()> {
    // Arrange
    let db = tempfile::tempdir()?;
    let backend = Arc::new(DelayedRecoveryBackend::new());
    let catalog = recovery_catalog(&backend, 1);
    let publication = catalog.segments.values().next().expect("publication");
    let local_path = db
        .path()
        .join("wal")
        .join(crate::wal::cloud_segment_file_name(publication.segment_id));
    std::fs::create_dir_all(local_path.parent().expect("WAL directory"))?;
    std::fs::write(
        &local_path,
        vec![0x5A; usize::try_from(publication.size_bytes).expect("WAL size fits platform")],
    )?;
    let stale_recovery = db.path().join("cloud_recovery").join("sentinel");
    std::fs::create_dir_all(stale_recovery.parent().expect("staging directory"))?;
    std::fs::write(&stale_recovery, b"retain until admitted")?;
    let cloud = crate::storage::cloud::CloudStorage::new(backend.clone(), "midge".into());

    // Act
    let result = CloudStartupRecovery::materialize_cloud_wal_recovery_dir_with_budget(
        &cloud,
        db.path(),
        crate::config::RecoveryPolicy::Strict,
        &catalog,
        publication.size_bytes * 2 - 1,
    );

    // Assert
    assert!(matches!(result, Err(MidgeError::NoSpace(_))));
    assert_eq!(backend.maximum_in_flight(), 0);
    assert_eq!(
        std::fs::metadata(&local_path)?.len(),
        publication.size_bytes
    );
    assert_eq!(std::fs::read(stale_recovery)?, b"retain until admitted");
    Ok(())
}

#[test]
fn should_count_non_wal_recovery_residue_before_downloading_new_wal_segments() -> MidgeResult<()> {
    // Arrange
    let db = tempfile::tempdir()?;
    let backend = Arc::new(DelayedRecoveryBackend::new());
    let catalog = recovery_catalog(&backend, 1);
    let required = catalog
        .segments
        .values()
        .next()
        .expect("publication")
        .size_bytes;
    let retained = db.path().join("cloud_recovery/retained.bin");
    std::fs::create_dir_all(retained.parent().expect("staging directory"))?;
    std::fs::write(&retained, [0x5A; 100])?;
    let cloud = crate::storage::cloud::CloudStorage::new(backend.clone(), "midge".into());

    // Act
    let result = CloudStartupRecovery::materialize_cloud_wal_recovery_dir_with_budget(
        &cloud,
        db.path(),
        crate::config::RecoveryPolicy::Strict,
        &catalog,
        required + 99,
    );

    // Assert
    assert!(matches!(result, Err(MidgeError::NoSpace(_))));
    assert_eq!(backend.maximum_in_flight(), 0);
    assert_eq!(std::fs::metadata(retained)?.len(), 100);
    Ok(())
}

#[test]
fn should_fail_strict_cloud_wal_hydration_when_publication_is_missing() -> MidgeResult<()> {
    // Arrange
    let db = tempfile::tempdir()?;
    let backend = std::sync::Arc::new(DelayedRecoveryBackend::new());
    let catalog = recovery_catalog(&backend, 9);
    backend.remove(&catalog.segments[&3].object_key);
    let cloud = crate::storage::cloud::CloudStorage::new(backend, "midge".to_string());

    // Act
    let result = CloudStartupRecovery::materialize_cloud_wal_recovery_dir(
        &cloud,
        db.path(),
        crate::config::RecoveryPolicy::Strict,
        &catalog,
    );

    // Assert
    let error = result
        .err()
        .expect("missing publication should fail strict recovery");
    assert!(error.to_string().contains(&catalog.segments[&3].object_key));
    Ok(())
}

#[test]
fn should_salvage_valid_cloud_wals_when_publications_are_missing_or_corrupt() -> MidgeResult<()> {
    // Arrange
    let db = tempfile::tempdir()?;
    let backend = std::sync::Arc::new(DelayedRecoveryBackend::new());
    let catalog = recovery_catalog(&backend, 9);
    backend.remove(&catalog.segments[&3].object_key);
    backend.insert(&catalog.segments[&7].object_key, b"corrupt".to_vec());
    let cloud = crate::storage::cloud::CloudStorage::new(backend, "midge".to_string());

    // Act
    let plan = CloudStartupRecovery::materialize_cloud_wal_recovery_dir(
        &cloud,
        db.path(),
        crate::config::RecoveryPolicy::Salvage,
        &catalog,
    )?;

    // Assert
    assert!(plan.opened_in_salvage_mode);
    assert_eq!(plan.remote_segments.len(), 7);
    assert!(!plan.remote_segments.contains_key(&3));
    assert!(!plan.remote_segments.contains_key(&7));
    Ok(())
}

#[test]
fn should_bound_cloud_wal_hydration_timeout_to_one_batch_deadline() -> MidgeResult<()> {
    // Arrange
    let db = tempfile::tempdir()?;
    let backend = std::sync::Arc::new(DelayedRecoveryBackend::new());
    let catalog = recovery_catalog(&backend, 9);
    backend.stall(&catalog.segments[&2].object_key);
    let cloud = crate::storage::cloud::CloudStorage::new_with_timeout(
        backend.clone(),
        "midge".to_string(),
        std::time::Duration::from_millis(25),
    );

    // Act
    let result = CloudStartupRecovery::materialize_cloud_wal_recovery_dir(
        &cloud,
        db.path(),
        crate::config::RecoveryPolicy::Strict,
        &catalog,
    );

    // Assert
    let error = result.err().expect("stalled publication should time out");
    assert!(error.to_string().contains("timed out"));
    assert!(backend.was_submitted(&catalog.segments[&8].object_key));
    assert!(!backend.was_submitted(&catalog.segments[&9].object_key));
    Ok(())
}

struct StartupWatchdogLease {
    validity: std::sync::Arc<crate::lease::LeaseValidity>,
    renewals: std::sync::atomic::AtomicUsize,
}

struct AcquisitionFailureLease {
    error: fn() -> crate::lease::LeaseError,
}

impl crate::lease::PrimaryLease for AcquisitionFailureLease {
    fn try_acquire(
        self: std::sync::Arc<Self>,
    ) -> Result<crate::lease::LeaseGuard, crate::lease::LeaseError> {
        Err((self.error)())
    }

    fn renew(&self) -> Result<(), crate::lease::LeaseError> {
        unreachable!("an unacquired lease must never renew")
    }

    fn release(&self) -> Result<(), crate::lease::LeaseError> {
        Ok(())
    }

    fn ttl(&self) -> std::time::Duration {
        std::time::Duration::from_secs(1)
    }

    fn holder_id(&self) -> String {
        "acquisition-failure".to_string()
    }

    fn epoch(&self) -> u64 {
        0
    }
}

impl crate::lease::PrimaryLease for StartupWatchdogLease {
    fn try_acquire(
        self: std::sync::Arc<Self>,
    ) -> Result<crate::lease::LeaseGuard, crate::lease::LeaseError> {
        self.validity.activate(
            1,
            std::time::Instant::now() + std::time::Duration::from_secs(2),
        )?;
        Ok(crate::lease::LeaseGuard::token())
    }

    fn renew(&self) -> Result<(), crate::lease::LeaseError> {
        self.validity.advance(
            1,
            std::time::Instant::now() + std::time::Duration::from_secs(2),
        )?;
        self.renewals
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    fn release(&self) -> Result<(), crate::lease::LeaseError> {
        self.validity.deactivate(1);
        Ok(())
    }

    fn ttl(&self) -> std::time::Duration {
        std::time::Duration::from_secs(2)
    }

    fn holder_id(&self) -> String {
        "startup-watchdog".to_string()
    }

    fn epoch(&self) -> u64 {
        1
    }
}

#[derive(Clone)]
struct CapturedLogs(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

struct CapturedLogWriter(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
    type Writer = CapturedLogWriter;

    fn make_writer(&'a self) -> Self::Writer {
        CapturedLogWriter(std::sync::Arc::clone(&self.0))
    }
}

impl std::io::Write for CapturedLogWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn should_not_log_cloud_credentials_when_tracing_engine_startup() {
    // Arrange
    let secrets = [
        "s3-access-do-not-log",
        "s3-secret-do-not-log",
        "azure-secret-do-not-log",
        "gcs-access-do-not-log",
        "gcs-secret-do-not-log",
        "gcs-bearer-do-not-log",
    ];
    let providers = [
        crate::storage::providers::CloudProviderConfig::s3_compatible(
            "bucket",
            "region",
            "https://s3.example",
            secrets[0],
            secrets[1],
        ),
        crate::config::CloudProviderConfig::azure_blob_connection_string(
            "container",
            "DefaultEndpointsProtocol=https;AccountName=account;AccountKey=azure-secret-do-not-log",
        ),
        crate::config::CloudProviderConfig::gcs_hmac("bucket", secrets[3], secrets[4]),
        crate::config::CloudProviderConfig::gcs_bearer_token("bucket", secrets[5]),
    ];
    let captured = CapturedLogs(std::sync::Arc::new(std::sync::Mutex::new(Vec::new())));
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_max_level(tracing::Level::TRACE)
        .with_writer(captured.clone())
        .finish();

    // Act
    tracing::subscriber::with_default(subscriber, || {
        for provider in providers {
            let topology = crate::config::CloudStorageTopology::new(
                crate::config::CloudStorageLocation::new(provider, "prefix"),
            )
            .with_sst(crate::config::CloudStorageLocation::new(
                crate::config::CloudProviderConfig::aws_s3("redaction-sst-bucket", "us-east-1"),
                "prefix",
            ))
            .with_control(crate::config::CloudStorageLocation::new(
                crate::config::CloudProviderConfig::aws_s3("redaction-control-bucket", "us-east-1"),
                "prefix",
            ));
            let opts = OpenOptions::cloud_multi("/tmp/midge-redaction", topology)
                .build()
                .expect("build redaction options");
            EngineStartup::trace_open(&opts);
        }
    });
    let logs = String::from_utf8(
        captured
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone(),
    )
    .expect("startup tracing should be UTF-8");

    // Assert
    for secret in secrets {
        assert!(
            !logs.contains(secret),
            "startup tracing leaked configured credential {secret:?}: {logs}"
        );
    }
    assert!(logs.contains("https://s3.example"));
    assert!(logs.contains("[REDACTED]"));
}

#[test]
fn should_apply_open_options_block_cache_policy_to_runtime_config() -> MidgeResult<()> {
    // Arrange
    let opts = OpenOptions::in_memory()
        .block_cache_policy(crate::engine::BlockCachePolicy::ClockPro)
        .build()?;
    let storage_path = StartupStoragePath::resolve(opts.storage());
    storage_path.prepare();
    let startup_lease = StartupLease::acquire(&opts)?;

    // Act
    let materialized =
        RuntimeStorageMaterialization::materialize(&opts, &storage_path, &startup_lease)?;

    // Assert
    assert_eq!(
        materialized.runtime_config.block_cache_policy,
        crate::sst::cache::CachePolicyType::ClockPro
    );
    Ok(())
}

#[test]
fn should_run_heartbeat_before_cloud_recovery_can_block_startup() -> MidgeResult<()> {
    // Arrange
    let lease = std::sync::Arc::new(StartupWatchdogLease {
        validity: std::sync::Arc::new(crate::lease::LeaseValidity::new()),
        renewals: std::sync::atomic::AtomicUsize::new(0),
    });
    let lease_object: std::sync::Arc<dyn crate::lease::PrimaryLease> = lease.clone();

    // Act
    let startup_lease =
        StartupLease::acquire_for_test(lease_object, Some(std::sync::Arc::clone(&lease.validity)))?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while lease.renewals.load(std::sync::atomic::Ordering::Acquire) == 0
        && std::time::Instant::now() < deadline
    {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }

    // Assert
    assert!(startup_lease.lease_heartbeat.is_some());
    assert!(lease.renewals.load(std::sync::atomic::Ordering::Acquire) > 0);
    startup_lease.ensure_healthy("while recovery is blocked")?;
    Ok(())
}

#[test]
fn should_return_lease_held_given_active_writer_when_acquiring_startup_lease() {
    // Arrange
    let lease: std::sync::Arc<dyn crate::lease::PrimaryLease> =
        std::sync::Arc::new(AcquisitionFailureLease {
            error: || crate::lease::LeaseError::AcquisitionFailed("active writer".to_string()),
        });

    // Act
    let result = StartupLease::acquire_for_test(lease, None);

    // Assert
    assert!(
        matches!(result, Err(MidgeError::LeaseHeld(message)) if message.contains("active writer"))
    );
}

#[test]
fn should_return_lease_unavailable_given_backend_failure_when_acquiring_startup_lease() {
    // Arrange
    let lease: std::sync::Arc<dyn crate::lease::PrimaryLease> =
        std::sync::Arc::new(AcquisitionFailureLease {
            error: || crate::lease::LeaseError::IoError("backend unavailable".to_string()),
        });

    // Act
    let result = StartupLease::acquire_for_test(lease, None);

    // Assert
    assert!(
        matches!(result, Err(MidgeError::LeaseUnavailable(message)) if message.contains("backend unavailable"))
    );
}

#[test]
fn should_return_fenced_given_lease_loss_after_startup_acquisition() -> MidgeResult<()> {
    // Arrange
    let lease = std::sync::Arc::new(StartupWatchdogLease {
        validity: std::sync::Arc::new(crate::lease::LeaseValidity::new()),
        renewals: std::sync::atomic::AtomicUsize::new(0),
    });
    let lease_object: std::sync::Arc<dyn crate::lease::PrimaryLease> = lease.clone();
    let startup_lease =
        StartupLease::acquire_for_test(lease_object, Some(std::sync::Arc::clone(&lease.validity)))?;

    // Act
    startup_lease
        .lease_healthy
        .store(false, std::sync::atomic::Ordering::Release);
    let result = startup_lease.ensure_healthy("after acquisition");

    // Assert
    assert!(matches!(result, Err(MidgeError::Fenced(_))));
    Ok(())
}
