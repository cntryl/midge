use super::*;
use crate::common::MidgeError;
use crate::io::FsError;

#[test]
fn should_make_new_database_root_chain_durable_before_startup_writes() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir()?;
    let parent = std::fs::canonicalize(temp.path())?;
    let db_path = parent.join("nested/database");
    let storage_path = StartupStoragePath {
        db_path: db_path.clone(),
        memory_mode: false,
    };
    crate::io::durable_dir::take_synced_dirs();

    // Act
    storage_path.prepare()?;

    // Assert: the parent entry for each newly created directory is synced
    // before startup proceeds to lease, WAL, metadata, or SST writes.
    assert!(db_path.is_dir());
    assert_eq!(
        crate::io::durable_dir::take_synced_dirs(),
        [parent.clone(), parent.join("nested"), parent.join("nested")]
    );
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
        crate::config::CloudProviderConfig::s3_compatible(
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
    storage_path.prepare()?;
    let startup_lease = StartupLease::acquire(&opts, 0)?;

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

fn leader_record_epoch(db_path: &std::path::Path) -> u64 {
    let record =
        std::fs::read_to_string(db_path.join(".midge_leader")).expect("read leader record");
    record
        .lines()
        .find_map(|line| line.strip_prefix("epoch: "))
        .expect("leader record epoch")
        .parse()
        .expect("parse leader record epoch")
}

fn remove_if_present(path: &std::path::Path) {
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => panic!("remove {}: {error}", path.display()),
    }
}

#[test]
fn should_grant_epoch_above_wal_writer_epoch_when_leader_record_is_deleted() -> MidgeResult<()> {
    // Arrange
    let dir = tempfile::tempdir().expect("database directory");
    let options = crate::OpenOptions::local(dir.path()).build()?;
    let mut engine = crate::Engine::open(options.clone())?;
    engine.shutdown(std::time::Duration::from_secs(10))?;
    drop(engine);
    let wal_epoch = 7;
    {
        let fs: std::sync::Arc<dyn crate::io::Fs> = std::sync::Arc::new(
            crate::io::RealFs::new(dir.path().join("wal")).map_err(FsError::into_midge)?,
        );
        let writer = crate::wal::fs::FsWalWriterIo::new(&crate::wal::segment_file_name(1_000), fs)?;
        crate::wal::WalWriter::append_record(
            &writer,
            &crate::wal::WalRecord::new(
                crate::wal::WalOpKind::Put,
                bytes::Bytes::from_static(b"written-at-epoch-7"),
                Some(bytes::Bytes::from_static(b"value")),
                1_000,
                wal_epoch,
            ),
        )?;
        crate::wal::WalWriter::sync(&writer)?;
    }
    remove_if_present(&dir.path().join(".midge_leader"));

    // Act
    let mut reopened = crate::Engine::open(options)?;

    // Assert
    let granted_epoch = leader_record_epoch(dir.path());
    assert!(
        granted_epoch > wal_epoch,
        "granted epoch {granted_epoch} must exceed WAL writer epoch {wal_epoch}"
    );
    reopened.shutdown(std::time::Duration::from_secs(10))?;
    Ok(())
}

#[test]
fn should_grant_epoch_above_catalog_fencing_epoch_when_cloud_lease_object_is_deleted(
) -> MidgeResult<()> {
    // Arrange
    let dir = tempfile::tempdir().expect("database directory");
    let options = crate::OpenOptions::cloud_simulated(dir.path(), "bucket", "epoch-floor")
        .background_compaction(false)
        .build()?;
    let mut engine = crate::Engine::open(options.clone())?;
    engine.shutdown(std::time::Duration::from_secs(10))?;
    drop(engine);
    let catalog_epoch = 9;
    let catalog_path = dir
        .path()
        .join("cloud_store/wal/publication-catalog.v1.json");
    let mut catalog = crate::wal::cloud_catalog::WalPublicationCatalog::decode(
        &std::fs::read(&catalog_path).expect("read catalog"),
    )
    .expect("decode catalog");
    catalog.fencing_epoch = catalog_epoch;
    let encoded = catalog.encode().expect("encode catalog");
    std::fs::write(&catalog_path, &encoded).expect("write catalog");
    std::fs::write(
        dir.path()
            .join("cloud_store/wal/publication-catalog.v1.mirror.json"),
        &encoded,
    )
    .expect("write catalog mirror");
    remove_if_present(&dir.path().join("midge_primary_lease.json"));
    remove_if_present(&dir.path().join(".midge_leader"));

    // Act
    let mut reopened = crate::Engine::open(options)?;

    // Assert
    let granted_epoch = leader_record_epoch(dir.path());
    assert!(
        granted_epoch > catalog_epoch,
        "granted epoch {granted_epoch} must exceed catalog fencing epoch {catalog_epoch}"
    );
    reopened.shutdown(std::time::Duration::from_secs(10))?;
    Ok(())
}

struct ProviderMetadataBootstrapFixture {
    local_cache: tempfile::TempDir,
    cloud: std::sync::Arc<crate::storage::cloud::CloudStorage>,
    sst_cloud: std::sync::Arc<crate::storage::cloud::CloudStorage>,
    hybrid: std::sync::Arc<crate::storage::HybridStorage>,
    _events: crossbeam::channel::Receiver<crate::storage::StorageEvent>,
    lease: std::sync::Arc<crate::lease::CloudStorageLease>,
    lease_guard: crate::lease::LeaseGuard,
    leader_store: std::sync::Arc<dyn crate::lease::LeaderStore>,
    wal_catalog: crate::wal::cloud_catalog::WalPublicationCatalog,
}

impl ProviderMetadataBootstrapFixture {
    fn new(backend: std::sync::Arc<dyn crate::storage::cloud::CloudBackend>) -> MidgeResult<Self> {
        Self::new_with_sst_backend(std::sync::Arc::clone(&backend), backend)
    }

    fn new_with_sst_backend(
        backend: std::sync::Arc<dyn crate::storage::cloud::CloudBackend>,
        sst_backend: std::sync::Arc<dyn crate::storage::cloud::CloudBackend>,
    ) -> MidgeResult<Self> {
        use crate::lease::PrimaryLease as _;

        let local_cache = tempfile::tempdir()?;
        let cloud = std::sync::Arc::new(crate::storage::cloud::CloudStorage::new(
            backend,
            String::new(),
        ));
        let sst_cloud = std::sync::Arc::new(crate::storage::cloud::CloudStorage::new(
            sst_backend,
            String::new(),
        ));
        let lease = std::sync::Arc::new(crate::lease::CloudStorageLease::new_provider_backed(
            crate::lease::CloudLeaseConfig {
                bucket: "test".into(),
                prefix: String::new(),
            },
            local_cache.path().to_path_buf(),
            std::sync::Arc::clone(&cloud),
        ));
        let lease_guard = std::sync::Arc::clone(&lease).try_acquire()?;
        let leader_store = lease
            .get_leader_store()
            .expect("provider-backed lease has a leader store");
        let local_backend: std::sync::Arc<dyn crate::storage::StorageBackend> = std::sync::Arc::new(
            crate::storage::filesystem::FileSystem::new(local_cache.path().join("hybrid_local"))?,
        );
        let wal_backend: std::sync::Arc<dyn crate::storage::StorageBackend> = cloud.clone();
        let sst_backend: std::sync::Arc<dyn crate::storage::StorageBackend> = sst_cloud.clone();
        let control_backend: std::sync::Arc<dyn crate::storage::StorageBackend> = cloud.clone();
        let (event_tx, events) = crossbeam::channel::unbounded();
        let hybrid = std::sync::Arc::new(
            crate::storage::HybridStorage::new_with_class_stores_and_event_sender(
                local_backend,
                wal_backend,
                sst_backend,
                control_backend,
                event_tx,
                std::time::Duration::from_secs(5),
            ),
        );
        let authority = crate::runtime::ddl::DdlLeaseAuthority {
            store: std::sync::Arc::clone(&leader_store),
            holder_id: lease.holder_id(),
            writer_epoch: lease.epoch(),
        };
        crate::runtime::ddl::fence_remote_registry_on_startup(
            &hybrid,
            &authority,
            &crate::common::OperationDeadline::from_budget(std::time::Duration::from_secs(5)),
        )?;
        let fenced_catalog = crate::runtime::hybrid_persistence::CloudPersistence::new(
            std::sync::Arc::clone(&hybrid),
        )
        .fence_cloud_wal_catalog(lease.epoch())?;
        let wal_catalog = (*fenced_catalog).clone();

        Ok(Self {
            local_cache,
            cloud,
            sst_cloud,
            hybrid,
            _events: events,
            lease,
            lease_guard,
            leader_store,
            wal_catalog,
        })
    }

    fn bootstrap(&self) -> MidgeResult<()> {
        self.bootstrap_with_catalog(&self.wal_catalog)
    }

    fn bootstrap_with_catalog(
        &self,
        wal_catalog: &crate::wal::cloud_catalog::WalPublicationCatalog,
    ) -> MidgeResult<()> {
        use crate::lease::PrimaryLease as _;

        let authority = crate::runtime::ddl::DdlLeaseAuthority {
            store: std::sync::Arc::clone(&self.leader_store),
            holder_id: self.lease.holder_id(),
            writer_epoch: self.lease.epoch(),
        };
        RuntimeStorageMaterialization::bootstrap_provider_metadata_if_uncommitted(
            &self.cloud,
            &self.sst_cloud,
            &self.hybrid,
            &authority,
            wal_catalog,
            self.local_cache.path(),
            std::time::Duration::from_secs(5),
        )
    }

    fn acquire_successor(&mut self) -> MidgeResult<()> {
        use crate::lease::PrimaryLease as _;

        self.lease.release()?;
        let successor = std::sync::Arc::new(crate::lease::CloudStorageLease::new_provider_backed(
            crate::lease::CloudLeaseConfig {
                bucket: "test".into(),
                prefix: String::new(),
            },
            self.local_cache.path().to_path_buf(),
            std::sync::Arc::clone(&self.cloud),
        ));
        let successor_guard = std::sync::Arc::clone(&successor).try_acquire()?;
        let successor_store = successor
            .get_leader_store()
            .expect("successor provider lease has a leader store");
        let authority = crate::runtime::ddl::DdlLeaseAuthority {
            store: std::sync::Arc::clone(&successor_store),
            holder_id: successor.holder_id(),
            writer_epoch: successor.epoch(),
        };
        crate::runtime::ddl::fence_remote_registry_on_startup(
            &self.hybrid,
            &authority,
            &crate::common::OperationDeadline::from_budget(std::time::Duration::from_secs(5)),
        )?;
        let fenced_catalog = crate::runtime::hybrid_persistence::CloudPersistence::new(
            std::sync::Arc::clone(&self.hybrid),
        )
        .fence_cloud_wal_catalog(successor.epoch())?;
        self.wal_catalog = (*fenced_catalog).clone();
        self.leader_store = successor_store;
        drop(std::mem::replace(&mut self.lease_guard, successor_guard));
        self.lease = successor;
        Ok(())
    }

    fn committed_generation(&self) -> crate::lease::CloudMetadataGeneration {
        match self
            .leader_store
            .read_committed_metadata(std::time::Duration::from_secs(5))
            .expect("read cloud metadata authority")
        {
            crate::lease::CloudMetadataHead::Committed(generation) => generation,
            other => panic!("expected committed cloud metadata, got {other:?}"),
        }
    }

    fn assert_uncommitted(&self) {
        assert!(matches!(
            self.leader_store
                .read_committed_metadata(std::time::Duration::from_secs(5))
                .expect("read cloud metadata authority"),
            crate::lease::CloudMetadataHead::Uncommitted
        ));
    }
}

struct FailOnceGenerationPutBackend {
    inner: std::sync::Arc<crate::storage::cloud::MockCloudBackend>,
    fail_next_generation_put: std::sync::atomic::AtomicBool,
}

struct LoseMetadataCommitCallbackBackend {
    inner: std::sync::Arc<crate::storage::cloud::MockCloudBackend>,
    armed: std::sync::atomic::AtomicBool,
    fired: std::sync::atomic::AtomicBool,
}

impl crate::storage::cloud::CloudBackend for LoseMetadataCommitCallbackBackend {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_get, submit_get_with_metadata, submit_get_range, submit_get_range_with_identity, submit_delete, submit_list, submit_head);

    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        if key == crate::cloud_layout::CloudObjectLayout::LEASE_OBJECT_KEY
            && self.armed.swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            let (inner_callback, completion) = std::sync::mpsc::channel();
            self.inner.submit_put(key, data, headers, inner_callback);
            match completion
                .recv()
                .expect("mock provider completes lease CAS")
            {
                crate::storage::cloud::CloudEvent::Put {
                    result: crate::storage::cloud::CloudOutcome::Ok(()),
                    ..
                } => {
                    self.fired.store(true, std::sync::atomic::Ordering::Release);
                    let _ = callback.send(crate::storage::cloud::CloudEvent::Put {
                        key: key.to_string(),
                        result: crate::storage::cloud::CloudOutcome::Err(
                            crate::storage::cloud::CloudError::Transport(
                                "injected lost metadata commit callback".into(),
                            ),
                        ),
                    });
                }
                other => {
                    let _ = callback.send(other);
                }
            }
            return;
        }
        self.inner.submit_put(key, data, headers, callback);
    }
}

impl crate::storage::cloud::CloudBackend for FailOnceGenerationPutBackend {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_get, submit_get_with_metadata, submit_get_range, submit_get_range_with_identity, submit_delete, submit_list, submit_head);

    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        if key.starts_with("metadata/generations/")
            && self
                .fail_next_generation_put
                .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            let _ = callback.send(crate::storage::cloud::CloudEvent::Put {
                key: key.to_string(),
                result: crate::storage::cloud::CloudOutcome::Err(
                    crate::storage::cloud::CloudError::Transport(
                        "injected first generation upload failure".into(),
                    ),
                ),
            });
            return;
        }
        self.inner.submit_put(key, data, headers, callback);
    }
}

#[test]
fn should_bootstrap_fresh_provider_metadata_before_recovery_uses_local_cache() -> MidgeResult<()> {
    // Arrange: a provider lease, WAL catalog, and DDL fence exist, but there
    // are no committed metadata or local cache files.
    let fixture = ProviderMetadataBootstrapFixture::new(std::sync::Arc::new(
        crate::storage::cloud::MockCloudBackend::new(),
    ))?;
    fixture.assert_uncommitted();

    // Act
    fixture.bootstrap()?;

    // Assert: the lease points at a complete generation and a new cache can
    // recover it without trusting the original local directory.
    let generation = fixture.committed_generation();
    for name in [
        crate::metadata::files::FORMAT,
        crate::metadata::files::MANIFEST_SNAPSHOT,
    ] {
        assert!(generation
            .objects
            .iter()
            .any(|object| object.file_name == name));
        assert!(fixture.local_cache.path().join(name).is_file());
    }
    let fresh_cache = tempfile::tempdir()?;
    crate::runtime::cloud_startup::CloudStartupRecovery::hydrate_cloud_metadata(
        &fixture.cloud,
        fixture.leader_store.as_ref(),
        fresh_cache.path(),
        crate::config::RecoveryPolicy::Strict,
    )?;
    let restored: crate::metadata::Manifest = serde_json::from_slice(&std::fs::read(
        fresh_cache
            .path()
            .join(crate::metadata::files::MANIFEST_SNAPSHOT),
    )?)
    .expect("decode committed manifest snapshot");
    assert_eq!(restored.last_persisted_sequence, 0);
    assert!(restored.files.is_empty());
    assert!(restored.column_families.is_empty());
    Ok(())
}

#[test]
fn should_retry_provider_metadata_bootstrap_with_same_cache_after_failed_generation_upload(
) -> MidgeResult<()> {
    use crate::lease::PrimaryLease as _;

    // Arrange: the first immutable generation upload fails before lease CAS.
    let backend = std::sync::Arc::new(FailOnceGenerationPutBackend {
        inner: std::sync::Arc::new(crate::storage::cloud::MockCloudBackend::new()),
        fail_next_generation_put: std::sync::atomic::AtomicBool::new(true),
    });
    let mut fixture = ProviderMetadataBootstrapFixture::new(backend.clone())?;

    // Act
    let first = fixture.bootstrap();

    // Assert: no local metadata or lease pointer was published, and retrying
    // the same cache now succeeds without manual cleanup.
    assert!(
        first.is_err(),
        "injected upload failure must abort bootstrap"
    );
    assert!(
        !backend
            .fail_next_generation_put
            .load(std::sync::atomic::Ordering::Acquire),
        "the injected failure must have reached a generation upload"
    );
    fixture.assert_uncommitted();
    assert!(!fixture
        .local_cache
        .path()
        .join(crate::metadata::files::FORMAT)
        .exists());
    assert!(!fixture
        .local_cache
        .path()
        .join(crate::metadata::files::MANIFEST_SNAPSHOT)
        .exists());
    let first_epoch = fixture.lease.epoch();
    fixture.acquire_successor()?;
    assert!(fixture.lease.epoch() > first_epoch);
    fixture.bootstrap()?;
    fixture.committed_generation();
    Ok(())
}

#[test]
fn should_hydrate_committed_provider_metadata_when_lease_cas_callback_is_lost() -> MidgeResult<()> {
    // Arrange: the provider applies the metadata-pointer CAS but reports a
    // transport error. Readback must resolve the ambiguous publication.
    let backend = std::sync::Arc::new(LoseMetadataCommitCallbackBackend {
        inner: std::sync::Arc::new(crate::storage::cloud::MockCloudBackend::new()),
        armed: std::sync::atomic::AtomicBool::new(false),
        fired: std::sync::atomic::AtomicBool::new(false),
    });
    let fixture = ProviderMetadataBootstrapFixture::new(backend.clone())?;
    backend
        .armed
        .store(true, std::sync::atomic::Ordering::Release);

    // Act
    fixture.bootstrap()?;

    // Assert
    assert!(backend.fired.load(std::sync::atomic::Ordering::Acquire));
    fixture.committed_generation();
    assert!(fixture
        .local_cache
        .path()
        .join(crate::metadata::files::MANIFEST_SNAPSHOT)
        .is_file());
    Ok(())
}

#[test]
fn should_reject_stale_local_cache_when_provider_metadata_is_uncommitted() -> MidgeResult<()> {
    // Arrange: local metadata may belong to another database, so the lease's
    // uncommitted pointer cannot authorize replacing it with a default state.
    let fixture = ProviderMetadataBootstrapFixture::new(std::sync::Arc::new(
        crate::storage::cloud::MockCloudBackend::new(),
    ))?;
    let format_path = fixture
        .local_cache
        .path()
        .join(crate::metadata::files::FORMAT);
    std::fs::write(&format_path, b"stale-local-format")?;

    // Act
    let result = fixture.bootstrap();

    // Assert
    assert!(matches!(result, Err(MidgeError::RecoveryFailed(_))));
    fixture.assert_uncommitted();
    assert_eq!(std::fs::read(format_path)?, b"stale-local-format");
    Ok(())
}

#[test]
fn should_reject_local_wal_residue_when_provider_metadata_is_uncommitted() -> MidgeResult<()> {
    // Arrange: the provider catalog is empty, but a reused cache still has
    // WAL bytes that an empty manifest must not silently replace.
    let fixture = ProviderMetadataBootstrapFixture::new(std::sync::Arc::new(
        crate::storage::cloud::MockCloudBackend::new(),
    ))?;
    let wal_dir = fixture.local_cache.path().join("wal");
    std::fs::create_dir(&wal_dir)?;
    let wal_path = wal_dir.join("active.wal");
    std::fs::write(&wal_path, b"retained WAL bytes")?;

    // Act
    let result = fixture.bootstrap();

    // Assert
    assert!(matches!(result, Err(MidgeError::RecoveryFailed(_))));
    fixture.assert_uncommitted();
    assert_eq!(std::fs::read(wal_path)?, b"retained WAL bytes");
    Ok(())
}

#[test]
fn should_reject_provider_bootstrap_when_cloud_wal_catalog_has_a_sequence_floor() -> MidgeResult<()>
{
    use crate::lease::PrimaryLease as _;

    // Arrange: a catalog floor proves prior remote WAL history even without
    // any currently published segment.
    let fixture = ProviderMetadataBootstrapFixture::new(std::sync::Arc::new(
        crate::storage::cloud::MockCloudBackend::new(),
    ))?;
    let persistence = crate::runtime::hybrid_persistence::CloudPersistence::new(
        std::sync::Arc::clone(&fixture.hybrid),
    );
    persistence.raise_wal_sequence_floor(fixture.lease.epoch(), 7)?;
    let catalog = persistence.fence_cloud_wal_catalog(fixture.lease.epoch())?;
    assert_eq!(catalog.sequence_floor, 7);

    // Act
    let result = fixture.bootstrap_with_catalog(&catalog);

    // Assert
    assert!(matches!(result, Err(MidgeError::RecoveryFailed(_))));
    fixture.assert_uncommitted();
    assert!(!fixture
        .local_cache
        .path()
        .join(crate::metadata::files::MANIFEST_SNAPSHOT)
        .exists());
    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_keep_floor_and_wal_recoverable_when_set_aside_crashes_after_floor_persist(
) -> MidgeResult<()> {
    use crate::lease::PrimaryLease as _;

    // Arrange: salvage planned to set aside an active WAL holding sequences
    // up to 5, which the catalog floor does not yet cover.
    let _guard = crate::failpoints::test_failpoint_guard();
    let fixture = ProviderMetadataBootstrapFixture::new(std::sync::Arc::new(
        crate::storage::cloud::MockCloudBackend::new(),
    ))?;
    let wal_dir = fixture.local_cache.path().join("wal");
    std::fs::create_dir_all(&wal_dir)?;
    let active = wal_dir.join(crate::wal::ACTIVE_FILE_NAME);
    std::fs::write(&active, b"stale active WAL bytes")?;
    let plan = crate::runtime::cloud_startup::CloudWalRecoveryPlan {
        remote_segments: std::collections::BTreeMap::new(),
        local_segments: std::collections::BTreeMap::new(),
        active_wal: None,
        opened_in_salvage_mode: true,
        unreplayed_segments: Vec::new(),
        max_unreplayed_sequence: 5,
        set_aside_local_paths: vec![active.clone()],
    };
    let persistence = crate::runtime::hybrid_persistence::CloudPersistence::new(
        std::sync::Arc::clone(&fixture.hybrid),
    );
    let epoch = fixture.lease.epoch();
    let scenario = fail::FailScenario::setup();
    fail::cfg("midge::cloud::after_wal_salvage_floor_persist", "return")
        .expect("configure crash boundary");

    // Act: crash after the floor is persisted and before the rename.
    let crashed = plan.commit_set_aside(
        &persistence,
        epoch,
        &fixture.wal_catalog,
        fixture.local_cache.path(),
    );
    fail::remove("midge::cloud::after_wal_salvage_floor_persist");
    scenario.teardown();

    // Assert: the floor is durable, the file is untouched, and a restart
    // that finishes the set-aside keeps the floor and retains the bytes.
    assert!(crashed.is_err());
    assert!(active.exists());
    let reopened = persistence.fence_cloud_wal_catalog(epoch)?;
    assert_eq!(reopened.sequence_floor, 5);
    plan.commit_set_aside(&persistence, epoch, &reopened, fixture.local_cache.path())?;
    assert!(!active.exists());
    assert_eq!(
        std::fs::read(wal_dir.join("wal.log.salvage-retained"))?,
        b"stale active WAL bytes"
    );
    assert_eq!(
        persistence.fence_cloud_wal_catalog(epoch)?.sequence_floor,
        5
    );
    Ok(())
}

#[test]
fn should_reject_provider_bootstrap_when_split_sst_store_has_remote_objects() -> MidgeResult<()> {
    // Arrange: the control store has no metadata, but a separate SST class
    // store still contains bytes that could belong to an older database.
    let fixture = ProviderMetadataBootstrapFixture::new_with_sst_backend(
        std::sync::Arc::new(crate::storage::cloud::MockCloudBackend::new()),
        std::sync::Arc::new(crate::storage::cloud::MockCloudBackend::new()),
    )?;
    let sst_key = crate::cloud_layout::object_key(&crate::cloud_layout::file_name(0, 0, 1));
    crate::runtime::cloud_startup::cloud_io::BlockingCloudIo::new(&fixture.sst_cloud)
        .put(&sst_key, b"retained remote SST bytes".to_vec())?;

    // Act
    let result = fixture.bootstrap();

    // Assert
    assert!(matches!(result, Err(MidgeError::RecoveryFailed(_))));
    fixture.assert_uncommitted();
    assert!(!fixture
        .local_cache
        .path()
        .join(crate::metadata::files::MANIFEST_SNAPSHOT)
        .exists());
    assert!(
        crate::runtime::cloud_startup::cloud_io::BlockingCloudIo::new(&fixture.sst_cloud)
            .head_optional(&sst_key)?
            .is_some()
    );
    Ok(())
}

#[test]
fn should_reject_provider_bootstrap_when_current_epoch_ddl_registry_has_history() -> MidgeResult<()>
{
    use crate::lease::PrimaryLease as _;

    // Arrange: the remote DDL registry has an operation but the test cache is
    // still empty. Its current writer epoch alone does not make it fresh.
    let fixture = ProviderMetadataBootstrapFixture::new(std::sync::Arc::new(
        crate::storage::cloud::MockCloudBackend::new(),
    ))?;
    let state_dir = tempfile::tempdir()?;
    let mut state = crate::runtime::RuntimeState::try_new(
        state_dir.path().to_path_buf(),
        false,
        crate::config::RecoveryPolicy::Strict,
    )?;
    let edit = crate::runtime::ddl::create_edit(&state, "existing-cf")?;
    let authority = crate::runtime::ddl::DdlLeaseAuthority {
        store: std::sync::Arc::clone(&fixture.leader_store),
        holder_id: fixture.lease.holder_id(),
        writer_epoch: fixture.lease.epoch(),
    };
    crate::runtime::ddl::execute_within(
        &mut state,
        Some(&fixture.hybrid),
        &edit,
        Some(&authority),
        &crate::common::OperationDeadline::from_budget(std::time::Duration::from_secs(5)),
    )?;

    // Act
    let result = fixture.bootstrap();

    // Assert
    assert!(matches!(result, Err(MidgeError::RecoveryFailed(_))));
    fixture.assert_uncommitted();
    assert!(!fixture
        .local_cache
        .path()
        .join(crate::metadata::files::MANIFEST_SNAPSHOT)
        .exists());
    Ok(())
}
