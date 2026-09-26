use super::*;
use crate::common::MidgeError;
use crate::io::FsError;

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
    storage_path.prepare();
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
