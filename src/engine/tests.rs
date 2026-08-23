use super::*;
use crate::lease::PrimaryLease;

#[derive(Default)]
struct BlockingReleaseState {
    started: bool,
    allowed: bool,
    completed: bool,
}

#[derive(Default)]
struct BlockingReleaseLease {
    state: std::sync::Mutex<BlockingReleaseState>,
    changed: std::sync::Condvar,
}

impl BlockingReleaseLease {
    fn wait_until_release_started(&self, timeout: Duration) -> bool {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (state, _) = self
            .changed
            .wait_timeout_while(state, timeout, |state| !state.started)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.started
    }

    fn allow_release(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.allowed = true;
        self.changed.notify_all();
    }

    fn wait_until_release_completed(&self, timeout: Duration) -> bool {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (state, _) = self
            .changed
            .wait_timeout_while(state, timeout, |state| !state.completed)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.completed
    }

    fn release_completed(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .completed
    }
}

impl crate::lease::PrimaryLease for BlockingReleaseLease {
    fn try_acquire(self: Arc<Self>) -> Result<crate::lease::LeaseGuard, crate::lease::LeaseError> {
        Ok(crate::lease::LeaseGuard::token())
    }

    fn renew(&self) -> Result<(), crate::lease::LeaseError> {
        Ok(())
    }

    fn release(&self) -> Result<(), crate::lease::LeaseError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.started = true;
        self.changed.notify_all();
        state = self
            .changed
            .wait_while(state, |state| !state.allowed)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.completed = true;
        self.changed.notify_all();
        Ok(())
    }

    fn ttl(&self) -> Duration {
        Duration::from_secs(30)
    }

    fn holder_id(&self) -> String {
        "blocking-release-test".to_string()
    }

    fn epoch(&self) -> u64 {
        1
    }
}

#[derive(Default, PartialEq, Eq)]
enum RenewalBlockState {
    #[default]
    Waiting,
    Blocked,
    Allowed,
}

#[derive(Default, PartialEq, Eq)]
enum ReleaseState {
    #[default]
    Waiting,
    Completed,
}

#[derive(Default)]
struct BlockingRenewalState {
    renewal: RenewalBlockState,
    release: ReleaseState,
}

struct BlockingRenewalLease {
    state: std::sync::Mutex<BlockingRenewalState>,
    changed: std::sync::Condvar,
    validity: Arc<crate::lease::LeaseValidity>,
    ttl: Duration,
}

impl BlockingRenewalLease {
    fn new(ttl: Duration) -> Self {
        Self {
            state: std::sync::Mutex::new(BlockingRenewalState::default()),
            changed: std::sync::Condvar::new(),
            validity: Arc::new(crate::lease::LeaseValidity::new()),
            ttl,
        }
    }

    fn wait_until_renewal_started(&self, timeout: Duration) -> bool {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (state, _) = self
            .changed
            .wait_timeout_while(state, timeout, |state| {
                state.renewal == RenewalBlockState::Waiting
            })
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.renewal != RenewalBlockState::Waiting
    }

    fn allow_renewal(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.renewal = RenewalBlockState::Allowed;
        self.changed.notify_all();
    }

    fn release_started(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .release
            != ReleaseState::Waiting
    }

    fn wait_until_release_completed(&self, timeout: Duration) -> bool {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (state, _) = self
            .changed
            .wait_timeout_while(state, timeout, |state| {
                state.release == ReleaseState::Waiting
            })
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.release == ReleaseState::Completed
    }
}

impl crate::lease::PrimaryLease for BlockingRenewalLease {
    fn try_acquire(self: Arc<Self>) -> Result<crate::lease::LeaseGuard, crate::lease::LeaseError> {
        self.validity
            .activate(1, std::time::Instant::now() + self.ttl)?;
        Ok(crate::lease::LeaseGuard::token())
    }

    fn renew(&self) -> Result<(), crate::lease::LeaseError> {
        let candidate_until = std::time::Instant::now() + self.ttl;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.renewal = RenewalBlockState::Blocked;
        self.changed.notify_all();
        state = self
            .changed
            .wait_while(state, |state| state.renewal != RenewalBlockState::Allowed)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        drop(state);
        self.validity.advance(1, candidate_until)
    }

    fn release(&self) -> Result<(), crate::lease::LeaseError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.validity.deactivate(1);
        state.release = ReleaseState::Completed;
        self.changed.notify_all();
        Ok(())
    }

    fn ttl(&self) -> Duration {
        self.ttl
    }

    fn holder_id(&self) -> String {
        "blocking-renewal-test".to_string()
    }

    fn epoch(&self) -> u64 {
        1
    }
}

fn install_blocking_renewal_lease(
    engine: &mut Engine,
    lease: &Arc<BlockingRenewalLease>,
) -> MidgeResult<()> {
    let heartbeat_mutex = engine
        .lease_state
        .heartbeat
        .take()
        .ok_or_else(|| MidgeError::Internal("engine heartbeat missing".to_string()))?;
    let mut heartbeat = heartbeat_mutex
        .into_inner()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let healthy = heartbeat.healthy_flag();
    heartbeat.stop();
    if let Some(original_lease) = engine.lease_state.lease.take() {
        original_lease.release()?;
    }
    engine.lease_state.guard.take();

    let lease_guard = Arc::clone(lease).try_acquire()?;
    let engine_lease: Arc<dyn crate::lease::PrimaryLease> = lease.clone();
    let mut heartbeat = crate::lease::LeaseHeartbeat::new_with_healthy_and_validity(
        Arc::clone(&engine_lease),
        healthy,
        Some(Arc::clone(&lease.validity)),
    );
    heartbeat.start();
    engine.lease_state.lease = Some(engine_lease);
    engine.lease_state.guard = Some(lease_guard);
    engine.lease_state.heartbeat = Some(std::sync::Mutex::new(heartbeat));
    Ok(())
}

#[test]
fn should_bound_shutdown_when_primary_lease_release_blocks() -> MidgeResult<()> {
    // Arrange
    let mut engine = Engine::open(OpenOptions::in_memory().build()?)?;
    let lease_heartbeat = engine.lease_state.heartbeat.take();
    let lease = engine.lease_state.lease.take();
    let lease_guard = engine.lease_state.guard.take();
    Engine::release_fencing_parts(lease_heartbeat, lease, lease_guard);

    let blocking_lease = Arc::new(BlockingReleaseLease::default());
    let lease_guard = Arc::clone(&blocking_lease).try_acquire()?;
    let engine_lease: Arc<dyn crate::lease::PrimaryLease> = blocking_lease.clone();
    engine.lease_state.lease = Some(engine_lease);
    engine.lease_state.guard = Some(lease_guard);
    let (shutdown_tx, shutdown_rx) = std::sync::mpsc::sync_channel(1);

    // Act
    let shutdown_thread = std::thread::Builder::new()
        .name("midge-blocking-release-shutdown-test".to_string())
        .spawn(move || {
            let started = std::time::Instant::now();
            let result = engine.shutdown(Duration::from_millis(25));
            let elapsed = started.elapsed();
            let _ = shutdown_tx.send((engine, result, elapsed));
        })
        .map_err(MidgeError::Io)?;
    let release_started = blocking_lease.wait_until_release_started(Duration::from_secs(2));
    let first_response = shutdown_rx.recv_timeout(Duration::from_millis(250));
    let returned_before_release = first_response.is_ok();
    let release_completed_before_unblock = blocking_lease.release_completed();
    blocking_lease.allow_release();
    let (mut engine, first_shutdown, shutdown_elapsed) = match first_response {
        Ok(response) => response,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => shutdown_rx
            .recv_timeout(Duration::from_secs(2))
            .map_err(|error| {
                MidgeError::Internal(format!(
                    "shutdown did not return after releasing the test lease: {error}"
                ))
            })?,
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            return Err(MidgeError::Internal(
                "shutdown test thread disconnected before returning the engine".to_string(),
            ));
        }
    };
    shutdown_thread
        .join()
        .map_err(|_| MidgeError::Internal("shutdown test thread panicked".to_string()))?;
    let release_completed = blocking_lease.wait_until_release_completed(Duration::from_secs(2));
    let retry_shutdown = engine.shutdown(Duration::from_secs(2));

    // Assert
    assert!(release_started, "shutdown never attempted lease release");
    assert!(
        returned_before_release,
        "shutdown exceeded its caller deadline while primary lease release was blocked"
    );
    assert!(
        matches!(first_shutdown, Err(MidgeError::Timeout(_))),
        "blocked fencing cleanup must return Timeout, got {first_shutdown:?}"
    );
    assert!(
        shutdown_elapsed < Duration::from_millis(250),
        "shutdown returned after its bounded deadline: {shutdown_elapsed:?}"
    );
    assert!(
        !release_completed_before_unblock,
        "shutdown released fencing resources before detached cleanup completed"
    );
    assert!(
        release_completed,
        "detached fencing cleanup did not finish after lease release resumed"
    );
    retry_shutdown?;
    Ok(())
}

#[test]
fn should_reject_writes_when_renewal_blocks_past_expiry() -> MidgeResult<()> {
    // Arrange
    let mut engine = Engine::open(OpenOptions::in_memory().build()?)?;
    let default_cf = engine
        .get_column_family("default")
        .ok_or_else(|| MidgeError::Internal("default column family missing".to_string()))?;
    let lease = Arc::new(BlockingRenewalLease::new(Duration::from_secs(2)));
    install_blocking_renewal_lease(&mut engine, &lease)?;

    // Act
    assert!(lease.wait_until_renewal_started(Duration::from_secs(5)));
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while engine.is_primary_lease_healthy() && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    let mut transaction = engine.begin_tx(default_cf.id(), TransactionMode::ReadWrite)?;
    transaction.put(b"fenced-key".to_vec(), b"value".to_vec(), None)?;
    let write = transaction.commit(WriteOptions::sync());

    // Assert
    assert!(!engine.is_primary_lease_healthy());
    assert!(matches!(write, Err(MidgeError::Fenced(_))));
    lease.allow_renewal();
    engine.shutdown(Duration::from_secs(2))?;
    Ok(())
}

#[test]
fn should_retain_fencing_resources_when_shutdown_times_out_on_blocked_renewal() -> MidgeResult<()> {
    // Arrange
    let mut engine = Engine::open(OpenOptions::in_memory().build()?)?;
    let lease = Arc::new(BlockingRenewalLease::new(Duration::from_secs(2)));
    install_blocking_renewal_lease(&mut engine, &lease)?;
    assert!(lease.wait_until_renewal_started(Duration::from_secs(5)));

    // Act
    let first_shutdown = engine.shutdown(Duration::from_millis(25));
    let released_before_unblock = lease.release_started();
    lease.allow_renewal();
    let cleanup_completed = lease.wait_until_release_completed(Duration::from_secs(2));
    let retry_shutdown = engine.shutdown(Duration::from_secs(2));

    // Assert
    assert!(matches!(first_shutdown, Err(MidgeError::Timeout(_))));
    assert!(!released_before_unblock);
    assert!(cleanup_completed);
    retry_shutdown?;
    Ok(())
}

#[test]
fn should_allow_shutdown_retry_when_live_transaction_is_released() -> MidgeResult<()> {
    // Arrange
    let mut engine = Engine::open(OpenOptions::in_memory().build()?)?;
    let default_cf = engine
        .get_column_family("default")
        .ok_or_else(|| MidgeError::Internal("default column family missing".to_string()))?;
    let transaction = engine.begin_tx(default_cf.id(), TransactionMode::ReadWrite)?;

    // Act
    let started = std::time::Instant::now();
    let first_shutdown = engine.shutdown(Duration::from_millis(25));

    // Assert
    assert!(matches!(first_shutdown, Err(MidgeError::Busy(_))));
    assert!(
        started.elapsed() < Duration::from_millis(250),
        "shutdown blocked on a transaction owned by the calling thread"
    );
    assert!(matches!(
        engine.begin_tx(default_cf.id(), TransactionMode::ReadOnly),
        Err(MidgeError::Busy(_))
    ));

    drop(transaction);
    engine.shutdown(Duration::from_secs(2))?;
    assert!(matches!(
        engine.list_column_families(),
        Err(MidgeError::Busy(_))
    ));
    Ok(())
}

#[test]
fn should_reap_engine_without_blocking_drop_when_transaction_is_live() -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir().map_err(MidgeError::Io)?;
    let engine = Engine::open(OpenOptions::local(temp_dir.path()).build()?)?;
    let default_cf = engine
        .get_column_family("default")
        .ok_or_else(|| MidgeError::Internal("default column family missing".to_string()))?;
    let transaction = engine.begin_tx(default_cf.id(), TransactionMode::ReadOnly)?;

    // Act
    let started = std::time::Instant::now();
    drop(engine);
    let drop_elapsed = started.elapsed();

    // Assert
    assert!(
        drop_elapsed < Duration::from_millis(250),
        "Engine::drop waited for its live transaction: {drop_elapsed:?}"
    );
    assert!(
        Engine::open(OpenOptions::local(temp_dir.path()).build()?).is_err(),
        "reaper released the primary lease while the runtime transaction was live"
    );

    drop(transaction);
    let reopen_deadline = std::time::Instant::now() + Duration::from_secs(2);
    let mut reopened = loop {
        match Engine::open(OpenOptions::local(temp_dir.path()).build()?) {
            Ok(engine) => break engine,
            Err(error) if std::time::Instant::now() < reopen_deadline => {
                tracing::trace!(%error, "waiting for engine reaper to release lease");
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(error) => return Err(error),
        }
    };
    reopened.shutdown(Duration::from_secs(2))?;
    Ok(())
}

#[test]
fn should_treat_flush_compact_as_noop_in_memory_mode() {
    // Arrange
    let opts = OpenOptions::in_memory().build().expect("build options");

    // Act
    let engine = Engine::open(opts).expect("open memory engine");
    let cf = engine
        .create_column_family("test")
        .expect("create column family");

    // Assert
    engine.flush_cf(&cf).expect("memory flush should succeed");
    engine
        .compact_all()
        .expect("memory compact_all should succeed");
}

fn cloud_wal_test_bytes(sequence: u64, writer_epoch: u64, key: &'static [u8]) -> Vec<u8> {
    let record = crate::wal::WalRecord::new(
        crate::wal::WalOpKind::Put,
        bytes::Bytes::from_static(key),
        Some(bytes::Bytes::from_static(b"value")),
        sequence,
        writer_epoch,
    );
    let payload = crate::wal::encoding::encode(&record).expect("encode WAL record");
    let mut bytes = Vec::new();
    crate::wal::frame::append_frame(&mut bytes, &payload).expect("frame WAL record");
    bytes
}

fn cloud_wal_test_catalog(
    fencing_epoch: u64,
    publications: &[(u64, u64, u64, Vec<u8>)],
) -> crate::wal::cloud_catalog::WalPublicationCatalog {
    let mut catalog =
        crate::wal::cloud_catalog::WalPublicationCatalog::empty(fencing_epoch).unwrap();
    for (segment_id, max_sequence, writer_epoch, bytes) in publications {
        let publication = crate::wal::cloud_catalog::PublishedWalSegment::from_validated_bytes(
            *segment_id,
            *max_sequence,
            *writer_epoch,
            bytes,
        );
        catalog.publish(fencing_epoch, publication).unwrap();
    }
    catalog
}

fn write_simulated_cloud_wal_publication(
    cloud_wal_dir: &std::path::Path,
    publication: &crate::wal::cloud_catalog::PublishedWalSegment,
    bytes: &[u8],
) {
    let relative = publication.object_key.strip_prefix("wal/").unwrap();
    let path = cloud_wal_dir.join(relative);
    std::fs::create_dir_all(path.parent().unwrap()).expect("create epoch WAL directory");
    std::fs::write(path, bytes).expect("write epoch-scoped WAL object");
}

#[test]
fn should_ignore_orphan_when_recovering_authoritative_catalog_wal() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let backend = Arc::new(crate::storage::cloud::MockCloudBackend::new());
    let cloud = crate::storage::cloud::CloudStorage::new(backend, "midge".to_string());
    let authoritative_bytes = cloud_wal_test_bytes(1, 7, b"authoritative");
    let orphan_bytes = cloud_wal_test_bytes(2, 6, b"late-stale-writer");
    let catalog = cloud_wal_test_catalog(8, &[(1, 1, 7, authoritative_bytes.clone())]);
    Engine::blocking_cloud_put(
        &cloud,
        &crate::wal::cloud_segment_object_key(1, 7),
        authoritative_bytes.clone(),
    )
    .expect("upload authoritative WAL object");
    Engine::blocking_cloud_put(
        &cloud,
        &crate::wal::cloud_segment_object_key(2, 6),
        orphan_bytes,
    )
    .expect("upload late stale-writer orphan");

    let staged_dir = Engine::materialize_cloud_wal_recovery_dir(
        &cloud,
        temp_dir.path(),
        RecoveryPolicy::Strict,
        &catalog,
    )
    .expect("materialize authoritative cloud WAL recovery dir");

    let mut staged_files: Vec<String> = std::fs::read_dir(&staged_dir)
        .expect("read staged wal dir")
        .map(|entry| {
            entry
                .expect("dir entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    staged_files.sort();

    // Act
    // Assert
    assert_eq!(staged_files, vec![crate::wal::cloud_segment_file_name(1)]);
    assert_eq!(
        std::fs::read(staged_dir.join(crate::wal::cloud_segment_file_name(1)))
            .expect("read staged wal 1"),
        authoritative_bytes
    );
}

#[test]
fn should_reject_legacy_segment_only_cloud_wal_without_catalog() {
    // Arrange
    let backend = Arc::new(crate::storage::cloud::MockCloudBackend::new());
    let cloud = crate::storage::cloud::CloudStorage::new(backend, "midge".to_string());
    Engine::blocking_cloud_put(&cloud, "wal/1.wal", cloud_wal_test_bytes(1, 1, b"legacy"))
        .expect("upload legacy WAL alias");

    // Act
    let result = startup::CloudStartupRecovery::reject_cloud_wal_without_catalog(&cloud);

    // Assert
    assert!(matches!(
        result,
        Err(crate::common::MidgeError::RecoveryFailed(message))
            if message.contains("publication catalog format v1")
    ));
}

#[test]
fn should_reject_legacy_segment_only_simulated_cloud_wal_without_catalog() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let cloud_wal_dir = temp_dir.path().join("cloud_store").join("wal");
    std::fs::create_dir_all(&cloud_wal_dir).expect("create simulated cloud WAL directory");
    std::fs::write(cloud_wal_dir.join("wal_000001.log"), b"legacy")
        .expect("write legacy WAL alias");

    // Act
    let result =
        startup::CloudStartupRecovery::reject_simulated_cloud_wal_without_catalog(&cloud_wal_dir);

    // Assert
    assert!(matches!(
        result,
        Err(crate::common::MidgeError::RecoveryFailed(message))
            if message.contains("publication catalog format v1")
    ));
}

#[test]
fn should_reject_epoch_scoped_simulated_cloud_wal_without_catalog() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let cloud_wal_dir = temp_dir.path().join("cloud_store").join("wal");
    let orphan_path = cloud_wal_dir
        .join("epochs")
        .join("00000000000000000007")
        .join(crate::wal::cloud_segment_file_name(1));
    std::fs::create_dir_all(orphan_path.parent().expect("epoch WAL parent"))
        .expect("create epoch WAL directory");
    std::fs::write(orphan_path, cloud_wal_test_bytes(1, 7, b"untracked"))
        .expect("write untracked epoch WAL object");

    // Act
    let result =
        startup::CloudStartupRecovery::reject_simulated_cloud_wal_without_catalog(&cloud_wal_dir);

    // Assert
    assert!(matches!(
        result,
        Err(crate::common::MidgeError::RecoveryFailed(message))
            if message.contains("object presence is not authority")
    ));
}

#[test]
fn should_fail_strict_recovery_given_conflicting_duplicate_local_wal_aliases() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let cloud_wal_dir = temp_dir.path().join("cloud_store").join("wal");
    let local_wal_dir = temp_dir.path().join("wal");
    std::fs::create_dir_all(&cloud_wal_dir).expect("create simulated cloud WAL directory");
    std::fs::create_dir_all(&local_wal_dir).expect("create local WAL directory");
    let wal_bytes = |key: &'static [u8]| {
        let record = crate::wal::WalRecord::new(
            crate::wal::WalOpKind::Put,
            bytes::Bytes::from_static(key),
            Some(bytes::Bytes::from_static(b"value")),
            1,
            0,
        );
        let payload = crate::wal::encoding::encode(&record).expect("encode WAL record");
        let mut bytes = Vec::new();
        crate::wal::frame::append_frame(&mut bytes, &payload).expect("frame WAL record");
        bytes
    };
    std::fs::write(local_wal_dir.join("1.wal"), wal_bytes(b"legacy"))
        .expect("write legacy local WAL alias");
    std::fs::write(
        local_wal_dir.join(crate::wal::segment_file_name(1)),
        wal_bytes(b"canonical"),
    )
    .expect("write canonical local WAL file");
    let catalog = cloud_wal_test_catalog(1, &[]);

    // Act
    let result = startup::CloudStartupRecovery::materialize_simulated_cloud_wal_recovery_dir(
        &cloud_wal_dir,
        temp_dir.path(),
        RecoveryPolicy::Strict,
        &catalog,
    );

    // Assert
    assert!(matches!(
        result,
        Err(crate::common::MidgeError::RecoveryFailed(message))
            if message.contains("conflicting duplicate local WAL")
    ));
}

#[test]
fn should_remove_matching_legacy_local_wal_alias_given_canonical_recovery() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let cloud_wal_dir = temp_dir.path().join("cloud_store").join("wal");
    let local_wal_dir = temp_dir.path().join("wal");
    std::fs::create_dir_all(&cloud_wal_dir).expect("create simulated cloud WAL directory");
    std::fs::create_dir_all(&local_wal_dir).expect("create local WAL directory");
    let wal_bytes = cloud_wal_test_bytes(1, 1, b"legacy-local");
    let legacy_path = local_wal_dir.join("1.wal");
    let canonical_path = local_wal_dir.join(crate::wal::segment_file_name(1));
    std::fs::write(&legacy_path, &wal_bytes).expect("write legacy local WAL alias");
    let catalog = cloud_wal_test_catalog(2, &[(1, 1, 1, wal_bytes.clone())]);
    let publication = catalog.segments.get(&1).unwrap();
    write_simulated_cloud_wal_publication(&cloud_wal_dir, publication, &wal_bytes);

    // Act
    let plan = startup::CloudStartupRecovery::materialize_simulated_cloud_wal_recovery_dir(
        &cloud_wal_dir,
        temp_dir.path(),
        RecoveryPolicy::Strict,
        &catalog,
    )
    .expect("materialize matching local and remote WAL");

    // Assert
    assert!(
        !legacy_path.exists(),
        "legacy alias must not survive recovery"
    );
    assert_eq!(
        std::fs::read(&canonical_path).expect("read canonical local WAL"),
        wal_bytes
    );
    assert_eq!(
        plan.remote_segments.get(&1),
        Some(&crate::runtime::RecoveredCloudWalSegment {
            max_sequence: 1,
            writer_epoch: 1,
        })
    );
    assert!(plan.local_segments.is_empty());
}

#[test]
fn should_fail_strict_recovery_given_later_local_wal_from_lower_writer_epoch() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let cloud_wal_dir = temp_dir.path().join("cloud_store").join("wal");
    let local_wal_dir = temp_dir.path().join("wal");
    std::fs::create_dir_all(&cloud_wal_dir).expect("create simulated cloud WAL directory");
    std::fs::create_dir_all(&local_wal_dir).expect("create local WAL directory");
    let wal_bytes = |sequence: u64, writer_epoch: u64| {
        let record = crate::wal::WalRecord::new(
            crate::wal::WalOpKind::Put,
            bytes::Bytes::from(format!("epoch-{writer_epoch}")),
            Some(bytes::Bytes::from_static(b"value")),
            sequence,
            writer_epoch,
        );
        let payload = crate::wal::encoding::encode(&record).expect("encode WAL record");
        let mut bytes = Vec::new();
        crate::wal::frame::append_frame(&mut bytes, &payload).expect("frame WAL record");
        bytes
    };
    let remote_bytes = wal_bytes(60, 2);
    let catalog = cloud_wal_test_catalog(3, &[(1, 60, 2, remote_bytes.clone())]);
    write_simulated_cloud_wal_publication(
        &cloud_wal_dir,
        catalog.segments.get(&1).unwrap(),
        &remote_bytes,
    );
    std::fs::write(
        local_wal_dir.join(crate::wal::segment_file_name(2)),
        wal_bytes(100, 1),
    )
    .expect("write later local-only stale-epoch WAL segment");

    // Act
    let result = startup::CloudStartupRecovery::materialize_simulated_cloud_wal_recovery_dir(
        &cloud_wal_dir,
        temp_dir.path(),
        RecoveryPolicy::Strict,
        &catalog,
    );

    // Assert
    assert!(matches!(
        result,
        Err(crate::common::MidgeError::RecoveryFailed(message))
            if message.contains("writer epoch regression")
                && message.contains("segment 2")
                && message.contains("1 < 2")
    ));
}

#[test]
fn should_skip_later_local_wal_from_lower_writer_epoch_during_salvage() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let cloud_wal_dir = temp_dir.path().join("cloud_store").join("wal");
    let local_wal_dir = temp_dir.path().join("wal");
    std::fs::create_dir_all(&cloud_wal_dir).expect("create simulated cloud WAL directory");
    std::fs::create_dir_all(&local_wal_dir).expect("create local WAL directory");
    let wal_bytes = |sequence: u64, writer_epoch: u64| {
        let record = crate::wal::WalRecord::new(
            crate::wal::WalOpKind::Put,
            bytes::Bytes::from(format!("epoch-{writer_epoch}")),
            Some(bytes::Bytes::from_static(b"value")),
            sequence,
            writer_epoch,
        );
        let payload = crate::wal::encoding::encode(&record).expect("encode WAL record");
        let mut bytes = Vec::new();
        crate::wal::frame::append_frame(&mut bytes, &payload).expect("frame WAL record");
        bytes
    };
    let stale_source = local_wal_dir.join(crate::wal::segment_file_name(2));
    let stale_bytes = wal_bytes(100, 1);
    let remote_bytes = wal_bytes(60, 2);
    let catalog = cloud_wal_test_catalog(3, &[(1, 60, 2, remote_bytes.clone())]);
    write_simulated_cloud_wal_publication(
        &cloud_wal_dir,
        catalog.segments.get(&1).unwrap(),
        &remote_bytes,
    );
    std::fs::write(&stale_source, &stale_bytes).expect("write later stale-epoch WAL segment");

    // Act
    let plan = startup::CloudStartupRecovery::materialize_simulated_cloud_wal_recovery_dir(
        &cloud_wal_dir,
        temp_dir.path(),
        RecoveryPolicy::Salvage,
        &catalog,
    )
    .expect("salvage later stale-epoch WAL segment");

    // Assert
    assert!(plan.opened_in_salvage_mode);
    assert_eq!(
        plan.remote_segments.get(&1),
        Some(&crate::runtime::RecoveredCloudWalSegment {
            max_sequence: 60,
            writer_epoch: 2,
        })
    );
    assert!(!plan.local_segments.contains_key(&2));
    assert!(!plan
        .replay_dir
        .join(crate::wal::cloud_segment_file_name(2))
        .exists());
    assert_eq!(
        std::fs::read(stale_source).expect("read authoritative stale-epoch WAL segment"),
        stale_bytes,
        "salvage must retain the authoritative source bytes"
    );
}

#[test]
fn should_recover_from_authoritative_catalog_without_listing_cloud_wal_objects() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let inner = Arc::new(crate::storage::cloud::MockCloudBackend::new());
    let backend: Arc<dyn crate::storage::cloud::CloudBackend> =
        Arc::new(ListOmittingCloudBackend::failing(inner, "wal/"));
    let cloud = crate::storage::cloud::CloudStorage::new(backend, "midge".to_string());

    // Act. Catalog format v1 replaced the old budgeted LIST-based recovery
    // path; recovery must consume only catalog publications.
    let catalog = cloud_wal_test_catalog(1, &[]);
    let result = Engine::materialize_cloud_wal_recovery_dir(
        &cloud,
        temp_dir.path(),
        RecoveryPolicy::Strict,
        &catalog,
    );

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_keep_salvage_healthy_given_catalog_recovery_when_irrelevant_list_fails() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let inner = Arc::new(crate::storage::cloud::MockCloudBackend::new());
    let backend: Arc<dyn crate::storage::cloud::CloudBackend> =
        Arc::new(ListOmittingCloudBackend::failing(inner, "wal/"));
    let cloud = crate::storage::cloud::CloudStorage::new(backend, "midge".to_string());

    // Act. A LIST failure cannot exhaust or degrade catalog-driven recovery
    // because LIST is no longer part of the authoritative WAL scan.
    let catalog = cloud_wal_test_catalog(1, &[]);
    let plan = startup::CloudStartupRecovery::materialize_cloud_wal_recovery_dir(
        &cloud,
        temp_dir.path(),
        RecoveryPolicy::Salvage,
        &catalog,
    )
    .expect("salvage cloud WAL materialization");

    // Assert
    assert!(!plan.opened_in_salvage_mode);
    assert!(plan.remote_segments.is_empty());
}

#[test]
fn should_recover_valid_active_cloud_wal_prefix_given_zero_filled_tail() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let cloud_wal_dir = temp_dir.path().join("cloud_store").join("wal");
    let local_wal_dir = temp_dir.path().join("wal");
    std::fs::create_dir_all(&cloud_wal_dir).expect("create cloud WAL directory");
    std::fs::create_dir_all(&local_wal_dir).expect("create local WAL directory");
    let record = crate::wal::WalRecord::new(
        crate::wal::WalOpKind::Put,
        bytes::Bytes::from_static(b"zero-tail-key"),
        Some(bytes::Bytes::from_static(b"zero-tail-value")),
        1,
        7,
    );
    let payload = crate::wal::encoding::encode(&record).expect("encode WAL record");
    let mut valid_bytes = Vec::new();
    crate::wal::frame::append_frame(&mut valid_bytes, &payload).expect("frame WAL record");
    let mut preallocated_bytes = valid_bytes.clone();
    preallocated_bytes.resize(valid_bytes.len() + 4096, 0);
    let active_path = local_wal_dir.join(crate::wal::ACTIVE_FILE_NAME);
    std::fs::write(&active_path, preallocated_bytes).expect("write preallocated active WAL");
    let catalog = cloud_wal_test_catalog(8, &[]);

    // Act
    let plan = startup::CloudStartupRecovery::materialize_simulated_cloud_wal_recovery_dir(
        &cloud_wal_dir,
        temp_dir.path(),
        RecoveryPolicy::Strict,
        &catalog,
    )
    .expect("recover active WAL with zero-filled tail");

    // Assert
    assert_eq!(
        plan.active_wal,
        Some(crate::runtime::RecoveredCloudActiveWal {
            max_sequence: 1,
            writer_epoch: 7,
            record_count: 1,
            valid_bytes: valid_bytes.len(),
        })
    );
    assert_eq!(
        std::fs::read(plan.replay_dir.join(crate::wal::ACTIVE_FILE_NAME))
            .expect("read staged active WAL"),
        valid_bytes
    );
    assert_eq!(
        std::fs::metadata(active_path)
            .expect("inspect truncated local active WAL")
            .len(),
        u64::try_from(valid_bytes.len()).expect("valid WAL length fits u64")
    );
}

#[test]
fn should_accept_strict_cloud_recovery_given_mixed_writer_epochs_in_active_wal() {
    // Arrange
    //
    // A local active WAL spanning more than one writer_epoch is the normal
    // shape after an ordinary failover (the file is reopened in place under
    // a new epoch and appended to), not corruption. Unlike a single
    // cloud-uploaded segment object — which is scoped to one epoch and
    // enforces that separately in `cloud_segment::inspect_bytes` — local
    // active-WAL inspection must accept this under the default (non-Salvage)
    // recovery policy. See lsm-spec format/wal.md §6.4 point 3.
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let cloud_wal_dir = temp_dir.path().join("cloud_store").join("wal");
    let local_wal_dir = temp_dir.path().join("wal");
    std::fs::create_dir_all(&cloud_wal_dir).expect("create cloud WAL directory");
    std::fs::create_dir_all(&local_wal_dir).expect("create local WAL directory");
    let mut active_bytes = Vec::new();
    for (sequence, writer_epoch) in [(1, 7), (2, 8)] {
        let record = crate::wal::WalRecord::new(
            crate::wal::WalOpKind::Put,
            bytes::Bytes::from(format!("epoch-{writer_epoch}")),
            Some(bytes::Bytes::from_static(b"value")),
            sequence,
            writer_epoch,
        );
        let payload = crate::wal::encoding::encode(&record).expect("encode WAL record");
        crate::wal::frame::append_frame(&mut active_bytes, &payload).expect("frame WAL record");
    }
    let active_path = local_wal_dir.join(crate::wal::ACTIVE_FILE_NAME);
    std::fs::write(&active_path, &active_bytes).expect("write mixed-epoch active WAL");
    let catalog = cloud_wal_test_catalog(9, &[]);

    // Act
    let plan = startup::CloudStartupRecovery::materialize_simulated_cloud_wal_recovery_dir(
        &cloud_wal_dir,
        temp_dir.path(),
        RecoveryPolicy::Strict,
        &catalog,
    )
    .expect("recover active WAL spanning a failover epoch bump");

    // Assert
    assert_eq!(
        plan.active_wal,
        Some(crate::runtime::RecoveredCloudActiveWal {
            max_sequence: 2,
            writer_epoch: 8,
            record_count: 2,
            valid_bytes: active_bytes.len(),
        })
    );
    assert_eq!(
        std::fs::read(plan.replay_dir.join(crate::wal::ACTIVE_FILE_NAME))
            .expect("read staged active WAL"),
        active_bytes
    );
}

#[test]
fn should_fail_strict_cloud_recovery_without_truncating_active_wal_given_corrupted_length_hides_valid_suffix(
) {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let cloud_wal_dir = temp_dir.path().join("cloud_store").join("wal");
    let local_wal_dir = temp_dir.path().join("wal");
    std::fs::create_dir_all(&cloud_wal_dir).expect("create cloud WAL directory");
    std::fs::create_dir_all(&local_wal_dir).expect("create local WAL directory");
    let framed_record = |sequence, key: &'static [u8]| {
        let record = crate::wal::WalRecord::new(
            crate::wal::WalOpKind::Put,
            bytes::Bytes::from_static(key),
            Some(bytes::Bytes::from_static(b"value")),
            sequence,
            0,
        );
        let payload = crate::wal::encoding::encode(&record).expect("encode WAL record");
        let mut frame = Vec::new();
        crate::wal::frame::append_frame(&mut frame, &payload).expect("frame WAL record");
        frame
    };
    let first = framed_record(1, b"first");
    let second = framed_record(2, b"verified-suffix");
    let mut corrupted_bytes = [first, second].concat();
    let corrupt_length = u32::try_from(corrupted_bytes.len()).expect("WAL length fits u32");
    corrupted_bytes[..4].copy_from_slice(&corrupt_length.to_le_bytes());
    let active_path = local_wal_dir.join(crate::wal::ACTIVE_FILE_NAME);
    std::fs::write(&active_path, &corrupted_bytes).expect("write corrupt active WAL");
    let catalog = cloud_wal_test_catalog(1, &[]);

    // Act
    let result = startup::CloudStartupRecovery::materialize_simulated_cloud_wal_recovery_dir(
        &cloud_wal_dir,
        temp_dir.path(),
        RecoveryPolicy::Strict,
        &catalog,
    );

    // Assert
    assert!(matches!(
        result,
        Err(crate::common::MidgeError::RecoveryFailed(message))
            if message.contains("hides a verified later frame")
    ));
    assert_eq!(
        std::fs::read(active_path).expect("read authoritative active WAL after failed recovery"),
        corrupted_bytes,
        "strict recovery must not mutate the authoritative WAL before validation succeeds"
    );
}

#[test]
fn should_not_overwrite_newer_remote_manifest_metadata_during_engine_mirror() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let backend = Arc::new(crate::storage::cloud::MockCloudBackend::new());
    let cloud = crate::storage::cloud::CloudStorage::new(backend, "midge".to_string());
    let local_manifest = crate::metadata::Manifest {
        last_persisted_sequence: 20,
        ..Default::default()
    };
    crate::metadata::ManifestPersistence::save(temp_dir.path(), &local_manifest)
        .expect("save local manifest");
    let remote_manifest = crate::metadata::Manifest {
        last_persisted_sequence: 21,
        ..Default::default()
    };
    Engine::blocking_cloud_put(
        &cloud,
        "metadata/manifest.json",
        serde_json::to_vec_pretty(&remote_manifest).expect("serialize remote manifest"),
    )
    .expect("upload newer remote manifest");

    let error = Engine::mirror_cloud_metadata(&cloud, temp_dir.path(), RecoveryPolicy::Strict)
        .expect_err("newer remote manifest metadata must reject stale engine mirror");

    // Act
    // Assert
    assert!(
        error.to_string().contains("newer")
            || error.to_string().contains("ahead")
            || error.to_string().contains("stale"),
        "unexpected stale engine metadata mirror error: {error}"
    );
    let retained: crate::metadata::Manifest = serde_json::from_slice(
        &Engine::blocking_cloud_get(&cloud, "metadata/manifest.json")
            .expect("download retained remote manifest"),
    )
    .expect("parse retained remote manifest");
    assert_eq!(
        retained.last_persisted_sequence, 21,
        "engine metadata mirror must not overwrite newer remote manifest"
    );
}

#[test]
fn should_not_rewrite_unchanged_cloud_metadata_during_engine_mirror() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let backend = Arc::new(crate::storage::cloud::MockCloudBackend::new());
    let cloud = crate::storage::cloud::CloudStorage::new(backend.clone(), "midge".to_string());
    crate::metadata::ManifestPersistence::save(
        temp_dir.path(),
        &crate::metadata::Manifest::default(),
    )
    .expect("save local manifest");
    Engine::mirror_cloud_metadata(&cloud, temp_dir.path(), RecoveryPolicy::Strict)
        .expect("perform initial metadata mirror");
    backend.clear_history();

    // Act
    Engine::mirror_cloud_metadata(&cloud, temp_dir.path(), RecoveryPolicy::Strict)
        .expect("repeat unchanged metadata mirror");

    // Assert
    assert!(
        backend.get_uploads().is_empty(),
        "startup metadata convergence must not create duplicate object versions"
    );
}

#[test]
fn should_hydrate_cloud_metadata_when_listing_is_stale_but_object_is_readable() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let inner = Arc::new(crate::storage::cloud::MockCloudBackend::new());
    let backend = Arc::new(ListOmittingCloudBackend::new(
        Arc::clone(&inner),
        "metadata/",
    ));
    let cloud = crate::storage::cloud::CloudStorage::new(backend, "midge".to_string());
    let remote_manifest = crate::metadata::Manifest {
        last_persisted_sequence: 42,
        ..Default::default()
    };
    Engine::blocking_cloud_put(
        &cloud,
        "metadata/manifest.json",
        serde_json::to_vec_pretty(&remote_manifest).expect("serialize remote manifest"),
    )
    .expect("upload readable remote manifest metadata");

    Engine::hydrate_cloud_metadata(&cloud, temp_dir.path(), RecoveryPolicy::Strict)
        .expect("stale metadata list must not hide directly readable metadata");

    let hydrated = crate::metadata::ManifestPersistence::load(temp_dir.path())
        .expect("load hydrated manifest");
    // Act
    // Assert
    assert_eq!(
        hydrated.last_persisted_sequence, 42,
        "metadata hydration must probe known metadata keys directly"
    );
}

#[test]
fn should_reject_mixed_cloud_manifest_metadata_without_journal() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let backend = Arc::new(crate::storage::cloud::MockCloudBackend::new());
    let cloud = crate::storage::cloud::CloudStorage::new(backend, "midge".to_string());
    let snapshot_manifest = crate::metadata::Manifest {
        last_persisted_sequence: 10,
        ..Default::default()
    };
    let current_manifest = crate::metadata::Manifest {
        last_persisted_sequence: 11,
        ..Default::default()
    };
    Engine::blocking_cloud_put(
        &cloud,
        "metadata/manifest.snapshot.json",
        serde_json::to_vec_pretty(&snapshot_manifest).expect("serialize snapshot manifest"),
    )
    .expect("upload stale snapshot");
    Engine::blocking_cloud_put(
        &cloud,
        "metadata/manifest.json",
        serde_json::to_vec_pretty(&current_manifest).expect("serialize current manifest"),
    )
    .expect("upload newer manifest");

    let error = Engine::hydrate_cloud_metadata(&cloud, temp_dir.path(), RecoveryPolicy::Strict)
        .expect_err("strict hydration must reject mixed manifest metadata without journal");

    // Act
    // Assert
    assert!(
        error.to_string().contains("mixed")
            || error.to_string().contains("inconsistent")
            || error.to_string().contains("sequence"),
        "unexpected mixed metadata error: {error}"
    );
}

#[test]
fn should_salvage_mixed_cloud_manifest_metadata_by_retaining_highest_sequence() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let backend = Arc::new(crate::storage::cloud::MockCloudBackend::new());
    let cloud = crate::storage::cloud::CloudStorage::new(backend, "midge".to_string());
    let snapshot_manifest = crate::metadata::Manifest {
        last_persisted_sequence: 10,
        ..Default::default()
    };
    let current_manifest = crate::metadata::Manifest {
        last_persisted_sequence: 11,
        ..Default::default()
    };
    Engine::blocking_cloud_put(
        &cloud,
        "metadata/manifest.snapshot.json",
        serde_json::to_vec_pretty(&snapshot_manifest).expect("serialize snapshot manifest"),
    )
    .expect("upload stale snapshot");
    Engine::blocking_cloud_put(
        &cloud,
        "metadata/manifest.json",
        serde_json::to_vec_pretty(&current_manifest).expect("serialize current manifest"),
    )
    .expect("upload newer manifest");

    Engine::hydrate_cloud_metadata(&cloud, temp_dir.path(), RecoveryPolicy::Salvage)
        .expect("salvage hydration should retain the highest sequence manifest metadata");

    let hydrated = crate::metadata::ManifestPersistence::load(temp_dir.path())
        .expect("load salvaged manifest metadata");
    // Act
    // Assert
    assert_eq!(
        hydrated.last_persisted_sequence, 11,
        "salvage hydration must not let a stale snapshot hide a newer manifest"
    );
}

struct ListOmittingCloudBackend {
    inner: Arc<crate::storage::cloud::MockCloudBackend>,
    omitted_prefix: String,
    fail_list: bool,
}

impl ListOmittingCloudBackend {
    fn new(
        inner: Arc<crate::storage::cloud::MockCloudBackend>,
        omitted_prefix: impl Into<String>,
    ) -> Self {
        Self {
            inner,
            omitted_prefix: omitted_prefix.into(),
            fail_list: false,
        }
    }

    fn failing(
        inner: Arc<crate::storage::cloud::MockCloudBackend>,
        failed_prefix: impl Into<String>,
    ) -> Self {
        Self {
            inner,
            omitted_prefix: failed_prefix.into(),
            fail_list: true,
        }
    }
}

impl crate::storage::cloud::CloudBackend for ListOmittingCloudBackend {
    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        self.inner.submit_put(key, data, headers, callback);
    }

    fn submit_get(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        self.inner.submit_get(key, callback);
    }

    fn submit_get_range(
        &self,
        key: &str,
        start: u64,
        end: Option<u64>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        self.inner.submit_get_range(key, start, end, callback);
    }

    fn submit_delete(
        &self,
        key: &str,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        self.inner.submit_delete(key, headers, callback);
    }

    fn submit_list(&self, prefix: &str, callback: crate::storage::cloud::CloudCallback) {
        if prefix.ends_with(&self.omitted_prefix) {
            if self.fail_list {
                let _ = callback.send(crate::storage::cloud::CloudEvent::List {
                    prefix: prefix.to_string(),
                    result: crate::storage::cloud::CloudOutcome::Err(
                        crate::storage::cloud::CloudError::ServerError(
                            "cloud LIST item count exceeded safety budget".to_string(),
                        ),
                    ),
                });
                return;
            }
            let _ = callback.send(crate::storage::cloud::CloudEvent::List {
                prefix: prefix.to_string(),
                result: crate::storage::cloud::CloudOutcome::Ok(Vec::new()),
            });
            return;
        }
        self.inner.submit_list(prefix, callback);
    }

    fn submit_head(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        self.inner.submit_head(key, callback);
    }
}

fn test_sst_bytes_with_key_value(key: &[u8], value: &[u8]) -> Vec<u8> {
    use crate::sst::traits::SstFactory;

    let factory = crate::sst::FsSstFactoryIo::new(Arc::new(crate::io::MockFs::new()), 4096);
    let mut writer = factory.create().expect("create test sst writer");
    writer
        .add_with_meta(key, Some(value), 1, 0, None)
        .expect("write test sst entry");
    writer.finish_bytes().expect("finish test sst bytes")
}

fn test_sst_bytes_with_value(value: &[u8]) -> Vec<u8> {
    test_sst_bytes_with_key_value(b"cloud-list-key", value)
}

fn test_sst_bytes() -> Vec<u8> {
    test_sst_bytes_with_value(b"cloud-list-value")
}

fn same_size_sst_with_different_crc(bytes: &[u8]) -> Vec<u8> {
    assert!(bytes.len() > 4, "test SST must contain a data block");
    let mut changed = bytes.to_vec();
    changed[4] ^= 0x01;
    assert_eq!(changed.len(), bytes.len());
    assert_ne!(changed, bytes);
    assert_ne!(crc32c::crc32c(&changed), crc32c::crc32c(bytes));

    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let path = temp_dir.path().join("changed.sst");
    std::fs::write(&path, &changed).expect("write changed SST");
    crate::sst::fs::SstFileIo::open_with_real_fs(&path)
        .expect("changed same-size SST should remain structurally readable");

    changed
}

fn cloud_with_stale_sst_listing() -> crate::storage::cloud::CloudStorage {
    let inner = Arc::new(crate::storage::cloud::MockCloudBackend::new());
    let backend = Arc::new(ListOmittingCloudBackend::new(Arc::clone(&inner), "sst/"));
    crate::storage::cloud::CloudStorage::new(backend, "midge".to_string())
}

#[test]
fn should_restore_manifest_sst_when_cloud_listing_is_stale_but_object_is_readable() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let mut state = crate::runtime::RuntimeState::try_new(
        temp_dir.path().to_path_buf(),
        false,
        RecoveryPolicy::Strict,
    )
    .expect("create runtime state");
    let sst_name = crate::sst::file_name(0, 0, 1);
    let sst_bytes = test_sst_bytes();
    state.manifest.files.push(crate::metadata::FileMeta {
        name: sst_name.clone(),
        level: 0,
        size_bytes: sst_bytes.len() as u64,
        cf_id: 0,
        sst_seq: 1,
        smallest_key: Some(b"cloud-list-key".to_vec()),
        largest_key: Some(b"cloud-list-key".to_vec()),
        smallest_seq: Some(1),
        largest_seq: Some(1),
        ..Default::default()
    });
    let cloud = cloud_with_stale_sst_listing();
    Engine::blocking_cloud_put(&cloud, &crate::sst::object_key(&sst_name), sst_bytes)
        .expect("upload test sst");

    Engine::ensure_local_sst_cache_from_cloud_storage(&mut state, &cloud)
        .expect("stale list should not make readable manifest SST unrecoverable");

    // Act
    // Assert
    assert!(
        state.sst_dir.join(&sst_name).exists(),
        "readable cloud SST should be restored despite stale LIST"
    );
}

#[test]
fn should_reject_manifest_sst_when_cloud_object_size_differs_from_manifest() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let mut state = crate::runtime::RuntimeState::try_new(
        temp_dir.path().to_path_buf(),
        false,
        RecoveryPolicy::Strict,
    )
    .expect("create runtime state");
    let sst_name = crate::sst::file_name(0, 0, 3);
    let committed_sst_bytes = test_sst_bytes_with_value(b"manifest-sized-value");
    let wrong_sst_bytes = test_sst_bytes_with_value(b"different-cloud-object-bytes");
    // Act
    // Assert
    assert_ne!(
        committed_sst_bytes.len(),
        wrong_sst_bytes.len(),
        "test must use a valid cloud SST with different size than the committed manifest"
    );
    state.manifest.files.push(crate::metadata::FileMeta {
        name: sst_name.clone(),
        level: 0,
        size_bytes: committed_sst_bytes.len() as u64,
        cf_id: 0,
        sst_seq: 3,
        smallest_key: Some(b"cloud-list-key".to_vec()),
        largest_key: Some(b"cloud-list-key".to_vec()),
        smallest_seq: Some(1),
        largest_seq: Some(1),
        ..Default::default()
    });
    let cloud = cloud_with_stale_sst_listing();
    Engine::blocking_cloud_put(&cloud, &crate::sst::object_key(&sst_name), wrong_sst_bytes)
        .expect("upload wrong-sized but structurally valid test sst");

    let error = Engine::ensure_local_sst_cache_from_cloud_storage(&mut state, &cloud)
        .expect_err("strict recovery must reject wrong-sized authoritative cloud SST");

    assert!(
        error.to_string().contains("size"),
        "unexpected wrong-sized cloud SST recovery error: {error}"
    );
    assert!(
        !state.sst_dir.join(&sst_name).exists(),
        "wrong-sized cloud SST must not be installed into the local cache"
    );
}

#[test]
fn should_reject_manifest_sst_when_same_size_cloud_object_crc_differs() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let mut state = crate::runtime::RuntimeState::try_new(
        temp_dir.path().to_path_buf(),
        false,
        RecoveryPolicy::Strict,
    )
    .expect("create runtime state");
    let sst_name = crate::sst::file_name(0, 0, 4);
    let wrong_sst_bytes = test_sst_bytes();
    let expected_crc = crc32c::crc32c(&wrong_sst_bytes) ^ 0xffff_ffff;
    state.manifest.files.push(crate::metadata::FileMeta {
        name: sst_name.clone(),
        level: 0,
        size_bytes: wrong_sst_bytes.len() as u64,
        content_crc32c: Some(expected_crc),
        cf_id: 0,
        sst_seq: 4,
        smallest_key: Some(b"cloud-list-key".to_vec()),
        largest_key: Some(b"cloud-list-key".to_vec()),
        smallest_seq: Some(1),
        largest_seq: Some(1),
        ..Default::default()
    });
    let cloud = cloud_with_stale_sst_listing();
    Engine::blocking_cloud_put(&cloud, &crate::sst::object_key(&sst_name), wrong_sst_bytes)
        .expect("upload same-sized but wrong-content test sst");

    let error = Engine::ensure_local_sst_cache_from_cloud_storage(&mut state, &cloud)
        .expect_err("strict recovery must reject wrong-content authoritative cloud SST");

    // Act
    // Assert
    assert!(
        error.to_string().contains("crc") || error.to_string().contains("content"),
        "unexpected wrong-content cloud SST recovery error: {error}"
    );
    assert!(
        !state.sst_dir.join(&sst_name).exists(),
        "wrong-content cloud SST must not be installed into the local cache"
    );
}

#[test]
fn should_replace_wrong_sized_local_sst_cache_from_authoritative_cloud_object() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let mut state = crate::runtime::RuntimeState::try_new(
        temp_dir.path().to_path_buf(),
        false,
        RecoveryPolicy::Strict,
    )
    .expect("create runtime state");
    let sst_name = crate::sst::file_name(0, 0, 5);
    let committed_sst_bytes = test_sst_bytes_with_value(b"manifest-sized-value");
    let stale_local_sst_bytes = test_sst_bytes_with_value(b"different-local-cache-bytes");
    // Act
    // Assert
    assert_ne!(
        committed_sst_bytes.len(),
        stale_local_sst_bytes.len(),
        "test must use a stale local SST with different size than the committed manifest"
    );
    state.manifest.files.push(crate::metadata::FileMeta {
        name: sst_name.clone(),
        level: 0,
        size_bytes: committed_sst_bytes.len() as u64,
        cf_id: 0,
        sst_seq: 4,
        smallest_key: Some(b"cloud-list-key".to_vec()),
        largest_key: Some(b"cloud-list-key".to_vec()),
        smallest_seq: Some(1),
        largest_seq: Some(1),
        ..Default::default()
    });
    std::fs::write(state.sst_dir.join(&sst_name), stale_local_sst_bytes)
        .expect("write stale local SST cache");
    let cloud = cloud_with_stale_sst_listing();
    Engine::blocking_cloud_put(
        &cloud,
        &crate::sst::object_key(&sst_name),
        committed_sst_bytes.clone(),
    )
    .expect("upload authoritative manifest-sized test sst");

    Engine::ensure_local_sst_cache_from_cloud_storage(&mut state, &cloud)
        .expect("wrong-sized local cache should be restored from authoritative cloud SST");

    assert_eq!(
        std::fs::read(state.sst_dir.join(&sst_name)).expect("read restored local SST"),
        committed_sst_bytes,
        "local SST cache must be replaced with the manifest-sized cloud object"
    );
}

#[test]
fn should_replace_same_size_wrong_crc_local_sst_cache_from_authoritative_cloud_object() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let mut state = crate::runtime::RuntimeState::try_new(
        temp_dir.path().to_path_buf(),
        false,
        RecoveryPolicy::Strict,
    )
    .expect("create runtime state");
    let sst_name = crate::sst::file_name(0, 0, 6);
    let committed_sst_bytes = test_sst_bytes();
    let stale_local_sst_bytes = same_size_sst_with_different_crc(&committed_sst_bytes);
    state.manifest.files.push(crate::metadata::FileMeta {
        name: sst_name.clone(),
        level: 0,
        size_bytes: committed_sst_bytes.len() as u64,
        content_crc32c: Some(crc32c::crc32c(&committed_sst_bytes)),
        cf_id: 0,
        sst_seq: 6,
        smallest_key: Some(b"cloud-list-key".to_vec()),
        largest_key: Some(b"cloud-list-key".to_vec()),
        smallest_seq: Some(1),
        largest_seq: Some(1),
        ..Default::default()
    });
    std::fs::write(state.sst_dir.join(&sst_name), stale_local_sst_bytes)
        .expect("write stale same-size local SST cache");
    let cloud = cloud_with_stale_sst_listing();
    Engine::blocking_cloud_put(
        &cloud,
        &crate::sst::object_key(&sst_name),
        committed_sst_bytes.clone(),
    )
    .expect("upload authoritative manifest-crc test sst");

    Engine::ensure_local_sst_cache_from_cloud_storage(&mut state, &cloud)
        .expect("same-size wrong local cache should be restored from authoritative cloud SST");

    // Act
    // Assert
    assert_eq!(
        std::fs::read(state.sst_dir.join(&sst_name)).expect("read restored local SST"),
        committed_sst_bytes,
        "local SST cache must be replaced with the manifest-crc cloud object"
    );
}

#[test]
fn should_salvage_retain_manifest_sst_when_local_cache_is_valid_but_cloud_crc_differs() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let mut state = crate::runtime::RuntimeState::try_new(
        temp_dir.path().to_path_buf(),
        false,
        RecoveryPolicy::Salvage,
    )
    .expect("create runtime state");
    let sst_name = crate::sst::file_name(0, 0, 7);
    let committed_sst_bytes = test_sst_bytes();
    let wrong_cloud_sst_bytes = same_size_sst_with_different_crc(&committed_sst_bytes);
    state.manifest.files.push(crate::metadata::FileMeta {
        name: sst_name.clone(),
        level: 0,
        size_bytes: committed_sst_bytes.len() as u64,
        content_crc32c: Some(crc32c::crc32c(&committed_sst_bytes)),
        cf_id: 0,
        sst_seq: 7,
        smallest_key: Some(b"cloud-list-key".to_vec()),
        largest_key: Some(b"cloud-list-key".to_vec()),
        smallest_seq: Some(1),
        largest_seq: Some(1),
        ..Default::default()
    });
    std::fs::write(state.sst_dir.join(&sst_name), &committed_sst_bytes)
        .expect("write valid local SST cache");
    let cloud = cloud_with_stale_sst_listing();
    Engine::blocking_cloud_put(
        &cloud,
        &crate::sst::object_key(&sst_name),
        wrong_cloud_sst_bytes,
    )
    .expect("upload wrong-content cloud SST");

    Engine::ensure_local_sst_cache_from_cloud_storage(&mut state, &cloud)
        .expect("salvage should keep a manifest SST when the local cache is valid");

    // Act
    // Assert
    assert!(
        state
            .manifest
            .files
            .iter()
            .any(|file| file.name == sst_name),
        "salvage must not drop a manifest SST that still has a valid local recoverable copy"
    );
    assert_eq!(
        std::fs::read(state.sst_dir.join(&sst_name)).expect("read retained local SST"),
        committed_sst_bytes,
        "valid local SST cache must remain intact"
    );
    assert!(
        state.persistence_anomaly_detected(),
        "salvage should still surface the invalid cloud copy as a persistence anomaly"
    );
}

#[test]
fn should_stage_intent_replay_sst_when_cloud_listing_is_stale_but_object_is_readable() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let mut state = crate::runtime::RuntimeState::try_new(
        temp_dir.path().to_path_buf(),
        false,
        RecoveryPolicy::Strict,
    )
    .expect("create runtime state");
    let sst_name = crate::sst::file_name(0, 0, 2);
    let sst_bytes = test_sst_bytes();
    let cloud = cloud_with_stale_sst_listing();
    Engine::blocking_cloud_put(&cloud, &crate::sst::object_key(&sst_name), sst_bytes)
        .expect("upload intent replay sst");

    Engine::ensure_named_sst_cache_from_cloud_storage(
        &mut state,
        &cloud,
        vec![CloudSstRecoveryProof::name_only(sst_name.clone())],
    )
    .expect("stale list should not make readable intent SST unstaged");

    // Act
    // Assert
    assert!(
        state.sst_dir.join(&sst_name).exists(),
        "readable cloud SST should be staged despite stale LIST"
    );
}

#[test]
fn should_reject_intent_replay_sst_when_cloud_object_crc_differs_from_intent() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let mut state = crate::runtime::RuntimeState::try_new(
        temp_dir.path().to_path_buf(),
        false,
        RecoveryPolicy::Strict,
    )
    .expect("create runtime state");
    let sst_name = crate::sst::file_name(0, 0, 7);
    let sst_bytes = test_sst_bytes();
    let expected_crc = crc32c::crc32c(&sst_bytes) ^ 0xffff_ffff;
    state
        .intent_log
        .push(crate::runtime::IntentLogEntry::SstAdded {
            file_meta: crate::runtime::FileMeta {
                name: sst_name.clone(),
                level: 0,
                size_bytes: sst_bytes.len() as u64,
                content_crc32c: Some(expected_crc),
                cf_id: 0,
                smallest_key: Some(b"cloud-list-key".to_vec()),
                largest_key: Some(b"cloud-list-key".to_vec()),
                smallest_seq: Some(1),
                largest_seq: Some(1),
            },
        });
    let cloud = cloud_with_stale_sst_listing();
    Engine::blocking_cloud_put(&cloud, &crate::sst::object_key(&sst_name), sst_bytes)
        .expect("upload intent SST with mismatched content proof");

    let proofs = Engine::cloud_recovery_sst_proofs_for_intent_replay(&state);
    let error = Engine::ensure_named_sst_cache_from_cloud_storage(&mut state, &cloud, proofs)
        .expect_err("strict recovery must reject intent SST with mismatched content proof");

    // Act
    // Assert
    assert!(
        error.to_string().contains("crc") || error.to_string().contains("content"),
        "unexpected intent SST proof error: {error}"
    );
    assert!(
        !state.sst_dir.join(&sst_name).exists(),
        "intent SST with mismatched proof must not be staged"
    );
}
