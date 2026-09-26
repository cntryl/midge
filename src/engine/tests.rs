use super::*;
use crate::lease::PrimaryLease;
use crate::types::EntryType;

#[test]
fn should_report_wal_recovery_counters_in_engine_diagnostics_after_replay() -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir().map_err(MidgeError::Io)?;
    {
        let mut engine = Engine::open(OpenOptions::local(directory.path()).build()?)?;
        let cf = engine
            .get_column_family("default")
            .expect("default column family");
        let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
        tx.put(b"replayed-key".to_vec(), b"replayed-value".to_vec(), None)?;
        tx.commit(WriteOptions::buffered())?;
        engine.shutdown(Duration::from_secs(5))?;
    }

    // Act
    let mut reopened = Engine::open(OpenOptions::local(directory.path()).build()?)?;
    let counters = reopened.runtime_handle.diagnostics.counters();

    // Assert
    assert!(counters.wal_recovery_records_replayed > 0);
    assert!(counters.wal_recovery_bytes_replayed > 0);
    reopened.shutdown(Duration::from_secs(5))
}

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
    super::LeaseState::release_fencing_parts(lease_heartbeat, lease, lease_guard)?;

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
    // Reaper completion is asynchronous and can lag under hosted machine load.
    let reopen_deadline = std::time::Instant::now() + Duration::from_secs(30);
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
fn should_preserve_fenced_kind_when_empty_sync_commit_loses_writer_authority() -> MidgeResult<()> {
    // Arrange: replace this temporary database's leader record with a newer
    // holder so the explicit WAL sync observes lost authority.
    let temp_dir = tempfile::tempdir()?;
    let engine = Engine::open(OpenOptions::local(temp_dir.path()).build()?)?;
    let default_cf = engine
        .get_column_family("default")
        .ok_or_else(|| MidgeError::Internal("default column family missing".into()))?;
    let transaction = engine.begin_tx(default_cf.id(), TransactionMode::ReadWrite)?;
    let record_path = temp_dir.path().join(".midge_leader");
    let record = std::fs::read_to_string(&record_path)?;
    let epoch = record
        .lines()
        .find_map(|line| line.strip_prefix("epoch: "))
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| MidgeError::Internal("leader epoch missing".into()))?;
    let acquired_at = record
        .lines()
        .find_map(|line| line.strip_prefix("acquired_at: "))
        .ok_or_else(|| MidgeError::Internal("leader acquisition time missing".into()))?;
    std::fs::write(
        &record_path,
        format!(
            "epoch: {}\nholder_id: replacement-writer\nacquired_at: {acquired_at}\n",
            epoch + 1
        ),
    )?;

    // Act
    let result = transaction.commit(WriteOptions::sync());

    // Assert
    assert!(matches!(result, Err(MidgeError::Fenced(_))), "{result:?}");
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

// These reopen qualifications wait for runtime shutdown, WAL drain, and lease
// cleanup. Their assertions concern the recovered data, not caller deadlines.
const PARTITIONED_COMPACTION_SHUTDOWN_TIMEOUT: Duration = Duration::from_mins(2);

fn partitioned_compaction_value(index: usize) -> Vec<u8> {
    (0..512)
        .map(|offset| {
            u8::try_from((index.wrapping_mul(31) + offset * 17) % 251)
                .expect("generated byte fits u8")
        })
        .collect()
}

fn assert_partitioned_compaction_reads(
    engine: &Engine,
    cf: &ColumnFamilyHandle,
) -> MidgeResult<()> {
    let layout = engine.metrics().get_storage_layout()?;
    let output_files: Vec<_> = layout
        .levels
        .iter()
        .flat_map(|level| level.files.iter())
        .filter(|file| file.cf_id == cf.id() && file.level > 0)
        .collect();
    assert!(
        output_files.len() > 1,
        "small target must produce multiple manifest outputs: {output_files:?}"
    );
    let unique_names: std::collections::HashSet<_> =
        output_files.iter().map(|file| file.name.as_str()).collect();
    assert_eq!(unique_names.len(), output_files.len());
    assert!(output_files.iter().all(|file| {
        crate::cloud_layout::parse_compaction_file_name(&file.name).is_some_and(
            |(name_cf, name_level, _, _)| name_cf == cf.id() && name_level == file.level,
        )
    }));

    let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
    assert_eq!(
        read.get(b"key-0000")?.as_deref(),
        Some(partitioned_compaction_value(0).as_slice())
    );
    assert_eq!(
        read.get(b"key-0191")?.as_deref(),
        Some(partitioned_compaction_value(191).as_slice())
    );
    assert_eq!(read.get(b"key-0060")?, None);
    let bounded = Query::new()
        .start_key(bytes::Bytes::from_static(b"key-0010"))
        .end_key(bytes::Bytes::from_static(b"key-0030"));
    let forward = read.scan(&bounded)?.try_collect()?;
    let reverse = read.scan(&bounded.clone().reverse())?.try_collect()?;
    let expected: Vec<_> = (10..30)
        .map(|index| format!("key-{index:04}").into_bytes())
        .collect();
    assert_eq!(
        forward
            .iter()
            .map(|pair| pair.0.as_ref())
            .collect::<Vec<_>>(),
        expected.iter().map(Vec::as_slice).collect::<Vec<_>>()
    );
    assert_eq!(
        reverse
            .iter()
            .map(|pair| pair.0.as_ref())
            .collect::<Vec<_>>(),
        expected.iter().rev().map(Vec::as_slice).collect::<Vec<_>>()
    );
    drop(read);

    Ok(())
}

fn assert_partitioned_compaction_reopen(mut reopened: Engine) -> MidgeResult<()> {
    const KEY_COUNT: usize = 192;

    let reopened_cf = reopened.get_column_family("partitioned").ok_or_else(|| {
        MidgeError::Internal("partitioned column family missing after reopen".to_string())
    })?;
    let read = reopened.begin_tx(reopened_cf.id(), TransactionMode::ReadOnly)?;
    assert_eq!(
        read.scan(&Query::new())?.try_collect()?.len(),
        KEY_COUNT - 40
    );
    assert_eq!(
        read.get(b"key-0096")?.as_deref(),
        Some(partitioned_compaction_value(96).as_slice())
    );
    drop(read);
    let reopened_layout = reopened.metrics().get_storage_layout()?;
    let reopened_names: Vec<_> = reopened_layout
        .levels
        .iter()
        .flat_map(|level| level.files.iter())
        .filter(|file| file.cf_id == reopened_cf.id())
        .map(|file| file.name.as_str())
        .collect();
    assert_eq!(
        reopened_names
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>()
            .len(),
        reopened_names.len()
    );
    reopened.shutdown(PARTITIONED_COMPACTION_SHUTDOWN_TIMEOUT)
}

fn assert_partitioned_compaction_engine_round_trip(
    mut open_options: impl FnMut() -> MidgeResult<OpenOptions>,
) -> MidgeResult<()> {
    const KEY_COUNT: usize = 192;
    const TARGET_SST_SIZE: usize = 4 * 1024;

    // Arrange
    let mut options = open_options()?;
    options.set_compaction_target_sst_size_for_test(TARGET_SST_SIZE);
    let mut engine = Engine::open(options)?;
    let cf = engine.create_column_family("partitioned")?;
    for batch in 0..4 {
        let mut transaction = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
        for offset in 0..(KEY_COUNT / 4) {
            let index = batch * (KEY_COUNT / 4) + offset;
            transaction.put(
                format!("key-{index:04}").into_bytes(),
                partitioned_compaction_value(index),
                None,
            )?;
        }
        let write_options = if engine.cloud_mode {
            WriteOptions::cloud_strict()
        } else {
            WriteOptions::buffered()
        };
        transaction.commit(write_options)?;
        engine.flush_cf(&cf)?;
    }
    let mut delete = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    delete.delete_range(b"key-0040".to_vec(), b"key-0080".to_vec())?;
    let delete_options = if engine.cloud_mode {
        WriteOptions::cloud_strict()
    } else {
        WriteOptions::buffered()
    };
    delete.commit(delete_options)?;
    engine.flush_cf(&cf)?;

    // Act
    engine.compact_all()?;

    // Assert
    assert_partitioned_compaction_reads(&engine, &cf)?;
    engine.shutdown(PARTITIONED_COMPACTION_SHUTDOWN_TIMEOUT)?;

    let mut reopen_options = open_options()?;
    reopen_options.set_compaction_target_sst_size_for_test(TARGET_SST_SIZE);
    assert_partitioned_compaction_reopen(Engine::open(reopen_options)?)
}

#[test]
fn should_preserve_partitioned_compaction_across_local_reopen() -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir().map_err(MidgeError::Io)?;

    // Act
    let result = assert_partitioned_compaction_engine_round_trip(|| {
        OpenOptions::local(temp_dir.path())
            .background_compaction(false)
            .with_memtable_size_limit(64 * 1024)
            // This qualifies partitioned output and recovery rather than the
            // response deadline. Hosted Windows runners can share disk load
            // with other test processes during the manual compaction.
            .runtime_response_timeout(Duration::from_mins(3))
            .build()
    });

    // Assert
    result
}

#[test]
fn should_preserve_partitioned_compaction_across_simulated_cloud_reopen() -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir().map_err(MidgeError::Io)?;

    // Act
    let result = assert_partitioned_compaction_engine_round_trip(|| {
        OpenOptions::cloud_simulated(temp_dir.path(), "partitioned-bucket", "partitioned-prefix")
            .background_compaction(false)
            .with_memtable_size_limit(64 * 1024)
            // This is a multi-output durability qualification, not a response
            // deadline test. Windows hosted runners can execute it alongside
            // the intentionally large compaction resource proof, so retain a
            // bounded but explicit allowance for that filesystem contention.
            .runtime_response_timeout(Duration::from_mins(3))
            .build()
    });

    // Assert
    result
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
fn should_not_mirror_manifest_when_format_marker_upload_fails_under_salvage() {
    // Arrange: a local FORMAT 4 upgrade must reach cloud before its manifest.
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    crate::metadata::ensure_or_create_format_marker(temp_dir.path())
        .expect("create FORMAT 4 marker");
    crate::metadata::ManifestPersistence::save(
        temp_dir.path(),
        &crate::metadata::Manifest::default(),
    )
    .expect("save local manifest");
    let inner = Arc::new(crate::storage::cloud::MockCloudBackend::new());
    let original_cloud =
        crate::storage::cloud::CloudStorage::new(inner.clone(), "midge".to_string());
    let old_format = b"midge-format-version=3\n".to_vec();
    Engine::blocking_cloud_put(&original_cloud, "metadata/FORMAT", old_format.clone())
        .expect("upload prior FORMAT marker");
    inner.clear_history();
    let cloud = crate::storage::cloud::CloudStorage::new(
        Arc::new(FormatPutFailBackend {
            inner: inner.clone(),
        }),
        "midge".to_string(),
    );

    // Act
    Engine::mirror_cloud_metadata(&cloud, temp_dir.path(), RecoveryPolicy::Salvage)
        .expect("salvage open tolerates failed FORMAT mirror");

    // Assert
    assert_eq!(
        Engine::blocking_cloud_get(&original_cloud, "metadata/FORMAT")
            .expect("read old FORMAT marker"),
        old_format
    );
    assert!(
        inner.get_uploads().is_empty(),
        "a failed FORMAT put must block every newer manifest object"
    );
}

struct FormatPutFailBackend {
    inner: Arc<crate::storage::cloud::MockCloudBackend>,
}

impl crate::storage::cloud::CloudBackend for FormatPutFailBackend {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_get, submit_get_with_metadata, submit_get_range, submit_get_range_with_identity, submit_delete, submit_list, submit_head);

    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        if key.ends_with("/metadata/FORMAT") {
            let _ = callback.send(crate::storage::cloud::CloudEvent::Put {
                key: key.to_string(),
                result: crate::storage::cloud::CloudOutcome::Err(
                    crate::storage::cloud::CloudError::Transport(
                        "injected FORMAT put failure".into(),
                    ),
                ),
            });
            return;
        }
        self.inner.submit_put(key, data, headers, callback);
    }
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
    full_reads: Arc<std::sync::atomic::AtomicU64>,
}

impl ListOmittingCloudBackend {
    fn new(
        inner: Arc<crate::storage::cloud::MockCloudBackend>,
        omitted_prefix: impl Into<String>,
    ) -> Self {
        Self {
            inner,
            omitted_prefix: omitted_prefix.into(),
            full_reads: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }
}

impl crate::storage::cloud::CloudBackend for ListOmittingCloudBackend {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_put, submit_get_range, submit_get_range_with_identity, submit_delete);

    fn submit_get(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        self.full_reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.submit_get(key, callback);
    }

    fn submit_get_with_metadata(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        self.full_reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.submit_get_with_metadata(key, callback);
    }

    fn submit_list(&self, prefix: &str, callback: crate::storage::cloud::CloudCallback) {
        if prefix.ends_with(&self.omitted_prefix) {
            let _ = callback.send(crate::storage::cloud::CloudEvent::List {
                prefix: prefix.to_string(),
                result: crate::storage::cloud::CloudOutcome::Ok(Vec::new()),
            });
            return;
        }
        self.inner.submit_list(prefix, callback);
    }

    crate::storage::cloud::forward_cloud_backend!(inner; submit_head);
}

fn test_sst_bytes_with_key_value(key: &[u8], value: &[u8]) -> Vec<u8> {
    use crate::sst::traits::SstFactory;

    let factory = crate::sst::FsSstFactoryIo::new(Arc::new(crate::io::MockFs::new()), 4096);
    let mut writer = factory.create().expect("create test sst writer");
    writer
        .add_with_meta(key, Some(value), 1, EntryType::Put, None)
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
fn should_leave_manifest_sst_remote_when_recovery_only_checks_cloud_metadata() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let mut state = crate::runtime::RuntimeState::try_new(
        temp_dir.path().to_path_buf(),
        false,
        RecoveryPolicy::Strict,
    )
    .expect("create runtime state");
    let sst_name = crate::cloud_layout::file_name(0, 0, 42);
    let sst_bytes = test_sst_bytes();
    state.manifest.files.push(crate::metadata::FileMeta {
        name: sst_name.clone(),
        size_bytes: sst_bytes.len() as u64,
        content_crc32c: Some(crc32c::crc32c(&sst_bytes)),
        ..Default::default()
    });
    let backend = Arc::new(ListOmittingCloudBackend::new(
        Arc::new(crate::storage::cloud::MockCloudBackend::new()),
        "sst/",
    ));
    let full_reads = Arc::clone(&backend.full_reads);
    let cloud = crate::storage::cloud::CloudStorage::new(backend, "midge".to_string());
    Engine::blocking_cloud_put(
        &cloud,
        &crate::cloud_layout::object_key(&sst_name),
        sst_bytes,
    )
    .expect("upload test sst");

    // Act
    Engine::ensure_local_sst_cache_from_cloud_storage(&mut state, &cloud)
        .expect("validate cloud SST inventory");
    let intent_proofs = Engine::cloud_recovery_sst_proofs_for_intent_replay(&state);

    // Assert
    assert!(
        !state.sst_dir.join(&sst_name).exists(),
        "startup must leave ordinary manifest SSTs in object storage"
    );
    assert!(
        intent_proofs.is_empty(),
        "ordinary manifest SSTs must not enter intent recovery staging"
    );
    assert_eq!(state.manifest.files.len(), 1);
    assert_eq!(full_reads.load(std::sync::atomic::Ordering::Relaxed), 0);
}

#[test]
fn should_exclude_unrelated_manifest_inventory_when_staging_interrupted_publications() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let mut state = crate::runtime::RuntimeState::try_new(
        temp_dir.path().to_path_buf(),
        false,
        RecoveryPolicy::Strict,
    )
    .expect("create runtime state");
    state.manifest.files.push(crate::metadata::FileMeta {
        name: crate::cloud_layout::file_name(0, 0, 41),
        size_bytes: 1 << 40,
        ..Default::default()
    });
    let interrupted = crate::runtime::FileMeta {
        name: crate::cloud_layout::file_name(0, 0, 42),
        level: 0,
        size_bytes: 4096,
        content_crc32c: None,
        cf_id: 0,
        smallest_key: None,
        largest_key: None,
        smallest_seq: None,
        largest_seq: None,
        key_bounds_complete: false,
    };
    state
        .intent_log
        .push(crate::runtime::IntentLogEntry::FlushPublish {
            phase: crate::runtime::PublicationPhase::ManifestPublished,
            file_meta: interrupted.clone(),
            cf_id: 0,
            sequence: 1,
        });

    // Act
    let proofs = Engine::cloud_recovery_sst_proofs_for_intent_replay(&state);

    // Assert
    assert_eq!(
        proofs,
        vec![CloudSstRecoveryProof::from_runtime(&interrupted)]
    );
}

#[test]
fn should_validate_remote_only_manifest_sst_when_cloud_listing_is_stale() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let mut state = crate::runtime::RuntimeState::try_new(
        temp_dir.path().to_path_buf(),
        false,
        RecoveryPolicy::Strict,
    )
    .expect("create runtime state");
    let sst_name = crate::cloud_layout::file_name(0, 0, 1);
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
    Engine::blocking_cloud_put(
        &cloud,
        &crate::cloud_layout::object_key(&sst_name),
        sst_bytes,
    )
    .expect("upload test sst");

    Engine::ensure_local_sst_cache_from_cloud_storage(&mut state, &cloud)
        .expect("stale list should not make readable manifest SST unrecoverable");

    // Act
    // Assert
    assert!(
        !state.sst_dir.join(&sst_name).exists(),
        "readable cloud SST must stay remote despite stale LIST"
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
    let sst_name = crate::cloud_layout::file_name(0, 0, 3);
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
    Engine::blocking_cloud_put(
        &cloud,
        &crate::cloud_layout::object_key(&sst_name),
        wrong_sst_bytes,
    )
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
fn should_defer_manifest_sst_body_checksum_until_blocks_are_read() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let mut state = crate::runtime::RuntimeState::try_new(
        temp_dir.path().to_path_buf(),
        false,
        RecoveryPolicy::Strict,
    )
    .expect("create runtime state");
    let sst_name = crate::cloud_layout::file_name(0, 0, 4);
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
    Engine::blocking_cloud_put(
        &cloud,
        &crate::cloud_layout::object_key(&sst_name),
        wrong_sst_bytes,
    )
    .expect("upload same-sized but wrong-content test sst");

    Engine::ensure_local_sst_cache_from_cloud_storage(&mut state, &cloud)
        .expect("startup validates object metadata without scanning the SST body");

    // Act
    // Assert
    assert!(
        !state.sst_dir.join(&sst_name).exists(),
        "startup must not install a full SST in the local cache"
    );
}

#[test]
fn should_leave_stale_local_sst_cache_untouched_when_cloud_metadata_is_valid() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let mut state = crate::runtime::RuntimeState::try_new(
        temp_dir.path().to_path_buf(),
        false,
        RecoveryPolicy::Strict,
    )
    .expect("create runtime state");
    let sst_name = crate::cloud_layout::file_name(0, 0, 5);
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
    std::fs::write(state.sst_dir.join(&sst_name), &stale_local_sst_bytes)
        .expect("write stale local SST cache");
    let cloud = cloud_with_stale_sst_listing();
    Engine::blocking_cloud_put(
        &cloud,
        &crate::cloud_layout::object_key(&sst_name),
        committed_sst_bytes.clone(),
    )
    .expect("upload authoritative manifest-sized test sst");

    Engine::ensure_local_sst_cache_from_cloud_storage(&mut state, &cloud)
        .expect("valid cloud metadata should permit opening without local cache restoration");

    assert_eq!(
        std::fs::read(state.sst_dir.join(&sst_name)).expect("read retained local SST"),
        stale_local_sst_bytes,
        "startup leaves disposable local bytes untouched; readers use authoritative cloud objects"
    );
}

#[test]
fn should_avoid_reading_same_size_local_sst_cache_when_cloud_metadata_is_valid() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let mut state = crate::runtime::RuntimeState::try_new(
        temp_dir.path().to_path_buf(),
        false,
        RecoveryPolicy::Strict,
    )
    .expect("create runtime state");
    let sst_name = crate::cloud_layout::file_name(0, 0, 6);
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
    std::fs::write(state.sst_dir.join(&sst_name), &stale_local_sst_bytes)
        .expect("write stale same-size local SST cache");
    let cloud = cloud_with_stale_sst_listing();
    Engine::blocking_cloud_put(
        &cloud,
        &crate::cloud_layout::object_key(&sst_name),
        committed_sst_bytes.clone(),
    )
    .expect("upload authoritative manifest-crc test sst");

    Engine::ensure_local_sst_cache_from_cloud_storage(&mut state, &cloud)
        .expect("cloud metadata permits opening without scanning stale local cache bytes");

    // Act
    // Assert
    assert_eq!(
        std::fs::read(state.sst_dir.join(&sst_name)).expect("read retained local SST"),
        stale_local_sst_bytes,
        "ordinary startup does not scan or replace disposable local SST bytes"
    );
}

#[test]
fn should_salvage_retain_verified_local_sst_when_cloud_object_is_missing() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let mut state = crate::runtime::RuntimeState::try_new(
        temp_dir.path().to_path_buf(),
        false,
        RecoveryPolicy::Salvage,
    )
    .expect("create runtime state");
    let sst_name = crate::cloud_layout::file_name(0, 0, 7);
    let committed_sst_bytes = test_sst_bytes();
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
    assert!(state.salvaged_local_ssts.contains(&sst_name));
}

#[test]
fn should_reject_legacy_local_sst_with_corrupt_data_blocks_when_cloud_object_is_missing() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("create recovery directory");
    let mut state = crate::runtime::RuntimeState::try_new(
        temp_dir.path().to_path_buf(),
        false,
        RecoveryPolicy::Salvage,
    )
    .expect("create runtime state");
    let name = crate::cloud_layout::file_name(0, 0, 8);
    let corrupt_bytes = same_size_sst_with_different_crc(&test_sst_bytes());
    state.manifest.files.push(crate::metadata::FileMeta {
        name: name.clone(),
        size_bytes: corrupt_bytes.len() as u64,
        content_crc32c: None,
        ..Default::default()
    });
    std::fs::write(state.sst_dir.join(&name), corrupt_bytes).expect("persist damaged local SST");
    let cloud = cloud_with_stale_sst_listing();

    // Act
    Engine::ensure_local_sst_cache_from_cloud_storage(&mut state, &cloud)
        .expect("salvage missing cloud object");

    // Assert
    assert!(state.salvaged_local_ssts.is_empty());
    assert!(state.manifest.files.is_empty());
    assert!(state.persistence_anomaly_detected());
    assert!(
        state.sst_dir.join(name).exists(),
        "unverified bytes remain available for inspection"
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
    let sst_name = crate::cloud_layout::file_name(0, 0, 2);
    let sst_bytes = test_sst_bytes();
    let cloud = cloud_with_stale_sst_listing();
    Engine::blocking_cloud_put(
        &cloud,
        &crate::cloud_layout::object_key(&sst_name),
        sst_bytes,
    )
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
    let sst_name = crate::cloud_layout::file_name(0, 0, 7);
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
                key_bounds_complete: true,
            },
        });
    let cloud = cloud_with_stale_sst_listing();
    Engine::blocking_cloud_put(
        &cloud,
        &crate::cloud_layout::object_key(&sst_name),
        sst_bytes,
    )
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

#[derive(Default)]
struct FailingReleaseLease {
    release_attempts: std::sync::atomic::AtomicUsize,
}

impl crate::lease::PrimaryLease for FailingReleaseLease {
    fn try_acquire(self: Arc<Self>) -> Result<crate::lease::LeaseGuard, crate::lease::LeaseError> {
        Ok(crate::lease::LeaseGuard::token())
    }

    fn renew(&self) -> Result<(), crate::lease::LeaseError> {
        Ok(())
    }

    fn release(&self) -> Result<(), crate::lease::LeaseError> {
        self.release_attempts
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(crate::lease::LeaseError::IoError(
            "lease store is read-only".to_string(),
        ))
    }

    fn ttl(&self) -> Duration {
        Duration::from_millis(300)
    }

    fn holder_id(&self) -> String {
        "failing-release-test".to_string()
    }

    fn epoch(&self) -> u64 {
        1
    }
}

#[test]
fn should_report_error_when_lease_release_keeps_failing() {
    // Arrange: after the heartbeat stops, an unreleased lease simply expires
    // at its TTL, so retrying past that only spins and floods the log.
    let lease = Arc::new(FailingReleaseLease::default());
    let engine_lease: Arc<dyn crate::lease::PrimaryLease> = lease.clone();
    let started = std::time::Instant::now();

    // Act
    let result = super::LeaseState::release_fencing_parts(None, Some(engine_lease), None);

    // Assert
    assert!(
        result.is_err(),
        "a release that never succeeds must be reported"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "release retries must stop near the lease TTL, took {:?}",
        started.elapsed()
    );
    let attempts = lease
        .release_attempts
        .load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        (2..=64).contains(&attempts),
        "retries must back off, not spin: {attempts} attempts"
    );
}

mod salvage_removes_definitively_lost_ssts {
    use super::*;

    /// A salvage-mode runtime whose on-disk manifest authority lists one SST.
    fn salvage_state_with_persisted_sst(
        sst_seq: u64,
        size_bytes: u64,
    ) -> (tempfile::TempDir, crate::runtime::RuntimeState, String) {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let mut state = crate::runtime::RuntimeState::try_new(
            temp_dir.path().to_path_buf(),
            false,
            RecoveryPolicy::Salvage,
        )
        .expect("create runtime state");
        let sst_name = crate::cloud_layout::file_name(0, 0, sst_seq);
        state.manifest.files.push(crate::metadata::FileMeta {
            name: sst_name.clone(),
            level: 0,
            size_bytes,
            cf_id: 0,
            sst_seq,
            ..Default::default()
        });
        crate::metadata::ManifestPersistence::save_snapshot_and_truncate_journal(
            &state.db_path,
            &state.manifest,
        )
        .expect("persist the manifest that lists the SST");
        (temp_dir, state, sst_name)
    }

    fn persisted_names(state: &crate::runtime::RuntimeState) -> Vec<String> {
        crate::metadata::ManifestPersistence::load(&state.db_path)
            .expect("load persisted manifest")
            .files
            .into_iter()
            .map(|file| file.name)
            .collect()
    }

    #[test]
    fn should_remove_the_manifest_entry_durably_when_salvage_finds_the_sst_missing_from_cloud() {
        // Arrange
        let (_temp, mut state, sst_name) = salvage_state_with_persisted_sst(6, 128);
        let cloud = cloud_with_stale_sst_listing();

        // Act
        Engine::ensure_local_sst_cache_from_cloud_storage(&mut state, &cloud)
            .expect("salvage tolerates a missing authoritative SST");

        // Assert
        assert!(state
            .manifest
            .files
            .iter()
            .all(|file| file.name != sst_name));
        assert!(
            !persisted_names(&state).contains(&sst_name),
            "the removal must survive a restart, not be reverted by the next snapshot"
        );
    }

    #[test]
    fn should_remove_the_manifest_entry_durably_when_salvage_finds_the_sst_wrongly_sized() {
        // Arrange
        let bytes = test_sst_bytes();
        let (_temp, mut state, sst_name) =
            salvage_state_with_persisted_sst(7, bytes.len() as u64 + 1);
        let cloud = cloud_with_stale_sst_listing();
        Engine::blocking_cloud_put(&cloud, &crate::cloud_layout::object_key(&sst_name), bytes)
            .expect("upload wrongly sized SST");

        // Act
        Engine::ensure_local_sst_cache_from_cloud_storage(&mut state, &cloud)
            .expect("salvage tolerates a wrongly sized authoritative SST");

        // Assert
        assert!(state
            .manifest
            .files
            .iter()
            .all(|file| file.name != sst_name));
        assert!(!persisted_names(&state).contains(&sst_name));
    }

    #[test]
    fn should_not_make_a_salvage_drop_durable_when_the_cloud_check_itself_fails() {
        // Arrange: `sst` is a file, so stat-ing an SST beneath it fails with an
        // error that is not NotFound. Nothing is known about the object, so it
        // must not be erased from the durable manifest.
        let (_temp, mut state, sst_name) = salvage_state_with_persisted_sst(9, 128);
        let cloud_root = tempfile::tempdir().expect("create cloud root");
        std::fs::write(cloud_root.path().join("sst"), b"not a directory")
            .expect("create a file where the SST directory should be");

        // Act
        super::startup::CloudStartupRecovery::ensure_local_sst_cache_from_cloud(
            &mut state,
            cloud_root.path(),
        )
        .expect("salvage tolerates an unverifiable authoritative SST");

        // Assert
        assert!(
            persisted_names(&state).contains(&sst_name),
            "an indeterminate failure must not erase the durable manifest entry"
        );
        assert!(
            state
                .manifest
                .files
                .iter()
                .any(|file| file.name == sst_name),
            "an indeterminate failure must remain in the running manifest"
        );
    }

    #[cfg(unix)]
    #[test]
    fn should_retain_indeterminate_sst_during_mixed_salvage() {
        // Arrange: one manifest SST is absent while a self-referential symlink
        // makes the other metadata check fail indeterminately.
        let (_temp, mut state, missing_name) = salvage_state_with_persisted_sst(10, 128);
        let indeterminate_name = crate::cloud_layout::file_name(0, 0, 11);
        state.manifest.files.push(crate::metadata::FileMeta {
            name: indeterminate_name.clone(),
            level: 0,
            size_bytes: 128,
            cf_id: 0,
            sst_seq: 11,
            ..Default::default()
        });
        crate::metadata::ManifestPersistence::save_snapshot_and_truncate_journal(
            &state.db_path,
            &state.manifest,
        )
        .expect("persist both manifest SSTs");
        let cloud_root = tempfile::tempdir().expect("create cloud root");
        let cloud_sst_dir = cloud_root.path().join("sst");
        std::fs::create_dir(&cloud_sst_dir).expect("create cloud SST directory");
        std::os::unix::fs::symlink(&indeterminate_name, cloud_sst_dir.join(&indeterminate_name))
            .expect("create indeterminate SST metadata path");

        // Act
        super::startup::CloudStartupRecovery::ensure_local_sst_cache_from_cloud(
            &mut state,
            cloud_root.path(),
        )
        .expect("salvage tolerates mixed definitive and indeterminate losses");
        crate::metadata::ManifestPersistence::save(&state.db_path, &state.manifest)
            .expect("persist the running manifest again");

        // Assert
        assert!(!persisted_names(&state).contains(&missing_name));
        assert!(persisted_names(&state).contains(&indeterminate_name));
        assert!(state
            .manifest
            .files
            .iter()
            .any(|file| file.name == indeterminate_name));
    }

    #[test]
    fn should_keep_the_manifest_entry_when_the_cloud_sst_is_valid() {
        // Arrange
        let bytes = test_sst_bytes();
        let (_temp, mut state, sst_name) = salvage_state_with_persisted_sst(8, bytes.len() as u64);
        let cloud = cloud_with_stale_sst_listing();
        Engine::blocking_cloud_put(&cloud, &crate::cloud_layout::object_key(&sst_name), bytes)
            .expect("upload SST");

        // Act
        Engine::ensure_local_sst_cache_from_cloud_storage(&mut state, &cloud)
            .expect("a valid SST is retained");

        // Assert
        assert!(state
            .manifest
            .files
            .iter()
            .any(|file| file.name == sst_name));
        assert!(persisted_names(&state).contains(&sst_name));
    }
}
