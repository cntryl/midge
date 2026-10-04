//! Public flush barriers over a genuinely held cloud publication worker.

use super::*;
use crate::lease::{CreatedLease, LeaseHeartbeat};
use crate::runtime::hybrid_persistence::CloudPersistence;
use crate::runtime::{RuntimeConfig, RuntimeState};
use crate::storage::{
    StorageBackend, StorageCallback, StorageEvent, StorageOutcome, StorageRequest,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::Instant;

const CALLER_WAIT: Duration = Duration::from_millis(200);
const WORKER_WAIT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
struct Publication {
    key: String,
    len: u64,
    crc32c: u32,
    if_absent: bool,
    started: Instant,
    completed: bool,
}

#[derive(Default)]
struct PublicationState {
    requests: Vec<Publication>,
    released: bool,
}

#[derive(Default)]
struct PublicationGate {
    state: Mutex<PublicationState>,
    changed: Condvar,
}

impl PublicationGate {
    fn lock(&self) -> MutexGuard<'_, PublicationState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn enter(&self, publication: Publication, timeout: Duration) -> Option<usize> {
        let mut state = self.lock();
        let index = state.requests.len();
        state.requests.push(publication);
        self.changed.notify_all();
        if index == 0 {
            let (observed, _) = self
                .changed
                .wait_timeout_while(state, timeout, |state| !state.released)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state = observed;
            if !state.released {
                return None;
            }
        }
        Some(index)
    }

    fn release(&self) {
        self.lock().released = true;
        self.changed.notify_all();
    }

    fn wait_for_start(&self) -> bool {
        let (state, _) = self
            .changed
            .wait_timeout_while(self.lock(), WORKER_WAIT, |state| state.requests.is_empty())
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        !state.requests.is_empty()
    }

    fn publications(&self) -> Vec<Publication> {
        self.lock().requests.clone()
    }
}

struct HeldSstStore {
    inner: crate::storage::filesystem::FileSystem,
    gate: Arc<PublicationGate>,
}

impl StorageBackend for HeldSstStore {
    crate::storage::forward_storage_backend!(inner; submit_delete_request, submit_head_request, submit_range_head_request, submit_range_read_request, submit_metadata_read_request);

    fn submit_write_request(
        &self,
        request: StorageRequest,
        data: Vec<u8>,
        callback: StorageCallback,
    ) {
        let publication = Publication {
            key: request.key.clone(),
            len: u64::try_from(data.len()).expect("fixture SST length fits u64"),
            crc32c: crc32c::crc32c(&data),
            if_absent: matches!(
                &request.precondition,
                crate::storage::StoragePrecondition::IfAbsent
            ),
            started: Instant::now(),
            completed: false,
        };
        let Some(index) = self.gate.enter(publication, request.remaining_timeout()) else {
            let _ = callback.send(StorageEvent::WriteComplete {
                key: request.key,
                result: StorageOutcome::Err(crate::storage::StorageError::timeout(
                    "held fixture publication did not receive its release",
                )),
            });
            return;
        };
        let (sender, receiver) = std::sync::mpsc::channel();
        self.inner.submit_write_request(request, data, sender);
        if let Ok(event) = receiver.recv_timeout(WORKER_WAIT) {
            if matches!(
                &event,
                StorageEvent::WriteComplete {
                    result: StorageOutcome::Ok(()),
                    ..
                }
            ) {
                self.gate.lock().requests[index].completed = true;
            }
            let _ = callback.send(event);
        }
    }
}

struct RetryFixture {
    engine: Engine,
    options: OpenOptions,
    directory: tempfile::TempDir,
    gate: Arc<PublicationGate>,
}

impl RetryFixture {
    fn new() -> MidgeResult<Self> {
        let directory = tempfile::tempdir()?;
        let options = OpenOptions::cloud_simulated(directory.path(), "flush-retry", "db")
            .background_compaction(false)
            .storage_io_timeout(WORKER_WAIT)
            .runtime_response_timeout(WORKER_WAIT * 2)
            .build()?;
        let gate = Arc::new(PublicationGate::default());
        let (storage, events) = fixture_storage(directory.path(), &gate)?;
        let state = RuntimeState::try_new(
            directory.path().to_path_buf(),
            false,
            options.recovery_policy(),
        )?;
        let created = crate::lease::create_lease_with_validity_and_timeout_and_ttl(
            options.storage(),
            options.lease_clock_skew_tolerance(),
            options.storage_io_timeout(),
            options.lease_ttl(),
        )?;
        let guard = Arc::clone(&created.lease).try_acquire()?;
        let healthy = Arc::new(AtomicBool::new(true));
        install_write_authority(&storage, &created, &healthy)?;
        CloudPersistence::new(Arc::clone(&storage))
            .fence_cloud_wal_catalog(created.lease.epoch())?;
        let mut config = fixture_config(&options, &created, &healthy, &storage, events);
        config.sst_read_fs = Some(Arc::new(crate::storage::remote_sst::RemoteSstFs::new(
            Arc::new(
                crate::io::RealFs::new(directory.path()).map_err(crate::io::FsError::into_midge)?,
            ),
            storage.remote_sst_backend(),
            options.storage_io_timeout(),
        )));
        let (runtime, _) = Runtime::new();
        let (runtime, handle) = runtime.start_with_config(state, config)?;
        let mut heartbeat = LeaseHeartbeat::new_with_healthy_and_validity(
            Arc::clone(&created.lease),
            healthy,
            created.validity,
        );
        heartbeat.start();
        let engine = fixture_engine(
            &options,
            directory.path(),
            runtime,
            handle,
            LeaseState::new(created.lease, guard, heartbeat, Some(storage)),
        );
        Ok(Self {
            engine,
            options,
            directory,
            gate,
        })
    }

    fn manifest(&self) -> MidgeResult<crate::metadata::Manifest> {
        crate::metadata::ManifestPersistence::load(self.directory.path())
            .map_err(MidgeError::Internal)
    }
}

impl Drop for RetryFixture {
    fn drop(&mut self) {
        // A failed assertion must not strand the real flush worker in Drop.
        self.gate.release();
    }
}

fn fixture_storage(
    directory: &std::path::Path,
    gate: &Arc<PublicationGate>,
) -> MidgeResult<(
    Arc<crate::storage::HybridStorage>,
    crossbeam::channel::Receiver<StorageEvent>,
)> {
    let cloud_root = crate::storage::simulated::simulated_cloud_root(directory);
    let local = Arc::new(crate::storage::filesystem::FileSystem::new(
        directory.join("hybrid_local"),
    )?);
    let cloud = Arc::new(crate::storage::filesystem::FileSystem::new(
        cloud_root.clone(),
    )?);
    let sst = Arc::new(HeldSstStore {
        inner: crate::storage::filesystem::FileSystem::new(cloud_root)?,
        gate: Arc::clone(gate),
    });
    let (sender, events) = crossbeam::channel::bounded(
        crate::storage::hybrid::backend::HYBRID_STORAGE_EVENT_CHANNEL_CAPACITY,
    );
    let storage = Arc::new(
        crate::storage::HybridStorage::new_with_class_stores_and_event_sender(
            local,
            cloud.clone(),
            sst,
            cloud,
            sender,
            WORKER_WAIT,
        ),
    );
    storage.enable_ephemeral_sst_cache(64 * 1024 * 1024);
    Ok((storage, events))
}

fn install_write_authority(
    storage: &crate::storage::HybridStorage,
    created: &CreatedLease,
    healthy: &Arc<AtomicBool>,
) -> MidgeResult<()> {
    let validity = created.validity.clone();
    let healthy = Arc::clone(healthy);
    let epoch = created.lease.epoch();
    storage.configure_write_authority(Arc::new(move || {
        if !healthy.load(Ordering::Acquire) {
            return Err(MidgeError::Fenced(
                "fixture lease heartbeat is unhealthy".into(),
            ));
        }
        if let Some(validity) = &validity {
            validity
                .remaining(epoch)
                .map_err(|error| error.into_validation_error("fixture storage authority"))?;
        }
        Ok(())
    }))
}

fn fixture_config(
    options: &OpenOptions,
    created: &CreatedLease,
    healthy: &Arc<AtomicBool>,
    storage: &Arc<crate::storage::HybridStorage>,
    events: crossbeam::channel::Receiver<StorageEvent>,
) -> RuntimeConfig {
    storage.configure_maintenance_memory(options.compaction_memory_pool_size());
    RuntimeConfig {
        ttl_clock: options.ttl_clock(),
        wal_durability_policy: crate::wal::DurabilityPolicy::CloudAsync,
        storage_io_timeout: options.storage_io_timeout(),
        runtime_response_timeout: options.runtime_response_timeout(),
        hybrid_storage: Some(Arc::clone(storage)),
        hybrid_storage_events: Some(events),
        compression_policy: options.compression_policy().clone(),
        block_cache_size: options.block_cache_size(),
        block_cache_policy: options.block_cache_policy_type(),
        target_sst_size: options.target_sst_size(),
        compaction_memory_limit: options.compaction_memory_pool_size(),
        flush_memory_limit: options.flush_memory_limit(),
        background_compaction: false,
        writer_epoch: created.lease.epoch(),
        lease_healthy: Some(Arc::clone(healthy)),
        lease_validity: created.validity.clone(),
        leader_store: created.lease.get_leader_store(),
        leader_holder_id: Some(created.lease.holder_id()),
        ..RuntimeConfig::default()
    }
}

fn fixture_engine(
    options: &OpenOptions,
    directory: &std::path::Path,
    runtime: Runtime,
    runtime_handle: RuntimeHandle,
    lease_state: LeaseState,
) -> Engine {
    let column_families = dashmap::DashMap::new();
    column_families.insert(0, ColumnFamilyHandle::new(0, "default".into()));
    let ingest_coordinators = dashmap::DashMap::new();
    ingest_coordinators.insert(0, Arc::new(ingest::IngestCoordinator::new(0)));
    Engine {
        runtime: Some(runtime),
        runtime_handle,
        db_path: directory.to_path_buf(),
        memory_mode: false,
        cloud_mode: true,
        simulated_cloud_mode: true,
        sequence: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        next_snapshot_id: std::sync::atomic::AtomicU64::new(1),
        column_families,
        lease_state,
        ingest_coordinators,
        transaction_memory_pool: Arc::new(
            crate::runtime::transaction_spill::TransactionMemoryPool::new(
                options.transaction_memory_pool_size(),
            ),
        ),
        ttl_clock: options.ttl_clock(),
    }
}

fn acknowledge_rows(engine: &Engine, cf: &ColumnFamilyHandle) -> MidgeResult<()> {
    let mut write = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    for index in 0..8 {
        write.put(
            format!("key-{index:02}").into_bytes(),
            format!("value-{index:02}").into_bytes(),
            None,
        )?;
    }
    write.commit(WriteOptions::cloud_async())?;
    Ok(())
}

fn assert_acknowledged_rows(engine: &Engine, cf: &ColumnFamilyHandle) -> MidgeResult<()> {
    let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
    for index in 0..8 {
        assert_eq!(
            read.get(format!("key-{index:02}").as_bytes())?,
            Some(bytes::Bytes::from(format!("value-{index:02}").into_bytes()))
        );
    }
    assert_eq!(read.get(b"not-acknowledged")?, None);
    Ok(())
}

fn abandon_accepted_flush(fixture: &RetryFixture, cf: &ColumnFamilyHandle) {
    let result = fixture.engine.flush_cf_with_timeout(cf, CALLER_WAIT);
    let returned = Instant::now();
    assert!(
        fixture.gate.wait_for_start(),
        "real SST publication never entered"
    );
    let publications = fixture.gate.publications();
    assert!(
        publications[0].started <= returned,
        "caller expired before publication acceptance"
    );
    assert!(
        !publications[0].completed,
        "held publication completed without release"
    );
    assert!(matches!(result, Err(MidgeError::Timeout(_))), "{result:?}");
}

fn assert_one_publication(fixture: &RetryFixture) -> MidgeResult<()> {
    let publications = fixture.gate.publications();
    assert_eq!(
        publications.len(),
        1,
        "barrier retry duplicated SST publication"
    );
    let publication = &publications[0];
    assert!(publication.completed);
    assert!(
        publication.if_absent,
        "immutable publication lost create-only identity"
    );
    let manifest = fixture.manifest()?;
    assert_eq!(
        manifest.files.len(),
        1,
        "barrier retry duplicated immutable ownership"
    );
    let file = &manifest.files[0];
    assert_eq!(publication.key, crate::cloud_layout::object_key(&file.name));
    assert_eq!(publication.len, file.size_bytes);
    assert_eq!(Some(publication.crc32c), file.content_crc32c);
    let bytes = std::fs::read(
        crate::storage::simulated::simulated_cloud_root(fixture.directory.path())
            .join(&publication.key),
    )?;
    assert_eq!(u64::try_from(bytes.len()).unwrap(), publication.len);
    assert_eq!(crc32c::crc32c(&bytes), publication.crc32c);
    Ok(())
}

#[test]
fn should_complete_one_cloud_flush_when_abandoned_barrier_callers_retry() -> MidgeResult<()> {
    // Arrange: the actual worker holds its first immutable conditional write;
    // ordinary CloudAsync commits and the primary heartbeat remain healthy.
    let mut fixture = RetryFixture::new()?;
    let cf = fixture.engine.get_column_family("default").unwrap();
    acknowledge_rows(&fixture.engine, &cf)?;

    // Act: abandoning two response routes does not cancel accepted ownership.
    abandon_accepted_flush(&fixture, &cf);
    let second = fixture.engine.flush_cf_with_timeout(&cf, CALLER_WAIT);
    assert!(matches!(second, Err(MidgeError::Timeout(_))), "{second:?}");
    assert!(fixture.engine.is_primary_lease_healthy());
    assert_eq!(fixture.gate.publications().len(), 1);
    assert!(fixture.manifest()?.files.is_empty());
    assert_acknowledged_rows(&fixture.engine, &cf)?;
    fixture.gate.release();
    fixture.engine.flush_cf(&cf)?;
    fixture.engine.flush_cf(&cf)?;

    // Assert: normal retries converge on the same exact durable SST and rows.
    assert_one_publication(&fixture)?;
    assert_acknowledged_rows(&fixture.engine, &cf)?;
    assert!(fixture.engine.is_primary_lease_healthy());
    fixture.engine.shutdown(WORKER_WAIT)?;
    let mut reopened = Engine::open(fixture.options.clone())?;
    let reopened_cf = reopened.get_column_family("default").unwrap();
    assert_acknowledged_rows(&reopened, &reopened_cf)?;
    reopened.flush_cf(&reopened_cf)?;
    reopened.shutdown(WORKER_WAIT)
}

fn lose_lease(engine: &Engine, expire_validity: bool) -> bool {
    let mut heartbeat = engine
        .lease_state
        .heartbeat
        .as_ref()
        .unwrap()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    heartbeat.stop();
    let was_healthy = heartbeat.is_healthy();
    if expire_validity {
        heartbeat.validity_for_test().unwrap().expire_for_test();
    } else {
        heartbeat.healthy_flag().store(false, Ordering::Release);
    }
    was_healthy
}

fn assert_terminal_flush_retry(expire_validity: bool) -> MidgeResult<()> {
    let mut fixture = RetryFixture::new()?;
    let cf = fixture.engine.get_column_family("default").unwrap();
    acknowledge_rows(&fixture.engine, &cf)?;
    abandon_accepted_flush(&fixture, &cf);
    let was_healthy = lose_lease(&fixture.engine, expire_validity);
    let (prompt, terminal, completed, held_manifest) = std::thread::scope(|scope| {
        let (sender, receiver) = std::sync::mpsc::channel();
        let engine = &fixture.engine;
        let cf = &cf;
        scope.spawn(move || {
            let _ = sender.send(engine.flush_cf(cf));
        });
        let prompt = receiver.recv_timeout(CALLER_WAIT);
        let completed = fixture.gate.publications()[0].completed;
        let held_manifest = fixture.manifest();
        // Release and join even on RED, before assertions or propagated errors.
        fixture.gate.release();
        let responded_promptly = prompt.is_ok();
        let terminal = prompt.or_else(|_| receiver.recv_timeout(WORKER_WAIT * 2));
        (responded_promptly, terminal, completed, held_manifest)
    });

    assert!(
        was_healthy,
        "stopped watchdog must leave health cached true"
    );
    assert!(
        matches!(terminal, Ok(Err(MidgeError::Fenced(_)))),
        "terminal lease loss must preserve fencing: {terminal:?}"
    );
    assert!(
        prompt,
        "normal public flush waited for held publication after terminal lease loss"
    );
    assert!(!fixture.engine.is_primary_lease_healthy());
    assert!(!completed);
    assert!(held_manifest?.files.is_empty());
    let _ = fixture.engine.shutdown(WORKER_WAIT);
    assert!(
        fixture.manifest()?.files.is_empty(),
        "lost lease installed accepted output into manifest authority"
    );
    Ok(())
}

fn assert_accepted_flush_fences_after_lease_loss(expire_validity: bool) -> MidgeResult<()> {
    let mut fixture = RetryFixture::new()?;
    let cf = fixture.engine.get_column_family("default").unwrap();
    acknowledge_rows(&fixture.engine, &cf)?;
    let (accepted, was_healthy, prompt, terminal, completed, held_manifest) =
        std::thread::scope(|scope| {
            let (sender, receiver) = std::sync::mpsc::channel();
            let engine = &fixture.engine;
            let cf = &cf;
            scope.spawn(move || {
                let _ = sender.send(engine.flush_cf(cf));
            });
            // This real write starts only after the healthy barrier is registered.
            let accepted = fixture.gate.wait_for_start();
            let was_healthy = accepted && lose_lease(&fixture.engine, expire_validity);
            let prompt = receiver.recv_timeout(CALLER_WAIT);
            let completed = fixture
                .gate
                .publications()
                .first()
                .is_some_and(|publication| publication.completed);
            let held_manifest = fixture.manifest();
            // The scoped public call always joins after release, including RED.
            fixture.gate.release();
            let responded_promptly = prompt.is_ok();
            let terminal = prompt.or_else(|_| receiver.recv_timeout(WORKER_WAIT * 2));
            (
                accepted,
                was_healthy,
                responded_promptly,
                terminal,
                completed,
                held_manifest,
            )
        });

    assert!(
        accepted,
        "healthy public flush never entered real publication"
    );
    assert!(
        was_healthy,
        "accepted barrier did not retain a healthy lease"
    );
    assert!(
        matches!(terminal, Ok(Err(MidgeError::Fenced(_)))),
        "accepted caller must preserve terminal fencing: {terminal:?}"
    );
    assert!(
        prompt,
        "already accepted public flush waited for held publication after terminal lease loss"
    );
    assert!(!fixture.engine.is_primary_lease_healthy());
    assert!(!completed);
    assert!(held_manifest?.files.is_empty());
    let _ = fixture.engine.shutdown(WORKER_WAIT);
    assert!(
        fixture.manifest()?.files.is_empty(),
        "lost lease installed accepted output into manifest authority"
    );
    Ok(())
}

fn assert_empty_flush_fences_after_lease_loss(expire_validity: bool) -> MidgeResult<()> {
    let mut fixture = RetryFixture::new()?;
    let cf = fixture.engine.get_column_family("default").unwrap();
    fixture.engine.flush_cf(&cf)?;
    assert!(fixture.engine.is_primary_lease_healthy());
    assert!(fixture.gate.publications().is_empty());
    assert!(fixture.manifest()?.files.is_empty());

    let was_healthy = lose_lease(&fixture.engine, expire_validity);
    let result = fixture.engine.flush_cf(&cf);

    assert!(
        was_healthy,
        "empty barrier must initially have a healthy lease"
    );
    assert!(
        matches!(result, Err(MidgeError::Fenced(_))),
        "empty frontier success must not hide terminal lease loss: {result:?}"
    );
    assert!(!fixture.engine.is_primary_lease_healthy());
    assert!(fixture.gate.publications().is_empty());
    assert!(fixture.manifest()?.files.is_empty());
    let _ = fixture.engine.shutdown(WORKER_WAIT);
    assert!(fixture.gate.publications().is_empty());
    assert!(fixture.manifest()?.files.is_empty());
    Ok(())
}

#[test]
fn should_fence_flush_retry_when_accepted_publication_outlives_healthy_lease() -> MidgeResult<()> {
    // Arrange: an accepted CloudAsync flush remains genuinely held.
    // Act: its heartbeat becomes terminally unhealthy before normal retry.
    // Assert: no waiter timeout, premature publication, or manifest authority.
    assert_terminal_flush_retry(false)
}

#[test]
fn should_fence_flush_retry_when_validity_expires_without_watchdog() -> MidgeResult<()> {
    // Arrange: accepted publication is held and watchdog is deliberately stopped.
    // Act: monotonic validity expires while the heartbeat health remains cached.
    // Assert: normal public retry discovers terminal expiry before waiting.
    assert_terminal_flush_retry(true)
}

#[test]
fn should_fence_accepted_flush_when_heartbeat_fails_during_publication() -> MidgeResult<()> {
    // Arrange: an ordinary public barrier enters real publication while healthy.
    // Act: its heartbeat becomes unhealthy while that accepted worker stays held.
    // Assert: the existing caller receives Fenced before release, preserving ownership.
    assert_accepted_flush_fences_after_lease_loss(false)
}

#[test]
fn should_fence_accepted_flush_when_validity_expires_without_watchdog() -> MidgeResult<()> {
    // Arrange: a healthy ordinary public barrier owns a held real publication.
    // Act: stop its watchdog, then expire the actual monotonic validity.
    // Assert: the existing caller discovers expiry without a retry or worker completion.
    assert_accepted_flush_fences_after_lease_loss(true)
}

#[test]
fn should_fence_empty_flush_when_heartbeat_is_unhealthy() -> MidgeResult<()> {
    // Arrange: a genuinely healthy empty public flush succeeds without an SST.
    // Act: stop the heartbeat and mark its shared health terminally false.
    // Assert: another empty public flush returns Fenced without publication.
    assert_empty_flush_fences_after_lease_loss(false)
}

#[test]
fn should_fence_empty_flush_when_validity_expires_without_watchdog() -> MidgeResult<()> {
    // Arrange: a healthy empty public flush succeeds without immutable work.
    // Act: stop the watchdog and expire its actual monotonic validity.
    // Assert: empty-frontier success cannot bypass fencing or create an SST.
    assert_empty_flush_fences_after_lease_loss(true)
}
