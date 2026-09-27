//! Persistent, lease-fenced memtable flush worker.
//!
//! The runtime thread owns freeze/install decisions. This actor owns one
//! bounded worker thread that performs only immutable build/publication I/O and
//! returns deltas for the runtime to validate and install.

use crate::common::{MidgeError, MidgeResult};
use crate::io::FsError;
use crate::sst::SstFactory;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Instant;

const FLUSH_WORKER_CHANNEL_CAPACITY: usize = 1;
#[cfg(test)]
const DEFAULT_FLUSH_MEMORY_BYTES: usize = 256 * 1024 * 1024;

#[path = "flush/build.rs"]
mod build;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FlushIdentity {
    pub flush_id: u64,
    pub writer_epoch: u64,
    pub cf_id: crate::types::ColumnFamilyId,
    pub sequence: u64,
}

#[derive(Clone)]
pub(crate) struct FlushBuildOutput {
    pub identity: FlushIdentity,
    pub staging_path: PathBuf,
    pub file_meta: crate::runtime::FileMeta,
    pub reservation: Option<crate::storage::hybrid::actor::StorageReservationToken>,
}

/// Storage capabilities needed to publish a flush. The worker depends on
/// this small contract so publication tests can supply provider and filesystem
/// doubles without constructing the hybrid-storage coordinator.
pub(crate) trait FlushStorage: Send + Sync {
    fn has_hybrid_storage(&self) -> bool;

    fn publish_sst(
        &self,
        key: &str,
        path: &Path,
        size_bytes: u64,
        checksum: u32,
        budget: &crate::common::resource_budget::ResourceBudget,
    ) -> MidgeResult<()>;

    fn mirror_control_metadata(
        &self,
        fs: &dyn crate::io::Fs,
        publication_lock: &crate::runtime::MetadataPublicationLock,
        manifest_sequence: u64,
        deadline: &crate::common::OperationDeadline,
        validate_lease: &mut dyn FnMut(&crate::common::OperationDeadline) -> MidgeResult<()>,
    ) -> MidgeResult<bool>;
}

#[derive(Clone, Default)]
pub(crate) struct HybridFlushStorage {
    hybrid: Option<Arc<crate::storage::HybridStorage>>,
    cloud_metadata: Option<Arc<crate::storage::cloud::CloudStorage>>,
}

impl HybridFlushStorage {
    pub(crate) fn new(
        hybrid: Option<Arc<crate::storage::HybridStorage>>,
        cloud_metadata: Option<Arc<crate::storage::cloud::CloudStorage>>,
    ) -> Self {
        Self {
            hybrid,
            cloud_metadata,
        }
    }
}

impl FlushStorage for HybridFlushStorage {
    fn has_hybrid_storage(&self) -> bool {
        self.hybrid.is_some()
    }

    fn publish_sst(
        &self,
        key: &str,
        path: &Path,
        size_bytes: u64,
        checksum: u32,
        budget: &crate::common::resource_budget::ResourceBudget,
    ) -> MidgeResult<()> {
        if let Some(hybrid) = &self.hybrid {
            hybrid.publish_immutable_file(key, path, size_bytes, checksum, budget)?;
        }
        Ok(())
    }

    fn mirror_control_metadata(
        &self,
        fs: &dyn crate::io::Fs,
        publication_lock: &crate::runtime::MetadataPublicationLock,
        manifest_sequence: u64,
        deadline: &crate::common::OperationDeadline,
        validate_lease: &mut dyn FnMut(&crate::common::OperationDeadline) -> MidgeResult<()>,
    ) -> MidgeResult<bool> {
        let Some(cloud) = &self.cloud_metadata else {
            return Ok(false);
        };
        crate::runtime::hybrid_persistence::mirror_control_metadata_within(
            cloud,
            fs,
            publication_lock,
            cloud.callback_timeout(),
            manifest_sequence,
            deadline,
            validate_lease,
        )?;
        Ok(true)
    }
}

pub(crate) struct FlushBuildCompletion {
    pub identity: FlushIdentity,
    pub memtable: Arc<crate::memtable::SkipListMemtable>,
    pub staging_path: PathBuf,
    pub reservation: Option<crate::storage::hybrid::actor::StorageReservationToken>,
    pub build_ns: u64,
    pub result: MidgeResult<crate::runtime::FileMeta>,
}

#[derive(Clone)]
pub(crate) struct FlushPublishTask {
    pub build: FlushBuildOutput,
    pub sst_name: String,
    pub sst_seq: u64,
    pub sst_dir: PathBuf,
    pub fs: Arc<dyn crate::io::Fs>,
    pub storage: Arc<dyn FlushStorage>,
    pub lease_healthy: Option<Arc<AtomicBool>>,
    pub leader_store: Option<Arc<dyn crate::lease::LeaderStore>>,
    pub leader_holder_id: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct FlushPublicationDelta {
    pub identity: FlushIdentity,
    pub file_meta: crate::runtime::FileMeta,
    pub next_sst_seq: u64,
}

pub(crate) struct FlushPublishCompletion {
    pub identity: FlushIdentity,
    pub reservation: Option<crate::storage::hybrid::actor::StorageReservationToken>,
    pub publish_ns: u64,
    pub result: MidgeResult<FlushPublicationDelta>,
}

pub(crate) struct FlushMirrorTask {
    pub delta: FlushPublicationDelta,
    pub reservation: Option<crate::storage::hybrid::actor::StorageReservationToken>,
    pub fs: Arc<dyn crate::io::Fs>,
    pub storage: Arc<dyn FlushStorage>,
    pub metadata_publication_lock: crate::runtime::MetadataPublicationLock,
    pub lease_healthy: Option<Arc<AtomicBool>>,
    pub leader_store: Option<Arc<dyn crate::lease::LeaderStore>>,
    pub leader_holder_id: Option<String>,
    pub manifest_sequence: u64,
    pub runtime_response_timeout: std::time::Duration,
}

pub(crate) struct FlushMirrorCompletion {
    pub delta: FlushPublicationDelta,
    pub reservation: Option<crate::storage::hybrid::actor::StorageReservationToken>,
    pub result: MidgeResult<bool>,
}

pub(crate) enum FlushWorkerResult {
    Build(FlushBuildCompletion),
    Publish(FlushPublishCompletion),
    Mirror(FlushMirrorCompletion),
}

#[derive(Clone)]
struct FlushBuildTask {
    identity: FlushIdentity,
    memtable: Arc<crate::memtable::SkipListMemtable>,
    staging_path: PathBuf,
    reservation: Option<crate::storage::hybrid::actor::StorageReservationToken>,
    hybrid_storage: Option<Arc<crate::storage::HybridStorage>>,
}

enum FlushWorkerTask {
    Build(FlushBuildTask),
    Publish(Box<FlushPublishTask>),
    Mirror(Box<FlushMirrorTask>),
    Shutdown,
}

/// One persistent worker with one executing task and one bounded channel slot.
pub struct FlushActor {
    in_progress: usize,
    memory_mode: bool,
    task_tx: Option<crossbeam::channel::Sender<FlushWorkerTask>>,
    worker_handle: Option<JoinHandle<()>>,
}

impl FlushActor {
    #[cfg(test)]
    pub fn new(
        sst_dir: &Path,
        memory_mode: bool,
        compression_policy: crate::codec::CompressionPolicy,
        completion_tx: crossbeam::channel::Sender<FlushWorkerResult>,
    ) -> MidgeResult<Self> {
        Self::new_with_memory_limit(
            sst_dir,
            memory_mode,
            compression_policy,
            completion_tx,
            DEFAULT_FLUSH_MEMORY_BYTES,
        )
    }

    pub(crate) fn new_with_memory_limit(
        sst_dir: &Path,
        memory_mode: bool,
        compression_policy: crate::codec::CompressionPolicy,
        completion_tx: crossbeam::channel::Sender<FlushWorkerResult>,
        memory_bytes: usize,
    ) -> MidgeResult<Self> {
        if memory_mode {
            return Ok(Self {
                in_progress: 0,
                memory_mode: true,
                task_tx: None,
                worker_handle: None,
            });
        }

        let fs = Arc::new(crate::io::RealFs::new(sst_dir).map_err(FsError::into_midge)?);
        let sst_factory = Arc::new(
            crate::sst::FsSstFactoryIo::new(fs, 64 * 1024)
                .with_compression_policy(compression_policy)
                .with_compaction_scratch_directory(sst_dir.join(".flush-staging")),
        );
        let (task_tx, task_rx) =
            crossbeam::channel::bounded::<FlushWorkerTask>(FLUSH_WORKER_CHANNEL_CAPACITY);
        let budget = crate::common::resource_budget::ResourceBudget::new(memory_bytes);
        let worker_handle = std::thread::Builder::new()
            .name("midge-flush".to_string())
            .spawn(move || Self::worker_loop(&task_rx, &completion_tx, &sst_factory, &budget))
            .map_err(|error| MidgeError::Internal(format!("spawn flush worker: {error}")))?;

        Ok(Self {
            in_progress: 0,
            memory_mode: false,
            task_tx: Some(task_tx),
            worker_handle: Some(worker_handle),
        })
    }

    pub(crate) fn is_inflight(&self) -> bool {
        self.in_progress != 0
    }

    pub(crate) fn submit_build(
        &mut self,
        identity: FlushIdentity,
        memtable: Arc<crate::memtable::SkipListMemtable>,
        staging_path: PathBuf,
        hybrid_storage: Option<Arc<crate::storage::HybridStorage>>,
    ) -> MidgeResult<()> {
        if self.memory_mode {
            return Ok(());
        }
        if self.is_inflight() {
            return Err(MidgeError::Busy(
                "flush worker already has an in-flight task".to_string(),
            ));
        }
        let task = FlushWorkerTask::Build(FlushBuildTask {
            identity,
            memtable,
            staging_path,
            reservation: None,
            hybrid_storage,
        });
        self.task_tx
            .as_ref()
            .ok_or_else(|| MidgeError::Internal("flush worker is unavailable".to_string()))?
            .try_send(task)
            .map_err(|error| MidgeError::Busy(format!("flush worker queue is full: {error}")))?;
        self.in_progress = 1;
        Ok(())
    }

    pub(crate) fn submit_publish(&mut self, task: FlushPublishTask) -> MidgeResult<()> {
        if self.memory_mode {
            return Ok(());
        }
        self.task_tx
            .as_ref()
            .ok_or_else(|| MidgeError::Internal("flush worker is unavailable".to_string()))?
            .send(FlushWorkerTask::Publish(Box::new(task)))
            .map_err(|error| MidgeError::Internal(format!("submit flush publication: {error}")))?;
        self.in_progress = 1;
        Ok(())
    }

    pub(crate) fn submit_mirror(&mut self, task: FlushMirrorTask) -> MidgeResult<()> {
        self.task_tx
            .as_ref()
            .ok_or_else(|| MidgeError::Internal("flush worker is unavailable".to_string()))?
            .send(FlushWorkerTask::Mirror(Box::new(task)))
            .map_err(|error| MidgeError::Internal(format!("submit flush mirror: {error}")))?;
        self.in_progress = 1;
        Ok(())
    }

    pub(crate) fn finish_pipeline(&mut self) {
        self.in_progress = 0;
    }

    pub(crate) fn reserve_flush(
        sba: Option<&Arc<crate::storage::HybridStorage>>,
        cf_id: crate::types::ColumnFamilyId,
        estimated_size: u64,
    ) -> MidgeResult<Option<crate::storage::hybrid::actor::StorageReservationToken>> {
        let Some(hybrid) = sba else {
            return Ok(None);
        };
        match hybrid.reserve_for_flush_with_token(estimated_size.max(1)) {
            Ok(token) => Ok(Some(token)),
            Err(crate::storage::hybrid::actor::ReservationResult::WaitForCloudUpload) => {
                hybrid.counters().record(|m| {
                    m.record_write_stall_cloud();
                });
                Err(MidgeError::WriteStall(format!(
                    "column family {cf_id} flush is waiting for cloud upload capacity"
                )))
            }
            Err(crate::storage::hybrid::actor::ReservationResult::WaitForCompaction) => {
                hybrid.counters().record(|m| {
                    m.record_write_stall_compaction();
                });
                Err(MidgeError::WriteStall(format!(
                    "column family {cf_id} flush is waiting for compaction capacity"
                )))
            }
            Err(crate::storage::hybrid::actor::ReservationResult::RejectNoSpace) => {
                hybrid.counters().record(|m| {
                    m.record_no_space_event();
                    m.record_write_stall_no_space();
                });
                Err(MidgeError::NoSpace(format!(
                    "column family {cf_id} flush has no durable capacity"
                )))
            }
            Err(crate::storage::hybrid::actor::ReservationResult::Ok) => Err(MidgeError::Internal(
                "storage admitted a reservation without a token".to_string(),
            )),
        }
    }

    pub(crate) fn release_reservation(
        sba: Option<&Arc<crate::storage::HybridStorage>>,
        reservation: Option<crate::storage::hybrid::actor::StorageReservationToken>,
    ) {
        if let (Some(hybrid), Some(token)) = (sba, reservation) {
            hybrid.flush_failed_with_token(token);
        }
    }

    pub(crate) fn shutdown_and_join(&mut self) -> MidgeResult<()> {
        if self.worker_handle.is_none() {
            return Ok(());
        }
        if let Some(task_tx) = &self.task_tx {
            let _ = task_tx.send(FlushWorkerTask::Shutdown);
        }
        if let Some(worker) = self.worker_handle.take() {
            worker.join().map_err(|_| {
                MidgeError::Internal("flush worker panicked during shutdown".into())
            })?;
        }
        self.task_tx = None;
        Ok(())
    }

    fn worker_loop(
        task_rx: &crossbeam::channel::Receiver<FlushWorkerTask>,
        completion_tx: &crossbeam::channel::Sender<FlushWorkerResult>,
        sst_factory: &Arc<crate::sst::FsSstFactoryIo>,
        budget: &crate::common::resource_budget::ResourceBudget,
    ) {
        while let Ok(task) = task_rx.recv() {
            match task {
                FlushWorkerTask::Build(mut task) => {
                    let started = Instant::now();
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        crate::failpoints::fail_point!("midge::flush_worker::before_build");
                        build::write(sst_factory, &mut task, budget)
                    }));
                    let build_ns = elapsed_ns(started);
                    let result = match result {
                        Ok(result) => result,
                        Err(panic) => {
                            tracing::error!(?panic, "flush build task panicked");
                            Err(MidgeError::Internal(
                                "flush build task panicked".to_string(),
                            ))
                        }
                    };
                    if !sst_factory.compaction_scratch_cleanup_verified() {
                        // A failed scratch unlink must not return disk capacity.
                        // The hybrid ledger retains this token until startup
                        // reconciles physical residue; block later builds too.
                        task.reservation = None;
                    }
                    let _ = completion_tx.send(FlushWorkerResult::Build(FlushBuildCompletion {
                        identity: task.identity,
                        memtable: task.memtable,
                        staging_path: task.staging_path,
                        reservation: task.reservation,
                        build_ns,
                        result,
                    }));
                }
                FlushWorkerTask::Publish(task) => {
                    let task = *task;
                    let fallback = task.clone();
                    let started = Instant::now();
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        crate::failpoints::fail_point!("midge::flush_worker::before_publication");
                        Self::publish_with_budget(&task, budget)
                    }));
                    let publish_ns = elapsed_ns(started);
                    let result = match result {
                        Ok(result) => result,
                        Err(panic) => {
                            tracing::error!(?panic, "flush publication task panicked");
                            Err(MidgeError::Internal(
                                "flush publication task panicked".to_string(),
                            ))
                        }
                    };
                    let _ =
                        completion_tx.send(FlushWorkerResult::Publish(FlushPublishCompletion {
                            identity: fallback.build.identity,
                            reservation: fallback.build.reservation,
                            publish_ns,
                            result,
                        }));
                }
                FlushWorkerTask::Mirror(task) => {
                    let task = *task;
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        mirror_after_local_commit(&task)
                    }));
                    let result = match result {
                        Ok(result) => result,
                        Err(panic) => {
                            tracing::error!(?panic, "flush mirror task panicked");
                            Err(MidgeError::Internal(
                                "flush mirror task panicked".to_string(),
                            ))
                        }
                    };
                    let _ = completion_tx.send(FlushWorkerResult::Mirror(FlushMirrorCompletion {
                        delta: task.delta,
                        reservation: task.reservation,
                        result,
                    }));
                }
                FlushWorkerTask::Shutdown => break,
            }
        }
    }

    #[cfg(test)]
    fn write_memtable_to_staging(
        factory: &Arc<crate::sst::FsSstFactoryIo>,
        task: &mut FlushBuildTask,
    ) -> MidgeResult<crate::runtime::FileMeta> {
        build::write(
            factory,
            task,
            &crate::common::resource_budget::ResourceBudget::new(DEFAULT_FLUSH_MEMORY_BYTES),
        )
    }

    #[cfg(test)]
    fn publish(task: &FlushPublishTask) -> MidgeResult<FlushPublicationDelta> {
        Self::publish_with_budget(
            task,
            &crate::common::resource_budget::ResourceBudget::new(DEFAULT_FLUSH_MEMORY_BYTES),
        )
    }

    fn publish_with_budget(
        task: &FlushPublishTask,
        budget: &crate::common::resource_budget::ResourceBudget,
    ) -> MidgeResult<FlushPublicationDelta> {
        validate_task_lease(task)?;
        let final_path = task.sst_dir.join(&task.sst_name);
        finalize_staged_sst(task, &final_path, budget)?;
        crate::failpoints::fail_point!("midge::flush_worker::after_sst_finalization");
        crate::failpoints::fail_point!("midge::flush::after_sst_write_before_publish");

        validate_task_lease(task)?;
        upload(task, &final_path, budget)?;
        crate::failpoints::fail_point!("midge::flush_worker::after_cloud_sst_upload");

        let mut file_meta = task.build.file_meta.clone();
        file_meta.name.clone_from(&task.sst_name);
        validate_task_lease(task)?;
        let next_sst_seq = task
            .sst_seq
            .checked_add(1)
            .ok_or_else(|| MidgeError::ResourceLimit("SST sequence space exhausted".to_string()))?;

        Ok(FlushPublicationDelta {
            identity: task.build.identity,
            file_meta,
            next_sst_seq,
        })
    }
}

fn upload(
    task: &FlushPublishTask,
    final_path: &Path,
    budget: &crate::common::resource_budget::ResourceBudget,
) -> MidgeResult<()> {
    let checksum = task
        .build
        .file_meta
        .content_crc32c
        .ok_or_else(|| MidgeError::Corruption("flush output lacks checksum".into()))?;
    task.storage.publish_sst(
        &crate::cloud_layout::object_key(&task.sst_name),
        final_path,
        task.build.file_meta.size_bytes,
        checksum,
        budget,
    )
}

fn elapsed_ns(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn validate_task_lease(task: &FlushPublishTask) -> MidgeResult<()> {
    validate_publication_lease(
        task.build.identity,
        task.lease_healthy.as_ref(),
        task.leader_store.as_ref(),
        task.leader_holder_id.as_deref(),
    )
}

fn validate_publication_lease(
    identity: FlushIdentity,
    lease_healthy: Option<&Arc<AtomicBool>>,
    leader_store: Option<&Arc<dyn crate::lease::LeaderStore>>,
    leader_holder_id: Option<&str>,
) -> MidgeResult<()> {
    if let Some(healthy) = lease_healthy {
        if !healthy.load(Ordering::Acquire) {
            return Err(MidgeError::Fenced(format!(
                "flush {} observed a failed lease heartbeat",
                identity.flush_id
            )));
        }
    }
    if let Some(store) = leader_store {
        store
            .validate_epoch(leader_holder_id.unwrap_or_default(), identity.writer_epoch)
            .map_err(|error| {
                error.into_validation_error(&format!("flush {} publish", identity.flush_id))
            })?;
    }
    Ok(())
}

fn mirror_after_local_commit(task: &FlushMirrorTask) -> MidgeResult<bool> {
    let validate = || {
        validate_publication_lease(
            task.delta.identity,
            task.lease_healthy.as_ref(),
            task.leader_store.as_ref(),
            task.leader_holder_id.as_deref(),
        )
    };
    validate()?;
    let deadline = crate::common::OperationDeadline::from_budget(task.runtime_response_timeout);
    let mut validate_lease = |_deadline: &crate::common::OperationDeadline| validate();
    let mirrored = task.storage.mirror_control_metadata(
        task.fs.as_ref(),
        &task.metadata_publication_lock,
        task.manifest_sequence,
        &deadline,
        &mut validate_lease,
    )?;
    crate::failpoints::fail_point!("midge::flush_worker::after_control_metadata_publication");
    Ok(mirrored || task.storage.has_hybrid_storage())
}

fn finalize_staged_sst(
    task: &FlushPublishTask,
    final_path: &Path,
    budget: &crate::common::resource_budget::ResourceBudget,
) -> MidgeResult<()> {
    let final_fs_path = db_relative_fs_path(task, final_path)?;
    if !task
        .fs
        .exists(&final_fs_path)
        .map_err(FsError::into_midge)?
    {
        task.fs
            .create_dir_all(&crate::io::FsPath::new("sst"))
            .map_err(FsError::into_midge)?;
        let staging_fs_path = db_relative_fs_path(task, &task.build.staging_path)?;
        task.fs
            .rename_atomic(&staging_fs_path, &final_fs_path)
            .map_err(FsError::into_midge)?;
    }
    validate_final_sst(task, final_path, &task.build.file_meta, budget)?;
    task.fs
        .sync_dir(
            &crate::io::FsPath::new("sst"),
            crate::io::Durability::Durable,
        )
        .map_err(FsError::into_midge)?;
    cleanup_non_authoritative_staging(task);
    Ok(())
}

fn db_relative_fs_path(task: &FlushPublishTask, path: &Path) -> MidgeResult<crate::io::FsPath> {
    crate::sst::fs::fs_relative_sst_path(&task.fs, path)
}

fn cleanup_non_authoritative_staging(task: &FlushPublishTask) {
    let Some(staging_dir) = task.build.staging_path.parent() else {
        return;
    };
    if staging_dir.file_name().and_then(|name| name.to_str()) != Some(".flush-staging") {
        return;
    }
    let Ok(staging_path) = db_relative_fs_path(task, &task.build.staging_path) else {
        tracing::warn!(
            path = %task.build.staging_path.display(),
            "retaining flush staging file outside the injected filesystem root"
        );
        return;
    };
    match task.fs.remove_file(&staging_path) {
        Ok(()) | Err(crate::io::FsError::NotFound(_)) => {}
        Err(error) => tracing::warn!(%error, "retaining non-authoritative flush staging file"),
    }
}

fn validate_final_sst(
    task: &FlushPublishTask,
    path: &Path,
    expected: &crate::runtime::FileMeta,
    budget: &crate::common::resource_budget::ResourceBudget,
) -> MidgeResult<()> {
    // A freshly written SST always carries both proofs, so anything missing
    // here is a defect in the writer rather than an older manifest.
    crate::sst::identity::SstIdentity::of_path(path)?
        .verify_against(
            expected.expected_sst(),
            None,
            crate::sst::identity::ProofPolicy::Required,
        )
        .map_err(|mismatch| {
            MidgeError::Corruption(format!(
                "staged SST identity changed at '{}': {mismatch}",
                path.display()
            ))
        })?;
    let fs_path = db_relative_fs_path(task, path)?;
    crate::sst::fs::SstFileIo::open_for_compaction(&fs_path.0, Arc::clone(&task.fs), budget.clone())
        .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct PublicationTestFs {
        inner: crate::io::RealFs,
        fail_rename: bool,
        fail_sync: AtomicBool,
        sync_dir_calls: parking_lot::Mutex<Vec<(crate::io::FsPath, crate::io::Durability)>>,
    }

    impl PublicationTestFs {
        fn set_sync_dir_failure(&self, fail: bool) {
            self.fail_sync.store(fail, Ordering::Relaxed);
        }

        fn sync_dir_calls(&self) -> Vec<(crate::io::FsPath, crate::io::Durability)> {
            self.sync_dir_calls.lock().clone()
        }
    }

    impl crate::io::Fs for PublicationTestFs {
        fn host_addressing(&self) -> Option<crate::io::HostAddressing<'_>> {
            crate::io::Fs::host_addressing(&self.inner)
        }

        fn coordination_key(&self) -> u64 {
            crate::io::Fs::coordination_key(&self.inner)
        }

        fn open(
            &self,
            path: &crate::io::FsPath,
            options: crate::io::OpenOptions,
        ) -> crate::io::FsResult<Box<dyn crate::io::File + '_>> {
            crate::io::Fs::open(&self.inner, path, options)
        }

        fn open_persistent_handle(
            &self,
            path: &crate::io::FsPath,
            options: crate::io::OpenOptions,
        ) -> crate::io::FsResult<Box<dyn crate::io::File>> {
            crate::io::Fs::open_persistent_handle(&self.inner, path, options)
        }

        fn remove_file(&self, path: &crate::io::FsPath) -> crate::io::FsResult<()> {
            crate::io::Fs::remove_file(&self.inner, path)
        }

        fn exists(&self, path: &crate::io::FsPath) -> crate::io::FsResult<bool> {
            crate::io::Fs::exists(&self.inner, path)
        }

        fn metadata(
            &self,
            path: &crate::io::FsPath,
        ) -> crate::io::FsResult<crate::io::traits::Metadata> {
            crate::io::Fs::metadata(&self.inner, path)
        }

        fn create_dir_all(&self, path: &crate::io::FsPath) -> crate::io::FsResult<()> {
            crate::io::Fs::create_dir_all(&self.inner, path)
        }

        fn list_dir(
            &self,
            path: &crate::io::FsPath,
        ) -> crate::io::FsResult<Vec<crate::io::traits::DirEntry>> {
            crate::io::Fs::list_dir(&self.inner, path)
        }

        fn remove_dir_all(&self, path: &crate::io::FsPath) -> crate::io::FsResult<()> {
            crate::io::Fs::remove_dir_all(&self.inner, path)
        }

        fn sync_dir(
            &self,
            path: &crate::io::FsPath,
            durability: crate::io::Durability,
        ) -> crate::io::FsResult<()> {
            self.sync_dir_calls.lock().push((path.clone(), durability));
            if self.fail_sync.load(Ordering::Relaxed) {
                return Err(crate::io::FsError::Unavailable(
                    "injected directory sync failure".to_string(),
                ));
            }
            crate::io::Fs::sync_dir(&self.inner, path, durability)
        }

        fn rename_atomic(
            &self,
            from: &crate::io::FsPath,
            to: &crate::io::FsPath,
        ) -> crate::io::FsResult<()> {
            if self.fail_rename {
                Err(crate::io::FsError::Unavailable(format!(
                    "injected rename failure from {from} to {to}"
                )))
            } else {
                crate::io::Fs::rename_atomic(&self.inner, from, to)
            }
        }
    }

    struct ScriptedLeaderStore {
        expected_epoch: u64,
        fail_at_validation: usize,
        validations: AtomicUsize,
    }

    impl crate::lease::LeaderStore for ScriptedLeaderStore {
        fn acquire_leadership(
            &self,
            _holder_id: &str,
        ) -> Result<crate::lease::LeaderRecord, crate::lease::LeaseError> {
            Err(crate::lease::LeaseError::AcquisitionFailed(
                "not used by flush publication tests".to_string(),
            ))
        }

        fn read_current(
            &self,
        ) -> Result<Option<crate::lease::LeaderRecord>, crate::lease::LeaseError> {
            let validation = self.validations.fetch_add(1, Ordering::SeqCst) + 1;
            let epoch = if validation == self.fail_at_validation {
                self.expected_epoch + 1
            } else {
                self.expected_epoch
            };
            Ok(Some(crate::lease::LeaderRecord {
                epoch,
                holder_id: "flush-test".to_string(),
                acquired_at: "2026-08-07T00:00:00Z".to_string(),
            }))
        }
    }

    struct PublicationFixture {
        directory: tempfile::TempDir,
        db_path: PathBuf,
        task: FlushPublishTask,
        hybrid_storage: Arc<crate::storage::HybridStorage>,
        sst_backend: Arc<crate::storage::cloud::MockCloudBackend>,
        control_backend: Arc<crate::storage::cloud::MockCloudBackend>,
        metadata_publication_lock: crate::runtime::MetadataPublicationLock,
    }

    fn publication_fixture(fail_at_validation: usize) -> MidgeResult<PublicationFixture> {
        let directory = tempfile::tempdir()?;
        let db_path = directory.path().join("db");
        crate::metadata::ensure_or_create_format_marker(&db_path)?;
        let sst_dir = db_path.join("sst");
        std::fs::create_dir_all(&sst_dir)?;
        let staging_path = db_path.join("staging").join("flush-1.sst");
        let identity = FlushIdentity {
            flush_id: 1,
            writer_epoch: 7,
            cf_id: 0,
            sequence: 1,
        };
        let memtable = Arc::new(crate::memtable::SkipListMemtable::new());
        memtable.put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)?;
        let mut build_task = FlushBuildTask {
            identity,
            memtable,
            staging_path: staging_path.clone(),
            reservation: None,
            hybrid_storage: None,
        };
        // Rooted at the database directory because this fixture stages flush
        // output beside `sst/` rather than inside it.
        let fs: Arc<dyn crate::io::Fs> =
            Arc::new(crate::io::RealFs::new(&db_path).map_err(FsError::into_midge)?);
        let sst_factory = Arc::new(
            crate::sst::FsSstFactoryIo::new(fs, 64 * 1024)
                .with_compression_policy(crate::codec::CompressionPolicy::default()),
        );
        let file_meta = FlushActor::write_memtable_to_staging(&sst_factory, &mut build_task)?;

        let local = Arc::new(crate::storage::filesystem::FileSystem::new(
            directory.path().join("local"),
        )?);
        let sst_backend = Arc::new(crate::storage::cloud::MockCloudBackend::new());
        let sst_cloud = Arc::new(crate::storage::cloud::CloudStorage::new(
            sst_backend.clone(),
            String::new(),
        ));
        let hybrid = Arc::new(crate::storage::HybridStorage::with_policy(
            local,
            sst_cloud,
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        ));
        let control_backend = Arc::new(crate::storage::cloud::MockCloudBackend::new());
        let control_cloud = Arc::new(crate::storage::cloud::CloudStorage::new(
            control_backend.clone(),
            String::new(),
        ));
        let leader_store: Arc<dyn crate::lease::LeaderStore> = Arc::new(ScriptedLeaderStore {
            expected_epoch: identity.writer_epoch,
            fail_at_validation,
            validations: AtomicUsize::new(0),
        });
        let publication_fs: Arc<dyn crate::io::Fs> =
            Arc::new(crate::io::RealFs::new(&db_path).map_err(FsError::into_midge)?);
        let task = FlushPublishTask {
            build: FlushBuildOutput {
                identity,
                staging_path,
                file_meta,
                reservation: None,
            },
            sst_name: crate::cloud_layout::file_name(identity.cf_id, 0, 1),
            sst_seq: 1,
            sst_dir,
            fs: publication_fs,
            storage: Arc::new(HybridFlushStorage::new(
                Some(Arc::clone(&hybrid)),
                Some(control_cloud),
            )),
            lease_healthy: Some(Arc::new(AtomicBool::new(true))),
            leader_store: Some(leader_store),
            leader_holder_id: Some("flush-test".to_string()),
        };
        Ok(PublicationFixture {
            directory,
            db_path,
            task,
            hybrid_storage: hybrid,
            sst_backend,
            control_backend,
            metadata_publication_lock: crate::runtime::MetadataPublicationLock::default(),
        })
    }

    fn mirror_task(fixture: &PublicationFixture, delta: FlushPublicationDelta) -> FlushMirrorTask {
        FlushMirrorTask {
            delta,
            reservation: None,
            fs: Arc::clone(&fixture.task.fs),
            storage: Arc::clone(&fixture.task.storage),
            metadata_publication_lock: fixture.metadata_publication_lock.clone(),
            lease_healthy: fixture.task.lease_healthy.clone(),
            leader_store: fixture.task.leader_store.clone(),
            leader_holder_id: fixture.task.leader_holder_id.clone(),
            manifest_sequence: 1,
            runtime_response_timeout: crate::config::DEFAULT_RUNTIME_RESPONSE_TIMEOUT,
        }
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_retain_disk_admission_when_scratch_unlink_fails() -> MidgeResult<()> {
        // Arrange
        let _test_guard = crate::failpoints::test_failpoint_guard();
        let scenario = fail::FailScenario::setup();
        let fixture = publication_fixture(usize::MAX)?;
        let scratch = fixture.directory.path().join("failed-scratch");
        let factory = Arc::new(
            crate::sst::FsSstFactoryIo::new(
                Arc::new(
                    crate::io::RealFs::new(fixture.directory.path())
                        .map_err(FsError::into_midge)?,
                ),
                4096,
            )
            .with_compaction_scratch_directory(scratch.clone()),
        );
        let injected = std::sync::Mutex::new(false);
        fail::cfg_callback("midge::flush_worker::after_scratch_creation", move || {
            let mut injected = injected
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if *injected {
                return;
            }
            let Ok(mut files) = std::fs::read_dir(&scratch) else {
                return;
            };
            let Some(Ok(file)) = files.next() else {
                return;
            };
            let file = file.path();
            std::fs::remove_file(&file).unwrap();
            std::fs::create_dir(&file).unwrap();
            *injected = true;
        })
        .unwrap();
        let memtable = Arc::new(crate::memtable::SkipListMemtable::new());
        memtable.put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)?;
        let task = FlushBuildTask {
            identity: fixture.task.build.identity,
            memtable,
            staging_path: fixture.directory.path().join("output.sst"),
            reservation: None,
            hybrid_storage: Some(fixture.hybrid_storage.clone()),
        };
        let (tx, rx) = crossbeam::channel::unbounded();
        let (completed, results) = crossbeam::channel::unbounded();
        tx.send(FlushWorkerTask::Build(task.clone())).unwrap();
        tx.send(FlushWorkerTask::Build(task)).unwrap();
        tx.send(FlushWorkerTask::Shutdown).unwrap();
        let budget = crate::common::resource_budget::ResourceBudget::new(1024 * 1024);

        // Act
        FlushActor::worker_loop(&rx, &completed, &factory, &budget);
        let first = results.recv().unwrap();
        let second = results.recv().unwrap();

        // Assert
        let FlushWorkerResult::Build(first) = first else {
            panic!("build result")
        };
        let FlushWorkerResult::Build(second) = second else {
            panic!("build result")
        };
        assert!(first.result.is_err());
        assert!(
            first.reservation.is_none(),
            "unconfirmed residue must retain its ledger token"
        );
        assert!(matches!(second.result, Err(MidgeError::ResourceLimit(_))));
        assert!(!factory.compaction_scratch_cleanup_verified());
        assert!(
            fixture
                .hybrid_storage
                .budget_snapshot()
                .total_committed_bytes
                > 0
        );
        assert_eq!(budget.used(), 0);
        scenario.teardown();
        Ok(())
    }

    #[test]
    fn should_reject_conflicting_remote_bytes_without_publishing_flush_metadata() -> MidgeResult<()>
    {
        // Arrange
        use crate::storage::cloud::CloudBackend;
        let fixture = publication_fixture(usize::MAX)?;
        let bytes = std::fs::read(&fixture.task.build.staging_path)?;
        let mut corrupted = bytes.clone();
        let middle = corrupted.len() / 2;
        corrupted[middle] ^= 1;
        let (tx, rx) = std::sync::mpsc::channel();
        fixture.sst_backend.submit_put(
            &crate::cloud_layout::object_key(&fixture.task.sst_name),
            corrupted,
            Vec::new(),
            tx,
        );
        rx.recv().unwrap();
        fixture.sst_backend.clear_history();
        let budget = crate::common::resource_budget::ResourceBudget::new(1024 * 1024);

        // Act
        let result = FlushActor::publish_with_budget(&fixture.task, &budget);

        // Assert
        assert!(matches!(result, Err(MidgeError::Corruption(_))));
        assert!(fixture.sst_backend.get_uploads().is_empty());
        assert!(fixture.control_backend.get_uploads().is_empty());
        assert_eq!(
            std::fs::read(fixture.task.sst_dir.join(&fixture.task.sst_name))?,
            bytes
        );
        Ok(())
    }

    #[test]
    fn should_retain_flush_output_when_upload_memory_cannot_be_admitted() -> MidgeResult<()> {
        // Arrange
        let fixture = publication_fixture(usize::MAX)?;
        fixture
            .hybrid_storage
            .enable_ephemeral_sst_cache(1024 * 1024);
        let budget = crate::common::resource_budget::ResourceBudget::new(128 * 1024);

        // Act
        let result = FlushActor::publish_with_budget(&fixture.task, &budget);

        // Assert
        assert!(matches!(result, Err(MidgeError::ResourceLimit(_))));
        assert!(fixture.task.sst_dir.join(&fixture.task.sst_name).exists());
        assert!(fixture.sst_backend.get_uploads().is_empty());
        assert!(fixture.control_backend.get_uploads().is_empty());
        assert_eq!(budget.used(), 0);
        Ok(())
    }

    #[test]
    fn should_publish_ephemeral_flush_with_bounded_identity_readback() -> MidgeResult<()> {
        // Arrange
        let fixture = publication_fixture(usize::MAX)?;
        fixture
            .hybrid_storage
            .enable_ephemeral_sst_cache(1024 * 1024);
        let budget = crate::common::resource_budget::ResourceBudget::new(1024 * 1024);

        // Act
        let result = FlushActor::publish_with_budget(&fixture.task, &budget)?;

        // Assert
        assert_eq!(result.identity, fixture.task.build.identity);
        assert_eq!(fixture.sst_backend.get_uploads().len(), 1);
        // A compatibility adapter may still be unwinding its completion. The
        // reservation must eventually return after all callbacks have finished.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while budget.used() != 0 && std::time::Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert_eq!(budget.used(), 0);
        Ok(())
    }

    #[test]
    fn should_not_write_manifest_from_flush_worker_when_publishing() -> MidgeResult<()> {
        // Arrange
        let fixture = publication_fixture(usize::MAX)?;

        // Act
        FlushActor::publish(&fixture.task)?;

        // Assert
        for name in [
            crate::metadata::files::JOURNAL,
            crate::metadata::files::MANIFEST_SNAPSHOT,
            "intent_log.json",
        ] {
            assert!(
                !fixture.db_path.join(name).exists(),
                "flush worker wrote {name}"
            );
        }
        Ok(())
    }

    #[test]
    fn should_preserve_reserved_names_when_publishing_an_earlier_sst() -> MidgeResult<()> {
        // Arrange
        let fixture = publication_fixture(usize::MAX)?;
        let mut manifest = crate::metadata::Manifest::default();
        manifest.next_sst_seqs.insert(0, 100);
        crate::metadata::ManifestPersistence::save_snapshot_and_truncate_journal(
            &fixture.db_path,
            &manifest,
        )
        .map_err(MidgeError::Internal)?;

        // Act
        let delta = FlushActor::publish(&fixture.task)?;
        let persisted = crate::metadata::ManifestPersistence::load(&fixture.db_path)
            .map_err(MidgeError::Internal)?;

        // Assert
        assert_eq!(delta.next_sst_seq, 2);
        assert_eq!(persisted.next_sst_seqs[&0], 100);
        assert!(persisted.files.is_empty());
        Ok(())
    }

    #[test]
    fn should_reject_flush_before_writing_sst_when_verification_staging_exceeds_disk_budget(
    ) -> MidgeResult<()> {
        // Arrange
        let fixture = publication_fixture(usize::MAX)?;
        let hybrid = &fixture.hybrid_storage;
        // The final SST itself fits; its concurrent readback copy does not.
        hybrid.enable_ephemeral_sst_cache(fixture.task.build.file_meta.size_bytes);
        let memtable = Arc::new(crate::memtable::SkipListMemtable::new());
        memtable.put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)?;
        let staging_path = fixture.directory.path().join("admitted/flush-2.sst");
        let mut task = FlushBuildTask {
            identity: fixture.task.build.identity,
            memtable,
            staging_path: staging_path.clone(),
            reservation: None,
            hybrid_storage: Some(Arc::clone(hybrid)),
        };
        let fs = Arc::new(
            crate::io::RealFs::new(fixture.directory.path()).map_err(FsError::into_midge)?,
        );
        let factory: Arc<crate::sst::FsSstFactoryIo> =
            Arc::new(crate::sst::FsSstFactoryIo::new(fs, 64 * 1024));

        // Act
        let result = FlushActor::write_memtable_to_staging(&factory, &mut task);

        // Assert
        assert!(matches!(result, Err(MidgeError::NoSpace(_))));
        assert!(!staging_path.exists());
        assert_eq!(
            std::fs::read_dir(staging_path.parent().unwrap())?.count(),
            0
        );
        assert!(task.reservation.is_none());
        assert_eq!(hybrid.budget_snapshot().total_committed_bytes, 0);
        Ok(())
    }

    #[test]
    fn should_retry_sst_directory_sync_when_prior_finalize_barrier_failed() -> MidgeResult<()> {
        // Arrange
        let mut fixture = publication_fixture(usize::MAX)?;
        let sync_fs = Arc::new(PublicationTestFs {
            inner: crate::io::RealFs::new(&fixture.db_path).map_err(FsError::into_midge)?,
            fail_rename: false,
            fail_sync: AtomicBool::new(false),
            sync_dir_calls: parking_lot::Mutex::new(Vec::new()),
        });
        sync_fs.set_sync_dir_failure(true);
        fixture.task.fs = sync_fs.clone();
        let final_path = fixture.task.sst_dir.join(&fixture.task.sst_name);

        // Act
        let error = finalize_staged_sst(
            &fixture.task,
            &final_path,
            &crate::common::resource_budget::ResourceBudget::new(DEFAULT_FLUSH_MEMORY_BYTES),
        )
        .expect_err("directory durability failure must fail SST finalization");
        sync_fs.set_sync_dir_failure(false);
        finalize_staged_sst(
            &fixture.task,
            &final_path,
            &crate::common::resource_budget::ResourceBudget::new(DEFAULT_FLUSH_MEMORY_BYTES),
        )?;

        // Assert
        assert!(matches!(error, MidgeError::Internal(_)));
        assert!(
            final_path.exists(),
            "renamed SST must be retained for retry"
        );
        assert_eq!(
            sync_fs.sync_dir_calls(),
            [
                (
                    crate::io::FsPath::new("sst"),
                    crate::io::Durability::Durable
                ),
                (
                    crate::io::FsPath::new("sst"),
                    crate::io::Durability::Durable
                ),
            ],
            "retry must re-establish the directory durability barrier"
        );
        Ok(())
    }

    #[test]
    fn should_not_publish_sst_when_injected_filesystem_rejects_atomic_rename() -> MidgeResult<()> {
        // Arrange
        let mut fixture = publication_fixture(usize::MAX)?;
        fixture.task.fs = Arc::new(PublicationTestFs {
            inner: crate::io::RealFs::new(&fixture.db_path).map_err(FsError::into_midge)?,
            fail_rename: true,
            fail_sync: AtomicBool::new(false),
            sync_dir_calls: parking_lot::Mutex::new(Vec::new()),
        });
        let final_path = fixture.task.sst_dir.join(&fixture.task.sst_name);

        // Act
        let error = finalize_staged_sst(
            &fixture.task,
            &final_path,
            &crate::common::resource_budget::ResourceBudget::new(DEFAULT_FLUSH_MEMORY_BYTES),
        )
        .expect_err("injected filesystem must own the atomic publication rename");

        // Assert
        assert!(matches!(error, MidgeError::Internal(_)));
        assert!(fixture.task.build.staging_path.exists());
        assert!(!final_path.exists());
        Ok(())
    }

    #[test]
    fn should_initialize_one_persistent_flush_worker_when_local() -> MidgeResult<()> {
        // Arrange
        let directory = tempfile::tempdir()?;
        let (completion_tx, _completion_rx) = crossbeam::channel::unbounded();

        // Act
        let mut actor = FlushActor::new(
            directory.path(),
            false,
            crate::codec::CompressionPolicy::default(),
            completion_tx,
        )?;

        // Assert
        assert!(actor.worker_handle.is_some());
        assert!(!actor.is_inflight());
        actor.shutdown_and_join()?;
        Ok(())
    }

    #[test]
    fn should_skip_flush_worker_when_memory_mode() -> MidgeResult<()> {
        // Arrange
        let (completion_tx, _completion_rx) = crossbeam::channel::unbounded();

        // Act
        let actor = FlushActor::new(
            Path::new("/unused"),
            true,
            crate::codec::CompressionPolicy::default(),
            completion_tx,
        )?;

        // Assert
        assert!(actor.worker_handle.is_none());
        Ok(())
    }

    #[test]
    fn should_fence_flush_before_sst_upload_when_epoch_changes() -> MidgeResult<()> {
        // Arrange
        let fixture = publication_fixture(2)?;

        // Act
        let error = FlushActor::publish(&fixture.task).expect_err("publication must be fenced");

        // Assert
        assert!(matches!(error, MidgeError::Fenced(_)));
        assert!(fixture.sst_backend.get_uploads().is_empty());
        assert!(!fixture.directory.path().join("db/intent_log.json").exists());
        Ok(())
    }

    #[test]
    fn should_leave_manifest_to_event_loop_when_epoch_changes_after_upload() -> MidgeResult<()> {
        // Arrange
        let fixture = publication_fixture(4)?;
        let db_path = fixture.db_path.clone();

        // Act
        let delta = FlushActor::publish(&fixture.task)?;

        // Assert
        assert_eq!(delta.identity, fixture.task.build.identity);
        assert_eq!(fixture.sst_backend.get_uploads().len(), 1);
        assert!(crate::metadata::ManifestPersistence::load(&db_path)
            .map_err(MidgeError::Internal)?
            .files
            .is_empty());
        assert!(!db_path.join("intent_log.json").exists());
        Ok(())
    }

    #[test]
    fn should_retain_committed_manifest_when_mirror_is_fenced() -> MidgeResult<()> {
        // Arrange
        let fixture = publication_fixture(5)?;
        let db_path = fixture.db_path.clone();
        let sst_name = fixture.task.sst_name.clone();

        // Act
        let delta = FlushActor::publish(&fixture.task)?;
        let mut state = crate::runtime::state::RuntimeState::new(db_path.clone(), false);
        state.record_flush_publication_intent(
            delta.identity.cf_id,
            delta.identity.sequence,
            &delta.file_meta,
        )?;
        state.commit_flush_publication(
            delta.identity.cf_id,
            delta.identity.sequence,
            &delta.file_meta,
            delta.next_sst_seq,
            true,
        )?;
        let error = mirror_after_local_commit(&mirror_task(&fixture, delta))
            .expect_err("mirror must be fenced");

        // Assert
        assert!(matches!(error, MidgeError::Fenced(_)));
        assert!(crate::metadata::ManifestPersistence::load(&db_path)
            .map_err(MidgeError::Internal)?
            .files
            .iter()
            .any(|file| file.name == sst_name));
        assert!(crate::runtime::IntentPersistence::load(&db_path)
            .map_err(MidgeError::Internal)?
            .is_empty());
        assert!(fixture.control_backend.get_uploads().is_empty());
        Ok(())
    }

    #[test]
    fn should_not_mirror_control_metadata_during_sst_upload() -> MidgeResult<()> {
        // Arrange
        let fixture = publication_fixture(usize::MAX)?;
        let db_path = fixture.db_path.clone();
        let sst_name = fixture.task.sst_name.clone();

        // Act
        let delta = FlushActor::publish(&fixture.task)?;

        // Assert
        assert_eq!(delta.file_meta.name, sst_name);
        assert!(crate::metadata::ManifestPersistence::load(&db_path)
            .map_err(MidgeError::Internal)?
            .files
            .is_empty());
        assert!(fixture.control_backend.get_uploads().is_empty());
        Ok(())
    }

    #[test]
    fn should_time_out_flush_metadata_mirror_when_publication_lock_is_held() -> MidgeResult<()> {
        // Arrange
        let fixture = publication_fixture(usize::MAX)?;
        let delta = FlushActor::publish(&fixture.task)?;
        let publication_lock = fixture.metadata_publication_lock.clone();
        let guard = publication_lock.lock();
        let mut task = mirror_task(&fixture, delta);
        task.runtime_response_timeout = std::time::Duration::from_millis(100);
        let (result_tx, result_rx) = std::sync::mpsc::channel();

        // Act: keep the lock on this thread so the old blocking lock path
        // cannot hang the test process.
        let worker = std::thread::spawn(move || {
            let result = mirror_after_local_commit(&task);
            result_tx.send(result).expect("publish result receiver");
        });
        let result = result_rx.recv_timeout(std::time::Duration::from_secs(2));
        drop(guard);
        worker
            .join()
            .expect("flush publish worker exits after timeout");

        // Assert
        assert!(matches!(
            result.expect("bounded publication result"),
            Err(MidgeError::Timeout(_))
        ));
        Ok(())
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_persist_intents_twice_for_successful_flush_publication() -> MidgeResult<()> {
        // Arrange
        let _test_guard = crate::failpoints::test_failpoint_guard();
        let scenario = fail::FailScenario::setup();
        let fixture = publication_fixture(usize::MAX)?;
        let saves = Arc::new(AtomicUsize::new(0));
        let callback_saves = Arc::clone(&saves);
        fail::cfg_callback("midge::intent::before_save", move || {
            callback_saves.fetch_add(1, Ordering::SeqCst);
        })
        .expect("configure intent save observer");

        // Act
        let delta = FlushActor::publish(&fixture.task)?;
        let mut state = crate::runtime::state::RuntimeState::new(fixture.db_path.clone(), false);
        let result = state
            .record_flush_publication_intent(
                delta.identity.cf_id,
                delta.identity.sequence,
                &delta.file_meta,
            )
            .and_then(|()| {
                state.commit_flush_publication(
                    delta.identity.cf_id,
                    delta.identity.sequence,
                    &delta.file_meta,
                    delta.next_sst_seq,
                    true,
                )
            });
        fail::remove("midge::intent::before_save");
        scenario.teardown();

        // Assert
        result?;
        assert_eq!(
            saves.load(Ordering::SeqCst),
            2,
            "publication needs one output-durable save and one durable clear"
        );
        Ok(())
    }
}
