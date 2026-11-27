//! Persistent, lease-fenced memtable flush worker.
//!
//! The runtime thread owns freeze/install decisions. This actor owns one
//! bounded worker thread that performs only immutable build/publication I/O and
//! returns deltas for the runtime to validate and install.

use cntryl_midge_core::common::{MidgeError, MidgeResult};
use cntryl_midge_core::io::FsError;
use cntryl_midge_table::sst::SstFactory;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Instant;

#[cfg(test)]
const DEFAULT_FLUSH_MEMORY_BYTES: usize = 256 * 1024 * 1024;

#[path = "flush/build.rs"]
mod build;
#[cfg(all(test, feature = "failpoints"))]
mod phase_tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FlushIdentity {
    pub flush_id: u64,
    pub writer_epoch: u64,
    pub cf_id: cntryl_midge_core::types::ColumnFamilyId,
    pub sequence: u64,
}

#[derive(Clone)]
pub(crate) struct FlushBuildOutput {
    pub identity: FlushIdentity,
    pub staging_path: PathBuf,
    pub file_meta: cntryl_midge_durability::metadata::intent::FileMeta,
    pub reservation: Option<cntryl_midge_storage::hybrid::actor::StorageReservationToken>,
}

/// Storage capabilities needed to publish a flush. The worker depends on
/// this small contract so publication tests can supply provider and filesystem
/// doubles without constructing the hybrid-storage coordinator.
pub(crate) trait FlushStorage: Send + Sync {
    fn has_hybrid_storage(&self) -> bool;

    fn publish_sst(
        &self,
        source: cntryl_midge_storage::hybrid::backend::ImmutableFileSource<'_>,
        budget: &cntryl_midge_core::common::resource_budget::ResourceBudget,
    ) -> MidgeResult<()>;

    fn mirror_control_metadata(
        &self,
        fs: &dyn cntryl_midge_core::io::Fs,
        publication_lock: &crate::runtime::MetadataPublicationLock,
        deadline: &cntryl_midge_core::common::OperationDeadline,
        authority: Option<crate::runtime::hybrid_persistence::CloudMetadataMirrorAuthority<'_>>,
        validate_lease: &mut dyn FnMut(
            &cntryl_midge_core::common::OperationDeadline,
        ) -> MidgeResult<()>,
    ) -> MidgeResult<bool>;

    fn mirror_captured_control_metadata(
        &self,
        _cut: &crate::runtime::CapturedMetadataCut,
        _publication_lock: &crate::runtime::MetadataPublicationLock,
        _deadline: &cntryl_midge_core::common::OperationDeadline,
        _authority: Option<crate::runtime::hybrid_persistence::CloudMetadataMirrorAuthority<'_>>,
        _validate_lease: &mut dyn FnMut(
            &cntryl_midge_core::common::OperationDeadline,
        ) -> MidgeResult<()>,
    ) -> MidgeResult<bool> {
        Err(MidgeError::NotSupported(
            "storage does not support captured metadata publication".into(),
        ))
    }
}

#[derive(Clone, Default)]
pub(crate) struct HybridFlushStorage {
    hybrid: Option<Arc<cntryl_midge_storage::HybridStorage>>,
    cloud_metadata: Option<Arc<cntryl_midge_storage::cloud::CloudStorage>>,
}

impl HybridFlushStorage {
    pub(crate) fn new(
        hybrid: Option<Arc<cntryl_midge_storage::HybridStorage>>,
        cloud_metadata: Option<Arc<cntryl_midge_storage::cloud::CloudStorage>>,
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
        source: cntryl_midge_storage::hybrid::backend::ImmutableFileSource<'_>,
        budget: &cntryl_midge_core::common::resource_budget::ResourceBudget,
    ) -> MidgeResult<()> {
        if let Some(hybrid) = &self.hybrid {
            hybrid.publish_immutable_file(source, budget)?;
        }
        Ok(())
    }

    fn mirror_control_metadata(
        &self,
        fs: &dyn cntryl_midge_core::io::Fs,
        publication_lock: &crate::runtime::MetadataPublicationLock,
        deadline: &cntryl_midge_core::common::OperationDeadline,
        authority: Option<crate::runtime::hybrid_persistence::CloudMetadataMirrorAuthority<'_>>,
        validate_lease: &mut dyn FnMut(
            &cntryl_midge_core::common::OperationDeadline,
        ) -> MidgeResult<()>,
    ) -> MidgeResult<bool> {
        let Some(cloud) = &self.cloud_metadata else {
            return Ok(false);
        };
        let authority = authority.ok_or_else(|| {
            MidgeError::Fenced("cloud metadata mirror has no leader store".into())
        })?;
        crate::runtime::hybrid_persistence::mirror_control_metadata_within(
            crate::runtime::hybrid_persistence::CloudMetadataMirrorContext {
                cloud,
                fs,
                publication_lock,
                lock_wait_budget: cloud.callback_timeout(),
                deadline,
                authority,
            },
            validate_lease,
        )?;
        Ok(true)
    }

    fn mirror_captured_control_metadata(
        &self,
        cut: &crate::runtime::CapturedMetadataCut,
        publication_lock: &crate::runtime::MetadataPublicationLock,
        deadline: &cntryl_midge_core::common::OperationDeadline,
        authority: Option<crate::runtime::hybrid_persistence::CloudMetadataMirrorAuthority<'_>>,
        validate_lease: &mut dyn FnMut(
            &cntryl_midge_core::common::OperationDeadline,
        ) -> MidgeResult<()>,
    ) -> MidgeResult<bool> {
        let Some(cloud) = &self.cloud_metadata else {
            return Ok(false);
        };
        let authority = authority.ok_or_else(|| {
            MidgeError::Fenced("cloud metadata mirror has no leader store".into())
        })?;
        crate::runtime::hybrid_persistence::mirror_captured_control_metadata_within(
            crate::runtime::hybrid_persistence::CloudMetadataMirrorContext {
                cloud,
                fs: cut.fs().as_ref(),
                publication_lock,
                lock_wait_budget: cloud.callback_timeout(),
                deadline,
                authority,
            },
            cut,
            validate_lease,
        )?;
        Ok(true)
    }
}

pub(crate) struct FlushBuildCompletion {
    pub identity: FlushIdentity,
    pub memtable: Arc<cntryl_midge_table::memtable::SkipListMemtable>,
    pub staging_path: PathBuf,
    pub reservation: Option<cntryl_midge_storage::hybrid::actor::StorageReservationToken>,
    pub build_ns: u64,
    pub result: MidgeResult<cntryl_midge_durability::metadata::intent::FileMeta>,
}

#[derive(Clone)]
pub(crate) struct FlushPublishTask {
    pub build: FlushBuildOutput,
    pub sst_name: String,
    pub sst_seq: u64,
    /// The database root; staging and final SSTs are root-relative in it.
    pub fs: Arc<dyn cntryl_midge_core::io::Fs>,
    pub storage: Arc<dyn FlushStorage>,
    pub lease_healthy: Option<Arc<AtomicBool>>,
    pub lease_validity: Option<Arc<cntryl_midge_durability::lease::LeaseValidity>>,
    pub leader_store: Option<Arc<dyn cntryl_midge_durability::lease::LeaderStore>>,
    pub leader_holder_id: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct FlushPublicationDelta {
    pub identity: FlushIdentity,
    pub file_meta: cntryl_midge_durability::metadata::intent::FileMeta,
    pub next_sst_seq: u64,
}

pub(crate) struct FlushPublishCompletion {
    pub identity: FlushIdentity,
    pub reservation: Option<cntryl_midge_storage::hybrid::actor::StorageReservationToken>,
    pub publish_ns: u64,
    pub result: MidgeResult<FlushPublicationDelta>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FlushMirrorPhase {
    ManifestPublished,
    IntentCleared,
}

pub(crate) struct FlushMirrorTask {
    pub phase: FlushMirrorPhase,
    pub delta: FlushPublicationDelta,
    pub reservation: Option<cntryl_midge_storage::hybrid::actor::StorageReservationToken>,
    pub fs: Arc<dyn cntryl_midge_core::io::Fs>,
    pub metadata_cut: Option<Arc<crate::runtime::CapturedMetadataCut>>,
    pub storage: Arc<dyn FlushStorage>,
    pub metadata_publication_lock: crate::runtime::MetadataPublicationLock,
    pub lease_healthy: Option<Arc<AtomicBool>>,
    pub lease_validity: Option<Arc<cntryl_midge_durability::lease::LeaseValidity>>,
    pub leader_store: Option<Arc<dyn cntryl_midge_durability::lease::LeaderStore>>,
    pub leader_holder_id: Option<String>,
    pub deadline: cntryl_midge_core::common::OperationDeadline,
}

pub(crate) struct FlushMirrorCompletion {
    pub phase: FlushMirrorPhase,
    pub deadline: cntryl_midge_core::common::OperationDeadline,
    pub delta: FlushPublicationDelta,
    pub reservation: Option<cntryl_midge_storage::hybrid::actor::StorageReservationToken>,
    pub result: MidgeResult<bool>,
}

pub(crate) enum FlushWorkerResult {
    Build(FlushBuildCompletion),
    Publish(FlushPublishCompletion),
    Mirror(FlushMirrorCompletion),
}

impl FlushWorkerResult {
    pub(crate) fn identity(&self) -> FlushIdentity {
        match self {
            Self::Build(result) => result.identity,
            Self::Publish(result) => result.identity,
            Self::Mirror(result) => result.delta.identity,
        }
    }
}

#[derive(Clone)]
struct FlushBuildTask {
    identity: FlushIdentity,
    memtable: Arc<cntryl_midge_table::memtable::SkipListMemtable>,
    staging_path: PathBuf,
    reservation: Option<cntryl_midge_storage::hybrid::actor::StorageReservationToken>,
    hybrid_storage: Option<Arc<cntryl_midge_storage::HybridStorage>>,
}

enum FlushWorkerTask {
    Build(FlushBuildTask),
    Publish(Box<FlushPublishTask>),
    Mirror(Box<FlushMirrorTask>),
    Shutdown,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FlushPhase {
    Build,
    Publish,
    Mirror(FlushMirrorPhase),
}

struct SubmittedFlushTask {
    task: FlushWorkerTask,
    finished: Arc<AtomicBool>,
    gate: cntryl_midge_core::failpoints::WorkerGate,
}

struct FlushOwner {
    identity: FlushIdentity,
    phase: Option<FlushPhase>,
    finished: Arc<AtomicBool>,
}

/// Bounded reusable workers, with one owned pipeline per column family.
pub struct FlushActor {
    owners: std::collections::HashMap<u64, FlushOwner>,
    worker_limit: usize,
    submitted: u64,
    memory_mode: bool,
    task_tx: Option<crossbeam::channel::Sender<SubmittedFlushTask>>,
    worker_handles: Vec<JoinHandle<()>>,
}

impl FlushActor {
    #[cfg(test)]
    pub fn new(
        fs: &Arc<dyn cntryl_midge_core::io::Fs>,
        memory_mode: bool,
        compression_policy: &cntryl_midge_core::codec::CompressionPolicy,
        completion_tx: &crossbeam::channel::Sender<FlushWorkerResult>,
    ) -> MidgeResult<Self> {
        Self::new_with_memory_limit(
            fs,
            memory_mode,
            compression_policy,
            completion_tx,
            DEFAULT_FLUSH_MEMORY_BYTES,
        )
    }

    /// Start the worker. It builds SSTs through `fs`, the database root, so
    /// staging, scratch and final outputs are all root-relative paths in it.
    pub(crate) fn new_with_memory_limit(
        fs: &Arc<dyn cntryl_midge_core::io::Fs>,
        memory_mode: bool,
        compression_policy: &cntryl_midge_core::codec::CompressionPolicy,
        completion_tx: &crossbeam::channel::Sender<FlushWorkerResult>,
        memory_bytes: usize,
    ) -> MidgeResult<Self> {
        Self::new_with_write_limiter(
            fs,
            memory_mode,
            compression_policy,
            completion_tx,
            memory_bytes,
            None,
        )
    }

    pub(crate) fn new_with_write_limiter(
        fs: &Arc<dyn cntryl_midge_core::io::Fs>,
        memory_mode: bool,
        compression_policy: &cntryl_midge_core::codec::CompressionPolicy,
        completion_tx: &crossbeam::channel::Sender<FlushWorkerResult>,
        memory_bytes: usize,
        limiter: Option<&Arc<cntryl_midge_core::common::write_rate::BackgroundWriteLimiter>>,
    ) -> MidgeResult<Self> {
        Self::new_with_worker_count(
            fs,
            memory_mode,
            compression_policy,
            completion_tx,
            memory_bytes,
            limiter,
            1,
        )
    }

    pub(crate) fn new_with_worker_count(
        fs: &Arc<dyn cntryl_midge_core::io::Fs>,
        memory_mode: bool,
        compression_policy: &cntryl_midge_core::codec::CompressionPolicy,
        completion_tx: &crossbeam::channel::Sender<FlushWorkerResult>,
        memory_bytes: usize,
        limiter: Option<&Arc<cntryl_midge_core::common::write_rate::BackgroundWriteLimiter>>,
        workers: usize,
    ) -> MidgeResult<Self> {
        if !(1..=64).contains(&workers) {
            return Err(MidgeError::InvalidArgument(
                "flush concurrency must be in 1..=64".into(),
            ));
        }
        let mut actor = Self {
            owners: std::collections::HashMap::new(),
            worker_limit: workers,
            submitted: 0,
            memory_mode,
            task_tx: None,
            worker_handles: Vec::new(),
        };
        if memory_mode {
            return Ok(actor);
        }
        let (task_tx, task_rx) = crossbeam::channel::bounded::<SubmittedFlushTask>(workers);
        actor.task_tx = Some(task_tx);
        let budget = cntryl_midge_core::common::resource_budget::ResourceBudget::new(memory_bytes);
        for index in 0..workers {
            // Scratch cleanup belongs to this worker, while memory and write
            // throttling share their original engine-wide admission ledgers.
            let factory = Arc::new(
                cntryl_midge_table::sst::FsSstFactoryIo::new(Arc::clone(fs), 64 * 1024)
                    .with_write_limiter(limiter.cloned())
                    .with_compression_policy(compression_policy.clone())
                    .with_compaction_scratch_directory(PathBuf::from(
                        cntryl_midge_table::sst::fs::FLUSH_STAGING_DIR,
                    )),
            );
            let task_rx = task_rx.clone();
            let completion_tx = completion_tx.clone();
            let budget = budget.clone();
            let worker = std::thread::Builder::new()
                .name(format!("midge-flush-{index}"))
                .spawn(move || Self::worker_loop(&task_rx, &completion_tx, &factory, &budget))
                .map_err(|error| MidgeError::Internal(format!("spawn flush worker: {error}")))?;
            actor.worker_handles.push(worker);
        }
        Ok(actor)
    }

    pub(crate) fn is_inflight(&self) -> bool {
        self.owners.values().any(|owner| owner.phase.is_some())
    }

    pub(crate) fn has_capacity(&self) -> bool {
        self.owners.len() < self.worker_limit
    }
    #[cfg(test)]
    pub(crate) fn has_pipeline(&self, id: u64) -> bool {
        self.owners.contains_key(&id)
    }
    pub(crate) fn submitted_work(&self) -> u64 {
        self.submitted
    }
    pub(crate) fn owns_pipeline(&self, cf_id: u32, flush_id: u64) -> bool {
        self.owners
            .get(&flush_id)
            .is_some_and(|owner| owner.identity.cf_id == cf_id)
    }
    pub(crate) fn can_start_or_resume(&self, cf_id: u32, flush_id: u64) -> bool {
        self.owners
            .get(&flush_id)
            .is_some_and(|owner| owner.identity.cf_id == cf_id && owner.phase.is_none())
            || (self.has_capacity()
                && !self
                    .owners
                    .values()
                    .any(|owner| owner.identity.cf_id == cf_id))
    }

    fn submit_task(
        &mut self,
        identity: FlushIdentity,
        task: FlushWorkerTask,
        resume: bool,
    ) -> MidgeResult<()> {
        if !self.can_start_or_resume(identity.cf_id, identity.flush_id)
            || (!resume && self.owners.contains_key(&identity.flush_id))
        {
            return Err(MidgeError::Busy(
                "flush pipeline capacity or column family is owned".into(),
            ));
        }
        if self
            .owners
            .get(&identity.flush_id)
            .is_some_and(|owner| owner.identity != identity)
        {
            return Err(MidgeError::Fenced(
                "flush task changed its accepted identity".into(),
            ));
        }
        let phase = match &task {
            FlushWorkerTask::Build(_) => FlushPhase::Build,
            FlushWorkerTask::Publish(_) => FlushPhase::Publish,
            FlushWorkerTask::Mirror(task) => FlushPhase::Mirror(task.phase),
            FlushWorkerTask::Shutdown => {
                return Err(MidgeError::Internal(
                    "shutdown is not a flush pipeline task".into(),
                ))
            }
        };
        let finished = Arc::new(AtomicBool::new(false));
        self.task_tx
            .as_ref()
            .ok_or_else(|| MidgeError::Internal("flush workers are unavailable".into()))?
            .try_send(SubmittedFlushTask {
                task,
                finished: Arc::clone(&finished),
                gate: cntryl_midge_core::failpoints::worker_gate(),
            })
            .map_err(|error| MidgeError::Busy(format!("flush worker queue is full: {error}")))?;
        self.owners.insert(
            identity.flush_id,
            FlushOwner {
                identity,
                phase: Some(phase),
                finished,
            },
        );
        self.submitted = self.submitted.saturating_add(1);
        Ok(())
    }

    /// A generic notification cannot finish an executing task, another phase,
    /// or another immutable generation. The worker owns the terminal flag.
    pub(crate) fn completion_is_ready(&self, completion: &FlushWorkerResult) -> bool {
        let (identity, phase) = match completion {
            FlushWorkerResult::Build(result) => (result.identity, FlushPhase::Build),
            FlushWorkerResult::Publish(result) => (result.identity, FlushPhase::Publish),
            FlushWorkerResult::Mirror(result) => {
                (result.delta.identity, FlushPhase::Mirror(result.phase))
            }
        };
        let Some(owner) = self.owners.get(&identity.flush_id) else {
            return false;
        };
        owner.identity == identity
            && owner.phase == Some(phase)
            && owner.finished.load(Ordering::Acquire)
    }

    pub(crate) fn take_completion(&mut self, completion: &FlushWorkerResult) -> bool {
        if !self.completion_is_ready(completion) {
            return false;
        }
        let owner = self
            .owners
            .get_mut(&completion.identity().flush_id)
            .expect("validated completion owner exists");
        owner.phase = None;
        true
    }

    pub(crate) fn submit_build(
        &mut self,
        identity: FlushIdentity,
        memtable: Arc<cntryl_midge_table::memtable::SkipListMemtable>,
        staging_path: PathBuf,
        hybrid_storage: Option<Arc<cntryl_midge_storage::HybridStorage>>,
    ) -> MidgeResult<()> {
        if self.memory_mode {
            return Ok(());
        }
        let task = FlushWorkerTask::Build(FlushBuildTask {
            identity,
            memtable,
            staging_path,
            reservation: None,
            hybrid_storage,
        });
        self.submit_task(identity, task, false)
    }

    pub(crate) fn submit_publish(&mut self, task: FlushPublishTask) -> MidgeResult<()> {
        if self.memory_mode {
            return Ok(());
        }
        self.submit_task(
            task.build.identity,
            FlushWorkerTask::Publish(Box::new(task)),
            true,
        )
    }

    pub(crate) fn submit_mirror(&mut self, task: FlushMirrorTask) -> MidgeResult<()> {
        self.submit_task(
            task.delta.identity,
            FlushWorkerTask::Mirror(Box::new(task)),
            true,
        )
    }

    pub(crate) fn finish_pipeline(&mut self, flush_id: u64) {
        if self
            .owners
            .get(&flush_id)
            .is_some_and(|owner| owner.phase.is_some())
        {
            tracing::warn!(
                flush_id,
                "retaining flush owner until its physical completion is consumed"
            );
            return;
        }
        self.owners.remove(&flush_id);
    }

    pub(crate) fn reserve_flush(
        sba: Option<&Arc<cntryl_midge_storage::HybridStorage>>,
        cf_id: cntryl_midge_core::types::ColumnFamilyId,
        estimated_size: u64,
    ) -> MidgeResult<Option<cntryl_midge_storage::hybrid::actor::StorageReservationToken>> {
        let Some(hybrid) = sba else {
            return Ok(None);
        };
        match hybrid.reserve_for_flush_with_token(estimated_size.max(1)) {
            Ok(token) => Ok(Some(token)),
            Err(cntryl_midge_storage::hybrid::actor::ReservationResult::WaitForCloudUpload) => {
                hybrid.counters().record(|m| {
                    m.record_write_stall_cloud();
                });
                Err(MidgeError::WriteStall(format!(
                    "column family {cf_id} flush is waiting for cloud upload capacity"
                )))
            }
            Err(cntryl_midge_storage::hybrid::actor::ReservationResult::WaitForCompaction) => {
                hybrid.counters().record(|m| {
                    m.record_write_stall_compaction();
                });
                Err(MidgeError::WriteStall(format!(
                    "column family {cf_id} flush is waiting for compaction capacity"
                )))
            }
            Err(cntryl_midge_storage::hybrid::actor::ReservationResult::RejectNoSpace) => {
                hybrid.counters().record(|m| {
                    m.record_no_space_event();
                    m.record_write_stall_no_space();
                });
                Err(MidgeError::NoSpace(format!(
                    "column family {cf_id} flush has no durable capacity"
                )))
            }
            Err(cntryl_midge_storage::hybrid::actor::ReservationResult::Ok) => Err(
                MidgeError::Internal("storage admitted a reservation without a token".to_string()),
            ),
        }
    }

    pub(crate) fn release_reservation(
        sba: Option<&Arc<cntryl_midge_storage::HybridStorage>>,
        reservation: Option<cntryl_midge_storage::hybrid::actor::StorageReservationToken>,
    ) {
        if let (Some(hybrid), Some(token)) = (sba, reservation) {
            hybrid.flush_failed_with_token(token);
        }
    }

    pub(crate) fn shutdown_and_join(&mut self) -> MidgeResult<()> {
        if self.worker_handles.is_empty() {
            return Ok(());
        }
        if let Some(task_tx) = &self.task_tx {
            for _ in &self.worker_handles {
                let _ = task_tx.send(SubmittedFlushTask {
                    task: FlushWorkerTask::Shutdown,
                    finished: Arc::new(AtomicBool::new(false)),
                    gate: cntryl_midge_core::failpoints::worker_gate(),
                });
            }
        }
        let mut error = None;
        for worker in self.worker_handles.drain(..) {
            if worker.join().is_err() {
                error = Some(MidgeError::Internal(
                    "flush worker panicked during shutdown".into(),
                ));
            }
        }
        self.task_tx = None;
        self.owners.clear();
        error.map_or(Ok(()), Err)
    }

    fn worker_loop(
        task_rx: &crossbeam::channel::Receiver<SubmittedFlushTask>,
        completion_tx: &crossbeam::channel::Sender<FlushWorkerResult>,
        sst_factory: &Arc<cntryl_midge_table::sst::FsSstFactoryIo>,
        budget: &cntryl_midge_core::common::resource_budget::ResourceBudget,
    ) {
        while let Ok(SubmittedFlushTask {
            task,
            finished,
            gate,
        }) = task_rx.recv()
        {
            if matches!(task, FlushWorkerTask::Shutdown) {
                break;
            }
            gate.run(|| Self::run_worker_task(task, &finished, completion_tx, sst_factory, budget));
        }
    }

    fn run_worker_task(
        task: FlushWorkerTask,
        finished: &AtomicBool,
        completion_tx: &crossbeam::channel::Sender<FlushWorkerResult>,
        sst_factory: &Arc<cntryl_midge_table::sst::FsSstFactoryIo>,
        budget: &cntryl_midge_core::common::resource_budget::ResourceBudget,
    ) {
        match task {
            FlushWorkerTask::Build(mut task) => {
                let started = Instant::now();
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    cntryl_midge_core::failpoints::fail_point!("midge::flush_worker::before_build");
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
                finished.store(true, Ordering::Release);
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
                    cntryl_midge_core::failpoints::fail_point!(
                        "midge::flush_worker::before_publication"
                    );
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
                finished.store(true, Ordering::Release);
                let _ = completion_tx.send(FlushWorkerResult::Publish(FlushPublishCompletion {
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
                finished.store(true, Ordering::Release);
                let _ = completion_tx.send(FlushWorkerResult::Mirror(FlushMirrorCompletion {
                    phase: task.phase,
                    deadline: task.deadline,
                    delta: task.delta,
                    reservation: task.reservation,
                    result,
                }));
            }
            FlushWorkerTask::Shutdown => {}
        }
    }

    #[cfg(test)]
    fn write_memtable_to_staging(
        factory: &Arc<cntryl_midge_table::sst::FsSstFactoryIo>,
        task: &mut FlushBuildTask,
    ) -> MidgeResult<cntryl_midge_durability::metadata::intent::FileMeta> {
        build::write(
            factory,
            task,
            &cntryl_midge_core::common::resource_budget::ResourceBudget::new(
                DEFAULT_FLUSH_MEMORY_BYTES,
            ),
        )
    }

    #[cfg(test)]
    fn publish(task: &FlushPublishTask) -> MidgeResult<FlushPublicationDelta> {
        Self::publish_with_budget(
            task,
            &cntryl_midge_core::common::resource_budget::ResourceBudget::new(
                DEFAULT_FLUSH_MEMORY_BYTES,
            ),
        )
    }

    fn publish_with_budget(
        task: &FlushPublishTask,
        budget: &cntryl_midge_core::common::resource_budget::ResourceBudget,
    ) -> MidgeResult<FlushPublicationDelta> {
        validate_task_lease(task)?;
        let final_path = cntryl_midge_table::sst::fs::sst_location(&task.sst_name);
        finalize_staged_sst(task, &final_path, budget)?;
        cntryl_midge_core::failpoints::fail_point!("midge::flush_worker::after_sst_finalization");
        cntryl_midge_core::failpoints::fail_point!("midge::flush::after_sst_write_before_publish");

        validate_task_lease(task)?;
        upload(task, &final_path, budget)?;
        cntryl_midge_core::failpoints::fail_point!("midge::flush_worker::after_cloud_sst_upload");

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
    budget: &cntryl_midge_core::common::resource_budget::ResourceBudget,
) -> MidgeResult<()> {
    let checksum = task
        .build
        .file_meta
        .content_crc32c
        .ok_or_else(|| MidgeError::Corruption("flush output lacks checksum".into()))?;
    task.storage.publish_sst(
        cntryl_midge_storage::hybrid::backend::ImmutableFileSource {
            key: &cntryl_midge_core::cloud_layout::object_key(&task.sst_name),
            fs: task.fs.as_ref(),
            path: &db_relative_fs_path(task, final_path)?,
            size: task.build.file_meta.size_bytes,
            checksum,
        },
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
        task.lease_validity.as_ref(),
        task.leader_store.as_ref(),
        task.leader_holder_id.as_deref(),
    )
}

fn validate_publication_lease(
    identity: FlushIdentity,
    lease_healthy: Option<&Arc<AtomicBool>>,
    lease_validity: Option<&Arc<cntryl_midge_durability::lease::LeaseValidity>>,
    leader_store: Option<&Arc<dyn cntryl_midge_durability::lease::LeaderStore>>,
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
    let validate = || validate_monotonic_lease(identity, lease_validity);
    validate()?;
    if let Some(store) = leader_store {
        store
            .validate_epoch(leader_holder_id.unwrap_or_default(), identity.writer_epoch)
            .map_err(|error| {
                error.into_validation_error(&format!("flush {} publish", identity.flush_id))
            })?;
    }
    validate()
}

fn validate_monotonic_lease(
    identity: FlushIdentity,
    validity: Option<&Arc<cntryl_midge_durability::lease::LeaseValidity>>,
) -> MidgeResult<()> {
    if let Some(validity) = validity {
        validity.remaining(identity.writer_epoch).map_err(|error| {
            error.into_validation_error(&format!("flush {} monotonic validity", identity.flush_id))
        })?;
    }
    Ok(())
}

fn mirror_after_local_commit(task: &FlushMirrorTask) -> MidgeResult<bool> {
    let deadline = task.deadline;
    let validate = || validate_mirror_lease(task, &deadline);
    validate()?;
    let mut validate_lease = |_deadline: &cntryl_midge_core::common::OperationDeadline| validate();
    let authority = task.leader_store.as_deref().map(|store| {
        crate::runtime::hybrid_persistence::CloudMetadataMirrorAuthority {
            store,
            holder_id: task.leader_holder_id.as_deref().unwrap_or_default(),
            writer_epoch: task.delta.identity.writer_epoch,
        }
    });
    let mirrored = if let Some(cut) = &task.metadata_cut {
        task.storage.mirror_captured_control_metadata(
            cut,
            &task.metadata_publication_lock,
            &deadline,
            authority,
            &mut validate_lease,
        )
    } else {
        task.storage.mirror_control_metadata(
            task.fs.as_ref(),
            &task.metadata_publication_lock,
            &deadline,
            authority,
            &mut validate_lease,
        )
    }?;
    cntryl_midge_core::failpoints::fail_point!(
        "midge::flush_worker::after_control_metadata_publication"
    );
    cntryl_midge_core::common::DeadlineScope::new(deadline)
        .check("flush metadata mirror completion")?;
    Ok(mirrored || task.storage.has_hybrid_storage())
}

fn validate_mirror_lease(
    task: &FlushMirrorTask,
    deadline: &cntryl_midge_core::common::OperationDeadline,
) -> MidgeResult<()> {
    cntryl_midge_core::common::DeadlineScope::new(*deadline)
        .check("flush metadata mirror authority")?;
    if task
        .lease_healthy
        .as_ref()
        .is_some_and(|healthy| !healthy.load(Ordering::Acquire))
    {
        return Err(MidgeError::Fenced(
            "flush metadata mirror lease is unhealthy".into(),
        ));
    }
    let epoch = task.delta.identity.writer_epoch;
    if let Some(validity) = &task.lease_validity {
        validity
            .remaining(epoch)
            .map_err(|error| error.into_validation_error("flush mirror lease validity"))?;
    }
    if let Some(store) = &task.leader_store {
        store
            .validate_metadata_epoch_with_timeout(
                task.leader_holder_id.as_deref().unwrap_or_default(),
                epoch,
                deadline.remaining(),
            )
            .map_err(|error| error.into_validation_error("flush mirror writer authority"))?;
    }
    cntryl_midge_core::common::DeadlineScope::new(*deadline)
        .check("flush metadata mirror authority completion")
}

fn finalize_staged_sst(
    task: &FlushPublishTask,
    final_path: &Path,
    budget: &cntryl_midge_core::common::resource_budget::ResourceBudget,
) -> MidgeResult<()> {
    let final_fs_path = db_relative_fs_path(task, final_path)?;
    if !task
        .fs
        .exists(&final_fs_path)
        .map_err(FsError::into_midge)?
    {
        validate_monotonic_lease(task.build.identity, task.lease_validity.as_ref())?;
        task.fs
            .create_dir_all(&cntryl_midge_core::io::FsPath::new(
                cntryl_midge_table::sst::fs::SST_DIR,
            ))
            .map_err(FsError::into_midge)?;
        let staging_fs_path = db_relative_fs_path(task, &task.build.staging_path)?;
        validate_monotonic_lease(task.build.identity, task.lease_validity.as_ref())?;
        task.fs
            .rename_atomic(&staging_fs_path, &final_fs_path)
            .map_err(FsError::into_midge)?;
    }
    validate_final_sst(task, final_path, &task.build.file_meta, budget)?;
    validate_monotonic_lease(task.build.identity, task.lease_validity.as_ref())?;
    task.fs
        .sync_dir(
            &cntryl_midge_core::io::FsPath::new(cntryl_midge_table::sst::fs::SST_DIR),
            cntryl_midge_core::io::Durability::Durable,
        )
        .map_err(FsError::into_midge)?;
    validate_monotonic_lease(task.build.identity, task.lease_validity.as_ref())?;
    cleanup_non_authoritative_staging(task);
    Ok(())
}

fn db_relative_fs_path(
    task: &FlushPublishTask,
    path: &Path,
) -> MidgeResult<cntryl_midge_core::io::FsPath> {
    cntryl_midge_table::sst::fs::fs_relative_sst_path(&task.fs, path)
}

fn cleanup_non_authoritative_staging(task: &FlushPublishTask) {
    let Some(staging_dir) = task.build.staging_path.parent() else {
        return;
    };
    if staging_dir.file_name().and_then(|name| name.to_str())
        != Some(cntryl_midge_table::sst::fs::STAGING_DIR_NAME)
    {
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
        Ok(()) | Err(cntryl_midge_core::io::FsError::NotFound(_)) => {}
        Err(error) => tracing::warn!(%error, "retaining non-authoritative flush staging file"),
    }
}

fn validate_final_sst(
    task: &FlushPublishTask,
    path: &Path,
    expected: &cntryl_midge_durability::metadata::intent::FileMeta,
    budget: &cntryl_midge_core::common::resource_budget::ResourceBudget,
) -> MidgeResult<()> {
    // A freshly written SST always carries both proofs, so anything missing
    // here is a defect in the writer rather than an older manifest.
    cntryl_midge_table::sst::fs::identity_with_fs(&task.fs, path)?
        .verify_against(
            expected.expected_sst(),
            None,
            cntryl_midge_table::sst::identity::ProofPolicy::Required,
        )
        .map_err(|mismatch| {
            MidgeError::Corruption(format!(
                "staged SST identity changed at '{}': {mismatch}",
                path.display()
            ))
        })?;
    let fs_path = db_relative_fs_path(task, path)?;
    cntryl_midge_table::sst::fs::SstFileIo::open_for_compaction(
        &fs_path.0,
        Arc::clone(&task.fs),
        budget.clone(),
    )
    .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct PublicationTestFs {
        inner: cntryl_midge_core::io::RealFs,
        fail_rename: bool,
        fail_sync: AtomicBool,
        sync_dir_calls: parking_lot::Mutex<
            Vec<(
                cntryl_midge_core::io::FsPath,
                cntryl_midge_core::io::Durability,
            )>,
        >,
    }

    impl PublicationTestFs {
        fn set_sync_dir_failure(&self, fail: bool) {
            self.fail_sync.store(fail, Ordering::Relaxed);
        }

        fn sync_dir_calls(
            &self,
        ) -> Vec<(
            cntryl_midge_core::io::FsPath,
            cntryl_midge_core::io::Durability,
        )> {
            self.sync_dir_calls.lock().clone()
        }
    }

    impl cntryl_midge_core::io::ReadFs for PublicationTestFs {
        fn open_read(
            &self,
            path: &cntryl_midge_core::io::FsPath,
        ) -> cntryl_midge_core::io::FsResult<Box<dyn cntryl_midge_core::io::ReadFile + '_>>
        {
            cntryl_midge_core::io::Fs::open(
                self,
                path,
                cntryl_midge_core::io::OpenOptions::READ_ONLY,
            )
            .map(|file| file as Box<dyn cntryl_midge_core::io::ReadFile>)
        }

        fn exists(
            &self,
            path: &cntryl_midge_core::io::FsPath,
        ) -> cntryl_midge_core::io::FsResult<bool> {
            cntryl_midge_core::io::ReadFs::exists(&self.inner, path)
        }

        fn metadata(
            &self,
            path: &cntryl_midge_core::io::FsPath,
        ) -> cntryl_midge_core::io::FsResult<cntryl_midge_core::io::traits::Metadata> {
            cntryl_midge_core::io::ReadFs::metadata(&self.inner, path)
        }

        fn list_dir(
            &self,
            path: &cntryl_midge_core::io::FsPath,
        ) -> cntryl_midge_core::io::FsResult<Vec<cntryl_midge_core::io::traits::DirEntry>> {
            cntryl_midge_core::io::ReadFs::list_dir(&self.inner, path)
        }
    }

    impl cntryl_midge_core::io::PersistentHandleFs for PublicationTestFs {
        fn open_persistent(
            &self,
            path: &cntryl_midge_core::io::FsPath,
            options: cntryl_midge_core::io::OpenOptions,
        ) -> cntryl_midge_core::io::FsResult<Box<dyn cntryl_midge_core::io::File>> {
            cntryl_midge_core::io::open_persistent_handle(&self.inner, path, options)
        }
    }

    impl cntryl_midge_core::io::Fs for PublicationTestFs {
        fn try_lock_exclusive(
            &self,
            path: &cntryl_midge_core::io::FsPath,
        ) -> cntryl_midge_core::io::FsResult<Box<dyn cntryl_midge_core::io::FsLock>> {
            self.inner.try_lock_exclusive(path)
        }

        fn persistent_handle_capability(
            &self,
        ) -> Option<&dyn cntryl_midge_core::io::PersistentHandleFs> {
            self.inner
                .persistent_handle_capability()
                .map(|_| self as &dyn cntryl_midge_core::io::PersistentHandleFs)
        }

        fn host_addressing(&self) -> Option<cntryl_midge_core::io::HostAddressing<'_>> {
            cntryl_midge_core::io::Fs::host_addressing(&self.inner)
        }

        fn coordination_key(&self) -> u64 {
            cntryl_midge_core::io::Fs::coordination_key(&self.inner)
        }

        fn open(
            &self,
            path: &cntryl_midge_core::io::FsPath,
            options: cntryl_midge_core::io::OpenOptions,
        ) -> cntryl_midge_core::io::FsResult<Box<dyn cntryl_midge_core::io::File + '_>> {
            cntryl_midge_core::io::Fs::open(&self.inner, path, options)
        }

        fn remove_file(
            &self,
            path: &cntryl_midge_core::io::FsPath,
        ) -> cntryl_midge_core::io::FsResult<()> {
            cntryl_midge_core::io::Fs::remove_file(&self.inner, path)
        }

        fn create_dir_all(
            &self,
            path: &cntryl_midge_core::io::FsPath,
        ) -> cntryl_midge_core::io::FsResult<()> {
            cntryl_midge_core::io::Fs::create_dir_all(&self.inner, path)
        }

        fn remove_dir_all(
            &self,
            path: &cntryl_midge_core::io::FsPath,
        ) -> cntryl_midge_core::io::FsResult<()> {
            cntryl_midge_core::io::Fs::remove_dir_all(&self.inner, path)
        }

        fn sync_dir(
            &self,
            path: &cntryl_midge_core::io::FsPath,
            durability: cntryl_midge_core::io::Durability,
        ) -> cntryl_midge_core::io::FsResult<()> {
            self.sync_dir_calls.lock().push((path.clone(), durability));
            if self.fail_sync.load(Ordering::Relaxed) {
                return Err(cntryl_midge_core::io::FsError::Unavailable(
                    "injected directory sync failure".to_string(),
                ));
            }
            cntryl_midge_core::io::Fs::sync_dir(&self.inner, path, durability)
        }

        fn rename_atomic(
            &self,
            from: &cntryl_midge_core::io::FsPath,
            to: &cntryl_midge_core::io::FsPath,
        ) -> cntryl_midge_core::io::FsResult<()> {
            if self.fail_rename {
                Err(cntryl_midge_core::io::FsError::Unavailable(format!(
                    "injected rename failure from {from} to {to}"
                )))
            } else {
                cntryl_midge_core::io::Fs::rename_atomic(&self.inner, from, to)
            }
        }
    }

    struct ScriptedLeaderStore {
        expected_epoch: u64,
        fail_at_validation: usize,
        validations: AtomicUsize,
    }

    impl cntryl_midge_durability::lease::LeaderStore for ScriptedLeaderStore {
        fn acquire_leadership(
            &self,
            _holder_id: &str,
        ) -> Result<
            cntryl_midge_durability::lease::LeaderRecord,
            cntryl_midge_durability::lease::LeaseError,
        > {
            Err(
                cntryl_midge_durability::lease::LeaseError::AcquisitionFailed(
                    "not used by flush publication tests".to_string(),
                ),
            )
        }

        fn read_current(
            &self,
        ) -> Result<
            Option<cntryl_midge_durability::lease::LeaderRecord>,
            cntryl_midge_durability::lease::LeaseError,
        > {
            let validation = self.validations.fetch_add(1, Ordering::SeqCst) + 1;
            let epoch = if validation == self.fail_at_validation {
                self.expected_epoch + 1
            } else {
                self.expected_epoch
            };
            Ok(Some(cntryl_midge_durability::lease::LeaderRecord {
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
        hybrid_storage: Arc<cntryl_midge_storage::HybridStorage>,
        sst_backend: Arc<cntryl_midge_storage::cloud::MockCloudBackend>,
        control_backend: Arc<cntryl_midge_storage::cloud::MockCloudBackend>,
        metadata_publication_lock: crate::runtime::MetadataPublicationLock,
    }

    fn publication_fixture(fail_at_validation: usize) -> MidgeResult<PublicationFixture> {
        let directory = tempfile::tempdir()?;
        let db_path = directory.path().join("db");
        cntryl_midge_durability::metadata::ensure_or_create_format_marker(
            &cntryl_midge_core::io::real::test_root(&db_path),
            &db_path,
        )?;
        let sst_dir = db_path.join("sst");
        std::fs::create_dir_all(&sst_dir)?;
        let staging_path = db_path.join("staging").join("flush-1.sst");
        let identity = FlushIdentity {
            flush_id: 1,
            writer_epoch: 7,
            cf_id: 0,
            sequence: 1,
        };
        let memtable = Arc::new(cntryl_midge_table::memtable::SkipListMemtable::new());
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
        let fs: Arc<dyn cntryl_midge_core::io::Fs> =
            Arc::new(cntryl_midge_core::io::RealFs::new(&db_path).map_err(FsError::into_midge)?);
        let sst_factory = Arc::new(
            cntryl_midge_table::sst::FsSstFactoryIo::new(fs, 64 * 1024)
                .with_compression_policy(cntryl_midge_core::codec::CompressionPolicy::default()),
        );
        let file_meta = FlushActor::write_memtable_to_staging(&sst_factory, &mut build_task)?;

        let sst_backend = Arc::new(cntryl_midge_storage::cloud::MockCloudBackend::new());
        let sst_cloud = Arc::new(cntryl_midge_storage::cloud::CloudStorage::new(
            sst_backend.clone(),
            String::new(),
        ));
        let hybrid = Arc::new(cntryl_midge_storage::HybridStorage::with_policy(
            sst_cloud,
            cntryl_midge_storage::hybrid::policy::StorageBudgetPolicy::default(),
        ));
        let control_backend = Arc::new(cntryl_midge_storage::cloud::MockCloudBackend::new());
        let control_cloud = Arc::new(cntryl_midge_storage::cloud::CloudStorage::new(
            control_backend.clone(),
            String::new(),
        ));
        let leader_store: Arc<dyn cntryl_midge_durability::lease::LeaderStore> =
            Arc::new(ScriptedLeaderStore {
                expected_epoch: identity.writer_epoch,
                fail_at_validation,
                validations: AtomicUsize::new(0),
            });
        let publication_fs: Arc<dyn cntryl_midge_core::io::Fs> =
            Arc::new(cntryl_midge_core::io::RealFs::new(&db_path).map_err(FsError::into_midge)?);
        let task = FlushPublishTask {
            build: FlushBuildOutput {
                identity,
                staging_path,
                file_meta,
                reservation: None,
            },
            sst_name: cntryl_midge_core::cloud_layout::file_name(identity.cf_id, 0, 1),
            sst_seq: 1,
            fs: publication_fs,
            storage: Arc::new(HybridFlushStorage::new(
                Some(Arc::clone(&hybrid)),
                Some(control_cloud),
            )),
            lease_validity: None,
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
            phase: FlushMirrorPhase::IntentCleared,
            delta,
            reservation: None,
            fs: Arc::clone(&fixture.task.fs),
            metadata_cut: None,
            storage: Arc::clone(&fixture.task.storage),
            metadata_publication_lock: fixture.metadata_publication_lock.clone(),
            lease_validity: fixture.task.lease_validity.clone(),
            lease_healthy: fixture.task.lease_healthy.clone(),
            leader_store: fixture.task.leader_store.clone(),
            leader_holder_id: fixture.task.leader_holder_id.clone(),
            deadline: cntryl_midge_core::common::OperationDeadline::from_budget(
                crate::config::DEFAULT_RUNTIME_RESPONSE_TIMEOUT,
            ),
        }
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_retain_disk_admission_when_scratch_unlink_fails() -> MidgeResult<()> {
        // Arrange
        let _test_guard = cntryl_midge_core::failpoints::test_failpoint_guard();
        let scenario = fail::FailScenario::setup();
        let fixture = publication_fixture(usize::MAX)?;
        let scratch = fixture.directory.path().join("failed-scratch");
        let factory = Arc::new(
            cntryl_midge_table::sst::FsSstFactoryIo::new(
                Arc::new(
                    cntryl_midge_core::io::RealFs::new(fixture.directory.path())
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
        let memtable = Arc::new(cntryl_midge_table::memtable::SkipListMemtable::new());
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
        for task in [
            FlushWorkerTask::Build(task.clone()),
            FlushWorkerTask::Build(task),
            FlushWorkerTask::Shutdown,
        ] {
            tx.send(SubmittedFlushTask {
                task,
                finished: Arc::new(AtomicBool::new(false)),
                gate: cntryl_midge_core::failpoints::worker_gate(),
            })
            .unwrap();
        }
        let budget = cntryl_midge_core::common::resource_budget::ResourceBudget::new(1024 * 1024);

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
        use cntryl_midge_storage::cloud::CloudWriteCapability;
        let fixture = publication_fixture(usize::MAX)?;
        let bytes = std::fs::read(&fixture.task.build.staging_path)?;
        let mut corrupted = bytes.clone();
        let middle = corrupted.len() / 2;
        corrupted[middle] ^= 1;
        let (tx, rx) = std::sync::mpsc::channel();
        fixture.sst_backend.submit_put(
            &cntryl_midge_core::cloud_layout::object_key(&fixture.task.sst_name),
            corrupted,
            Vec::new(),
            tx,
        );
        rx.recv().unwrap();
        fixture.sst_backend.clear_history();
        let budget = cntryl_midge_core::common::resource_budget::ResourceBudget::new(1024 * 1024);

        // Act
        let result = FlushActor::publish_with_budget(&fixture.task, &budget);

        // Assert
        assert!(matches!(result, Err(MidgeError::Corruption(_))));
        assert_eq!(fixture.sst_backend.get_uploads().len(), 0);
        assert_eq!(fixture.control_backend.get_uploads().len(), 0);
        assert_eq!(
            std::fs::read(
                fixture
                    .db_path
                    .join(cntryl_midge_table::sst::fs::sst_location(
                        &fixture.task.sst_name
                    ))
            )?,
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
        let budget = cntryl_midge_core::common::resource_budget::ResourceBudget::new(128 * 1024);

        // Act
        let result = FlushActor::publish_with_budget(&fixture.task, &budget);

        // Assert
        assert!(matches!(result, Err(MidgeError::ResourceLimit(_))));
        assert!(fixture
            .db_path
            .join(cntryl_midge_table::sst::fs::sst_location(
                &fixture.task.sst_name
            ))
            .exists());
        assert_eq!(fixture.sst_backend.get_uploads().len(), 0);
        assert_eq!(fixture.control_backend.get_uploads().len(), 0);
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
        let budget = cntryl_midge_core::common::resource_budget::ResourceBudget::new(1024 * 1024);

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
            cntryl_midge_durability::metadata::files::JOURNAL,
            cntryl_midge_durability::metadata::files::MANIFEST_SNAPSHOT,
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
        let mut manifest = cntryl_midge_durability::metadata::Manifest::default();
        manifest.next_sst_seqs.insert(0, 100);
        cntryl_midge_durability::metadata::ManifestPersistence::save_snapshot_and_truncate_journal(
            &fixture.db_path,
            &manifest,
        )
        .map_err(MidgeError::Internal)?;

        // Act
        let delta = FlushActor::publish(&fixture.task)?;
        let persisted =
            cntryl_midge_durability::metadata::ManifestPersistence::load(&fixture.db_path)
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
        let memtable = Arc::new(cntryl_midge_table::memtable::SkipListMemtable::new());
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
            cntryl_midge_core::io::RealFs::new(fixture.directory.path())
                .map_err(FsError::into_midge)?,
        );
        let factory: Arc<cntryl_midge_table::sst::FsSstFactoryIo> =
            Arc::new(cntryl_midge_table::sst::FsSstFactoryIo::new(fs, 64 * 1024));

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
            inner: cntryl_midge_core::io::RealFs::new(&fixture.db_path)
                .map_err(FsError::into_midge)?,
            fail_rename: false,
            fail_sync: AtomicBool::new(false),
            sync_dir_calls: parking_lot::Mutex::new(Vec::new()),
        });
        sync_fs.set_sync_dir_failure(true);
        fixture.task.fs = sync_fs.clone();
        let final_path = fixture
            .db_path
            .join(cntryl_midge_table::sst::fs::sst_location(
                &fixture.task.sst_name,
            ));

        // Act
        let error = finalize_staged_sst(
            &fixture.task,
            &final_path,
            &cntryl_midge_core::common::resource_budget::ResourceBudget::new(
                DEFAULT_FLUSH_MEMORY_BYTES,
            ),
        )
        .expect_err("directory durability failure must fail SST finalization");
        sync_fs.set_sync_dir_failure(false);
        finalize_staged_sst(
            &fixture.task,
            &final_path,
            &cntryl_midge_core::common::resource_budget::ResourceBudget::new(
                DEFAULT_FLUSH_MEMORY_BYTES,
            ),
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
                    cntryl_midge_core::io::FsPath::new("sst"),
                    cntryl_midge_core::io::Durability::Durable
                ),
                (
                    cntryl_midge_core::io::FsPath::new("sst"),
                    cntryl_midge_core::io::Durability::Durable
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
            inner: cntryl_midge_core::io::RealFs::new(&fixture.db_path)
                .map_err(FsError::into_midge)?,
            fail_rename: true,
            fail_sync: AtomicBool::new(false),
            sync_dir_calls: parking_lot::Mutex::new(Vec::new()),
        });
        let final_path = fixture
            .db_path
            .join(cntryl_midge_table::sst::fs::sst_location(
                &fixture.task.sst_name,
            ));

        // Act
        let error = finalize_staged_sst(
            &fixture.task,
            &final_path,
            &cntryl_midge_core::common::resource_budget::ResourceBudget::new(
                DEFAULT_FLUSH_MEMORY_BYTES,
            ),
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
            &cntryl_midge_core::io::real::test_root(directory.path()),
            false,
            &cntryl_midge_core::codec::CompressionPolicy::default(),
            &completion_tx,
        )?;

        // Assert
        assert_eq!(actor.worker_handles.len(), 1);
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
            &(Arc::new(cntryl_midge_core::io::MockFs::new()) as Arc<dyn cntryl_midge_core::io::Fs>),
            true,
            &cntryl_midge_core::codec::CompressionPolicy::default(),
            &completion_tx,
        )?;

        // Assert
        assert!(actor.worker_handles.is_empty());
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
        assert_eq!(fixture.sst_backend.get_uploads().len(), 0);
        assert!(!fixture.directory.path().join("db/intent_log.json").exists());
        Ok(())
    }

    #[test]
    fn should_reject_flush_publication_when_validity_expires_without_watchdog() -> MidgeResult<()> {
        // Arrange
        let mut fixture = publication_fixture(usize::MAX)?;
        let validity = Arc::new(cntryl_midge_durability::lease::LeaseValidity::new());
        validity
            .activate(7, Instant::now() + std::time::Duration::from_mins(1))
            .unwrap();
        fixture.task.lease_validity = Some(Arc::clone(&validity));
        validity.expire_for_test();
        // Act
        let result = FlushActor::publish(&fixture.task);
        // Assert
        assert!(matches!(result, Err(MidgeError::Fenced(_))));
        assert_eq!(fixture.sst_backend.get_uploads().len(), 0);
        assert!(fixture.task.build.staging_path.exists());
        Ok(())
    }

    #[test]
    fn should_reject_control_mirror_when_validity_expires_after_local_publication(
    ) -> MidgeResult<()> {
        // Arrange
        let mut fixture = publication_fixture(usize::MAX)?;
        let validity = Arc::new(cntryl_midge_durability::lease::LeaseValidity::new());
        validity
            .activate(7, Instant::now() + std::time::Duration::from_mins(1))
            .unwrap();
        fixture.task.lease_validity = Some(Arc::clone(&validity));
        let delta = FlushActor::publish(&fixture.task)?;
        let task = mirror_task(&fixture, delta);
        validity.expire_for_test();
        // Act
        let result = mirror_after_local_commit(&task);
        // Assert
        assert!(matches!(result, Err(MidgeError::Fenced(_))));
        assert_eq!(fixture.control_backend.get_uploads().len(), 0);
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
        assert!(
            cntryl_midge_durability::metadata::ManifestPersistence::load(&db_path)
                .map_err(MidgeError::Internal)?
                .files
                .is_empty()
        );
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
        assert!(
            cntryl_midge_durability::metadata::ManifestPersistence::load(&db_path)
                .map_err(MidgeError::Internal)?
                .files
                .iter()
                .any(|file| file.name == sst_name)
        );
        assert!(
            cntryl_midge_durability::metadata::intent_persistence::IntentPersistence::load(
                &db_path
            )
            .map_err(MidgeError::Internal)?
            .is_empty()
        );
        assert_eq!(fixture.control_backend.get_uploads().len(), 0);
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
        assert!(
            cntryl_midge_durability::metadata::ManifestPersistence::load(&db_path)
                .map_err(MidgeError::Internal)?
                .files
                .is_empty()
        );
        assert_eq!(fixture.control_backend.get_uploads().len(), 0);
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
        task.deadline = cntryl_midge_core::common::OperationDeadline::from_budget(
            std::time::Duration::from_millis(100),
        );
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
        let _test_guard = cntryl_midge_core::failpoints::test_failpoint_guard();
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
