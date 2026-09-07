//! Persistent, lease-fenced memtable flush worker.
//!
//! The runtime thread owns freeze/install decisions. This actor owns one
//! bounded worker thread that performs only immutable build/publication I/O and
//! returns deltas for the runtime to validate and install.

use crate::common::{MidgeError, MidgeResult};
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

pub(crate) struct FlushBuildCompletion {
    pub identity: FlushIdentity,
    pub memtable: Arc<crate::sst::SkipListMemtable>,
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
    pub db_path: PathBuf,
    pub sst_dir: PathBuf,
    pub fs: Arc<dyn crate::io::Fs>,
    pub recovery_policy: crate::config::RecoveryPolicy,
    pub hybrid_storage: Option<Arc<crate::storage::HybridStorage>>,
    pub cloud_metadata_storage: Option<Arc<crate::storage::cloud::CloudStorage>>,
    pub lease_healthy: Option<Arc<AtomicBool>>,
    pub leader_store: Option<Arc<dyn crate::lease::LeaderStore>>,
    pub leader_holder_id: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct FlushPublicationDelta {
    pub identity: FlushIdentity,
    pub file_meta: crate::runtime::FileMeta,
    pub next_sst_seq: u64,
    pub cloud_metadata_published: bool,
    pub persistence_anomaly: bool,
}

pub(crate) struct FlushPublishCompletion {
    pub identity: FlushIdentity,
    pub reservation: Option<crate::storage::hybrid::actor::StorageReservationToken>,
    pub publish_ns: u64,
    pub result: MidgeResult<FlushPublicationDelta>,
}

pub(crate) enum FlushWorkerResult {
    Build(FlushBuildCompletion),
    Publish(FlushPublishCompletion),
}

#[derive(Clone)]
struct FlushBuildTask {
    identity: FlushIdentity,
    memtable: Arc<crate::sst::SkipListMemtable>,
    staging_path: PathBuf,
    reservation: Option<crate::storage::hybrid::actor::StorageReservationToken>,
    hybrid_storage: Option<Arc<crate::storage::HybridStorage>>,
}

enum FlushWorkerTask {
    Build(FlushBuildTask),
    Publish(Box<FlushPublishTask>),
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
        compression_policy: crate::sst::compression::CompressionPolicy,
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
        compression_policy: crate::sst::compression::CompressionPolicy,
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

        let fs = Arc::new(crate::io::RealFs::new(sst_dir)?);
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
        memtable: Arc<crate::sst::SkipListMemtable>,
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
                if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                    telemetry.metrics().record_write_stall_cloud();
                }
                Err(MidgeError::WriteStall(format!(
                    "column family {cf_id} flush is waiting for cloud upload capacity"
                )))
            }
            Err(crate::storage::hybrid::actor::ReservationResult::WaitForCompaction) => {
                if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                    telemetry.metrics().record_write_stall_compaction();
                }
                Err(MidgeError::WriteStall(format!(
                    "column family {cf_id} flush is waiting for compaction capacity"
                )))
            }
            Err(crate::storage::hybrid::actor::ReservationResult::RejectNoSpace) => {
                if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                    telemetry.metrics().record_no_space_event();
                    telemetry.metrics().record_write_stall_no_space();
                }
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
        persist_output_durable_intent(task, &file_meta)?;

        crate::failpoints::fail_point!("midge::flush_worker::before_manifest_persist");
        validate_task_lease(task)?;
        let mut manifest = load_manifest(task)?;
        let manifest_meta = runtime_to_manifest_meta(&file_meta);
        let next_sst_seq = task
            .sst_seq
            .checked_add(1)
            .ok_or_else(|| MidgeError::ResourceLimit("SST sequence space exhausted".to_string()))?
            .max(
                manifest
                    .next_sst_seqs
                    .get(&task.build.identity.cf_id)
                    .copied()
                    .unwrap_or(1),
            );
        match manifest
            .files
            .iter()
            .find(|file| file.name == task.sst_name)
        {
            Some(existing) if same_manifest_file(existing, &manifest_meta) => {}
            Some(_) => {
                return Err(MidgeError::Corruption(format!(
                    "flush identity {} conflicts with existing SST {}",
                    task.build.identity.flush_id, task.sst_name
                )))
            }
            None => {
                crate::metadata::append_edit_batch(
                    &task.db_path,
                    &[
                        crate::metadata::ManifestEdit::BumpNextSstSeq {
                            cf_id: task.build.identity.cf_id,
                            next_seq: next_sst_seq,
                        },
                        crate::metadata::ManifestEdit::AddSst(manifest_meta.clone()),
                    ],
                )?;
                manifest
                    .next_sst_seqs
                    .insert(task.build.identity.cf_id, next_sst_seq);
                manifest.add_file(manifest_meta);
            }
        }
        manifest
            .next_sst_seqs
            .entry(task.build.identity.cf_id)
            .and_modify(|next| *next = (*next).max(next_sst_seq))
            .or_insert(next_sst_seq);
        manifest.last_persisted_sequence = manifest
            .last_persisted_sequence
            .max(task.build.identity.sequence);
        crate::failpoints::fail_point!("midge::flush_worker::after_manifest_journal");

        validate_task_lease(task)?;
        clear_manifest_published_intent(task, &task.sst_name)?;
        let persistence_anomaly =
            match crate::metadata::ManifestPersistence::save_snapshot_and_truncate_journal(
                &task.db_path,
                &manifest,
            ) {
                Ok(()) => false,
                Err(error) if task.cloud_metadata_storage.is_none() => {
                    tracing::warn!(%error, "manifest journal is durable but checkpoint save failed");
                    true
                }
                Err(error) => return Err(MidgeError::Internal(error)),
            };

        let cloud_metadata_published = if let Some(cloud) = &task.cloud_metadata_storage {
            mirror_control_metadata(task, cloud, manifest.last_persisted_sequence)?;
            true
        } else {
            // Filesystem-backed cloud simulation keeps control metadata in the
            // durable local database while HybridStorage owns remote WAL/SST
            // objects. The local manifest checkpoint above is its control
            // publication acknowledgement.
            task.hybrid_storage.is_some()
        };
        crate::failpoints::fail_point!("midge::flush_worker::after_control_metadata_publication");

        Ok(FlushPublicationDelta {
            identity: task.build.identity,
            file_meta,
            next_sst_seq,
            cloud_metadata_published,
            persistence_anomaly,
        })
    }
}

fn upload(
    task: &FlushPublishTask,
    final_path: &Path,
    budget: &crate::common::resource_budget::ResourceBudget,
) -> MidgeResult<()> {
    if let Some(hybrid) = &task.hybrid_storage {
        hybrid.publish_immutable_file(
            &crate::sst::object_key(&task.sst_name),
            final_path,
            task.build.file_meta.size_bytes,
            task.build
                .file_meta
                .content_crc32c
                .ok_or_else(|| MidgeError::Corruption("flush output lacks checksum".into()))?,
            budget,
        )?;
    }
    Ok(())
}

fn elapsed_ns(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn validate_task_lease(task: &FlushPublishTask) -> MidgeResult<()> {
    if let Some(healthy) = &task.lease_healthy {
        if !healthy.load(Ordering::Acquire) {
            return Err(MidgeError::Fenced(format!(
                "flush {} observed a failed lease heartbeat",
                task.build.identity.flush_id
            )));
        }
    }
    if let Some(store) = &task.leader_store {
        store
            .validate_epoch(
                task.leader_holder_id.as_deref().unwrap_or_default(),
                task.build.identity.writer_epoch,
            )
            .map_err(|error| MidgeError::Fenced(error.to_string()))?;
    }
    Ok(())
}

fn finalize_staged_sst(
    task: &FlushPublishTask,
    final_path: &Path,
    budget: &crate::common::resource_budget::ResourceBudget,
) -> MidgeResult<()> {
    if !final_path.exists() {
        let parent = final_path.parent().ok_or(MidgeError::InvalidPath)?;
        std::fs::create_dir_all(parent)?;
        std::fs::rename(&task.build.staging_path, final_path)?;
    }
    validate_final_sst(final_path, &task.build.file_meta, budget)?;
    task.fs.sync_dir(
        &crate::io::FsPath::new("sst"),
        crate::io::Durability::Durable,
    )?;
    cleanup_non_authoritative_staging(task);
    Ok(())
}

fn cleanup_non_authoritative_staging(task: &FlushPublishTask) {
    let Some(staging_dir) = task.build.staging_path.parent() else {
        return;
    };
    if staging_dir.file_name().and_then(|name| name.to_str()) != Some(".flush-staging") {
        return;
    }
    match std::fs::remove_file(&task.build.staging_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => tracing::warn!(%error, "retaining non-authoritative flush staging file"),
    }
    match std::fs::remove_dir(staging_dir) {
        Ok(()) => {}
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::DirectoryNotEmpty
            ) => {}
        Err(error) => tracing::warn!(%error, "retaining flush staging directory"),
    }
}

fn validate_final_sst(
    path: &Path,
    expected: &crate::runtime::FileMeta,
    budget: &crate::common::resource_budget::ResourceBudget,
) -> MidgeResult<()> {
    let (actual_size, actual_crc) = crate::sst::fs::file_identity(path)?;
    if actual_size != expected.size_bytes || expected.content_crc32c != Some(actual_crc) {
        return Err(MidgeError::Corruption(format!(
            "staged SST identity changed at '{}': size {actual_size}, crc {actual_crc}",
            path.display()
        )));
    }
    let parent = path.parent().ok_or(MidgeError::InvalidPath)?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(MidgeError::InvalidPath)?;
    let fs = Arc::new(crate::io::RealFs::new(parent)?);
    crate::sst::fs::SstFileIo::open_for_compaction(name, fs, budget.clone()).map(|_| ())
}

fn load_manifest(task: &FlushPublishTask) -> MidgeResult<crate::metadata::Manifest> {
    let real = crate::io::RealFs::new(&task.db_path)
        .map_err(|error| MidgeError::Internal(format!("open manifest filesystem: {error:?}")))?;
    let fs: Arc<dyn crate::io::Fs> = Arc::new(real);
    crate::metadata::ManifestPersistence::load_with_fs_and_policy(&fs, task.recovery_policy)
        .map_err(MidgeError::Internal)
}

fn load_intents(task: &FlushPublishTask) -> MidgeResult<Vec<crate::runtime::IntentLogEntry>> {
    let real = crate::io::RealFs::new(&task.db_path)
        .map_err(|error| MidgeError::Internal(format!("open intent filesystem: {error:?}")))?;
    let fs: Arc<dyn crate::io::Fs> = Arc::new(real);
    crate::runtime::IntentPersistence::load_with_fs_and_policy(&fs, task.recovery_policy)
        .map_err(MidgeError::Internal)
}

fn persist_output_durable_intent(
    task: &FlushPublishTask,
    file_meta: &crate::runtime::FileMeta,
) -> MidgeResult<()> {
    let mut intents = load_intents(task)?;
    intents.retain(|entry| {
        !matches!(
            entry,
            crate::runtime::IntentLogEntry::FlushPublish {
                file_meta: existing,
                ..
            } if existing.name == file_meta.name
        )
    });
    intents.push(crate::runtime::IntentLogEntry::FlushPublish {
        phase: crate::runtime::PublicationPhase::OutputDurable,
        cf_id: task.build.identity.cf_id,
        sequence: task.build.identity.sequence,
        file_meta: file_meta.clone(),
    });
    crate::runtime::IntentPersistence::save(&task.db_path, &intents).map_err(MidgeError::Internal)
}

fn clear_manifest_published_intent(task: &FlushPublishTask, sst_name: &str) -> MidgeResult<()> {
    let mut intents = load_intents(task)?;
    intents.retain(|entry| {
        !matches!(
            entry,
            crate::runtime::IntentLogEntry::FlushPublish { file_meta, .. }
                if file_meta.name == sst_name
        )
    });
    crate::runtime::IntentPersistence::save(&task.db_path, &intents).map_err(MidgeError::Internal)
}

fn runtime_to_manifest_meta(file: &crate::runtime::FileMeta) -> crate::metadata::FileMeta {
    crate::metadata::FileMeta {
        name: file.name.clone(),
        level: file.level,
        size_bytes: file.size_bytes,
        content_crc32c: file.content_crc32c,
        cf_id: file.cf_id,
        smallest_key: file.smallest_key.clone(),
        largest_key: file.largest_key.clone(),
        smallest_seq: file.smallest_seq,
        largest_seq: file.largest_seq,
        key_bounds_complete: file.key_bounds_complete,
        ..Default::default()
    }
}

fn same_manifest_file(left: &crate::metadata::FileMeta, right: &crate::metadata::FileMeta) -> bool {
    left.name == right.name
        && left.level == right.level
        && left.size_bytes == right.size_bytes
        && left.content_crc32c == right.content_crc32c
        && left.cf_id == right.cf_id
        && left.smallest_key == right.smallest_key
        && left.largest_key == right.largest_key
        && left.smallest_seq == right.smallest_seq
        && left.largest_seq == right.largest_seq
        && left.key_bounds_complete == right.key_bounds_complete
}

fn mirror_control_metadata(
    task: &FlushPublishTask,
    cloud: &crate::storage::cloud::CloudStorage,
    local_manifest_sequence: u64,
) -> MidgeResult<()> {
    let _publication_guard = cloud.lock_metadata_publication();
    for file_name in crate::storage::cloud::CLOUD_METADATA_FILES {
        let path = task.db_path.join(file_name);
        if !path.exists() {
            continue;
        }
        validate_task_lease(task)?;
        let data = std::fs::read(path)?;
        let key = crate::storage::cloud::cloud_metadata_key(file_name);
        conditional_metadata_put(cloud, file_name, &key, data, local_manifest_sequence)?;
    }
    Ok(())
}

fn conditional_metadata_put(
    cloud: &crate::storage::cloud::CloudStorage,
    file_name: &str,
    key: &str,
    data: Vec<u8>,
    local_manifest_sequence: u64,
) -> MidgeResult<()> {
    let headers = match blocking_head_optional(cloud, key).map_err(MidgeError::Internal)? {
        Some(metadata) => {
            let current = blocking_get_optional(cloud, key)
                .map_err(MidgeError::Internal)?
                .ok_or_else(|| {
                    MidgeError::Internal(format!("cloud metadata '{key}' disappeared after HEAD"))
                })?;
            if let Some(remote_sequence) = remote_manifest_sequence(file_name, &current)? {
                if remote_sequence > local_manifest_sequence {
                    return Err(MidgeError::Fenced(format!(
                        "remote {file_name} sequence {remote_sequence} is ahead of local {local_manifest_sequence}"
                    )));
                }
            }
            if current == data {
                return Ok(());
            }
            crate::storage::cloud::object_match_precondition_headers(
                &metadata.etag,
                metadata.generation.as_deref(),
            )
            .ok_or_else(|| {
                MidgeError::Internal(format!(
                    "cloud metadata '{key}' has no conditional identity token"
                ))
            })?
        }
        None => vec![("If-None-Match".to_string(), "*".to_string())],
    };
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.submit_put(key, data, headers, tx);
    match rx.recv_timeout(cloud.callback_timeout()) {
        Ok(crate::storage::cloud::CloudEvent::Put {
            result: crate::storage::cloud::CloudOutcome::Ok(()),
            ..
        }) => Ok(()),
        Ok(crate::storage::cloud::CloudEvent::Put {
            result: crate::storage::cloud::CloudOutcome::Err(error),
            ..
        }) => Err(MidgeError::Internal(format!(
            "cloud metadata put '{key}' failed: {error}"
        ))),
        Ok(other) => Err(MidgeError::Internal(format!(
            "unexpected cloud metadata put response: {other:?}"
        ))),
        Err(error) => Err(MidgeError::Timeout(format!(
            "cloud metadata put '{key}' timed out: {error}"
        ))),
    }
}

fn blocking_get_optional(
    cloud: &crate::storage::cloud::CloudStorage,
    key: &str,
) -> Result<Option<Vec<u8>>, String> {
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.submit_get(key, tx);
    match rx.recv_timeout(cloud.callback_timeout()) {
        Ok(crate::storage::cloud::CloudEvent::Get {
            result: crate::storage::cloud::CloudOutcome::Ok(data),
            ..
        }) => Ok(Some(data)),
        Ok(crate::storage::cloud::CloudEvent::Get {
            result: crate::storage::cloud::CloudOutcome::Err(error),
            ..
        }) if crate::storage::cloud::is_not_found_error(&error) => Ok(None),
        Ok(crate::storage::cloud::CloudEvent::Get {
            result: crate::storage::cloud::CloudOutcome::Err(error),
            ..
        }) => Err(error.to_string()),
        Ok(other) => Err(format!("unexpected cloud get response: {other:?}")),
        Err(error) => Err(format!("cloud get timed out: {error}")),
    }
}

fn blocking_head_optional(
    cloud: &crate::storage::cloud::CloudStorage,
    key: &str,
) -> Result<Option<crate::storage::cloud::ObjectMetadata>, String> {
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.submit_head(key, tx);
    match rx.recv_timeout(cloud.callback_timeout()) {
        Ok(crate::storage::cloud::CloudEvent::Head {
            result: crate::storage::cloud::CloudOutcome::Ok(metadata),
            ..
        }) => Ok(Some(metadata)),
        Ok(crate::storage::cloud::CloudEvent::Head {
            result: crate::storage::cloud::CloudOutcome::Err(error),
            ..
        }) if crate::storage::cloud::is_not_found_error(&error) => Ok(None),
        Ok(crate::storage::cloud::CloudEvent::Head {
            result: crate::storage::cloud::CloudOutcome::Err(error),
            ..
        }) => Err(error.to_string()),
        Ok(other) => Err(format!("unexpected cloud head response: {other:?}")),
        Err(error) => Err(format!("cloud head timed out: {error}")),
    }
}

fn remote_manifest_sequence(file_name: &str, data: &[u8]) -> MidgeResult<Option<u64>> {
    if !matches!(file_name, "manifest.json" | "manifest.snapshot.json") {
        return Ok(None);
    }
    let manifest: crate::metadata::Manifest = serde_json::from_slice(data)
        .map_err(|error| MidgeError::Corruption(format!("remote manifest JSON: {error}")))?;
    Ok(Some(manifest.last_persisted_sequence))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

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
        task: FlushPublishTask,
        sst_backend: Arc<crate::storage::cloud::MockCloudBackend>,
        control_backend: Arc<crate::storage::cloud::MockCloudBackend>,
    }

    fn publication_fixture(fail_at_validation: usize) -> MidgeResult<PublicationFixture> {
        let directory = tempfile::tempdir()?;
        let db_path = directory.path().join("db");
        let sst_dir = db_path.join("sst");
        std::fs::create_dir_all(&sst_dir)?;
        let staging_path = db_path.join("staging").join("flush-1.sst");
        let identity = FlushIdentity {
            flush_id: 1,
            writer_epoch: 7,
            cf_id: 0,
            sequence: 1,
        };
        let memtable = Arc::new(crate::sst::SkipListMemtable::new());
        memtable.put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)?;
        let mut build_task = FlushBuildTask {
            identity,
            memtable,
            staging_path: staging_path.clone(),
            reservation: None,
            hybrid_storage: None,
        };
        let fs: Arc<dyn crate::io::Fs> = Arc::new(crate::io::RealFs::new(&sst_dir)?);
        let sst_factory = Arc::new(
            crate::sst::FsSstFactoryIo::new(fs, 64 * 1024)
                .with_compression_policy(crate::sst::compression::CompressionPolicy::default()),
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
        let task = FlushPublishTask {
            build: FlushBuildOutput {
                identity,
                staging_path,
                file_meta,
                reservation: None,
            },
            sst_name: crate::sst::file_name(identity.cf_id, 0, 1),
            sst_seq: 1,
            db_path,
            sst_dir,
            fs: Arc::new(crate::io::MockFs::new()),
            recovery_policy: crate::config::RecoveryPolicy::Strict,
            hybrid_storage: Some(hybrid),
            cloud_metadata_storage: Some(control_cloud),
            lease_healthy: Some(Arc::new(AtomicBool::new(true))),
            leader_store: Some(leader_store),
            leader_holder_id: Some("flush-test".to_string()),
        };
        Ok(PublicationFixture {
            directory,
            task,
            sst_backend,
            control_backend,
        })
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
            crate::sst::FsSstFactoryIo::new(Arc::new(crate::io::MockFs::new()), 4096)
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
        let memtable = Arc::new(crate::sst::SkipListMemtable::new());
        memtable.put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)?;
        let task = FlushBuildTask {
            identity: fixture.task.build.identity,
            memtable,
            staging_path: fixture.directory.path().join("output.sst"),
            reservation: None,
            hybrid_storage: fixture.task.hybrid_storage.clone(),
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
                .task
                .hybrid_storage
                .as_ref()
                .unwrap()
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
            &crate::sst::object_key(&fixture.task.sst_name),
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
            .task
            .hybrid_storage
            .as_ref()
            .unwrap()
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
            .task
            .hybrid_storage
            .as_ref()
            .unwrap()
            .enable_ephemeral_sst_cache(1024 * 1024);
        let budget = crate::common::resource_budget::ResourceBudget::new(1024 * 1024);

        // Act
        let result = FlushActor::publish_with_budget(&fixture.task, &budget)?;

        // Assert
        assert!(result.cloud_metadata_published);
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
    fn should_preserve_reserved_names_when_publishing_an_earlier_sst() -> MidgeResult<()> {
        // Arrange
        let fixture = publication_fixture(usize::MAX)?;
        let mut manifest = crate::metadata::Manifest::default();
        manifest.next_sst_seqs.insert(0, 100);
        crate::metadata::ManifestPersistence::save_snapshot_and_truncate_journal(
            &fixture.task.db_path,
            &manifest,
        )
        .map_err(MidgeError::Internal)?;

        // Act
        let delta = FlushActor::publish(&fixture.task)?;
        let persisted = crate::metadata::ManifestPersistence::load(&fixture.task.db_path)
            .map_err(MidgeError::Internal)?;

        // Assert
        assert_eq!(delta.next_sst_seq, 100);
        assert_eq!(persisted.next_sst_seqs[&0], 100);
        assert_eq!(persisted.files.len(), 1);
        Ok(())
    }

    #[test]
    fn should_reject_flush_before_writing_sst_when_verification_staging_exceeds_disk_budget(
    ) -> MidgeResult<()> {
        // Arrange
        let fixture = publication_fixture(usize::MAX)?;
        let hybrid = fixture
            .task
            .hybrid_storage
            .as_ref()
            .expect("hybrid storage");
        // The final SST itself fits; its concurrent readback copy does not.
        hybrid.enable_ephemeral_sst_cache(fixture.task.build.file_meta.size_bytes);
        let memtable = Arc::new(crate::sst::SkipListMemtable::new());
        memtable.put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)?;
        let staging_path = fixture.directory.path().join("admitted/flush-2.sst");
        let mut task = FlushBuildTask {
            identity: fixture.task.build.identity,
            memtable,
            staging_path: staging_path.clone(),
            reservation: None,
            hybrid_storage: Some(Arc::clone(hybrid)),
        };
        let fs = Arc::new(crate::io::RealFs::new(&fixture.task.sst_dir)?);
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
        let sync_fs = Arc::new(crate::io::MockFs::new());
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
    fn should_initialize_one_persistent_flush_worker_when_local() -> MidgeResult<()> {
        // Arrange
        let directory = tempfile::tempdir()?;
        let (completion_tx, _completion_rx) = crossbeam::channel::unbounded();

        // Act
        let mut actor = FlushActor::new(
            directory.path(),
            false,
            crate::sst::compression::CompressionPolicy::default(),
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
            crate::sst::compression::CompressionPolicy::default(),
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
    fn should_fence_flush_before_manifest_persistence_when_epoch_changes() -> MidgeResult<()> {
        // Arrange
        let fixture = publication_fixture(4)?;
        let db_path = fixture.task.db_path.clone();

        // Act
        let error = FlushActor::publish(&fixture.task).expect_err("publication must be fenced");

        // Assert
        assert!(matches!(error, MidgeError::Fenced(_)));
        assert_eq!(fixture.sst_backend.get_uploads().len(), 1);
        assert!(crate::metadata::ManifestPersistence::load(&db_path)
            .map_err(MidgeError::Internal)?
            .files
            .is_empty());
        assert_eq!(
            crate::runtime::IntentPersistence::load(&db_path)
                .map_err(MidgeError::Internal)?
                .len(),
            1
        );
        Ok(())
    }

    #[test]
    fn should_recover_durable_publication_without_accepting_stale_completion() -> MidgeResult<()> {
        // Arrange
        let fixture = publication_fixture(5)?;
        let db_path = fixture.task.db_path.clone();
        let sst_name = fixture.task.sst_name.clone();

        // Act
        let error = FlushActor::publish(&fixture.task).expect_err("completion must be fenced");

        // Assert
        assert!(matches!(error, MidgeError::Fenced(_)));
        assert!(crate::metadata::ManifestPersistence::load(&db_path)
            .map_err(MidgeError::Internal)?
            .files
            .iter()
            .any(|file| file.name == sst_name));
        assert_eq!(
            crate::runtime::IntentPersistence::load(&db_path)
                .map_err(MidgeError::Internal)?
                .len(),
            1
        );
        assert!(fixture.control_backend.get_uploads().is_empty());
        Ok(())
    }

    #[test]
    fn should_fence_flush_before_control_metadata_publication_when_epoch_changes() -> MidgeResult<()>
    {
        // Arrange
        let fixture = publication_fixture(6)?;
        let db_path = fixture.task.db_path.clone();
        let sst_name = fixture.task.sst_name.clone();

        // Act
        let error = FlushActor::publish(&fixture.task).expect_err("publication must be fenced");

        // Assert
        assert!(matches!(error, MidgeError::Fenced(_)));
        assert!(crate::metadata::ManifestPersistence::load(&db_path)
            .map_err(MidgeError::Internal)?
            .files
            .iter()
            .any(|file| file.name == sst_name));
        assert!(fixture.control_backend.get_uploads().is_empty());
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
        let result = FlushActor::publish(&fixture.task);
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
