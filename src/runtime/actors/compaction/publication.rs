//! Lease-fenced compaction publication worker.
//!
//! The event loop owns the manifest mutation and read-view installation. This
//! worker owns only full-file and provider work, returning a small completion
//! after each durable publication boundary.

use super::PreparedCompactionOutput;
use crate::common::{MidgeError, MidgeResult, OperationDeadline};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

const COMPACTION_PUBLISH_CHANNEL_CAPACITY: usize = 1;

/// Exact operation identity carried across asynchronous publication phases.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompactionPublicationToken {
    pub request_id: u64,
    pub writer_epoch: u64,
    pub cf_id: crate::types::ColumnFamilyId,
    pub target_level: u32,
    pub output_generation: u64,
    pub input_ssts: Vec<String>,
    pub output_ssts: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompactionPublishPhase {
    /// The `OutputDurable` intent is local; prove outputs and mirror that intent.
    OutputDurable,
    /// The manifest batch is local; mirror its new control state.
    ManifestPublished,
    /// The local intent was cleared after snapshot/GC handoff; mirror that clear.
    IntentCleared,
}

#[derive(Clone)]
pub(crate) struct CompactionPublishTask {
    pub token: CompactionPublicationToken,
    pub phase: CompactionPublishPhase,
    pub outputs: Vec<PreparedCompactionOutput>,
    pub sst_dir: PathBuf,
    pub fs: Arc<dyn crate::io::Fs>,
    pub hybrid_storage: Option<Arc<crate::storage::HybridStorage>>,
    pub cloud_metadata_storage: Option<Arc<crate::storage::cloud::CloudStorage>>,
    pub metadata_publication_lock: crate::runtime::MetadataPublicationLock,
    pub lease_healthy: Option<Arc<AtomicBool>>,
    pub leader_store: Option<Arc<dyn crate::lease::LeaderStore>>,
    pub leader_holder_id: Option<String>,
    pub metadata_sequence: u64,
    pub publication_memory_limit: usize,
    pub runtime_response_timeout: std::time::Duration,
}

pub(crate) struct CompactionPublishCompletion {
    pub token: CompactionPublicationToken,
    pub phase: CompactionPublishPhase,
    pub result: MidgeResult<()>,
}

enum CompactionPublishWorkerTask {
    Publish(Box<CompactionPublishTask>),
    Shutdown,
}

/// One bounded publication worker. It is intentionally distinct from the
/// flush worker: a busy flush build must not make compaction completion block
/// the runtime thread while it waits to enqueue publication I/O.
pub(crate) struct CompactionPublishActor {
    in_progress: bool,
    inline: bool,
    completion_tx: crossbeam::channel::Sender<CompactionPublishCompletion>,
    task_tx: Option<crossbeam::channel::Sender<CompactionPublishWorkerTask>>,
    worker_handle: Option<JoinHandle<()>>,
}

impl CompactionPublishActor {
    pub(crate) fn new(
        completion_tx: crossbeam::channel::Sender<CompactionPublishCompletion>,
        inline: bool,
    ) -> MidgeResult<Self> {
        if inline {
            return Ok(Self {
                in_progress: false,
                inline: true,
                completion_tx,
                task_tx: None,
                worker_handle: None,
            });
        }

        let (task_tx, task_rx) = crossbeam::channel::bounded(COMPACTION_PUBLISH_CHANNEL_CAPACITY);
        let worker_completion_tx = completion_tx.clone();
        let worker_handle = std::thread::Builder::new()
            .name("midge-compaction-publish".to_string())
            .spawn(move || Self::worker_loop(&task_rx, &worker_completion_tx))
            .map_err(|error| {
                MidgeError::Internal(format!("spawn compaction publisher: {error}"))
            })?;

        Ok(Self {
            in_progress: false,
            inline: false,
            completion_tx,
            task_tx: Some(task_tx),
            worker_handle: Some(worker_handle),
        })
    }

    pub(crate) const fn is_inflight(&self) -> bool {
        self.in_progress
    }

    pub(crate) const fn is_inline(&self) -> bool {
        self.inline
    }

    pub(crate) fn submit(&mut self, task: CompactionPublishTask) -> MidgeResult<()> {
        if self.in_progress {
            return Err(MidgeError::Busy(
                "compaction publication worker already has an in-flight task".to_string(),
            ));
        }
        if self.inline {
            self.in_progress = true;
            Self::send_completion(&self.completion_tx, &task);
            return Ok(());
        }

        self.task_tx
            .as_ref()
            .ok_or_else(|| {
                MidgeError::Internal("compaction publication worker is unavailable".to_string())
            })?
            .try_send(CompactionPublishWorkerTask::Publish(Box::new(task)))
            .map_err(|error| {
                MidgeError::Busy(format!(
                    "compaction publication worker queue is full: {error}"
                ))
            })?;
        self.in_progress = true;
        Ok(())
    }

    pub(crate) fn finish_task(&mut self) {
        self.in_progress = false;
    }

    pub(crate) fn shutdown_and_join(&mut self) -> MidgeResult<()> {
        if let Some(task_tx) = self.task_tx.take() {
            task_tx
                .send(CompactionPublishWorkerTask::Shutdown)
                .map_err(|error| {
                    MidgeError::Internal(format!("stop compaction publisher: {error}"))
                })?;
        }
        if let Some(handle) = self.worker_handle.take() {
            handle.join().map_err(|_| {
                MidgeError::Internal("compaction publication worker panicked".to_string())
            })?;
        }
        self.in_progress = false;
        Ok(())
    }

    fn worker_loop(
        task_rx: &crossbeam::channel::Receiver<CompactionPublishWorkerTask>,
        completion_tx: &crossbeam::channel::Sender<CompactionPublishCompletion>,
    ) {
        while let Ok(task) = task_rx.recv() {
            match task {
                CompactionPublishWorkerTask::Publish(task) => {
                    Self::send_completion(completion_tx, &task);
                }
                CompactionPublishWorkerTask::Shutdown => break,
            }
        }
    }

    fn send_completion(
        completion_tx: &crossbeam::channel::Sender<CompactionPublishCompletion>,
        task: &CompactionPublishTask,
    ) {
        let token = task.token.clone();
        let phase = task.phase;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_task(task)))
            .unwrap_or_else(|panic| {
                tracing::error!(?panic, "compaction publication task panicked");
                Err(MidgeError::Internal(
                    "compaction publication task panicked".to_string(),
                ))
            });
        let _ = completion_tx.send(CompactionPublishCompletion {
            token,
            phase,
            result,
        });
    }
}

fn run_task(task: &CompactionPublishTask) -> MidgeResult<()> {
    let deadline = OperationDeadline::from_budget(task.runtime_response_timeout);
    match task.phase {
        CompactionPublishPhase::OutputDurable => {
            validate_task_lease(task, &deadline)?;
            mirror_control_metadata(task, &deadline)?;
            validate_task_lease(task, &deadline)?;
            verify_or_stage_outputs(task, &deadline)?;
            crate::failpoints::fail_point!(
                "slice7::after_compaction_output_durable_before_manifest_publish"
            );
        }
        CompactionPublishPhase::ManifestPublished => {
            validate_task_lease(task, &deadline)?;
            mirror_control_metadata(task, &deadline)?;
            // This is the last provider-authority observation before the
            // event loop may retire inputs. The loop checks only the shared
            // health flag so it does not reintroduce a provider round trip.
            validate_task_lease(task, &deadline)?;
        }
        CompactionPublishPhase::IntentCleared => {
            validate_task_lease(task, &deadline)?;
            mirror_control_metadata(task, &deadline)?;
        }
    }
    Ok(())
}

fn validate_task_lease(
    task: &CompactionPublishTask,
    deadline: &OperationDeadline,
) -> MidgeResult<()> {
    if let Some(healthy) = &task.lease_healthy {
        if !healthy.load(Ordering::Acquire) {
            return Err(MidgeError::Fenced(
                "lease heartbeat reports unhealthy — refusing compaction publication".to_string(),
            ));
        }
    }
    if deadline.is_expired() {
        return Err(MidgeError::Timeout(
            "operation deadline exhausted before compaction lease validation".to_string(),
        ));
    }
    let Some(store) = &task.leader_store else {
        return Ok(());
    };
    let holder_id = task.leader_holder_id.as_deref().unwrap_or_default();
    store
        .validate_epoch_with_timeout(holder_id, task.token.writer_epoch, deadline.remaining())
        .map_err(|error| {
            let mapped = if deadline.is_expired() {
                MidgeError::Timeout(format!(
                    "compaction lease validation exceeded the operation deadline: {error}"
                ))
            } else {
                error.into_validation_error("compaction lease validation failed")
            };
            if matches!(mapped, MidgeError::Fenced(_)) {
                if let Some(healthy) = &task.lease_healthy {
                    healthy.store(false, Ordering::Release);
                }
                tracing::error!(%mapped, "compaction publication lease validation failed; runtime fenced");
            }
            mapped
        })
}

fn verify_or_stage_outputs(
    task: &CompactionPublishTask,
    deadline: &OperationDeadline,
) -> MidgeResult<()> {
    let Some(hybrid) = &task.hybrid_storage else {
        return Ok(());
    };
    let budget = crate::common::resource_budget::ResourceBudget::new(task.publication_memory_limit);
    let mut proofs = Vec::new();
    for output in &task.outputs {
        if let Some(proof) = output.proof.clone() {
            proofs.push(proof);
            continue;
        }
        let checksum = output.metadata.content_crc32c.ok_or_else(|| {
            MidgeError::Corruption(format!(
                "compaction output '{}' lacks a checksum for worker staging",
                output.metadata.name
            ))
        })?;
        hybrid.publish_immutable_file(
            &crate::cloud_layout::object_key(&output.metadata.name),
            &task.sst_dir.join(&output.metadata.name),
            output.metadata.size_bytes,
            checksum,
            &budget,
        )?;
    }
    if !proofs.is_empty() {
        hybrid.verify_remote_object_guards_within(&proofs, deadline)?;
    }
    Ok(())
}

fn mirror_control_metadata(
    task: &CompactionPublishTask,
    deadline: &OperationDeadline,
) -> MidgeResult<()> {
    let Some(cloud) = &task.cloud_metadata_storage else {
        return Ok(());
    };
    crate::runtime::hybrid_persistence::mirror_control_metadata_within(
        cloud,
        task.fs.as_ref(),
        &task.metadata_publication_lock,
        cloud.callback_timeout(),
        task.metadata_sequence,
        deadline,
        |deadline| validate_task_lease(task, deadline),
    )
}

#[cfg(test)]
pub(crate) fn checksummed_file_crc_for_publication_test(
    path: &std::path::Path,
    budget: &crate::common::resource_budget::ResourceBudget,
) -> MidgeResult<u32> {
    const CRC_BUFFER_SIZE: usize = 64 * 1024;
    let _reservation = budget.reserve(CRC_BUFFER_SIZE, "SST checksum buffer")?;
    Ok(crate::sst::identity::SstIdentity::of_path(path)?.crc32c)
}
