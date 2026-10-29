//! Submit immutable control-file cuts; only the event loop applies completion.

use super::{EventLoop, MetadataMirrorJob, MirrorContinuation};
use crate::common::{MidgeError, MidgeResult, OperationDeadline};
use std::sync::Arc;

struct MetadataControlWork {
    cloud: Arc<crate::storage::cloud::CloudStorage>,
    fs: Arc<dyn crate::io::Fs>,
    source: Option<Arc<crate::runtime::CapturedMetadataCut>>,
    publication_lock: crate::runtime::MetadataPublicationLock,
    fence: super::super::fencing::RuntimeFence,
    deadline: OperationDeadline,
    storage: Option<Arc<crate::storage::HybridStorage>>,
    sweep: Option<super::orphans::OrphanSweepTask>,
}

impl MetadataControlWork {
    fn run(self) -> MidgeResult<()> {
        let context = crate::runtime::hybrid_persistence::CloudMetadataMirrorContext {
            cloud: &self.cloud,
            fs: self.fs.as_ref(),
            publication_lock: &self.publication_lock,
            lock_wait_budget: self.cloud.callback_timeout(),
            deadline: &self.deadline,
            authority: crate::runtime::hybrid_persistence::CloudMetadataMirrorAuthority {
                store: self.fence.leader_store.as_deref().expect("admitted store"),
                holder_id: self.fence.leader_holder_id.as_deref().unwrap_or_default(),
                writer_epoch: self.fence.writer_epoch,
            },
        };
        let outcome = if let Some(source) = self.source.as_ref() {
            crate::runtime::hybrid_persistence::mirror_captured_control_metadata_within(
                context,
                source,
                |deadline| self.fence.validate_within(deadline),
            )
        } else {
            crate::runtime::hybrid_persistence::collect_selected_metadata_within(
                &context,
                |deadline| self.fence.validate_within(deadline),
            )
        }
        .and_then(|()| self.fence.check_health());
        // The copied body has no remaining user before its source charge can
        // drop. Maintenance then observes pending publication owners itself.
        drop(self.fs);
        drop(self.source);
        outcome?;
        if let Some(sweep) = self.sweep {
            let storage = self
                .storage
                .as_deref()
                .ok_or_else(|| MidgeError::Fenced("orphan sweep lost its storage owner".into()))?;
            sweep.execute(
                storage,
                &self.cloud,
                &self.publication_lock,
                &self.fence,
                &self.deadline,
            )?;
        }
        self.fence.check_health()
    }
}

impl EventLoop {
    /// `true` means the continuation is retained by the worker owner. Local-only
    /// and deterministic inline fixtures complete in this turn and return false.
    pub(in crate::runtime::event_loop) fn start_metadata_mirror(
        &mut self,
        continuation: MirrorContinuation,
        deadline: OperationDeadline,
    ) -> MidgeResult<bool> {
        let Some(cloud) = self.actors.cloud_coordinator.cloud_metadata_storage.clone() else {
            return Ok(false);
        };
        if self.pipeline.inline_flush_worker {
            self.mirror_metadata_to_authoritative_cloud_within(&deadline)?;
            return Ok(false);
        }
        let pipeline = &self.actors.cloud_coordinator.metadata_mirror;
        if pipeline.is_running() {
            return Err(MidgeError::Busy(
                "metadata mirror already owns publication".into(),
            ));
        }
        let id = pipeline.next_operation.checked_add(1).ok_or_else(|| {
            MidgeError::ResourceLimit("metadata mirror operation identity exhausted".into())
        })?;
        let owner = super::super::coordination::ManifestPublicationOwner::MetadataMirror {
            operation_id: id,
        };
        if !self.publication.publication_gate.try_acquire(owner.clone()) {
            return Err(MidgeError::Busy(
                "metadata mirror waits for publication owner".into(),
            ));
        }
        let admitted = self.spawn_metadata_mirror(id, continuation, deadline, cloud);
        // Capture registered the owned cut before provider work can run.
        self.publication.publication_gate.release(&owner);
        admitted.map(|()| true)
    }

    fn spawn_metadata_mirror(
        &mut self,
        id: u64,
        continuation: MirrorContinuation,
        deadline: OperationDeadline,
        cloud: Arc<crate::storage::cloud::CloudStorage>,
    ) -> MidgeResult<()> {
        self.check_lease_health()?;
        let fence = self.authority.fencing.clone();
        if fence.writer_epoch != self.state.writer_epoch {
            return Err(MidgeError::Fenced(
                "metadata mirror authority epoch differs from runtime".into(),
            ));
        }
        if fence.leader_store.is_none() {
            return Err(MidgeError::Fenced(
                "metadata mirror has no leader store".into(),
            ));
        }
        // The loop owns journal/intent changes during this capture. A cloned
        // live Fs would allow later appends to change the admitted operation.
        // Provider work sees only this exact presence/absence and byte cut.
        let maintenance = matches!(
            &continuation,
            MirrorContinuation::Retention | MirrorContinuation::OrphanSweep(_)
        );
        let source = (!maintenance)
            .then(|| {
                self.publication
                    .metadata_publication_lock
                    .capture_control_cut(&self.state.fs, self.state.writer_epoch, &deadline)
            })
            .transpose()?;
        // Maintenance never reads these control bodies; publication uses the
        // immutable captured Fs rather than this live root.
        let fs = source
            .as_ref()
            .map_or_else(|| Arc::clone(&self.state.fs), |cut| Arc::clone(cut.fs()));
        let publication_lock = self.publication.metadata_publication_lock.clone();
        let storage = self.actors.cloud_coordinator.hybrid_storage.clone();
        let epoch = self.state.writer_epoch;
        let sweep = match &continuation {
            MirrorContinuation::OrphanSweep(task) => Some(task.clone()),
            _ => None,
        };
        let work = MetadataControlWork {
            cloud,
            fs,
            source,
            publication_lock,
            fence,
            deadline,
            storage: storage.clone(),
            sweep,
        };
        let (result_tx, result) = crossbeam::channel::bounded(1);
        let failpoint_gate = crate::failpoints::worker_gate();
        let worker = std::thread::Builder::new()
            .name("midge-metadata-mirror".into())
            .spawn(move || {
                failpoint_gate.run(|| {
                    crate::failpoints::fail_point!("midge::metadata_mirror::before_provider");
                    let outcome = work.run();
                    crate::failpoints::fail_point!("midge::metadata_mirror::before_completion");
                    let _ = result_tx.send(outcome);
                    if let Some(storage) = storage {
                        storage.queue_cloud_metadata_mirror_complete(id, epoch);
                    }
                });
            })
            .map_err(|error| {
                MidgeError::Internal(format!("metadata mirror worker could not start: {error}"))
            })?;
        let pipeline = &mut self.actors.cloud_coordinator.metadata_mirror;
        pipeline.next_operation = id;
        pipeline.running = Some(MetadataMirrorJob {
            id,
            epoch,
            continuation,
            result,
            worker,
            started: std::time::Instant::now(),
        });
        Ok(())
    }
}
