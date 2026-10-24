//! Submit immutable control-file cuts; only the event loop applies completion.

use super::{EventLoop, MetadataMirrorJob, MirrorContinuation};
use crate::common::{MidgeError, MidgeResult, OperationDeadline};
use crate::io::{Fs, FsError, FsPath};
use std::sync::Arc;

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
        if admitted.is_err() {
            self.publication.publication_gate.release(&owner);
        }
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
        let source = capture_metadata_cut(self.state.fs.as_ref(), &deadline)?;
        let publication_lock = self.publication.metadata_publication_lock.clone();
        let storage = self.actors.cloud_coordinator.hybrid_storage.clone();
        let epoch = self.state.writer_epoch;
        let (result_tx, result) = crossbeam::channel::bounded(1);
        let worker = std::thread::Builder::new()
            .name("midge-metadata-mirror".into())
            .spawn(move || {
                crate::failpoints::fail_point!("midge::metadata_mirror::before_provider");
                let outcome = crate::runtime::hybrid_persistence::mirror_control_metadata_within(
                    crate::runtime::hybrid_persistence::CloudMetadataMirrorContext {
                        cloud: &cloud,
                        fs: source.as_ref(),
                        publication_lock: &publication_lock,
                        lock_wait_budget: cloud.callback_timeout(),
                        deadline: &deadline,
                        authority:
                            crate::runtime::hybrid_persistence::CloudMetadataMirrorAuthority {
                                store: fence.leader_store.as_deref().expect("admitted store"),
                                holder_id: fence.leader_holder_id.as_deref().unwrap_or_default(),
                                writer_epoch: epoch,
                            },
                    },
                    |deadline| fence.validate_within(deadline),
                )
                .and_then(|()| fence.check_health());
                crate::failpoints::fail_point!("midge::metadata_mirror::before_completion");
                let _ = result_tx.send(outcome);
                if let Some(storage) = storage {
                    storage.queue_cloud_metadata_mirror_complete(id, epoch);
                }
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

fn capture_metadata_cut(fs: &dyn Fs, deadline: &OperationDeadline) -> MidgeResult<Arc<dyn Fs>> {
    let captured = crate::io::MockFs::new();
    for name in crate::metadata::files::CLOUD_MIRRORED {
        if deadline.is_expired() {
            return Err(MidgeError::Timeout(
                "metadata capture deadline exhausted".into(),
            ));
        }
        let path = FsPath::new(*name);
        let bytes = match crate::io::read_all(fs, &path) {
            Ok(bytes) => bytes,
            Err(FsError::NotFound(_)) => continue,
            Err(error) => return Err(error.into_midge()),
        };
        let mut target = captured
            .open(
                &path,
                crate::io::OpenOptions {
                    mode: crate::io::OpenMode::ReadWrite,
                    create: true,
                    create_new: true,
                    truncate: false,
                },
            )
            .map_err(FsError::into_midge)?;
        target.write_at(0, bytes).map_err(FsError::into_midge)?;
    }
    Ok(Arc::new(captured))
}
