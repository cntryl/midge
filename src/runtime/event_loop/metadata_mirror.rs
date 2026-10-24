use super::EventLoop;
use crate::common::{MidgeError, MidgeResult, OperationDeadline};

#[cfg(all(test, feature = "failpoints"))]
mod tests;
mod worker;

pub(super) enum MirrorContinuation {
    Request { request_id: u64 },
    Auxiliary { context: String },
    SstReservation { cf_id: u32, next_seq: u64 },
    Reclamation,
}

pub(super) struct MetadataMirrorJob {
    id: u64,
    epoch: u64,
    continuation: MirrorContinuation,
    result: crossbeam::channel::Receiver<MidgeResult<()>>,
    worker: std::thread::JoinHandle<()>,
    started: std::time::Instant,
}

#[derive(Default)]
pub(super) struct MetadataMirrorPipeline {
    next_operation: u64,
    running: Option<MetadataMirrorJob>,
    pub(super) pending_compaction:
        Option<(crate::compaction::CompactionPlan, Option<OperationDeadline>)>,
    pub(super) pending_flush: Option<u64>,
}

impl MetadataMirrorPipeline {
    pub(super) fn is_running(&self) -> bool {
        self.running.is_some()
    }
    pub(super) fn has_ready_result(&self) -> bool {
        self.running
            .as_ref()
            .is_some_and(|job| !job.result.is_empty() || job.worker.is_finished())
    }
}

impl Drop for MetadataMirrorPipeline {
    fn drop(&mut self) {
        // A caller timeout does not cancel an admitted CAS. Keep its immutable
        // source and authority alive until the provider operation terminates.
        if let Some(job) = self.running.take() {
            let _ = job.worker.join();
        }
    }
}

impl EventLoop {
    #[cfg(test)]
    pub(super) fn mirror_metadata_to_authoritative_cloud(&self) -> crate::common::MidgeResult<()> {
        self.mirror_metadata_to_authoritative_cloud_within(&self.event_loop_cloud_deadline())
    }

    pub(super) fn mirror_metadata_to_authoritative_cloud_within(
        &self,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<()> {
        let started = std::time::Instant::now();
        let result = self.mirror_metadata_within(deadline);
        // Local-only engines take this path as a no-op; only a real provider
        // round trip belongs in the histogram.
        if self.actors.cloud_coordinator.hybrid_storage.is_some() {
            self.state
                .diagnostics
                .record(|m| m.record_cloud_metadata_mirror_latency(started.elapsed()));
        }
        result
    }

    fn mirror_metadata_within(
        &self,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<()> {
        self.actors.cloud_coordinator.mirror_metadata_within(
            self.state.fs.as_ref(),
            &self.publication.metadata_publication_lock,
            deadline,
            self.authority.fencing.leader_store.as_deref().map(|store| {
                crate::runtime::hybrid_persistence::CloudMetadataMirrorAuthority {
                    store,
                    holder_id: self
                        .authority
                        .fencing
                        .leader_holder_id
                        .as_deref()
                        .unwrap_or_default(),
                    writer_epoch: self.authority.fencing.writer_epoch,
                }
            }),
            |deadline| self.validate_runtime_writer_lease_within(deadline),
        )
    }

    pub(super) fn mirror_metadata_after_local_commit_within(
        &mut self,
        context: &str,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<()> {
        let result = self
            .start_metadata_mirror(
                MirrorContinuation::Auxiliary {
                    context: context.into(),
                },
                *deadline,
            )
            .map(|_| ());
        self.accept_metadata_mirror_result(result, context)
    }

    pub(super) fn accept_metadata_mirror_result(
        &mut self,
        result: MidgeResult<()>,
        context: &str,
    ) -> MidgeResult<()> {
        match result {
            Ok(()) => Ok(()),
            Err(error)
                if self.state.recovery_policy() == crate::config::RecoveryPolicy::Salvage =>
            {
                self.state.mark_persistence_anomaly();
                tracing::warn!(%error, context, "cloud metadata mirror failed during salvage-capable operation");
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    pub(super) fn complete_metadata_mirror(&mut self, id: u64, epoch: u64) {
        let pipeline = &mut self.actors.cloud_coordinator.metadata_mirror;
        if !pipeline
            .running
            .as_ref()
            .is_some_and(|job| job.id == id && job.epoch == epoch)
            || !pipeline.has_ready_result()
        {
            return;
        }
        let job = pipeline.running.take().expect("matched metadata mirror");
        let result = job.result.try_recv().unwrap_or_else(|_| {
            Err(MidgeError::Internal(
                "metadata mirror worker ended without completion".into(),
            ))
        });
        let _ = job.worker.join();
        self.state
            .diagnostics
            .record(|m| m.record_cloud_metadata_mirror_latency(job.started.elapsed()));
        let result = if epoch != self.state.writer_epoch {
            Err(MidgeError::Fenced("stale metadata mirror epoch".into()))
        } else {
            self.check_lease_health().and(result)
        };
        self.publication.publication_gate.release(
            &super::coordination::ManifestPublicationOwner::MetadataMirror { operation_id: id },
        );
        self.finish_metadata_mirror(job.continuation, result);
    }

    fn finish_metadata_mirror(
        &mut self,
        continuation: MirrorContinuation,
        result: MidgeResult<()>,
    ) {
        match continuation {
            MirrorContinuation::Request { request_id } => {
                let result = self.accept_metadata_mirror_result(result, "manifest persist");
                if result.is_err() {
                    self.state.mark_persistence_anomaly();
                }
                self.respond(
                    request_id,
                    result.map_or_else(
                        |error| crate::runtime::RuntimeResponse::Error { request_id, error },
                        |()| crate::runtime::RuntimeResponse::Ok { request_id },
                    ),
                );
            }
            MirrorContinuation::Auxiliary { context } => {
                if let Err(error) = result {
                    self.state.mark_persistence_anomaly();
                    tracing::warn!(%error, context, "committed metadata mirror remains degraded");
                    self.publish_snapshot();
                }
            }
            MirrorContinuation::SstReservation { cf_id, next_seq } => {
                if result.is_ok() {
                    self.state
                        .sst_names
                        .reserved_through
                        .entry(cf_id)
                        .and_modify(|old| *old = (*old).max(next_seq))
                        .or_insert(next_seq);
                } else {
                    self.state.mark_persistence_anomaly();
                }
                if let Some(flush_id) = self
                    .actors
                    .cloud_coordinator
                    .metadata_mirror
                    .pending_flush
                    .take()
                {
                    if let Err(error) = &result {
                        // Preserve failure/backoff and the original waiter
                        // contract; provider failure is not an endless Busy.
                        self.fail_flush_pipeline(flush_id, None, error);
                    }
                }
                if let Some((plan, deadline)) = self
                    .actors
                    .cloud_coordinator
                    .metadata_mirror
                    .pending_compaction
                    .take()
                {
                    self.resume_compaction_after_metadata_mirror(plan, deadline, result);
                }
            }
            MirrorContinuation::Reclamation => {
                super::gc::GcCoordinator::finish_manifest_reclamation(self, result)
            }
        }
    }

    pub(super) fn reap_metadata_mirror(&mut self) {
        let identity = self
            .actors
            .cloud_coordinator
            .metadata_mirror
            .running
            .as_ref()
            .filter(|_| {
                self.actors
                    .cloud_coordinator
                    .metadata_mirror
                    .has_ready_result()
            })
            .map(|job| (job.id, job.epoch));
        if let Some((id, epoch)) = identity {
            self.complete_metadata_mirror(id, epoch);
        }
    }

    pub(super) fn drain_metadata_mirror_within(
        &mut self,
        deadline: &OperationDeadline,
    ) -> MidgeResult<()> {
        while self.actors.cloud_coordinator.metadata_mirror.is_running() {
            self.reap_metadata_mirror();
            if !self.actors.cloud_coordinator.metadata_mirror.is_running() {
                break;
            }
            if deadline.is_expired() {
                return Err(MidgeError::Timeout(
                    "metadata mirror drain deadline exhausted; operation remains owned".into(),
                ));
            }
            std::thread::sleep(
                deadline
                    .remaining()
                    .min(std::time::Duration::from_millis(1)),
            );
        }
        Ok(())
    }
}
