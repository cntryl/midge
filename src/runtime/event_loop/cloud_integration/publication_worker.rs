//! One off-loop verifier/catalog publisher. Uploads can finish in any order;
//! only the loop chooses a contiguous run and applies a matching completion.

use super::super::fencing::RuntimeFence;
use super::super::EventLoop;
use crate::common::{MidgeError, MidgeResult, OperationDeadline};
use crate::runtime::hybrid_persistence::{CloudPersistence, PublicationError};
use crate::wal::cloud_catalog::CatalogEntry;
use crate::wal::publish::OwedSegment;
use std::collections::BTreeMap;

const MAX_PENDING_ACKS: usize = 4096;

enum Work {
    Verify(crate::io::FsLocation, u64),
    Publish(Vec<CatalogEntry>),
}

enum Completion {
    Verified(CatalogEntry),
    Published,
}

pub(in crate::runtime::event_loop) struct CloudPublicationJob {
    id: u64,
    epoch: u64,
    acknowledged: OwedSegment,
    deadline: OperationDeadline,
    ready: Vec<OwedSegment>,
    verification_bytes: u64,
    result: crossbeam::channel::Receiver<Result<Completion, PublicationError>>,
    worker: std::thread::JoinHandle<()>,
}

#[derive(Default)]
pub(in crate::runtime::event_loop) struct CloudPublicationPipeline {
    next_operation: u64,
    running: Option<CloudPublicationJob>,
    pending: BTreeMap<u64, (u64, OperationDeadline, u64)>,
}

impl CloudPublicationPipeline {
    pub(in crate::runtime::event_loop) fn held_uploads(
        &self,
    ) -> crate::storage::hybrid::backend::HeldUploads {
        let running = self
            .running
            .as_ref()
            .map_or(0, |job| job.verification_bytes);
        crate::storage::hybrid::backend::HeldUploads {
            entries: self.pending.len().saturating_add(usize::from(running > 0)),
            bytes: self
                .pending
                .values()
                .fold(running, |total, (_, _, bytes)| total.saturating_add(*bytes)),
        }
    }
}

impl Drop for CloudPublicationPipeline {
    fn drop(&mut self) {
        // A callback timeout cannot revoke an admitted provider effect. Retain
        // worker ownership through its terminal result before releasing storage.
        if let Some(job) = self.running.take() {
            let _ = job.worker.join();
        }
    }
}

impl EventLoop {
    pub(in crate::runtime::event_loop) fn queue_cloud_wal_ack(
        &mut self,
        acknowledged: OwedSegment,
        deadline: OperationDeadline,
    ) {
        if self.state.wal.frontiers.cloud_durable() >= acknowledged.max_sequence {
            return;
        }
        let size = match self
            .state
            .fs
            .metadata(&crate::wal::segment_key(acknowledged.segment_id))
        {
            Ok(metadata) => metadata.len,
            Err(error) => {
                self.requeue_failed_cloud_upload(acknowledged.segment_id, &error.into_midge());
                return;
            }
        };
        let pipeline = &mut self.actors.cloud_coordinator.cloud_publication;
        if pipeline.running.as_ref().is_some_and(|job| {
            job.acknowledged.segment_id == acknowledged.segment_id
                || job
                    .ready
                    .iter()
                    .any(|segment| segment.segment_id == acknowledged.segment_id)
        }) || pipeline.pending.contains_key(&acknowledged.segment_id)
        {
            return;
        }
        if pipeline.pending.len() >= MAX_PENDING_ACKS {
            self.requeue_failed_cloud_upload(
                acknowledged.segment_id,
                &MidgeError::WriteStall("cloud WAL publication queue is full".into()),
            );
            return;
        }
        pipeline.pending.insert(
            acknowledged.segment_id,
            (acknowledged.max_sequence, deadline, size),
        );
        self.start_next_cloud_verification();
    }

    fn start_next_cloud_verification(&mut self) {
        let pipeline = &mut self.actors.cloud_coordinator.cloud_publication;
        if pipeline.running.is_some() {
            return;
        }
        let (segment_id, max_sequence, deadline, size) = loop {
            let Some((segment_id, (max_sequence, deadline, size))) = pipeline.pending.pop_first()
            else {
                return;
            };
            if self.state.wal.frontiers.cloud_durable() < max_sequence {
                break (segment_id, max_sequence, deadline, size);
            }
        };
        let segment = OwedSegment {
            segment_id,
            max_sequence,
        };
        let source = crate::io::FsLocation::new(
            std::sync::Arc::clone(&self.state.fs),
            crate::wal::segment_key(segment_id),
        );
        self.start_cloud_publication_job(segment, Vec::new(), deadline, Work::Verify(source, size));
    }

    pub(in crate::runtime::event_loop) fn start_cloud_catalog_publication(
        &mut self,
        acknowledged: OwedSegment,
        ready: Vec<OwedSegment>,
        deadline: OperationDeadline,
    ) {
        let run = self
            .actors
            .cloud_coordinator
            .cloud_wal
            .acks()
            .unpublished(&ready);
        let acknowledged = run.first().map_or(acknowledged, |entry| OwedSegment {
            segment_id: entry.id(),
            max_sequence: entry.max_sequence(),
        });
        self.start_cloud_publication_job(acknowledged, ready, deadline, Work::Publish(run));
    }

    fn start_cloud_publication_job(
        &mut self,
        acknowledged: OwedSegment,
        ready: Vec<OwedSegment>,
        deadline: OperationDeadline,
        work: Work,
    ) {
        let Some(id) = self
            .actors
            .cloud_coordinator
            .cloud_publication
            .next_operation
            .checked_add(1)
        else {
            self.fence_cloud_upload(
                acknowledged.segment_id,
                &MidgeError::ResourceLimit(
                    "cloud WAL publication operation identity exhausted".into(),
                ),
            );
            return;
        };
        let persistence = match self.cloud_persistence() {
            Ok(persistence) => persistence,
            Err(error) => {
                self.route_cloud_publication_error(
                    acknowledged,
                    error,
                    "cloud WAL worker admission",
                    &deadline,
                );
                return;
            }
        };
        let storage = self
            .actors
            .cloud_coordinator
            .hybrid_storage
            .as_ref()
            .expect("persistence storage")
            .clone();
        let fence = self.authority.fencing.clone();
        let verification_bytes = match &work {
            Work::Verify(_, size) => *size,
            Work::Publish(_) => 0,
        };
        let epoch = self.state.writer_epoch;
        let (result_tx, result) = crossbeam::channel::bounded(1);
        let failpoint_gate = crate::failpoints::worker_gate();
        let worker = std::thread::Builder::new()
            .name("midge-wal-publication".into())
            .spawn(move || {
                failpoint_gate.run(|| {
                    let completion = run_work(&persistence, &fence, acknowledged, deadline, work);
                    let _ = result_tx.send(completion);
                    storage.queue_cloud_wal_publication_complete(id, epoch);
                })
            });
        match worker {
            Ok(worker) => {
                let pipeline = &mut self.actors.cloud_coordinator.cloud_publication;
                pipeline.next_operation = id;
                pipeline.running = Some(CloudPublicationJob {
                    id,
                    epoch,
                    acknowledged,
                    deadline,
                    ready,
                    verification_bytes,
                    result,
                    worker,
                });
            }
            Err(error) => self.requeue_failed_cloud_upload(
                acknowledged.segment_id,
                &MidgeError::Internal(format!(
                    "cloud WAL publication worker could not start: {error}"
                )),
            ),
        }
    }

    pub(in crate::runtime::event_loop) fn reap_cloud_publication(&mut self) {
        if self.wal_group_in_flight() {
            return;
        }
        let identity = self
            .actors
            .cloud_coordinator
            .cloud_publication
            .running
            .as_ref()
            .filter(|job| !job.result.is_empty() || job.worker.is_finished())
            .map(|job| (job.id, job.epoch));
        if let Some((id, epoch)) = identity {
            self.complete_cloud_publication(id, epoch);
        }
    }

    pub(in crate::runtime::event_loop) fn complete_cloud_publication(
        &mut self,
        id: u64,
        epoch: u64,
    ) {
        let pipeline = &mut self.actors.cloud_coordinator.cloud_publication;
        if !pipeline
            .running
            .as_ref()
            .is_some_and(|job| job.id == id && job.epoch == epoch)
        {
            return;
        }
        // A forged/early event cannot strand the actual worker result.
        if pipeline
            .running
            .as_ref()
            .is_some_and(|job| job.result.is_empty() && !job.worker.is_finished())
        {
            return;
        }
        let job = pipeline.running.take().expect("matched operation");
        let result = job.result.try_recv().unwrap_or_else(|_| {
            Err(PublicationError::Retriable(MidgeError::Internal(
                "cloud WAL publication worker ended without completion".into(),
            )))
        });
        let _ = job.worker.join();
        let result = if epoch == self.state.writer_epoch {
            self.check_lease_health()
                .map_err(PublicationError::Fenced)
                .and(result)
        } else {
            Err(PublicationError::Fenced(MidgeError::Fenced(
                "stale cloud WAL publication epoch".into(),
            )))
        };
        match result {
            Ok(Completion::Verified(upload)) => {
                self.accept_verified_cloud_wal_upload(job.acknowledged, upload, job.deadline);
            }
            Ok(Completion::Published) => {
                self.actors
                    .cloud_coordinator
                    .cloud_wal
                    .mark_published(&job.ready);
                if let Some(durable) = job.ready.last().copied() {
                    self.commit_contiguous_cloud_ack(&job.ready, durable);
                }
            }
            Err(error) => self.route_cloud_publication_error(
                job.acknowledged,
                error,
                "cloud WAL publication completion",
                &job.deadline,
            ),
        }
        self.start_next_cloud_verification();
    }

    pub(in crate::runtime::event_loop) fn drain_cloud_publication_within(
        &mut self,
        deadline: &OperationDeadline,
    ) -> MidgeResult<()> {
        self.drain_metadata_mirror_within(deadline)?;
        loop {
            self.reap_cloud_publication();
            self.start_next_cloud_verification();
            let pipeline = &self.actors.cloud_coordinator.cloud_publication;
            if pipeline.running.is_none() && pipeline.pending.is_empty() {
                return Ok(());
            }
            if deadline.is_expired() {
                return Err(MidgeError::Timeout(
                    "cloud WAL publication drain deadline exhausted; worker remains owned".into(),
                ));
            }
            if let Some(job) = &pipeline.running {
                // Readiness is consumed only by the matching completion path.
                std::thread::sleep(
                    deadline
                        .remaining()
                        .min(std::time::Duration::from_millis(1)),
                );
                if !job.worker.is_finished() {
                    std::thread::yield_now();
                }
            }
        }
    }
}

fn run_work(
    persistence: &CloudPersistence,
    fence: &RuntimeFence,
    acknowledged: OwedSegment,
    deadline: OperationDeadline,
    work: Work,
) -> Result<Completion, PublicationError> {
    crate::failpoints::fail_point!("midge::cloud::before_wal_publication_work");
    fence
        .validate_within(&deadline)
        .map_err(authority_failure)?;
    let result = match work {
        Work::Verify(source, _) => persistence
            .verify_uploaded_wal_segment(
                acknowledged.segment_id,
                acknowledged.max_sequence,
                &source,
                &deadline,
            )
            .map(Completion::Verified),
        Work::Publish(run) => persistence
            .publish_wal_segments(&run, fence.writer_epoch, &deadline)
            .map(|()| Completion::Published),
    };
    let completion = result?;
    crate::failpoints::fail_point!("midge::cloud::after_wal_publication_work");
    fence
        .validate_within(&deadline)
        .map_err(authority_failure)?;
    Ok(completion)
}

fn authority_failure(error: MidgeError) -> PublicationError {
    match error {
        error @ (MidgeError::Fenced(_) | MidgeError::LeaseIndeterminate(_)) => {
            PublicationError::Fenced(error)
        }
        error => PublicationError::Retriable(error),
    }
}
