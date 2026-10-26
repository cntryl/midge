//! Owned spill-source reads on the existing bounded validation worker.

use super::{ValidationCompletion, ValidationTask, ValidationWorker, MAX_VALIDATION_JOBS};
use crate::common::resource_budget::{ResourceBudget, ResourceReservation};
use crate::common::{MidgeError, MidgeResult, OperationDeadline};
use crate::runtime::actors::wal::{SpilledValidationInput, SpilledValidationProof};
use crate::runtime::event_loop::wal::{SpilledTransactionRequest, WalCoordinator};
use crate::runtime::event_loop::EventLoop;
use crate::runtime::snapshot_pins::OwnedSnapshotPin;
use crate::runtime::state::RecentDeleteRange;
use crate::runtime::{ReadSnapshot, RuntimeMsg, RuntimeResponse};
use crossbeam::channel::Receiver;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub(super) struct SpilledValidationJob {
    pub(super) request: SpilledTransactionRequest,
    snapshots: HashMap<u32, Arc<ReadSnapshot>>,
    recent_ranges: Vec<RecentDeleteRange>,
    applied_sequence: u64,
    writer_epoch: u64,
    deadline: OperationDeadline,
    memory: ResourceBudget,
    _pins: Vec<OwnedSnapshotPin>,
    _reservation: ResourceReservation,
}

impl SpilledValidationJob {
    pub(super) fn read(self, cancelled: &std::sync::atomic::AtomicBool) -> ValidationCompletion {
        let proof = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            crate::failpoints::fail_point!("midge::validation::before_spill_snapshot_reads");
            if self.deadline.is_expired() || cancelled.load(std::sync::atomic::Ordering::Acquire) {
                return Err(MidgeError::Timeout(
                    "spilled validation expired before storage reads".into(),
                ));
            }
            let proof = SpilledValidationProof::read(SpilledValidationInput {
                source: &self.request.source,
                snapshots: &self.snapshots,
                recent_ranges: &self.recent_ranges,
                applied_sequence: self.applied_sequence,
                assertions: &self.request.assertions,
                start_sequence: self.request.start_sequence,
                conflict_policy: self.request.conflict_policy,
                deadline: self.deadline,
                cancelled,
                memory: &self.memory,
            });
            crate::failpoints::fail_point!("midge::validation::after_spill_snapshot_reads");
            proof
        }))
        .unwrap_or_else(|_| {
            Err(MidgeError::Internal(
                "detached spilled validation panicked".into(),
            ))
        });
        ValidationCompletion::Spilled { job: self, proof }
    }

    fn sources_match(&self, event_loop: &EventLoop) -> bool {
        self.applied_sequence == event_loop.state.applied_sequence
            && self.snapshots.iter().all(|(cf_id, snapshot)| {
                event_loop.state.get_cf(*cf_id).is_some_and(|family| {
                    snapshot.has_sources(
                        &family.memtable,
                        &family.immutable_memtables,
                        &event_loop.state.manifest.read_view_for(*cf_id),
                    )
                })
            })
    }
}

impl EventLoop {
    pub(in crate::runtime::event_loop) fn prepare_detached_spilled_validation(
        &mut self,
        mut request: SpilledTransactionRequest,
        touched_cfs: &[u32],
    ) -> Option<SpilledTransactionRequest> {
        let request_id = request.request_id;
        if let Err(error) = self.check_lease_health() {
            self.respond(request_id, RuntimeResponse::Error { request_id, error });
            return None;
        }
        let families = touched_cfs
            .iter()
            .copied()
            .chain(request.assertions.iter().map(|assertion| assertion.cf_id))
            .collect::<HashSet<_>>();
        if request
            .validation
            .as_ref()
            .is_some_and(|proof| proof.matches_sources(&self.state))
        {
            return Some(request);
        }
        request.validation = None;
        if !families.iter().any(|cf_id| {
            self.state
                .manifest
                .files
                .iter()
                .any(|file| file.cf_id == *cf_id)
        }) {
            return Some(request);
        }
        if let Err(error) = self.enqueue_spilled_validation(request, families) {
            self.respond(request_id, RuntimeResponse::Error { request_id, error });
        }
        None
    }

    fn enqueue_spilled_validation(
        &mut self,
        request: SpilledTransactionRequest,
        families: HashSet<u32>,
    ) -> MidgeResult<()> {
        if self.pipeline.validation.pending >= MAX_VALIDATION_JOBS {
            return Err(MidgeError::WriteStall(
                "detached transaction validation queue is full".into(),
            ));
        }
        let request_id = request.request_id;
        let bytes =
            spill_retained_bytes(&request, &self.state.recent_delete_ranges, families.len())?;
        let reservation = self
            .pipeline
            .validation
            .memory
            .reserve(bytes, "detached spill validation metadata")?;
        let job = self.capture_spilled_validation_job(request, families, reservation)?;
        if self.pipeline.validation.worker.is_none() {
            self.pipeline.validation.worker =
                Some(ValidationWorker::new(self.pipeline.worker_msg_tx.clone())?);
        }
        self.pipeline
            .validation
            .worker
            .as_ref()
            .expect("initialized validation worker")
            .requests
            .as_ref()
            .expect("open validation worker")
            .try_send(ValidationTask::Spilled(job))
            .map_err(|_| {
                MidgeError::WriteStall("detached transaction validation worker unavailable".into())
            })?;
        self.pipeline.validation.pending += 1;
        self.pipeline.validation.callers.insert(request_id);
        Ok(())
    }

    fn capture_spilled_validation_job(
        &self,
        request: SpilledTransactionRequest,
        families: HashSet<u32>,
        reservation: ResourceReservation,
    ) -> MidgeResult<SpilledValidationJob> {
        let applied_sequence = self.state.applied_sequence;
        let registry = Arc::clone(&self.state.snapshot_pins);
        let acquisition = registry.begin_acquisition(applied_sequence);
        let mut snapshots = HashMap::new();
        let mut pins = Vec::new();
        for cf_id in families {
            let snapshot = self.create_read_snapshot(cf_id).ok_or_else(|| {
                MidgeError::InvalidArgument(format!("column family {cf_id} does not exist"))
            })?;
            pins.push(registry.pin_sources_while_acquiring(
                applied_sequence,
                snapshot.pinned_sst_names(),
                &acquisition,
            )?);
            snapshots.insert(cf_id, Arc::new(snapshot));
        }
        let memory = ResourceBudget::new(request.source.streaming_memory_bound()?);
        let deadline = self.registered_request_deadline(request.request_id);
        Ok(SpilledValidationJob {
            request,
            snapshots,
            recent_ranges: self.state.recent_delete_ranges.clone(),
            applied_sequence,
            writer_epoch: self.state.writer_epoch,
            deadline,
            memory,
            _pins: pins,
            _reservation: reservation,
        })
    }

    pub(super) fn complete_spilled_validation(
        &mut self,
        mut job: SpilledValidationJob,
        proof: MidgeResult<SpilledValidationProof>,
        msg_rx: &Receiver<RuntimeMsg>,
    ) {
        let request_id = job.request.request_id;
        let ready = self.check_lease_health().and_then(|()| {
            if job.writer_epoch != self.state.writer_epoch {
                Err(MidgeError::Fenced(
                    "writer epoch changed during spilled validation".into(),
                ))
            } else if self.shutdown.shutting_down {
                Err(MidgeError::WriteStall(
                    "runtime is shutting down before spilled validation admission".into(),
                ))
            } else if job.deadline.is_expired() {
                Err(MidgeError::Timeout(
                    "spilled validation expired before WAL admission".into(),
                ))
            } else {
                Ok(())
            }
        });
        if let Err(error) = ready {
            self.respond(request_id, RuntimeResponse::Error { request_id, error });
            return;
        }
        if self.wal_group_in_flight() || !job.sources_match(self) {
            // The coordinator's existing drained spill barrier discards the
            // old proof while a group is pending; otherwise capture fresh
            // sources before performing another bounded streaming read.
            WalCoordinator::apply_spilled_transaction(self, msg_rx, job.request, None);
            return;
        }
        match proof {
            Ok(proof) => {
                job.request.validation = Some(Arc::new(proof));
                WalCoordinator::apply_spilled_transaction(self, msg_rx, job.request, None);
            }
            Err(error) => self.respond(request_id, RuntimeResponse::Error { request_id, error }),
        }
    }
}

fn spill_retained_bytes(
    request: &SpilledTransactionRequest,
    ranges: &[RecentDeleteRange],
    families: usize,
) -> MidgeResult<usize> {
    // The source is moved, never cloned, and retains its transaction-pool
    // and scratch-disk ownership. Charge the new capture rather than placing
    // a second, smaller admission ceiling on an already supported spill.
    let mut bytes = families
        .checked_mul(512)
        .ok_or_else(spill_metadata_overflow)?;
    for assertion in &request.assertions {
        bytes = bytes
            .checked_add(512)
            .and_then(|bytes| bytes.checked_add(assertion.key.len()))
            .ok_or_else(spill_metadata_overflow)?;
    }
    for range in ranges {
        bytes = bytes
            .checked_add(size_of::<RecentDeleteRange>())
            .and_then(|bytes| bytes.checked_add(range.start_key.len()))
            .and_then(|bytes| bytes.checked_add(range.end_key.len()))
            .ok_or_else(spill_metadata_overflow)?;
    }
    Ok(bytes)
}

fn spill_metadata_overflow() -> MidgeError {
    MidgeError::ResourceLimit("spilled validation metadata overflow".into())
}
