//! Bounded detached commit reads and their exact generation handoff.
//!
//! A receipt is used only while its applied cut still equals the loop's cut.
//! Any intervening applied write discards both positive and negative results
//! and starts another read pass. This is retry, never a conservative conflict.
//! Original transaction pins retain post-start conflict history across flush;
//! these job pins additionally protect the exact sources used during I/O.

use super::wal::{ApplyTransactionRequest, WalCoordinator};
use super::EventLoop;
use crate::common::resource_budget::{ResourceBudget, ResourceReservation};
use crate::common::{MidgeError, MidgeResult, OperationDeadline};
use crate::runtime::actors::wal::CommitValidationProof;
use crate::runtime::snapshot_pins::OwnedSnapshotPin;
use crate::runtime::{ConflictPolicy, ReadSnapshot, RuntimeMsg, RuntimeResponse, TransactionOp};
use crossbeam::channel::{Receiver, Sender};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::thread::JoinHandle;

const MAX_VALIDATION_JOBS: usize = 16;
const VALIDATION_MEMORY_BYTES: usize = 8 * 1024 * 1024;

mod spilled;
use spilled::SpilledValidationJob;

#[cfg(all(test, feature = "failpoints"))]
mod tests;

struct ValidationJob {
    request: ApplyTransactionRequest,
    snapshots: HashMap<u32, Arc<ReadSnapshot>>,
    applied_sequence: u64,
    writer_epoch: u64,
    deadline: OperationDeadline,
    _pins: Vec<OwnedSnapshotPin>,
    _reservation: ResourceReservation,
}

enum ValidationTask {
    Resident(ValidationJob),
    Spilled(SpilledValidationJob),
}

enum ValidationCompletion {
    Resident {
        job: ValidationJob,
        proof: MidgeResult<CommitValidationProof>,
    },
    Spilled {
        job: SpilledValidationJob,
        proof: MidgeResult<crate::runtime::actors::wal::SpilledValidationProof>,
    },
}

impl ValidationCompletion {
    fn request_id(&self) -> u64 {
        match self {
            Self::Resident { job, .. } => job.request.request_id,
            Self::Spilled { job, .. } => job.request.request_id,
        }
    }
}

impl ValidationTask {
    fn read(self, cancelled: &std::sync::atomic::AtomicBool) -> ValidationCompletion {
        match self {
            Self::Spilled(job) => job.read(cancelled),
            Self::Resident(job) => {
                let proof = read_resident_validation(&job, cancelled);
                ValidationCompletion::Resident { job, proof }
            }
        }
    }
}

fn read_resident_validation(
    job: &ValidationJob,
    cancelled: &std::sync::atomic::AtomicBool,
) -> MidgeResult<CommitValidationProof> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        crate::failpoints::fail_point!("midge::validation::before_snapshot_reads");
        if job.deadline.is_expired() || cancelled.load(std::sync::atomic::Ordering::Acquire) {
            return Err(MidgeError::Timeout(
                "transaction validation expired before storage reads".into(),
            ));
        }
        CommitValidationProof::read(
            job.applied_sequence,
            &job.snapshots,
            &job.request.ops,
            &job.request.assertions,
            job.request.conflict_policy,
            job.request.start_sequence,
            job.deadline,
        )
    }))
    .unwrap_or_else(|_| {
        Err(MidgeError::Internal(
            "detached transaction validation panicked".into(),
        ))
    })
}

struct ValidationWorker {
    requests: Option<Sender<ValidationTask>>,
    completions: Receiver<ValidationCompletion>,
    handle: Option<JoinHandle<()>>,
    cancelled: Arc<std::sync::atomic::AtomicBool>,
    continue_after_completion: Option<Sender<()>>,
}

impl ValidationWorker {
    fn new() -> MidgeResult<Self> {
        let (requests, incoming) =
            crossbeam::channel::bounded::<ValidationTask>(MAX_VALIDATION_JOBS);
        let (completed, completions) = crossbeam::channel::unbounded();
        let (continue_after_completion, continued) = crossbeam::channel::bounded(1);
        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cancellation = Arc::clone(&cancelled);
        let gate = crate::failpoints::worker_gate();
        let handle = std::thread::Builder::new()
            .name("midge-commit-validation".into())
            .spawn(move || {
                gate.run(|| {
                    for job in incoming {
                        if completed.send(job.read(&cancellation)).is_err() {
                            break;
                        }
                        // One streaming workspace may survive in a completed
                        // first-error key. Wait until the loop consumes it
                        // before reading another job's payloads.
                        if continued.recv().is_err() {
                            break;
                        }
                    }
                });
            })
            .map_err(MidgeError::Io)?;
        Ok(Self {
            requests: Some(requests),
            completions,
            handle: Some(handle),
            cancelled,
            continue_after_completion: Some(continue_after_completion),
        })
    }
}

impl Drop for ValidationWorker {
    fn drop(&mut self) {
        self.requests.take();
        self.continue_after_completion.take();
        if let Some(handle) = self.handle.take() {
            // Reads have no mutation effects, but their sources remain pinned
            // in each job until the final provider callback has returned.
            let _ = handle.join();
        }
    }
}

pub(super) struct ValidationCoordinator {
    worker: Option<ValidationWorker>,
    pending: usize,
    callers: HashSet<u64>,
    memory: ResourceBudget,
}

impl Default for ValidationCoordinator {
    fn default() -> Self {
        Self {
            worker: None,
            pending: 0,
            callers: HashSet::new(),
            memory: ResourceBudget::new(VALIDATION_MEMORY_BYTES),
        }
    }
}

impl ValidationCoordinator {
    pub(super) fn has_ready_result(&self) -> bool {
        self.worker
            .as_ref()
            .is_some_and(|worker| !worker.completions.is_empty())
    }

    pub(super) fn is_pending(&self) -> bool {
        self.pending != 0
    }
}

impl EventLoop {
    fn needs_detached_validation(&self, request: &ApplyTransactionRequest) -> bool {
        let conflict_checked = request.conflict_policy == ConflictPolicy::AbortOnWriteConflict;
        let point_reads = request.ops.iter().filter_map(|op| match op {
            TransactionOp::Put {
                cf_id, insert_only, ..
            } if *insert_only || conflict_checked => Some(*cf_id),
            TransactionOp::Delete { cf_id, .. } | TransactionOp::DeleteRange { cf_id, .. }
                if conflict_checked =>
            {
                Some(*cf_id)
            }
            _ => None,
        });
        point_reads
            .chain(request.assertions.iter().map(|assertion| assertion.cf_id))
            .any(|cf_id| {
                self.state
                    .manifest
                    .files
                    .iter()
                    .any(|file| file.cf_id == cf_id)
            })
    }

    /// Return a ready request or retain it in the bounded read stage. No
    /// sequence is allocated and no WAL bytes are submitted before readiness.
    pub(super) fn prepare_detached_validation(
        &mut self,
        mut request: ApplyTransactionRequest,
    ) -> Option<ApplyTransactionRequest> {
        if self.shutdown.shutting_down {
            self.respond(
                request.request_id,
                RuntimeResponse::Error {
                    request_id: request.request_id,
                    error: MidgeError::WriteStall(
                        "runtime is shutting down before transaction validation admission".into(),
                    ),
                },
            );
            return None;
        }
        if let Err(error) = self.check_lease_health() {
            self.respond(
                request.request_id,
                RuntimeResponse::Error {
                    request_id: request.request_id,
                    error,
                },
            );
            return None;
        }
        if !self.needs_detached_validation(&request) {
            return Some(request);
        }
        if request
            .validation
            .as_ref()
            .is_some_and(|proof| proof.applied_sequence == self.state.applied_sequence)
        {
            return Some(request);
        }
        request.validation = None;
        let request_id = request.request_id;
        if let Err(error) = self.enqueue_validation(request) {
            self.respond(request_id, RuntimeResponse::Error { request_id, error });
        }
        None
    }

    pub(super) fn validation_would_defer(&self, request: &ApplyTransactionRequest) -> bool {
        self.needs_detached_validation(request)
            && request
                .validation
                .as_ref()
                .is_none_or(|proof| proof.applied_sequence != self.state.applied_sequence)
    }

    fn enqueue_validation(&mut self, request: ApplyTransactionRequest) -> MidgeResult<()> {
        let request_id = request.request_id;
        if self.pipeline.validation.pending >= MAX_VALIDATION_JOBS {
            return Err(MidgeError::WriteStall(
                "detached transaction validation queue is full".into(),
            ));
        }
        // A timed-out caller can release its transaction while this job still
        // owns Bytes handles. Charge their payloads through read completion.
        let metadata_bytes = request
            .ops
            .iter()
            .fold(0usize, |total, op| {
                let payload = match op {
                    TransactionOp::Put { key, value, .. } => key.len().saturating_add(value.len()),
                    TransactionOp::Delete { key, .. } => key.len(),
                    TransactionOp::DeleteRange {
                        start_key, end_key, ..
                    } => start_key.len().saturating_add(end_key.len()),
                };
                total.saturating_add(512).saturating_add(payload)
            })
            .saturating_add(request.assertions.iter().fold(0usize, |total, assertion| {
                total
                    .saturating_add(512)
                    .saturating_add(assertion.key.len())
            }));
        let reservation = self
            .pipeline
            .validation
            .memory
            .reserve(metadata_bytes, "detached transaction validation")?;
        let job = self.capture_validation_job(request, reservation)?;
        if self.pipeline.validation.worker.is_none() {
            self.pipeline.validation.worker = Some(ValidationWorker::new()?);
        }
        let worker = self
            .pipeline
            .validation
            .worker
            .as_ref()
            .expect("validation worker initialized");
        worker
            .requests
            .as_ref()
            .expect("validation worker open")
            .try_send(ValidationTask::Resident(job))
            .map_err(|_| {
                MidgeError::WriteStall("detached transaction validation worker unavailable".into())
            })?;
        self.pipeline.validation.pending += 1;
        self.pipeline.validation.callers.insert(request_id);
        Ok(())
    }

    fn capture_validation_job(
        &self,
        request: ApplyTransactionRequest,
        reservation: ResourceReservation,
    ) -> MidgeResult<ValidationJob> {
        let applied_sequence = self.state.applied_sequence;
        let registry = Arc::clone(&self.state.snapshot_pins);
        let acquisition = registry.begin_acquisition(applied_sequence);
        let mut families = Self::transaction_cf_ids(&request.ops)
            .into_iter()
            .collect::<HashSet<_>>();
        families.extend(request.assertions.iter().map(|assertion| assertion.cf_id));
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
        let deadline = self.registered_request_deadline(request.request_id);
        Ok(ValidationJob {
            request,
            snapshots,
            applied_sequence,
            writer_epoch: self.state.writer_epoch,
            deadline,
            _pins: pins,
            _reservation: reservation,
        })
    }

    pub(super) fn drain_validation_results(&mut self, msg_rx: &Receiver<RuntimeMsg>) {
        if self
            .publication
            .publication_gate
            .blocks_validation_completion()
            || self.publication.verification_barrier.is_active()
        {
            return;
        }
        loop {
            let completion = self
                .pipeline
                .validation
                .worker
                .as_ref()
                .and_then(|worker| worker.completions.try_recv().ok());
            let Some(completion) = completion else {
                break;
            };
            self.pipeline.validation.pending = self.pipeline.validation.pending.saturating_sub(1);
            if !self
                .pipeline
                .validation
                .callers
                .remove(&completion.request_id())
            {
                drop(completion);
                self.release_validation_worker();
                continue;
            }
            match completion {
                ValidationCompletion::Resident { job, proof } => {
                    self.complete_validation(job, proof, msg_rx);
                }
                ValidationCompletion::Spilled { job, proof } => {
                    self.complete_spilled_validation(job, proof, msg_rx);
                }
            }
            self.release_validation_worker();
        }
    }

    fn release_validation_worker(&self) {
        if let Some(continued) = self
            .pipeline
            .validation
            .worker
            .as_ref()
            .and_then(|worker| worker.continue_after_completion.as_ref())
        {
            let _ = continued.try_send(());
        }
    }

    fn complete_validation(
        &mut self,
        job: ValidationJob,
        proof: MidgeResult<CommitValidationProof>,
        msg_rx: &Receiver<RuntimeMsg>,
    ) {
        let mut job = job;
        let request_id = job.request.request_id;
        let result = self.check_lease_health().and_then(|()| {
            if job.writer_epoch != self.state.writer_epoch {
                Err(MidgeError::Fenced(
                    "writer epoch changed during detached transaction validation".into(),
                ))
            } else if self.shutdown.shutting_down {
                Err(MidgeError::WriteStall(
                    "runtime is shutting down before transaction validation admission".into(),
                ))
            } else if job.deadline.is_expired() {
                Err(MidgeError::Timeout(
                    "transaction validation expired before WAL admission".into(),
                ))
            } else {
                Ok(())
            }
        });
        if let Err(error) = result {
            self.respond(request_id, RuntimeResponse::Error { request_id, error });
            return;
        }
        if job.applied_sequence != self.state.applied_sequence {
            // An SST read, including a negative/error result, has no authority
            // over writes applied after its captured cut. Retry the new view.
            self.prepare_detached_validation(job.request);
            return;
        }
        match proof {
            Ok(proof) => {
                job.request.validation = Some(Arc::new(proof));
                WalCoordinator::apply_transaction(self, msg_rx, job.request, None);
            }
            Err(error) => self.respond(request_id, RuntimeResponse::Error { request_id, error }),
        }
    }

    pub(super) fn fail_validation_requests_on_shutdown(&mut self) {
        if let Some(worker) = &self.pipeline.validation.worker {
            worker
                .cancelled
                .store(true, std::sync::atomic::Ordering::Release);
        }
        let callers = std::mem::take(&mut self.pipeline.validation.callers);
        for request_id in callers {
            self.respond(
                request_id,
                RuntimeResponse::Error {
                    request_id,
                    error: MidgeError::WriteStall(
                        "runtime shut down before transaction validation admission".into(),
                    ),
                },
            );
        }
    }
}
