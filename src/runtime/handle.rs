//! Runtime request handle and synchronous request APIs.

use super::{
    next_request_id, snapshot_cache, snapshot_pins, ResponseRouter, RuntimeLifecycle,
    RuntimeLifecycleState, RuntimeMsg, RuntimeResponse, RuntimeTransactionGuard, TransactionOp,
};
use crate::common::{MidgeError, MidgeResult};
use crate::types::ConflictPolicy;
use crate::wal::DurabilityPolicy;
use crossbeam::channel::{self, Sender};
use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;

const WRITE_STALL_STATUS_TIMEOUT: Duration = Duration::from_millis(250);

/// Handle for submitting work to the runtime.
///
/// Maintainer:
/// - Route responses by `request_id` using `ResponseRouter`.
/// - Use per-request channels (bounded(1)) created via `ResponseRouter::register`.
/// - Never use a single shared `response_rx`.
/// - `RuntimeHandle` MUST be thread-safe and support concurrent callers.
#[derive(Clone)]
pub struct RuntimeHandle {
    pub(super) msg_tx: Sender<RuntimeMsg>,
    pub(super) router: Arc<ResponseRouter>,
    pub(super) ingest_active: Arc<std::sync::atomic::AtomicBool>,
    /// Lock-free snapshot cache for read-path bypass.
    ///
    /// Allows `begin_tx` to capture a read snapshot without event loop round-trip.
    pub(crate) snapshot_cache: Arc<snapshot_cache::SnapshotCache>,
    /// Concurrent snapshot pins observed by GC and compaction.
    pub(crate) snapshot_pins: Arc<snapshot_pins::SnapshotPinRegistry>,
    /// Per-runtime read-path diagnostics shared with the event loop and
    /// read resources.
    pub(crate) diagnostics: Arc<crate::diagnostics::RuntimeDiagnostics>,
    pub(super) lifecycle: Arc<RuntimeLifecycle>,
}

impl RuntimeHandle {
    fn map_submission_error(error: crossbeam::channel::TrySendError<RuntimeMsg>) -> MidgeError {
        match error {
            crossbeam::channel::TrySendError::Full(message) => {
                drop(message);
                MidgeError::WriteStall("runtime request queue is full".to_string())
            }
            crossbeam::channel::TrySendError::Disconnected(message) => {
                drop(message);
                MidgeError::Internal("Runtime channel closed".to_string())
            }
        }
    }

    pub(crate) fn ensure_open(&self) -> MidgeResult<()> {
        self.lifecycle.ensure_open()
    }

    pub(crate) fn is_open(&self) -> bool {
        self.lifecycle.state() == RuntimeLifecycleState::Open
    }

    pub(crate) fn read_path_diagnostics_snapshot(
        &self,
    ) -> crate::diagnostics::ReadPathDiagnosticsSnapshot {
        self.diagnostics.snapshot()
    }

    pub(crate) fn acquire_transaction_guard(&self) -> MidgeResult<RuntimeTransactionGuard> {
        self.lifecycle.acquire()
    }
    /// Return whether the runtime ingest barrier is currently active.
    ///
    /// This is intentionally a direct atomic read instead of an event-loop
    /// message because transaction creation sits on the API hot path.
    pub(crate) fn ingest_active(&self) -> bool {
        self.ingest_active.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn begin_snapshot_acquisition(&self) -> snapshot_pins::SnapshotAcquisitionGuard<'_> {
        self.snapshot_pins.begin_acquisition()
    }

    pub(crate) fn register_snapshot_pin_while_acquiring(
        &self,
        snapshot_id: u64,
        sequence: u64,
        pinned_sst_names: Vec<String>,
    ) -> bool {
        self.snapshot_pins
            .register_while_acquired(snapshot_id, sequence, pinned_sst_names)
    }

    pub(crate) fn unregister_snapshot_pin(&self, snapshot_id: u64) -> bool {
        let removed = self.snapshot_pins.unregister(snapshot_id);
        if removed {
            // Snapshot release must never block transaction drop, especially
            // after shutdown has entered Closing. Maintenance can retry GC on
            // its normal cadence if this bounded queue is currently full.
            let _ = self.msg_tx.try_send(RuntimeMsg::RetryGc);
        }
        removed
    }

    /// Submit a message to the runtime (fire-and-forget).
    ///
    /// For messages that expect a response, prefer `send_and_wait`.
    pub fn send(&self, msg: RuntimeMsg) -> MidgeResult<()> {
        let submission_guard = self.lifecycle.begin_submission()?;
        let result = self
            .msg_tx
            .try_send(msg)
            .map_err(Self::map_submission_error);
        drop(submission_guard);
        result
    }

    /// Release an online-verification barrier even after lifecycle closing begins.
    ///
    /// Shutdown is deferred behind this token, so this narrow control path must
    /// bypass ordinary submission rejection or the runtime and its lease could
    /// never reach a quiescent state.
    pub(crate) fn release_storage_verification_barrier(&self, token: u64) -> MidgeResult<()> {
        let request_id = next_request_id()?;
        let response_rx = self.router.register(request_id);
        if self
            .msg_tx
            .send(RuntimeMsg::EndStorageVerification { request_id, token })
            .is_err()
        {
            self.router.unregister(request_id);
            return Err(MidgeError::Internal(
                "runtime channel closed before verification barrier release".to_string(),
            ));
        }

        match response_rx.recv() {
            Ok(RuntimeResponse::Ok { .. }) => Ok(()),
            Ok(RuntimeResponse::Error { error, .. }) => Err(error),
            Ok(other) => Err(MidgeError::Internal(format!(
                "unexpected verification barrier release response: {other:?}"
            ))),
            Err(_) => Err(MidgeError::Internal(
                "runtime response channel closed during verification barrier release".to_string(),
            )),
        }
    }

    /// Submit a message and wait synchronously for its response.
    ///
    /// The `RuntimeMsg` MUST carry a `request_id`. Use `next_request_id()` when
    /// constructing such messages.
    pub fn send_and_wait(&self, msg: RuntimeMsg) -> MidgeResult<RuntimeResponse> {
        let submission_guard = self.lifecycle.begin_submission()?;
        let request_id = msg.request_id().ok_or_else(|| {
            MidgeError::Internal(
                "send_and_wait called with message that has no request_id (e.g. Shutdown)"
                    .to_string(),
            )
        })?;

        let msg_kind = msg.kind_name();

        // Register for the response before sending the request.
        let rx = self.router.register(request_id);

        if let Err(error) = self.msg_tx.try_send(msg) {
            self.router.unregister(request_id);
            return Err(Self::map_submission_error(error));
        }
        drop(submission_guard);

        // Block waiting for the single response.
        // If debug-wait mode is enabled, emit a periodic warning to help
        // diagnose hangs (e.g., CloudAsync waiting on CloudAck).
        if std::env::var_os("MIDGE_DEBUG_WAIT").is_some() {
            let mut waited = std::time::Duration::from_secs(0);
            loop {
                match rx.recv_timeout(std::time::Duration::from_secs(2)) {
                    Ok(resp) => return Ok(resp),
                    Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                        waited += std::time::Duration::from_secs(2);
                        eprintln!(
                            "[midge] still waiting for response: request_id={request_id} kind={msg_kind} waited={waited:?}"
                        );
                    }
                    Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                        return Err(MidgeError::Internal("Response channel closed".to_string()));
                    }
                }
            }
        }

        rx.recv()
            .map_err(|_| MidgeError::Internal("Response channel closed".to_string()))
    }

    /// Submit a message and wait up to `timeout` for its response.
    ///
    /// Returns `Ok(None)` on timeout. In that case, the pending response slot
    /// is unregistered to avoid leaking the router map.
    pub fn send_and_wait_timeout(
        &self,
        msg: RuntimeMsg,
        timeout: std::time::Duration,
    ) -> MidgeResult<Option<RuntimeResponse>> {
        let submission_guard = self.lifecycle.begin_submission()?;
        let request_id = msg.request_id().ok_or_else(|| {
            MidgeError::Internal(
                "send_and_wait_timeout called with message that has no request_id".to_string(),
            )
        })?;

        // Register for the response before sending the request.
        let rx = self.router.register(request_id);

        if let Err(error) = self.msg_tx.try_send(msg) {
            self.router.unregister(request_id);
            return Err(Self::map_submission_error(error));
        }
        drop(submission_guard);

        match rx.recv_timeout(timeout) {
            Ok(resp) => Ok(Some(resp)),
            Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                self.router.unregister(request_id);
                Ok(None)
            }
            Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                self.router.unregister(request_id);
                Err(MidgeError::Internal("Response channel closed".to_string()))
            }
        }
    }

    /// Submit a message and wait for a response that matches a predicate.
    ///
    /// Since each `request_id` yields exactly one response, this is mainly
    /// useful for callers that want to validate the response shape.
    pub fn send_and_wait_filtered<F>(
        &self,
        msg: RuntimeMsg,
        mut predicate: F,
    ) -> MidgeResult<RuntimeResponse>
    where
        F: FnMut(&RuntimeResponse) -> bool,
    {
        let resp = self.send_and_wait(msg)?;
        if predicate(&resp) {
            Ok(resp)
        } else {
            Err(MidgeError::Internal(
                "Response did not satisfy predicate".to_string(),
            ))
        }
    }

    /// Request runtime shutdown and wait no longer than `timeout` for the
    /// final durability result.
    pub fn shutdown(&self, timeout: Duration) -> MidgeResult<()> {
        let started = std::time::Instant::now();
        self.lifecycle.begin_shutdown();
        let active_transactions = self.lifecycle.active_transaction_count();
        if active_transactions != 0 {
            return Err(MidgeError::Busy(format!(
                "{active_transactions} transaction(s) are still active"
            )));
        }

        if self.lifecycle.state() == RuntimeLifecycleState::Closed
            || !self.lifecycle.running.load(Ordering::Acquire)
        {
            self.lifecycle.mark_closed();
            return Ok(());
        }

        if self.lifecycle.shutdown_acknowledged.load(Ordering::Acquire) {
            let remaining = timeout.saturating_sub(started.elapsed());
            return if self.lifecycle.wait_until_stopped(remaining) {
                Ok(())
            } else {
                Err(MidgeError::Timeout(
                    "runtime worker did not terminate before shutdown deadline".to_string(),
                ))
            };
        }

        let mut shutdown_response = self
            .lifecycle
            .shutdown_response
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if shutdown_response.is_none() {
            let request_id = next_request_id()?;
            let response_rx = self.router.register(request_id);
            let remaining = timeout.saturating_sub(started.elapsed());
            match self
                .msg_tx
                .send_timeout(RuntimeMsg::ShutdownWithResponse { request_id }, remaining)
            {
                Ok(()) => {}
                Err(crossbeam::channel::SendTimeoutError::Timeout(_)) => {
                    self.router.unregister(request_id);
                    return Err(MidgeError::Timeout(
                        "runtime shutdown request queue remained full until deadline".to_string(),
                    ));
                }
                Err(crossbeam::channel::SendTimeoutError::Disconnected(_)) => {
                    self.router.unregister(request_id);
                    if self
                        .lifecycle
                        .wait_until_stopped(timeout.saturating_sub(started.elapsed()))
                    {
                        return Ok(());
                    }
                    return Err(MidgeError::Timeout(
                        "runtime shutdown channel closed before worker termination".to_string(),
                    ));
                }
            }
            *shutdown_response = Some(response_rx);
        }

        let remaining = timeout.saturating_sub(started.elapsed());
        let response = shutdown_response
            .as_ref()
            .expect("shutdown response receiver must be registered")
            .recv_timeout(remaining);
        match response {
            Ok(response) => {
                shutdown_response.take();
                self.lifecycle
                    .shutdown_acknowledged
                    .store(true, Ordering::Release);
                match response {
                    RuntimeResponse::Ok { .. } => Ok(()),
                    RuntimeResponse::Error { error, .. } => Err(error),
                    other => Err(MidgeError::Internal(format!(
                        "Unexpected response to shutdown: {other:?}"
                    ))),
                }
            }
            Err(crossbeam::channel::RecvTimeoutError::Timeout) => Err(MidgeError::Timeout(
                "runtime did not acknowledge shutdown before deadline".to_string(),
            )),
            Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                shutdown_response.take();
                if self
                    .lifecycle
                    .wait_until_stopped(timeout.saturating_sub(started.elapsed()))
                {
                    Ok(())
                } else {
                    Err(MidgeError::Timeout(
                        "runtime response channel closed before worker termination".to_string(),
                    ))
                }
            }
        }
    }

    pub(crate) fn send_apply_transaction_and_wait(
        &self,
        request_id: u64,
        ops: Vec<TransactionOp>,
        durability_policy: Option<DurabilityPolicy>,
        start_sequence: Option<u64>,
        conflict_policy: ConflictPolicy,
    ) -> MidgeResult<RuntimeResponse> {
        let submission_guard = self.lifecycle.begin_submission()?;
        let (response_tx, response_rx) = channel::bounded(1);
        self.msg_tx
            .try_send(RuntimeMsg::ApplyTransaction {
                request_id,
                ops,
                durability_policy,
                start_sequence,
                conflict_policy,
                response_tx: Some(response_tx),
            })
            .map_err(Self::map_submission_error)?;
        drop(submission_guard);

        response_rx
            .recv()
            .map_err(|_| MidgeError::Internal("Response channel closed".to_string()))
    }

    pub(crate) fn send_apply_transaction_and_wait_timeout(
        &self,
        request_id: u64,
        ops: Vec<TransactionOp>,
        durability_policy: Option<DurabilityPolicy>,
        start_sequence: Option<u64>,
        conflict_policy: ConflictPolicy,
        timeout: Duration,
    ) -> MidgeResult<Option<RuntimeResponse>> {
        let submission_guard = self.lifecycle.begin_submission()?;
        let (response_tx, response_rx) = channel::bounded(1);
        self.msg_tx
            .try_send(RuntimeMsg::ApplyTransaction {
                request_id,
                ops,
                durability_policy,
                start_sequence,
                conflict_policy,
                response_tx: Some(response_tx),
            })
            .map_err(Self::map_submission_error)?;
        drop(submission_guard);

        match response_rx.recv_timeout(timeout) {
            Ok(response) => Ok(Some(response)),
            Err(crossbeam::channel::RecvTimeoutError::Timeout) => Ok(None),
            Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                Err(MidgeError::Internal("Response channel closed".to_string()))
            }
        }
    }

    pub(crate) fn send_spilled_transaction_and_wait(
        &self,
        request_id: u64,
        source: crate::runtime::transaction_spill::TransactionOpSource,
        durability_policy: Option<DurabilityPolicy>,
        start_sequence: u64,
        conflict_policy: ConflictPolicy,
    ) -> MidgeResult<RuntimeResponse> {
        let submission_guard = self.lifecycle.begin_submission()?;
        let (response_tx, response_rx) = channel::bounded(1);
        self.msg_tx
            .try_send(RuntimeMsg::ApplySpilledTransaction {
                request_id,
                source,
                durability_policy,
                start_sequence,
                conflict_policy,
                response_tx: Some(response_tx),
            })
            .map_err(Self::map_submission_error)?;
        drop(submission_guard);

        response_rx
            .recv()
            .map_err(|_| MidgeError::Internal("Response channel closed".to_string()))
    }

    /// Check if writes should be stalled for the given column family.
    ///
    /// Returns `true` if memory budget is exceeded (immutable memtable queue full
    /// or total memtable memory over threshold).
    ///
    /// This probe is an advisory preflight. If the runtime cannot answer promptly,
    /// report stalled instead of letting a client commit block indefinitely.
    ///
    /// Used by `Engine::commit()` to expose backpressure to clients before
    /// accepting new write transactions.
    pub fn check_write_stall(&self, cf_id: crate::types::ColumnFamilyId) -> MidgeResult<bool> {
        let response = self.send_and_wait_timeout(
            RuntimeMsg::CheckWriteStall {
                request_id: next_request_id()?,
                cf_id,
            },
            WRITE_STALL_STATUS_TIMEOUT,
        )?;

        match response {
            Some(RuntimeResponse::WriteStallStatus { is_stalled, .. }) => Ok(is_stalled),
            Some(RuntimeResponse::Error { error, .. }) => Err(error),
            Some(_) => Err(MidgeError::Internal(
                "Unexpected response to CheckWriteStall".to_string(),
            )),
            None => Ok(true),
        }
    }
}
