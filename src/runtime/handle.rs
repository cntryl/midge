//! Runtime request handle and synchronous request APIs.

use super::lifecycle::{ShutdownState, ShutdownTerminal};
use super::{
    next_request_id, snapshot_cache, snapshot_pins, ResponseRouter, RuntimeLifecycle,
    RuntimeLifecycleState, RuntimeMsg, RuntimeResponse, RuntimeTransactionGuard,
    SpilledTransactionSubmission, TransactionSubmission,
};
use crate::common::{MidgeError, MidgeResult, OperationDeadline};
use crossbeam::channel::Sender;
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
    pub(super) runtime_response_timeout: Duration,
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

    /// Whether `MIDGE_DEBUG_WAIT` diagnostics are enabled.
    ///
    /// Read once rather than per request; this sits on the synchronous request
    /// path.
    fn debug_waits_enabled() -> bool {
        static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *ENABLED.get_or_init(|| std::env::var_os("MIDGE_DEBUG_WAIT").is_some())
    }

    fn response_timeout_error(
        request_kind: &'static str,
        request_id: u64,
        timeout: Duration,
    ) -> MidgeError {
        MidgeError::Timeout(format!(
            "runtime request {request_kind} request_id={request_id} exceeded response timeout {timeout:?}"
        ))
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
        let (removed, released_last_sst_pin) =
            self.snapshot_pins.unregister_with_gc_hint(snapshot_id);
        if released_last_sst_pin {
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
        let msg = RuntimeMsg::EndStorageVerification { request_id, token };
        let msg_kind = msg.kind_name();
        let deadline = OperationDeadline::from_budget(self.runtime_response_timeout);
        let response_rx = self.router.register(request_id, msg_kind);
        match self.msg_tx.send_timeout(msg, deadline.remaining()) {
            Ok(()) => {}
            Err(crossbeam::channel::SendTimeoutError::Timeout(_)) => {
                self.router.cancel(request_id);
                return Err(Self::response_timeout_error(
                    msg_kind,
                    request_id,
                    self.runtime_response_timeout,
                ));
            }
            Err(crossbeam::channel::SendTimeoutError::Disconnected(_)) => {
                self.router.cancel(request_id);
                return Err(MidgeError::Internal(
                    "runtime channel closed before verification barrier release".to_string(),
                ));
            }
        }

        match response_rx.recv_timeout(deadline.remaining()) {
            Ok(RuntimeResponse::Ok { .. }) => Ok(()),
            Ok(RuntimeResponse::Error { error, .. }) => Err(error),
            Ok(other) => Err(MidgeError::Internal(format!(
                "unexpected verification barrier release response: {other:?}"
            ))),
            Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                if self
                    .router
                    .abandon(request_id, self.runtime_response_timeout)
                {
                    Err(Self::response_timeout_error(
                        msg_kind,
                        request_id,
                        self.runtime_response_timeout,
                    ))
                } else {
                    let response = response_rx.recv().map_err(|_| {
                        MidgeError::Internal(
                            "runtime response channel closed while completion owned verification barrier release"
                                .to_string(),
                        )
                    })?;
                    match response {
                        RuntimeResponse::Ok { .. } => Ok(()),
                        RuntimeResponse::Error { error, .. } => Err(error),
                        other => Err(MidgeError::Internal(format!(
                            "unexpected verification barrier release response: {other:?}"
                        ))),
                    }
                }
            }
            Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                self.router.cancel(request_id);
                Err(MidgeError::Internal(
                    "runtime response channel closed during verification barrier release"
                        .to_string(),
                ))
            }
        }
    }

    /// Submit a message and wait synchronously for its response.
    ///
    /// The `RuntimeMsg` MUST carry a `request_id`. Use `next_request_id()` when
    /// constructing such messages. The wait is bounded by the runtime response
    /// timeout configured when the Engine was opened.
    pub fn send_and_wait(&self, msg: RuntimeMsg) -> MidgeResult<RuntimeResponse> {
        let request_id = msg.request_id().ok_or_else(|| {
            MidgeError::Internal(
                "send_and_wait called with message that has no request_id (e.g. Shutdown)"
                    .to_string(),
            )
        })?;
        let msg_kind = msg.kind_name();
        self.send_and_wait_with_timeout(msg, self.runtime_response_timeout, true)?
            .ok_or_else(|| {
                Self::response_timeout_error(msg_kind, request_id, self.runtime_response_timeout)
            })
    }

    /// Submit a message and wait up to `timeout` for its response.
    ///
    /// Returns `Ok(None)` on timeout. In that case, the pending response slot
    /// becomes a bounded diagnostic tombstone for any late response.
    pub fn send_and_wait_timeout(
        &self,
        msg: RuntimeMsg,
        timeout: std::time::Duration,
    ) -> MidgeResult<Option<RuntimeResponse>> {
        self.send_and_wait_with_timeout(msg, timeout, false)
    }

    fn send_and_wait_with_timeout(
        &self,
        msg: RuntimeMsg,
        timeout: Duration,
        emit_debug_waits: bool,
    ) -> MidgeResult<Option<RuntimeResponse>> {
        let submission_guard = self.lifecycle.begin_submission()?;
        let request_id = msg.request_id().ok_or_else(|| {
            MidgeError::Internal(
                "runtime request submitted without a request_id (e.g. Shutdown)".to_string(),
            )
        })?;
        let msg_kind = msg.kind_name();

        // Register for the response before sending the request.
        let rx = self.router.register(request_id, msg_kind);

        // Deliberately non-blocking, unlike the shutdown and verification-barrier
        // paths which spend their deadline on the send. A full queue here means
        // the runtime is already saturated, and this is the hot write path: a
        // caller is better served by immediate `WriteStall` backpressure it can
        // react to than by silently spending its response budget queueing.
        if let Err(error) = self.msg_tx.try_send(msg) {
            self.router.cancel(request_id);
            return Err(Self::map_submission_error(error));
        }
        drop(submission_guard);

        let started_at = std::time::Instant::now();
        let debug_waits = emit_debug_waits && Self::debug_waits_enabled();
        loop {
            let remaining = timeout.saturating_sub(started_at.elapsed());
            let wait_for = if debug_waits {
                remaining.min(Duration::from_secs(2))
            } else {
                remaining
            };
            match rx.recv_timeout(wait_for) {
                Ok(resp) => return Ok(Some(resp)),
                Err(crossbeam::channel::RecvTimeoutError::Timeout)
                    if started_at.elapsed() >= timeout =>
                {
                    if self.router.abandon(request_id, timeout) {
                        return Ok(None);
                    }
                    return rx.recv().map(Some).map_err(|_| {
                        MidgeError::Internal(
                            "Response channel closed while completion owned the request"
                                .to_string(),
                        )
                    });
                }
                Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                    tracing::debug!(
                        request_id,
                        kind = msg_kind,
                        waited = ?started_at.elapsed(),
                        "still waiting for runtime response"
                    );
                }
                Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                    self.router.cancel(request_id);
                    return Err(MidgeError::Internal("Response channel closed".to_string()));
                }
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
    ///
    /// A successful cloud-backed shutdown seals and publishes the active WAL,
    /// checkpoints non-empty memtables through SST/manifest publication, and
    /// conservatively retires covered WAL authority. Any checkpoint failure is
    /// returned while the WAL is retained for recovery.
    pub fn shutdown(&self, timeout: Duration) -> MidgeResult<()> {
        let deadline = OperationDeadline::from_budget(timeout);

        {
            let shutdown = self
                .lifecycle
                .shutdown
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let ShutdownState::Terminal(terminal) = &*shutdown {
                return terminal.replay();
            }
        }

        self.lifecycle.begin_shutdown();
        let active_transactions = self.lifecycle.active_transaction_count();
        if active_transactions != 0 {
            return Err(MidgeError::Busy(format!(
                "{active_transactions} transaction(s) are still active"
            )));
        }

        loop {
            let mut shutdown = self
                .lifecycle
                .shutdown
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match &*shutdown {
                ShutdownState::Terminal(terminal) => return terminal.replay(),
                ShutdownState::Idle => {
                    if self.lifecycle.state() == RuntimeLifecycleState::Closed
                        || !self.lifecycle.running.load(Ordering::Acquire)
                    {
                        *shutdown = ShutdownState::Terminal(ShutdownTerminal::Success);
                        self.lifecycle.shutdown.changed.notify_all();
                        self.lifecycle.mark_closed();
                        return Ok(());
                    }
                    *shutdown = ShutdownState::Sending;
                    drop(shutdown);
                    return self.send_shutdown_request(&deadline);
                }
                ShutdownState::Ready { .. } => {
                    let previous = std::mem::replace(&mut *shutdown, ShutdownState::Idle);
                    let ShutdownState::Ready {
                        request_id,
                        response_rx,
                    } = previous
                    else {
                        unreachable!("matched an available shutdown response receiver")
                    };
                    *shutdown = ShutdownState::Receiving { request_id };
                    drop(shutdown);
                    return self.receive_shutdown_response(request_id, response_rx, &deadline);
                }
                ShutdownState::ResponseDisconnected => {
                    drop(shutdown);
                    return self.wait_for_shutdown_stop(&deadline);
                }
                ShutdownState::Sending | ShutdownState::Receiving { .. } => {
                    let remaining = deadline.remaining();
                    if remaining.is_zero() {
                        return Err(Self::shutdown_ack_timeout());
                    }
                    let (next_shutdown, _) = self
                        .lifecycle
                        .shutdown
                        .changed
                        .wait_timeout(shutdown, remaining)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    drop(next_shutdown);
                }
            }
        }
    }

    fn send_shutdown_request(&self, deadline: &OperationDeadline) -> MidgeResult<()> {
        let request_id = match next_request_id() {
            Ok(request_id) => request_id,
            Err(error) => {
                self.reset_shutdown_sender();
                return Err(error);
            }
        };
        let msg = RuntimeMsg::ShutdownWithResponse { request_id };
        let response_rx = self.router.register(request_id, msg.kind_name());
        match self.msg_tx.send_timeout(msg, deadline.remaining()) {
            Ok(()) => {
                let mut shutdown = self
                    .lifecycle
                    .shutdown
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                *shutdown = ShutdownState::Receiving { request_id };
                drop(shutdown);
                self.receive_shutdown_response(request_id, response_rx, deadline)
            }
            Err(crossbeam::channel::SendTimeoutError::Timeout(_)) => {
                self.router.cancel(request_id);
                self.reset_shutdown_sender();
                Err(MidgeError::Timeout(
                    "runtime shutdown request queue remained full until deadline".to_string(),
                ))
            }
            Err(crossbeam::channel::SendTimeoutError::Disconnected(_)) => {
                self.router.cancel(request_id);
                self.mark_shutdown_response_disconnected();
                self.wait_for_shutdown_stop(deadline)
            }
        }
    }

    fn receive_shutdown_response(
        &self,
        request_id: u64,
        response_rx: crossbeam::channel::Receiver<RuntimeResponse>,
        deadline: &OperationDeadline,
    ) -> MidgeResult<()> {
        match response_rx.recv_timeout(deadline.remaining()) {
            Ok(response) => self.publish_shutdown_terminal(Self::shutdown_terminal(response)),
            Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                if !self.lifecycle.running.load(Ordering::Acquire) {
                    if let Ok(response) = response_rx.try_recv() {
                        return self.publish_shutdown_terminal(Self::shutdown_terminal(response));
                    }
                    self.router.cancel(request_id);
                    return self.publish_shutdown_terminal(ShutdownTerminal::Success);
                }
                let mut shutdown = self
                    .lifecycle
                    .shutdown
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                debug_assert!(matches!(
                    &*shutdown,
                    ShutdownState::Receiving {
                        request_id: active_request_id
                    } if *active_request_id == request_id
                ));
                *shutdown = ShutdownState::Ready {
                    request_id,
                    response_rx,
                };
                self.lifecycle.shutdown.changed.notify_all();
                Err(Self::shutdown_ack_timeout())
            }
            Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                self.mark_shutdown_response_disconnected();
                self.wait_for_shutdown_stop(deadline)
            }
        }
    }

    fn wait_for_shutdown_stop(&self, deadline: &OperationDeadline) -> MidgeResult<()> {
        if self.lifecycle.wait_until_stopped(deadline.remaining()) {
            self.publish_shutdown_terminal(ShutdownTerminal::Success)
        } else {
            Err(MidgeError::Timeout(
                "runtime response channel closed before worker termination".to_string(),
            ))
        }
    }

    fn publish_shutdown_terminal(&self, terminal: ShutdownTerminal) -> MidgeResult<()> {
        let mut shutdown = self
            .lifecycle
            .shutdown
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let ShutdownState::Terminal(existing) = &*shutdown {
            return existing.replay();
        }
        let result = terminal.replay();
        *shutdown = ShutdownState::Terminal(terminal);
        self.lifecycle.shutdown.changed.notify_all();
        result
    }

    fn reset_shutdown_sender(&self) {
        let mut shutdown = self
            .lifecycle
            .shutdown
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *shutdown = ShutdownState::Idle;
        self.lifecycle.shutdown.changed.notify_all();
    }

    fn mark_shutdown_response_disconnected(&self) {
        let mut shutdown = self
            .lifecycle
            .shutdown
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !matches!(*shutdown, ShutdownState::Terminal(_)) {
            *shutdown = ShutdownState::ResponseDisconnected;
        }
        self.lifecycle.shutdown.changed.notify_all();
    }

    fn shutdown_ack_timeout() -> MidgeError {
        MidgeError::Timeout("runtime did not acknowledge shutdown before deadline".to_string())
    }

    fn shutdown_terminal(response: RuntimeResponse) -> ShutdownTerminal {
        match response {
            RuntimeResponse::Ok { .. } => ShutdownTerminal::Success,
            RuntimeResponse::Error { error, .. } => ShutdownTerminal::Error(error),
            other => ShutdownTerminal::Error(MidgeError::Internal(format!(
                "Unexpected response to shutdown: {other:?}"
            ))),
        }
    }

    pub(crate) fn send_apply_transaction_and_wait(
        &self,
        request_id: u64,
        submission: TransactionSubmission,
    ) -> MidgeResult<RuntimeResponse> {
        let submission_guard = self.lifecycle.begin_submission()?;
        let response_rx = self.router.register(request_id, "ApplyTransaction");
        if let Err(error) = self.msg_tx.try_send(RuntimeMsg::ApplyTransaction {
            request_id,
            ops: submission.ops,
            assertions: submission.assertions,
            durability_policy: submission.durability_policy,
            start_sequence: submission.start_sequence,
            conflict_policy: submission.conflict_policy,
            response_tx: None,
        }) {
            self.router.cancel(request_id);
            return Err(Self::map_submission_error(error));
        }
        drop(submission_guard);

        self.receive_routed_response(
            &response_rx,
            "ApplyTransaction",
            request_id,
            self.runtime_response_timeout,
        )
    }

    pub(crate) fn send_spilled_transaction_and_wait(
        &self,
        request_id: u64,
        submission: SpilledTransactionSubmission,
    ) -> MidgeResult<RuntimeResponse> {
        let submission_guard = self.lifecycle.begin_submission()?;
        let response_rx = self.router.register(request_id, "ApplySpilledTransaction");
        if let Err(error) = self.msg_tx.try_send(RuntimeMsg::ApplySpilledTransaction {
            request_id,
            source: submission.source,
            assertions: submission.assertions,
            durability_policy: submission.durability_policy,
            start_sequence: submission.start_sequence,
            conflict_policy: submission.conflict_policy,
            response_tx: None,
        }) {
            self.router.cancel(request_id);
            return Err(Self::map_submission_error(error));
        }
        drop(submission_guard);

        self.receive_routed_response(
            &response_rx,
            "ApplySpilledTransaction",
            request_id,
            self.runtime_response_timeout,
        )
    }

    fn receive_routed_response(
        &self,
        response_rx: &crossbeam::channel::Receiver<RuntimeResponse>,
        request_kind: &'static str,
        request_id: u64,
        timeout: Duration,
    ) -> MidgeResult<RuntimeResponse> {
        match response_rx.recv_timeout(timeout) {
            Ok(response) => Ok(response),
            Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                if self.router.abandon(request_id, timeout) {
                    Err(Self::response_timeout_error(
                        request_kind,
                        request_id,
                        timeout,
                    ))
                } else {
                    response_rx.recv().map_err(|_| {
                        MidgeError::Internal(
                            "Response channel closed while transaction completion owned the request"
                                .to_string(),
                        )
                    })
                }
            }
            Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                Err(MidgeError::Internal("Response channel closed".to_string()))
            }
        }
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
