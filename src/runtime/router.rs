//! Per-request response routing.

use super::RuntimeResponse;
use crate::common::MidgeError;
use crossbeam::channel::{self, Receiver, Sender};
use dashmap::DashMap;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError, TryLockError};
use std::time::{Duration, Instant};

const ABANDONED_REQUEST_CAPACITY: usize = 1_024;

#[derive(Debug)]
struct PendingRequest {
    response_tx: Sender<RuntimeResponse>,
    request_kind: &'static str,
    registered_at: Instant,
}

#[derive(Clone, Copy, Debug)]
struct AbandonedRequest {
    request_kind: &'static str,
    timeout: Duration,
    abandoned_at: Instant,
    registered_at: Instant,
}

#[derive(Debug, Default)]
struct AbandonedRequests {
    by_request_id: HashMap<u64, AbandonedRequest>,
    fifo: VecDeque<u64>,
}

impl AbandonedRequests {
    fn insert(&mut self, request_id: u64, request: AbandonedRequest) -> bool {
        if self.by_request_id.contains_key(&request_id) {
            return false;
        }
        self.by_request_id.insert(request_id, request);
        self.fifo.push_back(request_id);

        while self.by_request_id.len() > ABANDONED_REQUEST_CAPACITY {
            let Some(evicted_id) = self.fifo.pop_front() else {
                break;
            };
            self.by_request_id.remove(&evicted_id);
        }

        true
    }
}

/// Uses `DashMap` for sharded concurrent access between caller threads
/// (register) and the event loop thread (complete). The tombstone mutex is
/// acquired only for abandonment and unmatched completion, keeping normal
/// matched routing off that lock.
#[derive(Debug)]
pub(crate) struct ResponseRouter {
    pending: DashMap<u64, PendingRequest>,
    abandoned: Mutex<AbandonedRequests>,
    /// Responses that arrived with no caller waiting for them. Counts work the
    /// runtime completed after its caller's deadline expired, which is the
    /// observable half of an ambiguous `MidgeError::Timeout`.
    late_responses_total: AtomicU64,
    /// Requests whose caller stopped waiting before a response arrived.
    abandoned_requests_total: AtomicU64,
}

impl ResponseRouter {
    pub fn new() -> Self {
        Self {
            pending: DashMap::new(),
            abandoned: Mutex::new(AbandonedRequests::default()),
            late_responses_total: AtomicU64::new(0),
            abandoned_requests_total: AtomicU64::new(0),
        }
    }

    /// Responses delivered with no matching pending request.
    pub(crate) fn late_responses_total(&self) -> u64 {
        self.late_responses_total.load(Ordering::Relaxed)
    }

    /// Requests abandoned by a caller that timed out.
    pub(crate) fn abandoned_requests_total(&self) -> u64 {
        self.abandoned_requests_total.load(Ordering::Relaxed)
    }

    /// Record a late response delivered outside this router's pending table.
    ///
    /// The inline response path (`EventLoop::register_inline_response`) bypasses
    /// `complete`, so it reports late arrivals here instead.
    pub(crate) fn record_late_response(&self) {
        self.late_responses_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Instant at which `request_id` began waiting, when it is still pending.
    ///
    /// `None` means the caller is already gone — either it never registered or
    /// it has been abandoned. Callers deriving a deadline treat `None` as
    /// cancellation rather than as an unbounded budget.
    pub(crate) fn registered_at(&self, request_id: u64) -> Option<Instant> {
        self.pending
            .get(&request_id)
            .map(|pending| pending.registered_at)
    }

    /// Register a new pending response for a given `request_id`.
    ///
    /// Returns a receiver that will yield exactly one `RuntimeResponse`.
    pub fn register(
        &self,
        request_id: u64,
        request_kind: &'static str,
    ) -> Receiver<RuntimeResponse> {
        let (tx, rx) = channel::bounded(1);
        self.pending.insert(
            request_id,
            PendingRequest {
                response_tx: tx,
                request_kind,
                registered_at: Instant::now(),
            },
        );
        rx
    }

    /// Complete a request by delivering its response to the waiting receiver.
    ///
    /// If no pending entry exists, counts a late response, logs a warning, and
    /// drops the response.
    pub fn complete(&self, response: RuntimeResponse) {
        let request_id = response.request_id();
        if let Some((_, pending)) = self.pending.remove(&request_id) {
            // Removing the route is the completion ownership claim. A caller
            // that reaches its timeout concurrently observes that the route is
            // gone and receives from the channel instead of returning Timeout.
            // If the receiver was independently dropped, this completion is
            // genuinely late and must still be reflected in telemetry.
            if pending.response_tx.send(response).is_err() {
                self.late_responses_total.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    request_id,
                    request_kind = pending.request_kind,
                    pending_depth = self.pending_len(),
                    "response receiver disappeared after completion claimed its route"
                );
            }
        } else {
            self.late_responses_total.fetch_add(1, Ordering::Relaxed);

            let completed_at = Instant::now();
            let response_variant = response.kind_name();
            let pending_depth = self.pending_len();
            // `complete` runs on the event loop thread while `abandon` runs on
            // arbitrary caller threads. A mass timeout is exactly when both are
            // busy, so the loop must never wait here: the counter above is
            // already exact, and only the enriched context is best-effort.
            let abandoned = match self.abandoned.try_lock() {
                Ok(abandoned) => abandoned.by_request_id.get(&request_id).copied(),
                Err(TryLockError::Poisoned(poisoned)) => poisoned
                    .into_inner()
                    .by_request_id
                    .get(&request_id)
                    .copied(),
                Err(TryLockError::WouldBlock) => None,
            };

            if let Some(abandoned) = abandoned {
                tracing::warn!(
                    request_id,
                    request_kind = abandoned.request_kind,
                    configured_timeout = ?abandoned.timeout,
                    age_since_abandonment = ?completed_at.saturating_duration_since(abandoned.abandoned_at),
                    total_age_since_registration = ?completed_at.saturating_duration_since(abandoned.registered_at),
                    response_variant,
                    pending_depth,
                    "response received with no matching pending request"
                );
            } else {
                tracing::warn!(
                    request_id,
                    pending_depth,
                    "response received with no matching pending request"
                );
            }
        }
    }

    /// Remove a request that was never submitted or can no longer receive a response.
    pub fn cancel(&self, request_id: u64) {
        let _ = self.pending.remove(&request_id);
    }

    /// Abandon a pending request after its configured response timeout.
    ///
    /// The tombstone keeps enough bounded diagnostic context to identify a late
    /// response. Locking the tombstone store before removing the pending entry
    /// ensures a concurrent completion either wins the response race or observes
    /// the newly inserted tombstone.
    ///
    /// Returns `true` when the caller claimed timeout ownership. `false` means
    /// completion already removed the route and the caller must receive the
    /// response that completion now owns.
    pub fn abandon(&self, request_id: u64, timeout: Duration) -> bool {
        let abandoned_at = Instant::now();
        let mut abandoned = self
            .abandoned
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let Some((_, pending)) = self.pending.remove(&request_id) else {
            return false;
        };
        let context = AbandonedRequest {
            request_kind: pending.request_kind,
            timeout,
            abandoned_at,
            registered_at: pending.registered_at,
        };
        if abandoned.insert(request_id, context) {
            self.abandoned_requests_total
                .fetch_add(1, Ordering::Relaxed);
        }
        true
    }

    /// Fail every pending request.
    ///
    /// Drains repeatedly rather than once: on the event-loop panic path the
    /// lifecycle gate is closed just before this runs, but a submission that
    /// already passed the gate can still register between two passes. Looping
    /// until the table is empty means such a caller is failed rather than left
    /// to wait out its full response timeout.
    pub(crate) fn fail_all(&self, message: &str) {
        loop {
            let pending = self
                .pending
                .iter()
                .map(|entry| *entry.key())
                .collect::<Vec<_>>();
            if pending.is_empty() {
                return;
            }
            for request_id in pending {
                if let Some((_, pending)) = self.pending.remove(&request_id) {
                    let _ = pending.response_tx.send(RuntimeResponse::Error {
                        request_id,
                        error: MidgeError::Internal(message.to_string()),
                    });
                }
            }
        }
    }

    pub(crate) fn pending_len(&self) -> usize {
        self.pending.len()
    }

    #[cfg(test)]
    fn abandoned_len(&self) -> usize {
        self.abandoned
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .by_request_id
            .len()
    }

    #[cfg(test)]
    fn has_abandoned(&self, request_id: u64) -> bool {
        self.abandoned
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .by_request_id
            .contains_key(&request_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[derive(Clone)]
    struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

    struct CapturedLogWriter(Arc<Mutex<Vec<u8>>>);

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
        type Writer = CapturedLogWriter;

        fn make_writer(&'a self) -> Self::Writer {
            CapturedLogWriter(Arc::clone(&self.0))
        }
    }

    impl Write for CapturedLogWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn capture_logs(action: impl FnOnce()) -> String {
        let captured = CapturedLogs(Arc::new(Mutex::new(Vec::new())));
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_max_level(tracing::Level::WARN)
            .with_writer(captured.clone())
            .finish();

        tracing::subscriber::with_default(subscriber, action);

        let logs = String::from_utf8(
            captured
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        )
        .expect("captured logs should be UTF-8");
        logs
    }

    #[test]
    fn should_remove_pending_entry_when_abandoned_after_timeout() {
        // Arrange
        let router = ResponseRouter::new();
        let request_id = 1;

        // Act: caller registers, then abandons the request the way
        // `send_and_wait_timeout` does when its `recv_timeout` elapses.
        let _rx = router.register(request_id, "Queue");
        assert_eq!(router.pending_len(), 1);
        router.abandon(request_id, Duration::from_millis(250));

        // Assert
        assert_eq!(router.pending_len(), 0);
    }

    #[test]
    fn should_not_leak_pending_entries_across_repeated_timeouts() {
        // Arrange
        let router = ResponseRouter::new();

        // Act: simulate several requests that each time out and are abandoned.
        for request_id in 0..10 {
            let _rx = router.register(request_id, "Queue");
            router.abandon(request_id, Duration::from_millis(250));
        }

        // Assert
        assert_eq!(
            router.pending_len(),
            0,
            "repeated register/abandon cycles must not accumulate pending entries"
        );
    }

    #[test]
    fn should_log_request_context_when_late_response_arrives_after_abandonment() {
        // Arrange
        let router = ResponseRouter::new();
        let request_id = 7;
        let _rx = router.register(request_id, "Queue");
        router.abandon(request_id, Duration::from_millis(250));

        // Act: the event loop finishes the abandoned request after the caller
        // already gave up (mirrors a worker thread unblocking post-timeout).
        let logs = capture_logs(|| router.complete(RuntimeResponse::Ok { request_id }));

        // Assert
        assert_eq!(router.pending_len(), 0);
        assert!(logs.contains("response received with no matching pending request"));
        assert!(logs.contains("request_id=7"), "missing request id: {logs}");
        assert!(
            logs.contains("request_kind=\"Queue\""),
            "missing kind: {logs}"
        );
        assert!(
            logs.contains("configured_timeout=250ms"),
            "missing timeout: {logs}"
        );
        assert!(
            logs.contains("age_since_abandonment="),
            "missing abandonment age: {logs}"
        );
        assert!(
            logs.contains("total_age_since_registration="),
            "missing total age: {logs}"
        );
        assert!(
            logs.contains("response_variant=\"Ok\""),
            "missing response variant: {logs}"
        );
        assert!(
            logs.contains("pending_depth=0"),
            "missing pending depth: {logs}"
        );
    }

    #[test]
    fn should_evict_oldest_tombstone_when_capacity_is_reached() {
        // Arrange
        let router = ResponseRouter::new();

        // Act
        for request_id in 0..=ABANDONED_REQUEST_CAPACITY as u64 {
            let _rx = router.register(request_id, "Queue");
            router.abandon(request_id, Duration::from_millis(250));
        }

        // Assert
        assert_eq!(router.abandoned_len(), ABANDONED_REQUEST_CAPACITY);
        assert!(
            !router.has_abandoned(0),
            "oldest tombstone should be evicted"
        );
        assert!(router.has_abandoned(1));
        assert!(router.has_abandoned(ABANDONED_REQUEST_CAPACITY as u64));
    }

    #[test]
    fn should_log_request_id_only_context_when_tombstone_is_unavailable() {
        // Arrange
        let router = ResponseRouter::new();

        // Act
        let logs = capture_logs(|| router.complete(RuntimeResponse::Ok { request_id: 77 }));

        // Assert
        assert!(logs.contains("response received with no matching pending request"));
        assert!(logs.contains("request_id=77"), "missing request id: {logs}");
        assert!(
            logs.contains("pending_depth=0"),
            "missing pending depth: {logs}"
        );
        assert!(
            !logs.contains("request_kind="),
            "unexpected tombstone: {logs}"
        );
        assert!(
            !logs.contains("configured_timeout="),
            "unexpected tombstone: {logs}"
        );
    }

    #[test]
    fn should_count_abandoned_request_given_pending_request_when_caller_times_out() {
        // Arrange
        let router = ResponseRouter::new();
        let request_id = 11;
        let _rx = router.register(request_id, "Queue");
        assert_eq!(router.abandoned_requests_total(), 0);

        // Act
        router.abandon(request_id, Duration::from_millis(250));

        // Assert
        assert_eq!(router.abandoned_requests_total(), 1);
    }

    #[test]
    fn should_not_count_abandoned_request_given_no_pending_entry_when_abandon_is_retried() {
        // Arrange: the first abandon removes the pending entry.
        let router = ResponseRouter::new();
        let request_id = 12;
        let _rx = router.register(request_id, "Queue");
        router.abandon(request_id, Duration::from_millis(250));

        // Act: a second abandon has nothing left to abandon.
        router.abandon(request_id, Duration::from_millis(250));

        // Assert
        assert_eq!(
            router.abandoned_requests_total(),
            1,
            "only a real abandonment counts"
        );
    }

    #[test]
    fn should_count_late_response_given_abandoned_request_when_response_arrives_after_timeout() {
        // Arrange
        let router = ResponseRouter::new();
        let request_id = 13;
        let _rx = router.register(request_id, "Queue");
        router.abandon(request_id, Duration::from_millis(250));

        // Act: the event loop finishes work whose caller already gave up.
        capture_logs(|| router.complete(RuntimeResponse::Ok { request_id }));

        // Assert
        assert_eq!(router.late_responses_total(), 1);
    }

    #[test]
    fn should_count_late_response_given_unknown_request_when_no_tombstone_exists() {
        // Arrange
        let router = ResponseRouter::new();

        // Act
        capture_logs(|| router.complete(RuntimeResponse::Ok { request_id: 14 }));

        // Assert
        assert_eq!(
            router.late_responses_total(),
            1,
            "a late response counts even without tombstone context"
        );
    }

    #[test]
    fn should_not_count_late_response_given_pending_request_when_response_is_matched() {
        // Arrange
        let router = ResponseRouter::new();
        let request_id = 15;
        let _rx = router.register(request_id, "Queue");

        // Act
        router.complete(RuntimeResponse::Ok { request_id });

        // Assert
        assert_eq!(router.late_responses_total(), 0);
        assert_eq!(router.abandoned_requests_total(), 0);
    }

    #[test]
    fn should_deliver_response_given_completion_claimed_route_before_timeout_is_recorded() {
        // Arrange: model the deadline-boundary ordering where the event loop
        // removes and completes the pending route just after `recv_timeout`
        // decides to time out, but before the caller records that decision.
        let router = ResponseRouter::new();
        let request_id = 19;
        let rx = router.register(request_id, "Queue");
        router.complete(RuntimeResponse::Ok { request_id });

        // Act
        router.abandon(request_id, Duration::from_millis(250));

        // Assert
        assert_eq!(
            router.abandoned_requests_total(),
            0,
            "completion ownership must turn the boundary timeout into a delivered response"
        );
        assert_eq!(router.late_responses_total(), 0);
        assert!(matches!(
            rx.recv_timeout(Duration::from_secs(1)),
            Ok(RuntimeResponse::Ok {
                request_id: response_id
            }) if response_id == request_id
        ));
    }

    #[test]
    fn should_count_late_response_given_receiver_drops_after_completion_claims_route() {
        // Arrange: a rendezvous channel pauses `complete` after it removes the
        // route, making the remove/send boundary deterministic.
        let router = Arc::new(ResponseRouter::new());
        let request_id = 20;
        let (response_tx, response_rx) = channel::bounded(0);
        router.pending.insert(
            request_id,
            PendingRequest {
                response_tx,
                request_kind: "Queue",
                registered_at: Instant::now(),
            },
        );
        let completer = Arc::clone(&router);
        let completion = std::thread::spawn(move || {
            completer.complete(RuntimeResponse::Ok { request_id });
        });
        let started = Instant::now();
        while router.pending.contains_key(&request_id) && started.elapsed() < Duration::from_secs(1)
        {
            std::thread::yield_now();
        }
        assert!(
            !router.pending.contains_key(&request_id),
            "completion must claim the route before the receiver is dropped"
        );

        // Act
        drop(response_rx);
        completion.join().expect("completion thread");

        // Assert
        assert_eq!(
            router.late_responses_total(),
            1,
            "a failed boundary send is a late response"
        );
    }

    #[test]
    fn should_not_block_completion_given_contended_tombstone_lock_when_response_arrives() {
        // Arrange: a caller thread holds the tombstone mutex, standing in for a
        // burst of concurrent `abandon` calls during a mass timeout.
        let router = Arc::new(ResponseRouter::new());
        let request_id = 16;
        let _rx = router.register(request_id, "Queue");
        router.abandon(request_id, Duration::from_millis(250));

        let holder = Arc::clone(&router);
        let (acquired_tx, acquired_rx) = channel::bounded::<()>(1);
        let (release_tx, release_rx) = channel::bounded::<()>(1);
        let holder_thread = std::thread::spawn(move || {
            let guard = holder
                .abandoned
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            acquired_tx.send(()).expect("signal lock acquired");
            release_rx.recv().expect("await release");
            drop(guard);
        });
        acquired_rx.recv().expect("tombstone lock held");

        // Act: the event loop thread must complete without waiting on it.
        let completer = Arc::clone(&router);
        let (done_tx, done_rx) = channel::bounded::<()>(1);
        let completer_thread = std::thread::spawn(move || {
            completer.complete(RuntimeResponse::Ok { request_id });
            let _ = done_tx.send(());
        });

        let completed_promptly = done_rx.recv_timeout(Duration::from_secs(5)).is_ok();
        release_tx.send(()).expect("release tombstone lock");
        holder_thread.join().expect("holder thread");
        completer_thread.join().expect("completer thread");

        // Assert
        assert!(
            completed_promptly,
            "the event loop blocked on a caller-contended tombstone lock"
        );
        assert_eq!(
            router.late_responses_total(),
            1,
            "the counter stays exact under contention"
        );
    }

    #[test]
    fn should_report_registration_instant_given_pending_request_when_deadline_is_derived() {
        // Arrange
        let router = ResponseRouter::new();
        let request_id = 17;
        let before = Instant::now();
        let _rx = router.register(request_id, "Queue");
        let after = Instant::now();

        // Act
        let registered_at = router.registered_at(request_id);

        // Assert
        let registered_at = registered_at.expect("pending request exposes its start instant");
        assert!(registered_at >= before && registered_at <= after);
    }

    #[test]
    fn should_report_no_registration_given_abandoned_request_when_deadline_is_derived() {
        // Arrange
        let router = ResponseRouter::new();
        let request_id = 18;
        let _rx = router.register(request_id, "Queue");
        router.abandon(request_id, Duration::from_millis(250));

        // Act
        let registered_at = router.registered_at(request_id);

        // Assert
        assert!(
            registered_at.is_none(),
            "an abandoned caller must read as cancelled, not as an unbounded budget"
        );
    }

    #[test]
    fn should_drain_every_pending_request_given_multiple_waiters_when_fail_all_runs() {
        // Arrange
        let router = ResponseRouter::new();
        let early = router.register(1, "Queue");
        let late = router.register(2, "Queue");

        // Act
        router.fail_all("runtime event loop panicked before responding");

        // Assert
        assert_eq!(router.pending_len(), 0, "no pending entry may survive");
        for (request_id, rx) in [(1, early), (2, late)] {
            match rx.try_recv() {
                Ok(RuntimeResponse::Error { .. }) => {}
                other => panic!("request {request_id} was not failed: {other:?}"),
            }
        }
    }
}
