//! Per-request response routing.

use super::RuntimeResponse;
use crate::common::MidgeError;
use crossbeam::channel::{self, Receiver, Sender};
use dashmap::DashMap;
use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, PoisonError};
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
    fn insert(&mut self, request_id: u64, request: AbandonedRequest) {
        if self.by_request_id.insert(request_id, request).is_some() {
            if let Some(position) = self
                .fifo
                .iter()
                .position(|existing_id| *existing_id == request_id)
            {
                let _ = self.fifo.remove(position);
            }
        }
        self.fifo.push_back(request_id);

        while self.by_request_id.len() > ABANDONED_REQUEST_CAPACITY {
            let Some(evicted_id) = self.fifo.pop_front() else {
                break;
            };
            self.by_request_id.remove(&evicted_id);
        }
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
}

impl ResponseRouter {
    pub fn new() -> Self {
        Self {
            pending: DashMap::new(),
            abandoned: Mutex::new(AbandonedRequests::default()),
        }
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
    /// If no pending entry exists, logs a warning and drops the response.
    pub fn complete(&self, response: RuntimeResponse) {
        let request_id = response.request_id();
        if let Some((_, pending)) = self.pending.remove(&request_id) {
            let _ = pending.response_tx.send(response);
        } else {
            let completed_at = Instant::now();
            let response_variant = response.kind_name();
            let pending_depth = self.pending_len();
            let abandoned = self
                .abandoned
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .by_request_id
                .get(&request_id)
                .copied();

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
    pub fn abandon(&self, request_id: u64, timeout: Duration) {
        let abandoned_at = Instant::now();
        let mut abandoned = self
            .abandoned
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some((_, pending)) = self.pending.remove(&request_id) {
            abandoned.insert(
                request_id,
                AbandonedRequest {
                    request_kind: pending.request_kind,
                    timeout,
                    abandoned_at,
                    registered_at: pending.registered_at,
                },
            );
        }
    }

    pub(crate) fn fail_all(&self, message: &str) {
        let pending = self
            .pending
            .iter()
            .map(|entry| *entry.key())
            .collect::<Vec<_>>();
        for request_id in pending {
            if let Some((_, pending)) = self.pending.remove(&request_id) {
                let _ = pending.response_tx.send(RuntimeResponse::Error {
                    request_id,
                    error: MidgeError::Internal(message.to_string()),
                });
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
}
