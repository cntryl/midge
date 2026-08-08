//! Per-request response routing.

use super::RuntimeResponse;
use crate::common::MidgeError;
use crossbeam::channel::{self, Receiver, Sender};
use dashmap::DashMap;

/// Uses `DashMap` for lock-free concurrent access, eliminating the
/// `Mutex<HashMap>` contention point between caller threads (register)
/// and the event loop thread (complete).
#[derive(Debug)]
pub(crate) struct ResponseRouter {
    pending: DashMap<u64, Sender<RuntimeResponse>>,
}

impl ResponseRouter {
    pub fn new() -> Self {
        Self {
            pending: DashMap::new(),
        }
    }

    /// Register a new pending response for a given `request_id`.
    ///
    /// Returns a receiver that will yield exactly one `RuntimeResponse`.
    pub fn register(&self, request_id: u64) -> Receiver<RuntimeResponse> {
        let (tx, rx) = channel::bounded(1);
        self.pending.insert(request_id, tx);
        rx
    }

    /// Complete a request by delivering its response to the waiting receiver.
    ///
    /// If no pending entry exists, logs a warning and drops the response.
    pub fn complete(&self, response: RuntimeResponse) {
        let request_id = response.request_id();
        if let Some((_, tx)) = self.pending.remove(&request_id) {
            let _ = tx.send(response);
        } else {
            tracing::warn!(
                request_id,
                "response received with no matching pending request"
            );
        }
    }

    /// Remove a pending request without delivering a response.
    ///
    /// Used by timeout-based waits so callers can abandon a request cleanly.
    pub fn unregister(&self, request_id: u64) {
        let _ = self.pending.remove(&request_id);
    }

    pub(crate) fn fail_all(&self, message: &str) {
        let pending = self
            .pending
            .iter()
            .map(|entry| *entry.key())
            .collect::<Vec<_>>();
        for request_id in pending {
            if let Some((_, tx)) = self.pending.remove(&request_id) {
                let _ = tx.send(RuntimeResponse::Error {
                    request_id,
                    error: MidgeError::Internal(message.to_string()),
                });
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn pending_len(&self) -> usize {
        self.pending.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_not_leak_pending_entry_when_unregistered_after_timeout() {
        // Arrange
        let router = ResponseRouter::new();
        let request_id = 1;

        // Act: caller registers, then abandons the request the way
        // `send_and_wait_timeout` does when its `recv_timeout` elapses.
        let _rx = router.register(request_id);
        assert_eq!(router.pending_len(), 1);
        router.unregister(request_id);

        // Assert
        assert_eq!(router.pending_len(), 0);
    }

    #[test]
    fn should_not_leak_pending_entries_across_repeated_timeouts() {
        // Arrange
        let router = ResponseRouter::new();

        // Act: simulate several requests that each time out and are abandoned.
        for request_id in 0..10 {
            let _rx = router.register(request_id);
            router.unregister(request_id);
        }

        // Assert
        assert_eq!(
            router.pending_len(),
            0,
            "repeated register/unregister cycles must not accumulate entries"
        );
    }

    #[test]
    fn should_ignore_late_response_for_a_request_already_unregistered() {
        // Arrange
        let router = ResponseRouter::new();
        let request_id = 7;
        let _rx = router.register(request_id);
        router.unregister(request_id);

        // Act: the event loop finishes the abandoned request after the caller
        // already gave up (mirrors a worker thread unblocking post-timeout).
        router.complete(RuntimeResponse::Ok { request_id });

        // Assert: no pending entry is resurrected by the late response.
        assert_eq!(router.pending_len(), 0);
    }
}
