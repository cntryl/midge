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
}
