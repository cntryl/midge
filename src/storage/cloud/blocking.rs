//! Deadline-aware synchronous adapter over the callback-oriented cloud API.
//!
//! Callers that must wait for a cloud round trip (startup recovery, the flush
//! worker, the event loop's metadata mirror) all go through this one type so a
//! timeout is reported as [`MidgeError::Timeout`] everywhere and a provider
//! failure carries the same context. It owns only the callback protocol,
//! deadline clamping, and response-shape validation; no policy lives here.

use super::{
    contextualize_operation_error, is_not_found_error, CloudEvent, CloudOutcome, CloudStorage,
    ObjectMetadata,
};
use crate::common::{MidgeError, MidgeResult, OperationDeadline};
use std::sync::mpsc::RecvTimeoutError;

pub(crate) struct BlockingCloud<'a> {
    cloud: &'a CloudStorage,
    deadline: &'a OperationDeadline,
}

impl<'a> BlockingCloud<'a> {
    pub(crate) fn new(cloud: &'a CloudStorage, deadline: &'a OperationDeadline) -> Self {
        Self { cloud, deadline }
    }

    fn timeout(&self, operation: &str, key: &str) -> MidgeResult<std::time::Duration> {
        self.deadline
            .clamp_nonzero(self.cloud.callback_timeout())
            .ok_or_else(|| {
                MidgeError::Timeout(format!(
                    "operation deadline exhausted before cloud {operation} for '{key}'"
                ))
            })
    }

    fn wait<T>(
        &self,
        rx: &std::sync::mpsc::Receiver<CloudEvent>,
        deadline: &OperationDeadline,
        operation: &str,
        key: &str,
        extract: impl Fn(CloudEvent) -> Option<CloudOutcome<T>>,
    ) -> MidgeResult<Option<T>> {
        let timeout = self.remaining(deadline, operation, key)?;
        let event = rx.recv_timeout(timeout);
        self.remaining(deadline, operation, key)?;
        match event {
            Ok(event) => match extract(event) {
                Some(CloudOutcome::Ok(value)) => Ok(Some(value)),
                Some(CloudOutcome::Err(error)) if is_not_found_error(&error) => Ok(None),
                Some(CloudOutcome::Err(error)) => Err(contextualize_operation_error(
                    &error,
                    format_args!("cloud {operation} '{key}' failed"),
                    self.deadline,
                )),
                None => Err(MidgeError::Internal(format!(
                    "unexpected cloud {operation} response for '{key}'"
                ))),
            },
            Err(RecvTimeoutError::Timeout) => Err(MidgeError::Timeout(format!(
                "cloud {operation} '{key}' exceeded the operation deadline"
            ))),
            Err(RecvTimeoutError::Disconnected) => Err(MidgeError::Internal(format!(
                "cloud {operation} callback closed for '{key}'"
            ))),
        }
    }

    fn begin(&self, operation: &str, key: &str) -> MidgeResult<OperationDeadline> {
        let started = std::time::Instant::now();
        let timeout = self.timeout(operation, key)?;
        Ok(OperationDeadline::from_start(started, timeout))
    }

    fn remaining(
        &self,
        deadline: &OperationDeadline,
        operation: &str,
        key: &str,
    ) -> MidgeResult<std::time::Duration> {
        self.cloud
            .check_startup_scope("cloud callback completion")?;
        deadline
            .clamp_nonzero(self.deadline.remaining())
            .ok_or_else(|| {
                MidgeError::Timeout(format!(
                    "operation deadline exhausted during cloud {operation} for '{key}'"
                ))
            })
    }

    pub(crate) fn get_optional(&self, key: &str) -> MidgeResult<Option<Vec<u8>>> {
        let read_deadline = self.begin("get", key)?;
        let (tx, rx) = std::sync::mpsc::channel();
        let request_timeout = self.remaining(&read_deadline, "get", key)?;
        self.cloud.submit_get_within(key, request_timeout, tx);
        self.wait(&rx, &read_deadline, "get", key, |event| match event {
            CloudEvent::Get { result, .. } => Some(result),
            _ => None,
        })
    }

    pub(crate) fn head_optional(&self, key: &str) -> MidgeResult<Option<ObjectMetadata>> {
        let deadline = self.begin("head", key)?;
        let (tx, rx) = std::sync::mpsc::channel();
        self.cloud
            .submit_head_within(key, self.remaining(&deadline, "head", key)?, tx);
        self.wait(&rx, &deadline, "head", key, |event| match event {
            CloudEvent::Head { result, .. } => Some(result),
            _ => None,
        })
    }

    pub(crate) fn list(&self, prefix: &str) -> MidgeResult<Vec<String>> {
        let deadline = self.begin("list", prefix)?;
        let (tx, rx) = std::sync::mpsc::channel();
        self.cloud
            .submit_list_within(prefix, self.remaining(&deadline, "list", prefix)?, tx);
        let keys = self.wait(&rx, &deadline, "list", prefix, |event| match event {
            CloudEvent::List { result, .. } => Some(result),
            _ => None,
        })?;
        Ok(keys.unwrap_or_default())
    }

    pub(crate) fn put_with_headers(
        &self,
        key: &str,
        data: Vec<u8>,
        mut headers: Vec<(String, String)>,
    ) -> MidgeResult<()> {
        let deadline = self.begin("put", key)?;
        let (tx, rx) = std::sync::mpsc::channel();
        let timeout = self.remaining(&deadline, "put", key)?;
        super::set_request_timeout_header(&mut headers, timeout);
        self.cloud.submit_put(key, data, headers, tx);
        let event = rx.recv_timeout(self.remaining(&deadline, "put", key)?);
        self.remaining(&deadline, "put", key)?;
        match event {
            Ok(CloudEvent::Put { result, .. }) => match result {
                CloudOutcome::Ok(()) => Ok(()),
                CloudOutcome::Err(error) => Err(contextualize_operation_error(
                    &error,
                    format_args!("cloud put '{key}' failed"),
                    self.deadline,
                )),
            },
            Ok(other) => Err(MidgeError::Internal(format!(
                "unexpected cloud put response for '{key}': {other:?}"
            ))),
            Err(RecvTimeoutError::Timeout) => Err(MidgeError::Timeout(format!(
                "cloud put '{key}' exceeded the operation deadline"
            ))),
            Err(RecvTimeoutError::Disconnected) => Err(MidgeError::Internal(format!(
                "cloud put callback closed for '{key}'"
            ))),
        }
    }

    pub(crate) fn put_with_precondition(
        &self,
        key: &str,
        data: Vec<u8>,
        precondition: &crate::storage::StoragePrecondition,
    ) -> MidgeResult<()> {
        let headers = precondition
            .headers()
            .map_err(|error| MidgeError::Internal(error.to_string()))?;
        self.put_with_headers(key, data, headers)
    }
}
