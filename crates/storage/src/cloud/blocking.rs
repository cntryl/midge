//! Deadline-aware synchronous adapter over the callback-oriented cloud API.
//!
//! Callers that must wait for a cloud round trip (startup recovery, the flush
//! worker, the event loop's metadata mirror) all go through this one type so a
//! timeout is reported as [`MidgeError::Timeout`] everywhere and a provider
//! failure carries the same context. It owns the callback protocol, bounded
//! local read admission, deadline clamping, and response-shape validation.

use super::{
    contextualize_operation_error, is_not_found_error, CloudEvent, CloudOutcome, CloudReadOptions,
    CloudRequestOptions, CloudStorage, ObjectMetadata,
};
use cntryl_midge_core::{MidgeError, MidgeResult, OperationDeadline};
use std::sync::mpsc::RecvTimeoutError;

pub(crate) struct BlockingCloud<'a> {
    cloud: &'a CloudStorage,
    deadline: &'a OperationDeadline,
    reservation:
        Option<std::sync::Arc<cntryl_midge_core::common::resource_budget::ResourceReservation>>,
}

impl<'a> BlockingCloud<'a> {
    pub(crate) fn new(cloud: &'a CloudStorage, deadline: &'a OperationDeadline) -> Self {
        Self {
            cloud,
            deadline,
            reservation: None,
        }
    }

    pub(crate) fn with_reservation(
        mut self,
        reservation: std::sync::Arc<
            cntryl_midge_core::common::resource_budget::ResourceReservation,
        >,
    ) -> Self {
        self.reservation = Some(reservation);
        self
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
        loop {
            self.remaining(&read_deadline, "get", key)?;
            let (tx, rx) = std::sync::mpsc::channel();
            // The backend recomputes timeout/header controls from the same
            // absolute deadline after every local admission wait.
            self.cloud.reads.read(
                &self.cloud.full_path(key),
                CloudReadOptions::default().with_request_options(
                    CloudRequestOptions::default()
                        .with_deadline(read_deadline)
                        .with_reservation(self.reservation.clone()),
                ),
                tx,
            );
            let result = self.wait(&rx, &read_deadline, "get", key, |event| match event {
                CloudEvent::Get { result, .. } => Some(result),
                _ => None,
            });
            match result {
                Err(error)
                    if self
                        .reservation
                        .as_ref()
                        .is_some_and(|memory| memory.admission_retryable(&error)) =>
                {
                    // Only a definitely unsubmitted read reaches this branch.
                    // Do not retry provider effects or renew the caller budget.
                    std::thread::sleep(
                        self.remaining(&read_deadline, "get", key)?
                            .min(std::time::Duration::from_millis(1)),
                    );
                }
                result => return result,
            }
        }
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

    pub(crate) fn range_identity(&self, key: &str) -> MidgeResult<Option<ObjectMetadata>> {
        let deadline = self.begin("range HEAD", key)?;
        let (tx, rx) = std::sync::mpsc::channel();
        self.cloud.heads.head_for_range(
            &self.cloud.full_path(key),
            CloudRequestOptions::default().with_deadline(deadline),
            tx,
        );
        self.wait(&rx, &deadline, "range HEAD", key, |event| match event {
            CloudEvent::Head { result, .. } => Some(result),
            _ => None,
        })
    }

    /// Read a bounded immutable body under the exact identity supplied by HEAD.
    pub(crate) fn get_exact_len(&self, key: &str, len: u64) -> MidgeResult<Option<Vec<u8>>> {
        let Some(identity) = self.range_identity(key)? else {
            return Ok(None);
        };
        if identity.size != len || len == 0 {
            return Err(MidgeError::Corruption(format!(
                "cloud object '{key}' differs from its selected length {len}"
            )));
        }
        self.get_identity_range(key, identity)
    }

    pub(crate) fn get_bounded(&self, key: &str, max_len: u64) -> MidgeResult<Option<Vec<u8>>> {
        let Some(identity) = self.range_identity(key)? else {
            return Ok(None);
        };
        if identity.size == 0 || identity.size > max_len {
            return Err(MidgeError::Corruption(format!(
                "cloud object '{key}' is outside its allowed range of 1 to {max_len} bytes"
            )));
        }
        self.get_identity_range(key, identity)
    }

    fn get_identity_range(
        &self,
        key: &str,
        identity: ObjectMetadata,
    ) -> MidgeResult<Option<Vec<u8>>> {
        let deadline = self.begin("range", key)?;
        let (tx, rx) = std::sync::mpsc::channel();
        self.cloud.reads.read(
            &self.cloud.full_path(key),
            CloudReadOptions::default()
                .with_range(0..identity.size, identity)
                .with_request_options(
                    CloudRequestOptions::default()
                        .with_deadline(deadline)
                        .with_reservation(self.reservation.clone()),
                ),
            tx,
        );
        self.wait(&rx, &deadline, "range", key, |event| match event {
            CloudEvent::GetRange { result, .. } => Some(result),
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

    pub(crate) fn list_page(
        &self,
        prefix: &str,
        page: super::CloudListPageRequest,
    ) -> MidgeResult<Option<super::CloudListPage>> {
        let deadline = self.begin("LIST page", prefix)?;
        let timeout = self.remaining(&deadline, "LIST page", prefix)?;
        let (tx, rx) = std::sync::mpsc::channel();
        let expected = page.clone();
        self.cloud
            .submit_list_page_within(prefix, page, deadline, tx);
        let result = rx.recv_timeout(timeout).map_err(|error| {
            MidgeError::Timeout(format!("bounded cloud LIST page callback: {error}"))
        })?;
        self.remaining(&deadline, "LIST page", prefix)?;
        result
            .map_err(|error| {
                contextualize_operation_error(
                    &error,
                    format_args!("bounded cloud LIST page '{prefix}' failed"),
                    self.deadline,
                )
            })?
            .map(|result| {
                expected
                    .accept(
                        &self.cloud.full_path(prefix),
                        result.keys,
                        result.continuation,
                    )
                    .map_err(|error| MidgeError::Corruption(error.to_string()))
            })
            .transpose()
    }

    /// An admitted FORMAT 10 stage can be reclaimed only after each payload
    /// PUT has a positive terminal reply. Readback alone can prove bytes, but
    /// cannot prove that a timed-out request will never create them again.
    pub(crate) fn put_immutable_terminal(&self, key: &str, bytes: &[u8]) -> MidgeResult<()> {
        use crate::{StorageBackend, StoragePrecondition, StorageRequest};
        let deadline = self.begin("immutable put", key)?;
        let timeout = self.remaining(&deadline, "immutable put", key)?;
        let mut request = StorageRequest::new(key, deadline, timeout)
            .with_precondition(StoragePrecondition::IfAbsent);
        request.reservation.clone_from(&self.reservation);
        let (tx, rx) = std::sync::mpsc::channel();
        self.cloud
            .submit_write_receipt_request(request, bytes.to_vec(), tx);
        match rx.recv_timeout(self.remaining(&deadline, "immutable put", key)?) {
            Ok(Ok(Some(receipt))) if receipt.matches(bytes) => Ok(()),
            Ok(Ok(None))
                if self.get_exact_len(key, bytes.len() as u64)?.as_deref() == Some(bytes) =>
            {
                Ok(())
            }
            Ok(Ok(_)) => Err(MidgeError::Corruption(format!(
                "immutable cloud PUT '{key}' differs from the reserved body"
            ))),
            Ok(Err(error)) => Err(MidgeError::OutcomeUnknown(format!(
                "immutable cloud PUT '{key}' has no positive terminal reply: {error}"
            ))),
            Err(error) => Err(MidgeError::OutcomeUnknown(format!(
                "immutable cloud PUT '{key}' callback could not prove completion: {error}"
            ))),
        }
    }

    #[cfg(test)]
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

    #[cfg(test)]
    pub(crate) fn put_with_precondition(
        &self,
        key: &str,
        data: Vec<u8>,
        precondition: &crate::StoragePrecondition,
    ) -> MidgeResult<()> {
        let headers = precondition
            .headers()
            .map_err(|error| MidgeError::Internal(error.to_string()))?;
        self.put_with_headers(key, data, headers)
    }
}
