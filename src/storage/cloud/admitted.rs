//! Deadline adapters that preserve asynchronous buffer ownership.

use super::{
    cloud_to_storage_outcome, Arc, CloudEvent, CloudStorage, StorageCallback, StorageEvent,
    StorageObjectMetadata, StorageOutcome, REQUEST_TIMEOUT_HEADER,
};

impl CloudStorage {
    pub(super) fn read_range_admitted(
        &self,
        key: &str,
        range: std::ops::Range<u64>,
        expected: StorageObjectMetadata,
        timeout: std::time::Duration,
        reservation: Option<Arc<crate::common::resource_budget::ResourceReservation>>,
        callback: &crate::storage::RangeReadCallback,
    ) {
        let start = range.start;
        let end = range.end;
        if timeout.is_zero()
            || start >= end
            || end > expected.size
            || !expected.same_version(&expected)
        {
            let _ = callback.send(Err("invalid conditional range request".into()));
            return;
        }
        let full_key = self.full_path(key);
        let (tx, rx) = std::sync::mpsc::channel();
        self.backend.submit_get_range_with_reservation(
            &full_key,
            start..end,
            expected,
            timeout,
            reservation,
            tx,
        );
        let result = match rx.recv_timeout(timeout) {
            Ok(CloudEvent::GetRange {
                key: returned,
                start: actual_start,
                end: actual_end,
                result,
            }) if returned == full_key && actual_start == start && actual_end == Some(end) => {
                result
                    .map_err(|error| {
                        if error.is_timeout() {
                            crate::storage::storage_timeout_error(error)
                        } else {
                            error.to_string()
                        }
                    })
                    .and_then(|bytes| {
                        if u64::try_from(bytes.len()).ok() == Some(end - start) {
                            Ok(bytes)
                        } else {
                            Err("remote SST range response length mismatch".into())
                        }
                    })
            }
            Ok(event) => Err(format!("unexpected conditional range response: {event:?}")),
            Err(error) => Err(crate::storage::storage_timeout_error(error)),
        };
        let _ = callback.send(result);
    }
    pub(super) fn write_admitted(
        &self,
        key: &str,
        data: Vec<u8>,
        mut headers: Vec<(String, String)>,
        timeout: std::time::Duration,
        reservation: Option<Arc<crate::common::resource_budget::ResourceReservation>>,
        callback: &StorageCallback,
    ) {
        if timeout.is_zero() {
            let _ = callback.send(StorageEvent::WriteComplete {
                key: key.to_string(),
                result: StorageOutcome::Err(crate::storage::storage_timeout_error(
                    "cloud PUT refused because no callback budget remained",
                )),
            });
            return;
        }
        let (tx, rx) = std::sync::mpsc::channel();
        if reservation.is_some() {
            headers.retain(|(name, _)| !name.eq_ignore_ascii_case(REQUEST_TIMEOUT_HEADER));
            headers.push((
                REQUEST_TIMEOUT_HEADER.into(),
                timeout.as_millis().max(1).to_string(),
            ));
        }
        self.backend.submit_put_with_reservation(
            &self.full_path(key),
            data,
            headers,
            reservation,
            tx,
        );
        let event = match rx.recv_timeout(timeout) {
            Ok(CloudEvent::Put { key, result }) => StorageEvent::WriteComplete {
                key,
                result: cloud_to_storage_outcome(result),
            },
            Ok(other) => StorageEvent::WriteComplete {
                key: key.to_string(),
                result: StorageOutcome::Err(format!("unexpected cloud PUT response: {other:?}")),
            },
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => StorageEvent::WriteComplete {
                key: key.to_string(),
                result: StorageOutcome::Err(crate::storage::storage_timeout_error(
                    "cloud PUT callback timed out",
                )),
            },
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => StorageEvent::WriteComplete {
                key: key.to_string(),
                result: StorageOutcome::Err("cloud PUT callback closed".to_string()),
            },
        };
        let _ = callback.send(event);
    }
}
