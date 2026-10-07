//! Deadline adapters that preserve asynchronous buffer ownership.

use super::{
    cloud_to_storage_outcome, Arc, CloudEvent, CloudStorage, StorageCallback, StorageEvent,
    StorageObjectMetadata, StorageOutcome,
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
        let timeout = self.scoped_timeout(timeout);
        let deadline = crate::common::OperationDeadline::from_budget(timeout);
        let start = range.start;
        let end = range.end;
        if timeout.is_zero() {
            let _ = callback.send(Err(crate::storage::storage_timeout_error(
                "conditional range request has no remaining budget",
            )));
            return;
        }
        if start >= end || end > expected.size || !expected.same_version(&expected) {
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
        let result = match super::adapter::await_cloud_event(self, &rx, &deadline, "range GET") {
            Ok(CloudEvent::GetRange {
                key: returned,
                start: actual_start,
                end: actual_end,
                result,
            }) if returned == full_key && actual_start == start && actual_end == Some(end) => {
                result
                    .map_err(super::storage_error_from_cloud)
                    .and_then(|bytes| {
                        if u64::try_from(bytes.len()).ok() == Some(end - start) {
                            Ok(bytes)
                        } else {
                            Err("remote SST range response length mismatch".into())
                        }
                    })
            }
            Ok(event) => Err(crate::storage::StorageError::protocol(format!(
                "unexpected conditional range response: {event:?}"
            ))),
            Err(error) => Err(error),
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
        let timeout = self.scoped_timeout(timeout);
        let deadline = crate::common::OperationDeadline::from_budget(timeout);
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
        super::set_request_timeout_header(&mut headers, timeout);
        self.backend.submit_put_with_reservation(
            &self.full_path(key),
            data,
            headers,
            reservation,
            tx,
        );
        let event = match super::adapter::await_cloud_event(self, &rx, &deadline, "PUT") {
            Ok(CloudEvent::Put { result, .. }) => StorageEvent::WriteComplete {
                key: key.to_string(),
                result: cloud_to_storage_outcome(result),
            },
            Ok(other) => StorageEvent::WriteComplete {
                key: key.to_string(),
                result: StorageOutcome::Err(
                    format!("unexpected cloud PUT response: {other:?}").into(),
                ),
            },
            Err(error) => StorageEvent::WriteComplete {
                key: key.to_string(),
                result: StorageOutcome::Err(error),
            },
        };
        let _ = callback.send(event);
    }
}
