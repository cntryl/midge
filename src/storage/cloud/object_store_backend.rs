//! The cloud callback interface over an existing object store. This lets a
//! filesystem object store run the same immutable metadata publication path.
//! Calls complete inline or wait within their request budget; no worker is spawned.

use super::{CloudBackend, CloudCallback, CloudError, CloudEvent, CloudStorage};
use crate::common::{DeadlineScope, OperationDeadline};
use crate::storage::inventory::ObjectStore;
use crate::storage::{
    StorageError, StorageErrorKind, StorageEvent, StorageObjectMetadata, StorageOutcome,
    StoragePrecondition, StorageRequest,
};
use std::sync::{mpsc, Arc};
use std::time::Duration;

struct ObjectStoreBackend {
    store: Arc<dyn ObjectStore>,
    timeout: Duration,
}

impl CloudStorage {
    pub(crate) fn from_object_store(store: Arc<dyn ObjectStore>, timeout: Duration) -> Self {
        Self::new_with_timeout(
            Arc::new(ObjectStoreBackend { store, timeout }),
            String::new(),
            timeout,
        )
    }
}

fn cloud_error(error: StorageError) -> CloudError {
    match error.kind() {
        StorageErrorKind::NotFound => CloudError::NotFound(error.into_message()),
        StorageErrorKind::PreconditionFailed => {
            CloudError::PreconditionFailed(error.into_message())
        }
        StorageErrorKind::Timeout => CloudError::Timeout(error.into_message()),
        StorageErrorKind::Transport => CloudError::Transport(error.into_message()),
        _ => CloudError::Protocol(error.into_message()),
    }
}

fn outcome<T: Clone>(result: StorageOutcome<T>) -> Result<T, CloudError> {
    match result {
        StorageOutcome::Ok(value) => Ok(value),
        StorageOutcome::Err(error) => Err(cloud_error(error)),
    }
}

fn wait<T>(rx: &mpsc::Receiver<T>, request: &StorageRequest) -> Result<T, CloudError> {
    if request.remaining_timeout().is_zero() {
        return Err(CloudError::Timeout(
            "object store request deadline exhausted".into(),
        ));
    }
    let answer = rx
        .recv_timeout(request.remaining_timeout())
        .map_err(|error| match error {
            mpsc::RecvTimeoutError::Timeout => {
                CloudError::Timeout("object store callback deadline exhausted".into())
            }
            mpsc::RecvTimeoutError::Disconnected => {
                CloudError::Protocol("object store callback closed".into())
            }
        })?;
    if request.remaining_timeout().is_zero() {
        return Err(CloudError::Timeout(
            "object store callback crossed deadline".into(),
        ));
    }
    Ok(answer)
}

impl ObjectStoreBackend {
    fn request(
        &self,
        key: &str,
        headers: &[(String, String)],
    ) -> Result<StorageRequest, CloudError> {
        let mut timeout = self.timeout;
        let mut condition = StoragePrecondition::None;
        for (name, value) in headers {
            let selected = if name.eq_ignore_ascii_case(super::REQUEST_TIMEOUT_HEADER) {
                timeout =
                    timeout.min(Duration::from_millis(value.parse().map_err(|_| {
                        CloudError::Protocol("invalid object store timeout".into())
                    })?));
                None
            } else if name.eq_ignore_ascii_case("If-None-Match") && value == "*" {
                Some(StoragePrecondition::IfAbsent)
            } else if name.eq_ignore_ascii_case("If-Match") {
                Some(StoragePrecondition::IfMatch(StorageObjectMetadata {
                    size: 0,
                    etag: value.clone(),
                    generation: None,
                }))
            } else if name.eq_ignore_ascii_case("x-goog-if-generation-match") {
                Some(if value == "0" {
                    StoragePrecondition::IfAbsent
                } else {
                    StoragePrecondition::IfMatch(StorageObjectMetadata {
                        size: 0,
                        etag: String::new(),
                        generation: Some(value.clone()),
                    })
                })
            } else {
                return Err(CloudError::Protocol(format!(
                    "unsupported object store request header '{name}'"
                )));
            };
            if let Some(selected) = selected {
                if condition != StoragePrecondition::None {
                    return Err(CloudError::Protocol(
                        "multiple object store mutation conditions".into(),
                    ));
                }
                condition = selected;
            }
        }
        Ok(
            StorageRequest::new(key, OperationDeadline::from_budget(timeout), timeout)
                .with_precondition(condition),
        )
    }

    fn get(
        &self,
        key: &str,
        timeout: Duration,
    ) -> Result<(Vec<u8>, StorageObjectMetadata), CloudError> {
        let request = StorageRequest::new(key, OperationDeadline::from_budget(timeout), timeout);
        let (tx, rx) = mpsc::channel();
        self.store.submit_metadata_read_request(request.clone(), tx);
        wait(&rx, &request)?.map_err(cloud_error)
    }
}

impl CloudBackend for ObjectStoreBackend {
    fn submit_put_with_reservation(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        reservation: Option<Arc<crate::common::resource_budget::ResourceReservation>>,
        callback: CloudCallback,
    ) {
        let result = self.request(key, &headers).and_then(|mut request| {
            request.reservation = reservation;
            let (tx, rx) = mpsc::channel();
            self.store.submit_write_request(request.clone(), data, tx);
            match wait(&rx, &request)? {
                StorageEvent::WriteComplete {
                    key: actual,
                    result,
                } if actual == key => outcome(result),
                _ => Err(CloudError::Protocol(
                    "unexpected object store PUT callback".into(),
                )),
            }
        });
        let _ = callback.send(CloudEvent::Put {
            key: key.into(),
            result,
        });
    }
    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        self.submit_put_with_reservation(key, data, headers, None, callback);
    }
    fn submit_get(&self, key: &str, callback: CloudCallback) {
        self.submit_get_with_timeout(key, self.timeout, callback);
    }
    fn submit_get_with_timeout(&self, key: &str, timeout: Duration, callback: CloudCallback) {
        let result = self
            .get(key, timeout.min(self.timeout))
            .map(|(bytes, _)| bytes);
        let _ = callback.send(CloudEvent::Get {
            key: key.into(),
            result,
        });
    }
    fn submit_get_with_metadata(&self, key: &str, callback: CloudCallback) {
        self.submit_get_with_metadata_with_timeout(key, self.timeout, callback);
    }
    fn submit_get_with_metadata_with_timeout(
        &self,
        key: &str,
        timeout: Duration,
        callback: CloudCallback,
    ) {
        let result = self.get(key, timeout.min(self.timeout));
        let _ = callback.send(CloudEvent::GetWithMetadata {
            key: key.into(),
            result,
        });
    }
    fn submit_get_range_with_identity(
        &self,
        key: &str,
        start: u64,
        end: u64,
        expected: StorageObjectMetadata,
        timeout: Duration,
        callback: CloudCallback,
    ) {
        self.submit_get_range_with_reservation(key, start..end, expected, timeout, None, callback);
    }

    fn submit_get_range_with_reservation(
        &self,
        key: &str,
        range: std::ops::Range<u64>,
        expected: StorageObjectMetadata,
        timeout: Duration,
        reservation: Option<Arc<crate::common::resource_budget::ResourceReservation>>,
        callback: CloudCallback,
    ) {
        let start = range.start;
        let end = range.end;
        let timeout = timeout.min(self.timeout);
        let mut request =
            StorageRequest::new(key, OperationDeadline::from_budget(timeout), timeout)
                .with_precondition(StoragePrecondition::IfMatch(expected));
        request.reservation = reservation;
        let (tx, rx) = mpsc::channel();
        self.store
            .submit_range_read_request(request.clone(), start..end, tx);
        let result = wait(&rx, &request).and_then(|answer| answer.map_err(cloud_error));
        let _ = callback.send(CloudEvent::GetRange {
            key: key.into(),
            start,
            end: Some(end),
            result,
        });
    }
    #[cfg(test)]
    fn submit_get_range(&self, key: &str, start: u64, end: Option<u64>, callback: CloudCallback) {
        let result = self.get(key, self.timeout).and_then(|(bytes, _)| {
            let start = usize::try_from(start)
                .map_err(|_| CloudError::Protocol("invalid range start".into()))?;
            let end = end
                .map_or(Ok(bytes.len()), usize::try_from)
                .map_err(|_| CloudError::Protocol("invalid range end".into()))?;
            bytes
                .get(start..end)
                .map(<[u8]>::to_vec)
                .ok_or_else(|| CloudError::Protocol("invalid range".into()))
        });
        let _ = callback.send(CloudEvent::GetRange {
            key: key.into(),
            start,
            end,
            result,
        });
    }
    fn submit_delete(&self, key: &str, headers: Vec<(String, String)>, callback: CloudCallback) {
        let result = self.request(key, &headers).and_then(|request| {
            let (tx, rx) = mpsc::channel();
            self.store.submit_delete_request(request.clone(), tx);
            match wait(&rx, &request)? {
                StorageEvent::DeleteComplete {
                    key: actual,
                    result,
                } if actual == key => outcome(result),
                _ => Err(CloudError::Protocol(
                    "unexpected object store DELETE callback".into(),
                )),
            }
        });
        let _ = callback.send(CloudEvent::Delete {
            key: key.into(),
            result,
        });
    }
    fn submit_head(&self, key: &str, callback: CloudCallback) {
        self.submit_head_with_headers(key, Vec::new(), callback);
    }
    fn submit_head_with_headers(
        &self,
        key: &str,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        let result = self.request(key, &headers).and_then(|request| {
            let (tx, rx) = mpsc::channel();
            self.store.submit_head_request(request.clone(), tx);
            match wait(&rx, &request)? {
                StorageEvent::HeadComplete {
                    key: actual,
                    result,
                } if actual == key => outcome(result),
                _ => Err(CloudError::Protocol(
                    "unexpected object store HEAD callback".into(),
                )),
            }
        });
        let _ = callback.send(CloudEvent::Head {
            key: key.into(),
            result,
        });
    }
    fn submit_list(&self, prefix: &str, callback: CloudCallback) {
        self.submit_list_with_headers(prefix, Vec::new(), callback);
    }
    fn submit_list_with_headers(
        &self,
        prefix: &str,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        let result = self.request(prefix, &headers).and_then(|request| {
            let scope = DeadlineScope::new(request.deadline);
            self.store
                .keys_under(prefix, Some(&scope))
                .map_err(|error| match error {
                    crate::common::MidgeError::Timeout(message) => CloudError::Timeout(message),
                    error => CloudError::Protocol(error.to_string()),
                })
        });
        let _ = callback.send(CloudEvent::List {
            prefix: prefix.into(),
            result,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::cloud::BlockingCloud;

    #[test]
    fn should_preserve_conditional_object_identity_when_wrapping_a_filesystem_store() {
        // Arrange
        let directory = tempfile::tempdir().unwrap();
        let store =
            Arc::new(crate::storage::filesystem::FileSystem::new(directory.path()).unwrap());
        let cloud = CloudStorage::from_object_store(store, Duration::from_secs(5));
        let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
        let io = BlockingCloud::new(&cloud, &deadline);
        io.put_with_precondition(
            "metadata/test",
            b"first".to_vec(),
            &StoragePrecondition::IfAbsent,
        )
        .unwrap();
        let first = io.head_optional("metadata/test").unwrap().unwrap();
        io.put_with_precondition(
            "metadata/test",
            b"second".to_vec(),
            &StoragePrecondition::IfMatch(first.clone()),
        )
        .unwrap();
        // Act
        let stale = io.put_with_precondition(
            "metadata/test",
            b"stale".to_vec(),
            &StoragePrecondition::IfMatch(first),
        );
        // Assert
        assert!(stale.is_err());
        assert_eq!(
            io.get_optional("metadata/test").unwrap().unwrap(),
            b"second"
        );
        assert_eq!(io.list("metadata/").unwrap(), ["metadata/test"]);
    }

    #[test]
    fn should_map_every_condition_header_cloud_storage_emits_when_building_a_store_request() {
        // Arrange: every header list the cloud storage encoders produce.
        let directory = tempfile::tempdir().unwrap();
        let backend = ObjectStoreBackend {
            store: Arc::new(crate::storage::filesystem::FileSystem::new(directory.path()).unwrap()),
            timeout: Duration::from_secs(5),
        };
        let by_etag = StorageObjectMetadata {
            size: 3,
            etag: "\"v1\"".into(),
            generation: None,
        };
        let by_generation = StorageObjectMetadata {
            generation: Some("7".into()),
            ..by_etag.clone()
        };
        let mut emitted = Vec::new();
        for condition in [
            StoragePrecondition::None,
            StoragePrecondition::IfAbsent,
            StoragePrecondition::IfMatch(by_etag.clone()),
            StoragePrecondition::IfMatch(by_generation.clone()),
        ] {
            emitted.push((condition.headers().unwrap(), condition));
        }
        for identity in [&by_etag, &by_generation] {
            emitted.push((
                super::super::object_match_precondition_headers(
                    &identity.etag,
                    identity.generation.as_deref(),
                )
                .unwrap(),
                StoragePrecondition::IfMatch(identity.clone()),
            ));
        }
        let identity = |condition: &StoragePrecondition| match condition {
            StoragePrecondition::IfMatch(metadata) => crate::storage::conditional_object_identity(
                &metadata.etag,
                metadata.generation.as_deref(),
            )
            .map(|(name, value)| (name, value.to_string())),
            _ => None,
        };

        for (mut headers, expected) in emitted {
            super::super::set_request_timeout_header(&mut headers, Duration::from_secs(1));

            // Act
            let request = backend.request("metadata/test", &headers).unwrap();

            // Assert
            assert_eq!(
                std::mem::discriminant(&request.precondition),
                std::mem::discriminant(&expected),
                "{headers:?}"
            );
            assert_eq!(identity(&request.precondition), identity(&expected));
            assert!(request.timeout <= Duration::from_secs(1), "{headers:?}");
        }
    }
}
