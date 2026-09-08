use crate::common::{MidgeError, MidgeResult};
use crate::storage::cloud::{CloudEvent, CloudOutcome, CloudStorage, ObjectMetadata};

/// Synchronous startup adapter over the runtime's callback-oriented cloud API.
///
/// Recovery policy remains in `CloudStartupRecovery`; this type owns only the
/// callback protocol, timeout, and response-shape validation.
pub(in crate::engine) struct BlockingCloudIo<'a> {
    cloud: &'a CloudStorage,
}

impl<'a> BlockingCloudIo<'a> {
    pub(in crate::engine) fn new(cloud: &'a CloudStorage) -> Self {
        Self { cloud }
    }

    pub(in crate::engine) fn list(&self, prefix: &str) -> MidgeResult<Vec<String>> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.cloud.submit_list(prefix, tx);
        match rx.recv_timeout(self.cloud.callback_timeout()) {
            Ok(CloudEvent::List {
                prefix: returned_prefix,
                result,
            }) => {
                let _ = returned_prefix;
                match result {
                    CloudOutcome::Ok(keys) => Ok(keys),
                    CloudOutcome::Err(error) => Err(MidgeError::Internal(format!(
                        "cloud list '{prefix}': {error}"
                    ))),
                }
            }
            Ok(other) => Err(MidgeError::Internal(format!(
                "unexpected cloud list response for '{prefix}': {other:?}"
            ))),
            Err(error) => Err(MidgeError::Internal(format!(
                "cloud list '{prefix}' timed out or failed: {error}"
            ))),
        }
    }

    pub(in crate::engine) fn get_optional(&self, key: &str) -> MidgeResult<Option<Vec<u8>>> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.cloud.submit_get(key, tx);
        match rx.recv_timeout(self.cloud.callback_timeout()) {
            Ok(CloudEvent::Get { result, .. }) => match result {
                CloudOutcome::Ok(data) => Ok(Some(data)),
                CloudOutcome::Err(error) if crate::storage::cloud::is_not_found_error(&error) => {
                    Ok(None)
                }
                CloudOutcome::Err(error) => {
                    Err(MidgeError::Internal(format!("cloud get '{key}': {error}")))
                }
            },
            Ok(other) => Err(MidgeError::Internal(format!(
                "unexpected cloud get response for '{key}': {other:?}"
            ))),
            Err(error) => Err(MidgeError::Internal(format!(
                "cloud get '{key}' timed out or failed: {error}"
            ))),
        }
    }

    pub(in crate::engine) fn head_optional(
        &self,
        key: &str,
    ) -> MidgeResult<Option<ObjectMetadata>> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.cloud.submit_head(key, tx);
        match rx.recv_timeout(self.cloud.callback_timeout()) {
            Ok(CloudEvent::Head { result, .. }) => match result {
                CloudOutcome::Ok(metadata) => Ok(Some(metadata)),
                CloudOutcome::Err(error) if crate::storage::cloud::is_not_found_error(&error) => {
                    Ok(None)
                }
                CloudOutcome::Err(error) => {
                    Err(MidgeError::Internal(format!("cloud head '{key}': {error}")))
                }
            },
            Ok(other) => Err(MidgeError::Internal(format!(
                "unexpected cloud head response for '{key}': {other:?}"
            ))),
            Err(error) => Err(MidgeError::Internal(format!(
                "cloud head '{key}' timed out or failed: {error}"
            ))),
        }
    }

    pub(in crate::engine) fn put_with_headers(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
    ) -> MidgeResult<()> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.cloud.submit_put(key, data, headers, tx);
        match rx.recv_timeout(self.cloud.callback_timeout()) {
            Ok(CloudEvent::Put { result, .. }) => match result {
                CloudOutcome::Ok(()) => Ok(()),
                CloudOutcome::Err(error) => {
                    Err(MidgeError::Internal(format!("cloud put '{key}': {error}")))
                }
            },
            Ok(other) => Err(MidgeError::Internal(format!(
                "unexpected cloud put response for '{key}': {other:?}"
            ))),
            Err(error) => Err(MidgeError::Internal(format!(
                "cloud put '{key}' timed out or failed: {error}"
            ))),
        }
    }

    #[cfg(test)]
    pub(in crate::engine) fn get(&self, key: &str) -> MidgeResult<Vec<u8>> {
        self.get_optional(key)?.ok_or(MidgeError::NotFound)
    }

    #[cfg(test)]
    pub(in crate::engine) fn put(&self, key: &str, data: Vec<u8>) -> MidgeResult<()> {
        self.put_with_headers(key, data, Vec::new())
    }

    pub(super) fn object_proof_optional(
        &self,
        key: &str,
    ) -> MidgeResult<Option<crate::storage::cloud::CloudObjectProof>> {
        crate::storage::cloud::blocking_cloud_object_proof(self.cloud, key)
            .map_err(MidgeError::Internal)
    }
}
