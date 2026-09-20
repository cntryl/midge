use crate::common::{MidgeError, MidgeResult, OperationDeadline};
use crate::storage::cloud::{BlockingCloud, CloudStorage, ObjectMetadata};

/// Synchronous startup adapter over the runtime's callback-oriented cloud API.
///
/// Recovery policy remains in `CloudStartupRecovery`; the callback protocol,
/// timeout handling, and response-shape validation belong to the shared
/// `BlockingCloud` adapter this type delegates to.
pub(in crate::engine) struct BlockingCloudIo<'a> {
    cloud: &'a CloudStorage,
    deadline: OperationDeadline,
}

impl<'a> BlockingCloudIo<'a> {
    pub(in crate::engine) fn new(cloud: &'a CloudStorage) -> Self {
        Self {
            cloud,
            deadline: OperationDeadline::unbounded(),
        }
    }

    fn io(&self) -> BlockingCloud<'_> {
        BlockingCloud::new(self.cloud, &self.deadline)
    }

    pub(in crate::engine) fn list(&self, prefix: &str) -> MidgeResult<Vec<String>> {
        self.io().list(prefix)
    }

    pub(in crate::engine) fn get_optional(&self, key: &str) -> MidgeResult<Option<Vec<u8>>> {
        self.io().get_optional(key)
    }

    pub(in crate::engine) fn head_optional(
        &self,
        key: &str,
    ) -> MidgeResult<Option<ObjectMetadata>> {
        self.io().head_optional(key)
    }

    pub(in crate::engine) fn put_with_headers(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
    ) -> MidgeResult<()> {
        self.io().put_with_headers(key, data, headers)
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
