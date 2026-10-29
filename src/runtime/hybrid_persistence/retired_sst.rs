//! Physical deletion consumes the original retired identity, never a new HEAD.

use super::CloudPersistence;
use crate::common::{MidgeError, MidgeResult, OperationDeadline};
use crate::metadata::FileMeta;
use crate::storage::{StorageObjectMetadata, StoragePrecondition, StorageRequest};

const READ_CHUNK: u64 = 256 * 1024;

#[cfg(test)]
mod tests;

impl CloudPersistence {
    pub(crate) fn delete_retired_sst_object_within(
        &self,
        file: &FileMeta,
        deadline: &OperationDeadline,
    ) -> MidgeResult<()> {
        crate::failpoints::fail_point!("midge::cloud::inject_fail_sst_delete", |_| Err(
            MidgeError::Internal("failpoint: cloud SST delete failed".to_string())
        ));
        let expected = file.content_crc32c.ok_or_else(|| {
            MidgeError::Corruption(format!(
                "retired SST '{}' lacks its original checksum; retaining it",
                file.name
            ))
        })?;
        crate::cloud_layout::PersistedSstName::parse(&file.name)?;
        let key = crate::cloud_layout::object_key(&file.name);
        let Some(metadata) = self.remote_range_metadata_optional_within(&key, deadline)? else {
            return Ok(());
        };
        if metadata.size != file.size_bytes {
            return Err(MidgeError::Corruption(format!(
                "retired SST '{}' has a different remote length; retaining replacement",
                file.name
            )));
        }
        if self.retired_sst_crc(&key, &metadata, deadline)? != expected {
            return Err(MidgeError::Corruption(format!(
                "retired SST '{}' has different remote bytes; retaining replacement",
                file.name
            )));
        }
        self.delete_remote_object_by_identity_blocking_within(&key, &metadata, deadline)
    }

    fn retired_sst_crc(
        &self,
        key: &str,
        metadata: &StorageObjectMetadata,
        deadline: &OperationDeadline,
    ) -> MidgeResult<u32> {
        let backend = self.remote_sst_backend();
        let mut offset = 0;
        let mut checksum = 0;
        while offset < metadata.size {
            let end = metadata.size.min(offset.saturating_add(READ_CHUNK));
            let timeout = deadline
                .clamp_nonzero(self.storage_io_timeout())
                .ok_or_else(|| {
                    MidgeError::Timeout("retired SST checksum budget exhausted".into())
                })?;
            let request = StorageRequest::new(key, *deadline, timeout)
                .with_precondition(StoragePrecondition::IfMatch(metadata.clone()));
            let (tx, rx) = std::sync::mpsc::channel();
            backend.submit_range_read_request(request, offset..end, tx);
            let bytes = rx
                .recv_timeout(timeout)
                .map_err(|error| MidgeError::Timeout(format!("retired SST range read: {error}")))?
                .map_err(|error| {
                    MidgeError::Internal(format!("retired SST range read: {error}"))
                })?;
            if bytes.len() as u64 != end - offset {
                return Err(MidgeError::Corruption(
                    "retired SST range length differs from original identity".into(),
                ));
            }
            checksum = crc32c::crc32c_append(checksum, &bytes);
            offset = end;
        }
        Ok(checksum)
    }
}
