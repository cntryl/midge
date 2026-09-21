//! Verified object reads: bytes plus the identity they were read under.

#[allow(clippy::wildcard_imports)]
use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CloudObjectProof {
    pub bytes: Vec<u8>,
    pub metadata: StorageObjectMetadata,
}

pub(crate) fn storage_object_metadata(metadata: ObjectMetadata) -> StorageObjectMetadata {
    StorageObjectMetadata {
        size: metadata.size,
        etag: metadata.etag,
        generation: metadata.generation,
    }
}

/// Validate observations from one metadata-bearing read. Never pair a body
/// with identity from a separate HEAD, even when both lengths agree.
pub(crate) fn validate_object_proof(
    key: &str,
    bytes: &[u8],
    metadata: &StorageObjectMetadata,
) -> crate::common::MidgeResult<()> {
    if metadata.size != u64::try_from(bytes.len()).unwrap_or(u64::MAX) {
        return Err(crate::common::MidgeError::Internal(format!(
            "cloud object '{key}' length mismatch: read={}, metadata={}",
            bytes.len(),
            metadata.size
        )));
    }
    if crate::storage::conditional_object_identity(&metadata.etag, metadata.generation.as_deref())
        .is_none()
    {
        return Err(crate::common::MidgeError::Internal(format!(
            "cloud object '{key}' is missing an identity token"
        )));
    }
    Ok(())
}

pub(crate) fn blocking_cloud_object_proof(
    cloud: &CloudStorage,
    key: &str,
) -> Result<Option<CloudObjectProof>, String> {
    blocking_cloud_object_proof_within(cloud, key, &crate::common::OperationDeadline::unbounded())
        .map_err(|error| error.to_string())
}

pub(crate) fn blocking_cloud_object_proof_within(
    cloud: &CloudStorage,
    key: &str,
    deadline: &crate::common::OperationDeadline,
) -> crate::common::MidgeResult<Option<CloudObjectProof>> {
    let get_timeout = deadline
        .clamp_nonzero(cloud.callback_timeout())
        .ok_or_else(|| {
            crate::common::MidgeError::Timeout(format!(
                "operation deadline exhausted before cloud object GET for '{key}'"
            ))
        })?;
    let (get_tx, get_rx) = std::sync::mpsc::channel();
    cloud.submit_get_with_metadata(key, get_tx);
    let (bytes, metadata) = match get_rx.recv_timeout(get_timeout) {
        Ok(CloudEvent::GetWithMetadata {
            result: CloudOutcome::Ok((bytes, metadata)),
            ..
        }) => (bytes, storage_object_metadata(metadata)),
        Ok(CloudEvent::GetWithMetadata {
            result: CloudOutcome::Err(error),
            ..
        }) if is_not_found_error(&error) => return Ok(None),
        Ok(CloudEvent::GetWithMetadata {
            result: CloudOutcome::Err(error),
            ..
        }) => {
            return Err(contextualize_operation_error(
                &error,
                format_args!("cloud object '{key}' is unreadable"),
                deadline,
            ))
        }
        Ok(other) => {
            return Err(crate::common::MidgeError::Internal(format!(
                "unexpected cloud object GET response for '{key}': {other:?}"
            )))
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            return Err(crate::common::MidgeError::Timeout(format!(
                "cloud object GET exceeded the operation deadline for '{key}'"
            )))
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            return Err(crate::common::MidgeError::Internal(format!(
                "cloud object GET callback closed for '{key}'"
            )))
        }
    };

    validate_object_proof(key, &bytes, &metadata)?;

    Ok(Some(CloudObjectProof { bytes, metadata }))
}
