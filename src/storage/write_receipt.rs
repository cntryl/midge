//! Provider-confirmed writes, with a conservative fallback for other backends.

use super::{StorageBackend, StorageError, StorageEvent, StorageOutcome, StorageRequest};

pub(crate) const FILE_UPLOAD_PART_BYTES: usize = 5 * 1024 * 1024;

/// Identity of the written version and CRC32C validated by the provider.
/// An `ETag` alone is an identity token, not an integrity checksum.
#[derive(Clone, Debug)]
pub struct StorageWriteReceipt {
    pub metadata: super::StorageObjectMetadata,
    pub content_crc32c: u32,
}

impl StorageWriteReceipt {
    pub(crate) fn matches(&self, bytes: &[u8]) -> bool {
        self.matches_identity(bytes.len() as u64, crc32c::crc32c(bytes))
    }

    pub(crate) fn matches_identity(&self, size: u64, checksum: u32) -> bool {
        self.metadata.size == size
            && super::conditional_object_identity(
                &self.metadata.etag,
                self.metadata.generation.as_deref(),
            )
            .is_some()
            && self.content_crc32c == checksum
    }
}

pub type WriteReceiptCallback =
    std::sync::mpsc::Sender<Result<Option<StorageWriteReceipt>, StorageError>>;

/// Owned immutable source for a bounded, conditional multipart upload.
#[cfg_attr(
    not(any(
        feature = "cloud-aws",
        feature = "cloud-azure",
        feature = "cloud-gcp",
        feature = "cloud-oci"
    )),
    allow(dead_code)
)]
pub struct StorageFileUpload {
    pub(crate) file: std::sync::Arc<dyn crate::io::File>,
    pub(crate) size: u64,
    pub(crate) content_crc32c: u32,
    pub(crate) part_size: usize,
}

pub(super) fn write_without_receipt(
    backend: &(impl StorageBackend + ?Sized),
    request: StorageRequest,
    data: Vec<u8>,
    callback: &WriteReceiptCallback,
) {
    let key = request.key.clone();
    let deadline = crate::common::OperationDeadline::from_budget(request.remaining_timeout());
    if deadline.is_expired() {
        let _ = callback.send(Err(super::storage_timeout_error(
            "write receipt deadline exhausted",
        )));
        return;
    }
    let (tx, rx) = std::sync::mpsc::channel();
    backend.submit_write_request(request, data, tx);
    let event = rx.recv_timeout(deadline.remaining());
    let result = match event {
        _ if deadline.is_expired() => Err(super::storage_timeout_error(
            "write receipt completion exceeded deadline",
        )),
        Ok(StorageEvent::WriteComplete {
            key: actual,
            result,
        }) if actual == key => match result {
            StorageOutcome::Ok(()) => Ok(None),
            StorageOutcome::Err(error) => Err(error),
        },
        Ok(event) => Err(StorageError::protocol(format!(
            "unexpected write completion: {event:?}"
        ))),
        Err(error) => Err(super::storage_timeout_error(error)),
    };
    let _ = callback.send(result);
}
