//! Provider-confirmed writes, with a conservative fallback for other backends.

use super::{StorageBackend, StorageError, StorageEvent, StorageOutcome, StorageRequest};

/// Identity of the written version and CRC32C validated by the provider.
/// An `ETag` alone is an identity token, not an integrity checksum.
#[derive(Clone, Debug)]
pub struct StorageWriteReceipt {
    pub metadata: super::StorageObjectMetadata,
    pub content_crc32c: u32,
}

impl StorageWriteReceipt {
    pub(crate) fn matches(&self, bytes: &[u8]) -> bool {
        self.metadata.size == bytes.len() as u64
            && super::conditional_object_identity(
                &self.metadata.etag,
                self.metadata.generation.as_deref(),
            )
            .is_some()
            && self.content_crc32c == crc32c::crc32c(bytes)
    }
}

pub type WriteReceiptCallback =
    std::sync::mpsc::Sender<Result<Option<StorageWriteReceipt>, StorageError>>;

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
