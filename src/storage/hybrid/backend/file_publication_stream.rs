//! Bounded publication of a finished immutable file through provider sessions.

use super::*;
use crate::storage::{StorageFileUpload, StoragePrecondition, StorageRequest, StorageWriteReceipt};

impl HybridStorage {
    pub(super) fn publish_streaming_file(
        &self,
        source: ImmutableFileSource<'_>,
        budget: &ResourceBudget,
        deadline: OperationDeadline,
    ) -> MidgeResult<super::super::GuardedObjectProof> {
        let memory = crate::storage::FILE_UPLOAD_PART_BYTES
            .saturating_mul(self.publication_copies())
            .saturating_add(FIXED_WORKSPACE);
        let admission = PublicationAdmission {
            memory: Arc::new(budget.reserve(memory, "streaming immutable upload parts")?),
            deadline,
        };
        let file: Arc<dyn crate::io::File> = Arc::from(
            crate::io::open_persistent_handle(source.fs, source.path, OpenOptions::READ_ONLY)
                .map_err(crate::sst::fs::engine_sst_error)?,
        );
        if file.len().map_err(FsError::into_midge)? != source.size {
            return Err(MidgeError::Corruption(
                "immutable upload file size changed".into(),
            ));
        }
        crate::failpoints::fail_point!("midge::cloud::inject_fail_sst_upload", |_| Err(
            MidgeError::Internal("failpoint: cloud SST upload failed".into())
        ));
        let metadata = self.publish_file_parts(source, &file, &admission)?;
        Ok(super::super::GuardedObjectProof::range_identity(
            Arc::clone(&self.stores.sst),
            source.key.into(),
            metadata,
        ))
    }

    fn publish_file_parts(
        &self,
        source: ImmutableFileSource<'_>,
        file: &Arc<dyn crate::io::File>,
        admission: &PublicationAdmission,
    ) -> MidgeResult<StorageObjectMetadata> {
        for attempt in 0..=self.immutable_retry.max_retries {
            if let Some(metadata) = self.file_publication_head(source.key, admission)? {
                self.verify_publication_file_ranges(source, file, &metadata, admission)?;
                return Ok(metadata);
            }
            let outcome = self.put_file_parts(source, file, admission);
            match outcome {
                Ok(Some(receipt)) => {
                    if !receipt.matches_identity(source.size, source.checksum) {
                        return Err(MidgeError::Corruption(
                            "streaming upload receipt differs from finalized file".into(),
                        ));
                    }
                    return Ok(receipt.metadata);
                }
                Err(error) if error.is_timeout() => {
                    return Err(MidgeError::Timeout(error.to_string()))
                }
                Err(error)
                    if error.kind() != crate::storage::StorageErrorKind::Transport
                        && !Self::storage_error_indicates_precondition_failure(&error) =>
                {
                    return Err(MidgeError::Internal(error.to_string()))
                }
                outcome => {
                    if let Some(metadata) = self.file_publication_head(source.key, admission)? {
                        self.verify_publication_file_ranges(source, file, &metadata, admission)?;
                        return Ok(metadata);
                    }
                    let Err(error) = outcome else {
                        return Err(MidgeError::Corruption(
                            "completed streaming object is absent".into(),
                        ));
                    };
                    if error.kind() != crate::storage::StorageErrorKind::Transport
                        || attempt == self.immutable_retry.max_retries
                    {
                        return Err(MidgeError::Internal(error.to_string()));
                    }
                    let remaining = admission.timeout(self, source.key)?;
                    std::thread::sleep(self.immutable_retry.delay(attempt).min(remaining));
                }
            }
        }
        unreachable!("the final streaming publication attempt returns its outcome")
    }

    fn put_file_parts(
        &self,
        source: ImmutableFileSource<'_>,
        file: &Arc<dyn crate::io::File>,
        admission: &PublicationAdmission,
    ) -> Result<Option<StorageWriteReceipt>, crate::storage::StorageError> {
        admission.timeout(self, source.key)?;
        let (tx, rx) = mpsc::channel();
        self.stores.sst.submit_file_write_receipt_request(
            StorageRequest::new(source.key, admission.deadline, self.callback_timeout)
                .with_precondition(StoragePrecondition::IfAbsent)
                .with_reservation(Arc::clone(&admission.memory)),
            StorageFileUpload {
                file: Arc::clone(file),
                size: source.size,
                content_crc32c: source.checksum,
                part_size: crate::storage::FILE_UPLOAD_PART_BYTES,
            },
            tx,
        );
        let result = rx
            .recv_timeout(admission.timeout(self, source.key)?)
            .map_err(|error| {
                crate::storage::storage_timeout_error(format!("streaming upload callback: {error}"))
            })?;
        admission.timeout(self, source.key)?;
        result
    }

    fn verify_publication_file_ranges(
        &self,
        source: ImmutableFileSource<'_>,
        file: &Arc<dyn crate::io::File>,
        metadata: &StorageObjectMetadata,
        admission: &PublicationAdmission,
    ) -> MidgeResult<()> {
        if metadata.size != source.size || !metadata.same_version(metadata) {
            return Err(MidgeError::Corruption(
                "streamed object size or identity differs".into(),
            ));
        }
        let mut offset = 0;
        let mut checksum = 0;
        while offset < source.size {
            admission.timeout(self, source.key)?;
            let end = offset
                .saturating_add(READBACK_BYTES as u64)
                .min(source.size);
            let expected = file
                .read_at(offset, end - offset)
                .map_err(FsError::into_midge)?;
            if expected.len() as u64 != end - offset {
                return Err(MidgeError::Corruption(
                    "immutable file changed during streaming verification".into(),
                ));
            }
            checksum = crc32c::crc32c_append(checksum, &expected);
            let (tx, rx) = mpsc::channel();
            self.stores.sst.submit_range_read_request(
                StorageRequest::new(source.key, admission.deadline, self.callback_timeout)
                    .with_precondition(StoragePrecondition::IfMatch(metadata.clone()))
                    .with_reservation(Arc::clone(&admission.memory)),
                offset..end,
                tx,
            );
            let actual = rx
                .recv_timeout(admission.timeout(self, source.key)?)
                .map_err(|error| {
                    MidgeError::Timeout(format!("streamed object range verification: {error}"))
                })?
                .map_err(|error| {
                    if error.is_timeout() {
                        MidgeError::Timeout(error.to_string())
                    } else {
                        MidgeError::Corruption(format!(
                            "streamed object range verification failed: {error}"
                        ))
                    }
                })?;
            if actual != expected {
                return Err(MidgeError::Corruption(
                    "streamed object differs from finalized file".into(),
                ));
            }
            offset = end;
        }
        admission.timeout(self, source.key)?;
        if checksum != source.checksum || file.len().map_err(FsError::into_midge)? != source.size {
            return Err(MidgeError::Corruption(
                "immutable file identity changed during streaming verification".into(),
            ));
        }
        let current = self
            .file_publication_head(source.key, admission)?
            .ok_or_else(|| {
                MidgeError::Corruption("streamed object disappeared during verification".into())
            })?;
        if !metadata.same_version(&current) {
            return Err(MidgeError::Corruption(
                "streamed object identity changed during verification".into(),
            ));
        }
        Ok(())
    }
}
