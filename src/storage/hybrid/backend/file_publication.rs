//! Admitted immutable-file publication with identity-pinned, bounded readback.

use super::{Arc, HybridStorage, StorageEvent, StorageObjectMetadata, StorageOutcome};
use crate::common::resource_budget::{ResourceBudget, ResourceReservation};
use crate::common::{MidgeError, MidgeResult, OperationDeadline};
use crate::io::{Fs, FsError, FsPath, OpenMode, OpenOptions};
use std::sync::mpsc;

const READBACK_BYTES: usize = 64 * 1024;
const COPY_FACTOR: usize = 4;
const FIXED_WORKSPACE: usize = 256 * 1024;

/// A finished immutable file, read through the filesystem that wrote it.
#[derive(Clone, Copy)]
pub(crate) struct ImmutableFileSource<'a> {
    pub(crate) key: &'a str,
    pub(crate) fs: &'a dyn Fs,
    pub(crate) path: &'a FsPath,
    pub(crate) size: u64,
    pub(crate) checksum: u32,
}

struct PublicationAdmission {
    memory: Arc<ResourceReservation>,
    deadline: OperationDeadline,
}

impl PublicationAdmission {
    fn timeout(&self, storage: &HybridStorage, key: &str) -> MidgeResult<std::time::Duration> {
        HybridStorage::deadline_timeout(
            key,
            "immutable file publication",
            storage.callback_timeout,
            &self.deadline,
        )
    }
}

impl HybridStorage {
    /// Leave three quarters of the variable workspace for live merge readers,
    /// the current writer, and one indivisible key. Publication temporarily
    /// needs four copies of the encoded partition, so using one quarter of the
    /// remaining pool prevents a valid streaming compaction from repeatedly
    /// failing when its input cursors retain more than half of the shared
    /// maintenance budget.
    pub(crate) fn immutable_file_partition_target(pool: usize) -> usize {
        (pool.saturating_sub(FIXED_WORKSPACE) / (COPY_FACTOR * 4)).max(1)
    }

    pub(crate) fn publish_immutable_file(
        &self,
        source: ImmutableFileSource<'_>,
        budget: &ResourceBudget,
    ) -> MidgeResult<super::GuardedObjectProof> {
        self.publish_immutable_file_within(source, budget, &OperationDeadline::unbounded())
    }

    pub(crate) fn publish_immutable_file_within(
        &self,
        source: ImmutableFileSource<'_>,
        budget: &ResourceBudget,
        deadline: &OperationDeadline,
    ) -> MidgeResult<super::GuardedObjectProof> {
        let ImmutableFileSource {
            key,
            fs,
            path,
            size,
            checksum,
        } = source;
        // Retain the existing whole-file attempt cap without refreshing the
        // initiating manual obligation for a later output partition.
        let deadline = deadline.earlier(OperationDeadline::from_budget(self.callback_timeout));
        Self::deadline_timeout(
            key,
            "immutable file admission",
            self.callback_timeout,
            &deadline,
        )?;
        let length = usize::try_from(size).map_err(|_| {
            MidgeError::ResourceLimit("immutable upload size exceeds address space".into())
        })?;
        // Admit input plus provider copy workspace before reading the file.
        // Native retries share their transport body; the conservative copy
        // envelope also covers compatibility providers. Fixed space covers
        // range buffers; completion adapters charge their own stacks.
        let admission = PublicationAdmission {
            deadline,
            memory: Arc::new(
                budget.reserve(
                    length
                        .saturating_mul(COPY_FACTOR)
                        .saturating_add(FIXED_WORKSPACE),
                    "immutable file upload and readback",
                )?,
            ),
        };
        let source = fs
            .open(
                path,
                OpenOptions {
                    mode: OpenMode::ReadOnly,
                    create: false,
                    create_new: false,
                    truncate: false,
                },
            )
            .map_err(crate::sst::fs::engine_sst_error)?;
        if source.len().map_err(FsError::into_midge)? != size {
            return Err(MidgeError::Corruption(
                "immutable upload file size changed".into(),
            ));
        }
        let bytes = source.read_at(0, size).map_err(FsError::into_midge)?;
        // Re-read the length so a file that grew under the read is rejected.
        if bytes.len() != length
            || source.len().map_err(FsError::into_midge)? != size
            || crc32c::crc32c(&bytes) != checksum
        {
            return Err(MidgeError::Corruption(
                "immutable upload file identity changed".into(),
            ));
        }
        drop(source);
        admission.timeout(self, key)?;
        crate::failpoints::fail_point!("midge::cloud::inject_fail_sst_upload", |_| Err(
            MidgeError::Internal("failpoint: cloud SST upload failed".into())
        ));
        let metadata = self.publish_file_bytes(key, &bytes, &admission)?;
        Ok(super::GuardedObjectProof::range_identity(
            Arc::clone(&self.stores.sst),
            key.into(),
            metadata,
        ))
    }

    fn publish_file_bytes(
        &self,
        key: &str,
        bytes: &[u8],
        admission: &PublicationAdmission,
    ) -> MidgeResult<StorageObjectMetadata> {
        if let Some(metadata) = self.file_publication_head(key, admission)? {
            self.verify_publication_ranges(key, bytes, &metadata, admission)?;
            return Ok(metadata);
        }
        admission.timeout(self, key)?;
        let (tx, rx) = mpsc::channel();
        self.stores.sst.submit_write_request(
            crate::storage::StorageRequest::new(key, admission.deadline, self.callback_timeout)
                .with_precondition(crate::storage::StoragePrecondition::IfAbsent)
                .with_reservation(Arc::clone(&admission.memory)),
            bytes.to_vec(),
            tx,
        );
        let timeout = admission.timeout(self, key)?;
        match rx.recv_timeout(timeout) {
            Ok(StorageEvent::WriteComplete {
                result: StorageOutcome::Ok(()),
                ..
            }) => {}
            Ok(StorageEvent::WriteComplete {
                result: StorageOutcome::Err(error),
                ..
            }) if !Self::storage_error_indicates_timeout(&error) => {
                // A conflicting or ambiguous response is accepted only after
                // every existing byte matches through one pinned identity.
            }
            Ok(StorageEvent::WriteComplete {
                result: StorageOutcome::Err(error),
                ..
            }) => {
                return Err(MidgeError::Timeout(error.to_string()));
            }
            Ok(event) => {
                return Err(MidgeError::Internal(format!(
                    "immutable upload did not complete: {event:?}"
                )))
            }
            Err(error) => {
                return Err(MidgeError::Timeout(format!(
                    "immutable upload callback: {error}"
                )))
            }
        }
        let metadata = self
            .file_publication_head(key, admission)?
            .ok_or_else(|| MidgeError::Corruption("uploaded immutable object is absent".into()))?;
        self.verify_publication_ranges(key, bytes, &metadata, admission)?;
        Ok(metadata)
    }

    fn file_publication_head(
        &self,
        key: &str,
        admission: &PublicationAdmission,
    ) -> MidgeResult<Option<StorageObjectMetadata>> {
        admission.timeout(self, key)?;
        let (tx, rx) = mpsc::channel();
        self.stores.sst.submit_range_head_request(
            crate::storage::StorageRequest::new(key, admission.deadline, self.callback_timeout),
            tx,
        );
        let timeout = admission.timeout(self, key)?;
        match rx.recv_timeout(timeout) {
            Ok(StorageEvent::HeadComplete {
                key: actual,
                result: StorageOutcome::Ok(metadata),
            }) if actual == key => {
                admission.timeout(self, key)?;
                Ok(Some(metadata))
            }
            Ok(StorageEvent::HeadComplete {
                key: actual,
                result: StorageOutcome::Err(error),
            }) if actual == key && Self::storage_error_indicates_missing(&error) => {
                admission.timeout(self, key)?;
                Ok(None)
            }
            Ok(StorageEvent::HeadComplete {
                key: actual,
                result: StorageOutcome::Err(error),
            }) if actual == key && error.is_timeout() => Err(MidgeError::Timeout(format!(
                "immutable file identity lookup: {error}"
            ))),
            Ok(event) => Err(MidgeError::Internal(format!(
                "immutable file identity lookup failed: {event:?}"
            ))),
            Err(error) => Err(MidgeError::Timeout(format!(
                "immutable file identity lookup: {error}"
            ))),
        }
    }

    fn verify_publication_ranges(
        &self,
        key: &str,
        bytes: &[u8],
        metadata: &StorageObjectMetadata,
        admission: &PublicationAdmission,
    ) -> MidgeResult<()> {
        if metadata.size != bytes.len() as u64 || !metadata.same_version(metadata) {
            return Err(MidgeError::Corruption(
                "immutable object has conflicting size or no pinned identity".into(),
            ));
        }
        for (index, expected) in bytes.chunks(READBACK_BYTES).enumerate() {
            let start = (index * READBACK_BYTES) as u64;
            let end = start + expected.len() as u64;
            admission.timeout(self, key)?;
            let (tx, rx) = mpsc::channel();
            self.stores.sst.submit_range_read_request(
                crate::storage::StorageRequest::new(key, admission.deadline, self.callback_timeout)
                    .with_precondition(crate::storage::StoragePrecondition::IfMatch(
                        metadata.clone(),
                    ))
                    .with_reservation(Arc::clone(&admission.memory)),
                start..end,
                tx,
            );
            let timeout = admission.timeout(self, key)?;
            let actual = rx
                .recv_timeout(timeout)
                .map_err(|error| MidgeError::Timeout(format!("immutable range readback: {error}")))?
                .map_err(|error| {
                    if error.is_timeout() {
                        MidgeError::Timeout(format!("immutable range readback: {error}"))
                    } else {
                        MidgeError::Internal(error.to_string())
                    }
                })?;
            admission.timeout(self, key)?;
            if actual != expected {
                return Err(MidgeError::Corruption(
                    "immutable object readback differs from publication".into(),
                ));
            }
        }
        let current = self.file_publication_head(key, admission)?.ok_or_else(|| {
            MidgeError::Corruption("immutable object disappeared during readback".into())
        })?;
        if !metadata.same_version(&current) {
            return Err(MidgeError::Corruption(
                "immutable object changed during readback".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "file_publication_tests.rs"]
mod tests;
