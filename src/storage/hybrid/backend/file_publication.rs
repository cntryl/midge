//! Admitted immutable-file publication with identity-pinned, bounded readback.

use super::{
    Arc, HybridStorage, StorageBackend, StorageEvent, StorageObjectMetadata, StorageOutcome,
};
use crate::common::resource_budget::{ResourceBudget, ResourceReservation};
use crate::common::{MidgeError, MidgeResult, OperationDeadline};
use std::io::Read;
use std::path::Path;
use std::sync::mpsc;

const READBACK_BYTES: usize = 64 * 1024;
const COPY_FACTOR: usize = 4;
const FIXED_WORKSPACE: usize = 256 * 1024;

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
    /// Leave half of the variable workspace for live merge readers and one
    /// indivisible key. Actual publication admission remains authoritative.
    pub(crate) fn immutable_file_partition_target(pool: usize) -> usize {
        (pool.saturating_sub(FIXED_WORKSPACE) / (COPY_FACTOR * 2)).max(1)
    }

    pub(crate) fn publish_immutable_file(
        &self,
        key: &str,
        path: &Path,
        size: u64,
        checksum: u32,
        budget: &ResourceBudget,
    ) -> MidgeResult<super::GuardedObjectProof> {
        let deadline = OperationDeadline::from_budget(self.callback_timeout);
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
        let mut source = std::fs::File::open(path)?;
        if source.metadata()?.len() != size {
            return Err(MidgeError::Corruption(
                "immutable upload file size changed".into(),
            ));
        }
        let mut bytes = vec![0; length];
        source.read_exact(&mut bytes)?;
        if source.read(&mut [0])? != 0 || crc32c::crc32c(&bytes) != checksum {
            return Err(MidgeError::Corruption(
                "immutable upload file identity changed".into(),
            ));
        }
        drop(source);
        let local = self.file_publication_head(&self.local, key, &admission)?;
        if let Some(metadata) = &local {
            self.verify_publication_ranges(&self.local, key, &bytes, metadata, &admission)?;
        }
        crate::failpoints::fail_point!("midge::cloud::inject_fail_sst_upload", |_| Err(
            MidgeError::Internal("failpoint: cloud SST upload failed".into())
        ));
        let metadata = self.publish_file_bytes(&self.cloud, key, &bytes, &admission)?;
        if local.is_none() && !self.ephemeral_sst_cache_enabled() {
            self.publish_file_bytes(&self.local, key, &bytes, &admission)?;
        }
        Ok(super::GuardedObjectProof::range_identity(
            Arc::clone(&self.cloud),
            key.into(),
            metadata,
        ))
    }

    fn publish_file_bytes(
        &self,
        backend: &Arc<dyn StorageBackend>,
        key: &str,
        bytes: &[u8],
        admission: &PublicationAdmission,
    ) -> MidgeResult<StorageObjectMetadata> {
        if let Some(metadata) = self.file_publication_head(backend, key, admission)? {
            self.verify_publication_ranges(backend, key, bytes, &metadata, admission)?;
            return Ok(metadata);
        }
        let timeout = admission.timeout(self, key)?;
        let (tx, rx) = mpsc::channel();
        backend.submit_write_with_reservation(
            key,
            bytes.to_vec(),
            vec![("If-None-Match".into(), "*".into())],
            timeout,
            Arc::clone(&admission.memory),
            tx,
        );
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
                return Err(MidgeError::Timeout(error));
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
            .file_publication_head(backend, key, admission)?
            .ok_or_else(|| MidgeError::Corruption("uploaded immutable object is absent".into()))?;
        self.verify_publication_ranges(backend, key, bytes, &metadata, admission)?;
        Ok(metadata)
    }

    fn file_publication_head(
        &self,
        backend: &Arc<dyn StorageBackend>,
        key: &str,
        admission: &PublicationAdmission,
    ) -> MidgeResult<Option<StorageObjectMetadata>> {
        let timeout = admission.timeout(self, key)?;
        let (tx, rx) = mpsc::channel();
        backend.submit_range_head(key, timeout, tx);
        match rx.recv_timeout(timeout) {
            Ok(StorageEvent::HeadComplete {
                key: actual,
                result: StorageOutcome::Ok(metadata),
            }) if actual == key => Ok(Some(metadata)),
            Ok(StorageEvent::HeadComplete {
                key: actual,
                result: StorageOutcome::Err(error),
            }) if actual == key && Self::storage_error_indicates_missing(&error) => Ok(None),
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
        backend: &Arc<dyn StorageBackend>,
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
            let timeout = admission.timeout(self, key)?;
            let (tx, rx) = mpsc::channel();
            backend.submit_read_range_with_reservation(
                key,
                start..end,
                metadata.clone(),
                timeout,
                Arc::clone(&admission.memory),
                tx,
            );
            let actual = rx
                .recv_timeout(timeout)
                .map_err(|error| MidgeError::Timeout(format!("immutable range readback: {error}")))?
                .map_err(MidgeError::Internal)?;
            if actual != expected {
                return Err(MidgeError::Corruption(
                    "immutable object readback differs from publication".into(),
                ));
            }
        }
        let current = self
            .file_publication_head(backend, key, admission)?
            .ok_or_else(|| {
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
