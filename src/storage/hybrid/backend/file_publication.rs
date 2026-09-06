//! Admitted immutable-file publication with identity-pinned, bounded readback.

use super::{
    Arc, HybridStorage, StorageBackend, StorageEvent, StorageObjectMetadata, StorageOutcome,
};
use crate::common::resource_budget::{ResourceBudget, ResourceReservation};
use crate::common::{MidgeError, MidgeResult};
use std::io::Read;
use std::path::Path;
use std::sync::mpsc;

const READBACK_BYTES: usize = 64 * 1024;

impl HybridStorage {
    pub(crate) fn publish_immutable_file(
        &self,
        key: &str,
        path: &Path,
        size: u64,
        checksum: u32,
        budget: &ResourceBudget,
    ) -> MidgeResult<()> {
        let length = usize::try_from(size).map_err(|_| {
            MidgeError::ResourceLimit("immutable upload size exceeds address space".into())
        })?;
        // Admit input plus provider copy workspace before reading the file.
        // Native retries share their transport body; the conservative copy
        // envelope also covers compatibility providers. Fixed space covers
        // range buffers; completion adapters charge their own stacks.
        let reservation = Arc::new(budget.reserve(
            length.saturating_mul(4).saturating_add(256 * 1024),
            "immutable file upload and readback",
        )?);
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
        let local = self.file_publication_head(&self.local, key)?;
        if let Some(metadata) = &local {
            self.verify_publication_ranges(&self.local, key, &bytes, metadata, &reservation)?;
        }
        crate::failpoints::fail_point!("midge::cloud::inject_fail_sst_upload", |_| Err(
            MidgeError::Internal("failpoint: cloud SST upload failed".into())
        ));
        self.publish_file_bytes(&self.cloud, key, &bytes, &reservation)?;
        if local.is_none() && !self.ephemeral_sst_cache_enabled() {
            self.publish_file_bytes(&self.local, key, &bytes, &reservation)?;
        }
        Ok(())
    }

    fn publish_file_bytes(
        &self,
        backend: &Arc<dyn StorageBackend>,
        key: &str,
        bytes: &[u8],
        reservation: &Arc<ResourceReservation>,
    ) -> MidgeResult<()> {
        if let Some(metadata) = self.file_publication_head(backend, key)? {
            return self.verify_publication_ranges(backend, key, bytes, &metadata, reservation);
        }
        let (tx, rx) = mpsc::channel();
        backend.submit_write_with_reservation(
            key,
            bytes.to_vec(),
            vec![("If-None-Match".into(), "*".into())],
            self.callback_timeout,
            Arc::clone(reservation),
            tx,
        );
        match rx.recv_timeout(self.callback_timeout) {
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
            .file_publication_head(backend, key)?
            .ok_or_else(|| MidgeError::Corruption("uploaded immutable object is absent".into()))?;
        self.verify_publication_ranges(backend, key, bytes, &metadata, reservation)
    }

    fn file_publication_head(
        &self,
        backend: &Arc<dyn StorageBackend>,
        key: &str,
    ) -> MidgeResult<Option<StorageObjectMetadata>> {
        let (tx, rx) = mpsc::channel();
        backend.submit_range_head(key, self.callback_timeout, tx);
        match rx.recv_timeout(self.callback_timeout) {
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
        reservation: &Arc<ResourceReservation>,
    ) -> MidgeResult<()> {
        if metadata.size != bytes.len() as u64 || !metadata.same_version(metadata) {
            return Err(MidgeError::Corruption(
                "immutable object has conflicting size or no pinned identity".into(),
            ));
        }
        for (index, expected) in bytes.chunks(READBACK_BYTES).enumerate() {
            let start = (index * READBACK_BYTES) as u64;
            let end = start + expected.len() as u64;
            let (tx, rx) = mpsc::channel();
            backend.submit_read_range_with_reservation(
                key,
                start..end,
                metadata.clone(),
                self.callback_timeout,
                Arc::clone(reservation),
                tx,
            );
            let actual = rx
                .recv_timeout(self.callback_timeout)
                .map_err(|error| MidgeError::Timeout(format!("immutable range readback: {error}")))?
                .map_err(MidgeError::Internal)?;
            if actual != expected {
                return Err(MidgeError::Corruption(
                    "immutable object readback differs from publication".into(),
                ));
            }
        }
        let current = self.file_publication_head(backend, key)?.ok_or_else(|| {
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
