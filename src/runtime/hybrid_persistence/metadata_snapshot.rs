//! Admitted metadata snapshots for resumable WAL cleanup.

use super::{
    Arc, CloudMetadataPruneGuard, CloudStorage, CloudWalPruneProgress, GuardedObjectProof,
    HybridStorage, Manifest, MidgeError, MidgeResult, StorageBackend,
};
use std::io::Read as _;

#[derive(Clone)]
pub(crate) struct CloudMetadataPruneSnapshot {
    cloud: Arc<CloudStorage>,
    db_path: std::path::PathBuf,
    fs: Arc<dyn crate::io::traits::Fs>,
    recovery_policy: crate::config::RecoveryPolicy,
    budget: crate::common::resource_budget::ResourceBudget,
    progress: CloudWalPruneProgress,
}

impl CloudMetadataPruneSnapshot {
    pub(crate) fn new(
        cloud: Arc<CloudStorage>,
        db_path: std::path::PathBuf,
        fs: Arc<dyn crate::io::traits::Fs>,
        recovery_policy: crate::config::RecoveryPolicy,
        budget: crate::common::resource_budget::ResourceBudget,
    ) -> Self {
        Self {
            cloud,
            db_path,
            fs,
            recovery_policy,
            budget,
            progress: CloudWalPruneProgress::default(),
        }
    }

    pub(crate) fn with_progress(mut self, progress: CloudWalPruneProgress) -> Self {
        self.progress = progress;
        self
    }

    /// Verify an exact, read-only metadata snapshot and keep cloud metadata
    /// publication serialized through the authority-changing operation.
    ///
    /// Cleanup must never repair cloud metadata from a captured snapshot: an
    /// intent or DDL edit can change without advancing the manifest sequence,
    /// so writing stale bytes here could roll authoritative metadata backward.
    /// A mismatch is a safe cleanup deferral. Holding the publication lock
    /// through `operation` prevents a verified snapshot from changing before
    /// the WAL catalog compare-exchange retires recovery authority.
    pub(crate) fn verify_exact_then<T>(
        &self,
        deadline: &crate::common::OperationDeadline,
        operation: impl FnOnce(Arc<Manifest>, CloudMetadataPruneGuard) -> MidgeResult<T>,
    ) -> MidgeResult<T> {
        let lock_timeout = deadline
            .clamp_nonzero(self.cloud.callback_timeout())
            .ok_or_else(|| {
                MidgeError::Timeout(
                    "metadata proof deadline exhausted before publication lock".into(),
                )
            })?;
        let _publication_guard = self
            .cloud
            .lock_metadata_publication_for(lock_timeout)
            .ok_or_else(|| {
                MidgeError::Timeout("metadata proof timed out acquiring publication lock".into())
            })?;

        // Admit retained manifest, journal replay, and decoding scratch before
        // loading either local metadata or remote bodies. The publication lock
        // keeps these local files stable through authority retirement.
        let mut encoded_bytes = 0usize;
        for name in crate::storage::cloud::CLOUD_METADATA_FILES {
            match std::fs::metadata(self.db_path.join(name)) {
                Ok(metadata) => {
                    encoded_bytes = encoded_bytes
                        .saturating_add(usize::try_from(metadata.len()).unwrap_or(usize::MAX));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        let mut cached = self.progress.0.lock().snapshot(&self.budget);
        if cached
            .as_ref()
            .is_some_and(|guard| guard.metadata.is_none())
        {
            cached = None;
            self.progress.discard_idle_proofs();
        }
        let reserve = || {
            self.budget
                .reserve(
                    encoded_bytes.saturating_mul(16).saturating_add(4096),
                    "WAL cleanup metadata decoding",
                )
                .map(Arc::new)
        };
        let mut memory = match &cached {
            Some(guard) => Arc::clone(guard.manifest_memory.as_ref().expect("admitted snapshot")),
            None => reserve()?,
        };
        let objects = self.verify_objects(deadline).inspect_err(|_| {
            // A larger replacement object may not fit beside retained admission.
            // Release it on deferral so the next attempt can admit fresh metadata.
            self.progress.discard_idle_proofs();
        })?;

        let unchanged = cached.as_ref().is_some_and(|guard| {
            let previous = &guard.metadata.as_ref().expect("metadata snapshot").objects;
            previous.len() == objects.len()
                && previous
                    .iter()
                    .zip(&objects)
                    .all(|(old, new)| old.same_identity(new))
        });
        if unchanged {
            return operation(
                Arc::clone(&cached.as_ref().expect("unchanged snapshot").manifest),
                CloudMetadataPruneGuard {
                    objects,
                    memory: Some(memory),
                },
            );
        }
        if cached.take().is_some() {
            self.progress.discard_idle_proofs();
            drop(memory);
            memory = reserve()?;
        }
        let manifest = crate::metadata::ManifestPersistence::load_with_fs_and_policy(
            &self.fs,
            self.recovery_policy,
        )
        .map_err(MidgeError::Internal)?;
        let guard = CloudMetadataPruneGuard {
            objects,
            memory: Some(memory),
        };
        operation(Arc::new(manifest), guard)
    }
    fn verify_objects(
        &self,
        deadline: &crate::common::OperationDeadline,
    ) -> MidgeResult<Vec<GuardedObjectProof>> {
        let backend: Arc<dyn StorageBackend> = self.cloud.clone();
        let mut objects = Vec::with_capacity(crate::storage::cloud::CLOUD_METADATA_FILES.len());
        let mut has_manifest_base = false;
        for file_name in crate::storage::cloud::CLOUD_METADATA_FILES {
            let local_path = self.db_path.join(file_name);
            let local = match std::fs::File::open(&local_path) {
                Ok(file) => Some(file),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(error.into()),
            };
            let key = crate::storage::cloud::cloud_metadata_key(file_name);
            let proof = HybridStorage::read_control_from_backend(
                &backend,
                &key,
                &self.budget,
                self.cloud.callback_timeout(),
                deadline,
            )?;
            let (mut local, proof) = match (local, proof) {
                (Some(local), Some(proof)) => (local, proof),
                (None, None) => continue,
                (Some(_), None) => {
                    return Err(MidgeError::Corruption(format!(
                        "cloud metadata '{key}' is missing"
                    )))
                }
                (None, Some(_)) => {
                    return Err(MidgeError::Corruption(format!(
                        "local metadata for '{key}' is missing"
                    )))
                }
            };
            let mut buffer = [0u8; 16 * 1024];
            for expected in proof.bytes().chunks(buffer.len()) {
                local.read_exact(&mut buffer[..expected.len()])?;
                if &buffer[..expected.len()] != expected {
                    return Err(MidgeError::Corruption(format!(
                        "cloud metadata '{key}' does not match the captured committed metadata"
                    )));
                }
            }
            if local.read(&mut buffer[..1])? != 0 {
                return Err(MidgeError::Corruption(format!(
                    "cloud metadata '{key}' has a different length"
                )));
            }
            if matches!(*file_name, "manifest.snapshot.json" | "manifest.json") {
                has_manifest_base = true;
            }
            // Exact byte comparison has completed against identity-pinned reads.
            // Retain the identity, not another copy of every metadata file.
            objects.push(GuardedObjectProof::range_identity(
                Arc::clone(&backend),
                key,
                proof.metadata().clone(),
            ));
        }

        if !has_manifest_base {
            return Err(MidgeError::Internal(
                "no committed cloud manifest base is available to guard WAL cleanup".to_string(),
            ));
        }

        Ok(objects)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::hybrid_persistence::CloudWalPruneGuard;

    fn fixture() -> (tempfile::TempDir, CloudMetadataPruneSnapshot, Manifest) {
        let directory = tempfile::tempdir().unwrap();
        let manifest = Manifest {
            next_sst_seqs: (0..1000).map(|id| (id, 1)).collect(),
            ..Manifest::default()
        };
        crate::metadata::ManifestPersistence::save(directory.path(), &manifest).unwrap();
        let encoded: usize = crate::storage::cloud::CLOUD_METADATA_FILES
            .iter()
            .filter_map(|name| std::fs::metadata(directory.path().join(name)).ok())
            .map(|metadata| usize::try_from(metadata.len()).unwrap())
            .sum();
        let charge = encoded * 16 + 4096;
        let snapshot = CloudMetadataPruneSnapshot::new(
            Arc::new(CloudStorage::new(
                Arc::new(crate::storage::cloud::MockCloudBackend::new()),
                String::new(),
            )),
            directory.path().to_path_buf(),
            Arc::new(crate::io::real::RealFs::new(directory.path()).unwrap()),
            crate::config::RecoveryPolicy::default(),
            crate::common::resource_budget::ResourceBudget::new(charge + 128 * 1024),
        );
        mirror(&snapshot);
        snapshot
            .verify_exact_then(
                &crate::common::OperationDeadline::unbounded(),
                |manifest, metadata| {
                    let guard = CloudWalPruneGuard::new(manifest, Some(metadata));
                    snapshot.progress.0.lock().retain_snapshot(&guard);
                    Ok(())
                },
            )
            .unwrap();
        assert!(snapshot.budget.used() > snapshot.budget.limit() / 2);
        (directory, snapshot, manifest)
    }

    fn mirror(snapshot: &CloudMetadataPruneSnapshot) {
        for name in crate::storage::cloud::CLOUD_METADATA_FILES {
            if let Ok(bytes) = std::fs::read(snapshot.db_path.join(name)) {
                let (tx, rx) = std::sync::mpsc::channel();
                snapshot.cloud.submit_put(
                    &crate::storage::cloud::cloud_metadata_key(name),
                    bytes,
                    Vec::new(),
                    tx,
                );
                assert!(matches!(
                    rx.recv().unwrap(),
                    crate::storage::cloud::CloudEvent::Put { result: Ok(()), .. }
                ));
            }
        }
    }

    #[test]
    fn should_replace_retained_snapshot_when_verified_metadata_identity_changes() {
        // Arrange
        let (_directory, snapshot, mut manifest) = fixture();
        manifest.next_sst_seqs.insert(0, 2);
        crate::metadata::ManifestPersistence::save(&snapshot.db_path, &manifest).unwrap();
        mirror(&snapshot);

        // Act
        snapshot
            .verify_exact_then(
                &crate::common::OperationDeadline::unbounded(),
                |manifest, _metadata| {
                    // Assert
                    assert_eq!(manifest.next_sst_seqs[&0], 2);
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(snapshot.budget.used(), 0);
    }

    #[test]
    fn should_release_retained_snapshot_when_local_and_remote_metadata_diverge() {
        // Arrange
        let (_directory, snapshot, mut manifest) = fixture();
        manifest.next_sst_seqs.insert(0, 2);
        crate::metadata::ManifestPersistence::save(&snapshot.db_path, &manifest).unwrap();

        // Act
        let result = snapshot
            .verify_exact_then(&crate::common::OperationDeadline::unbounded(), |_, _| {
                panic!("divergent metadata cannot authorize retirement")
            });

        // Assert
        assert!(matches!(result, Err::<(), _>(MidgeError::Corruption(_))));
        assert_eq!(snapshot.budget.used(), 0);
    }
}
