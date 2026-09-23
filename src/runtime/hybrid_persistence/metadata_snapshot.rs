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
    metadata_publication_lock: crate::runtime::MetadataPublicationLock,
    budget: crate::common::resource_budget::ResourceBudget,
    progress: CloudWalPruneProgress,
}

impl CloudMetadataPruneSnapshot {
    pub(crate) fn new(
        cloud: Arc<CloudStorage>,
        db_path: std::path::PathBuf,
        fs: Arc<dyn crate::io::traits::Fs>,
        budget: crate::common::resource_budget::ResourceBudget,
        metadata_publication_lock: crate::runtime::MetadataPublicationLock,
    ) -> Self {
        Self {
            cloud,
            db_path,
            fs,
            budget,
            metadata_publication_lock,
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
            .metadata_publication_lock
            .lock_for(lock_timeout)
            .ok_or_else(|| {
                MidgeError::Timeout("metadata proof timed out acquiring publication lock".into())
            })?;

        // Admit retained manifest, journal replay, and decoding scratch before
        // loading either local metadata or remote bodies. The publication lock
        // keeps these local files stable through authority retirement.
        let mut encoded_bytes = 0usize;
        for name in crate::metadata::files::CLOUD_MIRRORED {
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
            crate::config::RecoveryPolicy::Strict,
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
        let mut objects = Vec::with_capacity(crate::metadata::files::CLOUD_MIRRORED.len());
        let mut has_manifest_base = false;
        for file_name in crate::metadata::files::CLOUD_MIRRORED {
            let local_path = self.db_path.join(file_name);
            let local = match std::fs::File::open(&local_path) {
                Ok(file) => Some(file),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(error.into()),
            };
            let key = crate::cloud_layout::CloudObjectLayout::metadata_key(file_name);
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
            if crate::metadata::files::is_manifest_body(file_name) {
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

/// Publish one local metadata body to the authoritative cloud mirror.
///
/// Every mirror writer (the event loop, the flush worker, startup recovery)
/// goes through this one function so the protocol and its verdicts stay
/// identical: a remote body whose manifest sequence is ahead of the local one
/// is `Fenced`, a cloud round trip that outlives the deadline is `Timeout`,
/// and the write itself is conditional on the identity the preceding HEAD saw.
pub(crate) fn conditional_metadata_mirror_put(
    cloud: &CloudStorage,
    file_name: &str,
    data: Vec<u8>,
    local_manifest_sequence: u64,
    deadline: &crate::common::OperationDeadline,
) -> MidgeResult<()> {
    let io = crate::storage::cloud::BlockingCloud::new(cloud, deadline);
    let key = crate::cloud_layout::CloudObjectLayout::metadata_key(file_name);
    let headers = match io.head_optional(&key)? {
        Some(metadata) => {
            let headers = crate::storage::cloud::object_match_precondition_headers(
                &metadata.etag,
                metadata.generation.as_deref(),
            )
            .ok_or_else(|| {
                MidgeError::Internal(format!(
                    "cloud metadata '{key}' cannot be conditionally updated without an identity token"
                ))
            })?;
            let current = io.get_optional(&key)?.ok_or_else(|| {
                MidgeError::Internal(format!(
                    "cloud metadata '{key}' disappeared after HEAD precondition"
                ))
            })?;
            crate::metadata::files::ensure_remote_not_ahead(
                file_name,
                &current,
                local_manifest_sequence,
            )?;
            if current == data {
                return Ok(());
            }
            headers
        }
        None => vec![("If-None-Match".to_string(), "*".to_string())],
    };
    io.put_with_headers(&key, data, headers)
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
        let encoded: usize = crate::metadata::files::CLOUD_MIRRORED
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
            crate::common::resource_budget::ResourceBudget::new(charge + 128 * 1024),
            crate::runtime::MetadataPublicationLock::default(),
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

    /// Wait for the shared budget to drain.
    ///
    /// The reservation is released when the last handle drops, which can
    /// trail the call that returned it, so sampling immediately makes the
    /// assertion depend on timing.
    fn assert_budget_drains(snapshot: &CloudMetadataPruneSnapshot) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while snapshot.budget.used() != 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "metadata proof retained {} bytes after completing",
                snapshot.budget.used()
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }

    fn cloud_with_remote_manifest(sequence: u64) -> CloudStorage {
        let cloud = CloudStorage::new(
            Arc::new(crate::storage::cloud::MockCloudBackend::new()),
            String::new(),
        );
        let body = serde_json::to_vec(&Manifest {
            last_persisted_sequence: sequence,
            ..Manifest::default()
        })
        .unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_put(
            &crate::cloud_layout::CloudObjectLayout::metadata_key(crate::metadata::files::MANIFEST),
            body,
            Vec::new(),
            tx,
        );
        assert!(matches!(
            rx.recv().unwrap(),
            crate::storage::cloud::CloudEvent::Put { result: Ok(()), .. }
        ));
        cloud
    }

    /// Every mirror writer shares this function, so the flush worker, the
    /// event loop and startup recovery all fence on a remote manifest that is
    /// ahead of the local one instead of disagreeing about the verdict.
    #[test]
    fn should_report_fenced_when_remote_manifest_is_ahead_of_local() {
        // Arrange
        let cloud = cloud_with_remote_manifest(10);
        let local = serde_json::to_vec(&Manifest {
            last_persisted_sequence: 5,
            ..Manifest::default()
        })
        .unwrap();

        // Act
        let error = conditional_metadata_mirror_put(
            &cloud,
            crate::metadata::files::MANIFEST,
            local,
            5,
            &crate::common::OperationDeadline::unbounded(),
        )
        .unwrap_err();

        // Assert
        assert!(
            matches!(error, MidgeError::Fenced(_)),
            "remote-ahead mirror must fence, got {error:?}"
        );
    }

    #[test]
    fn should_report_timeout_when_mirror_exceeds_operation_deadline() {
        // Arrange
        let cloud = cloud_with_remote_manifest(1);
        let expired =
            crate::common::OperationDeadline::from_budget(std::time::Duration::from_nanos(1));
        std::thread::sleep(std::time::Duration::from_millis(5));

        // Act
        let error = conditional_metadata_mirror_put(
            &cloud,
            crate::metadata::files::MANIFEST,
            Vec::new(),
            9,
            &expired,
        )
        .unwrap_err();

        // Assert
        assert!(
            matches!(error, MidgeError::Timeout(_)),
            "an exhausted deadline must report Timeout, got {error:?}"
        );
    }

    fn mirror(snapshot: &CloudMetadataPruneSnapshot) {
        for name in crate::metadata::files::CLOUD_MIRRORED {
            if let Ok(bytes) = std::fs::read(snapshot.db_path.join(name)) {
                let (tx, rx) = std::sync::mpsc::channel();
                snapshot.cloud.submit_put(
                    &crate::cloud_layout::CloudObjectLayout::metadata_key(name),
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
    fn should_serialize_prune_proofs_across_separately_constructed_dispatchers() {
        // Arrange
        let directory = tempfile::tempdir().unwrap();
        let manifest = Manifest::default();
        crate::metadata::ManifestPersistence::save(directory.path(), &manifest).unwrap();
        let lock = crate::runtime::MetadataPublicationLock::default();
        let fs: Arc<dyn crate::io::traits::Fs> =
            Arc::new(crate::io::real::RealFs::new(directory.path()).unwrap());
        let first = CloudMetadataPruneSnapshot::new(
            Arc::new(CloudStorage::new(
                Arc::new(crate::storage::cloud::MockCloudBackend::new()),
                "first-control-dispatcher".into(),
            )),
            directory.path().to_path_buf(),
            Arc::clone(&fs),
            crate::common::resource_budget::ResourceBudget::new(1024 * 1024),
            lock.clone(),
        );
        let second = CloudMetadataPruneSnapshot::new(
            Arc::new(CloudStorage::new(
                Arc::new(crate::storage::cloud::MockCloudBackend::new()),
                "second-control-dispatcher".into(),
            )),
            directory.path().to_path_buf(),
            fs,
            crate::common::resource_budget::ResourceBudget::new(1024 * 1024),
            lock,
        );
        mirror(&first);
        mirror(&second);
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let first_worker = std::thread::spawn(move || {
            first.verify_exact_then(&crate::common::OperationDeadline::unbounded(), |_, _| {
                entered_tx.send(()).expect("signal held publication lock");
                release_rx.recv().expect("release publication lock");
                Ok(())
            })
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("first dispatcher holds publication lock");
        let deadline =
            crate::common::OperationDeadline::from_budget(std::time::Duration::from_millis(50));

        // Act
        let result: MidgeResult<()> = second.verify_exact_then(&deadline, |_, _| {
            panic!("second dispatcher must not publish while the first owns the runtime lock")
        });
        release_tx.send(()).expect("release first dispatcher");

        // Assert
        assert!(matches!(result, Err(MidgeError::Timeout(_))));
        first_worker
            .join()
            .expect("join first dispatcher")
            .expect("first proof completes after release");
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
        assert_budget_drains(&snapshot);
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
        assert_budget_drains(&snapshot);
    }
}
