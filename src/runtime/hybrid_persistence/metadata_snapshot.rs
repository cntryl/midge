//! Admitted metadata snapshots for resumable WAL cleanup.

use super::{
    Arc, CloudMetadataPruneGuard, CloudStorage, CloudWalPruneProgress, GuardedObjectProof,
    HybridStorage, Manifest, MidgeError, MidgeResult, StorageBackend,
};
use crate::io::FsError;
use std::io::Read as _;

#[derive(Clone)]
pub(crate) struct CloudMetadataPruneSnapshot {
    cloud: Arc<CloudStorage>,
    db_path: std::path::PathBuf,
    fs: Arc<dyn crate::io::traits::Fs>,
    metadata_publication_lock: crate::runtime::MetadataPublicationLock,
    budget: crate::common::resource_budget::ResourceBudget,
    progress: CloudWalPruneProgress,
    authority: Option<(Arc<dyn crate::lease::LeaderStore>, String, u64)>,
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
            authority: None,
        }
    }

    pub(crate) fn with_authority(
        mut self,
        store: Arc<dyn crate::lease::LeaderStore>,
        holder_id: String,
        writer_epoch: u64,
    ) -> Self {
        self.authority = Some((store, holder_id, writer_epoch));
        self
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
        let (objects, generation) = self.verify_objects(deadline).inspect_err(|_| {
            // A larger replacement object may not fit beside retained admission.
            // Release it on deferral so the next attempt can admit fresh metadata.
            self.progress.discard_idle_proofs();
        })?;

        let authority = self.authority.as_ref().zip(generation).map(
            |((store, holder_id, writer_epoch), generation)| super::CloudMetadataAuthorityProof {
                store: Arc::clone(store),
                holder_id: holder_id.clone(),
                writer_epoch: *writer_epoch,
                generation,
            },
        );
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
                    authority,
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
            authority,
        };
        operation(Arc::new(manifest), guard)
    }
    fn verify_objects(
        &self,
        deadline: &crate::common::OperationDeadline,
    ) -> MidgeResult<(
        Vec<GuardedObjectProof>,
        Option<crate::lease::CloudMetadataGeneration>,
    )> {
        if let Some((store, holder_id, writer_epoch)) = &self.authority {
            return self
                .verify_committed_objects(deadline, store.as_ref(), holder_id, *writer_epoch)
                .map(|(objects, generation)| (objects, Some(generation)));
        }
        if !cfg!(test) {
            return Err(MidgeError::Fenced(
                "cloud WAL cleanup has no committed metadata authority".into(),
            ));
        }
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

        Ok((objects, None))
    }

    fn verify_committed_objects(
        &self,
        deadline: &crate::common::OperationDeadline,
        store: &dyn crate::lease::LeaderStore,
        holder_id: &str,
        writer_epoch: u64,
    ) -> MidgeResult<(
        Vec<GuardedObjectProof>,
        crate::lease::CloudMetadataGeneration,
    )> {
        store
            .validate_epoch_with_timeout(holder_id, writer_epoch, deadline.remaining())
            .map_err(|error| error.into_validation_error("cloud metadata cleanup lease check"))?;
        let crate::lease::CloudMetadataHead::Committed(generation) = store
            .read_committed_metadata(deadline.remaining())
            .map_err(|error| error.into_validation_error("cloud metadata cleanup pointer"))?
        else {
            return Err(MidgeError::Corruption(
                "cloud WAL cleanup has no committed metadata generation".into(),
            ));
        };
        let backend: Arc<dyn StorageBackend> = self.cloud.clone();
        let mut objects = Vec::with_capacity(generation.objects.len());
        let mut has_manifest_base = false;
        for file_name in crate::metadata::files::CLOUD_MIRRORED {
            let local_path = self.db_path.join(file_name);
            let local = match std::fs::File::open(&local_path) {
                Ok(file) => Some(file),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(error.into()),
            };
            let committed = generation
                .objects
                .iter()
                .find(|object| object.file_name == *file_name);
            let (mut local, committed) = match (local, committed) {
                (None, None) => continue,
                (Some(local), Some(committed)) => (local, committed),
                _ => {
                    return Err(MidgeError::Corruption(format!(
                        "local metadata '{file_name}' differs from committed presence map"
                    )));
                }
            };
            let key = &committed.object_key;
            let proof = HybridStorage::read_control_from_backend(
                &backend,
                key,
                &self.budget,
                self.cloud.callback_timeout(),
                deadline,
            )?
            .ok_or_else(|| {
                MidgeError::Corruption(format!("committed cloud metadata '{key}' is missing"))
            })?;
            if u64::try_from(proof.bytes().len()).unwrap_or(u64::MAX) != committed.len
                || crc32c::crc32c(proof.bytes()) != committed.crc32c
            {
                return Err(MidgeError::Corruption(format!(
                    "committed cloud metadata '{key}' fails length or checksum proof"
                )));
            }
            let mut buffer = [0u8; 16 * 1024];
            for expected in proof.bytes().chunks(buffer.len()) {
                local.read_exact(&mut buffer[..expected.len()])?;
                if &buffer[..expected.len()] != expected {
                    return Err(MidgeError::Corruption(format!(
                        "local metadata '{file_name}' differs from committed generation"
                    )));
                }
            }
            if local.read(&mut buffer[..1])? != 0 {
                return Err(MidgeError::Corruption(format!(
                    "local metadata '{file_name}' has a different length"
                )));
            }
            if crate::metadata::files::is_manifest_body(file_name) {
                has_manifest_base = true;
            }
            objects.push(GuardedObjectProof::range_identity(
                Arc::clone(&backend),
                key.clone(),
                proof.metadata().clone(),
            ));
        }
        if !has_manifest_base {
            return Err(MidgeError::Corruption(
                "committed cloud metadata has no manifest snapshot for WAL cleanup".into(),
            ));
        }
        Ok((objects, generation))
    }
}

/// Commit the local control files as one immutable, lease-pointed generation.
///
/// The lease object's compare-exchange is the publication boundary. Staged
/// objects that lose that CAS are harmless orphans and cannot change recovery.
#[derive(Clone, Copy)]
pub(crate) struct CloudMetadataMirrorAuthority<'a> {
    pub(crate) store: &'a dyn crate::lease::LeaderStore,
    pub(crate) holder_id: &'a str,
    pub(crate) writer_epoch: u64,
}

#[derive(Clone, Copy)]
pub(crate) struct CloudMetadataMirrorContext<'a> {
    pub(crate) cloud: &'a CloudStorage,
    pub(crate) fs: &'a dyn crate::io::traits::Fs,
    pub(crate) publication_lock: &'a crate::runtime::MetadataPublicationLock,
    pub(crate) lock_wait_budget: std::time::Duration,
    pub(crate) local_manifest_sequence: u64,
    pub(crate) deadline: &'a crate::common::OperationDeadline,
    pub(crate) authority: CloudMetadataMirrorAuthority<'a>,
}

pub(crate) fn mirror_control_metadata_within(
    context: CloudMetadataMirrorContext<'_>,
    mut validate_lease: impl FnMut(&crate::common::OperationDeadline) -> MidgeResult<()>,
) -> MidgeResult<()> {
    let CloudMetadataMirrorContext {
        cloud,
        fs: _,
        publication_lock,
        lock_wait_budget,
        local_manifest_sequence,
        deadline,
        authority:
            CloudMetadataMirrorAuthority {
                store: leader_store,
                holder_id,
                writer_epoch,
            },
    } = context;
    let lock_timeout = if lock_wait_budget.is_zero() {
        if deadline.is_expired() {
            return Err(MidgeError::Timeout(
                "metadata mirror deadline exhausted before publication lock".into(),
            ));
        }
        std::time::Duration::ZERO
    } else {
        deadline
            .clamp_nonzero(lock_wait_budget.min(cloud.callback_timeout()))
            .ok_or_else(|| {
                MidgeError::Timeout(
                    "metadata mirror deadline exhausted before publication lock".into(),
                )
            })?
    };
    let _publication_guard = publication_lock.lock_for(lock_timeout).ok_or_else(|| {
        MidgeError::Timeout("metadata mirror timed out acquiring publication lock".into())
    })?;

    validate_lease(deadline)?;
    let previous = match leader_store
        .read_committed_metadata(deadline.remaining())
        .map_err(|error| error.into_validation_error("cloud metadata pointer read"))?
    {
        crate::lease::CloudMetadataHead::Committed(generation) => {
            if generation.manifest_sequence > local_manifest_sequence {
                return Err(MidgeError::Fenced(format!(
                    "cloud metadata generation sequence {} is ahead of local sequence {local_manifest_sequence}",
                    generation.manifest_sequence
                )));
            }
            Some(generation)
        }
        crate::lease::CloudMetadataHead::Uncommitted => None,
        crate::lease::CloudMetadataHead::MissingLease => {
            return Err(MidgeError::Fenced(
                "cloud metadata lease disappeared before publication".into(),
            ));
        }
    };
    let objects = stage_control_metadata_objects(&context, previous.as_ref(), &mut validate_lease)?;
    let generation = crate::lease::CloudMetadataGeneration {
        manifest_sequence: local_manifest_sequence,
        objects,
    };
    if previous.as_ref() == Some(&generation) {
        return Ok(());
    }
    validate_lease(deadline)?;
    leader_store
        .publish_committed_metadata(
            holder_id,
            writer_epoch,
            previous.as_ref(),
            generation,
            deadline.remaining(),
        )
        .map_err(|error| error.into_validation_error("cloud metadata generation publication"))?;
    Ok(())
}

fn stage_control_metadata_objects(
    context: &CloudMetadataMirrorContext<'_>,
    previous: Option<&crate::lease::CloudMetadataGeneration>,
    validate_lease: &mut impl FnMut(&crate::common::OperationDeadline) -> MidgeResult<()>,
) -> MidgeResult<Vec<crate::lease::CloudMetadataObject>> {
    let generation_id = uuid::Uuid::new_v4();
    let mut objects = Vec::with_capacity(crate::metadata::files::CLOUD_MIRRORED.len());
    for file_name in crate::metadata::files::CLOUD_MIRRORED {
        let old_object = previous.and_then(|generation| {
            generation
                .objects
                .iter()
                .find(|entry| entry.file_name == *file_name)
        });
        if let Some(object) = stage_control_metadata_object(
            context,
            generation_id,
            file_name,
            old_object,
            validate_lease,
        )? {
            objects.push(object);
        }
    }
    for required in [
        crate::metadata::files::FORMAT,
        crate::metadata::files::MANIFEST_SNAPSHOT,
    ] {
        if !objects.iter().any(|object| object.file_name == required) {
            return Err(MidgeError::Corruption(format!(
                "cannot commit cloud metadata without '{required}'"
            )));
        }
    }
    Ok(objects)
}

fn stage_control_metadata_object(
    context: &CloudMetadataMirrorContext<'_>,
    generation_id: uuid::Uuid,
    file_name: &str,
    old_object: Option<&crate::lease::CloudMetadataObject>,
    validate_lease: &mut impl FnMut(&crate::common::OperationDeadline) -> MidgeResult<()>,
) -> MidgeResult<Option<crate::lease::CloudMetadataObject>> {
    if context.deadline.is_expired() {
        return Err(MidgeError::Timeout(format!(
            "operation deadline exhausted before cloud metadata local mirror preparation for '{file_name}'"
        )));
    }
    let io = crate::storage::cloud::BlockingCloud::new(context.cloud, context.deadline);
    let old_bytes = old_object
        .map(|object| {
            let bytes = io.get_optional(&object.object_key)?.ok_or_else(|| {
                MidgeError::Corruption(format!(
                    "committed cloud metadata '{}' is missing",
                    object.object_key
                ))
            })?;
            if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != object.len
                || crc32c::crc32c(&bytes) != object.crc32c
            {
                return Err(MidgeError::Corruption(format!(
                    "committed cloud metadata '{}' fails length or checksum proof",
                    object.object_key
                )));
            }
            if crate::metadata::files::is_manifest_body(file_name) {
                crate::metadata::files::ensure_remote_not_ahead(
                    file_name,
                    &bytes,
                    context.local_manifest_sequence,
                )?;
            }
            Ok(bytes)
        })
        .transpose()?;
    let path = crate::io::FsPath::new(file_name);
    if !context.fs.exists(&path).map_err(FsError::into_midge)? {
        return Ok(None);
    }
    let file = context
        .fs
        .open(
            &path,
            crate::io::OpenOptions {
                mode: crate::io::OpenMode::ReadOnly,
                create: false,
                create_new: false,
                truncate: false,
            },
        )
        .map_err(FsError::into_midge)?;
    let data = file
        .read_at(0, file.len().map_err(FsError::into_midge)?)
        .map_err(FsError::into_midge)?
        .to_vec();
    if let (Some(old_object), Some(old_bytes)) = (old_object, old_bytes.as_ref()) {
        if old_bytes == &data {
            return Ok(Some(old_object.clone()));
        }
    }
    drop(old_bytes);
    let object_key = format!("metadata/generations/{generation_id}/{file_name}");
    let object = crate::lease::CloudMetadataObject {
        file_name: file_name.to_string(),
        object_key: object_key.clone(),
        len: u64::try_from(data.len()).unwrap_or(u64::MAX),
        crc32c: crc32c::crc32c(&data),
    };
    validate_lease(context.deadline)?;
    io.put_with_precondition(
        &object_key,
        data,
        &crate::storage::StoragePrecondition::IfAbsent,
    )?;
    let readback = io.get_optional(&object_key)?.ok_or_else(|| {
        MidgeError::Corruption(format!(
            "new cloud metadata '{object_key}' is missing after upload"
        ))
    })?;
    if u64::try_from(readback.len()).unwrap_or(u64::MAX) != object.len
        || crc32c::crc32c(&readback) != object.crc32c
    {
        return Err(MidgeError::Corruption(format!(
            "new cloud metadata '{object_key}' failed readback"
        )));
    }
    for (index, chunk) in readback.chunks(64 * 1024).enumerate() {
        let offset = u64::try_from(index.saturating_mul(64 * 1024))
            .map_err(|_| MidgeError::ResourceLimit("metadata readback offset overflow".into()))?;
        let local = file
            .read_at(offset, u64::try_from(chunk.len()).unwrap_or(u64::MAX))
            .map_err(FsError::into_midge)?;
        if local.as_ref() != chunk {
            return Err(MidgeError::Corruption(format!(
                "new cloud metadata '{object_key}' differs from local readback"
            )));
        }
    }
    Ok(Some(object))
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

    #[test]
    fn should_check_remote_manifest_before_writing_any_mirror_file() {
        use crate::lease::{CloudLeaseConfig, CloudStorageLease, PrimaryLease};

        // Arrange
        let directory = tempfile::tempdir().unwrap();
        let remote = Manifest {
            last_persisted_sequence: 10,
            ..Manifest::default()
        };
        crate::metadata::ensure_or_create_format_marker(directory.path())
            .expect("create local format marker");
        crate::metadata::ManifestPersistence::save(directory.path(), &remote).unwrap();
        let backend = Arc::new(crate::storage::cloud::MockCloudBackend::new());
        let cloud = Arc::new(CloudStorage::new(backend.clone(), String::new()));
        let lease = Arc::new(CloudStorageLease::new_provider_backed(
            CloudLeaseConfig {
                bucket: "test".into(),
                prefix: String::new(),
            },
            directory.path().to_path_buf(),
            Arc::clone(&cloud),
        ));
        let _guard = Arc::clone(&lease)
            .try_acquire()
            .expect("acquire cloud lease");
        let store = lease.get_leader_store().expect("provider leader store");
        let fs: Arc<dyn crate::io::traits::Fs> =
            Arc::new(crate::io::real::RealFs::new(directory.path()).unwrap());
        let deadline =
            crate::common::OperationDeadline::from_budget(std::time::Duration::from_secs(5));
        let publication_lock = crate::runtime::MetadataPublicationLock::default();
        mirror_control_metadata_within(
            CloudMetadataMirrorContext {
                cloud: &cloud,
                fs: fs.as_ref(),
                publication_lock: &publication_lock,
                lock_wait_budget: std::time::Duration::from_secs(5),
                local_manifest_sequence: 10,
                deadline: &deadline,
                authority: CloudMetadataMirrorAuthority {
                    store: store.as_ref(),
                    holder_id: &lease.holder_id(),
                    writer_epoch: lease.epoch(),
                },
            },
            |_| Ok(()),
        )
        .expect("commit remote generation");
        let prior_uploads = backend.get_uploads().len();
        let local = Manifest {
            last_persisted_sequence: 5,
            ..Manifest::default()
        };
        crate::metadata::ManifestPersistence::save(directory.path(), &local).unwrap();

        // Act
        let error = mirror_control_metadata_within(
            CloudMetadataMirrorContext {
                cloud: &cloud,
                fs: fs.as_ref(),
                publication_lock: &publication_lock,
                lock_wait_budget: std::time::Duration::from_secs(5),
                local_manifest_sequence: local.last_persisted_sequence,
                deadline: &crate::common::OperationDeadline::from_budget(
                    std::time::Duration::from_secs(5),
                ),
                authority: CloudMetadataMirrorAuthority {
                    store: store.as_ref(),
                    holder_id: &lease.holder_id(),
                    writer_epoch: lease.epoch(),
                },
            },
            |_| Ok(()),
        )
        .expect_err("remote-ahead metadata must fence the mirror before any write");

        // Assert
        assert!(matches!(error, MidgeError::Fenced(_)));
        assert_eq!(
            backend.get_uploads().len(),
            prior_uploads,
            "remote-ahead rejection must not stage another immutable object"
        );
    }

    #[test]
    fn should_not_commit_metadata_generation_without_required_format() {
        use crate::lease::{CloudLeaseConfig, CloudMetadataHead, CloudStorageLease, PrimaryLease};

        // Arrange
        let directory = tempfile::tempdir().expect("local cloud cache");
        crate::metadata::ensure_or_create_format_marker(directory.path())
            .expect("create local format marker");
        crate::metadata::ManifestPersistence::save(directory.path(), &Manifest::default())
            .expect("save local manifest");
        std::fs::remove_file(directory.path().join(crate::metadata::files::FORMAT))
            .expect("remove required format");
        let cloud = Arc::new(CloudStorage::new(
            Arc::new(crate::storage::cloud::MockCloudBackend::new()),
            String::new(),
        ));
        let lease = Arc::new(CloudStorageLease::new_provider_backed(
            CloudLeaseConfig {
                bucket: "test".into(),
                prefix: String::new(),
            },
            directory.path().to_path_buf(),
            Arc::clone(&cloud),
        ));
        let _guard = Arc::clone(&lease)
            .try_acquire()
            .expect("acquire cloud lease");
        let store = lease.get_leader_store().expect("provider leader store");

        // Act
        let result = mirror_control_metadata_within(
            CloudMetadataMirrorContext {
                cloud: &cloud,
                fs: &crate::io::RealFs::new(directory.path()).expect("local fs"),
                publication_lock: &crate::runtime::MetadataPublicationLock::default(),
                lock_wait_budget: std::time::Duration::from_secs(5),
                local_manifest_sequence: 0,
                deadline: &crate::common::OperationDeadline::from_budget(
                    std::time::Duration::from_secs(5),
                ),
                authority: CloudMetadataMirrorAuthority {
                    store: store.as_ref(),
                    holder_id: &lease.holder_id(),
                    writer_epoch: lease.epoch(),
                },
            },
            |_| Ok(()),
        );

        // Assert
        assert!(matches!(result, Err(MidgeError::Corruption(_))));
        assert!(matches!(
            store
                .read_committed_metadata(std::time::Duration::from_secs(5))
                .expect("read pointer after failed publication"),
            CloudMetadataHead::Uncommitted
        ));
    }

    #[test]
    fn should_report_timeout_when_mirror_exceeds_operation_deadline() {
        use crate::lease::{CloudLeaseConfig, CloudStorageLease, PrimaryLease};

        // Arrange
        let directory = tempfile::tempdir().expect("local cloud cache");
        crate::metadata::ensure_or_create_format_marker(directory.path())
            .expect("create local format marker");
        crate::metadata::ManifestPersistence::save(directory.path(), &Manifest::default())
            .expect("save local manifest");
        let cloud = Arc::new(CloudStorage::new(
            Arc::new(crate::storage::cloud::MockCloudBackend::new()),
            String::new(),
        ));
        let lease = Arc::new(CloudStorageLease::new_provider_backed(
            CloudLeaseConfig {
                bucket: "test".into(),
                prefix: String::new(),
            },
            directory.path().to_path_buf(),
            Arc::clone(&cloud),
        ));
        let _guard = Arc::clone(&lease)
            .try_acquire()
            .expect("acquire cloud lease");
        let store = lease.get_leader_store().expect("provider leader store");
        let fs = crate::io::RealFs::new(directory.path()).expect("local fs");
        let expired =
            crate::common::OperationDeadline::from_budget(std::time::Duration::from_nanos(1));
        std::thread::sleep(std::time::Duration::from_millis(5));

        // Act
        let error = mirror_control_metadata_within(
            CloudMetadataMirrorContext {
                cloud: &cloud,
                fs: &fs,
                publication_lock: &crate::runtime::MetadataPublicationLock::default(),
                lock_wait_budget: std::time::Duration::from_secs(5),
                local_manifest_sequence: 0,
                deadline: &expired,
                authority: CloudMetadataMirrorAuthority {
                    store: store.as_ref(),
                    holder_id: &lease.holder_id(),
                    writer_epoch: lease.epoch(),
                },
            },
            |_| Ok(()),
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

    struct PausingMetadataPutBackend {
        inner: crate::storage::cloud::MockCloudBackend,
        pause: std::sync::atomic::AtomicBool,
        entered: std::sync::Barrier,
        resume: std::sync::Barrier,
    }

    impl PausingMetadataPutBackend {
        fn new() -> Self {
            Self {
                inner: crate::storage::cloud::MockCloudBackend::new(),
                pause: std::sync::atomic::AtomicBool::new(false),
                entered: std::sync::Barrier::new(2),
                resume: std::sync::Barrier::new(2),
            }
        }
    }

    impl crate::storage::cloud::CloudBackend for PausingMetadataPutBackend {
        fn submit_put(
            &self,
            key: &str,
            data: Vec<u8>,
            headers: Vec<(String, String)>,
            callback: crate::storage::cloud::CloudCallback,
        ) {
            if key.ends_with(crate::cloud_layout::CloudObjectLayout::LEASE_OBJECT_KEY)
                && self.pause.swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                self.entered.wait();
                self.resume.wait();
            }
            self.inner.submit_put(key, data, headers, callback);
        }

        crate::storage::cloud::forward_cloud_backend!(inner; submit_get, submit_get_with_metadata, submit_get_range, submit_get_range_with_identity, submit_delete, submit_list, submit_head);
    }

    fn commit_baseline_metadata(
        cloud: &CloudStorage,
        directory: &std::path::Path,
        store: &dyn crate::lease::LeaderStore,
        holder_id: &str,
        epoch: u64,
    ) -> (Manifest, crate::lease::CloudMetadataHead) {
        let original = Manifest {
            last_persisted_sequence: 5,
            ..Manifest::default()
        };
        crate::metadata::ManifestPersistence::save(directory, &original)
            .expect("save baseline local manifest");
        let fs = crate::io::RealFs::new(directory).expect("baseline local fs");
        let deadline =
            crate::common::OperationDeadline::from_budget(std::time::Duration::from_secs(5));
        mirror_control_metadata_within(
            CloudMetadataMirrorContext {
                cloud,
                fs: &fs,
                publication_lock: &crate::runtime::MetadataPublicationLock::default(),
                lock_wait_budget: std::time::Duration::from_secs(5),
                local_manifest_sequence: 5,
                deadline: &deadline,
                authority: CloudMetadataMirrorAuthority {
                    store,
                    holder_id,
                    writer_epoch: epoch,
                },
            },
            |_| Ok(()),
        )
        .expect("commit baseline generation");
        let baseline = store
            .read_committed_metadata(std::time::Duration::from_secs(5))
            .expect("read committed baseline");
        assert!(matches!(
            baseline,
            crate::lease::CloudMetadataHead::Committed(_)
        ));
        (original, baseline)
    }

    fn spawn_paused_metadata_publisher(
        cloud: Arc<CloudStorage>,
        directory: std::path::PathBuf,
        store: Arc<dyn crate::lease::LeaderStore>,
        holder_id: String,
        epoch: u64,
    ) -> std::thread::JoinHandle<MidgeResult<()>> {
        std::thread::spawn(move || {
            let fs = crate::io::RealFs::new(&directory).expect("first writer local fs");
            mirror_control_metadata_within(
                CloudMetadataMirrorContext {
                    cloud: &cloud,
                    fs: &fs,
                    publication_lock: &crate::runtime::MetadataPublicationLock::default(),
                    lock_wait_budget: std::time::Duration::from_secs(5),
                    local_manifest_sequence: 10,
                    deadline: &crate::common::OperationDeadline::from_budget(
                        std::time::Duration::from_secs(5),
                    ),
                    authority: CloudMetadataMirrorAuthority {
                        store: store.as_ref(),
                        holder_id: &holder_id,
                        writer_epoch: epoch,
                    },
                },
                |deadline| {
                    store
                        .validate_epoch_with_timeout(&holder_id, epoch, deadline.remaining())
                        .map_err(|error| MidgeError::Fenced(error.to_string()))
                },
            )
        })
    }

    fn assert_pointer_and_epoch(
        store: &dyn crate::lease::LeaderStore,
        expected: &crate::lease::CloudMetadataHead,
        epoch: u64,
    ) {
        assert_eq!(
            store
                .read_committed_metadata(std::time::Duration::from_secs(5))
                .expect("read committed metadata pointer"),
            expected.clone()
        );
        assert_eq!(
            store
                .read_current()
                .expect("read current lease")
                .expect("lease held")
                .epoch,
            epoch
        );
    }

    #[test]
    fn should_reject_metadata_mirror_when_lease_changes_after_head_before_put() {
        use crate::lease::{CloudLeaseConfig, CloudStorageLease, PrimaryLease};
        use std::sync::atomic::Ordering;
        use std::time::Duration;

        // Arrange: two provider-backed writers share one lease and metadata store.
        let directory = tempfile::tempdir().expect("cache directory");
        crate::metadata::ensure_or_create_format_marker(directory.path())
            .expect("create local format marker");
        let backend = Arc::new(PausingMetadataPutBackend::new());
        let cloud = Arc::new(CloudStorage::new(backend.clone(), String::new()));
        let lease = || {
            Arc::new(
                CloudStorageLease::new_provider_backed_with_clock_skew_tolerance_and_ttl(
                    CloudLeaseConfig {
                        bucket: "test".into(),
                        prefix: String::new(),
                    },
                    directory.path().to_path_buf(),
                    Arc::clone(&cloud),
                    Duration::ZERO,
                    Duration::from_secs(30),
                ),
            )
        };
        let first = lease();
        let _first_guard = Arc::clone(&first)
            .try_acquire()
            .expect("first writer acquires lease");
        let first_epoch = first.epoch();
        let first_holder = first.holder_id();
        let first_store = first.get_leader_store().expect("provider leader store");
        let (original, baseline) = commit_baseline_metadata(
            &cloud,
            directory.path(),
            first_store.as_ref(),
            &first_holder,
            first_epoch,
        );
        let newer = Manifest {
            last_persisted_sequence: 10,
            ..Manifest::default()
        };
        crate::metadata::ManifestPersistence::save(directory.path(), &newer)
            .expect("save first writer's local manifest");

        backend.pause.store(true, Ordering::SeqCst);
        let publisher = spawn_paused_metadata_publisher(
            Arc::clone(&cloud),
            directory.path().to_path_buf(),
            Arc::clone(&first_store),
            first_holder,
            first_epoch,
        );
        backend.entered.wait();
        // The second writer inherits the committed pointer after takeover.
        first
            .release()
            .expect("release first lease after metadata HEAD");
        let second = lease();
        let _second_guard = Arc::clone(&second)
            .try_acquire()
            .expect("second writer takes over");
        assert!(second.epoch() > first_epoch);
        let second_store = second.get_leader_store().expect("second provider store");
        assert_pointer_and_epoch(second_store.as_ref(), &baseline, second.epoch());

        // Act: the first writer's lease-object CAS cannot pass after takeover.
        backend.resume.wait();
        let result = publisher.join().expect("publisher thread");

        // Assert: the old writer cannot change committed metadata after takeover.
        assert!(
            matches!(result, Err(MidgeError::Fenced(_))),
            "stale metadata publication must be fenced: {result:?}"
        );
        assert_pointer_and_epoch(second_store.as_ref(), &baseline, second.epoch());
        crate::metadata::ManifestPersistence::save(directory.path(), &original)
            .expect("restore new holder's baseline local metadata");
        let first_fs = crate::io::RealFs::new(directory.path()).expect("baseline local fs");
        let second_result = mirror_control_metadata_within(
            CloudMetadataMirrorContext {
                cloud: &cloud,
                fs: &first_fs,
                publication_lock: &crate::runtime::MetadataPublicationLock::default(),
                lock_wait_budget: Duration::from_secs(5),
                local_manifest_sequence: 5,
                deadline: &crate::common::OperationDeadline::from_budget(Duration::from_secs(5)),
                authority: CloudMetadataMirrorAuthority {
                    store: second_store.as_ref(),
                    holder_id: &second.holder_id(),
                    writer_epoch: second.epoch(),
                },
            },
            |_| Ok(()),
        );
        assert!(
            second_result.is_ok(),
            "new holder can publish: {second_result:?}"
        );
    }
}
