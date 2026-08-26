//! Format-aware orchestration for hybrid persistence.
//!
//! `HybridStorage` deliberately knows only object keys, bytes, provider
//! identities, bounded callbacks, and conditional deletion. This runtime layer
//! owns WAL/SST decoding and manifest coverage decisions.

use crate::common::{MidgeError, MidgeResult};
use crate::metadata::{FileMeta, Manifest};
use crate::storage::cloud::CloudStorage;
use crate::storage::hybrid::backend::{GuardedObjectProof, HybridStorage, RemoteObjectProof};
use crate::storage::{StorageBackend, StorageObjectMetadata};
use crate::wal::cloud_catalog::{PublishedWalSegment, WalPublicationCatalog};
use crate::wal::cloud_segment::DataCoverageRecord;
use std::io::Write as _;
use std::path::Path;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub(crate) struct CloudMetadataPruneProof {
    pub(crate) key: String,
    pub(crate) expected_bytes: Vec<u8>,
    pub(crate) remote: StorageObjectMetadata,
}

#[derive(Clone)]
pub(crate) struct CloudMetadataPruneGuard {
    objects: Vec<GuardedObjectProof>,
}

impl CloudMetadataPruneGuard {
    pub(crate) fn new(cloud: Arc<CloudStorage>, objects: Vec<CloudMetadataPruneProof>) -> Self {
        let backend: Arc<dyn StorageBackend> = cloud;
        let objects = objects
            .into_iter()
            .map(|proof| {
                GuardedObjectProof::exact(
                    Arc::clone(&backend),
                    proof.key,
                    proof.expected_bytes,
                    proof.remote,
                )
            })
            .collect();
        Self { objects }
    }
}

#[derive(Clone)]
pub(crate) struct CloudMetadataPruneSnapshot {
    cloud: Arc<CloudStorage>,
    db_path: std::path::PathBuf,
    fs: Arc<dyn crate::io::traits::Fs>,
    recovery_policy: crate::config::RecoveryPolicy,
}

impl CloudMetadataPruneSnapshot {
    pub(crate) fn new(
        cloud: Arc<CloudStorage>,
        db_path: std::path::PathBuf,
        fs: Arc<dyn crate::io::traits::Fs>,
        recovery_policy: crate::config::RecoveryPolicy,
    ) -> Self {
        Self {
            cloud,
            db_path,
            fs,
            recovery_policy,
        }
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
        operation: impl FnOnce(Manifest, CloudMetadataPruneGuard) -> MidgeResult<T>,
    ) -> MidgeResult<T> {
        let deadline = crate::common::OperationDeadline::unbounded();
        let _publication_guard = self.cloud.lock_metadata_publication();

        let mut proofs = Vec::with_capacity(crate::storage::cloud::CLOUD_METADATA_FILES.len());
        let mut has_manifest_base = false;
        for file_name in crate::storage::cloud::CLOUD_METADATA_FILES {
            let local_path = self.db_path.join(file_name);
            let local_bytes = match std::fs::read(&local_path) {
                Ok(bytes) => Some(bytes),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => {
                    return Err(MidgeError::Io(std::io::Error::new(
                        error.kind(),
                        format!(
                            "local metadata '{}' is unreadable during WAL cleanup: {error}",
                            local_path.display()
                        ),
                    )))
                }
            };
            let key = crate::storage::cloud::cloud_metadata_key(file_name);
            let proof = crate::storage::cloud::blocking_cloud_object_proof_within(
                &self.cloud,
                &key,
                &deadline,
            )?;

            let (local_bytes, proof) = match (local_bytes, proof) {
                (Some(local_bytes), Some(proof)) => (local_bytes, proof),
                (None, None) => continue,
                (Some(_), None) => {
                    return Err(MidgeError::Internal(format!(
                        "cloud metadata '{key}' is missing"
                    )))
                }
                (None, Some(_)) => {
                    return Err(MidgeError::Internal(format!(
                        "cloud metadata '{key}' exists without committed local metadata"
                    )))
                }
            };

            if proof.bytes != local_bytes {
                return Err(MidgeError::Internal(format!(
                    "cloud metadata '{key}' does not match the captured committed metadata"
                )));
            }
            if matches!(*file_name, "manifest.snapshot.json" | "manifest.json") {
                has_manifest_base = true;
            }
            proofs.push(CloudMetadataPruneProof {
                key,
                expected_bytes: local_bytes,
                remote: proof.metadata,
            });
        }

        if !has_manifest_base {
            return Err(MidgeError::Internal(
                "no committed cloud manifest base is available to guard WAL cleanup".to_string(),
            ));
        }

        let manifest = crate::metadata::ManifestPersistence::load_with_fs_and_policy(
            &self.fs,
            self.recovery_policy,
        )
        .map_err(MidgeError::Internal)?;
        let guard = CloudMetadataPruneGuard::new(Arc::clone(&self.cloud), proofs);
        operation(manifest, guard)
    }
}

#[derive(Clone, Default)]
pub(crate) struct CloudWalPruneGuard {
    manifest: Manifest,
    metadata: Option<CloudMetadataPruneGuard>,
}

impl CloudWalPruneGuard {
    pub(crate) fn new(manifest: Manifest, metadata: Option<CloudMetadataPruneGuard>) -> Self {
        Self { manifest, metadata }
    }
}

struct ValidatedWalObject {
    proof: RemoteObjectProof,
    data_records: Vec<DataCoverageRecord>,
}

/// Runtime-owned format operations layered over raw hybrid object I/O.
pub(crate) trait HybridPersistence {
    fn enqueue_wal_segment(
        &self,
        segment_id: u64,
        local_path: &Path,
        max_sequence: u64,
    ) -> MidgeResult<String>;

    fn fence_cloud_wal_catalog(&self, writer_epoch: u64) -> MidgeResult<WalPublicationCatalog>;

    #[cfg(test)]
    fn verify_remote_wal_segment(
        &self,
        segment_id: u64,
        expected_max_sequence: u64,
    ) -> Result<(), String>;

    /// Publish a sealed WAL segment to the authoritative cloud catalog.
    ///
    /// `deadline` is the shared budget for every cloud round trip this makes —
    /// two object proofs plus a catalog compare-exchange, up to seven calls. It
    /// belongs to the caller waiting on the acknowledgement, so the whole
    /// sequence stays inside that caller's response timeout.
    fn publish_remote_wal_segment(
        &self,
        segment_id: u64,
        expected_max_sequence: u64,
        local_path: &Path,
        fencing_epoch: u64,
        deadline: &crate::common::OperationDeadline,
    ) -> MidgeResult<()>;

    #[cfg(test)]
    fn verify_manifest_cloud_objects(&self, manifest: &Manifest) -> Result<(), String>;

    #[cfg(test)]
    fn verify_manifest_cloud_objects_within(
        &self,
        manifest: &Manifest,
        deadline: &crate::common::OperationDeadline,
    ) -> MidgeResult<()>;

    #[cfg(test)]
    fn prune_cloud_wal_segment(
        &self,
        segment_id: u64,
        expected_max_sequence: u64,
        guard: CloudWalPruneGuard,
        fencing_epoch: u64,
    ) -> Result<(), String>;

    fn prune_cloud_wal_segment_within(
        &self,
        segment_id: u64,
        expected_max_sequence: u64,
        guard: CloudWalPruneGuard,
        fencing_epoch: u64,
        deadline: &crate::common::OperationDeadline,
    ) -> MidgeResult<()>;

    fn write_sst_object(&self, sst_name: &str, data: Vec<u8>) -> MidgeResult<()>;

    fn write_sst_object_within(
        &self,
        sst_name: &str,
        data: Vec<u8>,
        deadline: &crate::common::OperationDeadline,
    ) -> MidgeResult<()> {
        let _ = deadline;
        self.write_sst_object(sst_name, data)
    }

    fn delete_sst_object_blocking(&self, sst_name: &str) -> MidgeResult<()>;
}

impl HybridPersistence for HybridStorage {
    fn enqueue_wal_segment(
        &self,
        segment_id: u64,
        local_path: &Path,
        max_sequence: u64,
    ) -> MidgeResult<String> {
        let bytes = std::fs::read(local_path).map_err(MidgeError::Io)?;
        let readback = crate::wal::cloud_segment::validate_bytes(
            &local_path.display().to_string(),
            &bytes,
            max_sequence,
        )
        .map_err(MidgeError::Corruption)?;
        let object_key =
            crate::wal::cloud_segment::object_key(segment_id, readback.validation.writer_epoch);
        self.enqueue_object_upload(segment_id, object_key.clone(), local_path, max_sequence)?;
        Ok(object_key)
    }

    fn fence_cloud_wal_catalog(&self, writer_epoch: u64) -> MidgeResult<WalPublicationCatalog> {
        let existing = self
            .remote_object_proof_optional(crate::wal::cloud_catalog::OBJECT_KEY)
            .map_err(MidgeError::Internal)?;
        let (mut catalog, expected) = match existing.as_ref() {
            Some(proof) => (
                WalPublicationCatalog::decode(proof.bytes()).map_err(MidgeError::Corruption)?,
                Some(proof.metadata()),
            ),
            None => (
                WalPublicationCatalog::empty(writer_epoch).map_err(MidgeError::Internal)?,
                None,
            ),
        };

        let changed = if existing.is_some() {
            catalog.fence_to(writer_epoch).map_err(MidgeError::Fenced)?
        } else {
            true
        };
        if changed {
            let bytes = catalog.encode().map_err(MidgeError::Internal)?;
            self.compare_exchange_remote_object(
                crate::wal::cloud_catalog::OBJECT_KEY,
                expected,
                bytes,
            )?;
        }
        Ok(catalog)
    }

    #[cfg(test)]
    fn verify_remote_wal_segment(
        &self,
        segment_id: u64,
        expected_max_sequence: u64,
    ) -> Result<(), String> {
        let (_, entry) = authoritative_wal_entry(self, segment_id)?;
        if entry.max_sequence != expected_max_sequence {
            return Err(format!(
                "cloud WAL catalog segment {segment_id} max sequence {} does not match expected {expected_max_sequence}",
                entry.max_sequence
            ));
        }
        validate_remote_wal(self, &entry, &crate::common::OperationDeadline::unbounded())
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn publish_remote_wal_segment(
        &self,
        segment_id: u64,
        expected_max_sequence: u64,
        local_path: &Path,
        fencing_epoch: u64,
        deadline: &crate::common::OperationDeadline,
    ) -> MidgeResult<()> {
        let local_bytes = std::fs::read(local_path).map_err(|error| {
            MidgeError::Io(std::io::Error::new(
                error.kind(),
                format!(
                    "failed to read local WAL segment '{}' before cloud acknowledgement: {error}",
                    local_path.display()
                ),
            ))
        })?;
        let local_readback = crate::wal::cloud_segment::validate_bytes(
            &local_path.display().to_string(),
            &local_bytes,
            expected_max_sequence,
        )
        .map_err(MidgeError::Internal)?;
        let entry = PublishedWalSegment::from_validated_bytes(
            segment_id,
            expected_max_sequence,
            local_readback.validation.writer_epoch,
            &local_bytes,
        );
        let remote = validate_remote_wal(self, &entry, deadline)?;
        if remote.proof.bytes() != local_bytes {
            return Err(MidgeError::Internal(format!(
                "cloud WAL segment {segment_id} does not match the locally sealed bytes for writer epoch {}",
                local_readback.validation.writer_epoch
            )));
        }
        let catalog_proof = self
            .remote_object_proof_within(crate::wal::cloud_catalog::OBJECT_KEY, deadline)
            .map_err(|error| {
                contextualize_cloud_error(error, "cloud WAL publication catalog unavailable")
            })?;
        let mut catalog =
            WalPublicationCatalog::decode(catalog_proof.bytes()).map_err(MidgeError::Internal)?;
        if catalog.fencing_epoch != fencing_epoch {
            return Err(MidgeError::Fenced(format!(
                "cloud WAL catalog mutation requires fencing epoch {}, writer epoch {fencing_epoch} rejected",
                catalog.fencing_epoch
            )));
        }
        if !catalog
            .publish(fencing_epoch, entry)
            .map_err(MidgeError::Internal)?
        {
            return Ok(());
        }
        let catalog_bytes = catalog.encode().map_err(MidgeError::Internal)?;
        let publication = self.compare_exchange_remote_object_within(
            crate::wal::cloud_catalog::OBJECT_KEY,
            Some(catalog_proof.metadata()),
            catalog_bytes,
            deadline,
        );
        match publication {
            Ok(_) => Ok(()),
            Err(MidgeError::Busy(conflict)) => {
                let winning_proof = self
                    .remote_object_proof_within(crate::wal::cloud_catalog::OBJECT_KEY, deadline)
                    .map_err(|error| {
                        contextualize_cloud_error(
                            error,
                            "cloud WAL catalog publication conflict readback failed",
                        )
                    })?;
                let winning_catalog = WalPublicationCatalog::decode(winning_proof.bytes())
                    .map_err(MidgeError::Internal)?;
                if winning_catalog.fencing_epoch > fencing_epoch {
                    return Err(MidgeError::Fenced(format!(
                        "cloud WAL catalog advanced to fencing epoch {}, writer epoch {fencing_epoch} rejected during publication",
                        winning_catalog.fencing_epoch
                    )));
                }
                Err(contextualize_cloud_error(
                    MidgeError::Busy(conflict),
                    "cloud WAL catalog publication failed",
                ))
            }
            Err(error) => Err(contextualize_cloud_error(
                error,
                "cloud WAL catalog publication failed",
            )),
        }
    }

    #[cfg(test)]
    fn verify_manifest_cloud_objects(&self, manifest: &Manifest) -> Result<(), String> {
        self.verify_manifest_cloud_objects_within(
            manifest,
            &crate::common::OperationDeadline::unbounded(),
        )
        .map_err(|error| error.to_string())
    }

    #[cfg(test)]
    fn verify_manifest_cloud_objects_within(
        &self,
        manifest: &Manifest,
        deadline: &crate::common::OperationDeadline,
    ) -> MidgeResult<()> {
        for file in &manifest.files {
            validate_remote_sst_within(self, file, deadline)?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn prune_cloud_wal_segment(
        &self,
        segment_id: u64,
        expected_max_sequence: u64,
        guard: CloudWalPruneGuard,
        fencing_epoch: u64,
    ) -> Result<(), String> {
        self.prune_cloud_wal_segment_within(
            segment_id,
            expected_max_sequence,
            guard,
            fencing_epoch,
            &crate::common::OperationDeadline::unbounded(),
        )
        .map_err(|error| error.to_string())
    }

    fn prune_cloud_wal_segment_within(
        &self,
        segment_id: u64,
        expected_max_sequence: u64,
        guard: CloudWalPruneGuard,
        fencing_epoch: u64,
        deadline: &crate::common::OperationDeadline,
    ) -> MidgeResult<()> {
        let (catalog_proof, catalog) = authoritative_wal_catalog_within(self, deadline)?;
        let Some(entry) = catalog.segments.get(&segment_id).cloned() else {
            // Eligibility and retirement are separate provider round trips. If
            // another attempt committed between them, settle this request as
            // idempotently complete and retain the now-unguarded object.
            self.queue_cloud_wal_prune_complete(segment_id, crate::storage::StorageOutcome::Ok(()));
            return Ok(());
        };
        if entry.max_sequence != expected_max_sequence {
            return Err(MidgeError::Internal(format!(
                "cloud WAL catalog segment {segment_id} max sequence {} does not match expected {expected_max_sequence}",
                entry.max_sequence
            )));
        }
        let validated = validate_remote_wal(self, &entry, deadline)?;
        if !wal_data_records_covered_by_manifest(&validated.data_records, &guard.manifest) {
            return Err(MidgeError::Internal(format!(
                "cloud WAL segment {segment_id} contains records not covered by the committed manifest"
            )));
        }

        let mut dependencies = Vec::with_capacity(
            guard.manifest.files.len().saturating_add(
                guard
                    .metadata
                    .as_ref()
                    .map_or(0, |guard| guard.objects.len()),
            ),
        );
        for file in &guard.manifest.files {
            let proof = validate_remote_sst_within(self, file, deadline)?;
            dependencies.push(self.remote_identity_guard(&proof));
        }
        if let Some(metadata) = guard.metadata {
            dependencies.extend(metadata.objects);
        }

        // Deterministic proof/publication boundary used to verify that an SST
        // identity change after semantic validation cannot retire WAL
        // authority. The worker still revalidates every dependency and the
        // target immediately before its conditional delete.
        crate::failpoints::fail_point!("midge::cloud::after_wal_prune_dependency_validation");

        // Publication authority is retired before physical deletion. A crash or
        // delete failure after this point can leak an ignored object but cannot
        // make recovery depend on a missing object.
        self.verify_remote_delete_guards_within(&validated.proof, &dependencies, deadline)?;
        let mut catalog = catalog;
        if catalog
            .retire(fencing_epoch, &entry)
            .map_err(MidgeError::Internal)?
        {
            self.compare_exchange_remote_object_within(
                crate::wal::cloud_catalog::OBJECT_KEY,
                Some(catalog_proof.metadata()),
                catalog.encode().map_err(MidgeError::Internal)?,
                deadline,
            )
            .map_err(|error| {
                contextualize_cloud_error(error, "cloud WAL catalog retirement failed")
            })?;
        }

        if let Err(error) =
            self.delete_remote_object_guarded(segment_id, validated.proof, dependencies)
        {
            tracing::warn!(
                segment_id,
                error = %error,
                "cloud WAL catalog entry retired; retaining orphan after delete admission failure"
            );
            self.queue_cloud_wal_prune_complete(segment_id, crate::storage::StorageOutcome::Ok(()));
        }
        Ok(())
    }

    fn write_sst_object(&self, sst_name: &str, data: Vec<u8>) -> MidgeResult<()> {
        self.write_sst_object_within(
            sst_name,
            data,
            &crate::common::OperationDeadline::unbounded(),
        )
    }

    fn write_sst_object_within(
        &self,
        sst_name: &str,
        data: Vec<u8>,
        deadline: &crate::common::OperationDeadline,
    ) -> MidgeResult<()> {
        let expected_size = data.len() as u64;
        let expected_crc = crc32c::crc32c(&data);
        validate_sst_object_bytes(sst_name, expected_size, None, None, &data)
            .map_err(MidgeError::Internal)?;
        crate::failpoints::fail_point!("midge::cloud::inject_fail_sst_upload", |_| Err(
            MidgeError::Internal("failpoint: cloud SST upload failed".to_string())
        ));

        let key = crate::sst::object_key(sst_name);
        self.publish_immutable_object_within(&key, data, deadline)?;
        let proof = self
            .remote_object_proof_within(&key, deadline)
            .map_err(|error| contextualize_cloud_error(error, "cloud SST readback failed"))?;
        validate_sst_object_bytes(
            sst_name,
            expected_size,
            Some(expected_crc),
            None,
            proof.bytes(),
        )
        .map_err(MidgeError::Internal)?;
        Ok(())
    }

    fn delete_sst_object_blocking(&self, sst_name: &str) -> MidgeResult<()> {
        self.delete_immutable_object_blocking(&crate::sst::object_key(sst_name))
    }
}

#[cfg(test)]
fn authoritative_wal_entry(
    storage: &HybridStorage,
    segment_id: u64,
) -> Result<(RemoteObjectProof, PublishedWalSegment), String> {
    authoritative_wal_entry_within(
        storage,
        segment_id,
        &crate::common::OperationDeadline::unbounded(),
    )
    .map_err(|error| error.to_string())
}

#[cfg(test)]
fn authoritative_wal_entry_within(
    storage: &HybridStorage,
    segment_id: u64,
    deadline: &crate::common::OperationDeadline,
) -> MidgeResult<(RemoteObjectProof, PublishedWalSegment)> {
    let (proof, catalog) = authoritative_wal_catalog_within(storage, deadline)?;
    let entry = catalog.segments.get(&segment_id).cloned().ok_or_else(|| {
        MidgeError::Internal(format!(
            "cloud WAL segment {segment_id} is not authoritative in the publication catalog"
        ))
    })?;
    Ok((proof, entry))
}

fn authoritative_wal_catalog_within(
    storage: &HybridStorage,
    deadline: &crate::common::OperationDeadline,
) -> MidgeResult<(RemoteObjectProof, WalPublicationCatalog)> {
    let proof = storage
        .remote_object_proof_within(crate::wal::cloud_catalog::OBJECT_KEY, deadline)
        .map_err(|error| {
            contextualize_cloud_error(error, "cloud WAL publication catalog unavailable")
        })?;
    let catalog = WalPublicationCatalog::decode(proof.bytes()).map_err(MidgeError::Internal)?;
    Ok((proof, catalog))
}

fn validate_remote_wal(
    storage: &HybridStorage,
    entry: &PublishedWalSegment,
    deadline: &crate::common::OperationDeadline,
) -> MidgeResult<ValidatedWalObject> {
    let proof = storage.remote_object_proof_within(&entry.object_key, deadline)?;
    entry
        .validate_bytes(proof.bytes())
        .map_err(MidgeError::Internal)?;
    let readback = crate::wal::cloud_segment::validate_bytes(
        &entry.object_key,
        proof.bytes(),
        entry.max_sequence,
    )
    .map_err(MidgeError::Internal)?;
    Ok(ValidatedWalObject {
        proof,
        data_records: readback.data_records,
    })
}

fn contextualize_cloud_error(error: MidgeError, context: &str) -> MidgeError {
    match error {
        MidgeError::Timeout(message) => MidgeError::Timeout(format!("{context}: {message}")),
        other => MidgeError::Internal(format!("{context}: {other}")),
    }
}

pub(crate) fn wal_data_records_covered_by_manifest(
    data_records: &[DataCoverageRecord],
    manifest: &Manifest,
) -> bool {
    data_records.iter().all(|record| {
        manifest
            .files
            .iter()
            .any(|file| file_covers_record(file, record))
    })
}

fn file_covers_record(file: &FileMeta, record: &DataCoverageRecord) -> bool {
    if file.cf_id != record.cf_id {
        return false;
    }

    let (Some(smallest_seq), Some(largest_seq)) = (file.smallest_seq, file.largest_seq) else {
        return false;
    };
    if record.seq < smallest_seq || record.seq > largest_seq {
        return false;
    }

    let (Some(smallest_key), Some(largest_key)) =
        (file.smallest_key.as_ref(), file.largest_key.as_ref())
    else {
        return false;
    };
    if let Some(range_end) = record.range_end.as_ref() {
        smallest_key.as_slice() <= record.key.as_slice()
            && range_end.as_slice() <= largest_key.as_slice()
    } else {
        smallest_key.as_slice() <= record.key.as_slice()
            && record.key.as_slice() <= largest_key.as_slice()
    }
}

fn validate_remote_sst_within(
    storage: &HybridStorage,
    file: &FileMeta,
    deadline: &crate::common::OperationDeadline,
) -> MidgeResult<RemoteObjectProof> {
    let key = crate::sst::object_key(&file.name);
    let proof = storage.remote_object_proof_within(&key, deadline)?;
    validate_sst_object_bytes(
        &file.name,
        file.size_bytes,
        file.content_crc32c,
        Some(file),
        proof.bytes(),
    )
    .map_err(MidgeError::Internal)?;
    Ok(proof)
}

fn validate_sst_object_bytes(
    sst_name: &str,
    expected_size_bytes: u64,
    expected_content_crc32c: Option<u32>,
    expected_file: Option<&FileMeta>,
    data: &[u8],
) -> Result<crate::sst::fs::SstFileSummary, String> {
    if expected_size_bytes > 0 && data.len() as u64 != expected_size_bytes {
        return Err(format!(
            "cloud SST '{sst_name}' size mismatch: manifest={expected_size_bytes}, object={}",
            data.len()
        ));
    }

    let actual_content_crc32c = crc32c::crc32c(data);
    if let Some(expected_content_crc32c) = expected_content_crc32c {
        if actual_content_crc32c != expected_content_crc32c {
            return Err(format!(
                "cloud SST '{sst_name}' content crc32c {actual_content_crc32c:08x} does not match manifest {expected_content_crc32c:08x}"
            ));
        }
    }

    let mut temp = tempfile::Builder::new()
        .prefix("midge-cloud-sst-verify-")
        .suffix(".sst")
        .tempfile()
        .map_err(|error| format!("create temp SST verifier for '{sst_name}': {error}"))?;
    temp.write_all(data)
        .map_err(|error| format!("write temp SST verifier for '{sst_name}': {error}"))?;
    temp.flush()
        .map_err(|error| format!("flush temp SST verifier for '{sst_name}': {error}"))?;

    let reader = crate::sst::fs::SstFileIo::open_with_real_fs(temp.path())
        .map_err(|error| format!("cloud SST '{sst_name}' failed validation: {error}"))?;
    let summary = reader
        .summary()
        .map_err(|error| format!("cloud SST '{sst_name}' summary validation: {error}"))?;
    if let Some(expected_file) = expected_file {
        verify_sst_summary_matches_manifest(sst_name, &summary, expected_file)?;
    }
    Ok(summary)
}

fn verify_sst_summary_matches_manifest(
    sst_name: &str,
    summary: &crate::sst::fs::SstFileSummary,
    file: &FileMeta,
) -> Result<(), String> {
    if file.size_bytes > 0 && summary.size_bytes != file.size_bytes {
        return Err(format!(
            "cloud SST '{sst_name}' physical size {} does not match manifest {}",
            summary.size_bytes, file.size_bytes
        ));
    }
    if file
        .smallest_key
        .as_ref()
        .is_some_and(|key| summary.smallest_key.as_slice() != key.as_slice())
    {
        return Err(format!(
            "cloud SST '{sst_name}' smallest key does not match manifest"
        ));
    }
    if file
        .largest_key
        .as_ref()
        .is_some_and(|key| summary.largest_key.as_slice() != key.as_slice())
    {
        return Err(format!(
            "cloud SST '{sst_name}' largest key does not match manifest"
        ));
    }
    if file
        .smallest_seq
        .is_some_and(|sequence| summary.smallest_seq != sequence)
    {
        return Err(format!(
            "cloud SST '{sst_name}' smallest sequence {} does not match manifest {:?}",
            summary.smallest_seq, file.smallest_seq
        ));
    }
    if file
        .largest_seq
        .is_some_and(|sequence| summary.largest_seq != sequence)
    {
        return Err(format!(
            "cloud SST '{sst_name}' largest sequence {} does not match manifest {:?}",
            summary.largest_seq, file.largest_seq
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_preserve_timeout_variant_when_adding_cloud_publication_context() {
        // Arrange
        let timeout = MidgeError::Timeout("remote CAS timed out".to_string());

        // Act
        let contextualized =
            contextualize_cloud_error(timeout, "cloud WAL catalog publication failed");

        // Assert
        assert!(matches!(
            contextualized,
            MidgeError::Timeout(message)
                if message.contains("catalog publication")
                    && message.contains("remote CAS timed out")
        ));
    }

    #[test]
    fn should_require_manifest_coverage_for_wal_records() {
        // Arrange
        let manifest = Manifest {
            files: vec![FileMeta {
                cf_id: 7,
                smallest_key: Some(b"a".to_vec()),
                largest_key: Some(b"m".to_vec()),
                smallest_seq: Some(10),
                largest_seq: Some(20),
                ..FileMeta::default()
            }],
            ..Manifest::default()
        };
        let covered = DataCoverageRecord {
            cf_id: 7,
            key: b"b".to_vec(),
            range_end: None,
            seq: 12,
        };
        let outside_key = DataCoverageRecord {
            key: b"z".to_vec(),
            ..covered.clone()
        };

        // Act
        let covered_result = wal_data_records_covered_by_manifest(&[covered], &manifest);
        let outside_result = wal_data_records_covered_by_manifest(&[outside_key], &manifest);

        // Assert
        assert!(covered_result);
        assert!(!outside_result);
    }

    #[test]
    fn should_require_full_range_coverage_for_wal_tombstones() {
        // Arrange
        let file = FileMeta {
            cf_id: 1,
            smallest_key: Some(b"a".to_vec()),
            largest_key: Some(b"m".to_vec()),
            smallest_seq: Some(1),
            largest_seq: Some(9),
            ..FileMeta::default()
        };
        let covered = DataCoverageRecord {
            cf_id: 1,
            key: b"c".to_vec(),
            range_end: Some(b"k".to_vec()),
            seq: 5,
        };
        let uncovered = DataCoverageRecord {
            range_end: Some(b"z".to_vec()),
            ..covered.clone()
        };

        // Act
        let covered_result = file_covers_record(&file, &covered);
        let uncovered_result = file_covers_record(&file, &uncovered);

        // Assert
        assert!(covered_result);
        assert!(!uncovered_result);
    }
}
