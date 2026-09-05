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
        deadline: &crate::common::OperationDeadline,
        operation: impl FnOnce(Manifest, CloudMetadataPruneGuard) -> MidgeResult<T>,
    ) -> MidgeResult<T> {
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
                deadline,
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

struct ValidatedWalPruneCandidate {
    segment_id: u64,
    entry: PublishedWalSegment,
    validated: ValidatedWalObject,
}

#[derive(Clone, Default)]
struct ExactCoverageState {
    state: Option<crate::sst::types::KeyState>,
    ambiguous: bool,
}

pub(crate) type CloudWalPruneBatchResults = Vec<(u64, MidgeResult<()>)>;

fn sorted_cloud_wal_prune_results(
    mut results: CloudWalPruneBatchResults,
) -> CloudWalPruneBatchResults {
    results.sort_by_key(|(segment_id, _)| *segment_id);
    results
}

fn validate_wal_prune_candidates_within(
    storage: &HybridStorage,
    candidates: &[(u64, u64)],
    catalog: &WalPublicationCatalog,
    deadline: &crate::common::OperationDeadline,
) -> (CloudWalPruneBatchResults, Vec<ValidatedWalPruneCandidate>) {
    let mut results = Vec::with_capacity(candidates.len());
    let mut validated = Vec::with_capacity(candidates.len());
    let mut blocked_by = None;
    for &(segment_id, expected_max_sequence) in candidates {
        let Some(entry) = catalog.segments.get(&segment_id).cloned() else {
            results.push((segment_id, Ok(())));
            continue;
        };
        if let Some(older_segment_id) = blocked_by {
            results.push((
                segment_id,
                Err(MidgeError::Busy(format!(
                    "cloud WAL segment {segment_id} cannot retire past older authoritative segment {older_segment_id}"
                ))),
            ));
            continue;
        }
        if entry.max_sequence != expected_max_sequence {
            results.push((
                segment_id,
                Err(MidgeError::Internal(format!(
                    "cloud WAL catalog segment {segment_id} max sequence {} does not match expected {expected_max_sequence}",
                    entry.max_sequence
                ))),
            ));
            blocked_by = Some(segment_id);
            continue;
        }
        match validate_remote_wal(storage, &entry, deadline) {
            Ok(validated_wal) => validated.push(ValidatedWalPruneCandidate {
                segment_id,
                entry,
                validated: validated_wal,
            }),
            Err(error) => {
                results.push((segment_id, Err(error)));
                blocked_by = Some(segment_id);
            }
        }
    }
    (results, validated)
}

fn partition_exactly_covered_wal_candidates(
    candidates: Vec<ValidatedWalPruneCandidate>,
    coverage: Vec<bool>,
    results: &mut CloudWalPruneBatchResults,
) -> Vec<ValidatedWalPruneCandidate> {
    let mut covered = Vec::with_capacity(candidates.len());
    let mut blocked_by = None;
    for (candidate, is_covered) in candidates.into_iter().zip(coverage) {
        if let Some(older_segment_id) = blocked_by {
            results.push((
                candidate.segment_id,
                Err(MidgeError::Busy(format!(
                    "cloud WAL segment {} cannot retire past older authoritative segment {older_segment_id}",
                    candidate.segment_id
                ))),
            ));
        } else if is_covered {
            covered.push(candidate);
        } else {
            blocked_by = Some(candidate.segment_id);
            results.push((
                candidate.segment_id,
                Err(MidgeError::Internal(format!(
                    "cloud WAL segment {} contains records not exactly covered by the committed manifest SSTs",
                    candidate.segment_id
                ))),
            ));
        }
    }
    covered
}

fn candidates_are_oldest_catalog_prefix(
    candidate_ids: impl IntoIterator<Item = u64>,
    catalog: &WalPublicationCatalog,
) -> bool {
    let candidate_ids = candidate_ids
        .into_iter()
        .filter(|segment_id| catalog.segments.contains_key(segment_id))
        .collect::<Vec<_>>();
    catalog
        .segments
        .keys()
        .take(candidate_ids.len())
        .copied()
        .eq(candidate_ids)
}

fn schedule_retired_wal_deletes(storage: &HybridStorage, retired: Vec<ValidatedWalPruneCandidate>) {
    let retired_ids = retired
        .iter()
        .map(|candidate| candidate.segment_id)
        .collect::<Vec<_>>();
    let delete_targets = retired
        .into_iter()
        .map(|candidate| (candidate.segment_id, candidate.validated.proof))
        .collect();
    if let Err(error) = storage.delete_remote_objects_guarded(delete_targets) {
        for segment_id in retired_ids {
            storage.queue_cloud_wal_prune_complete(
                segment_id,
                crate::storage::StorageOutcome::Err(format!(
                    "catalog authority retired but physical delete was not admitted: {error}"
                )),
            );
        }
    }
}

fn retire_covered_wal_catalog_prefix_within(
    storage: &HybridStorage,
    covered_candidates: Vec<ValidatedWalPruneCandidate>,
    fencing_epoch: u64,
    deadline: &crate::common::OperationDeadline,
    results: &mut CloudWalPruneBatchResults,
) -> MidgeResult<Vec<ValidatedWalPruneCandidate>> {
    let catalog_mutation =
        storage.lock_wal_catalog_mutation_within(deadline, "cloud WAL retirement")?;
    let authority = load_and_repair_catalog_within(storage, deadline)?.ok_or_else(|| {
        MidgeError::Internal("cloud WAL publication catalog is missing".to_string())
    })?;
    let mut current_catalog = authority.catalog;
    if current_catalog.fencing_epoch != fencing_epoch {
        for candidate in covered_candidates {
            results.push((
                candidate.segment_id,
                Err(MidgeError::Fenced(format!(
                    "cloud WAL catalog advanced to fencing epoch {}, writer epoch {fencing_epoch} rejected during retirement",
                    current_catalog.fencing_epoch
                ))),
            ));
        }
        return Ok(Vec::new());
    }
    if !candidates_are_oldest_catalog_prefix(
        covered_candidates
            .iter()
            .map(|candidate| candidate.segment_id),
        &current_catalog,
    ) {
        let oldest = current_catalog
            .segments
            .keys()
            .next()
            .copied()
            .unwrap_or_default();
        for candidate in covered_candidates {
            results.push((
                candidate.segment_id,
                Err(MidgeError::Busy(format!(
                    "cloud WAL segment {} lost oldest-prefix authority before retirement; oldest is {oldest}",
                    candidate.segment_id
                ))),
            ));
        }
        return Ok(Vec::new());
    }

    let mut retired = Vec::with_capacity(covered_candidates.len());
    let mut blocked_by = None;
    for candidate in covered_candidates {
        if let Some(older_segment_id) = blocked_by {
            results.push((
                candidate.segment_id,
                Err(MidgeError::Busy(format!(
                    "cloud WAL segment {} cannot retire past changed authoritative segment {older_segment_id}",
                    candidate.segment_id
                ))),
            ));
            continue;
        }
        match current_catalog.segments.get(&candidate.segment_id) {
            None => results.push((candidate.segment_id, Ok(()))),
            Some(actual) if actual == &candidate.entry => {
                current_catalog
                    .retire(fencing_epoch, &candidate.entry)
                    .map_err(MidgeError::Internal)?;
                retired.push(candidate);
            }
            Some(_) => {
                blocked_by = Some(candidate.segment_id);
                results.push((
                    candidate.segment_id,
                    Err(MidgeError::Busy(format!(
                        "cloud WAL catalog segment {} changed before retirement",
                        candidate.segment_id
                    ))),
                ));
            }
        }
    }

    if !retired.is_empty() {
        if let Err(error) = commit_catalog_within(
            storage,
            Some(&authority.primary),
            &current_catalog,
            deadline,
        ) {
            let message =
                contextualize_cloud_error(error, "cloud WAL catalog batch retirement failed")
                    .to_string();
            for candidate in retired {
                results.push((
                    candidate.segment_id,
                    Err(MidgeError::Internal(message.clone())),
                ));
            }
            return Ok(Vec::new());
        }
    }
    drop(catalog_mutation);
    Ok(retired)
}

struct CatalogAuthority {
    primary: RemoteObjectProof,
    catalog: WalPublicationCatalog,
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
    /// `deadline` is the shared budget for every cloud round trip this makes,
    /// including immutable-WAL proof, both catalog proofs, conditional writes,
    /// and exact readback. It belongs to the caller waiting on the
    /// acknowledgement, so the whole sequence stays inside that caller's
    /// response timeout.
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

    #[cfg(test)]
    fn prune_cloud_wal_segment_within(
        &self,
        segment_id: u64,
        expected_max_sequence: u64,
        guard: CloudWalPruneGuard,
        fencing_epoch: u64,
        deadline: &crate::common::OperationDeadline,
    ) -> MidgeResult<()>;

    fn prune_cloud_wal_segments_within(
        &self,
        candidates: &[(u64, u64)],
        guard: CloudWalPruneGuard,
        fencing_epoch: u64,
        deadline: &crate::common::OperationDeadline,
    ) -> MidgeResult<CloudWalPruneBatchResults>;

    fn write_sst_object(&self, sst_name: &str, data: Vec<u8>) -> MidgeResult<()>;

    fn write_sst_object_with_proof(
        &self,
        sst_name: &str,
        data: Vec<u8>,
        deadline: &crate::common::OperationDeadline,
    ) -> MidgeResult<GuardedObjectProof>;

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
        let deadline = crate::common::OperationDeadline::unbounded();
        let existing = load_and_repair_catalog_within(self, &deadline)?;
        let (mut catalog, expected) = if let Some(authority) = existing {
            (authority.catalog, Some(authority.primary))
        } else {
            (
                WalPublicationCatalog::empty(writer_epoch).map_err(MidgeError::Internal)?,
                None,
            )
        };

        let changed = if expected.is_some() {
            catalog.fence_to(writer_epoch).map_err(MidgeError::Fenced)?
        } else {
            true
        };
        if changed {
            commit_catalog_within(self, expected.as_ref(), &catalog, &deadline)?;
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
        let _catalog_mutation =
            self.lock_wal_catalog_mutation_within(deadline, "cloud WAL publication")?;
        let authority = load_and_repair_catalog_within(self, deadline)?.ok_or_else(|| {
            MidgeError::Internal("cloud WAL publication catalog is missing".to_string())
        })?;
        let mut catalog = authority.catalog;
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
        let publication = commit_catalog_within(self, Some(&authority.primary), &catalog, deadline);
        match publication {
            Ok(_) => Ok(()),
            Err(MidgeError::Busy(conflict)) => {
                let winning_catalog = load_and_repair_catalog_within(self, deadline)?
                    .ok_or_else(|| {
                        MidgeError::Internal(
                            "cloud WAL publication catalog disappeared after conflict".to_string(),
                        )
                    })?
                    .catalog;
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

    #[cfg(test)]
    fn prune_cloud_wal_segment_within(
        &self,
        segment_id: u64,
        expected_max_sequence: u64,
        guard: CloudWalPruneGuard,
        fencing_epoch: u64,
        deadline: &crate::common::OperationDeadline,
    ) -> MidgeResult<()> {
        let mut results = self.prune_cloud_wal_segments_within(
            &[(segment_id, expected_max_sequence)],
            guard,
            fencing_epoch,
            deadline,
        )?;
        let Some((_, result)) = results.pop() else {
            // Catalog authority was retired and a storage-owned conditional
            // delete worker now owns the terminal completion event.
            return Ok(());
        };
        if result.is_ok() {
            self.queue_cloud_wal_prune_complete(segment_id, crate::storage::StorageOutcome::Ok(()));
        }
        result
    }

    fn prune_cloud_wal_segments_within(
        &self,
        candidates: &[(u64, u64)],
        guard: CloudWalPruneGuard,
        fencing_epoch: u64,
        deadline: &crate::common::OperationDeadline,
    ) -> MidgeResult<CloudWalPruneBatchResults> {
        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        let mut ordered_candidates = candidates.to_vec();
        ordered_candidates.sort_unstable_by_key(|(segment_id, _)| *segment_id);
        if ordered_candidates
            .windows(2)
            .any(|pair| pair[0].0 == pair[1].0)
        {
            return Err(MidgeError::Internal(
                "cloud WAL prune batch contains duplicate segment ids".to_string(),
            ));
        }

        let (_, catalog) = authoritative_wal_catalog_within(self, deadline)?;
        if !candidates_are_oldest_catalog_prefix(
            ordered_candidates.iter().map(|(segment_id, _)| *segment_id),
            &catalog,
        ) {
            let oldest = catalog.segments.keys().next().copied().unwrap_or_default();
            return Ok(ordered_candidates
                .into_iter()
                .map(|(segment_id, _)| {
                    if catalog.segments.contains_key(&segment_id) {
                        (
                            segment_id,
                            Err(MidgeError::Busy(format!(
                                "cloud WAL segment {segment_id} cannot retire before oldest authoritative segment {oldest}"
                            ))),
                        )
                    } else {
                        (segment_id, Ok(()))
                    }
                })
                .collect());
        }
        let (mut results, validated_candidates) =
            validate_wal_prune_candidates_within(self, &ordered_candidates, &catalog, deadline);

        if validated_candidates.is_empty() {
            return Ok(sorted_cloud_wal_prune_results(results));
        }

        let mut dependencies = Vec::new();
        let (coverage, sst_dependencies) = validate_manifest_sst_coverage_within(
            self,
            &guard.manifest,
            &validated_candidates,
            deadline,
        )?;
        let covered_candidates =
            partition_exactly_covered_wal_candidates(validated_candidates, coverage, &mut results);

        if covered_candidates.is_empty() {
            return Ok(sorted_cloud_wal_prune_results(results));
        }

        dependencies.extend(sst_dependencies);
        if let Some(metadata) = guard.metadata {
            dependencies.extend(metadata.objects);
        }

        // Deterministic proof/publication boundary used to verify that an SST
        // identity change after semantic validation cannot retire WAL
        // authority. Dependencies are revalidated before the catalog CAS;
        // post-CAS cleanup needs only the target's conditional identity.
        crate::failpoints::fail_point!("midge::cloud::after_wal_prune_dependency_validation");

        // Publication authority is retired before physical deletion. A crash or
        // delete failure after this point can leak an ignored object but cannot
        // make recovery depend on a missing object. Re-read under the local
        // mutation lock so a same-epoch publication cannot be overwritten by a
        // retirement CAS built from an older catalog snapshot.
        let targets = covered_candidates
            .iter()
            .map(|candidate| candidate.validated.proof.clone())
            .collect::<Vec<_>>();
        self.verify_remote_delete_batch_guards_within(&targets, &dependencies, deadline)?;

        let retired = retire_covered_wal_catalog_prefix_within(
            self,
            covered_candidates,
            fencing_epoch,
            deadline,
            &mut results,
        )?;
        schedule_retired_wal_deletes(self, retired);
        Ok(sorted_cloud_wal_prune_results(results))
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
        self.write_sst_object_with_proof(sst_name, data, deadline)
            .map(|_| ())
    }

    fn write_sst_object_with_proof(
        &self,
        sst_name: &str,
        data: Vec<u8>,
        deadline: &crate::common::OperationDeadline,
    ) -> MidgeResult<GuardedObjectProof> {
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
        Ok(self.remote_identity_guard(&proof))
    }

    fn delete_sst_object_blocking(&self, sst_name: &str) -> MidgeResult<()> {
        crate::failpoints::fail_point!("midge::cloud::inject_fail_sst_delete", |_| Err(
            MidgeError::Internal("failpoint: cloud SST delete failed".to_string())
        ));
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
    let _catalog_mutation =
        storage.lock_wal_catalog_mutation_within(deadline, "cloud WAL catalog read")?;
    let authority = load_and_repair_catalog_within(storage, deadline)?.ok_or_else(|| {
        MidgeError::Internal("cloud WAL publication catalog is missing".to_string())
    })?;
    Ok((authority.primary, authority.catalog))
}

fn load_and_repair_catalog_within(
    storage: &HybridStorage,
    deadline: &crate::common::OperationDeadline,
) -> MidgeResult<Option<CatalogAuthority>> {
    let primary = storage
        .remote_object_proof_optional_within(crate::wal::cloud_catalog::OBJECT_KEY, deadline)
        .map_err(|error| {
            contextualize_cloud_error(error, "cloud WAL publication catalog unavailable")
        })?;

    if let Some(primary) = primary {
        match WalPublicationCatalog::decode(primary.bytes()) {
            Ok(catalog) => {
                sync_catalog_copy_within(
                    storage,
                    crate::wal::cloud_catalog::MIRROR_OBJECT_KEY,
                    primary.bytes(),
                    deadline,
                )?;
                return Ok(Some(CatalogAuthority { primary, catalog }));
            }
            Err(primary_error) => {
                let mirror = storage
                    .remote_object_proof_optional_within(
                        crate::wal::cloud_catalog::MIRROR_OBJECT_KEY,
                        deadline,
                    )
                    .map_err(|error| {
                        contextualize_cloud_error(
                            error,
                            "cloud WAL publication catalog mirror unavailable",
                        )
                    })?
                    .ok_or_else(|| {
                        MidgeError::Corruption(format!(
                            "primary cloud WAL publication catalog is invalid and no mirror exists: {primary_error}"
                        ))
                    })?;
                let catalog = WalPublicationCatalog::decode(mirror.bytes()).map_err(|mirror_error| {
                    MidgeError::Corruption(format!(
                        "both cloud WAL publication catalogs are invalid; primary: {primary_error}; mirror: {mirror_error}"
                    ))
                })?;
                tracing::warn!(
                    error = %primary_error,
                    "repairing invalid cloud WAL publication catalog from validated mirror"
                );
                let repaired = sync_catalog_copy_within(
                    storage,
                    crate::wal::cloud_catalog::OBJECT_KEY,
                    mirror.bytes(),
                    deadline,
                )?;
                return Ok(Some(CatalogAuthority {
                    primary: repaired,
                    catalog,
                }));
            }
        }
    }

    let Some(mirror) = storage
        .remote_object_proof_optional_within(crate::wal::cloud_catalog::MIRROR_OBJECT_KEY, deadline)
        .map_err(|error| {
            contextualize_cloud_error(error, "cloud WAL publication catalog mirror unavailable")
        })?
    else {
        return Ok(None);
    };
    let catalog = WalPublicationCatalog::decode(mirror.bytes()).map_err(|error| {
        MidgeError::Corruption(format!(
            "primary cloud WAL publication catalog is missing and its mirror is invalid: {error}"
        ))
    })?;
    tracing::warn!("restoring missing cloud WAL publication catalog from validated mirror");
    let repaired = sync_catalog_copy_within(
        storage,
        crate::wal::cloud_catalog::OBJECT_KEY,
        mirror.bytes(),
        deadline,
    )?;
    Ok(Some(CatalogAuthority {
        primary: repaired,
        catalog,
    }))
}

fn commit_catalog_within(
    storage: &HybridStorage,
    expected_primary: Option<&RemoteObjectProof>,
    catalog: &WalPublicationCatalog,
    deadline: &crate::common::OperationDeadline,
) -> MidgeResult<CatalogAuthority> {
    let bytes = catalog.encode().map_err(MidgeError::Internal)?;

    let primary = storage.compare_exchange_remote_object_within(
        crate::wal::cloud_catalog::OBJECT_KEY,
        expected_primary.map(RemoteObjectProof::metadata),
        bytes.clone(),
        deadline,
    )?;
    sync_catalog_copy_within(
        storage,
        crate::wal::cloud_catalog::MIRROR_OBJECT_KEY,
        &bytes,
        deadline,
    )?;
    Ok(CatalogAuthority {
        primary,
        catalog: catalog.clone(),
    })
}

fn sync_catalog_copy_within(
    storage: &HybridStorage,
    key: &str,
    bytes: &[u8],
    deadline: &crate::common::OperationDeadline,
) -> MidgeResult<RemoteObjectProof> {
    let existing = storage
        .remote_object_proof_optional_within(key, deadline)
        .map_err(|error| contextualize_cloud_error(error, "cloud WAL catalog copy unavailable"))?;
    if let Some(existing) = existing.as_ref() {
        if existing.bytes() == bytes {
            return Ok(existing.clone());
        }
    }
    storage
        .compare_exchange_remote_object_within(
            key,
            existing.as_ref().map(RemoteObjectProof::metadata),
            bytes.to_vec(),
            deadline,
        )
        .map_err(|error| contextualize_cloud_error(error, "cloud WAL catalog copy update failed"))
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

fn exact_state_sequence(state: &crate::sst::types::KeyState) -> Option<u64> {
    match state {
        crate::sst::types::KeyState::Absent => None,
        crate::sst::types::KeyState::Tombstone(sequence)
        | crate::sst::types::KeyState::Value(_, sequence, _, _) => Some(*sequence),
    }
}

impl ExactCoverageState {
    fn observe(&mut self, state: crate::sst::types::KeyState) {
        let Some(sequence) = exact_state_sequence(&state) else {
            return;
        };
        let Some(current) = self.state.as_ref() else {
            self.state = Some(state);
            return;
        };
        let current_sequence = exact_state_sequence(current).unwrap_or_default();
        match sequence.cmp(&current_sequence) {
            std::cmp::Ordering::Greater => {
                self.state = Some(state);
                self.ambiguous = false;
            }
            std::cmp::Ordering::Equal if current != &state => self.ambiguous = true,
            std::cmp::Ordering::Equal | std::cmp::Ordering::Less => {}
        }
    }

    fn exactly_covers(&self, record: &DataCoverageRecord) -> bool {
        use crate::sst::types::KeyState;
        use crate::wal::types::WalOpRole;

        if self.ambiguous || matches!(record.op.role(), WalOpRole::RangeDelete) {
            return false;
        }
        match self.state.as_ref() {
            Some(KeyState::Value(value, sequence, expiration, op_type)) => {
                *sequence > record.seq
                    || *sequence == record.seq
                        && matches!(record.op.role(), WalOpRole::ValueWrite)
                        && record.value.as_deref() == Some(value.as_ref())
                        && record.expiration == *expiration
                        && crate::wal::WalOpKind::from_wire_format(*op_type)
                            .is_ok_and(|op| matches!(op.role(), WalOpRole::ValueWrite))
            }
            Some(KeyState::Tombstone(sequence)) => {
                *sequence > record.seq
                    || *sequence == record.seq && matches!(record.op.role(), WalOpRole::PointDelete)
            }
            Some(KeyState::Absent) | None => false,
        }
    }
}

pub(crate) struct VerifiedManifestWalCoverage<'a> {
    sst_dir: std::path::PathBuf,
    manifest: &'a Manifest,
    readers: std::cell::RefCell<
        std::collections::HashMap<String, Option<Box<dyn crate::sst::traits::SstReaderExt>>>,
    >,
}

impl<'a> VerifiedManifestWalCoverage<'a> {
    pub(crate) fn open(sst_dir: &Path, manifest: &'a Manifest) -> Self {
        Self {
            sst_dir: sst_dir.to_path_buf(),
            manifest,
            readers: std::cell::RefCell::new(std::collections::HashMap::new()),
        }
    }

    fn state_for(&self, file: &FileMeta, key: &[u8]) -> Option<crate::sst::types::KeyState> {
        let mut readers = self.readers.borrow_mut();
        let reader = readers.entry(file.name.clone()).or_insert_with(|| {
            let name = crate::sst::PersistedSstName::parse(&file.name).ok()?;
            let bytes = std::fs::read(self.sst_dir.join(name.as_str())).ok()?;
            if file.size_bytes != 0
                && u64::try_from(bytes.len()).unwrap_or(u64::MAX) != file.size_bytes
            {
                return None;
            }
            if file
                .content_crc32c
                .is_none_or(|expected| crc32c::crc32c(&bytes) != expected)
            {
                return None;
            }
            let fs = crate::io::RealFs::new(&self.sst_dir).ok()?;
            let factory = crate::sst::FsSstFactoryIo::new(Arc::new(fs), 64 * 1024);
            crate::sst::SstFactory::open(&factory, Path::new(&file.name)).ok()
        });
        reader.as_ref()?.get_state(key).ok()
    }

    pub(crate) fn exactly_covers_data_records(&self, records: &[DataCoverageRecord]) -> bool {
        records.iter().all(|record| {
            if !matches!(record.op.role(), crate::wal::types::WalOpRole::ValueWrite) {
                return false;
            }
            let mut state = ExactCoverageState::default();
            for file in &self.manifest.files {
                if !file_covers_record(file, record) {
                    continue;
                }
                let Some(observed) = self.state_for(file, &record.key) else {
                    return false;
                };
                state.observe(observed);
            }
            state.exactly_covers(record)
        })
    }

    pub(crate) fn contains_wal_record(
        &self,
        file: &FileMeta,
        record: &crate::wal::WalRecord,
    ) -> bool {
        let Some(state) = self.state_for(file, record.key.as_ref()) else {
            return false;
        };
        match state {
            crate::sst::types::KeyState::Value(value, sequence, _, _) => {
                sequence > record.seq
                    || sequence == record.seq
                        && record
                            .value
                            .as_ref()
                            .is_some_and(|expected| expected == &value)
            }
            crate::sst::types::KeyState::Tombstone(sequence) => sequence >= record.seq,
            crate::sst::types::KeyState::Absent => false,
        }
    }
}

fn wal_data_records_exactly_covered_by_manifest(
    data_records: &[DataCoverageRecord],
    states: &[ExactCoverageState],
) -> bool {
    data_records
        .iter()
        .zip(states)
        .all(|(record, state)| state.exactly_covers(record))
}

fn file_may_contain_record_key(file: &FileMeta, record: &DataCoverageRecord) -> bool {
    if file.cf_id != record.cf_id {
        return false;
    }
    let (Some(smallest_key), Some(largest_key)) =
        (file.smallest_key.as_ref(), file.largest_key.as_ref())
    else {
        return false;
    };
    smallest_key.as_slice() <= record.key.as_slice()
        && record.key.as_slice() <= largest_key.as_slice()
}

#[cfg(test)]
pub(crate) fn wal_data_records_covered_by_manifest(
    data_records: &[DataCoverageRecord],
    manifest: &Manifest,
) -> bool {
    data_records.iter().all(|record| {
        if !matches!(record.op.role(), crate::wal::types::WalOpRole::ValueWrite) {
            return false;
        }
        manifest
            .files
            .iter()
            .any(|file| file_covers_record(file, record))
    })
}

#[cfg(test)]
pub(crate) fn wal_record_covered_by_manifest(
    record: &crate::wal::WalRecord,
    manifest: &Manifest,
) -> bool {
    wal_record_covered_by_verified_manifest(record, manifest, &|_, _| true)
}

pub(crate) fn wal_record_covered_by_verified_manifest(
    record: &crate::wal::WalRecord,
    manifest: &Manifest,
    contains_record: &dyn Fn(&FileMeta, &crate::wal::WalRecord) -> bool,
) -> bool {
    use crate::wal::types::WalOpRole;

    let range_end = match record.op.role() {
        WalOpRole::ValueWrite => None,
        // An SST's key and sequence bounds do not prove that a particular
        // tombstone was included: a concurrent flush can publish unrelated
        // entries on both sides of it. Replaying deletes is conservative and
        // sequence-safe, whereas suppressing one can resurrect an older value.
        WalOpRole::PointDelete
        | WalOpRole::RangeDelete
        | WalOpRole::TransactionBegin
        | WalOpRole::TransactionCommit
        | WalOpRole::TransactionBatch => return false,
    };
    let coverage = DataCoverageRecord {
        cf_id: record.cf_id,
        op: record.op,
        key: record.key.to_vec(),
        value: record.value.as_ref().map(|value| value.to_vec()),
        expiration: record.expiration,
        range_end,
        seq: record.seq,
    };
    manifest
        .files
        .iter()
        .any(|file| file_covers_record(file, &coverage) && contains_record(file, record))
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

fn validate_manifest_sst_coverage_within(
    storage: &HybridStorage,
    manifest: &Manifest,
    candidates: &[ValidatedWalPruneCandidate],
    deadline: &crate::common::OperationDeadline,
) -> MidgeResult<(Vec<bool>, Vec<GuardedObjectProof>)> {
    let mut states = candidates
        .iter()
        .map(|candidate| {
            vec![ExactCoverageState::default(); candidate.validated.data_records.len()]
        })
        .collect::<Vec<_>>();
    let mut dependencies = Vec::new();

    for file in &manifest.files {
        let (reader, dependency) = if storage.ephemeral_sst_cache_enabled() {
            // Complete bounds exclude unrelated objects without cloud I/O.
            // Older manifests without complete bounds remain conservative.
            if file.key_bounds_complete
                && !candidates.iter().any(|candidate| {
                    candidate
                        .validated
                        .data_records
                        .iter()
                        .any(|record| file_may_contain_record_key(file, record))
                })
            {
                continue;
            }
            open_verified_remote_sst_ranges(storage, file, deadline)?
        } else {
            let proof = validate_remote_sst_within(storage, file, deadline)?;
            let reader = open_sst_reader_from_bytes(&file.name, proof.bytes())
                .map_err(MidgeError::Internal)?;
            (reader, storage.remote_identity_guard(&proof))
        };

        for (candidate, candidate_states) in candidates.iter().zip(&mut states) {
            for (record, state) in candidate
                .validated
                .data_records
                .iter()
                .zip(candidate_states)
            {
                if !file_may_contain_record_key(file, record) {
                    continue;
                }
                let mut exclusive_end = record.key.clone();
                exclusive_end.push(0);
                let raw_states = reader
                    .scan_range_raw_state(Some(&record.key), Some(&exclusive_end))
                    .map_err(|error| {
                        MidgeError::Internal(format!(
                            "read raw cloud SST state from '{}': {error}",
                            file.name
                        ))
                    })?;
                for (key, raw_state) in raw_states {
                    if key.as_ref() == record.key.as_slice() {
                        state.observe(raw_state);
                    }
                }
            }
        }

        dependencies.push(dependency);
    }

    let coverage = candidates
        .iter()
        .zip(&states)
        .map(|(candidate, states)| {
            wal_data_records_exactly_covered_by_manifest(&candidate.validated.data_records, states)
        })
        .collect();
    Ok((coverage, dependencies))
}

/// Retain the manifest's full-content CRC gate without retaining or staging a
/// whole object. Only SSTs relevant to this bounded WAL prune batch reach this
/// path; a pinned object identity spans the CRC, summary, and exact key reads.
fn open_verified_remote_sst_ranges(
    storage: &HybridStorage,
    file: &FileMeta,
    deadline: &crate::common::OperationDeadline,
) -> MidgeResult<(
    Box<dyn crate::sst::traits::SstReaderExt>,
    GuardedObjectProof,
)> {
    let key = crate::sst::object_key(&file.name);
    let metadata = storage.remote_range_metadata_within(&key, deadline)?;
    if file.size_bytes != 0 && file.size_bytes != metadata.size {
        return Err(MidgeError::Corruption(format!(
            "cloud SST '{}' size mismatch: manifest={}, object={}",
            file.name, file.size_bytes, metadata.size
        )));
    }
    let backend = storage.remote_sst_backend();
    let fs: Arc<dyn crate::io::Fs> = Arc::new(
        crate::storage::remote_sst::RemoteSstFs::for_object(
            Arc::new(crate::io::MockFs::new()),
            Arc::clone(&backend),
            key.clone(),
            metadata.clone(),
            storage.storage_io_timeout(),
        )
        .with_deadline(*deadline),
    );
    if let Some(expected_crc) = file.content_crc32c {
        let handle = fs.open(
            &crate::io::FsPath::new(&file.name),
            crate::io::OpenOptions {
                mode: crate::io::OpenMode::ReadOnly,
                create: false,
                create_new: false,
                truncate: false,
            },
        )?;
        let mut offset = 0;
        let mut crc = 0;
        while offset < metadata.size {
            let length = (metadata.size - offset).min(256 * 1024);
            let bytes = handle.read_at(offset, length)?;
            if bytes.len() as u64 != length {
                return Err(MidgeError::Corruption(format!(
                    "cloud SST '{}' CRC range was truncated",
                    file.name
                )));
            }
            crc = crc32c::crc32c_append(crc, &bytes);
            offset += length;
        }
        if crc != expected_crc {
            return Err(MidgeError::Corruption(format!(
                "cloud SST '{}' content crc32c {crc:08x} does not match manifest {expected_crc:08x}",
                file.name
            )));
        }
    }
    let summary = crate::sst::fs::SstFileIo::summarize_with_fs(&file.name, Arc::clone(&fs))?;
    verify_sst_summary_matches_manifest(&file.name, &summary, file)
        .map_err(MidgeError::Corruption)?;
    let reader = crate::sst::fs::SstFileIo::open(&file.name, fs)?;
    let dependency = GuardedObjectProof::range_identity(backend, key, metadata);
    Ok((Box::new(reader), dependency))
}

fn open_sst_reader_from_bytes(
    sst_name: &str,
    data: &[u8],
) -> Result<Box<dyn crate::sst::traits::SstReaderExt>, String> {
    let fs: Arc<dyn crate::io::Fs> = Arc::new(crate::io::MockFs::new());
    let path = crate::io::FsPath::new(sst_name);
    let mut file = fs
        .open(
            &path,
            crate::io::OpenOptions {
                mode: crate::io::OpenMode::ReadWrite,
                create: true,
                create_new: true,
                truncate: false,
            },
        )
        .map_err(|error| format!("stage cloud SST '{sst_name}' for exact coverage: {error}"))?;
    file.write_at(0, bytes::Bytes::copy_from_slice(data))
        .map_err(|error| format!("write cloud SST '{sst_name}' for exact coverage: {error}"))?;
    drop(file);

    let factory = crate::sst::FsSstFactoryIo::new(fs, 64 * 1024);
    factory
        .open(Path::new(sst_name))
        .map_err(|error| format!("open cloud SST '{sst_name}' for exact coverage: {error}"))
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
            op: crate::wal::WalOpKind::Put,
            key: b"b".to_vec(),
            value: Some(b"value".to_vec()),
            expiration: None,
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
    fn should_classify_individual_wal_record_coverage_from_manifest_proof() {
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
        let covered = crate::wal::WalRecord::new_cf(
            7,
            crate::wal::WalOpKind::Put,
            bytes::Bytes::from_static(b"b"),
            Some(bytes::Bytes::from_static(b"old")),
            12,
            1,
        );
        let outside_sequence = crate::wal::WalRecord {
            seq: 21,
            ..covered.clone()
        };
        let transaction_marker = crate::wal::WalRecord {
            op: crate::wal::WalOpKind::TxnBatch,
            ..covered.clone()
        };
        let point_tombstone = crate::wal::WalRecord {
            op: crate::wal::WalOpKind::Delete,
            value: None,
            ..covered.clone()
        };

        // Act
        let covered_result = wal_record_covered_by_manifest(&covered, &manifest);
        let outside_result = wal_record_covered_by_manifest(&outside_sequence, &manifest);
        let marker_result = wal_record_covered_by_manifest(&transaction_marker, &manifest);
        let tombstone_result = wal_record_covered_by_manifest(&point_tombstone, &manifest);
        let unverified_result =
            wal_record_covered_by_verified_manifest(&covered, &manifest, &|_, _| false);

        // Assert
        assert!(covered_result);
        assert!(!outside_result);
        assert!(!marker_result);
        assert!(!tombstone_result);
        assert!(!unverified_result);
    }

    #[test]
    fn should_not_treat_manifest_bounds_as_exact_value_coverage() {
        // Arrange: a concurrent flush can place unrelated entries on both
        // sides of this WAL write without persisting the write itself.
        let manifest = Manifest {
            files: vec![FileMeta {
                cf_id: 7,
                smallest_key: Some(b"a".to_vec()),
                largest_key: Some(b"z".to_vec()),
                smallest_seq: Some(10),
                largest_seq: Some(20),
                ..FileMeta::default()
            }],
            ..Manifest::default()
        };
        let overwrite = crate::wal::WalRecord::new_cf(
            7,
            crate::wal::WalOpKind::Put,
            bytes::Bytes::from_static(b"target"),
            Some(bytes::Bytes::from_static(b"new")),
            15,
            1,
        );

        // Act
        let covered = wal_record_covered_by_verified_manifest(&overwrite, &manifest, &|_, _| false);

        // Assert
        assert!(!covered, "bounds alone cannot prove exact value coverage");
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
            op: crate::wal::WalOpKind::DeleteRange,
            key: b"c".to_vec(),
            value: None,
            expiration: None,
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
