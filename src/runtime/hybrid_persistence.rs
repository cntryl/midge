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
#[cfg(test)]
use std::io::Write as _;
use std::path::Path;
use std::sync::Arc;

mod catalog;
mod metadata_snapshot;
pub(crate) use metadata_snapshot::{conditional_metadata_mirror_put, CloudMetadataPruneSnapshot};
mod streaming_prune;
use crate::storage::hybrid::backend::ControlObject;
use catalog::{commit_catalog_within, load_and_repair_catalog_within, AdmittedCatalog};

#[derive(Clone, Default)]
pub(crate) struct CloudWalPruneProgress(Arc<parking_lot::Mutex<streaming_prune::Progress>>);

impl CloudWalPruneProgress {
    pub(crate) fn discard_idle_proofs(&self) {
        if let Some(mut progress) = self.0.try_lock() {
            progress.discard_proofs();
        }
    }

    /// Sample after the proof worker has released its publication turn. An
    /// unexpected active proof must defer compaction rather than block runtime.
    pub(crate) fn retained_bytes(&self) -> Option<usize> {
        self.0.try_lock().map(|progress| progress.retained_bytes())
    }
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub(crate) struct CloudMetadataPruneProof {
    pub(crate) key: String,
    pub(crate) expected_bytes: Vec<u8>,
    pub(crate) remote: StorageObjectMetadata,
}

#[derive(Clone)]
pub(crate) struct CloudMetadataPruneGuard {
    objects: Vec<GuardedObjectProof>,
    memory: Option<Arc<crate::common::resource_budget::ResourceReservation>>,
}

impl CloudMetadataPruneGuard {
    #[cfg(test)]
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
        Self {
            objects,
            memory: None,
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct CloudWalPruneGuard {
    manifest: Arc<Manifest>,
    metadata: Option<CloudMetadataPruneGuard>,
    manifest_memory: Option<Arc<crate::common::resource_budget::ResourceReservation>>,
    memory_limit: Option<usize>,
    progress: CloudWalPruneProgress,
    work_quantum: Option<std::time::Duration>,
}

impl CloudWalPruneGuard {
    pub(crate) fn new(
        manifest: impl Into<Arc<Manifest>>,
        metadata: Option<CloudMetadataPruneGuard>,
    ) -> Self {
        Self {
            manifest: manifest.into(),
            manifest_memory: metadata
                .as_ref()
                .and_then(|metadata| metadata.memory.clone()),
            metadata,
            memory_limit: None,
            progress: CloudWalPruneProgress::default(),
            work_quantum: None,
        }
    }

    pub(crate) fn admitted_local_snapshot(
        manifest: &Manifest,
        budget: &crate::common::resource_budget::ResourceBudget,
        progress: &CloudWalPruneProgress,
    ) -> MidgeResult<Self> {
        if let Some(cached) = progress.0.lock().snapshot(budget) {
            // Local simulation uses only SST coverage from this snapshot.
            if cached.metadata.is_none()
                && cached.manifest.files.len() == manifest.files.len()
                && cached
                    .manifest
                    .files
                    .iter()
                    .zip(&manifest.files)
                    .all(|(old, new)| streaming_prune::progress::same_file(old, new))
            {
                return Ok(cached);
            }
        }
        // A changed authority cannot reuse semantic work. Release its snapshot
        // before admitting the replacement, including under tight budgets.
        progress.discard_idle_proofs();
        let mut count = crate::common::resource_budget::ByteCounter::default();
        serde_json::to_writer(&mut count, manifest)
            .map_err(|error| MidgeError::Internal(error.to_string()))?;
        let memory = Arc::new(budget.reserve(
            count.0.saturating_mul(16).saturating_add(4096),
            "WAL cleanup manifest clone",
        )?);
        let mut guard = Self::new(manifest.clone(), None);
        guard.manifest_memory = Some(memory);
        Ok(guard)
    }

    pub(crate) fn with_progress(mut self, progress: CloudWalPruneProgress) -> Self {
        self.progress = progress;
        self
    }

    pub(crate) fn progress(&self) -> CloudWalPruneProgress {
        self.progress.clone()
    }

    /// Limit callerless proof work between successful resumable checkpoints.
    /// Provider callbacks retain the enclosing operation's separate deadline.
    pub(crate) fn with_work_quantum(mut self, quantum: std::time::Duration) -> Self {
        self.work_quantum = Some(quantum);
        self
    }

    pub(crate) fn work_quantum(&self) -> Option<std::time::Duration> {
        self.work_quantum
    }

    pub(crate) fn with_memory_limit(mut self, memory_limit: usize) -> Self {
        self.memory_limit = Some(memory_limit);
        self
    }

    pub(crate) fn memory_limit(&self) -> usize {
        self.memory_limit
            .unwrap_or(crate::compaction::DEFAULT_COMPACTION_MEMORY_LIMIT)
    }
}

struct ValidatedWalObject {
    proof: RemoteObjectProof,
}

struct ValidatedWalPruneCandidate {
    segment_id: u64,
    entry: PublishedWalSegment,
    validated: ValidatedWalObject,
}

#[derive(Clone, Default)]
struct ExactCoverageState {
    state: Option<crate::types::KeyState>,
    ambiguous: bool,
    /// SST range tombstones at or above a range-delete record's sequence,
    /// clipped to the record's range.
    range_cover: Vec<(Vec<u8>, Vec<u8>)>,
}

pub(crate) type CloudWalPruneBatchResults = Vec<(u64, MidgeResult<()>)>;

fn sorted_cloud_wal_prune_results(
    mut results: CloudWalPruneBatchResults,
) -> CloudWalPruneBatchResults {
    results.sort_by_key(|(segment_id, _)| *segment_id);
    results
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
                crate::storage::StorageOutcome::Err(
                    format!(
                        "catalog authority retired but physical delete was not admitted: {error}"
                    )
                    .into(),
                ),
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

/// Runtime-owned format operations layered over raw hybrid object I/O.
///
/// `HybridStorage` stays format-neutral: it moves keyed bytes and enforces
/// budgets. `CloudPersistence` owns everything above that - WAL catalog
/// authority, manifest coverage, and SST publication proofs - so the format
/// stack has exactly one owner instead of an extension trait bolted onto the
/// storage type.
///
/// The `Deref` is the composition edge, not a convenience: every format
/// operation below is expressed in terms of the raw object I/O it wraps, and
/// callers that already hold this handle read and write plain objects through
/// the same value.
pub(crate) struct CloudPersistence {
    storage: Arc<HybridStorage>,
}

impl CloudPersistence {
    pub(crate) fn new(storage: Arc<HybridStorage>) -> Self {
        Self { storage }
    }
}

impl std::ops::Deref for CloudPersistence {
    type Target = HybridStorage;

    fn deref(&self) -> &HybridStorage {
        &self.storage
    }
}

#[cfg(test)]
fn validate_remote_sst_within(
    storage: &HybridStorage,
    file: &FileMeta,
    deadline: &crate::common::OperationDeadline,
) -> MidgeResult<RemoteObjectProof> {
    let key = crate::cloud_layout::object_key(&file.name);
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

#[cfg(test)]
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

impl CloudPersistence {
    pub(crate) fn enqueue_wal_segment(
        &self,
        segment_id: u64,
        local_path: &Path,
        max_sequence: u64,
    ) -> MidgeResult<String> {
        let bytes = std::fs::read(local_path).map_err(MidgeError::Io)?;
        let readback = crate::wal::cloud_segment::validate_bytes_with_coverage(
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

    pub(crate) fn fence_cloud_wal_catalog(
        &self,
        writer_epoch: u64,
    ) -> MidgeResult<AdmittedCatalog> {
        let deadline = crate::common::OperationDeadline::unbounded();
        let existing = load_and_repair_catalog_within(self, &deadline)?;
        let (mut catalog, expected) = if let Some(authority) = existing {
            (authority.catalog, Some(authority.primary))
        } else {
            (AdmittedCatalog::empty(self, writer_epoch)?, None)
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

    pub(crate) fn publish_remote_wal_segment(
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
            local_readback.writer_epoch,
            &local_bytes,
        );
        let remote = validate_remote_wal(self, &entry, deadline)?;
        if remote.proof.bytes() != local_bytes {
            return Err(MidgeError::Internal(format!(
                "cloud WAL segment {segment_id} does not match the locally sealed bytes for writer epoch {}",
                local_readback.writer_epoch
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

    pub(crate) fn prune_cloud_wal_segments_within(
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
        let streaming_prune::StreamedValidation {
            mut results,
            candidates: validated_candidates,
            coverage,
            mut dependencies,
            reservations: _proof_reservations,
        } = streaming_prune::validate(self, &ordered_candidates, &catalog, &guard, deadline)?;
        let covered_candidates =
            partition_exactly_covered_wal_candidates(validated_candidates, coverage, &mut results);

        if covered_candidates.is_empty() {
            return Ok(sorted_cloud_wal_prune_results(results));
        }

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
        guard.progress.0.lock().after_retirement(&retired);
        schedule_retired_wal_deletes(self, retired);
        Ok(sorted_cloud_wal_prune_results(results))
    }

    pub(crate) fn delete_sst_object_blocking(&self, sst_name: &str) -> MidgeResult<()> {
        crate::failpoints::fail_point!("midge::cloud::inject_fail_sst_delete", |_| Err(
            MidgeError::Internal("failpoint: cloud SST delete failed".to_string())
        ));
        self.delete_immutable_object_blocking(&crate::cloud_layout::object_key(sst_name))
    }
}

#[cfg(test)]
fn authoritative_wal_entry(
    storage: &HybridStorage,
    segment_id: u64,
) -> Result<(ControlObject, PublishedWalSegment), String> {
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
) -> MidgeResult<(ControlObject, PublishedWalSegment)> {
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
) -> MidgeResult<(ControlObject, AdmittedCatalog)> {
    let _catalog_mutation =
        storage.lock_wal_catalog_mutation_within(deadline, "cloud WAL catalog read")?;
    let authority = load_and_repair_catalog_within(storage, deadline)?.ok_or_else(|| {
        MidgeError::Internal("cloud WAL publication catalog is missing".to_string())
    })?;
    Ok((authority.primary, authority.catalog))
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
    crate::wal::cloud_segment::validate_bytes(&entry.object_key, proof.bytes(), entry.max_sequence)
        .map_err(MidgeError::Internal)?;
    Ok(ValidatedWalObject { proof })
}

fn contextualize_cloud_error(error: MidgeError, context: &str) -> MidgeError {
    match error {
        MidgeError::Timeout(message) => MidgeError::Timeout(format!("{context}: {message}")),
        MidgeError::ResourceLimit(message) => {
            MidgeError::ResourceLimit(format!("{context}: {message}"))
        }
        other => MidgeError::Internal(format!("{context}: {other}")),
    }
}

fn exact_state_sequence(state: &crate::types::KeyState) -> Option<u64> {
    match state {
        crate::types::KeyState::Absent => None,
        crate::types::KeyState::Tombstone(sequence)
        | crate::types::KeyState::Value(_, sequence, _, _) => Some(*sequence),
    }
}

impl ExactCoverageState {
    fn observe(&mut self, state: crate::types::KeyState) {
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

    /// Record an SST range tombstone for a range-delete record. Only a
    /// tombstone at or above the record's sequence can stand in for it.
    fn observe_range_tombstone(
        &mut self,
        tombstone: &crate::types::RangeTombstone,
        record: &DataCoverageRecord,
    ) {
        let Some(range_end) = record.range_end.as_deref() else {
            return;
        };
        if tombstone.seq < record.seq {
            return;
        }
        let start = tombstone.start.as_slice().max(record.key.as_slice());
        let end = tombstone.end.as_slice().min(range_end);
        if start < end {
            self.range_cover.push((start.to_vec(), end.to_vec()));
        }
    }

    /// Whether the observed tombstones cover `[start, end)` with no gap. An
    /// empty or inverted range is malformed, so it is never treated as proven.
    fn range_covered(&self, start: &[u8], end: &[u8]) -> bool {
        if start >= end {
            return false;
        }
        let mut intervals: Vec<_> = self.range_cover.iter().collect();
        intervals.sort_by(|left, right| left.0.cmp(&right.0));
        let mut covered_to = start;
        for (interval_start, interval_end) in intervals {
            if interval_start.as_slice() > covered_to {
                return false;
            }
            covered_to = covered_to.max(interval_end.as_slice());
            if covered_to >= end {
                return true;
            }
        }
        covered_to >= end
    }

    fn exactly_covers(&self, record: &DataCoverageRecord) -> bool {
        use crate::types::KeyState;
        use crate::wal::types::WalOpRole;

        if self.ambiguous {
            return false;
        }
        if matches!(record.op.role(), WalOpRole::RangeDelete) {
            return record
                .range_end
                .as_deref()
                .is_some_and(|end| self.range_covered(&record.key, end));
        }
        match self.state.as_ref() {
            Some(KeyState::Value(value, sequence, expiration, op_type)) => {
                *sequence > record.seq
                    || *sequence == record.seq
                        && matches!(record.op.role(), WalOpRole::ValueWrite)
                        && record.value.as_deref() == Some(value.as_ref())
                        && record.expiration == *expiration
                        && op_type.is_value_write()
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

    fn state_for(&self, file: &FileMeta, key: &[u8]) -> Option<crate::types::KeyState> {
        let mut readers = self.readers.borrow_mut();
        let reader = readers.entry(file.name.clone()).or_insert_with(|| {
            let name = crate::cloud_layout::PersistedSstName::parse(&file.name).ok()?;
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
            crate::types::KeyState::Value(value, sequence, _, _) => {
                sequence > record.seq
                    || sequence == record.seq
                        && record
                            .value
                            .as_ref()
                            .is_some_and(|expected| expected == &value)
            }
            // A newer tombstone supersedes a value write. At the same sequence a
            // value write and a delete contradict each other, so only a delete
            // record is covered by it; skipping the value on that evidence could
            // lose data, and replaying it is the safe direction.
            crate::types::KeyState::Tombstone(sequence) => {
                if matches!(record.op.role(), crate::wal::types::WalOpRole::ValueWrite) {
                    sequence > record.seq
                } else {
                    sequence >= record.seq
                }
            }
            crate::types::KeyState::Absent => false,
        }
    }
}

/// Whether `file` may hold range tombstones overlapping a range-delete record.
/// Files without trustworthy key bounds are always consulted.
fn file_may_overlap_record_range(file: &FileMeta, record: &DataCoverageRecord) -> bool {
    if file.cf_id != record.cf_id {
        return false;
    }
    let Some(range_end) = record.range_end.as_deref() else {
        return false;
    };
    let (Some(smallest_key), Some(largest_key)) =
        (file.smallest_key.as_ref(), file.largest_key.as_ref())
    else {
        return !file.key_bounds_complete;
    };
    !file.key_bounds_complete
        || smallest_key.as_slice() < range_end && record.key.as_slice() <= largest_key.as_slice()
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

fn verify_sst_summary_matches_manifest(
    sst_name: &str,
    summary: &crate::sst::fs::SstFileSummary,
    file: &FileMeta,
) -> Result<(), String> {
    // The CRC is proven separately by the streaming pass, so this compares
    // only what the decoded summary can attest to.
    crate::sst::identity::verify_summary_against(
        summary,
        file.expected_sst(),
        crate::sst::identity::ProofPolicy::Legacy,
    )
    .map_err(|mismatch| format!("cloud SST '{sst_name}': {mismatch}"))
}

#[cfg(test)]
mod tests;
