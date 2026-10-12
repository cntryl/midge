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
use crate::wal::cloud_catalog::{CatalogEntry, WalPublicationCatalog};
use crate::wal::cloud_segment::DataCoverageRecord;
use std::path::Path;
use std::sync::Arc;

mod catalog;
mod metadata_snapshot;
pub(crate) use metadata_snapshot::{
    mirror_control_metadata_within, CloudMetadataMirrorAuthority, CloudMetadataMirrorContext,
    CloudMetadataPruneSnapshot,
};
mod streaming_prune;
use crate::storage::hybrid::backend::ControlObject;
pub(crate) use catalog::refusal_error;
pub(crate) use catalog::AdmittedCatalog;
use catalog::{load_and_repair_catalog_within, mutate_wal_catalog_within, Missing, Mutation};

/// Why a WAL upload could not be verified or published (#841).
#[derive(Debug)]
pub(crate) enum PublicationError {
    /// The object under `segment_id`'s key holds other bytes than the
    /// sealed segment. Objects are never overwritten, so no retry can fix it.
    Mismatch { segment_id: u64, detail: String },
    /// The catalog refuses the run: publishing it would break the catalog's
    /// order or identity. No retry can fix it.
    Refused(crate::wal::publish::Refusal),
    /// Another writer epoch owns the catalog.
    Fenced(MidgeError),
    /// A failure a later attempt may not repeat: I/O, a timeout, a lost
    /// compare-and-swap race, memory pressure.
    Retriable(MidgeError),
}

impl std::fmt::Display for PublicationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mismatch { detail, .. } => f.write_str(detail),
            Self::Refused(refusal) => write!(f, "cloud WAL publication refused: {refusal}"),
            Self::Fenced(error) | Self::Retriable(error) => write!(f, "{error}"),
        }
    }
}

impl From<MidgeError> for PublicationError {
    fn from(error: MidgeError) -> Self {
        match error {
            error @ MidgeError::Fenced(_) => Self::Fenced(error),
            error => Self::Retriable(error),
        }
    }
}

impl From<crate::wal::publish::Refusal> for PublicationError {
    fn from(refusal: crate::wal::publish::Refusal) -> Self {
        match refusal {
            crate::wal::publish::Refusal::EpochMismatch { .. } => {
                Self::Fenced(MidgeError::Fenced(refusal.to_string()))
            }
            refusal => Self::Refused(refusal),
        }
    }
}

impl From<PublicationError> for MidgeError {
    /// The engine error a caller waiting on the upload sees.
    fn from(error: PublicationError) -> Self {
        match error {
            PublicationError::Mismatch { detail, .. } => MidgeError::Corruption(detail),
            PublicationError::Refused(refusal) => refusal.into(),
            PublicationError::Fenced(error) | PublicationError::Retriable(error) => error,
        }
    }
}

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
    authority: Option<CloudMetadataAuthorityProof>,
}

#[derive(Clone)]
struct CloudMetadataAuthorityProof {
    store: Arc<dyn crate::lease::LeaderStore>,
    holder_id: String,
    writer_epoch: u64,
    generation: crate::lease::CloudMetadataGeneration,
}

impl CloudMetadataAuthorityProof {
    fn verify_current(&self, deadline: &crate::common::OperationDeadline) -> MidgeResult<()> {
        self.store
            .validate_epoch_with_timeout(&self.holder_id, self.writer_epoch, deadline.remaining())
            .map_err(|error| error.into_validation_error("cloud WAL prune lease check"))?;
        match self
            .store
            .read_committed_metadata(deadline.remaining())
            .map_err(|error| error.into_validation_error("cloud WAL prune metadata pointer"))?
        {
            crate::lease::CloudMetadataHead::Committed(current) if current == self.generation => {
                Ok(())
            }
            _ => Err(MidgeError::Busy(
                "cloud metadata pointer changed during WAL cleanup proof".into(),
            )),
        }
    }
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
            authority: None,
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
    entry: CatalogEntry,
    validated: ValidatedWalObject,
}

/// The exact-coverage rule: whether the SST versions observed for a WAL
/// record's key prove that record durable. Every coverage decision (cloud and
/// local WAL pruning, local recovery and cloud startup replay) folds its
/// observations through this one type, so the rule cannot drift between them.
#[derive(Clone, Default)]
pub(crate) struct ExactCoverageState {
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

/// Retire `covered_candidates` from the catalog in one compare-and-swap
/// ([`WalPublicationCatalog::retire_prefix`]): only the catalog's oldest
/// segments retire, each exactly as validated. Returns the candidates it
/// retired; every other candidate's outcome is in `results`.
fn retire_covered_wal_catalog_prefix_within(
    storage: &HybridStorage,
    covered_candidates: Vec<ValidatedWalPruneCandidate>,
    fencing_epoch: u64,
    deadline: &crate::common::OperationDeadline,
    results: &mut CloudWalPruneBatchResults,
) -> MidgeResult<Vec<ValidatedWalPruneCandidate>> {
    let expected: Vec<CatalogEntry> = covered_candidates
        .iter()
        .map(|candidate| candidate.entry.clone())
        .collect();
    let retirement = mutate_wal_catalog_within(
        storage,
        "cloud WAL retirement",
        deadline,
        &|| storage.check_write_authority(),
        Missing::Refuse,
        |catalog| {
            catalog
                .retire_prefix(fencing_epoch, &expected)
                .map(|retired| Mutation::of(!retired.is_empty(), retired))
        },
    );
    let retired_ids = match retirement {
        Ok(Ok((retired, _))) => retired,
        Ok(Err(refusal)) => {
            let error = refusal_error(refusal);
            let error = match error {
                error @ MidgeError::Fenced(_) => error,
                // The catalog moved since validation; a later batch retries.
                error => MidgeError::Busy(error.to_string()),
            };
            for candidate in covered_candidates {
                results.push((candidate.segment_id, Err(error.replay())));
            }
            return Ok(Vec::new());
        }
        Err(error @ MidgeError::Fenced(_)) => return Err(error),
        Err(error) => {
            let message =
                contextualize_cloud_error(error, "cloud WAL catalog batch retirement failed")
                    .to_string();
            for candidate in covered_candidates {
                results.push((
                    candidate.segment_id,
                    Err(MidgeError::Internal(message.clone())),
                ));
            }
            return Ok(Vec::new());
        }
    };
    let mut retired = Vec::with_capacity(retired_ids.len());
    for candidate in covered_candidates {
        if retired_ids.contains(&candidate.segment_id) {
            retired.push(candidate);
        } else {
            // The catalog already retired it: a lost answer to an earlier
            // compare-and-swap.
            results.push((candidate.segment_id, Ok(())));
        }
    }
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

    let fs: Arc<dyn crate::io::Fs> = Arc::new(crate::io::MockFs::new());
    let verifier = crate::io::FsPath::new("verify.sst");
    fs.open(
        &verifier,
        crate::io::OpenOptions {
            mode: crate::io::OpenMode::ReadWrite,
            create: true,
            create_new: true,
            truncate: false,
        },
    )
    .and_then(|mut file| file.append(bytes::Bytes::copy_from_slice(data)))
    .map_err(|error| format!("stage SST verifier for '{sst_name}': {error}"))?;

    let summary = crate::sst::fs::SstFileIo::summarize_with_fs(&verifier.0, fs)
        .map_err(|error| format!("cloud SST '{sst_name}' summary validation: {error}"))?;
    if let Some(expected_file) = expected_file {
        verify_sst_summary_matches_manifest(sst_name, &summary, expected_file)?;
    }
    Ok(summary)
}

impl CloudPersistence {
    /// Validate the local segment at `source` and queue it for upload.
    pub(crate) fn enqueue_wal_segment(
        &self,
        source: &crate::io::FsLocation,
        segment_id: u64,
        max_sequence: u64,
    ) -> MidgeResult<String> {
        let bytes = source
            .read_all()
            .map_err(|error| MidgeError::Io(error.into_io()))?;
        let readback = crate::wal::cloud_segment::validate_bytes_with_coverage(
            &source.to_string(),
            &bytes,
            max_sequence,
        )
        .map_err(MidgeError::Corruption)?;
        // The object is named for the epoch of the writer that wrote it,
        // which its header records (#843).
        let entry = CatalogEntry::describing(readback.validation.identity(), &bytes);
        if entry.id() != segment_id {
            return Err(MidgeError::Corruption(format!(
                "local WAL segment '{source}' holds segment {}, not {segment_id}",
                entry.id()
            )));
        }
        let object_key = entry.object_key();
        self.enqueue_object_upload(segment_id, object_key.clone(), source.clone(), max_sequence)?;
        Ok(object_key)
    }

    pub(crate) fn fence_cloud_wal_catalog(
        &self,
        writer_epoch: u64,
    ) -> MidgeResult<AdmittedCatalog> {
        self.fence_cloud_wal_catalog_within(
            writer_epoch,
            &crate::common::OperationDeadline::unbounded(),
        )
    }

    pub(crate) fn fence_cloud_wal_catalog_within(
        &self,
        writer_epoch: u64,
        deadline: &crate::common::OperationDeadline,
    ) -> MidgeResult<AdmittedCatalog> {
        mutate_wal_catalog_within(
            self,
            "cloud WAL catalog fencing",
            deadline,
            &|| self.check_write_authority(),
            Missing::Create(writer_epoch),
            |catalog| {
                catalog
                    .fence_to(writer_epoch)
                    .map(|changed| Mutation::of(changed, ()))
            },
        )?
        .map(|((), catalog)| catalog)
        .map_err(refusal_error)
    }

    /// Salvage (#844): in one catalog compare-and-swap, set aside the
    /// cataloged segments from `from`, which must be `expected` as planning
    /// saw them, and raise the sequence floor to at least `floor`, so no
    /// later open replays them or reuses their sequences. Their objects stay
    /// in place. `validate` is the writer's authority check.
    ///
    /// # Errors
    ///
    /// `Fenced` when another epoch owns the catalog, `Corruption` when the
    /// catalog's suffix is not what planning saw, and the load or commit
    /// error.
    pub(crate) fn set_aside_catalog_suffix_with_authority_within(
        &self,
        writer_epoch: u64,
        from: Option<crate::wal::ledger::SegmentId>,
        expected: &[CatalogEntry],
        floor: u64,
        validate: &dyn Fn() -> MidgeResult<()>,
        deadline: &crate::common::OperationDeadline,
    ) -> MidgeResult<()> {
        mutate_wal_catalog_within(
            self,
            "cloud WAL salvage",
            deadline,
            validate,
            Missing::Refuse,
            |catalog| {
                catalog
                    .set_aside_suffix(writer_epoch, from, expected, floor)
                    .map(|changed| Mutation::of(changed, ()))
            },
        )?
        .map(drop)
        .map_err(refusal_error)
    }

    /// Prove that the remote object for the sealed segment at `source` holds
    /// exactly its local bytes, and describe it for publication.
    ///
    /// This only records an upload; publishing it is
    /// [`Self::publish_wal_segments`]'s job, once every earlier segment is
    /// uploaded too. An existing object with the same bytes counts as this
    /// upload.
    ///
    /// # Errors
    ///
    /// Returns [`PublicationError::Mismatch`] when the object holds other
    /// bytes: objects are immutable, so it is never overwritten.
    pub(crate) fn verify_uploaded_wal_segment(
        &self,
        segment_id: u64,
        expected_max_sequence: u64,
        source: &crate::io::FsLocation,
        deadline: &crate::common::OperationDeadline,
    ) -> Result<CatalogEntry, PublicationError> {
        let local_bytes = source.read_all().map_err(|error| {
            let error = error.into_io();
            MidgeError::Io(std::io::Error::new(
                error.kind(),
                format!(
                    "failed to read local WAL segment '{source}' before cloud acknowledgement: {error}"
                ),
            ))
        })?;
        let local_readback = crate::wal::cloud_segment::validate_bytes(
            &source.to_string(),
            &local_bytes,
            expected_max_sequence,
        )
        .map_err(MidgeError::Internal)?;
        let entry = CatalogEntry::describing(local_readback.identity(), &local_bytes);
        if entry.id() != segment_id {
            return Err(PublicationError::Mismatch {
                segment_id,
                detail: format!(
                    "local WAL segment '{source}' holds segment {}, not {segment_id}",
                    entry.id()
                ),
            });
        }
        let remote = validate_remote_wal(self, &entry, deadline)?;
        if remote.proof.bytes() != local_bytes {
            return Err(PublicationError::Mismatch {
                segment_id,
                detail: format!(
                    "cloud WAL segment {segment_id} does not match the locally sealed bytes for writer epoch {}",
                    local_readback.writer_epoch
                ),
            });
        }
        Ok(entry)
    }

    /// Publish `run`, verified uploads oldest first, in one catalog
    /// compare-and-swap.
    ///
    /// The caller selects `run` as the contiguous uploads its writer owes the
    /// catalog ([`crate::wal::publish::uploaded_run`]); the catalog appends
    /// it past its head ([`WalPublicationCatalog::append_run`]). Segments it
    /// already publishes identically add nothing.
    ///
    /// # Errors
    ///
    /// Returns [`PublicationError::Fenced`] when another writer epoch owns
    /// the catalog, [`PublicationError::Refused`] when the run would break
    /// the catalog's order or identity, and [`PublicationError::Retriable`]
    /// for a failure a later attempt may not repeat.
    pub(crate) fn publish_wal_segments(
        &self,
        run: &[CatalogEntry],
        fencing_epoch: u64,
        deadline: &crate::common::OperationDeadline,
    ) -> Result<(), PublicationError> {
        let publication = mutate_wal_catalog_within(
            self,
            "cloud WAL publication",
            deadline,
            &|| self.check_write_authority(),
            Missing::Refuse,
            |catalog| {
                catalog
                    .append_run(fencing_epoch, run)
                    .map(|changed| Mutation::of(changed, ()))
            },
        );
        match publication {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(refusal)) => Err(refusal.into()),
            Err(error @ MidgeError::Fenced(_)) => Err(PublicationError::Fenced(error)),
            Err(error) => Err(PublicationError::Retriable(contextualize_cloud_error(
                error,
                "cloud WAL catalog publication failed",
            ))),
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
        if let Err(refusal) =
            catalog.oldest_prefix(ordered_candidates.iter().map(|(segment_id, _)| *segment_id))
        {
            return Ok(ordered_candidates
                .into_iter()
                .map(|(segment_id, _)| {
                    // Only a segment at or below the anchor retired; one the
                    // catalog no longer lists above it was set aside.
                    if segment_id <= catalog.anchor().id {
                        (segment_id, Ok(()))
                    } else {
                        (segment_id, Err(MidgeError::Busy(refusal.to_string())))
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

        let metadata_authority = guard
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.authority.clone());
        if let Some(metadata) = guard.metadata {
            dependencies.extend(metadata.objects);
        }

        // Deterministic proof/publication boundary used to verify that an SST
        // identity change after semantic validation cannot retire WAL
        // authority. Dependencies are revalidated before the catalog CAS;
        // post-CAS cleanup needs only the target's conditional identity.
        crate::failpoints::fail_point!("midge::cloud::after_wal_prune_dependency_validation");

        if let Some(authority) = metadata_authority {
            authority.verify_current(deadline)?;
        }

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
) -> Result<(ControlObject, CatalogEntry), String> {
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
) -> MidgeResult<(ControlObject, CatalogEntry)> {
    let (proof, catalog) = authoritative_wal_catalog_within(storage, deadline)?;
    let entry = catalog.entry(segment_id).cloned().ok_or_else(|| {
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
    entry: &CatalogEntry,
    deadline: &crate::common::OperationDeadline,
) -> Result<ValidatedWalObject, PublicationError> {
    let proof = storage.remote_object_proof_within(&entry.object_key(), deadline)?;
    // The object exists but holds other bytes than the segment it names.
    entry
        .validate_bytes(proof.bytes())
        .map_err(|detail| PublicationError::Mismatch {
            segment_id: entry.id(),
            detail,
        })?;
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

#[derive(Clone, Copy)]
struct ExactCoverageRecordRef<'a> {
    op: crate::wal::types::WalOpKind,
    key: &'a [u8],
    value: Option<&'a [u8]>,
    expiration: Option<u64>,
    range_end: Option<&'a [u8]>,
    seq: u64,
}

impl<'a> From<&'a DataCoverageRecord> for ExactCoverageRecordRef<'a> {
    fn from(record: &'a DataCoverageRecord) -> Self {
        Self {
            op: record.op,
            key: &record.key,
            value: record.value.as_deref(),
            expiration: record.expiration,
            range_end: record.range_end.as_deref(),
            seq: record.seq,
        }
    }
}

impl<'a> From<&'a crate::wal::WalRecord> for ExactCoverageRecordRef<'a> {
    fn from(record: &'a crate::wal::WalRecord) -> Self {
        Self {
            op: record.op,
            key: record.key.as_ref(),
            value: record.value.as_deref(),
            expiration: record.expiration,
            range_end: None,
            seq: record.seq,
        }
    }
}

#[cfg(test)]
mod exact_coverage_record_ref_tests {
    use super::ExactCoverageRecordRef;
    use bytes::Bytes;

    #[test]
    fn should_borrow_wal_payload_when_building_exact_coverage_view() {
        // Arrange
        let record = crate::wal::WalRecord::new(
            crate::wal::WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from(vec![7; 1024 * 1024])),
            9,
        );

        // Act
        let borrowed = ExactCoverageRecordRef::from(&record);

        // Assert
        assert_eq!(borrowed.key.as_ptr(), record.key.as_ptr());
        let value = record.value.as_ref().expect("WAL put payload");
        let borrowed_value = borrowed.value.expect("borrowed payload");
        assert_eq!(borrowed_value.len(), value.len());
        assert_eq!(borrowed_value.as_ptr(), value.as_ptr());
    }
}

impl ExactCoverageState {
    /// Whether observing `state` would make it the newest observed version.
    pub(crate) fn supersedes(&self, state: &crate::types::KeyState) -> bool {
        exact_state_sequence(state).is_some_and(|sequence| {
            self.state
                .as_ref()
                .and_then(exact_state_sequence)
                .is_none_or(|current| sequence > current)
        })
    }

    /// Fold one raw, clock-free SST state for the record's key into the
    /// proof. Returns whether `state` became the newest observed version, so
    /// a caller that budgets the retained value knows when to charge it.
    pub(crate) fn observe(&mut self, state: crate::types::KeyState) -> bool {
        let Some(sequence) = exact_state_sequence(&state) else {
            return false;
        };
        let Some(current) = self.state.as_ref() else {
            self.state = Some(state);
            return true;
        };
        let current_sequence = exact_state_sequence(current).unwrap_or_default();
        match sequence.cmp(&current_sequence) {
            std::cmp::Ordering::Greater => {
                self.state = Some(state);
                self.ambiguous = false;
                true
            }
            std::cmp::Ordering::Equal
                if crate::types::resolve_same_sequence(
                    crate::types::VersionContent::from_state(current).expect("present state"),
                    crate::types::VersionContent::from_state(&state).expect("present state"),
                )
                .is_err() =>
            {
                self.ambiguous = true;
                false
            }
            std::cmp::Ordering::Equal | std::cmp::Ordering::Less => false,
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

    pub(crate) fn exactly_covers(&self, record: &DataCoverageRecord) -> bool {
        self.exactly_covers_ref(record.into())
    }

    /// Check a point WAL record without allocating a second key or value.
    pub(crate) fn exactly_covers_wal_point(&self, record: &crate::wal::WalRecord) -> bool {
        self.exactly_covers_ref(record.into())
    }

    fn exactly_covers_ref(&self, record: ExactCoverageRecordRef<'_>) -> bool {
        use crate::types::KeyState;
        use crate::wal::types::WalOpRole;

        if self.ambiguous {
            return false;
        }
        if matches!(record.op.role(), WalOpRole::RangeDelete) {
            return record
                .range_end
                .is_some_and(|end| self.range_covered(record.key, end));
        }
        match self.state.as_ref() {
            Some(KeyState::Value(value, sequence, expiration, op_type)) => {
                *sequence > record.seq
                    || *sequence == record.seq
                        && matches!(record.op.role(), WalOpRole::ValueWrite)
                        && op_type.is_value_write()
                        && crate::types::resolve_same_sequence(
                            crate::types::VersionContent {
                                is_tombstone: false,
                                value: Some(value.as_ref()),
                                expiration: *expiration,
                            },
                            crate::types::VersionContent {
                                is_tombstone: false,
                                value: record.value,
                                expiration: record.expiration,
                            },
                        )
                        .is_ok()
            }
            Some(KeyState::Tombstone(sequence)) => {
                *sequence > record.seq
                    || *sequence == record.seq
                        && matches!(record.op.role(), WalOpRole::PointDelete)
                        && crate::types::resolve_same_sequence(
                            crate::types::VersionContent {
                                is_tombstone: true,
                                value: None,
                                expiration: None,
                            },
                            crate::types::VersionContent {
                                is_tombstone: true,
                                value: record.value,
                                expiration: record.expiration,
                            },
                        )
                        .is_ok()
            }
            Some(KeyState::Absent) | None => false,
        }
    }
}

/// Why a WAL segment's records are not provably in the manifest's SSTs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UncoveredWal {
    /// The segment holds a record the per-record proof can never certify (a
    /// delete or range delete). Only the recovery floor can retire it.
    Unprovable,
    /// A value write is not exactly in this column family's SSTs. Whether it
    /// is depends only on that family's manifest files (`file_covers_record`
    /// matches on `cf_id`), so only a change to them can cover it.
    MissingFrom(u32),
    /// An SST the proof needed could not be read or verified. This may be
    /// transient and says nothing about coverage, so it must not be
    /// remembered as a failed proof.
    Unverifiable,
}

/// Fingerprint of the manifest metadata that decides whether a record of
/// `cf_id` is covered: every field `file_covers_record` and the SST open in
/// `state_for` read. SST contents are immutable by name. A collision or a
/// reordering can only cause an extra proof or longer retention.
pub(crate) fn coverage_fingerprint(manifest: &Manifest, cf_id: u32) -> u64 {
    use std::hash::{Hash as _, Hasher as _};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for file in manifest.files.iter().filter(|file| file.cf_id == cf_id) {
        file.name.hash(&mut hasher);
        file.size_bytes.hash(&mut hasher);
        file.content_crc32c.hash(&mut hasher);
        file.smallest_seq.hash(&mut hasher);
        file.largest_seq.hash(&mut hasher);
        file.smallest_key.hash(&mut hasher);
        file.largest_key.hash(&mut hasher);
        file.key_bounds_complete.hash(&mut hasher);
    }
    hasher.finish()
}

/// A local WAL segment whose coverage proof failed, and what it failed on,
/// so prune repeats the proof only when the answer could change (#490).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FailedWalProof {
    reason: UncoveredWal,
    /// For `MissingFrom(cf)`: that family's coverage fingerprint at the time.
    fingerprint: Option<u64>,
}

impl FailedWalProof {
    /// The proof to remember for `reason`, or `None` when the failure must
    /// not be remembered (`Unverifiable`).
    pub(crate) fn remember(
        reason: UncoveredWal,
        fingerprint_of: &mut impl FnMut(u32) -> u64,
    ) -> Option<Self> {
        let fingerprint = match reason {
            UncoveredWal::Unprovable => None,
            UncoveredWal::MissingFrom(cf_id) => Some(fingerprint_of(cf_id)),
            UncoveredWal::Unverifiable => return None,
        };
        Some(Self {
            reason,
            fingerprint,
        })
    }

    /// Whether repeating the proof must fail again. Skipping it only retains
    /// the segment longer; nothing is removed on this answer.
    pub(crate) fn still_fails(&self, fingerprint_of: &mut impl FnMut(u32) -> u64) -> bool {
        match self.reason {
            UncoveredWal::Unprovable => true,
            UncoveredWal::MissingFrom(cf_id) => self.fingerprint == Some(fingerprint_of(cf_id)),
            UncoveredWal::Unverifiable => false,
        }
    }
}

/// Manifest SSTs this process has read in full and matched against their
/// recorded size and CRC32C, keyed by that identity.
///
/// SST names are never reused, so a file proven once stays the file its
/// manifest entry names; later passes re-check only its length. Block
/// checksums still guard every block a proof reads. An entry whose recorded
/// identity changes is proven again.
#[derive(Debug, Default)]
pub(crate) struct ProvenSstIdentities(std::collections::HashSet<(String, u64, u32)>);

pub(crate) struct VerifiedManifestWalCoverage<'a> {
    fs: Arc<dyn crate::io::Fs>,
    sst_prefix: &'a str,
    manifest: &'a Manifest,
    proven: std::cell::RefCell<&'a mut ProvenSstIdentities>,
    readers: std::cell::RefCell<
        std::collections::HashMap<String, Option<Box<dyn crate::sst::traits::SstReaderExt>>>,
    >,
}

impl<'a> VerifiedManifestWalCoverage<'a> {
    /// A prover reading the manifest's SSTs through `fs`, where each file
    /// lives at `sst_prefix` followed by its name.
    pub(crate) fn open(
        fs: Arc<dyn crate::io::Fs>,
        sst_prefix: &'a str,
        manifest: &'a Manifest,
        proven: &'a mut ProvenSstIdentities,
    ) -> Self {
        Self {
            fs,
            sst_prefix,
            manifest,
            proven: std::cell::RefCell::new(proven),
            readers: std::cell::RefCell::new(std::collections::HashMap::new()),
        }
    }

    fn state_for(&self, file: &FileMeta, key: &[u8]) -> Option<crate::types::KeyState> {
        let mut readers = self.readers.borrow_mut();
        let reader = readers
            .entry(file.name.clone())
            .or_insert_with(|| self.open_verified(file));
        // Coverage compares persisted identity, so read the raw state with no
        // TTL clock: wall-clock expiry would turn an expired but present exact
        // version into a tombstone and hide it from the proof.
        reader
            .as_ref()?
            .get_state_at_with_time(key, u64::MAX, 0)
            .ok()
    }

    /// A reader over `file`, or `None` when it cannot be shown to be the SST
    /// the manifest names: unreadable, a different length, no recorded
    /// CRC32C, or a CRC32C that does not match.
    fn open_verified(&self, file: &FileMeta) -> Option<Box<dyn crate::sst::traits::SstReaderExt>> {
        let name = crate::cloud_layout::PersistedSstName::parse(&file.name).ok()?;
        let expected_crc = file.content_crc32c?;
        let path = crate::io::FsPath::new(format!("{}{}", self.sst_prefix, name.as_str()));
        let pinned = self
            .fs
            .immutable_read_view(&path)
            .ok()
            .flatten()
            .unwrap_or_else(|| Arc::clone(&self.fs));
        let identity = (file.name.clone(), file.size_bytes, expected_crc);
        let len = pinned.metadata(&path).ok()?.len;
        if file.size_bytes != 0 && len != file.size_bytes {
            return None;
        }
        if !self.proven.borrow().0.contains(&identity) {
            let handle = pinned
                .open(
                    &path,
                    crate::io::OpenOptions {
                        mode: crate::io::OpenMode::ReadOnly,
                        create: false,
                        create_new: false,
                        truncate: false,
                    },
                )
                .ok()?;
            let actual =
                crate::sst::identity::SstIdentity::of_file(handle.as_ref(), len, None).ok()?;
            if actual.crc32c != expected_crc {
                return None;
            }
            self.proven.borrow_mut().0.insert(identity);
        }
        let factory = crate::sst::FsSstFactoryIo::new(pinned, 64 * 1024);
        crate::sst::SstFactory::open(&factory, Path::new(&path.0)).ok()
    }

    /// Why `records` are not all exactly in the manifest's SSTs, or `None`
    /// when they are.
    pub(crate) fn first_uncovered(&self, records: &[DataCoverageRecord]) -> Option<UncoveredWal> {
        if records
            .iter()
            .any(|record| !matches!(record.op.role(), crate::wal::types::WalOpRole::ValueWrite))
        {
            return Some(UncoveredWal::Unprovable);
        }
        records
            .iter()
            .find_map(|record| self.value_write_gap(record))
    }

    /// `None` when `record` is exactly in its family's SSTs.
    fn value_write_gap(&self, record: &DataCoverageRecord) -> Option<UncoveredWal> {
        let mut state = ExactCoverageState::default();
        for file in &self.manifest.files {
            if !file_covers_record(file, record) {
                continue;
            }
            let Some(observed) = self.state_for(file, &record.key) else {
                // The SST could not be read, sized, checksummed or queried:
                // nothing is known about coverage.
                return Some(UncoveredWal::Unverifiable);
            };
            state.observe(observed);
        }
        (!state.exactly_covers(record)).then_some(UncoveredWal::MissingFrom(record.cf_id))
    }

    /// Whether recovery may skip replaying `record` because the manifest's
    /// SSTs already hold it.
    ///
    /// This is the same exact-coverage rule WAL pruning uses (#503): every
    /// covering SST must be readable and agree, and one must hold exactly
    /// this value and expiration, or a newer version must supersede it.
    /// Deletes and transaction markers are never skipped: replaying one is
    /// sequence-safe, while suppressing a delete can resurrect an older
    /// value. Replaying a covered record is harmless, so every doubt replays.
    /// A record that disagrees with an SST at its own sequence (a different
    /// expiration, say) is not covered: replaying it surfaces the
    /// disagreement later as a compaction conflict instead of silently
    /// dropping the WAL's version.
    pub(crate) fn covers_wal_record(&self, record: &crate::wal::WalRecord) -> bool {
        if !matches!(record.op.role(), crate::wal::types::WalOpRole::ValueWrite) {
            return false;
        }
        self.value_write_gap(&point_coverage_record(record))
            .is_none()
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

/// The coverage view of a point WAL record (a put, insert or delete).
pub(crate) fn point_coverage_record(record: &crate::wal::WalRecord) -> DataCoverageRecord {
    DataCoverageRecord {
        cf_id: record.cf_id,
        op: record.op,
        key: record.key.to_vec(),
        value: record.value.as_ref().map(|value| value.to_vec()),
        expiration: record.expiration,
        range_end: None,
        seq: record.seq,
    }
}

/// Whether `file` is a candidate to hold `record`: same column family, with
/// complete sequence and key bounds that contain the record.
pub(crate) fn file_covers_record(file: &FileMeta, record: &DataCoverageRecord) -> bool {
    file_covers_record_fields(
        file,
        record.cf_id,
        record.seq,
        &record.key,
        record.range_end.as_deref(),
    )
}

/// Candidate check for replay's borrowed point WAL record.
pub(crate) fn file_covers_wal_point_record(
    file: &FileMeta,
    record: &crate::wal::WalRecord,
) -> bool {
    file_covers_record_fields(file, record.cf_id, record.seq, record.key.as_ref(), None)
}

fn file_covers_record_fields(
    file: &FileMeta,
    cf_id: u32,
    seq: u64,
    key: &[u8],
    range_end: Option<&[u8]>,
) -> bool {
    if file.cf_id != cf_id {
        return false;
    }

    let (Some(smallest_seq), Some(largest_seq)) = (file.smallest_seq, file.largest_seq) else {
        return false;
    };
    if seq < smallest_seq || seq > largest_seq {
        return false;
    }

    let (Some(smallest_key), Some(largest_key)) =
        (file.smallest_key.as_ref(), file.largest_key.as_ref())
    else {
        return false;
    };
    if let Some(range_end) = range_end {
        smallest_key.as_slice() <= key && range_end <= largest_key.as_slice()
    } else {
        smallest_key.as_slice() <= key && key <= largest_key.as_slice()
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
mod metadata_authority_tests {
    use super::CloudMetadataAuthorityProof;
    use crate::lease::{
        CloudLeaseConfig, CloudMetadataGeneration, CloudMetadataObject, CloudStorageLease,
        PrimaryLease,
    };
    use crate::storage::cloud::CloudStorage;
    use std::sync::Arc;
    use std::time::Duration;

    fn generation(sequence: u64) -> CloudMetadataGeneration {
        let id = uuid::Uuid::new_v4();
        CloudMetadataGeneration {
            manifest_sequence: sequence,
            objects: [
                crate::metadata::files::FORMAT,
                crate::metadata::files::MANIFEST_SNAPSHOT,
            ]
            .into_iter()
            .map(|file_name| CloudMetadataObject {
                file_name: file_name.to_string(),
                object_key: format!("metadata/generations/{id}/{file_name}"),
                len: 1,
                crc32c: 0,
            })
            .collect(),
        }
    }

    #[test]
    fn should_reject_wal_prune_authority_when_committed_pointer_changes() {
        // Arrange
        let cache = tempfile::tempdir().expect("lease cache directory");
        let cloud = Arc::new(CloudStorage::with_mock());
        let lease = Arc::new(CloudStorageLease::new_provider_backed(
            CloudLeaseConfig {
                bucket: "test".to_string(),
                prefix: String::new(),
            },
            cache.path().to_path_buf(),
            cloud,
        ));
        let _guard = Arc::clone(&lease).try_acquire().expect("acquire lease");
        let store = lease.get_leader_store().expect("provider leader store");
        let holder_id = lease.holder_id();
        let epoch = lease.epoch();
        let first = generation(1);
        store
            .publish_committed_metadata(
                &holder_id,
                epoch,
                None,
                first.clone(),
                Duration::from_secs(5),
            )
            .expect("commit first pointer");
        let proof = CloudMetadataAuthorityProof {
            store: Arc::clone(&store),
            holder_id: holder_id.clone(),
            writer_epoch: epoch,
            generation: first.clone(),
        };
        let deadline = crate::common::OperationDeadline::from_budget(Duration::from_secs(5));
        proof
            .verify_current(&deadline)
            .expect("first pointer is current");
        let second = generation(2);

        // Act
        store
            .publish_committed_metadata(
                &holder_id,
                epoch,
                Some(&first),
                second,
                Duration::from_secs(5),
            )
            .expect("commit successor pointer");
        let result = proof.verify_current(&deadline);

        // Assert
        assert!(
            matches!(result, Err(crate::common::MidgeError::Busy(_))),
            "WAL cleanup must reject a superseded metadata pointer: {result:?}"
        );
    }

    #[test]
    fn should_reject_wal_prune_authority_after_lease_release() {
        // Arrange
        let cache = tempfile::tempdir().expect("lease cache directory");
        let cloud = Arc::new(CloudStorage::with_mock());
        let lease = Arc::new(CloudStorageLease::new_provider_backed(
            CloudLeaseConfig {
                bucket: "test".to_string(),
                prefix: String::new(),
            },
            cache.path().to_path_buf(),
            cloud,
        ));
        let _guard = Arc::clone(&lease).try_acquire().expect("acquire lease");
        let store = lease.get_leader_store().expect("provider leader store");
        let holder_id = lease.holder_id();
        let epoch = lease.epoch();
        let committed = generation(1);
        store
            .publish_committed_metadata(
                &holder_id,
                epoch,
                None,
                committed.clone(),
                Duration::from_secs(5),
            )
            .expect("commit pointer");
        let proof = CloudMetadataAuthorityProof {
            store,
            holder_id,
            writer_epoch: epoch,
            generation: committed,
        };
        lease.release().expect("release lease");

        // Act
        let result = proof.verify_current(&crate::common::OperationDeadline::from_budget(
            Duration::from_secs(5),
        ));

        // Assert
        assert!(
            matches!(result, Err(crate::common::MidgeError::Fenced(_))),
            "WAL cleanup must reject a released lease before catalog retirement: {result:?}"
        );
    }
}

#[cfg(test)]
mod tests;
