//! Format-aware orchestration for hybrid persistence.
//!
//! `HybridStorage` deliberately knows only object keys, bytes, provider
//! identities, bounded callbacks, and conditional deletion. This runtime layer
//! owns WAL/SST decoding and durable frontier decisions.

use crate::common::{MidgeError, MidgeResult};
#[cfg(test)]
use crate::metadata::{FileMeta, Manifest};
#[cfg(test)]
use crate::storage::cloud::CloudStorage;
#[cfg(test)]
use crate::storage::hybrid::backend::GuardedObjectProof;
use crate::storage::hybrid::backend::{HybridStorage, RemoteObjectProof};
#[cfg(test)]
use crate::storage::{StorageBackend, StorageObjectMetadata};
use crate::wal::cloud_catalog::{CatalogEntry, WalPublicationCatalog};
use std::sync::Arc;

mod catalog;
mod metadata_snapshot;
use crate::storage::hybrid::backend::ControlObject;
pub(crate) use catalog::refusal_error;
pub(crate) use catalog::AdmittedCatalog;
use catalog::{load_and_repair_catalog_within, mutate_wal_catalog_within, Missing, Mutation};
pub(crate) use metadata_snapshot::{
    decode_generation_manifest, mirror_control_metadata_within,
    read_committed_metadata_bodies_within, CloudMetadataMirrorAuthority,
    CloudMetadataMirrorContext, SharedMetadataSnapshot, SharedRetirementAuthority,
};

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

struct ValidatedWalObject {
    proof: RemoteObjectProof,
}

pub(crate) type CloudWalPruneBatchResults = Vec<(u64, MidgeResult<()>)>;

fn cloud_wal_retirement_ids(candidates: &[(u64, u64)]) -> MidgeResult<Vec<u64>> {
    let mut ids = candidates.iter().map(|&(id, _)| id).collect::<Vec<_>>();
    ids.sort_unstable();
    if ids.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(MidgeError::Internal(
            "cloud WAL retirement contains duplicate ids".into(),
        ));
    }
    Ok(ids)
}

/// Runtime-owned format operations layered over raw hybrid object I/O.
///
/// `HybridStorage` stays format-neutral: it moves keyed bytes and enforces
/// budgets. `CloudPersistence` owns everything above that - WAL catalog
/// authority, family flush authority, and SST publication proofs - so the format
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
        let readback =
            crate::wal::cloud_segment::validate_bytes(&source.to_string(), &bytes, max_sequence)
                .map_err(MidgeError::Corruption)?;
        // The object is named for the epoch of the writer that wrote it,
        // which its header records (#843).
        let entry = CatalogEntry::describing(&readback, &bytes);
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
        let entry = CatalogEntry::describing(&local_readback, &local_bytes);
        if entry.id() != segment_id {
            return Err(PublicationError::Mismatch {
                segment_id,
                detail: format!(
                    "local WAL segment '{source}' holds segment {}, not {segment_id}",
                    entry.id()
                ),
            });
        }
        if self.take_verified_upload(segment_id, &entry.object_key(), &local_bytes) {
            return Ok(entry);
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

    /// Retire only the catalog prefix covered by a committed shared generation.
    /// Object identities are pinned before the catalog CAS; only after durable
    /// retirement do conditional physical deletes run.
    pub(crate) fn prune_cloud_wal_segments_within(
        &self,
        candidates: &[(u64, u64)],
        authority: &SharedRetirementAuthority,
        fencing_epoch: u64,
        deadline: &crate::common::OperationDeadline,
    ) -> MidgeResult<CloudWalPruneBatchResults> {
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let ids = cloud_wal_retirement_ids(candidates)?;
        if authority.writer_epoch() != fencing_epoch {
            return Err(MidgeError::Fenced(
                "shared retirement authority belongs to another writer epoch".into(),
            ));
        }
        authority.verify_current(deadline)?;
        let (_, catalog) = authoritative_wal_catalog_within(self, deadline)?;
        let mut results = Vec::new();
        let active = ids
            .iter()
            .copied()
            .filter(|id| *id > catalog.anchor().id)
            .collect::<Vec<_>>();
        for &id in &ids {
            if id <= catalog.anchor().id {
                results.push((id, Ok(())));
            }
        }
        catalog
            .oldest_prefix(active.iter().copied())
            .map_err(refusal_error)?;
        let mut expected = Vec::new();
        let mut targets = Vec::new();
        let covered_count = catalog
            .retirable_prefix(|cf, sequence| match authority.frontiers().get(cf) {
                Ok(crate::metadata::FamilyFrontier::Live { through_sequence }) => {
                    sequence <= through_sequence
                }
                Ok(crate::metadata::FamilyFrontier::Dropped) => true,
                Err(_) => false,
            })
            .len()
            .min(active.len());
        for (index, id) in active.into_iter().enumerate() {
            let entry = catalog.entry(id).expect("oldest prefix checked");
            if index >= covered_count {
                results.push((
                    id,
                    Err(MidgeError::Busy(
                        "oldest WAL segment is not flushed in the committed generation".into(),
                    )),
                ));
                continue;
            }
            // HEAD supplies only a conditional target identity, never replay or
            // retirement evidence. Selection uses persisted verified summaries.
            let identity =
                self.remote_range_metadata_optional_within(&entry.object_key(), deadline)?;
            targets.push((id, entry.object_key(), identity));
            expected.push(entry.clone());
        }
        if expected.is_empty() {
            return Ok(results);
        }
        // Reserve bounded physical cleanup ownership before switching authority.
        // Worker exhaustion can delay deletion, but cannot lose the retired target.
        let deletes = targets
            .iter()
            .filter_map(|(id, key, identity)| {
                identity
                    .as_ref()
                    .map(|identity| (*id, key.clone(), identity.clone()))
            })
            .collect();
        let cleanup = self
            .admit_remote_deletes_by_identity(deletes)
            .map_err(|error| MidgeError::Busy(error.to_string()))?;
        let retired = mutate_wal_catalog_within(
            self,
            "cloud WAL retirement",
            deadline,
            &|| {
                self.check_write_authority()?;
                authority.verify_current(deadline)
            },
            Missing::Refuse,
            |catalog| {
                catalog
                    .retire_prefix(fencing_epoch, &expected, authority.generation_id())
                    .map(|retired| Mutation::of(!retired.is_empty(), retired))
            },
        )?;
        let (retired, _) = retired.map_err(refusal_error)?;

        for (id, _key, identity) in targets {
            if !retired.contains(&id) {
                results.push((id, Ok(())));
                continue;
            }
            match identity {
                Some(_) => {}
                None => {
                    self.queue_cloud_wal_prune_complete(id, crate::storage::StorageOutcome::Ok(()));
                }
            }
        }
        cleanup.schedule();
        results.sort_by_key(|(id, _)| *id);
        Ok(results)
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

#[cfg(test)]
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
