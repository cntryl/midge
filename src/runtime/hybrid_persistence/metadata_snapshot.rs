//! Immutable shared metadata publications and the authority they give WAL retirement.

use crate::common::resource_budget::{ResourceBudget, ResourceReservation};
use crate::common::{DeadlineScope, MidgeError, MidgeResult, OperationDeadline};
use crate::io::{FsError, FsPath};
use crate::lease::{CloudMetadataGeneration, CloudMetadataHead, LeaderStore, SharedGenerationId};
use crate::metadata::{files, Manifest, ManifestPersistence};
use crate::storage::cloud::{BlockingCloud, CloudStorage};
use std::collections::BTreeMap;
use std::sync::Arc;

#[cfg(test)]
mod cache_tests;

#[derive(Clone)]
pub(crate) struct FileFingerprint {
    length: usize,
    digest: Option<[u8; 32]>,
}

impl FileFingerprint {
    pub(super) fn unchanged_prefix(&self, bytes: &[u8]) -> Option<usize> {
        let prefix = bytes.get(..self.length)?;
        let expected = self.digest?;
        (metadata_digest(prefix) == Some(expected)).then_some(self.length)
    }

    fn unchanged(&self, bytes: &[u8]) -> bool {
        bytes.len() == self.length && self.unchanged_prefix(bytes).is_some()
    }
}

fn metadata_digest(bytes: &[u8]) -> Option<[u8; 32]> {
    use sha2::Digest;
    if bytes.len() as u64 > crate::metadata::cloud_manifest::MAX_CHAIN_BYTES {
        return None;
    }
    Some(sha2::Sha256::digest(bytes).into())
}

/// Only authority fields and fixed-size fingerprints are retained, not SST
/// inventories or copied control-file bodies.
#[derive(Clone)]
pub(crate) struct MetadataMirrorCache {
    pub(super) generation: CloudMetadataGeneration,
    pub(super) writer_epoch: u64,
    pub(super) manifest: Manifest,
    pub(super) fingerprints: BTreeMap<String, FileFingerprint>,
    pub(super) cut: Option<crate::runtime::metadata_publication::MetadataCutProof>,
    pub(super) terminal_witnesses:
        Option<Arc<crate::runtime::metadata_publication::SelectedTerminalWitnesses>>,
}

impl MetadataMirrorCache {
    pub(super) fn new(
        generation: CloudMetadataGeneration,
        writer_epoch: u64,
        mut manifest: Manifest,
        local: &BTreeMap<String, Vec<u8>>,
    ) -> Self {
        reduce_to_authority(&mut manifest);
        Self {
            generation,
            writer_epoch,
            manifest,
            cut: None,
            terminal_witnesses: None,
            fingerprints: local
                .iter()
                .map(|(name, bytes)| {
                    (
                        name.clone(),
                        FileFingerprint {
                            length: bytes.len(),
                            digest: metadata_digest(bytes),
                        },
                    )
                })
                .collect(),
        }
    }

    pub(crate) fn matches(&self, generation: &CloudMetadataGeneration, epoch: u64) -> bool {
        self.writer_epoch == epoch
            && &self.generation == generation
            && self
                .fingerprints
                .values()
                .all(|fingerprint| fingerprint.digest.is_some())
    }

    fn unchanged(&self, local: &BTreeMap<String, Vec<u8>>) -> bool {
        self.fingerprints.len() == local.len()
            && local.iter().all(|(name, bytes)| {
                self.fingerprints
                    .get(name)
                    .is_some_and(|fingerprint| fingerprint.unchanged(bytes))
            })
    }
}

pub(crate) fn reduce_to_authority(manifest: &mut Manifest) {
    manifest.files = Vec::new();
    manifest.retired_ssts = Default::default();
    manifest.next_sst_seqs = Default::default();
    for family in &mut manifest.column_families {
        family.dropped_sst_names = Vec::new();
    }
}

/// A committed shared manifest, decoded from the exact immutable pointer bodies.
/// Only the shared loader can construct one; local progress never grants authority.
pub(crate) struct SharedRetirementAuthority {
    generation: CloudMetadataGeneration,
    manifest: Manifest,
    store: Arc<dyn LeaderStore>,
    holder_id: String,
    writer_epoch: u64,
    _memory: ResourceReservation,
}

impl SharedRetirementAuthority {
    pub(crate) fn generation_id(&self) -> SharedGenerationId {
        self.generation.id
    }
    pub(crate) fn writer_epoch(&self) -> u64 {
        self.writer_epoch
    }
    pub(crate) fn frontiers(&self) -> &crate::metadata::frontiers::FlushFrontiers {
        self.manifest.flush_frontiers()
    }

    pub(crate) fn verify_current(&self, deadline: &OperationDeadline) -> MidgeResult<()> {
        self.store
            .validate_epoch_with_timeout(&self.holder_id, self.writer_epoch, deadline.remaining())
            .map_err(|error| error.into_validation_error("shared retirement lease"))?;
        let current = self
            .store
            .read_committed_metadata(deadline.remaining())
            .map_err(|error| error.into_validation_error("shared retirement pointer"))?;
        if current != CloudMetadataHead::Committed(self.generation.clone()) {
            return Err(MidgeError::Fenced(
                "shared metadata pointer changed before WAL retirement".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct SharedMetadataSnapshot {
    cloud: Arc<CloudStorage>,
    budget: ResourceBudget,
    store: Arc<dyn LeaderStore>,
    holder_id: String,
    writer_epoch: u64,
    publication_lock: crate::runtime::MetadataPublicationLock,
}

impl SharedMetadataSnapshot {
    pub(crate) fn new(
        cloud: Arc<CloudStorage>,
        budget: ResourceBudget,
        store: Arc<dyn LeaderStore>,
        holder_id: String,
        writer_epoch: u64,
        publication_lock: crate::runtime::MetadataPublicationLock,
    ) -> Self {
        Self {
            cloud,
            budget,
            store,
            holder_id,
            writer_epoch,
            publication_lock,
        }
    }

    pub(crate) fn committed_retirement_authority_within(
        &self,
        deadline: &OperationDeadline,
    ) -> MidgeResult<SharedRetirementAuthority> {
        // Selection and its exact body closure are one protected fetch. A
        // retention worker cannot unlink this cut before decoding finishes.
        let _capture = self
            .publication_lock
            .lock_for(deadline.remaining().min(self.cloud.callback_timeout()))
            .ok_or_else(|| {
                MidgeError::Timeout("shared metadata capture waits for publication".into())
            })?;
        self.store
            .validate_epoch_with_timeout(&self.holder_id, self.writer_epoch, deadline.remaining())
            .map_err(|error| error.into_validation_error("shared metadata snapshot lease"))?;
        let CloudMetadataHead::Committed(generation) = self
            .store
            .read_committed_metadata(deadline.remaining())
            .map_err(|error| error.into_validation_error("shared metadata snapshot pointer"))?
        else {
            return Err(MidgeError::Corruption(
                "cloud WAL retirement requires a committed shared manifest".into(),
            ));
        };
        let encoded = generation
            .objects
            .iter()
            .try_fold(0usize, |total, object| {
                usize::try_from(object.len)
                    .ok()
                    .and_then(|len| total.checked_add(len))
                    .ok_or_else(|| {
                        MidgeError::ResourceLimit(
                            "shared metadata length exceeds address space".into(),
                        )
                    })
            })?;
        let admitted = encoded
            .checked_mul(16)
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or_else(|| {
                MidgeError::ResourceLimit("shared metadata decoding admission overflow".into())
            })?;
        let memory = self
            .budget
            .reserve(admitted, "shared metadata manifest decoding")?;
        let bodies = read_committed_metadata_bodies_within(&self.cloud, &generation, deadline)?;
        let manifest = decode_generation_manifest(&generation, &bodies, deadline)?;
        let authority = SharedRetirementAuthority {
            generation,
            manifest,
            store: Arc::clone(&self.store),
            holder_id: self.holder_id.clone(),
            writer_epoch: self.writer_epoch,
            _memory: memory,
        };
        authority.verify_current(deadline)?;
        Ok(authority)
    }
}

pub(crate) fn read_committed_metadata_bodies_within(
    cloud: &CloudStorage,
    generation: &CloudMetadataGeneration,
    deadline: &OperationDeadline,
) -> MidgeResult<BTreeMap<String, Vec<u8>>> {
    read_committed_metadata_bodies_reserved_within(cloud, generation, deadline, None)
}

pub(super) fn read_committed_metadata_bodies_reserved_within(
    cloud: &CloudStorage,
    generation: &CloudMetadataGeneration,
    deadline: &OperationDeadline,
    reservation: Option<Arc<ResourceReservation>>,
) -> MidgeResult<BTreeMap<String, Vec<u8>>> {
    if generation.id == SharedGenerationId::NONE {
        return Err(MidgeError::Corruption(
            "committed metadata has zero generation ordinal".into(),
        ));
    }
    if super::metadata_chain::is_chain(generation) {
        let bodies = if let Some(reservation) = reservation {
            super::metadata_chain::read_with_cut_reserved(
                cloud,
                generation,
                deadline,
                Some(reservation),
            )?
            .0
        } else {
            super::metadata_chain::read(cloud, generation, deadline)?
        };
        for required in [files::FORMAT, files::MANIFEST_SNAPSHOT] {
            if !bodies.contains_key(required) {
                return Err(MidgeError::Corruption(format!(
                    "committed cloud manifest lacks '{required}'"
                )));
            }
        }
        if bodies[files::FORMAT] != crate::metadata::format::current_format_marker_bytes() {
            return Err(MidgeError::CompatibilityError(
                "cloud manifest FORMAT differs from the current format".into(),
            ));
        }
        return Ok(bodies);
    }
    let mut io = BlockingCloud::new(cloud, deadline);
    if let Some(reservation) = reservation {
        io = io.with_reservation(reservation);
    }
    let mut bodies = BTreeMap::new();
    for object in &generation.objects {
        if !files::CLOUD_MIRRORED.contains(&object.file_name.as_str())
            || bodies.contains_key(&object.file_name)
        {
            return Err(MidgeError::Corruption(
                "shared metadata has unknown or duplicate filenames".into(),
            ));
        }
        let canonical_key = object
            .object_key
            .strip_prefix("metadata/generations/")
            .and_then(|suffix| suffix.split_once('/'))
            .is_some_and(|(id, name)| {
                name == object.file_name
                    && uuid::Uuid::parse_str(id).is_ok_and(|parsed| parsed.to_string() == id)
            });
        if !canonical_key {
            return Err(MidgeError::Corruption(
                "shared metadata has an invalid immutable object key".into(),
            ));
        }
        let data = io.get_optional(&object.object_key)?.ok_or_else(|| {
            MidgeError::Corruption(format!(
                "committed cloud metadata '{}' is missing",
                object.file_name
            ))
        })?;
        if u64::try_from(data.len()).unwrap_or(u64::MAX) != object.len
            || crc32c::crc32c(&data) != object.crc32c
        {
            return Err(MidgeError::Corruption(format!(
                "committed cloud metadata '{}' does not match its length and checksum",
                object.file_name
            )));
        }
        bodies.insert(object.file_name.clone(), data);
    }
    for required in [files::FORMAT, files::MANIFEST_SNAPSHOT] {
        if !bodies.contains_key(required) {
            return Err(MidgeError::Corruption(format!(
                "committed cloud metadata generation is missing '{required}'"
            )));
        }
    }
    if bodies[files::FORMAT] != crate::metadata::format::current_format_marker_bytes() {
        return Err(MidgeError::CompatibilityError(
            "committed cloud FORMAT differs from the current format".into(),
        ));
    }
    Ok(bodies)
}

pub(crate) fn decode_generation_manifest(
    generation: &CloudMetadataGeneration,
    bodies: &BTreeMap<String, Vec<u8>>,
    deadline: &OperationDeadline,
) -> MidgeResult<Manifest> {
    let manifest = decode_manifest_bodies(bodies, deadline)?;
    if manifest.edit_checkpoint_id != generation.manifest_horizon {
        return Err(MidgeError::Corruption(format!(
            "committed manifest horizon {} differs from shared pointer horizon {}",
            manifest.edit_checkpoint_id, generation.manifest_horizon
        )));
    }
    Ok(manifest)
}

pub(super) fn decode_manifest_bodies(
    bodies: &BTreeMap<String, Vec<u8>>,
    deadline: &OperationDeadline,
) -> MidgeResult<Manifest> {
    let snapshot = bodies.get(files::MANIFEST_SNAPSHOT).ok_or_else(|| {
        MidgeError::Corruption("metadata publication lacks manifest snapshot".into())
    })?;
    ManifestPersistence::decode_bodies(
        snapshot,
        bodies.get(files::JOURNAL).map(Vec::as_slice),
        &DeadlineScope::new(*deadline),
    )
}

#[derive(Clone, Copy)]
pub(crate) struct CloudMetadataMirrorAuthority<'a> {
    pub(crate) store: &'a dyn LeaderStore,
    pub(crate) holder_id: &'a str,
    pub(crate) writer_epoch: u64,
}

#[derive(Clone, Copy)]
pub(crate) struct CloudMetadataMirrorContext<'a> {
    pub(crate) cloud: &'a CloudStorage,
    pub(crate) fs: &'a dyn crate::io::Fs,
    pub(crate) publication_lock: &'a crate::runtime::MetadataPublicationLock,
    pub(crate) lock_wait_budget: std::time::Duration,
    pub(crate) deadline: &'a OperationDeadline,
    pub(crate) authority: CloudMetadataMirrorAuthority<'a>,
}

pub(super) fn capture_local_metadata(
    context: &CloudMetadataMirrorContext<'_>,
) -> MidgeResult<BTreeMap<String, Vec<u8>>> {
    let mut local = BTreeMap::new();
    for name in files::CLOUD_MIRRORED {
        if context.deadline.is_expired() {
            return Err(MidgeError::Timeout(
                "metadata capture deadline exhausted".into(),
            ));
        }
        match crate::io::read_all(context.fs, &FsPath::new(*name)) {
            Ok(bytes) => {
                local.insert((*name).to_string(), bytes.to_vec());
            }
            Err(FsError::NotFound(_)) => {}
            Err(error) => return Err(error.into_midge()),
        }
    }
    if local.get(files::FORMAT) != Some(&crate::metadata::format::current_format_marker_bytes()) {
        return Err(MidgeError::Corruption(
            "cannot commit cloud metadata without current FORMAT".into(),
        ));
    }
    Ok(local)
}

pub(crate) fn validate_manifest_successor(
    candidate: &Manifest,
    committed: &Manifest,
) -> MidgeResult<()> {
    if candidate.edit_checkpoint_id < committed.edit_checkpoint_id {
        return Err(MidgeError::Fenced(
            "candidate shared manifest regresses the committed journal horizon".into(),
        ));
    }
    candidate
        .flush_frontiers()
        .dominates(committed.flush_frontiers())
        .map_err(|error| {
            MidgeError::Fenced(format!(
                "candidate shared manifest regresses committed family authority: {error}"
            ))
        })?;
    for old in &committed.column_families {
        let current = candidate
            .column_families
            .iter()
            .find(|family| family.id == old.id)
            .ok_or_else(|| {
                MidgeError::Fenced("candidate shared manifest removes a family identity".into())
            })?;
        if current.name != old.name || current.created_at != old.created_at {
            return Err(MidgeError::Fenced(
                "candidate shared manifest replaces an existing family identity".into(),
            ));
        }
    }
    Ok(())
}

fn verify_unchanged_publication(
    context: &CloudMetadataMirrorContext<'_>,
    previous: &CloudMetadataGeneration,
) -> MidgeResult<()> {
    context
        .authority
        .store
        .validate_epoch_with_timeout(
            context.authority.holder_id,
            context.authority.writer_epoch,
            context.deadline.remaining(),
        )
        .map_err(|error| error.into_validation_error("unchanged metadata publication lease"))?;
    if context
        .authority
        .store
        .read_committed_metadata(context.deadline.remaining())
        .map_err(|error| error.into_validation_error("unchanged metadata pointer readback"))?
        != CloudMetadataHead::Committed(previous.clone())
    {
        return Err(MidgeError::Fenced(
            "shared metadata pointer changed during unchanged publication".into(),
        ));
    }
    Ok(())
}

fn commit_metadata_generation(
    context: &CloudMetadataMirrorContext<'_>,
    previous: Option<&CloudMetadataGeneration>,
    generation: CloudMetadataGeneration,
) -> MidgeResult<()> {
    loop {
        DeadlineScope::new(*context.deadline).check("cloud metadata pointer publication")?;
        match context.authority.store.publish_committed_metadata(
            context.authority.holder_id,
            context.authority.writer_epoch,
            previous,
            generation.clone(),
            context.deadline.remaining(),
        ) {
            Ok(()) => break,
            Err(crate::lease::LeaseError::AcquisitionFailed(_)) => {
                // A definite rejection has no unknown write to reconcile.
                // Refresh both authority and identity before retrying the
                // same immutable candidate within its original deadline.
                context
                    .authority
                    .store
                    .validate_epoch_with_timeout(
                        context.authority.holder_id,
                        context.authority.writer_epoch,
                        context.deadline.remaining(),
                    )
                    .map_err(|error| error.into_validation_error("metadata collision lease"))?;
                let current = context
                    .authority
                    .store
                    .read_committed_metadata(context.deadline.remaining())
                    .map_err(|error| error.into_validation_error("metadata collision pointer"))?;
                let expected = previous.map_or(CloudMetadataHead::Uncommitted, |value| {
                    CloudMetadataHead::Committed(value.clone())
                });
                if current != expected {
                    return Err(MidgeError::Fenced(
                        "shared metadata predecessor changed during publication".into(),
                    ));
                }
            }
            Err(error) => {
                return Err(error.into_validation_error("cloud metadata generation publication"))
            }
        }
    }
    let readback = context
        .authority
        .store
        .read_committed_metadata_for_writer(
            context.authority.holder_id,
            context.authority.writer_epoch,
            context.deadline.remaining(),
        )
        .map_err(|error| error.into_validation_error("cloud metadata generation readback"))?;
    if readback != CloudMetadataHead::Committed(generation) {
        return Err(MidgeError::Fenced(
            "shared metadata pointer changed after publication".into(),
        ));
    }
    Ok(())
}

/// The exact predecessor CAS is the shared publication boundary. Staged objects
/// that lose it remain harmless orphans. Durable family frontiers never regress.
pub(crate) fn mirror_control_metadata_within(
    context: CloudMetadataMirrorContext<'_>,
    mut validate_lease: impl FnMut(&OperationDeadline) -> MidgeResult<()>,
) -> MidgeResult<()> {
    let timeout = if context.lock_wait_budget.is_zero() {
        if context.deadline.is_expired() {
            return Err(MidgeError::Timeout(
                "metadata mirror deadline exhausted before publication lock".into(),
            ));
        }
        std::time::Duration::ZERO
    } else {
        context
            .deadline
            .clamp_nonzero(
                context
                    .lock_wait_budget
                    .min(context.cloud.callback_timeout()),
            )
            .ok_or_else(|| {
                MidgeError::Timeout(
                    "metadata mirror deadline exhausted before publication lock".into(),
                )
            })?
    };
    let mut guard = context.publication_lock.lock_for(timeout).ok_or_else(|| {
        MidgeError::Timeout("metadata mirror timed out acquiring publication lock".into())
    })?;
    mirror_under_lock(&context, &mut guard.cached, &mut validate_lease)?;
    if let Some(cached) = &guard.cached {
        let generation = cached.generation.clone();
        super::metadata_retention::collect_after_commit(
            &context,
            &generation,
            &mut guard.retention,
        );
    }
    Ok(())
}

fn mirror_under_lock(
    context: &CloudMetadataMirrorContext<'_>,
    cache: &mut Option<MetadataMirrorCache>,
    validate_lease: &mut impl FnMut(&OperationDeadline) -> MidgeResult<()>,
) -> MidgeResult<()> {
    validate_lease(context.deadline)?;
    context
        .authority
        .store
        .validate_metadata_epoch_with_timeout(
            context.authority.holder_id,
            context.authority.writer_epoch,
            context.deadline.remaining(),
        )
        .map_err(|error| error.into_validation_error("metadata publication lease"))?;
    let previous = match context
        .authority
        .store
        .read_committed_metadata_for_writer(
            context.authority.holder_id,
            context.authority.writer_epoch,
            context.deadline.remaining(),
        )
        .map_err(|error| error.into_validation_error("cloud metadata pointer read"))?
    {
        CloudMetadataHead::Committed(generation) => Some(generation),
        CloudMetadataHead::Uncommitted => None,
        CloudMetadataHead::MissingLease => {
            return Err(MidgeError::Fenced(
                "cloud metadata lease disappeared before publication".into(),
            ))
        }
    };
    let local = capture_local_metadata(context)?;
    let candidate = decode_manifest_bodies(&local, context.deadline)?;
    let cached = cache.as_ref().filter(|cached| {
        previous
            .as_ref()
            .is_some_and(|generation| cached.matches(generation, context.authority.writer_epoch))
    });
    let old_bodies = previous
        .as_ref()
        .filter(|_| cached.is_none())
        .map(|generation| {
            read_committed_metadata_bodies_within(context.cloud, generation, context.deadline)
        })
        .transpose()?;
    if let Some(previous) = previous.as_ref() {
        if validate_cached_successor(
            context,
            previous,
            &candidate,
            &local,
            cached,
            old_bodies.as_ref(),
        )? {
            verify_unchanged_publication(context, previous)?;
            *cache = Some(MetadataMirrorCache::new(
                previous.clone(),
                context.authority.writer_epoch,
                candidate,
                &local,
            ));
            return Ok(());
        }
    }
    let id = previous
        .as_ref()
        .map_or(Some(SharedGenerationId::FIRST), |generation| {
            generation.id.checked_next()
        })
        .ok_or_else(|| {
            MidgeError::ResourceLimit("shared metadata generation ordinal exhausted".into())
        })?;
    let objects = super::metadata_chain::publish(
        context,
        &local,
        previous.as_ref(),
        old_bodies.as_ref(),
        cached.map(|cached| &cached.fingerprints),
        id,
    )?;
    let generation = CloudMetadataGeneration {
        id,
        manifest_horizon: candidate.edit_checkpoint_id,
        objects,
    };
    validate_lease(context.deadline)?;
    commit_metadata_generation(context, previous.as_ref(), generation.clone())?;
    *cache = Some(MetadataMirrorCache::new(
        generation,
        context.authority.writer_epoch,
        candidate,
        &local,
    ));
    Ok(())
}

fn validate_cached_successor(
    context: &CloudMetadataMirrorContext<'_>,
    previous: &CloudMetadataGeneration,
    candidate: &Manifest,
    local: &BTreeMap<String, Vec<u8>>,
    cached: Option<&MetadataMirrorCache>,
    old_bodies: Option<&BTreeMap<String, Vec<u8>>>,
) -> MidgeResult<bool> {
    if let Some(cached) = cached {
        validate_manifest_successor(candidate, &cached.manifest)?;
        Ok(cached.unchanged(local))
    } else {
        let committed = decode_generation_manifest(
            previous,
            old_bodies.expect("uncached predecessor loaded"),
            context.deadline,
        )?;
        validate_manifest_successor(candidate, &committed)?;
        Ok(old_bodies == Some(local))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::Fs;
    use crate::lease::fs_leader_store::FsLeaderStore;

    #[test]
    fn should_release_discarded_inventory_storage_when_reducing_to_authority() {
        // Arrange
        let mut manifest = Manifest::default();
        manifest
            .apply_edit(&crate::metadata::ManifestEdit::CreateColumnFamily {
                id: 1,
                name: "retired-family".into(),
                created_at: 7,
            })
            .unwrap();
        for sequence in 1..=64 {
            manifest.files.push(crate::metadata::FileMeta {
                name: crate::cloud_layout::file_name(1, 0, sequence),
                cf_id: 1,
                ..crate::metadata::FileMeta::default()
            });
            let retired = crate::metadata::FileMeta {
                name: crate::cloud_layout::file_name(0, 0, sequence),
                ..crate::metadata::FileMeta::default()
            };
            manifest.retired_ssts.insert(retired.name.clone(), retired);
        }
        manifest
            .apply_edit(&crate::metadata::ManifestEdit::DropColumnFamilyAt {
                id: 1,
                drop_sequence: 9,
                dropped_sst_names: manifest
                    .files
                    .iter()
                    .map(|file| file.name.clone())
                    .collect(),
            })
            .unwrap();
        manifest.next_sst_seqs.insert(1, 65);
        manifest.edit_checkpoint_id = 17;
        manifest.validate_persisted().unwrap();
        let frontiers = manifest.flush_frontiers().clone();

        // Act
        reduce_to_authority(&mut manifest);

        // Assert: authority survives while the discarded inventories release
        // their backing allocations rather than retaining zero-length capacity.
        assert_eq!(manifest.files.capacity(), 0);
        assert_eq!(manifest.retired_ssts.capacity(), 0);
        assert_eq!(manifest.next_sst_seqs.capacity(), 0);
        assert_eq!(manifest.column_families[0].dropped_sst_names.capacity(), 0);
        assert_eq!(manifest.edit_checkpoint_id, 17);
        assert_eq!(manifest.flush_frontiers(), &frontiers);
        assert_eq!(manifest.column_families[0].name, "retired-family");
        assert!(manifest.column_families[0].deleted_at.is_some());
    }

    struct Fixture {
        _directory: tempfile::TempDir,
        fs: Arc<dyn Fs>,
        cloud: Arc<CloudStorage>,
        store: Arc<FsLeaderStore>,
        lock: crate::runtime::MetadataPublicationLock,
    }

    impl Fixture {
        fn new() -> Self {
            let directory = tempfile::tempdir().unwrap();
            let fs = crate::io::real::test_root(directory.path());
            crate::metadata::ensure_or_create_format_marker(&fs, directory.path()).unwrap();
            ManifestPersistence::save_with_fs(&fs, &Manifest::default()).unwrap();
            let store = Arc::new(FsLeaderStore::new(
                Arc::clone(&fs),
                crate::lease::LeaseClock::default(),
            ));
            store.acquire_leadership("writer").unwrap();
            Self {
                _directory: directory,
                fs,
                cloud: Arc::new(CloudStorage::with_mock()),
                store,
                lock: crate::runtime::MetadataPublicationLock::default(),
            }
        }
        fn mirror(&self) -> MidgeResult<()> {
            mirror_control_metadata_within(
                CloudMetadataMirrorContext {
                    cloud: &self.cloud,
                    fs: self.fs.as_ref(),
                    publication_lock: &self.lock,
                    lock_wait_budget: std::time::Duration::from_secs(5),
                    deadline: &OperationDeadline::from_budget(std::time::Duration::from_secs(5)),
                    authority: CloudMetadataMirrorAuthority {
                        store: self.store.as_ref(),
                        holder_id: "writer",
                        writer_epoch: 1,
                    },
                },
                |_| Ok(()),
            )
        }
        fn generation(&self) -> CloudMetadataGeneration {
            let CloudMetadataHead::Committed(generation) = self
                .store
                .read_committed_metadata(std::time::Duration::from_secs(5))
                .unwrap()
            else {
                panic!("committed pointer required")
            };
            generation
        }
    }

    #[test]
    fn should_reuse_generation_when_control_bodies_are_unchanged() {
        // Arrange
        let fixture = Fixture::new();
        fixture.mirror().unwrap();
        let previous = fixture.generation();
        // Act
        fixture.mirror().unwrap();
        // Assert
        assert_eq!(fixture.generation(), previous);
        assert_eq!(previous.id, SharedGenerationId::FIRST);
    }

    #[test]
    fn should_recover_control_files_when_versioned_suffix_edits_cross_checkpoint_limit() {
        // Arrange
        let fixture = Fixture::new();
        fixture.mirror().unwrap();

        // Act
        for sequence in 1..=20 {
            crate::runtime::IntentPersistence::save_with_fs(
                &fixture.fs,
                &[crate::runtime::IntentLogEntry::SeqnoAllocated {
                    seqno: sequence,
                    cf_id: 0,
                }],
            )
            .unwrap();
            fixture.mirror().unwrap();
            assert!(
                fixture.generation().objects.len()
                    <= crate::metadata::cloud_manifest::MAX_CHAIN_OBJECTS
            );
        }
        let generation = fixture.generation();
        let bodies = read_committed_metadata_bodies_within(
            &fixture.cloud,
            &generation,
            &OperationDeadline::from_budget(std::time::Duration::from_secs(5)),
        )
        .unwrap();

        // Assert: the bounded pointer closure reconstructs exact local bytes,
        // including intent suffixes and a reset to a standalone snapshot.
        assert!(super::super::metadata_chain::is_chain(&generation));
        assert!(generation.objects.len() < 16);
        assert_eq!(
            bodies[files::MANIFEST_SNAPSHOT],
            crate::io::read_all(fixture.fs.as_ref(), &FsPath::new(files::MANIFEST_SNAPSHOT))
                .unwrap()
        );
        assert_eq!(
            bodies[files::INTENT_LOG],
            crate::io::read_all(fixture.fs.as_ref(), &FsPath::new(files::INTENT_LOG)).unwrap()
        );
    }

    #[test]
    fn should_ignore_uncommitted_high_ordinal_when_loading_cloud_manifest() {
        // Arrange
        let fixture = Fixture::new();
        fixture.mirror().unwrap();
        let committed = fixture.generation();
        let deadline = OperationDeadline::from_budget(std::time::Duration::from_secs(5));
        let context = CloudMetadataMirrorContext {
            cloud: &fixture.cloud,
            fs: fixture.fs.as_ref(),
            publication_lock: &fixture.lock,
            lock_wait_budget: std::time::Duration::from_secs(5),
            deadline: &deadline,
            authority: CloudMetadataMirrorAuthority {
                store: fixture.store.as_ref(),
                holder_id: "writer",
                writer_epoch: 1,
            },
        };
        let local = capture_local_metadata(&context).unwrap();
        // Upload a valid independent snapshot without selecting it in authority.
        super::super::metadata_chain::publish(
            &context,
            &local,
            None,
            None,
            None,
            SharedGenerationId::new(99).unwrap(),
        )
        .unwrap();

        // Act
        let bodies =
            read_committed_metadata_bodies_within(&fixture.cloud, &fixture.generation(), &deadline)
                .unwrap();

        // Assert
        assert_eq!(fixture.generation(), committed);
        assert_eq!(bodies, local);
    }

    #[test]
    fn should_advance_shared_ordinal_when_metadata_changes_without_a_journal_edit() {
        // Arrange
        let fixture = Fixture::new();
        fixture.mirror().unwrap();
        let previous = fixture.generation();
        crate::io::staging::stage_bytes(
            &fixture.fs,
            &FsPath::new("intent.tmp"),
            &FsPath::new(files::INTENT_LOG),
            b"{}",
            MidgeError::Internal,
        )
        .unwrap();
        // Act
        fixture.mirror().unwrap();
        // Assert
        let current = fixture.generation();
        assert_eq!(current.id, previous.id.checked_next().unwrap());
        assert_eq!(current.manifest_horizon, previous.manifest_horizon);
    }

    #[test]
    fn should_load_retirement_frontiers_only_from_the_committed_shared_bodies() {
        // Arrange
        let fixture = Fixture::new();
        fixture.mirror().unwrap();
        fixture
            .fs
            .remove_file(&FsPath::new(files::MANIFEST_SNAPSHOT))
            .unwrap();
        let snapshot = SharedMetadataSnapshot::new(
            Arc::clone(&fixture.cloud),
            ResourceBudget::new(1024 * 1024),
            fixture.store.clone(),
            "writer".into(),
            1,
            fixture.lock.clone(),
        );
        // Act
        let authority = snapshot
            .committed_retirement_authority_within(&OperationDeadline::from_budget(
                std::time::Duration::from_secs(5),
            ))
            .unwrap();
        // Assert
        assert_eq!(authority.generation_id(), SharedGenerationId::FIRST);
        assert_eq!(
            authority.frontiers().get(0).unwrap(),
            crate::metadata::frontiers::FamilyFrontier::Live {
                through_sequence: 0
            }
        );
    }

    #[test]
    fn should_not_grant_retirement_authority_without_a_shared_publication() {
        // Arrange
        let fixture = Fixture::new();
        let snapshot = SharedMetadataSnapshot::new(
            Arc::clone(&fixture.cloud),
            ResourceBudget::new(1024 * 1024),
            fixture.store.clone(),
            "writer".into(),
            1,
            fixture.lock.clone(),
        );
        // Act
        let result = snapshot.committed_retirement_authority_within(
            &OperationDeadline::from_budget(std::time::Duration::from_secs(5)),
        );
        // Assert
        assert!(matches!(result, Err(MidgeError::Corruption(_))));
    }

    fn manifest_at(through: u64, horizon: u64) -> Manifest {
        let mut value = serde_json::to_value(Manifest::default()).unwrap();
        value["flushed_through"][0]["through_sequence"] = through.into();
        value["edit_checkpoint_id"] = horizon.into();
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn should_refuse_regressed_frontiers_even_when_local_journal_horizon_is_greater() {
        // Arrange
        let fixture = Fixture::new();
        ManifestPersistence::save_with_fs(&fixture.fs, &manifest_at(40, 10)).unwrap();
        fixture.mirror().unwrap();
        let previous = fixture.generation();
        ManifestPersistence::save_with_fs(&fixture.fs, &manifest_at(20, 15)).unwrap();
        // Act
        let result = fixture.mirror();
        // Assert
        assert!(matches!(result, Err(MidgeError::Fenced(_))));
        assert_eq!(fixture.generation(), previous);
    }

    #[test]
    fn should_refuse_regressed_manifest_horizon_when_frontiers_are_unchanged() {
        // Arrange
        let fixture = Fixture::new();
        ManifestPersistence::save_with_fs(&fixture.fs, &manifest_at(40, 10)).unwrap();
        fixture.mirror().unwrap();
        let previous = fixture.generation();
        ManifestPersistence::save_with_fs(&fixture.fs, &manifest_at(40, 9)).unwrap();
        // Act
        let result = fixture.mirror();
        // Assert
        assert!(matches!(result, Err(MidgeError::Fenced(_))));
        assert_eq!(fixture.generation(), previous);
    }

    #[test]
    fn should_order_takeover_publications_independently_of_discarded_local_journal_ids() {
        // Arrange: successor hydrates shared horizon10; predecessor's unpublished
        // local edits11..15 do not become the shared publication clock.
        let fixture = Fixture::new();
        ManifestPersistence::save_with_fs(&fixture.fs, &manifest_at(40, 10)).unwrap();
        fixture.mirror().unwrap();
        let previous = fixture.generation();
        ManifestPersistence::save_with_fs(&fixture.fs, &manifest_at(50, 15)).unwrap();
        let successor = fixture.store.acquire_leadership("successor").unwrap();
        ManifestPersistence::save_with_fs(&fixture.fs, &manifest_at(45, 11)).unwrap();
        // Act
        mirror_control_metadata_within(
            CloudMetadataMirrorContext {
                cloud: &fixture.cloud,
                fs: fixture.fs.as_ref(),
                publication_lock: &fixture.lock,
                lock_wait_budget: std::time::Duration::from_secs(5),
                deadline: &OperationDeadline::from_budget(std::time::Duration::from_secs(5)),
                authority: CloudMetadataMirrorAuthority {
                    store: fixture.store.as_ref(),
                    holder_id: "successor",
                    writer_epoch: successor.epoch,
                },
            },
            |_| Ok(()),
        )
        .unwrap();
        // Assert
        let current = fixture.generation();
        assert_eq!(current.id, previous.id.checked_next().unwrap());
        assert_eq!(current.manifest_horizon, 11);
        assert!(fixture.mirror().is_err());
    }

    #[test]
    fn should_reject_hydration_before_writing_cache_when_catalog_retired_with_a_newer_generation() {
        // Arrange
        let fixture = Fixture::new();
        fixture.mirror().unwrap();
        let cache = tempfile::tempdir().unwrap();
        let root = crate::io::real::test_root(cache.path());
        // Act
        let result =
            crate::runtime::cloud_startup::CloudStartupRecovery::hydrate_cloud_metadata_within(
                &fixture.cloud,
                fixture.store.as_ref(),
                &root,
                cache.path(),
                crate::config::RecoveryPolicy::Salvage,
                SharedGenerationId::FIRST.checked_next().unwrap(),
                &DeadlineScope::new(OperationDeadline::from_budget(
                    std::time::Duration::from_secs(5),
                )),
            );
        // Assert
        assert!(matches!(result, Err(MidgeError::RecoveryFailed(_))));
        assert!(!root.exists(&FsPath::new(files::FORMAT)).unwrap());
        assert!(!root.exists(&FsPath::new(files::MANIFEST_SNAPSHOT)).unwrap());
    }
}
