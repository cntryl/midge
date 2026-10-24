//! Immutable local control cuts and bounded, exact coalescence witnesses.

use super::MetadataPublicationLock;
use crate::common::{DeadlineScope, MidgeError, MidgeResult, OperationDeadline};
use crate::io::{Fs, FsError, FsPath, MockFs, OpenOptions};
use crate::metadata::{files, Manifest, ManifestPersistence};
use crate::runtime::{IntentLogEntry, IntentPersistence, PublicationPhase};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

const MAX_CAPTURED_CUTS: usize = 64;
type CopiedControlFiles = (Arc<dyn Fs>, BTreeMap<String, Vec<u8>>);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CutWitness {
    ordinal: u64,
    digest: [u8; 32],
}

/// The pointer pins the manifest body containing this proof. Each witness
/// attests to one exact, still-owned earlier capture, not just a high-water mark.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MetadataCutProof {
    pub(crate) writer_epoch: u64,
    session: String,
    pub(crate) ordinal: u64,
    digest: [u8; 32],
    covers: Vec<CutWitness>,
}

impl MetadataCutProof {
    pub(crate) fn covers(&self, earlier: &Self) -> bool {
        self.writer_epoch == earlier.writer_epoch
            && self.session == earlier.session
            && (self == earlier
                || (self.ordinal > earlier.ordinal && self.covers.contains(&earlier.witness())))
    }

    pub(crate) fn same_lineage(&self, other: &Self) -> bool {
        self.writer_epoch == other.writer_epoch && self.session == other.session
    }

    fn witness(&self) -> CutWitness {
        CutWitness {
            ordinal: self.ordinal,
            digest: self.digest,
        }
    }

    pub(crate) fn validate(&self, bodies: &BTreeMap<String, Vec<u8>>) -> MidgeResult<()> {
        if self.writer_epoch == 0
            || self.ordinal == 0
            || uuid::Uuid::parse_str(&self.session).is_err()
            || self.covers.len() >= MAX_CAPTURED_CUTS
            || self.digest != digest_bodies(bodies)
            || self
                .covers
                .iter()
                .any(|cut| cut.ordinal == 0 || cut.ordinal >= self.ordinal)
            || self
                .covers
                .windows(2)
                .any(|pair| pair[0].ordinal >= pair[1].ordinal)
        {
            return Err(MidgeError::Corruption(
                "cloud metadata cut proof differs from its exact body".into(),
            ));
        }
        Ok(())
    }
}

pub(crate) struct CapturedMetadataCut {
    fs: Arc<dyn Fs>,
    pub(crate) proof: MetadataCutProof,
    manifest: Manifest,
    publication_intents: BTreeSet<[u8; 32]>,
    terminal: AtomicBool,
    registry: Weak<parking_lot::Mutex<CutRegistry>>,
}

impl CapturedMetadataCut {
    pub(crate) fn fs(&self) -> &Arc<dyn Fs> {
        &self.fs
    }
    pub(crate) fn writer_epoch(&self) -> u64 {
        self.proof.writer_epoch
    }
    pub(crate) fn cut_ordinal(&self) -> u64 {
        self.proof.ordinal
    }
    pub(crate) fn mark_committed(&self) {
        self.terminal.store(true, Ordering::Release);
        if let Some(registry) = self.registry.upgrade() {
            registry.lock().pending.retain(|_, cut| {
                if self.proof.covers(&cut.proof) {
                    cut.terminal.store(true, Ordering::Release);
                    false
                } else {
                    true
                }
            });
        }
    }
}

pub(super) struct CutRegistry {
    epoch: u64,
    session: String,
    next: u64,
    owned: Vec<Weak<CapturedMetadataCut>>,
    pending: BTreeMap<u64, Arc<CapturedMetadataCut>>,
}

impl Default for CutRegistry {
    fn default() -> Self {
        Self {
            epoch: 0,
            session: uuid::Uuid::new_v4().to_string(),
            next: 1,
            owned: Vec::new(),
            pending: BTreeMap::new(),
        }
    }
}

impl MetadataPublicationLock {
    /// Capture and admission share the same local lock as reclamation. Remote
    /// publication never holds this lock. Intent and checkpoint writers share
    /// the manifest writer lock, making all four files one exact local cut.
    pub(crate) fn capture_control_cut(
        &self,
        fs: &Arc<dyn Fs>,
        epoch: u64,
        deadline: &OperationDeadline,
    ) -> MidgeResult<Arc<CapturedMetadataCut>> {
        let _local = self.lock_for(deadline.remaining()).ok_or_else(|| {
            MidgeError::Timeout("metadata cut capture waits for local turn".into())
        })?;
        crate::metadata::journal::with_manifest_writer_lock_within(
            fs,
            &DeadlineScope::new(*deadline),
            || self.capture_locked(fs, epoch, deadline),
        )
    }

    fn capture_locked(
        &self,
        source: &Arc<dyn Fs>,
        epoch: u64,
        deadline: &OperationDeadline,
    ) -> MidgeResult<Arc<CapturedMetadataCut>> {
        if epoch == 0 {
            return Err(MidgeError::Fenced(
                "metadata cut requires an active epoch".into(),
            ));
        }
        let (fs, bodies) = copy_control_files(source, deadline)?;
        let snapshot = bodies
            .get(files::MANIFEST_SNAPSHOT)
            .ok_or_else(|| MidgeError::Corruption("metadata cut lacks manifest snapshot".into()))?;
        let mut manifest = ManifestPersistence::decode_bodies(
            snapshot,
            bodies.get(files::JOURNAL).map(Vec::as_slice),
            &DeadlineScope::new(*deadline),
        )?;
        crate::runtime::hybrid_persistence::metadata_snapshot::reduce_to_authority(&mut manifest);
        let publication_intents = publication_intents(&fs)?;
        let mut registry = self.cuts.lock();
        registry.owned.retain(|cut| cut.strong_count() > 0);
        if registry.owned.len() >= MAX_CAPTURED_CUTS {
            return Err(MidgeError::Busy("too many owned metadata cuts".into()));
        }
        if registry.epoch != epoch {
            if !registry.owned.is_empty() {
                return Err(MidgeError::Fenced(
                    "metadata cuts from another epoch are still owned".into(),
                ));
            }
            registry.epoch = epoch;
            registry.session = uuid::Uuid::new_v4().to_string();
            registry.next = 1;
        }
        let covers = coverage_witnesses(&registry, &manifest, &publication_intents)?;
        let proof = MetadataCutProof {
            writer_epoch: epoch,
            session: registry.session.clone(),
            ordinal: registry.next,
            digest: digest_bodies(&bodies),
            covers,
        };
        registry.next = registry
            .next
            .checked_add(1)
            .ok_or_else(|| MidgeError::ResourceLimit("metadata cut ordinal exhausted".into()))?;
        let cut = Arc::new(CapturedMetadataCut {
            fs,
            proof,
            manifest,
            publication_intents,
            terminal: AtomicBool::new(false),
            registry: Arc::downgrade(&self.cuts),
        });
        registry.owned.push(Arc::downgrade(&cut));
        registry.pending.insert(cut.cut_ordinal(), Arc::clone(&cut));
        Ok(cut)
    }

    /// Callers hold the local publication lock across this check and any
    /// physical reclamation, excluding admission of a conflicting capture.
    pub(crate) fn pending_captures(&self) -> usize {
        self.cuts.lock().pending.len()
    }
}

fn coverage_witnesses(
    registry: &CutRegistry,
    manifest: &Manifest,
    intents: &BTreeSet<[u8; 32]>,
) -> MidgeResult<Vec<CutWitness>> {
    let mut witnesses = Vec::new();
    for prior in registry.owned.iter().filter_map(Weak::upgrade) {
        crate::runtime::hybrid_persistence::metadata_snapshot::validate_manifest_successor(
            manifest,
            &prior.manifest,
        )?;
        if !prior.terminal.load(Ordering::Acquire) && !prior.publication_intents.is_subset(intents)
        {
            return Err(MidgeError::Busy(
                "new metadata cut removes an unsettled publication intent".into(),
            ));
        }
        witnesses.push(prior.proof.witness());
    }
    Ok(witnesses)
}

fn publication_intents(fs: &Arc<dyn Fs>) -> MidgeResult<BTreeSet<[u8; 32]>> {
    IntentPersistence::load_with_fs_and_policy_typed(fs, crate::config::RecoveryPolicy::Strict)?
        .into_iter()
        .filter_map(|mut intent| match &mut intent {
            IntentLogEntry::FlushPublish { phase, .. }
            | IntentLogEntry::CompactionPublish { phase, .. }
            | IntentLogEntry::MoveSstsPublish { phase, .. } => {
                *phase = PublicationPhase::OutputDurable;
                Some(
                    crate::metadata::codec::encode(crate::metadata::codec::INTENT, &intent)
                        .map(|bytes| Sha256::digest(&bytes).into()),
                )
            }
            _ => None,
        })
        .collect()
}

fn copy_control_files(
    source: &Arc<dyn Fs>,
    deadline: &OperationDeadline,
) -> MidgeResult<CopiedControlFiles> {
    let target: Arc<dyn Fs> = Arc::new(MockFs::new());
    let mut bodies = BTreeMap::new();
    let mut total = 0_u64;
    for name in files::CLOUD_MIRRORED {
        DeadlineScope::new(*deadline).check("metadata control cut capture")?;
        let path = FsPath::new(*name);
        let bytes = match crate::io::read_all(source.as_ref(), &path) {
            Ok(bytes) => bytes,
            Err(FsError::NotFound(_)) => continue,
            Err(error) => return Err(error.into_midge()),
        };
        total = total.saturating_add(bytes.len() as u64);
        if total > crate::metadata::cloud_manifest::MAX_CHAIN_BYTES {
            return Err(MidgeError::ResourceLimit(
                "captured metadata exceeds cloud body bound".into(),
            ));
        }
        target
            .open(
                &path,
                OpenOptions {
                    mode: crate::io::OpenMode::ReadWrite,
                    create: true,
                    create_new: true,
                    truncate: false,
                },
            )
            .and_then(|mut file| file.append(bytes.clone()))
            .map_err(FsError::into_midge)?;
        bodies.insert((*name).into(), bytes.to_vec());
    }
    if bodies.get(files::FORMAT) != Some(&crate::metadata::format::current_format_marker_bytes()) {
        return Err(MidgeError::Corruption(
            "metadata cut lacks current FORMAT".into(),
        ));
    }
    Ok((target, bodies))
}

fn digest_bodies(bodies: &BTreeMap<String, Vec<u8>>) -> [u8; 32] {
    let mut digest = Sha256::new();
    for name in files::CLOUD_MIRRORED {
        digest.update((name.len() as u64).to_be_bytes());
        digest.update(name.as_bytes());
        if let Some(bytes) = bodies.get(*name) {
            digest.update([1]);
            digest.update((bytes.len() as u64).to_be_bytes());
            digest.update(bytes);
        } else {
            digest.update([0]);
        }
    }
    digest.finalize().into()
}
