//! Immutable local control cuts and bounded, exact coalescence witnesses.

use super::MetadataPublicationLock;
use crate::common::resource_budget::{ResourceBudget, ResourceReservation};
use crate::common::{DeadlineScope, MidgeError, MidgeResult, OperationDeadline};
use crate::io::{Fs, FsError, FsPath, MockFs, OpenOptions};
use crate::metadata::{files, Manifest, ManifestPersistence};
use crate::runtime::{IntentLogEntry, IntentPersistence, PublicationPhase};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Weak};

const MAX_CAPTURED_CUTS: usize = 64;
const COMPACT_WITNESS_BYTES_PER_ENCODED_BYTE: usize = 4;
// Old entries, a merged candidate and its final boxed slice can overlap during
// replacement. Removed receipt charges survive until old storage is freed.
const RECEIPT_COPIES: usize = 3;
type CopiedControlFiles = (Arc<dyn Fs>, BTreeMap<String, Vec<u8>>);

#[cfg(test)]
mod tests;
mod workspace;

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
    #[serde(deserialize_with = "deserialize_cut_witnesses")]
    covers: Vec<CutWitness>,
}

fn deserialize_cut_witnesses<'de, D: serde::Deserializer<'de>>(
    decoder: D,
) -> Result<Vec<CutWitness>, D::Error> {
    struct Witnesses;
    impl<'de> serde::de::Visitor<'de> for Witnesses {
        type Value = Vec<CutWitness>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("fewer than 64 metadata cut witnesses")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut sequence: A,
        ) -> Result<Self::Value, A::Error> {
            let hint = sequence.size_hint().unwrap_or(0);
            if hint >= MAX_CAPTURED_CUTS {
                return Err(serde::de::Error::custom(
                    "metadata cut witness count exceeds limit",
                ));
            }
            let mut witnesses = Vec::with_capacity(hint);
            while let Some(witness) = sequence.next_element()? {
                if witnesses.len() == MAX_CAPTURED_CUTS - 1 {
                    return Err(serde::de::Error::custom(
                        "metadata cut witness count exceeds limit",
                    ));
                }
                witnesses.push(witness);
            }
            Ok(witnesses)
        }
    }
    decoder.deserialize_seq(Witnesses)
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
    publication_intents: PublicationIntentBoundary,
    registry: Weak<parking_lot::Mutex<CutRegistry>>,
    staged: parking_lot::Mutex<Option<crate::runtime::hybrid_persistence::MetadataMirrorCache>>,
    memory: Arc<ResourceReservation>,
    terminal_witnesses: Option<Arc<SelectedTerminalWitnesses>>,
    _slot: ResourceReservation,
}

impl CapturedMetadataCut {
    pub(crate) fn terminal_witnesses(&self) -> Option<Arc<SelectedTerminalWitnesses>> {
        self.terminal_witnesses.clone()
    }
    pub(crate) fn fs(&self) -> &Arc<dyn Fs> {
        &self.fs
    }
    pub(crate) fn writer_epoch(&self) -> u64 {
        self.proof.writer_epoch
    }
    pub(crate) fn cut_ordinal(&self) -> u64 {
        self.proof.ordinal
    }
    pub(crate) fn memory_reservation(&self) -> Arc<ResourceReservation> {
        Arc::clone(&self.memory)
    }
    pub(crate) fn remember_staged(
        &self,
        cache: crate::runtime::hybrid_persistence::MetadataMirrorCache,
    ) {
        *self.staged.lock() = Some(cache);
    }
    pub(crate) fn mark_committed(&self) {
        if let Some(registry) = self.registry.upgrade() {
            let mut registry = registry.lock();
            if let Some(witnesses) = &self.terminal_witnesses {
                registry.published_intents.record(
                    &self.publication_intents.manifest_published,
                    self.manifest.edit_checkpoint_id,
                    &witnesses.memory,
                );
            }
            registry
                .pending
                .retain(|_, cut| !self.proof.covers(&cut.proof));
            prune_intent_receipts(&mut registry, &self.publication_intents);
        }
    }
}

impl Drop for CapturedMetadataCut {
    fn drop(&mut self) {
        let Some(registry) = self.registry.upgrade() else {
            return;
        };
        // Covered pending ownership may be dropped while the registry is
        // locked. That caller prunes after its batch; ordinary task drop can
        // prune here and release receipt/source accounting immediately.
        if let Some(mut registry) = registry.try_lock() {
            registry.owned.retain(|cut| cut.strong_count() > 0);
            prune_intent_receipts(&mut registry, &PublicationIntentBoundary::default());
        };
    }
}

struct PublishedIntentReceipt {
    horizon: u64,
    memory: Arc<ResourceReservation>,
}

type ReceiptEntry = ([u8; 32], PublishedIntentReceipt);

#[derive(Default)]
struct PublishedIntentReceipts(Box<[ReceiptEntry]>);

impl PublishedIntentReceipts {
    fn record(
        &mut self,
        digests: &BTreeSet<[u8; 32]>,
        horizon: u64,
        memory: &Arc<ResourceReservation>,
    ) {
        if digests.is_empty() {
            return;
        }
        let mut old = std::mem::take(&mut self.0)
            .into_vec()
            .into_iter()
            .peekable();
        let mut merged = Vec::with_capacity(old.len().saturating_add(digests.len()));
        for digest in digests {
            while old.peek().is_some_and(|(key, _)| key < digest) {
                merged.push(old.next().expect("peeked receipt"));
            }
            if old.peek().is_some_and(|(key, _)| key == digest) {
                let (key, mut receipt) = old.next().expect("peeked receipt");
                receipt.horizon = receipt.horizon.max(horizon);
                merged.push((key, receipt));
            } else {
                merged.push((
                    *digest,
                    PublishedIntentReceipt {
                        horizon,
                        memory: Arc::clone(memory),
                    },
                ));
            }
        }
        merged.extend(&mut old);
        drop(old);
        self.0 = merged.into_boxed_slice();
    }

    fn get(&self, digest: &[u8; 32]) -> Option<&PublishedIntentReceipt> {
        self.0
            .binary_search_by_key(digest, |(key, _)| *key)
            .ok()
            .map(|index| &self.0[index].1)
    }

    fn retain(&mut self, mut keep: impl FnMut(&[u8; 32]) -> bool) {
        let mut entries = std::mem::take(&mut self.0).into_vec();
        let mut removed = Vec::with_capacity(entries.len());
        entries.retain(|(digest, receipt)| {
            if keep(digest) {
                true
            } else {
                removed.push(Arc::clone(&receipt.memory));
                false
            }
        });
        self.0 = entries.into_boxed_slice();
        // Release removed charges after their old allocation, including the
        // final empty box, has actually been freed by into_boxed_slice.
        drop(removed);
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.0.len()
    }
}

/// Compact evidence derived from one exact control body/cut. It becomes
/// authoritative only when the matching cache is installed after selection.
pub(crate) struct SelectedTerminalWitnesses {
    writer_epoch: u64,
    session: uuid::Uuid,
    ordinal: u64,
    digest: [u8; 32],
    horizon: u64,
    digests: Box<[[u8; 32]]>,
    memory: Arc<ResourceReservation>,
}

impl SelectedTerminalWitnesses {
    fn allocation_bytes(memory: &ResourceReservation, entries: usize) -> MidgeResult<usize> {
        if entries == 0 {
            return Ok(0);
        }
        entries
            .checked_mul(
                std::mem::size_of::<[u8; 32]>()
                    + RECEIPT_COPIES * std::mem::size_of::<ReceiptEntry>(),
            )
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>()))
            .and_then(|bytes| bytes.checked_add(memory.allocation_bytes()))
            .and_then(|bytes| bytes.checked_add(4 * std::mem::size_of::<usize>()))
            .ok_or_else(|| {
                MidgeError::ResourceLimit("selected intent witness size overflow".into())
            })
    }

    fn prepare(
        memory: &mut Arc<ResourceReservation>,
        proof: &MetadataCutProof,
        horizon: u64,
        digests: &BTreeSet<[u8; 32]>,
        encoded: usize,
    ) -> MidgeResult<Option<Arc<Self>>> {
        if digests.is_empty() {
            return Ok(None);
        }
        // Account the exact retained digest payload and requested struct/
        // reservation storage, plus any receipt nodes whose final source Drop
        // cannot prune immediately. UUID and boxed digests avoid a separate
        // compact-tree allocation even for a single terminal intent.
        let bytes = Self::allocation_bytes(memory, digests.len())?;
        if bytes > encoded.saturating_mul(COMPACT_WITNESS_BYTES_PER_ENCODED_BYTE) {
            return Err(MidgeError::ResourceLimit(
                "compact selected witness exceeds its encoded metadata allowance".into(),
            ));
        }
        let reservation = Arc::get_mut(memory)
            .ok_or_else(|| {
                MidgeError::Internal("new metadata workspace was already shared".into())
            })?
            .split(bytes)?;
        Ok(Some(Arc::new(Self {
            writer_epoch: proof.writer_epoch,
            session: uuid::Uuid::parse_str(&proof.session).map_err(|_| {
                MidgeError::Corruption("selected intent witness has invalid session".into())
            })?,
            ordinal: proof.ordinal,
            digest: proof.digest,
            horizon,
            digests: digests.iter().copied().collect(),
            memory: Arc::new(reservation),
        })))
    }
}

struct SelectedTerminalCache {
    generation: crate::lease::SharedGenerationId,
    witnesses: Option<Arc<SelectedTerminalWitnesses>>,
}

pub(super) struct CutRegistry {
    epoch: u64,
    session: String,
    next: u64,
    owned: Vec<Weak<CapturedMetadataCut>>,
    pending: BTreeMap<u64, Arc<CapturedMetadataCut>>,
    /// Only selected `ManifestPublished` phases discharge an owned operation's
    /// intent. An `OutputDurable` cut receipt proves a weaker event.
    published_intents: PublishedIntentReceipts,
    selected_terminal: Option<SelectedTerminalCache>,
}

impl Default for CutRegistry {
    fn default() -> Self {
        Self {
            epoch: 0,
            session: uuid::Uuid::new_v4().to_string(),
            next: 1,
            owned: Vec::new(),
            pending: BTreeMap::new(),
            published_intents: PublishedIntentReceipts::default(),
            selected_terminal: None,
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
        let slot = self
            .capture_slots
            .get_or_init(|| ResourceBudget::new(MAX_CAPTURED_CUTS).with_contention_errors())
            .reserve(1, "owned metadata cut slot")?;
        let mut memory = self.reserve_capture_memory(source, deadline)?;
        let (fs, bodies) = copy_control_files(source, deadline)?;
        self.ensure_capture_workspace(&mut memory, &bodies, deadline)?;
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
        prune_intent_receipts(&mut registry, &publication_intents);
        if registry.epoch != epoch {
            if !registry.owned.is_empty() {
                return Err(MidgeError::Fenced(
                    "metadata cuts from another epoch are still owned".into(),
                ));
            }
            registry.epoch = epoch;
            registry.session = uuid::Uuid::new_v4().to_string();
            registry.next = 1;
            registry.selected_terminal = None;
        }
        restore_owned_terminal_receipts(&mut registry, manifest.edit_checkpoint_id);
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
        let terminal_witnesses = SelectedTerminalWitnesses::prepare(
            &mut memory,
            &proof,
            manifest.edit_checkpoint_id,
            &publication_intents.manifest_published,
            bodies.values().map(Vec::len).sum(),
        )?;
        let cut = Arc::new(CapturedMetadataCut {
            fs,
            proof,
            manifest,
            publication_intents,
            registry: Arc::downgrade(&self.cuts),
            memory,
            terminal_witnesses,
            _slot: slot,
            staged: parking_lot::Mutex::new(None),
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

    /// A validated immutable PUT plus the exact fresh selected pointer is a
    /// complete readback proof. Reuse its owned workspace rather than require
    /// another full allocation to resolve a lost pointer-CAS callback.
    pub(crate) fn staged_metadata_cache(
        &self,
        generation: &crate::lease::CloudMetadataGeneration,
        epoch: u64,
    ) -> Option<(
        crate::runtime::hybrid_persistence::MetadataMirrorCache,
        Arc<ResourceReservation>,
    )> {
        let registry = self.cuts.lock();
        for cut in registry.owned.iter().filter_map(Weak::upgrade) {
            let staged = cut.staged.lock();
            if let Some(cache) = staged
                .as_ref()
                .filter(|cache| cache.matches(generation, epoch))
            {
                return Some((cache.clone(), cut.memory_reservation()));
            }
        }
        None
    }

    fn reserve_capture_memory(
        &self,
        fs: &Arc<dyn Fs>,
        deadline: &OperationDeadline,
    ) -> MidgeResult<Arc<ResourceReservation>> {
        let mut encoded = 0_usize;
        for name in files::CLOUD_MIRRORED {
            DeadlineScope::new(*deadline).check("metadata capture admission")?;
            let length = match fs.metadata(&FsPath::new(*name)) {
                Ok(metadata) => metadata.len,
                Err(FsError::NotFound(_)) => continue,
                Err(error) => return Err(error.into_midge()),
            };
            encoded = usize::try_from(length)
                .ok()
                .and_then(|length| encoded.checked_add(length))
                .ok_or_else(|| {
                    MidgeError::ResourceLimit(
                        "metadata capture length overflows address space".into(),
                    )
                })?;
        }
        self.reserve_metadata_body_memory(u64::try_from(encoded).unwrap_or(u64::MAX))
    }

    pub(crate) fn reserve_metadata_body_memory(
        &self,
        encoded: u64,
    ) -> MidgeResult<Arc<ResourceReservation>> {
        self.prune_released_cut_receipts();
        if encoded > crate::metadata::cloud_manifest::MAX_CHAIN_BYTES {
            return Err(MidgeError::ResourceLimit(
                "metadata capture exceeds cloud body bound".into(),
            ));
        }
        let encoded = usize::try_from(encoded).map_err(|_| {
            MidgeError::ResourceLimit("metadata body length exceeds address space".into())
        })?;
        let admitted = encoded
            .checked_mul(16)
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or_else(|| {
                MidgeError::ResourceLimit(
                    "metadata capture workspace overflows address space".into(),
                )
            })?;
        let pool = self.capture_memory.get_or_init(|| {
            ResourceBudget::new(crate::compaction::DEFAULT_COMPACTION_MEMORY_LIMIT)
                .with_contention_errors()
        });
        if admitted > pool.limit() {
            return pool
                .reserve(admitted, "owned metadata capture and decoder workspace")
                .map(Arc::new);
        }
        let credit = self
            .cuts
            .lock()
            .selected_terminal
            .as_ref()
            .and_then(|cached| cached.witnesses.as_ref())
            .map(|witnesses| Arc::clone(&witnesses.memory));
        // This minimum admits raw copies before shape preflight; it is not a
        // universal typed-decoder bound. Preflight grows independent charge
        // before allocating typed structures. Credit only includes an actual
        // retained compact witness, capped at 4x encoded bytes; larger caches
        // remain separately charged.
        let credit = credit.filter(|credit| {
            credit.bytes() <= encoded.saturating_mul(COMPACT_WITNESS_BYTES_PER_ENCODED_BYTE)
        });
        match credit {
            Some(credit) => pool.reserve_with_shared_credit(
                admitted,
                &credit,
                "owned metadata capture and decoder workspace",
            ),
            None => pool.reserve(admitted, "owned metadata capture and decoder workspace"),
        }
        .map(Arc::new)
    }

    fn prune_released_cut_receipts(&self) {
        // Final source drop can skip its opportunistic prune while any
        // registry reader owns the mutex. Cleanup must precede admission;
        // otherwise its obsolete receipt can occupy all available workspace.
        let mut registry = self.cuts.lock();
        registry.owned.retain(|cut| cut.strong_count() > 0);
        prune_intent_receipts(&mut registry, &PublicationIntentBoundary::default());
    }
}

impl MetadataPublicationLock {
    pub(crate) fn note_selected_terminal_witnesses(
        &self,
        generation: crate::lease::SharedGenerationId,
        proof: &MetadataCutProof,
        witnesses: Option<Arc<SelectedTerminalWitnesses>>,
    ) -> MidgeResult<()> {
        let mut registry = self.cuts.lock();
        if registry.epoch != proof.writer_epoch || registry.session != proof.session {
            return Ok(());
        }
        if witnesses.as_ref().is_some_and(|witnesses| {
            witnesses.writer_epoch != proof.writer_epoch
                || Some(witnesses.session) != uuid::Uuid::parse_str(&proof.session).ok()
                || witnesses.ordinal != proof.ordinal
                || witnesses.digest != proof.digest
        }) {
            return Err(MidgeError::Corruption(
                "selected terminal witness differs from its pinned control cut".into(),
            ));
        }
        if registry
            .selected_terminal
            .as_ref()
            .is_none_or(|current| current.generation <= generation)
        {
            registry.selected_terminal = Some(SelectedTerminalCache {
                generation,
                witnesses,
            });
        }
        Ok(())
    }
}

fn restore_owned_terminal_receipts(registry: &mut CutRegistry, horizon: u64) {
    let Some(selected) = registry
        .selected_terminal
        .as_ref()
        .and_then(|selected| selected.witnesses.as_ref())
        .cloned()
    else {
        return;
    };
    if selected.horizon > horizon {
        return;
    }
    let needed = registry
        .owned
        .iter()
        .filter_map(Weak::upgrade)
        .flat_map(|prior| prior.publication_intents.all.clone())
        .filter(|digest| selected.digests.binary_search(digest).is_ok())
        .collect();
    registry
        .published_intents
        .record(&needed, selected.horizon, &selected.memory);
}

fn coverage_witnesses(
    registry: &CutRegistry,
    manifest: &Manifest,
    intents: &PublicationIntentBoundary,
) -> MidgeResult<Vec<CutWitness>> {
    let mut witnesses = Vec::new();
    for prior in registry.owned.iter().filter_map(Weak::upgrade) {
        crate::runtime::hybrid_persistence::metadata_snapshot::validate_manifest_successor(
            manifest,
            &prior.manifest,
        )?;
        for digest in &prior.publication_intents.all {
            if !intents.all.contains(digest)
                && registry
                    .published_intents
                    .get(digest)
                    .is_none_or(|receipt| receipt.horizon > manifest.edit_checkpoint_id)
            {
                return Err(MidgeError::Busy(format!(
                    "new metadata cut removes an intent without selected ManifestPublished proof \
                     (intent={digest:02x?}, prior_cut={}, prior_horizon={}, candidate_horizon={}, \
                     selected_horizon={:?}, prior_manifest_published={})",
                    prior.cut_ordinal(),
                    prior.manifest.edit_checkpoint_id,
                    manifest.edit_checkpoint_id,
                    registry
                        .published_intents
                        .get(digest)
                        .map(|receipt| receipt.horizon),
                    prior
                        .publication_intents
                        .manifest_published
                        .contains(digest),
                )));
            }
            if prior
                .publication_intents
                .manifest_published
                .contains(digest)
                && intents.all.contains(digest)
                && !intents.manifest_published.contains(digest)
            {
                return Err(MidgeError::Busy(
                    "new metadata cut regresses a publication intent phase".into(),
                ));
            }
        }
        witnesses.push(prior.proof.witness());
    }
    Ok(witnesses)
}

#[derive(Default)]
struct PublicationIntentBoundary {
    all: BTreeSet<[u8; 32]>,
    manifest_published: BTreeSet<[u8; 32]>,
}

fn prune_intent_receipts(registry: &mut CutRegistry, current: &PublicationIntentBoundary) {
    let mut needed = current.all.clone();
    for cut in registry.owned.iter().filter_map(Weak::upgrade) {
        needed.extend(&cut.publication_intents.all);
    }
    registry
        .published_intents
        .retain(|digest| needed.contains(digest));
}

fn publication_intents(fs: &Arc<dyn Fs>) -> MidgeResult<PublicationIntentBoundary> {
    let mut boundary = PublicationIntentBoundary::default();
    for mut intent in
        IntentPersistence::load_with_fs_and_policy_typed(fs, crate::config::RecoveryPolicy::Strict)?
    {
        let published = match &mut intent {
            IntentLogEntry::FlushPublish { phase, .. }
            | IntentLogEntry::CompactionPublish { phase, .. }
            | IntentLogEntry::MoveSstsPublish { phase, .. } => {
                let published = *phase == PublicationPhase::ManifestPublished;
                *phase = PublicationPhase::OutputDurable;
                published
            }
            _ => continue,
        };
        let digest = Sha256::digest(crate::metadata::codec::encode(
            crate::metadata::codec::INTENT,
            &intent,
        )?)
        .into();
        if !boundary.all.insert(digest) {
            return Err(MidgeError::Corruption(
                "metadata cut repeats a publication intent identity".into(),
            ));
        }
        if published {
            boundary.manifest_published.insert(digest);
        }
    }
    Ok(boundary)
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
