//! Lease-fenced authority for remotely published WAL segments (catalog v3,
//! #844).
//!
//! Immutable WAL objects are not authoritative merely because they exist.
//! Only entries in this catalog are eligible for recovery. The catalog is a
//! chain, like the segment ledger: its `anchor` is the link of the last
//! segment it retired (the genesis link before any), each entry links the
//! one before it, and `next_segment_id` passes every id a writer ever
//! published. Retiring entries therefore never loses the chain or reuses an
//! id, however much of the catalog retires.
//!
//! Every mutation is conditioned on the catalog object's provider identity
//! and on the lease epoch. The mutators here are pure: each checks
//! everything before it changes anything and leaves a valid catalog, or
//! refuses and leaves it as it was.

use crate::types::SharedGenerationId;
use crate::wal::format::{ChainLink, SealedFacts, SegmentId, SegmentIdentity};
use serde::{Deserialize, Serialize};
mod allocation;

/// The catalog format this build reads and writes. Any other version is a
/// compatibility error; nothing migrates it.
pub(crate) const FORMAT_VERSION: u32 = 3;
pub(crate) const OBJECT_KEY: &str = crate::cloud_layout::CloudObjectLayout::WAL_CATALOG_OBJECT_KEY;
pub(crate) const MIRROR_OBJECT_KEY: &str =
    crate::cloud_layout::CloudObjectLayout::WAL_CATALOG_MIRROR_OBJECT_KEY;

/// One published segment: the epoch of the writer that wrote it, where it
/// sits in the chain, and the size and checksum of its object.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogEntry {
    epoch: u64,
    prev: ChainLink,
    facts: SealedFacts,
    size_bytes: u64,
    crc32c: u32,
}

impl CatalogEntry {
    /// The entry publishing `bytes`, a sealed segment holding at least one
    /// record.
    ///
    /// # Errors
    ///
    /// Why the bytes are not such a segment.
    #[cfg(any(test, feature = "internal-testing"))]
    pub fn from_sealed_bytes(bytes: &[u8]) -> Result<Self, String> {
        let validation =
            crate::wal::cloud_segment::validate_segment_bytes("sealed segment", bytes)?;
        Ok(Self::describing(&validation, bytes))
    }

    /// The entry for `bytes`, a segment that says it is `identity`.
    pub(crate) fn describing(
        validation: &crate::wal::cloud_segment::SegmentValidation,
        bytes: &[u8],
    ) -> Self {
        let identity = validation.identity();
        Self {
            epoch: identity.epoch,
            prev: identity.prev,
            facts: validation.facts.clone(),
            size_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            crc32c: crc32c::crc32c(bytes),
        }
    }

    /// The segment's id.
    #[must_use]
    pub fn id(&self) -> SegmentId {
        self.facts.link.id
    }

    /// The last sequence the segment holds.
    #[must_use]
    pub fn max_sequence(&self) -> u64 {
        self.facts.link.last_seq
    }

    /// The epoch of the writer that wrote the segment.
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// The link of the segment before it.
    #[must_use]
    pub fn prev(&self) -> ChainLink {
        self.prev
    }

    /// The link the segment's footer seals.
    #[must_use]
    pub fn link(&self) -> ChainLink {
        self.facts.link
    }

    /// Verified sealing facts retained after local cache retirement.
    #[must_use]
    pub fn facts(&self) -> &SealedFacts {
        &self.facts
    }

    pub(crate) fn check_facts(&self, facts: &SealedFacts) -> Result<(), String> {
        if &self.facts != facts {
            return Err(format!(
                "cloud WAL segment '{}' footer facts differ from catalog",
                self.object_key()
            ));
        }
        Ok(())
    }

    /// The size of the segment's object.
    #[must_use]
    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    /// The crc32c of the segment's object.
    #[must_use]
    pub fn crc32c(&self) -> u32 {
        self.crc32c
    }

    /// The object holding the segment, named for its id and epoch.
    #[must_use]
    pub fn object_key(&self) -> String {
        crate::wal::segment_object_key(self.id(), self.epoch)
    }

    /// Whether `bytes`, read from [`Self::object_key`], are the segment this
    /// entry publishes: its size and crc32c, and a sealed segment whose
    /// header and footer name this entry's epoch and chain position.
    ///
    /// # Errors
    ///
    /// What differs, naming the object.
    pub(crate) fn validate_bytes(&self, bytes: &[u8]) -> Result<(), String> {
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != self.size_bytes {
            return Err(format!(
                "published cloud WAL segment {} size {} does not match catalog {}",
                self.id(),
                bytes.len(),
                self.size_bytes
            ));
        }
        let actual_crc = crc32c::crc32c(bytes);
        if actual_crc != self.crc32c {
            return Err(format!(
                "published cloud WAL segment {} crc32c {actual_crc:08x} does not match catalog {:08x}",
                self.id(),
                self.crc32c
            ));
        }
        let validation =
            crate::wal::cloud_segment::validate_segment_bytes(&self.object_key(), bytes)?;
        self.check(&validation.identity())?;
        self.check_facts(&validation.facts)
    }

    /// Whether a segment that says it is `identity` is the one this entry
    /// names (#843, #844): the same writer epoch, the same link before it
    /// and the same sealed link. The one identity check every reader of a
    /// published or publishing segment applies.
    ///
    /// # Errors
    ///
    /// What differs, naming the object.
    pub(crate) fn check(&self, identity: &SegmentIdentity) -> Result<(), String> {
        let key = self.object_key();
        if identity.link.id != self.id() {
            return Err(format!(
                "cloud WAL segment '{key}' header names segment {}, not {}",
                identity.link.id,
                self.id()
            ));
        }
        if identity.epoch != self.epoch {
            return Err(format!(
                "cloud WAL segment '{key}' header names writer epoch {}, not {}",
                identity.epoch, self.epoch
            ));
        }
        if identity.prev != self.prev {
            return Err(format!(
                "cloud WAL segment '{key}' links segment {} (last sequence {}), not the catalog's segment {} (last sequence {})",
                identity.prev.id, identity.prev.last_seq, self.prev.id, self.prev.last_seq
            ));
        }
        if identity.link != self.facts.link {
            return Err(format!(
                "cloud WAL segment '{key}' seals sequences through {} with another digest than the catalog's, through {}",
                identity.link.last_seq, self.facts.link.last_seq
            ));
        }
        Ok(())
    }
}

/// Why a catalog mutation is refused. A refusal changes nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CatalogRefusal {
    /// Another writer epoch owns the catalog.
    EpochMismatch { catalog: u64, writer: u64 },
    /// Only the catalog's oldest entries may retire, in order.
    NotOldestPrefix {
        segment_id: SegmentId,
        oldest: Option<SegmentId>,
    },
    /// The catalog publishes something else for this segment than the
    /// caller expected.
    Changed { segment_id: SegmentId },
    GenerationRegression {
        retired_by: SharedGenerationId,
        proposed: SharedGenerationId,
    },
    /// Only a suffix of the catalog may be set aside, as the caller saw it.
    NotSuffix { from: SegmentId },
}

impl std::fmt::Display for CatalogRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EpochMismatch { catalog, writer } => write!(
                f,
                "cloud WAL catalog mutation requires fencing epoch {catalog}, writer epoch {writer} rejected"
            ),
            Self::NotOldestPrefix { segment_id, oldest } => write!(
                f,
                "cloud WAL segment {segment_id} cannot retire before the catalog's oldest segment {}",
                oldest.map_or_else(|| "(none)".to_string(), |oldest| oldest.to_string())
            ),
            Self::Changed { segment_id } => write!(
                f,
                "cloud WAL catalog segment {segment_id} changed before the mutation"
            ),
            Self::GenerationRegression { retired_by, proposed } => write!(f, "shared retirement generation {} is below required {}", proposed.get(), retired_by.get()),
            Self::NotSuffix { from } => write!(
                f,
                "cloud WAL catalog segments from {from} changed before they were set aside"
            ),
        }
    }
}

/// Why catalog bytes do not decode.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CatalogDecodeError {
    /// Another format version.
    Incompatible(u32),
    /// Not a valid catalog of this format.
    Invalid(String),
}

impl std::fmt::Display for CatalogDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Incompatible(version) => write!(
                f,
                "cloud WAL publication catalog format version {version} is not supported; this build reads version {FORMAT_VERSION}"
            ),
            Self::Invalid(reason) => {
                write!(f, "invalid cloud WAL publication catalog: {reason}")
            }
        }
    }
}

/// The catalog. See the module documentation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WalPublicationCatalog {
    format_version: u32,
    fencing_epoch: u64,
    /// The link the first entry names: the last retired segment's, or the
    /// genesis link.
    anchor: ChainLink,
    /// Past every id any writer published.
    next_segment_id: SegmentId,
    /// Every sequence at or below this may belong to WAL salvage set aside
    /// unreplayed, so a later open must not assign it again (#529).
    seq_floor: u64,
    retired_by: SharedGenerationId,
    /// Oldest first; each links the one before it.
    segments: Vec<CatalogEntry>,
}

/// Only the version, read before the rest so another format is reported as
/// such rather than as an invalid document.
#[derive(Deserialize)]
struct VersionProbe {
    format_version: Option<u32>,
}

impl WalPublicationCatalog {
    /// A catalog nothing was ever published to, owned by `fencing_epoch`.
    ///
    /// # Errors
    ///
    /// Refuses the zero epoch.
    pub(crate) fn empty(fencing_epoch: u64) -> Result<Self, String> {
        let catalog = Self {
            format_version: FORMAT_VERSION,
            fencing_epoch,
            anchor: ChainLink::GENESIS,
            next_segment_id: 1,
            seq_floor: 0,
            retired_by: SharedGenerationId::NONE,
            segments: Vec::new(),
        };
        catalog.validate()?;
        Ok(catalog)
    }

    /// Decode and validate catalog bytes.
    ///
    /// # Errors
    ///
    /// [`CatalogDecodeError::Incompatible`] for another format version, and
    /// [`CatalogDecodeError::Invalid`] for anything else that is not a valid
    /// catalog.
    pub fn decode(bytes: &[u8]) -> Result<Self, CatalogDecodeError> {
        Self::validate_format(bytes)?;
        let catalog: Self = serde_json::from_slice(bytes)
            .map_err(|error| CatalogDecodeError::Invalid(format!("JSON: {error}")))?;
        catalog.validate().map_err(CatalogDecodeError::Invalid)?;
        Ok(catalog)
    }

    /// The catalog's bytes.
    ///
    /// # Errors
    ///
    /// Refuses an invalid catalog, which no mutator produces.
    #[cfg(any(test, feature = "internal-testing"))]
    pub fn encode(&self) -> Result<Vec<u8>, String> {
        self.validate()?;
        serde_json::to_vec(self)
            .map_err(|error| format!("serialize cloud WAL publication catalog: {error}"))
    }

    /// The epoch that owns the catalog.
    #[must_use]
    pub fn fencing_epoch(&self) -> u64 {
        self.fencing_epoch
    }

    /// The link of the last retired segment, or the genesis link.
    #[must_use]
    pub fn anchor(&self) -> ChainLink {
        self.anchor
    }

    /// The link of the newest published segment, or the anchor when the
    /// catalog publishes none.
    #[must_use]
    pub fn head(&self) -> ChainLink {
        self.segments
            .last()
            .map_or(self.anchor, |entry| entry.facts.link)
    }

    /// The least id a new segment may take: past every published id.
    #[must_use]
    pub fn next_segment_id(&self) -> SegmentId {
        self.next_segment_id
    }

    /// Every sequence at or below it may belong to set-aside WAL.
    #[must_use]
    pub fn seq_floor(&self) -> u64 {
        self.seq_floor
    }

    /// Shared metadata generation that authorized the retired prefix.
    #[must_use]
    pub fn retired_by(&self) -> SharedGenerationId {
        self.retired_by
    }

    /// The published segments, oldest first.
    #[must_use]
    pub fn entries(&self) -> &[CatalogEntry] {
        &self.segments
    }

    /// Leading published segments whose verified family maxima are all covered.
    #[must_use]
    pub(crate) fn retirable_prefix(
        &self,
        mut covered: impl FnMut(u32, u64) -> bool,
    ) -> &[CatalogEntry] {
        let count = self
            .segments
            .iter()
            .take_while(|entry| {
                !entry.facts.max_seq_per_cf.is_empty()
                    && entry
                        .facts
                        .max_seq_per_cf
                        .iter()
                        .all(|(cf, maximum)| covered(*cf, *maximum))
            })
            .count();
        &self.segments[..count]
    }

    /// The entry publishing segment `id`, if any.
    #[must_use]
    pub fn entry(&self, id: SegmentId) -> Option<&CatalogEntry> {
        self.segments
            .binary_search_by_key(&id, CatalogEntry::id)
            .ok()
            .map(|index| &self.segments[index])
    }

    /// Whether nothing was ever published or set aside: the catalog a new
    /// database starts with.
    #[must_use]
    pub fn is_pristine(&self) -> bool {
        self.segments.is_empty()
            && self.anchor == ChainLink::GENESIS
            && self.next_segment_id == 1
            && self.seq_floor == 0
            && self.retired_by == SharedGenerationId::NONE
    }

    /// Hand the catalog to `writer_epoch`. Returns whether it changed.
    ///
    /// # Errors
    ///
    /// Refuses an epoch below the catalog's.
    pub(crate) fn fence_to(&mut self, writer_epoch: u64) -> Result<bool, CatalogRefusal> {
        if writer_epoch < self.fencing_epoch {
            return Err(CatalogRefusal::EpochMismatch {
                catalog: self.fencing_epoch,
                writer: writer_epoch,
            });
        }
        let changed = writer_epoch != self.fencing_epoch;
        self.fencing_epoch = writer_epoch;
        Ok(changed)
    }

    /// Append `run`, verified uploads oldest first, after the catalog's head
    /// ([`crate::wal::publish::additions`]). Segments the catalog already
    /// publishes identically add nothing. Returns whether the catalog
    /// changed.
    ///
    /// # Errors
    ///
    /// Refuses a writer epoch other than the catalog's, and any run
    /// [`crate::wal::publish::additions`] refuses; the catalog is then
    /// unchanged.
    pub(crate) fn append_run(
        &mut self,
        fencing_epoch: u64,
        run: &[CatalogEntry],
    ) -> Result<bool, crate::wal::publish::Refusal> {
        if fencing_epoch != self.fencing_epoch {
            return Err(crate::wal::publish::Refusal::EpochMismatch {
                catalog: self.fencing_epoch,
                writer: fencing_epoch,
            });
        }
        let added = crate::wal::publish::additions(self.head(), &self.segments, run)?;
        let Some(last) = added.last() else {
            return Ok(false);
        };
        if let Some(entry) = added.iter().find(|entry| entry.epoch > self.fencing_epoch) {
            return Err(crate::wal::publish::Refusal::EpochMismatch {
                catalog: self.fencing_epoch,
                writer: entry.epoch,
            });
        }
        let next = self.next_segment_id.max(last.id().saturating_add(1));
        self.segments.reserve_exact(added.len());
        self.segments.extend(added);
        self.next_segment_id = next;
        Ok(true)
    }

    /// [`Self::append_run`] for fixtures outside the crate, with the
    /// refusal as text.
    ///
    /// # Errors
    ///
    /// The refusal [`Self::append_run`] returns.
    #[cfg(any(test, feature = "internal-testing"))]
    pub fn append(&mut self, fencing_epoch: u64, run: &[CatalogEntry]) -> Result<bool, String> {
        self.append_run(fencing_epoch, run)
            .map_err(|refusal| refusal.to_string())
    }

    /// Retire the oldest entries `expected` names, as the caller saw them.
    /// The anchor becomes the last retired entry's link; the next id stays.
    /// Expected segments the catalog already retired are skipped. Returns
    /// the ids retired.
    ///
    /// # Errors
    ///
    /// Refuses another writer epoch, a segment that is not among the
    /// catalog's oldest, in order, and an entry that changed.
    pub(crate) fn retire_prefix(
        &mut self,
        fencing_epoch: u64,
        expected: &[CatalogEntry],
        generation: SharedGenerationId,
    ) -> Result<Vec<SegmentId>, CatalogRefusal> {
        self.require_epoch(fencing_epoch)?;
        if generation == SharedGenerationId::NONE || generation < self.retired_by {
            return Err(CatalogRefusal::GenerationRegression {
                retired_by: self.retired_by,
                proposed: generation,
            });
        }
        let pending = self.oldest_prefix(expected.iter().map(CatalogEntry::id))?;
        for (actual, entry) in self.segments.iter().zip(
            expected
                .iter()
                .filter(|entry| pending.contains(&entry.id())),
        ) {
            if actual != entry {
                return Err(CatalogRefusal::Changed {
                    segment_id: entry.id(),
                });
            }
        }
        let retired: Vec<CatalogEntry> = self.segments.drain(..pending.len()).collect();
        if let Some(last) = retired.last() {
            self.anchor = last.facts.link;
            self.retired_by = generation;
        }
        Ok(pending)
    }

    /// The segments of `candidates`, ascending, that the catalog still
    /// publishes, when they are its oldest, in order: what
    /// [`Self::retire_prefix`] retires. Candidates at or below the anchor
    /// already retired.
    ///
    /// # Errors
    ///
    /// Refuses a candidate that is not among the catalog's oldest, in order.
    pub(crate) fn oldest_prefix(
        &self,
        candidates: impl IntoIterator<Item = SegmentId>,
    ) -> Result<Vec<SegmentId>, CatalogRefusal> {
        let pending: Vec<SegmentId> = candidates
            .into_iter()
            .filter(|id| *id > self.anchor.id)
            .collect();
        for (index, id) in pending.iter().enumerate() {
            match self.segments.get(index) {
                Some(entry) if entry.id() == *id => {}
                entry => {
                    return Err(CatalogRefusal::NotOldestPrefix {
                        segment_id: *id,
                        oldest: entry.or(self.segments.first()).map(CatalogEntry::id),
                    })
                }
            }
        }
        Ok(pending)
    }

    /// Salvage (#844): set aside every entry from `from` on, which must be
    /// exactly `expected`, and raise the sequence floor to at least `floor`.
    /// The next id stays, so no set-aside id is handed out again. Returns
    /// whether the catalog changed.
    ///
    /// # Errors
    ///
    /// Refuses another writer epoch, and a suffix other than `expected`.
    pub(crate) fn set_aside_suffix(
        &mut self,
        fencing_epoch: u64,
        from: Option<SegmentId>,
        expected: &[CatalogEntry],
        floor: u64,
    ) -> Result<bool, CatalogRefusal> {
        self.require_epoch(fencing_epoch)?;
        let kept = from.map_or(self.segments.len(), |from| {
            self.segments.partition_point(|entry| entry.id() < from)
        });
        if self.segments[kept..] != *expected {
            return Err(CatalogRefusal::NotSuffix {
                from: from.unwrap_or(self.next_segment_id),
            });
        }
        let changed = kept < self.segments.len() || floor > self.seq_floor;
        self.segments.truncate(kept);
        self.seq_floor = self.seq_floor.max(floor);
        Ok(changed)
    }

    fn require_epoch(&self, writer_epoch: u64) -> Result<(), CatalogRefusal> {
        if writer_epoch == self.fencing_epoch {
            Ok(())
        } else {
            Err(CatalogRefusal::EpochMismatch {
                catalog: self.fencing_epoch,
                writer: writer_epoch,
            })
        }
    }

    /// Whether the catalog is one the mutators can reach: this format, a
    /// non-zero epoch no entry passes, entries that chain from the anchor
    /// with rising ids and sequences and non-decreasing epochs, objects that
    /// are not empty, and a next id past the head.
    ///
    /// # Errors
    ///
    /// The first rule the catalog breaks.
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.format_version != FORMAT_VERSION {
            return Err(format!(
                "unsupported cloud WAL publication catalog format version {}; expected {FORMAT_VERSION}",
                self.format_version
            ));
        }
        if self.fencing_epoch == 0 {
            return Err("cloud WAL catalog fencing epoch must be non-zero".to_string());
        }
        if (self.anchor == ChainLink::GENESIS) != (self.retired_by == SharedGenerationId::NONE) {
            return Err("cloud WAL catalog retired anchor and shared generation disagree".into());
        }
        let mut prev = self.anchor;
        let mut epoch = 0;
        for entry in &self.segments {
            let id = entry.id();
            if entry.prev != prev {
                return Err(format!(
                    "cloud WAL catalog segment {id} links segment {} (last sequence {}), not segment {} (last sequence {}) before it",
                    entry.prev.id, entry.prev.last_seq, prev.id, prev.last_seq
                ));
            }
            if id <= prev.id || entry.facts.link.last_seq <= prev.last_seq {
                return Err(format!(
                    "cloud WAL catalog segment {id} does not follow segment {}",
                    prev.id
                ));
            }
            if entry.epoch < epoch {
                return Err(format!(
                    "cloud WAL catalog segment {id} lowers the writer epoch from {epoch} to {}",
                    entry.epoch
                ));
            }
            if entry.epoch > self.fencing_epoch {
                return Err(format!(
                    "cloud WAL catalog segment {id} writer epoch {} exceeds fencing epoch {}",
                    entry.epoch, self.fencing_epoch
                ));
            }
            let facts = &entry.facts;
            if facts.record_count == 0
                || facts.max_seq_per_cf.is_empty()
                || facts.data_len < crate::wal::format::HEADER_LEN as u64
                || facts.data_len >= entry.size_bytes
                || facts
                    .max_seq_per_cf
                    .windows(2)
                    .any(|pair| pair[0].0 >= pair[1].0)
                || facts
                    .max_seq_per_cf
                    .iter()
                    .any(|(_, sequence)| *sequence == 0 || *sequence > facts.link.last_seq)
                || facts
                    .max_seq_per_cf
                    .iter()
                    .map(|(_, sequence)| *sequence)
                    .max()
                    != Some(facts.link.last_seq)
                || facts.record_count < facts.max_seq_per_cf.len() as u64
            {
                return Err(format!(
                    "cloud WAL catalog segment {id} has invalid sealing facts"
                ));
            }
            if entry.size_bytes == 0 {
                return Err(format!("cloud WAL catalog segment {id} has invalid size"));
            }
            prev = entry.facts.link;
            epoch = entry.epoch;
        }
        if self.next_segment_id <= self.head().id {
            return Err(format!(
                "cloud WAL catalog next segment id {} does not pass its head, segment {}",
                self.next_segment_id,
                self.head().id
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::format::segment::SegmentBuilder;

    #[test]
    fn should_preserve_retirement_generation_when_every_catalog_entry_retires() {
        // Arrange
        let (mut catalog, entries) = published(8, 2);
        let generation = SharedGenerationId::new(7).unwrap();
        // Act
        catalog.retire_prefix(8, &entries, generation).unwrap();
        let reopened = WalPublicationCatalog::decode(&catalog.encode().unwrap()).unwrap();
        // Assert
        assert_eq!(reopened.entries(), []);
        assert_eq!(reopened.retired_by(), generation);
        assert_eq!(reopened.next_segment_id(), 3);
    }

    #[test]
    fn should_refuse_older_generation_without_changing_catalog_authority() {
        // Arrange
        let (mut catalog, entries) = published(8, 2);
        catalog
            .retire_prefix(8, &entries[..1], SharedGenerationId::new(7).unwrap())
            .unwrap();
        let previous = catalog.clone();
        // Act
        let result = catalog.retire_prefix(8, &entries[1..], SharedGenerationId::new(6).unwrap());
        // Assert
        assert!(matches!(
            result,
            Err(CatalogRefusal::GenerationRegression { .. })
        ));
        assert_eq!(catalog, previous);
    }

    #[test]
    fn should_stop_retirable_prefix_at_the_first_uncovered_segment() {
        // Arrange
        let (catalog, entries) = published(8, 3);
        // Act
        let prefix = catalog.retirable_prefix(|_, maximum| maximum != 2);
        // Assert
        assert_eq!(prefix, &entries[..1]);
    }

    #[test]
    fn should_persist_verified_family_summary_when_publishing_a_segment() {
        // Arrange
        let (catalog, _) = published(8, 1);

        // Act
        let body: serde_json::Value = serde_json::from_slice(&catalog.encode().unwrap()).unwrap();

        // Assert: a cold cache can select retention without rereading WAL payloads.
        assert_eq!(
            body["segments"][0]["facts"]["max_seq_per_cf"],
            serde_json::json!([[0, 1]])
        );
        assert_eq!(body["segments"][0]["facts"]["record_count"], 1);
    }

    #[test]
    fn should_reject_changed_family_summary_when_validating_published_bytes() {
        // Arrange
        let bytes = sealed(ChainLink::GENESIS, 1..=1, 8).remove(0);
        let entry = CatalogEntry::from_sealed_bytes(&bytes).unwrap();
        let mut encoded = serde_json::to_value(entry).unwrap();
        encoded["facts"]["max_seq_per_cf"] = serde_json::json!([[0, 0]]);

        // Act
        let altered: Result<CatalogEntry, _> = serde_json::from_value(encoded);

        // Assert: either malformed shape or verified-byte mismatch must refuse it.
        assert!(
            altered.is_ok(),
            "the persisted summary must be part of the current format"
        );
        assert!(altered.unwrap().validate_bytes(&bytes).is_err());
    }

    fn put(seq: u64) -> crate::wal::WalRecord {
        crate::wal::WalRecord::new(
            crate::wal::WalOpKind::Put,
            bytes::Bytes::from_static(b"key"),
            Some(bytes::Bytes::from_static(b"value")),
            seq,
        )
    }

    /// Sealed segments `ids`, in order, by `epoch`, each holding one record
    /// and linking the one before, from `prev`.
    fn sealed(
        prev: ChainLink,
        ids: std::ops::RangeInclusive<SegmentId>,
        epoch: u64,
    ) -> Vec<Vec<u8>> {
        let mut prev = prev;
        ids.map(|id| {
            let (bytes, link) = SegmentBuilder::new(id, epoch, prev)
                .record(&put(prev.last_seq + 1))
                .seal();
            prev = link;
            bytes
        })
        .collect()
    }

    fn entries(segments: &[Vec<u8>]) -> Vec<CatalogEntry> {
        segments
            .iter()
            .map(|bytes| CatalogEntry::from_sealed_bytes(bytes).unwrap())
            .collect()
    }

    /// `catalog`'s JSON with `edit` applied, decoded.
    fn edited(
        catalog: &WalPublicationCatalog,
        edit: impl FnOnce(&mut serde_json::Value),
    ) -> Result<WalPublicationCatalog, CatalogDecodeError> {
        let mut json = serde_json::to_value(catalog).unwrap();
        edit(&mut json);
        WalPublicationCatalog::decode(&serde_json::to_vec(&json).unwrap())
    }

    fn published(epoch: u64, count: u64) -> (WalPublicationCatalog, Vec<CatalogEntry>) {
        let mut catalog = WalPublicationCatalog::empty(epoch).unwrap();
        let entries = entries(&sealed(ChainLink::GENESIS, 1..=count, epoch));
        catalog.append_run(epoch, &entries).unwrap();
        (catalog, entries)
    }

    #[test]
    fn should_derive_entry_identity_from_its_link() {
        // Arrange
        let bytes = sealed(ChainLink::GENESIS, 4..=4, 7).remove(0);

        // Act
        let entry = CatalogEntry::from_sealed_bytes(&bytes).unwrap();

        // Assert
        assert_eq!(entry.id(), 4);
        assert_eq!(entry.max_sequence(), 1);
        assert_eq!(entry.object_key(), crate::wal::segment_object_key(4, 7));
        assert_eq!(entry.prev(), ChainLink::GENESIS);
        assert!(entry.validate_bytes(&bytes).is_ok());
    }

    #[test]
    fn should_reject_stale_catalog_mutation_after_lease_takeover() {
        // Arrange
        let mut catalog = WalPublicationCatalog::empty(7).unwrap();
        catalog.fence_to(8).unwrap();
        let run = entries(&sealed(ChainLink::GENESIS, 1..=1, 7));

        // Act
        let result = catalog.append_run(7, &run);

        // Assert
        let error = result.unwrap_err().to_string();
        assert!(error.contains("requires fencing epoch 8"), "{error}");
        assert_eq!(catalog.entries(), []);
    }

    #[test]
    fn should_retain_publications_when_fencing_catalog_to_new_epoch() {
        // Arrange
        let (mut catalog, entries) = published(7, 1);

        // Act
        let changed = catalog.fence_to(8).unwrap();

        // Assert
        assert!(changed);
        assert_eq!(catalog.fencing_epoch(), 8);
        assert_eq!(catalog.entries(), entries.as_slice());
    }

    #[test]
    fn should_reject_catalog_whose_first_entry_does_not_link_the_anchor() {
        // Arrange
        let (catalog, _) = published(7, 2);

        // Act
        let result = edited(&catalog, |json| {
            json["segments"][0]["prev"]["last_seq"] = serde_json::json!(9);
        });

        // Assert
        assert!(
            matches!(&result, Err(CatalogDecodeError::Invalid(reason)) if reason.contains("links")),
            "{result:?}"
        );
    }

    #[test]
    fn should_reject_catalog_whose_entries_lower_the_epoch() {
        // Arrange
        let (catalog, _) = published(7, 2);

        // Act
        let result = edited(&catalog, |json| {
            json["segments"][0]["epoch"] = serde_json::json!(7);
            json["segments"][1]["epoch"] = serde_json::json!(6);
        });

        // Assert
        assert!(
            matches!(&result, Err(CatalogDecodeError::Invalid(reason)) if reason.contains("lowers the writer epoch")),
            "{result:?}"
        );
    }

    #[test]
    fn should_reject_catalog_whose_next_segment_id_does_not_pass_the_head() {
        // Arrange
        let (catalog, _) = published(7, 2);

        // Act
        let result = edited(&catalog, |json| {
            json["next_segment_id"] = serde_json::json!(2);
        });

        // Assert
        assert!(
            matches!(&result, Err(CatalogDecodeError::Invalid(reason)) if reason.contains("next segment id")),
            "{result:?}"
        );
    }

    #[test]
    fn should_reject_catalog_entry_from_future_writer_epoch() {
        // Arrange
        let (catalog, _) = published(7, 1);

        // Act
        let result = edited(&catalog, |json| {
            json["segments"][0]["epoch"] = serde_json::json!(8);
        });

        // Assert
        assert!(
            matches!(&result, Err(CatalogDecodeError::Invalid(reason)) if reason.contains("exceeds fencing epoch")),
            "{result:?}"
        );
    }

    #[test]
    fn should_report_another_format_version_as_incompatible() {
        // Arrange
        let v1 = br#"{"format_version":1,"fencing_epoch":7,"segments":{}}"#;

        // Act
        let result = WalPublicationCatalog::decode(v1);

        // Assert
        assert_eq!(result, Err(CatalogDecodeError::Incompatible(1)));
    }

    #[test]
    fn should_refuse_a_run_that_does_not_link_the_head() {
        // Arrange
        let (mut catalog, _) = published(7, 1);
        let stranger = ChainLink {
            id: 1,
            last_seq: 1,
            digest: [0x5a; 16],
        };
        let run = entries(&sealed(stranger, 2..=2, 7));

        // Act
        let result = catalog.append_run(7, &run);

        // Assert
        assert!(
            matches!(
                result,
                Err(crate::wal::publish::Refusal::Unchained { segment_id: 2, .. })
            ),
            "{result:?}"
        );
        assert_eq!(catalog.entries().len(), 1);
    }

    #[test]
    fn should_retire_only_exact_catalog_entry() {
        // Arrange
        let (mut catalog, entries) = published(8, 1);
        let mut stale = entries[0].clone();
        stale.crc32c ^= 1;

        // Act
        let result = catalog.retire_prefix(8, &[stale], SharedGenerationId::FIRST);

        // Assert
        assert_eq!(result, Err(CatalogRefusal::Changed { segment_id: 1 }));
        assert_eq!(catalog.entries(), entries.as_slice());
    }

    #[test]
    fn should_refuse_retirement_of_a_segment_behind_an_older_one() {
        // Arrange
        let (mut catalog, entries) = published(8, 2);

        // Act
        let result = catalog.retire_prefix(8, &entries[1..], SharedGenerationId::FIRST);

        // Assert
        assert_eq!(
            result,
            Err(CatalogRefusal::NotOldestPrefix {
                segment_id: 2,
                oldest: Some(1)
            })
        );
        assert_eq!(catalog.entries().len(), 2);
    }

    #[test]
    fn should_keep_the_chain_when_every_entry_retires() {
        // Arrange
        let (mut catalog, entries) = published(8, 2);

        // Act
        let retired = catalog
            .retire_prefix(8, &entries, SharedGenerationId::FIRST)
            .unwrap();

        // Assert: the chain and the ids outlive the entries.
        assert_eq!(retired, vec![1, 2]);
        assert_eq!(catalog.entries(), []);
        assert_eq!(catalog.anchor(), entries[1].link());
        assert_eq!(catalog.head(), entries[1].link());
        assert_eq!(catalog.next_segment_id(), 3);
        assert!(!catalog.is_pristine());
        let decoded = WalPublicationCatalog::decode(&catalog.encode().unwrap()).unwrap();
        assert_eq!(decoded, catalog);
    }

    #[test]
    fn should_skip_segments_the_catalog_already_retired_when_retiring_a_prefix() {
        // Arrange
        let (mut catalog, entries) = published(8, 2);
        catalog
            .retire_prefix(8, &entries[..1], SharedGenerationId::FIRST)
            .unwrap();

        // Act
        let retired = catalog.retire_prefix(8, &entries, SharedGenerationId::FIRST);

        // Assert
        assert_eq!(retired, Ok(vec![2]));
        assert_eq!(catalog.entries(), []);
    }

    #[test]
    fn should_set_aside_a_suffix_keeping_the_next_segment_id() {
        // Arrange
        let (mut catalog, entries) = published(8, 3);

        // Act
        let changed = catalog
            .set_aside_suffix(8, Some(2), &entries[1..], 9)
            .unwrap();

        // Assert
        assert!(changed);
        assert_eq!(catalog.entries(), &entries[..1]);
        assert_eq!(catalog.seq_floor(), 9);
        assert_eq!(catalog.next_segment_id(), 4);
    }

    #[test]
    fn should_refuse_to_set_aside_a_suffix_that_changed() {
        // Arrange
        let (mut catalog, entries) = published(8, 3);

        // Act
        let result = catalog.set_aside_suffix(8, Some(2), &entries[2..], 9);

        // Assert
        assert_eq!(result, Err(CatalogRefusal::NotSuffix { from: 2 }));
        assert_eq!(catalog.entries(), entries.as_slice());
        assert_eq!(catalog.seq_floor(), 0);
    }

    #[test]
    fn should_only_raise_sequence_floor_when_salvage_sets_wal_aside() {
        // Arrange
        let mut catalog = WalPublicationCatalog::empty(7).unwrap();

        // Act
        let raised = catalog.set_aside_suffix(7, None, &[], 9).unwrap();
        let lowered = catalog.set_aside_suffix(7, None, &[], 4).unwrap();
        let stale = catalog.set_aside_suffix(6, None, &[], 12);

        // Assert
        assert!(raised);
        assert!(!lowered);
        assert_eq!(
            stale,
            Err(CatalogRefusal::EpochMismatch {
                catalog: 7,
                writer: 6
            })
        );
        assert_eq!(catalog.seq_floor(), 9);
    }

    #[test]
    fn should_round_trip_sequence_floor_when_encoded() {
        // Arrange
        let mut catalog = WalPublicationCatalog::empty(7).unwrap();
        catalog.set_aside_suffix(7, None, &[], 9).unwrap();

        // Act
        let decoded = WalPublicationCatalog::decode(&catalog.encode().unwrap()).unwrap();

        // Assert
        assert_eq!(decoded.seq_floor(), 9);
        assert!(!decoded.is_pristine());
    }
}
