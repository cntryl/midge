//! Lease-fenced authority for remotely published WAL segments.
//!
//! Immutable WAL objects are not authoritative merely because they exist.
//! Only entries in this catalog are eligible for recovery. Every mutation is
//! conditioned on both the catalog object's provider identity and the current
//! lease epoch.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub(crate) const FORMAT_VERSION: u32 = 1;
pub(crate) const OBJECT_KEY: &str = crate::cloud_layout::CloudObjectLayout::WAL_CATALOG_OBJECT_KEY;
pub(crate) const MIRROR_OBJECT_KEY: &str =
    crate::cloud_layout::CloudObjectLayout::WAL_CATALOG_MIRROR_OBJECT_KEY;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct PublishedWalSegment {
    pub(crate) segment_id: u64,
    pub(crate) writer_epoch: u64,
    pub(crate) max_sequence: u64,
    pub(crate) size_bytes: u64,
    pub(crate) content_crc32c: u32,
    pub(crate) object_key: String,
}

impl PublishedWalSegment {
    pub(crate) fn from_validated_bytes(
        segment_id: u64,
        max_sequence: u64,
        writer_epoch: u64,
        bytes: &[u8],
    ) -> Self {
        Self {
            segment_id,
            writer_epoch,
            max_sequence,
            size_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            content_crc32c: crc32c::crc32c(bytes),
            object_key: crate::wal::segment_object_key(segment_id, writer_epoch),
        }
    }

    pub(crate) fn validate_bytes(&self, bytes: &[u8]) -> Result<(), String> {
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != self.size_bytes {
            return Err(format!(
                "published cloud WAL segment {} size {} does not match catalog {}",
                self.segment_id,
                bytes.len(),
                self.size_bytes
            ));
        }
        let actual_crc = crc32c::crc32c(bytes);
        if actual_crc != self.content_crc32c {
            return Err(format!(
                "published cloud WAL segment {} crc32c {actual_crc:08x} does not match catalog {:08x}",
                self.segment_id, self.content_crc32c
            ));
        }
        let validation =
            crate::wal::cloud_segment::validate_bytes(&self.object_key, bytes, self.max_sequence)?;
        self.check(&validation.identity())
    }

    /// Whether a segment that says it is `identity` is the one this entry
    /// names (#843): the same id, writer epoch and last sequence. The one
    /// identity check every reader of a published or publishing segment
    /// applies.
    ///
    /// # Errors
    ///
    /// What differs, naming the object.
    pub(crate) fn check(
        &self,
        identity: &crate::wal::format::SegmentIdentity,
    ) -> Result<(), String> {
        let key = &self.object_key;
        if identity.id != self.segment_id {
            return Err(format!(
                "cloud WAL segment '{key}' header names segment {}, not {}",
                identity.id, self.segment_id
            ));
        }
        if identity.epoch != self.writer_epoch {
            return Err(format!(
                "cloud WAL segment '{key}' header names writer epoch {}, not {}",
                identity.epoch, self.writer_epoch
            ));
        }
        if identity.last_seq != self.max_sequence {
            return Err(format!(
                "cloud WAL segment '{key}' holds sequences through {}, not {}",
                identity.last_seq, self.max_sequence
            ));
        }
        Ok(())
    }
}

/// Why a catalog mutation is refused (#858). A refusal changes nothing,
/// and no retry fixes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CatalogRefusal {
    /// Another writer epoch owns the catalog.
    EpochMismatch { catalog: u64, writer: u64 },
    /// Only the catalog's oldest segments may retire, in order.
    NotOldestPrefix {
        segment_id: u64,
        oldest: Option<u64>,
    },
    /// The catalog publishes something else for this segment than the
    /// caller saw.
    Changed { segment_id: u64 },
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
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct WalPublicationCatalog {
    pub(crate) format_version: u32,
    pub(crate) fencing_epoch: u64,
    pub(crate) segments: BTreeMap<u64, PublishedWalSegment>,
    /// Every sequence at or below this may belong to WAL that salvage set
    /// aside unreplayed, so a later open must not assign it again (#529).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub(crate) sequence_floor: u64,
}

#[allow(clippy::trivially_copy_pass_by_ref)] // serde passes fields by reference
fn is_zero(value: &u64) -> bool {
    *value == 0
}

impl WalPublicationCatalog {
    pub(crate) fn empty(fencing_epoch: u64) -> Result<Self, String> {
        if fencing_epoch == 0 {
            return Err("cloud WAL catalog fencing epoch must be non-zero".to_string());
        }
        Ok(Self {
            format_version: FORMAT_VERSION,
            fencing_epoch,
            segments: BTreeMap::new(),
            sequence_floor: 0,
        })
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, String> {
        let catalog: Self = serde_json::from_slice(bytes)
            .map_err(|error| format!("invalid cloud WAL publication catalog JSON: {error}"))?;
        catalog.validate()?;
        Ok(catalog)
    }

    #[cfg(test)]
    pub(crate) fn encode(&self) -> Result<Vec<u8>, String> {
        self.validate()?;
        serde_json::to_vec_pretty(self)
            .map_err(|error| format!("serialize cloud WAL publication catalog: {error}"))
    }

    /// The epoch that owns the catalog.
    pub(crate) fn fencing_epoch(&self) -> u64 {
        self.fencing_epoch
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
        if writer_epoch == self.fencing_epoch {
            return Ok(false);
        }
        self.fencing_epoch = writer_epoch;
        Ok(true)
    }

    /// Append `run`, verified uploads oldest first, past the catalog's head
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
        run: &[PublishedWalSegment],
    ) -> Result<bool, crate::wal::publish::Refusal> {
        if fencing_epoch != self.fencing_epoch {
            return Err(crate::wal::publish::Refusal::EpochMismatch {
                catalog: self.fencing_epoch,
                writer: fencing_epoch,
            });
        }
        let added = crate::wal::publish::additions(&self.segments, run)?;
        let changed = !added.is_empty();
        for segment in added {
            self.segments.insert(segment.segment_id, segment);
        }
        Ok(changed)
    }

    /// Retire the catalog's oldest segments, which must be `expected`, as
    /// the caller saw them; expected segments the catalog no longer
    /// publishes are skipped. Returns the ids retired.
    ///
    /// # Errors
    ///
    /// Refuses another writer epoch, a segment that is not among the
    /// catalog's oldest, in order, and a segment that changed. A refusal
    /// changes nothing.
    pub(crate) fn retire_prefix(
        &mut self,
        fencing_epoch: u64,
        expected: &[PublishedWalSegment],
    ) -> Result<Vec<u64>, CatalogRefusal> {
        self.require_epoch(fencing_epoch)?;
        let pending = self.oldest_prefix(expected.iter().map(|entry| entry.segment_id))?;
        for entry in expected
            .iter()
            .filter(|entry| pending.contains(&entry.segment_id))
        {
            if self.segments.get(&entry.segment_id) != Some(entry) {
                return Err(CatalogRefusal::Changed {
                    segment_id: entry.segment_id,
                });
            }
        }
        for segment_id in &pending {
            self.segments.remove(segment_id);
        }
        Ok(pending)
    }

    /// The segments of `candidates` the catalog still publishes, when they
    /// are its oldest, in order: what [`Self::retire_prefix`] would retire.
    ///
    /// # Errors
    ///
    /// Refuses a candidate that is not among the catalog's oldest, in order.
    pub(crate) fn oldest_prefix(
        &self,
        candidates: impl IntoIterator<Item = u64>,
    ) -> Result<Vec<u64>, CatalogRefusal> {
        let pending: Vec<u64> = candidates
            .into_iter()
            .filter(|segment_id| self.segments.contains_key(segment_id))
            .collect();
        for (oldest, segment_id) in self.segments.keys().zip(&pending) {
            if oldest != segment_id {
                return Err(CatalogRefusal::NotOldestPrefix {
                    segment_id: *segment_id,
                    oldest: Some(*oldest),
                });
            }
        }
        Ok(pending)
    }

    /// Salvage: stop publishing `segments`, the cataloged segments a salvage
    /// open did not replay, as the caller saw them. Segments already gone
    /// are skipped. Returns whether the catalog changed.
    ///
    /// # Errors
    ///
    /// Refuses another writer epoch, and a segment that changed.
    pub(crate) fn set_aside(
        &mut self,
        fencing_epoch: u64,
        segments: &[PublishedWalSegment],
    ) -> Result<bool, CatalogRefusal> {
        self.require_epoch(fencing_epoch)?;
        for segment in segments {
            if self
                .segments
                .get(&segment.segment_id)
                .is_some_and(|actual| actual != segment)
            {
                return Err(CatalogRefusal::Changed {
                    segment_id: segment.segment_id,
                });
            }
        }
        let before = self.segments.len();
        for segment in segments {
            self.segments.remove(&segment.segment_id);
        }
        Ok(self.segments.len() != before)
    }

    /// Raises the sequence floor; it never falls.
    pub(crate) fn raise_sequence_floor(
        &mut self,
        fencing_epoch: u64,
        floor: u64,
    ) -> Result<bool, CatalogRefusal> {
        self.require_epoch(fencing_epoch)?;
        if floor <= self.sequence_floor {
            return Ok(false);
        }
        self.sequence_floor = floor;
        Ok(true)
    }

    fn require_epoch(&self, writer_epoch: u64) -> Result<(), CatalogRefusal> {
        if writer_epoch == self.fencing_epoch {
            return Ok(());
        }
        Err(CatalogRefusal::EpochMismatch {
            catalog: self.fencing_epoch,
            writer: writer_epoch,
        })
    }

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
        for (segment_id, segment) in &self.segments {
            if *segment_id != segment.segment_id {
                return Err(format!(
                    "cloud WAL catalog key {segment_id} does not match embedded segment id {}",
                    segment.segment_id
                ));
            }
            if segment.size_bytes == 0 {
                return Err(format!(
                    "cloud WAL catalog segment {segment_id} has invalid size"
                ));
            }
            if segment.writer_epoch > self.fencing_epoch {
                return Err(format!(
                    "cloud WAL catalog segment {segment_id} writer epoch {} exceeds fencing epoch {}",
                    segment.writer_epoch, self.fencing_epoch
                ));
            }
            let expected_key = crate::wal::segment_object_key(*segment_id, segment.writer_epoch);
            if segment.object_key != expected_key {
                return Err(format!(
                    "cloud WAL catalog segment {segment_id} object key '{}' does not match '{}'",
                    segment.object_key, expected_key
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn segment(segment_id: u64, writer_epoch: u64) -> PublishedWalSegment {
        PublishedWalSegment {
            segment_id,
            writer_epoch,
            max_sequence: segment_id * 10,
            size_bytes: 42,
            content_crc32c: 7,
            object_key: crate::wal::segment_object_key(segment_id, writer_epoch),
        }
    }

    #[test]
    fn should_reject_stale_catalog_mutation_after_lease_takeover() {
        // Arrange
        let mut catalog = WalPublicationCatalog::empty(7).unwrap();
        catalog.fence_to(8).unwrap();

        // Act
        let result = catalog.append_run(7, &[segment(1, 7)]);

        // Assert
        let error = result.unwrap_err().to_string();
        assert!(error.contains("requires fencing epoch 8"), "{error}");
        assert!(catalog.segments.is_empty());
    }

    #[test]
    fn should_retain_publications_when_fencing_catalog_to_new_epoch() {
        // Arrange
        let mut catalog = WalPublicationCatalog::empty(7).unwrap();
        catalog.append_run(7, &[segment(1, 7)]).unwrap();

        // Act
        let changed = catalog.fence_to(8).unwrap();

        // Assert
        assert!(changed);
        assert_eq!(catalog.fencing_epoch, 8);
        assert_eq!(catalog.segments.get(&1), Some(&segment(1, 7)));
    }

    #[test]
    fn should_reject_catalog_entry_with_noncanonical_epoch_scoped_key() {
        // Arrange
        let mut invalid = segment(1, 7);
        invalid.object_key = "wal/00000000000000000001.wal".to_string();
        let catalog = WalPublicationCatalog {
            format_version: FORMAT_VERSION,
            fencing_epoch: 7,
            segments: BTreeMap::from([(1, invalid)]),
            sequence_floor: 0,
        };
        let bytes = serde_json::to_vec(&catalog).unwrap();

        // Act
        let result = WalPublicationCatalog::decode(&bytes);

        // Assert
        assert!(result.unwrap_err().contains("object key"));
    }

    #[test]
    fn should_reject_catalog_entry_from_future_writer_epoch() {
        // Arrange
        let catalog = WalPublicationCatalog {
            format_version: FORMAT_VERSION,
            fencing_epoch: 7,
            segments: BTreeMap::from([(1, segment(1, 8))]),
            sequence_floor: 0,
        };
        let bytes = serde_json::to_vec(&catalog).unwrap();

        // Act
        let result = WalPublicationCatalog::decode(&bytes);

        // Assert
        assert!(result.unwrap_err().contains("exceeds fencing epoch"));
    }

    #[test]
    fn should_retire_only_exact_catalog_entry() {
        // Arrange
        let expected = segment(1, 7);
        let mut catalog = WalPublicationCatalog::empty(8).unwrap();
        catalog
            .append_run(8, std::slice::from_ref(&expected))
            .unwrap();
        let mut stale = expected.clone();
        stale.content_crc32c = 99;

        // Act
        let result = catalog.retire_prefix(8, &[stale]);

        // Assert
        assert_eq!(result, Err(CatalogRefusal::Changed { segment_id: 1 }));
        assert_eq!(catalog.segments.get(&1), Some(&expected));
    }

    #[test]
    fn should_refuse_retirement_of_a_segment_behind_an_older_one() {
        // Arrange
        let mut catalog = WalPublicationCatalog::empty(8).unwrap();
        catalog
            .append_run(8, &[segment(1, 7), segment(2, 7)])
            .unwrap();

        // Act
        let result = catalog.retire_prefix(8, &[segment(2, 7)]);

        // Assert
        assert_eq!(
            result,
            Err(CatalogRefusal::NotOldestPrefix {
                segment_id: 2,
                oldest: Some(1)
            })
        );
        assert_eq!(catalog.segments.len(), 2);
    }

    #[test]
    fn should_skip_segments_the_catalog_already_retired_when_retiring_a_prefix() {
        // Arrange
        let mut catalog = WalPublicationCatalog::empty(8).unwrap();
        catalog
            .append_run(8, &[segment(1, 7), segment(2, 7)])
            .unwrap();
        catalog.retire_prefix(8, &[segment(1, 7)]).unwrap();

        // Act
        let retired = catalog.retire_prefix(8, &[segment(1, 7), segment(2, 7)]);

        // Assert
        assert_eq!(retired, Ok(vec![2]));
        assert!(catalog.segments.is_empty());
    }

    #[test]
    fn should_only_raise_sequence_floor_when_salvage_sets_wal_aside() {
        // Arrange
        let mut catalog = WalPublicationCatalog::empty(7).unwrap();

        // Act
        let raised = catalog.raise_sequence_floor(7, 9).unwrap();
        let lowered = catalog.raise_sequence_floor(7, 4).unwrap();
        let stale = catalog.raise_sequence_floor(6, 12);

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
        assert_eq!(catalog.sequence_floor, 9);
    }

    #[test]
    fn should_round_trip_sequence_floor_when_encoded() {
        // Arrange
        let mut catalog = WalPublicationCatalog::empty(7).unwrap();
        catalog.raise_sequence_floor(7, 9).unwrap();

        // Act
        let decoded = WalPublicationCatalog::decode(&catalog.encode().unwrap()).unwrap();

        // Assert
        assert_eq!(decoded.sequence_floor, 9);
    }

    #[test]
    fn should_read_zero_sequence_floor_when_catalog_was_written_without_one() {
        // Arrange
        let bytes = serde_json::to_vec(&WalPublicationCatalog::empty(7).unwrap()).unwrap();

        // Act
        let decoded = WalPublicationCatalog::decode(&bytes).unwrap();

        // Assert
        assert!(!String::from_utf8(bytes).unwrap().contains("sequence_floor"));
        assert_eq!(decoded.sequence_floor, 0);
    }
}
