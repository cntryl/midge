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
            object_key: crate::wal::cloud_segment_object_key(segment_id, writer_epoch),
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
        let readback =
            crate::wal::cloud_segment::validate_bytes(&self.object_key, bytes, self.max_sequence)?;
        if readback.validation.writer_epoch != self.writer_epoch {
            return Err(format!(
                "published cloud WAL segment {} writer epoch {} does not match catalog {}",
                self.segment_id, readback.validation.writer_epoch, self.writer_epoch
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct WalPublicationCatalog {
    pub(crate) format_version: u32,
    pub(crate) fencing_epoch: u64,
    pub(crate) segments: BTreeMap<u64, PublishedWalSegment>,
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
        })
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, String> {
        let catalog: Self = serde_json::from_slice(bytes)
            .map_err(|error| format!("invalid cloud WAL publication catalog JSON: {error}"))?;
        catalog.validate()?;
        Ok(catalog)
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, String> {
        self.validate()?;
        serde_json::to_vec_pretty(self)
            .map_err(|error| format!("serialize cloud WAL publication catalog: {error}"))
    }

    pub(crate) fn fence_to(&mut self, writer_epoch: u64) -> Result<bool, String> {
        if writer_epoch < self.fencing_epoch {
            return Err(format!(
                "cloud WAL catalog is fenced at epoch {}, stale writer epoch {writer_epoch} rejected",
                self.fencing_epoch
            ));
        }
        if writer_epoch == self.fencing_epoch {
            return Ok(false);
        }
        self.fencing_epoch = writer_epoch;
        Ok(true)
    }

    pub(crate) fn publish(
        &mut self,
        fencing_epoch: u64,
        segment: PublishedWalSegment,
    ) -> Result<bool, String> {
        self.require_epoch(fencing_epoch)?;
        match self.segments.get(&segment.segment_id) {
            Some(existing) if existing == &segment => Ok(false),
            Some(existing) => Err(format!(
                "cloud WAL catalog segment {} conflicts with existing publication at writer epoch {}",
                segment.segment_id, existing.writer_epoch
            )),
            None => {
                self.segments.insert(segment.segment_id, segment);
                Ok(true)
            }
        }
    }

    pub(crate) fn retire(
        &mut self,
        fencing_epoch: u64,
        expected: &PublishedWalSegment,
    ) -> Result<bool, String> {
        self.require_epoch(fencing_epoch)?;
        match self.segments.get(&expected.segment_id) {
            Some(actual) if actual == expected => {
                self.segments.remove(&expected.segment_id);
                Ok(true)
            }
            Some(_) => Err(format!(
                "cloud WAL catalog segment {} changed before retirement",
                expected.segment_id
            )),
            None => Ok(false),
        }
    }

    fn require_epoch(&self, writer_epoch: u64) -> Result<(), String> {
        if writer_epoch == self.fencing_epoch {
            return Ok(());
        }
        Err(format!(
            "cloud WAL catalog mutation requires fencing epoch {}, writer epoch {writer_epoch} rejected",
            self.fencing_epoch
        ))
    }

    fn validate(&self) -> Result<(), String> {
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
            let expected_key =
                crate::wal::cloud_segment_object_key(*segment_id, segment.writer_epoch);
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
            object_key: crate::wal::cloud_segment_object_key(segment_id, writer_epoch),
        }
    }

    #[test]
    fn should_reject_stale_catalog_mutation_after_lease_takeover() {
        // Arrange
        let mut catalog = WalPublicationCatalog::empty(7).unwrap();
        catalog.fence_to(8).unwrap();

        // Act
        let result = catalog.publish(7, segment(1, 7));

        // Assert
        let error = result.unwrap_err();
        assert!(error.contains("requires fencing epoch 8"), "{error}");
        assert!(catalog.segments.is_empty());
    }

    #[test]
    fn should_retain_publications_when_fencing_catalog_to_new_epoch() {
        // Arrange
        let mut catalog = WalPublicationCatalog::empty(7).unwrap();
        catalog.publish(7, segment(1, 7)).unwrap();

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
        catalog.publish(8, expected.clone()).unwrap();
        let mut stale = expected.clone();
        stale.content_crc32c = 99;

        // Act
        let result = catalog.retire(8, &stale);

        // Assert
        assert!(result.unwrap_err().contains("changed before retirement"));
        assert_eq!(catalog.segments.get(&1), Some(&expected));
    }
}
