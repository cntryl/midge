//! Persisted runtime intent records and their wire-compatible file metadata.

use serde::{Deserialize, Serialize};

/// Crash-recovery phase marker for publish workflows.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum PublicationPhase {
    /// Output files are durable, but the manifest journal has not been made authoritative yet.
    OutputDurable,
    /// The manifest journal now reflects the new state; replay may finalize cleanup idempotently.
    ManifestPublished,
}

/// File metadata embedded in persisted intent records.
///
/// This is intentionally distinct from the richer manifest [`FileMeta`]. Its
/// serde representation is part of the durable intent-log format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMeta {
    pub name: String,
    pub level: u32,
    pub size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_crc32c: Option<u32>,
    pub cf_id: cntryl_midge_core::types::ColumnFamilyId,
    pub smallest_key: Option<Vec<u8>>,
    pub largest_key: Option<Vec<u8>>,
    pub smallest_seq: Option<u64>,
    pub largest_seq: Option<u64>,
    #[serde(default)]
    pub key_bounds_complete: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub earliest_expiration_millis: Option<u64>,
}

impl FileMeta {
    /// Borrow this entry's recorded proofs for the SST identity checker.
    #[must_use]
    pub fn expected_sst(&self) -> cntryl_midge_core::types::ExpectedSst<'_> {
        cntryl_midge_core::types::ExpectedSst {
            name: &self.name,
            size_bytes: self.size_bytes,
            content_crc32c: self.content_crc32c,
            smallest_key: self.smallest_key.as_deref(),
            largest_key: self.largest_key.as_deref(),
            smallest_seq: self.smallest_seq,
            largest_seq: self.largest_seq,
        }
    }
}

impl From<&FileMeta> for crate::metadata::manifest::FileMeta {
    fn from(value: &FileMeta) -> Self {
        let FileMeta {
            name,
            level,
            size_bytes,
            content_crc32c,
            cf_id,
            smallest_key,
            largest_key,
            smallest_seq,
            largest_seq,
            key_bounds_complete,
            earliest_expiration_millis,
        } = value;
        Self {
            name: name.clone(),
            level: *level,
            size_bytes: *size_bytes,
            content_crc32c: *content_crc32c,
            cf_id: *cf_id,
            smallest_key: smallest_key.clone(),
            largest_key: largest_key.clone(),
            smallest_seq: *smallest_seq,
            largest_seq: *largest_seq,
            key_bounds_complete: *key_bounds_complete,
            earliest_expiration_millis: *earliest_expiration_millis,
            ..Self::default()
        }
    }
}

impl From<&crate::metadata::manifest::FileMeta> for FileMeta {
    fn from(value: &crate::metadata::manifest::FileMeta) -> Self {
        let crate::metadata::manifest::FileMeta {
            name,
            level,
            size_bytes,
            content_crc32c,
            cf_id,
            sst_seq: _,
            smallest_key,
            largest_key,
            smallest_seq,
            largest_seq,
            key_bounds_complete,
            earliest_expiration_millis,
            sublevel: _,
            read_count: _,
        } = value;
        Self {
            name: name.clone(),
            level: *level,
            size_bytes: *size_bytes,
            content_crc32c: *content_crc32c,
            cf_id: *cf_id,
            smallest_key: smallest_key.clone(),
            largest_key: largest_key.clone(),
            smallest_seq: *smallest_seq,
            largest_seq: *largest_seq,
            key_bounds_complete: *key_bounds_complete,
            earliest_expiration_millis: *earliest_expiration_millis,
        }
    }
}

/// Intent log entry - records all state transitions for deterministic replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IntentLogEntry {
    SeqnoAllocated {
        seqno: u64,
        cf_id: cntryl_midge_core::types::ColumnFamilyId,
    },
    FlushPlanned {
        cf_id: cntryl_midge_core::types::ColumnFamilyId,
        seqno_range: (u64, u64),
    },
    CompactionPlanned {
        input_files: Vec<String>,
        output_level: u32,
    },
    FlushPublish {
        phase: PublicationPhase,
        cf_id: cntryl_midge_core::types::ColumnFamilyId,
        sequence: u64,
        file_meta: FileMeta,
    },
    CompactionPublish {
        phase: PublicationPhase,
        cf_id: cntryl_midge_core::types::ColumnFamilyId,
        removed: Vec<String>,
        added: Vec<FileMeta>,
    },
    MoveSstsPublish {
        phase: PublicationPhase,
        captured: Vec<crate::metadata::manifest::FileMeta>,
        target_level: u32,
    },
    SstAdded {
        file_meta: FileMeta,
    },
    CompactionApplied {
        removed: Vec<String>,
        added: Vec<FileMeta>,
    },
    WalSynced {
        segment_id: u64,
        seqno: u64,
    },
    CloudUploadComplete {
        resource: String,
        seqno: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::FileMeta;

    #[test]
    fn should_round_trip_every_proof_field_across_file_meta_conversion() {
        // Arrange
        let runtime = FileMeta {
            name: "proof.sst".into(),
            level: 3,
            size_bytes: 1234,
            content_crc32c: Some(0x1234_5678),
            cf_id: 7,
            smallest_key: Some(b"alpha".to_vec()),
            largest_key: Some(b"omega".to_vec()),
            smallest_seq: Some(11),
            largest_seq: Some(99),
            key_bounds_complete: true,
            earliest_expiration_millis: None,
        };

        // Act
        let manifest = crate::metadata::manifest::FileMeta::from(&runtime);
        let round_trip = FileMeta::from(&manifest);

        // Assert
        assert_eq!(
            serde_json::to_value(&runtime).unwrap(),
            serde_json::to_value(&round_trip).unwrap()
        );
        assert!(manifest.same_identity(&crate::metadata::manifest::FileMeta::from(&round_trip)));
        let mut different = manifest.clone();
        different.content_crc32c = Some(0);
        assert!(!manifest.same_identity(&different));
    }
}
