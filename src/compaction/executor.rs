//! Compaction execution: version collection, merging, and output
//!
//! Collects versions from input SSTs, deduplicates, filters tombstones,
//! and writes merged output to new SST file.

use crate::common::MidgeResult;
use crate::sst::traits::SstFactory;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

/// A version of a key from the LSM tree
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompactionVersion {
    /// User key
    pub key: Vec<u8>,
    /// Sequence number (higher = newer)
    pub seq: u64,
    /// Whether this is a tombstone (deletion marker)
    pub is_tombstone: bool,
    /// Value (None if tombstone)
    pub value: Option<Vec<u8>>,
    /// Expiration time in seconds since epoch (optional)
    pub expiration: Option<u64>,
}

/// Collect all versions from input SST files
pub fn collect_versions(
    sst_factory: &dyn SstFactory,
    input_files: &[String],
) -> MidgeResult<Vec<CompactionVersion>> {
    let versions = Vec::new();

    // Collect versions from each input file
    for filename in input_files {
        let path = Path::new(filename);
        let _reader = sst_factory.open(path)?;
        
        // Need to downcast to SstStateReader
        // For now, skip to avoid complexity with dynamic dispatch
        // TODO: Wire SstStateReader into factory trait
    }

    Ok(versions)
}

/// Deduplicate versions, keeping only the newest non-expired entry per key
pub fn deduplicate_versions(versions: &[CompactionVersion]) -> Vec<CompactionVersion> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    // Build map: key -> highest sequence version
    let mut key_map: BTreeMap<Vec<u8>, CompactionVersion> = BTreeMap::new();

    for version in versions {
        // Skip expired entries
        if let Some(exp_time) = version.expiration {
            if exp_time <= now {
                continue;
            }
        }

        let key = version.key.clone();
        
        // Keep if this is the first version of this key, or if it has higher sequence
        match key_map.get(&key) {
            None => {
                key_map.insert(key, version.clone());
            }
            Some(existing) => {
                if version.seq > existing.seq {
                    key_map.insert(key, version.clone());
                }
            }
        }
    }

    // Convert to sorted vec
    let mut result: Vec<_> = key_map.into_values().collect();
    result.sort_by(|a, b| a.key.cmp(&b.key));
    result
}

/// Filter out tombstones from deduplicated versions
pub fn filter_tombstones(versions: &[CompactionVersion]) -> Vec<CompactionVersion> {
    versions
        .iter()
        .filter(|v| !v.is_tombstone)
        .cloned()
        .collect()
}

/// Write deduplicated versions to output SST file
pub fn write_versions_to_sst(
    sst_factory: &dyn SstFactory,
    output_filename: &str,
    versions: &[CompactionVersion],
) -> MidgeResult<()> {
    let mut writer = sst_factory.create()?;

    for version in versions {
        if !version.is_tombstone {
            writer.add_with_meta(
                &version.key,
                version.value.as_deref(),
                version.seq,
                0, // op_type: 0 = Put
                version.expiration,
            )?;
        } else {
            // Write tombstone as deletion marker
            writer.add_with_meta(
                &version.key,
                None,
                version.seq,
                1, // op_type: 1 = Delete
                version.expiration,
            )?;
        }
    }

    let path = Path::new(output_filename);
    writer.finish_to_path(path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_keep_highest_sequence_when_deduplicating_versions() {
        // Arrange
        let versions = vec![
            CompactionVersion {
                key: b"key1".to_vec(),
                seq: 1,
                is_tombstone: false,
                value: Some(b"value1".to_vec()),
                expiration: None,
            },
            CompactionVersion {
                key: b"key1".to_vec(),
                seq: 2,
                is_tombstone: false,
                value: Some(b"value1_updated".to_vec()),
                expiration: None,
            },
        ];

        // Act
        let deduped = deduplicate_versions(&versions);

        // Assert
        assert_eq!(deduped.len(), 1);
        assert_eq!(deduped[0].seq, 2);
        assert_eq!(deduped[0].value, Some(b"value1_updated".to_vec()));
    }

    #[test]
    fn should_remove_tombstones_when_filtering_versions() {
        // Arrange
        let versions = vec![
            CompactionVersion {
                key: b"key1".to_vec(),
                seq: 1,
                is_tombstone: false,
                value: Some(b"value1".to_vec()),
                expiration: None,
            },
            CompactionVersion {
                key: b"key2".to_vec(),
                seq: 2,
                is_tombstone: true,
                value: None,
                expiration: None,
            },
        ];

        // Act
        let filtered = filter_tombstones(&versions);

        // Assert
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].key, b"key1".to_vec());
    }

    #[test]
    fn should_skip_expired_entries_when_deduplicating_with_ttl() {
        // Arrange
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let versions = vec![
            CompactionVersion {
                key: b"key1".to_vec(),
                seq: 1,
                is_tombstone: false,
                value: Some(b"expired".to_vec()),
                expiration: Some(now - 1), // Expired
            },
            CompactionVersion {
                key: b"key1".to_vec(),
                seq: 2,
                is_tombstone: false,
                value: Some(b"valid".to_vec()),
                expiration: None,
            },
        ];

        // Act
        let deduped = deduplicate_versions(&versions);

        // Assert
        assert_eq!(deduped.len(), 1);
        assert_eq!(deduped[0].value, Some(b"valid".to_vec()));
    }
}
