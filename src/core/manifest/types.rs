///! Core data types for the manifest.

use serde::{Deserialize, Serialize};

use crate::api::column_family::ColumnFamilyConfig;

/// The manifest tracks all SSTs, column families, and checkpoint state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Manifest {
    pub last_persisted_sequence: u64,
    pub ssts: Vec<String>,
    #[serde(default)]
    pub files: Vec<FileMeta>,
    /// Column families in this database. Maps CF ID to (name, config).
    /// Always includes the default CF (id=0, name="default").
    #[serde(default)]
    pub column_families: Vec<ColumnFamilyMeta>,
    /// Cloud checkpoint tracking for WAL pruning coordination
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloud_checkpoint: Option<CloudCheckpoint>,
}

/// Checkpoint tracking highest WAL sequence fully materialized to cloud SSTs.
/// Used to coordinate safe WAL pruning - WAL segments can only be deleted
/// after their data is durably persisted to cloud SSTs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudCheckpoint {
    /// Highest WAL sequence number fully materialized to cloud SSTs
    pub checkpoint_sequence: u64,
    /// SST file names that cover up to this checkpoint
    pub covering_ssts: Vec<String>,
    /// Timestamp when this checkpoint was created
    pub checkpoint_time: std::time::SystemTime,
}

/// Metadata for a column family in the manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnFamilyMeta {
    pub id: u32,
    pub name: String,
    #[serde(default)]
    pub config: Option<ColumnFamilyConfig>,
}

/// Minimal file metadata to bootstrap a version set.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FileMeta {
    pub name: String,
    pub level: u32,
    pub size_bytes: u64,
    /// Column family this file belongs to. Defaults to 0 (default CF).
    #[serde(default)]
    pub cf_id: u32,
    #[serde(default)]
    pub smallest_key: Option<Vec<u8>>,
    #[serde(default)]
    pub largest_key: Option<Vec<u8>>,
    #[serde(default)]
    pub smallest_seq: Option<u64>,
    #[serde(default)]
    pub largest_seq: Option<u64>,
    /// Sublevel within L0 (0 = oldest/highest sublevel, higher = newer/lower sublevel)
    /// Only used for level=0. Files within same sublevel don't overlap in key space.
    /// Files in lower sublevels (higher numbers) are newer and should be checked first.
    #[serde(default)]
    pub sublevel: u32,

    // === Cloud SST Fields ===
    /// Full cloud storage path (e.g., "s3://bucket/realm/area/resource/sst/sst_000123.blob")
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloud_location: Option<String>,
    /// Checksum verified after cloud upload (xxhash)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloud_checksum: Option<u64>,
    /// Timestamp when uploaded to cloud
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloud_uploaded_at: Option<std::time::SystemTime>,
    /// Current lifecycle state in cloud storage
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloud_state: Option<crate::sst::cloud::SstLifecycleState>,

    // === Tombstone Tracking Fields ===
    /// Number of point tombstones in this SST
    #[serde(default)]
    pub point_tombstone_count: u64,
    /// Number of range tombstones in this SST
    #[serde(default)]
    pub range_tombstone_count: u64,
    /// Total number of entries (including tombstones) in this SST
    #[serde(default)]
    pub total_entries: u64,
}

impl FileMeta {
    /// Calculate the tombstone density percentage for this SST.
    /// Returns a value between 0.0 and 100.0.
    ///
    /// # Examples
    /// - 0% = No tombstones (pure live data)
    /// - 50% = Half the entries are tombstones
    /// - 100% = All entries are tombstones
    pub fn tombstone_density(&self) -> f64 {
        if self.total_entries == 0 {
            return 0.0;
        }
        let total_tombstones = self.point_tombstone_count + self.range_tombstone_count;
        (total_tombstones as f64 / self.total_entries as f64) * 100.0
    }

    /// Check if this SST has high tombstone density above the threshold.
    ///
    /// # Arguments
    /// * `threshold` - Percentage threshold (0.0 to 100.0)
    ///
    /// # Returns
    /// true if tombstone density >= threshold
    pub fn has_high_tombstone_density(&self, threshold: f64) -> bool {
        self.tombstone_density() >= threshold
    }

    /// Get total tombstone count (point + range).
    pub fn total_tombstones(&self) -> u64 {
        self.point_tombstone_count + self.range_tombstone_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::column_family::{ColumnFamilyConfig, ColumnFamilyId, DEFAULT_CF_ID};
    use crate::common::timestamp;

    #[test]
    fn should_serialize_manifest_with_column_families() {
        // Arrange
        let mut manifest = Manifest {
            last_persisted_sequence: 100,
            ..Default::default()
        };
        manifest.add_cf(DEFAULT_CF_ID, "default".to_string(), None);
        manifest.add_cf(
            ColumnFamilyId::new(1),
            "cf1".to_string(),
            Some(ColumnFamilyConfig::default()),
        );
        manifest.files.push(FileMeta {
            name: "test.sst".to_string(),
            level: 0,
            cf_id: 1,
            size_bytes: 1024,
            ..Default::default()
        });

        // Act
        let json = serde_json::to_string(&manifest).expect("serialize");

        // Assert
        assert!(json.contains("\"last_persisted_sequence\":100"));
    }

    #[test]
    fn should_deserialize_manifest_with_column_families() {
        // Arrange
        let mut manifest = Manifest {
            last_persisted_sequence: 100,
            ..Default::default()
        };
        manifest.add_cf(DEFAULT_CF_ID, "default".to_string(), None);
        manifest.add_cf(
            ColumnFamilyId::new(1),
            "cf1".to_string(),
            Some(ColumnFamilyConfig::default()),
        );
        manifest.files.push(FileMeta {
            name: "test.sst".to_string(),
            level: 0,
            cf_id: 1,
            size_bytes: 1024,
            ..Default::default()
        });
        let json = serde_json::to_string(&manifest).expect("serialize");

        // Act
        let deserialized: Manifest = serde_json::from_str(&json).expect("deserialize");

        // Assert
        assert_eq!(deserialized.last_persisted_sequence, 100);
        assert_eq!(deserialized.column_families.len(), 2);
        assert_eq!(deserialized.files.len(), 1);
        assert_eq!(deserialized.files[0].cf_id, 1);
    }

    #[test]
    fn should_serialize_file_metadata_with_cloud_upload_flag() {
        // Arrange
        let file = FileMeta {
            name: "cloud_file.sst".to_string(),
            level: 1,
            size_bytes: 2048,
            cloud_location: Some("s3://bucket/file.sst".to_string()),
            cloud_checksum: Some(0xABCD),
            cloud_uploaded_at: Some(timestamp::now()),
            cloud_state: Some(crate::sst::cloud::SstLifecycleState::Active),
            ..Default::default()
        };

        // Act
        let json = serde_json::to_string(&file).expect("serialize");
        let deserialized: FileMeta = serde_json::from_str(&json).expect("deserialize");

        // Assert
        assert_eq!(deserialized.cloud_location.unwrap(), "s3://bucket/file.sst");
        assert_eq!(deserialized.cloud_checksum.unwrap(), 0xABCD);
    }
}
