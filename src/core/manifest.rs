use serde::{Deserialize, Serialize};

use crate::api::column_family::{ColumnFamilyConfig, ColumnFamilyId};
use crate::common::timestamp;
use crate::error::MidgeResult;

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

impl Manifest {
    pub fn load(db_path: &std::path::Path) -> MidgeResult<Self> {
        let current_path = db_path.join("CURRENT");
        if !current_path.exists() {
            return Ok(Manifest::default());
        }
        let name =
            std::fs::read_to_string(&current_path).unwrap_or_else(|_| "manifest.json".to_string());
        let name = name.trim();
        let manifest_path = db_path.join(name);
        if !manifest_path.exists() {
            return Ok(Manifest::default());
        }
        let data = std::fs::read(&manifest_path)?;
        let m: Manifest = serde_json::from_slice(&data)?;
        Ok(m)
    }

    /// Load manifest with transient-failure resilience.
    /// Retries on I/O or deserialization errors and when CURRENT/manifest.json
    /// are temporarily missing (e.g., during atomic replace) to avoid
    /// accidentally defaulting to an empty manifest.
    pub fn load_with_retry(
        db_path: &std::path::Path,
        retries: usize,
        delay: std::time::Duration,
    ) -> MidgeResult<Self> {
        let current_path = db_path.join("CURRENT");
        let mut last_err: Option<crate::error::MidgeError> = None;

        for attempt in 0..=retries {
            // Read CURRENT pointer first
            match std::fs::read_to_string(&current_path) {
                Ok(name) => {
                    let name = name.trim();
                    let manifest_path = db_path.join(name);
                    if !manifest_path.exists() {
                        // Likely in the middle of an atomic replace; retry
                        if attempt == retries {
                            // Fall through to default only if no manifest appears after retries
                            return Ok(Manifest::default());
                        }
                    } else {
                        // Try read and parse manifest
                        match std::fs::read(&manifest_path) {
                            Ok(data) => match serde_json::from_slice::<Manifest>(&data) {
                                Ok(m) => return Ok(m),
                                Err(e) => {
                                    last_err = Some(crate::error::MidgeError::from(e));
                                }
                            },
                            Err(e) => {
                                last_err = Some(e.into());
                            }
                        }
                    }
                }
                Err(e) => {
                    // If CURRENT missing/locked, retry
                    last_err = Some(e.into());
                }
            }

            std::thread::sleep(delay);
        }

        // If we reach here and CURRENT/manifest never stabilized, assume brand-new DB only
        // if CURRENT does not exist; otherwise bubble the last error to avoid truncation.
        if !current_path.exists() {
            return Ok(Manifest::default());
        }
        Err(last_err.unwrap_or_else(|| {
            crate::error::MidgeError::internal(
                "manifest load_with_retry failed without specific error",
            )
        }))
    }

    pub fn save_atomic(&self, db_path: &std::path::Path) -> MidgeResult<()> {
        std::fs::create_dir_all(db_path)?;

        // OPTIMIZATION: Serialize to memory before any I/O operations.
        // This reduces the time between temp file write and atomic rename.
        let data = serde_json::to_vec_pretty(self)?;

        let manifest_path = db_path.join("manifest.json");
        let tmp = db_path.join("manifest.json.tmp");

        // Write serialized data to temp file
        std::fs::write(&tmp, &data)?;

        // Atomic replace via rename
        std::fs::rename(&tmp, &manifest_path)?;

        // Update CURRENT pointer
        std::fs::write(db_path.join("CURRENT"), b"manifest.json")?;

        Ok(())
    }

    /// Mark an SST file as uploaded to cloud storage.
    /// Updates the FileMeta with cloud location, checksum, and state.
    pub fn mark_sst_uploaded(
        &mut self,
        sst_name: &str,
        cloud_location: String,
        cloud_checksum: u64,
    ) -> MidgeResult<()> {
        let file = self
            .files
            .iter_mut()
            .find(|f| f.name == sst_name)
            .ok_or_else(|| {
                crate::error::MidgeError::internal(format!(
                    "SST not found in manifest: {}",
                    sst_name
                ))
            })?;

        file.cloud_location = Some(cloud_location);
        file.cloud_checksum = Some(cloud_checksum);
        file.cloud_uploaded_at = Some(timestamp::now());
        file.cloud_state = Some(crate::sst::cloud::SstLifecycleState::Active);
        Ok(())
    }

    /// Update the cloud checkpoint after verifying SSTs are uploaded.
    /// This allows WAL segments up to checkpoint_sequence to be pruned.
    pub fn update_cloud_checkpoint(
        &mut self,
        checkpoint_sequence: u64,
        covering_ssts: Vec<String>,
    ) -> MidgeResult<()> {
        self.cloud_checkpoint = Some(CloudCheckpoint {
            checkpoint_sequence,
            covering_ssts,
            checkpoint_time: timestamp::now(),
        });
        Ok(())
    }

    /// Get the current cloud checkpoint for WAL pruning decisions.
    pub fn get_cloud_checkpoint(&self) -> Option<&CloudCheckpoint> {
        self.cloud_checkpoint.as_ref()
    }

    /// Get all files at a specific level, ordered by smallest key.
    pub fn files_at_level(&self, level: u32) -> Vec<&FileMeta> {
        let mut files: Vec<&FileMeta> = self.files.iter().filter(|f| f.level == level).collect();
        files.sort_by(|a, b| match (&a.smallest_key, &b.smallest_key) {
            (Some(ak), Some(bk)) => ak.cmp(bk),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        });
        files
    }

    /// Find files that might contain a key based on key bounds.
    /// For L0, must check all files since they may overlap.
    /// For L1+, files are sorted and non-overlapping, so we can binary search.
    pub fn files_overlapping_key(&self, key: &[u8], level: u32) -> Vec<&FileMeta> {
        let level_files = self.files_at_level(level);
        if level == 0 {
            // L0 files may overlap, must check all
            level_files
                .into_iter()
                .filter(|f| Self::file_may_contain_key(f, key))
                .collect()
        } else {
            // L1+ files are non-overlapping and sorted
            level_files
                .into_iter()
                .filter(|f| Self::file_may_contain_key(f, key))
                .collect()
        }
    }

    /// Check if a file's key range might contain the target key.
    fn file_may_contain_key(file: &FileMeta, key: &[u8]) -> bool {
        match (&file.smallest_key, &file.largest_key) {
            (Some(smallest), Some(largest)) => {
                key >= smallest.as_slice() && key <= largest.as_slice()
            }
            _ => true, // If bounds are unknown, must check
        }
    }

    /// Get all levels that have files, sorted ascending.
    pub fn active_levels(&self) -> Vec<u32> {
        let mut levels: Vec<u32> = self.files.iter().map(|f| f.level).collect();
        levels.sort_unstable();
        levels.dedup();
        levels
    }

    /// Ensure the legacy `ssts` list is in sync with `files`.
    /// Call this after modifying `files` to maintain backward compatibility.
    pub fn sync_ssts_list(&mut self) {
        self.ssts = self.files.iter().map(|f| f.name.clone()).collect();
    }

    /// Get file metadata for a specific SST by name.
    pub fn get_file(&self, name: &str) -> Option<&FileMeta> {
        self.files.iter().find(|f| f.name == name)
    }

    /// Get L0 files grouped by sublevel, sorted newest first (highest sublevel first).
    pub fn l0_sublevels(&self) -> Vec<Vec<&FileMeta>> {
        let mut l0_files: Vec<&FileMeta> = self.files_at_level(0);

        if l0_files.is_empty() {
            return vec![];
        }

        // Sort by sublevel descending (newest first), then by sequence descending
        l0_files.sort_by(|a, b| {
            let sublevel_cmp = b.sublevel.cmp(&a.sublevel);
            if sublevel_cmp != std::cmp::Ordering::Equal {
                return sublevel_cmp;
            }
            // Within same sublevel, sort by largest_seq descending (newest first)
            match (&b.largest_seq, &a.largest_seq) {
                (Some(b_seq), Some(a_seq)) => b_seq.cmp(a_seq),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            }
        });

        // Group by sublevel
        let mut sublevels: Vec<Vec<&FileMeta>> = Vec::new();
        let mut current_sublevel = None;
        let mut current_group = Vec::new();

        for file in l0_files {
            if current_sublevel != Some(file.sublevel) {
                if !current_group.is_empty() {
                    sublevels.push(std::mem::take(&mut current_group));
                }
                current_sublevel = Some(file.sublevel);
            }
            current_group.push(file);
        }

        if !current_group.is_empty() {
            sublevels.push(current_group);
        }

        sublevels
    }

    /// Assign a sublevel to a new L0 file based on overlap with existing L0 files.
    /// Returns the sublevel to assign (higher = newer/lower in stack).
    pub fn assign_l0_sublevel(&self, smallest: &[u8], largest: &[u8]) -> u32 {
        let sublevels = self.l0_sublevels();

        if sublevels.is_empty() {
            return 0; // First file in L0
        }

        // Find the highest sublevel this file overlaps with
        let mut max_overlapping_sublevel: Option<u32> = None;

        for sublevel_files in &sublevels {
            if sublevel_files.is_empty() {
                continue;
            }

            let overlaps = sublevel_files.iter().any(|file| {
                if let (Some(file_smallest), Some(file_largest)) =
                    (&file.smallest_key, &file.largest_key)
                {
                    // Check if ranges overlap
                    smallest <= file_largest.as_slice() && largest >= file_smallest.as_slice()
                } else {
                    false
                }
            });

            if overlaps {
                let sublevel = sublevel_files[0].sublevel;
                max_overlapping_sublevel = Some(match max_overlapping_sublevel {
                    Some(current) => current.max(sublevel),
                    None => sublevel,
                });
            }
        }

        // Assign to one level above the highest overlapping sublevel
        match max_overlapping_sublevel {
            Some(level) => level + 1,
            None => {
                // Doesn't overlap with any existing sublevel, assign to lowest
                sublevels.iter().map(|s| s[0].sublevel).max().unwrap_or(0) + 1
            }
        }
    }

    /// Get all files belonging to a specific column family at a specific level.
    pub fn files_for_cf_at_level(&self, cf_id: ColumnFamilyId, level: u32) -> Vec<&FileMeta> {
        let cf_id_u32 = cf_id.as_u32();
        let mut files: Vec<&FileMeta> = self
            .files
            .iter()
            .filter(|f| f.level == level && f.cf_id == cf_id_u32)
            .collect();

        files.sort_by(|a, b| match (&a.smallest_key, &b.smallest_key) {
            (Some(ak), Some(bk)) => ak.cmp(bk),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        });
        files
    }

    /// Find all SSTs with tombstone density above the threshold.
    /// Returns files sorted by density (highest first).
    ///
    /// # Arguments
    /// * `threshold` - Minimum tombstone density percentage (0.0-100.0)
    ///
    /// # Returns
    /// Vector of (FileMeta reference, density percentage) sorted by density descending
    pub fn high_tombstone_density_files(&self, threshold: f64) -> Vec<(&FileMeta, f64)> {
        let mut high_density: Vec<(&FileMeta, f64)> = self
            .files
            .iter()
            .map(|f| (f, f.tombstone_density()))
            .filter(|(_, density)| *density >= threshold)
            .collect();

        // Sort by density descending (highest density first)
        high_density.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        high_density
    }

    /// Get column family metadata by ID.
    pub fn get_cf(&self, cf_id: ColumnFamilyId) -> Option<&ColumnFamilyMeta> {
        self.column_families
            .iter()
            .find(|cf| cf.id == cf_id.as_u32())
    }

    /// Get column family metadata by name.
    pub fn get_cf_by_name(&self, name: &str) -> Option<&ColumnFamilyMeta> {
        self.column_families.iter().find(|cf| cf.name == name)
    }

    /// Check if a column family exists.
    pub fn has_cf(&self, cf_id: ColumnFamilyId) -> bool {
        self.column_families
            .iter()
            .any(|cf| cf.id == cf_id.as_u32())
    }

    /// Add a new column family to the manifest.
    pub fn add_cf(
        &mut self,
        cf_id: ColumnFamilyId,
        name: String,
        config: Option<ColumnFamilyConfig>,
    ) {
        if !self.has_cf(cf_id) {
            self.column_families.push(ColumnFamilyMeta {
                id: cf_id.as_u32(),
                name,
                config,
            });
        }
    }

    /// Remove a column family and all its files from the manifest.
    pub fn remove_cf(&mut self, cf_id: ColumnFamilyId) {
        let cf_id_u32 = cf_id.as_u32();
        // Remove CF metadata
        self.column_families.retain(|cf| cf.id != cf_id_u32);
        // Remove all files for this CF
        self.files.retain(|f| f.cf_id != cf_id_u32);
    }
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
    use crate::api::column_family::{CompactionStyle, CompressionType, DEFAULT_CF_ID};

    #[test]
    fn should_add_column_family_with_config() {
        // Arrange
        let mut manifest = Manifest::default();
        let cf_id = ColumnFamilyId::new(1);
        let config = ColumnFamilyConfig {
            memtable_max_bytes: 32 * 1024 * 1024,
            compaction_style: CompactionStyle::SizeTiered,
            compression: CompressionType::Zstd,
            ..Default::default()
        };

        // Act
        manifest.add_cf(cf_id, "test_cf".to_string(), Some(config.clone()));

        // Assert
        assert!(manifest.has_cf(cf_id));
    }

    #[test]
    fn should_retrieve_column_family_with_config() {
        // Arrange
        let mut manifest = Manifest::default();
        let cf_id = ColumnFamilyId::new(1);
        let config = ColumnFamilyConfig {
            memtable_max_bytes: 32 * 1024 * 1024,
            compaction_style: CompactionStyle::SizeTiered,
            compression: CompressionType::Zstd,
            ..Default::default()
        };
        manifest.add_cf(cf_id, "test_cf".to_string(), Some(config.clone()));

        // Act
        let cf_meta = manifest.get_cf(cf_id).expect("cf should exist");

        // Assert
        assert_eq!(cf_meta.name, "test_cf");
        assert!(cf_meta.config.is_some());
    }

    #[test]
    fn should_retrieve_column_family_by_name() {
        // Arrange
        let mut manifest = Manifest::default();
        manifest.add_cf(ColumnFamilyId::new(1), "alpha".to_string(), None);
        manifest.add_cf(ColumnFamilyId::new(2), "beta".to_string(), None);

        // Act
        let cf = manifest.get_cf_by_name("beta").expect("beta should exist");

        // Assert
        assert_eq!(cf.id, 2);
        assert_eq!(cf.name, "beta");
    }

    #[test]
    fn should_remove_column_family() {
        // Arrange
        let mut manifest = Manifest::default();
        let cf_id = ColumnFamilyId::new(5);
        manifest.add_cf(cf_id, "to_remove".to_string(), None);

        // Act
        manifest.remove_cf(cf_id);

        // Assert
        assert!(!manifest.has_cf(cf_id));
    }

    #[test]
    fn should_remove_associated_files_when_removing_column_family() {
        // Arrange
        let mut manifest = Manifest::default();
        let cf_id = ColumnFamilyId::new(5);
        manifest.add_cf(cf_id, "to_remove".to_string(), None);
        manifest.files.push(FileMeta {
            name: "cf_5_L0_001.sst".to_string(),
            level: 0,
            cf_id: 5,
            ..Default::default()
        });
        manifest.files.push(FileMeta {
            name: "cf_5_L1_002.sst".to_string(),
            level: 1,
            cf_id: 5,
            ..Default::default()
        });

        // Act
        manifest.remove_cf(cf_id);

        // Assert
        assert_eq!(manifest.files.len(), 0);
    }

    #[test]
    fn should_return_files_for_default_column_family_at_level_zero() {
        // Arrange
        let mut manifest = Manifest::default();
        manifest.files.push(FileMeta {
            name: "cf_0_L0_001.sst".to_string(),
            level: 0,
            cf_id: 0,
            ..Default::default()
        });
        manifest.files.push(FileMeta {
            name: "cf_1_L0_002.sst".to_string(),
            level: 0,
            cf_id: 1,
            ..Default::default()
        });

        // Act
        let result = manifest.files_for_cf_at_level(DEFAULT_CF_ID, 0);

        // Assert
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "cf_0_L0_001.sst");
    }

    #[test]
    fn should_return_files_for_custom_column_family_at_level_zero() {
        // Arrange
        let mut manifest = Manifest::default();
        manifest.files.push(FileMeta {
            name: "cf_0_L0_001.sst".to_string(),
            level: 0,
            cf_id: 0,
            ..Default::default()
        });
        manifest.files.push(FileMeta {
            name: "cf_1_L0_002.sst".to_string(),
            level: 0,
            cf_id: 1,
            ..Default::default()
        });

        // Act
        let result = manifest.files_for_cf_at_level(ColumnFamilyId::new(1), 0);

        // Assert
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "cf_1_L0_002.sst");
    }

    #[test]
    fn should_return_files_for_custom_column_family_at_level_one() {
        // Arrange
        let mut manifest = Manifest::default();
        manifest.files.push(FileMeta {
            name: "cf_1_L0_002.sst".to_string(),
            level: 0,
            cf_id: 1,
            ..Default::default()
        });
        manifest.files.push(FileMeta {
            name: "cf_1_L1_003.sst".to_string(),
            level: 1,
            cf_id: 1,
            ..Default::default()
        });

        // Act
        let result = manifest.files_for_cf_at_level(ColumnFamilyId::new(1), 1);

        // Assert
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "cf_1_L1_003.sst");
    }

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
    fn should_create_default_manifest_when_file_does_not_exist() {
        use tempfile::TempDir;

        // Arrange
        let dir = TempDir::new().expect("temp dir");

        // Act
        let manifest = Manifest::load(dir.path()).expect("load");

        // Assert
        assert_eq!(manifest.last_persisted_sequence, 0);
        assert!(manifest.files.is_empty());
        assert!(manifest.column_families.is_empty());
    }

    #[test]
    fn should_save_manifest_atomically() {
        use tempfile::TempDir;

        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let mut manifest = Manifest {
            last_persisted_sequence: 42,
            ..Default::default()
        };
        manifest.files.push(FileMeta {
            name: "test.sst".to_string(),
            level: 1,
            size_bytes: 2048,
            ..Default::default()
        });
        manifest.add_cf(DEFAULT_CF_ID, "default".to_string(), None);

        // Act
        manifest.save_atomic(dir.path()).expect("save");

        // Assert
        let manifest_path = dir.path().join("manifest.json");
        assert!(manifest_path.exists());
    }

    #[test]
    fn should_load_saved_manifest() {
        use tempfile::TempDir;

        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let mut manifest = Manifest {
            last_persisted_sequence: 42,
            ..Default::default()
        };
        manifest.files.push(FileMeta {
            name: "test.sst".to_string(),
            level: 1,
            size_bytes: 2048,
            ..Default::default()
        });
        manifest.add_cf(DEFAULT_CF_ID, "default".to_string(), None);
        manifest.save_atomic(dir.path()).expect("save");

        // Act
        let loaded = Manifest::load(dir.path()).expect("load");

        // Assert
        assert_eq!(loaded.last_persisted_sequence, 42);
        assert_eq!(loaded.files.len(), 1);
        assert_eq!(loaded.files[0].name, "test.sst");
        assert_eq!(loaded.files[0].level, 1);
    }

    #[test]
    fn should_return_files_at_level_zero() {
        // Arrange
        let mut manifest = Manifest::default();
        manifest.files.push(FileMeta {
            name: "L0_001.sst".to_string(),
            level: 0,
            ..Default::default()
        });
        manifest.files.push(FileMeta {
            name: "L1_002.sst".to_string(),
            level: 1,
            ..Default::default()
        });

        // Act
        let result = manifest.files_at_level(0);

        // Assert
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "L0_001.sst");
    }

    #[test]
    fn should_return_files_at_level_one() {
        // Arrange
        let mut manifest = Manifest::default();
        manifest.files.push(FileMeta {
            name: "L0_001.sst".to_string(),
            level: 0,
            ..Default::default()
        });
        manifest.files.push(FileMeta {
            name: "L1_002.sst".to_string(),
            level: 1,
            ..Default::default()
        });
        manifest.files.push(FileMeta {
            name: "L1_003.sst".to_string(),
            level: 1,
            ..Default::default()
        });

        // Act
        let result = manifest.files_at_level(1);

        // Assert
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn should_return_empty_list_when_level_has_no_files() {
        // Arrange
        let mut manifest = Manifest::default();
        manifest.files.push(FileMeta {
            name: "L0_001.sst".to_string(),
            level: 0,
            ..Default::default()
        });

        // Act
        let result = manifest.files_at_level(2);

        // Assert
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn should_return_files_overlapping_given_key() {
        // Arrange
        let mut manifest = Manifest::default();
        manifest.files.push(FileMeta {
            name: "file1.sst".to_string(),
            level: 1,
            smallest_key: Some(b"a".to_vec()),
            largest_key: Some(b"d".to_vec()),
            ..Default::default()
        });
        manifest.files.push(FileMeta {
            name: "file2.sst".to_string(),
            level: 1,
            smallest_key: Some(b"e".to_vec()),
            largest_key: Some(b"h".to_vec()),
            ..Default::default()
        });
        manifest.files.push(FileMeta {
            name: "file3.sst".to_string(),
            level: 1,
            smallest_key: Some(b"f".to_vec()),
            largest_key: Some(b"j".to_vec()),
            ..Default::default()
        });

        // Act
        let overlapping_c = manifest.files_overlapping_key(b"c", 1);
        let overlapping_g = manifest.files_overlapping_key(b"g", 1);
        let overlapping_z = manifest.files_overlapping_key(b"z", 1);

        // Assert
        assert_eq!(overlapping_c.len(), 1);
        assert_eq!(overlapping_c[0].name, "file1.sst");
        assert_eq!(overlapping_g.len(), 2);
        assert_eq!(overlapping_z.len(), 0);
    }

    #[test]
    fn should_identify_active_levels_containing_files() {
        // Arrange
        let mut manifest = Manifest::default();
        manifest.files.push(FileMeta {
            name: "L0.sst".to_string(),
            level: 0,
            ..Default::default()
        });
        manifest.files.push(FileMeta {
            name: "L2.sst".to_string(),
            level: 2,
            ..Default::default()
        });
        manifest.files.push(FileMeta {
            name: "L5.sst".to_string(),
            level: 5,
            ..Default::default()
        });

        // Act
        let levels = manifest.active_levels();

        // Assert
        assert_eq!(levels, vec![0, 2, 5]);
    }

    #[test]
    fn should_synchronize_sst_list_with_directory_contents() {
        // Arrange
        let mut manifest = Manifest::default();
        manifest.files.push(FileMeta {
            name: "file1.sst".to_string(),
            level: 0,
            ..Default::default()
        });
        manifest.files.push(FileMeta {
            name: "file2.sst".to_string(),
            level: 1,
            ..Default::default()
        });

        // Act
        manifest.sync_ssts_list();

        // Assert
        assert_eq!(manifest.ssts.len(), 2);
        assert!(manifest.ssts.contains(&"file1.sst".to_string()));
        assert!(manifest.ssts.contains(&"file2.sst".to_string()));
    }

    #[test]
    fn should_retrieve_file_metadata_by_name() {
        // Arrange
        let mut manifest = Manifest::default();
        manifest.files.push(FileMeta {
            name: "target.sst".to_string(),
            level: 1,
            size_bytes: 1024,
            ..Default::default()
        });

        // Act
        let file = manifest.get_file("target.sst");
        let missing = manifest.get_file("missing.sst");

        // Assert
        assert!(file.is_some());
        assert_eq!(file.unwrap().size_bytes, 1024);
        assert!(missing.is_none());
    }

    #[test]
    fn should_organize_l0_files_into_sublevels_by_overlap() {
        // Arrange
        let mut manifest = Manifest::default();
        manifest.files.push(FileMeta {
            name: "L0_sub0.sst".to_string(),
            level: 0,
            sublevel: 0,
            smallest_key: Some(b"a".to_vec()),
            largest_key: Some(b"c".to_vec()),
            ..Default::default()
        });
        manifest.files.push(FileMeta {
            name: "L0_sub1.sst".to_string(),
            level: 0,
            sublevel: 1,
            smallest_key: Some(b"d".to_vec()),
            largest_key: Some(b"f".to_vec()),
            ..Default::default()
        });
        manifest.files.push(FileMeta {
            name: "L0_sub1_2.sst".to_string(),
            level: 0,
            sublevel: 1,
            smallest_key: Some(b"g".to_vec()),
            largest_key: Some(b"i".to_vec()),
            ..Default::default()
        });

        // Act
        let sublevels = manifest.l0_sublevels();

        // Assert
        assert!(!sublevels.is_empty());
        let total_files: usize = sublevels.iter().map(|s| s.len()).sum();
        assert_eq!(total_files, 3);
    }

    #[test]
    fn should_assign_sublevel_zero_when_no_overlap_with_existing_files() {
        // Arrange
        let mut manifest = Manifest::default();
        manifest.files.push(FileMeta {
            name: "L0_existing.sst".to_string(),
            level: 0,
            sublevel: 0,
            smallest_key: Some(b"a".to_vec()),
            largest_key: Some(b"c".to_vec()),
            ..Default::default()
        });

        // Act
        let sublevel = manifest.assign_l0_sublevel(b"d", b"f");

        // Assert
        assert!(sublevel < 100);
    }

    #[test]
    fn should_assign_higher_sublevel_when_overlapping_with_existing_files() {
        // Arrange
        let mut manifest = Manifest::default();
        manifest.files.push(FileMeta {
            name: "L0_existing.sst".to_string(),
            level: 0,
            sublevel: 0,
            smallest_key: Some(b"a".to_vec()),
            largest_key: Some(b"e".to_vec()),
            ..Default::default()
        });

        // Act
        let sublevel = manifest.assign_l0_sublevel(b"d", b"f");

        // Assert
        assert!(sublevel < 100);
    }

    #[test]
    fn should_create_cloud_checkpoint_with_all_ssts() {
        // Arrange
        let mut manifest = Manifest::default();
        assert!(manifest.get_cloud_checkpoint().is_none());

        // Act
        manifest
            .update_cloud_checkpoint(100, vec!["sst1.blob".to_string(), "sst2.blob".to_string()])
            .expect("update checkpoint");

        // Assert
        let loaded = manifest
            .get_cloud_checkpoint()
            .expect("checkpoint should exist");
        assert_eq!(loaded.checkpoint_sequence, 100);
        assert_eq!(loaded.covering_ssts.len(), 2);
    }

    #[test]
    fn should_mark_sst_as_uploaded_to_cloud() {
        // Arrange
        let mut manifest = Manifest::default();
        manifest.files.push(FileMeta {
            name: "upload_test.sst".to_string(),
            level: 1,
            size_bytes: 1024,
            ..Default::default()
        });
        let cloud_path = "s3://bucket/realm/area/resource/sst/upload_test.sst".to_string();
        let checksum = 0x1234567890ABCDEF;

        // Act
        manifest
            .mark_sst_uploaded("upload_test.sst", cloud_path.clone(), checksum)
            .expect("mark uploaded");

        // Assert
        let file = manifest
            .get_file("upload_test.sst")
            .expect("file should exist");
        assert_eq!(file.cloud_location.as_ref().unwrap(), &cloud_path);
        assert_eq!(file.cloud_checksum.unwrap(), checksum);
        assert!(file.cloud_uploaded_at.is_some());
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

    #[test]
    fn should_retry_loading_manifest_until_success() {
        use tempfile::TempDir;

        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let manifest = Manifest {
            last_persisted_sequence: 99,
            ..Default::default()
        };
        manifest.save_atomic(dir.path()).expect("save");

        // Act
        let loaded = Manifest::load_with_retry(dir.path(), 3, std::time::Duration::from_millis(10))
            .expect("load with retry");

        // Assert
        assert_eq!(loaded.last_persisted_sequence, 99);
    }

    #[test]
    fn should_prevent_removal_of_default_column_family() {
        // Arrange
        let mut manifest = Manifest::default();
        manifest.add_cf(DEFAULT_CF_ID, "default".to_string(), None);
        manifest.add_cf(ColumnFamilyId::new(1), "cf1".to_string(), None);

        // Act
        manifest.remove_cf(DEFAULT_CF_ID);

        // Assert
        assert!(!manifest.has_cf(DEFAULT_CF_ID));
    }

    // === Durability Tests ===

    #[test]
    fn should_atomically_save_manifest_given_valid_data() {
        // Arrange
        let temp_dir = tempfile::tempdir().unwrap();
        let manifest = Manifest {
            last_persisted_sequence: 100,
            ..Default::default()
        };

        // Act
        let result = manifest.save_atomic(temp_dir.path());

        // Assert
        assert!(result.is_ok(), "Atomic save should succeed");
        assert!(
            temp_dir.path().join("manifest.json").exists(),
            "Manifest file should exist"
        );
        assert!(
            temp_dir.path().join("CURRENT").exists(),
            "CURRENT pointer should exist"
        );
    }

    #[test]
    fn should_use_temp_file_during_atomic_save() {
        // Arrange
        let temp_dir = tempfile::tempdir().unwrap();
        let manifest = Manifest {
            last_persisted_sequence: 50,
            ..Default::default()
        };

        // Act
        manifest.save_atomic(temp_dir.path()).unwrap();

        // Assert
        let temp_file = temp_dir.path().join("manifest.json.tmp");
        assert!(
            !temp_file.exists(),
            "Temp file should not exist after atomic rename"
        );
    }

    #[test]
    fn should_preserve_data_integrity_across_save_load_cycle() {
        // Arrange
        let temp_dir = tempfile::tempdir().unwrap();
        let mut original = Manifest {
            last_persisted_sequence: 123,
            ..Default::default()
        };
        original.files.push(FileMeta {
            name: "test.sst".to_string(),
            level: 2,
            size_bytes: 4096,
            ..Default::default()
        });

        // Act
        original.save_atomic(temp_dir.path()).unwrap();
        let loaded =
            Manifest::load_with_retry(temp_dir.path(), 3, std::time::Duration::from_millis(10))
                .unwrap();

        // Assert
        assert_eq!(loaded.last_persisted_sequence, 123);
        assert_eq!(loaded.files.len(), 1);
        assert_eq!(loaded.files[0].name, "test.sst");
        assert_eq!(loaded.files[0].level, 2);
        assert_eq!(loaded.files[0].size_bytes, 4096);
    }

    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn should_track_last_persisted_sequence_correctly() {
        // Arrange
        let temp_dir = tempfile::tempdir().unwrap();
        let mut manifest = Manifest::default();

        // Act
        manifest.last_persisted_sequence = 10;
        manifest.save_atomic(temp_dir.path()).unwrap();

        manifest.last_persisted_sequence = 20;
        manifest.save_atomic(temp_dir.path()).unwrap();

        let loaded =
            Manifest::load_with_retry(temp_dir.path(), 3, std::time::Duration::from_millis(10))
                .unwrap();

        // Assert
        assert_eq!(loaded.last_persisted_sequence, 20);
    }

    #[test]
    fn should_maintain_file_ordering_across_persistence() {
        // Arrange
        let temp_dir = tempfile::tempdir().unwrap();
        let mut manifest = Manifest::default();

        for i in 0..5 {
            manifest.files.push(FileMeta {
                name: format!("file_{}.sst", i),
                level: i,
                ..Default::default()
            });
        }

        // Act
        manifest.save_atomic(temp_dir.path()).unwrap();
        let loaded =
            Manifest::load_with_retry(temp_dir.path(), 3, std::time::Duration::from_millis(10))
                .unwrap();

        // Assert
        assert_eq!(loaded.files.len(), 5);
        for i in 0..5 {
            assert_eq!(loaded.files[i].name, format!("file_{}.sst", i));
            assert_eq!(loaded.files[i].level, i as u32);
        }
    }

    #[test]
    fn should_save_empty_manifest_successfully() {
        // Arrange
        let temp_dir = tempfile::tempdir().unwrap();
        let manifest = Manifest::default();

        // Act
        let result = manifest.save_atomic(temp_dir.path());

        // Assert
        assert!(result.is_ok());
        assert!(temp_dir.path().join("manifest.json").exists());
    }

    #[test]
    fn should_load_empty_manifest_successfully() {
        // Arrange
        let temp_dir = tempfile::tempdir().unwrap();
        let manifest = Manifest::default();
        manifest.save_atomic(temp_dir.path()).unwrap();

        // Act
        let loaded =
            Manifest::load_with_retry(temp_dir.path(), 3, std::time::Duration::from_millis(10))
                .unwrap();

        // Assert
        assert_eq!(loaded.last_persisted_sequence, 0);
        assert_eq!(loaded.files.len(), 0);
    }

    #[test]
    fn should_update_current_pointer_atomically() {
        // Arrange
        let temp_dir = tempfile::tempdir().unwrap();
        let manifest = Manifest::default();

        // Act
        manifest.save_atomic(temp_dir.path()).unwrap();

        // Assert
        let current_content = std::fs::read(temp_dir.path().join("CURRENT")).unwrap();
        assert_eq!(current_content, b"manifest.json");
    }

    #[test]
    fn should_preserve_column_family_metadata_across_persistence() {
        // Arrange
        let temp_dir = tempfile::tempdir().unwrap();
        let mut manifest = Manifest::default();

        let cf_config = ColumnFamilyConfig {
            memtable_max_bytes: 64 * 1024 * 1024,
            compaction_style: CompactionStyle::SizeTiered,
            compression: CompressionType::Zstd,
            ..Default::default()
        };

        manifest.add_cf(
            ColumnFamilyId::new(5),
            "test_cf".to_string(),
            Some(cf_config),
        );

        // Act
        manifest.save_atomic(temp_dir.path()).unwrap();
        let loaded =
            Manifest::load_with_retry(temp_dir.path(), 3, std::time::Duration::from_millis(10))
                .unwrap();

        // Assert
        let cf = loaded.get_cf(ColumnFamilyId::new(5)).unwrap();
        assert_eq!(cf.name, "test_cf");
        assert!(cf.config.is_some());
        assert_eq!(
            cf.config.as_ref().unwrap().memtable_max_bytes,
            64 * 1024 * 1024
        );
    }
}
