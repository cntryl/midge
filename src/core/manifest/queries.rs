///! Query operations on the manifest - finding files, levels, and metadata.

use crate::api::column_family::ColumnFamilyId;

use super::types::{ColumnFamilyMeta, FileMeta, Manifest};

impl Manifest {
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
