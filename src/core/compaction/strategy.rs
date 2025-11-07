use crate::error::MidgeResult;
use crate::manifest::FileMeta;

#[derive(Debug, Default)]
pub struct CompactionPlan {
    pub input_files: Vec<String>,
    pub output_files: Vec<String>,
    pub source_level: u32,
    pub target_level: u32,
}

/// Configuration for leveled compaction
#[derive(Debug, Clone)]
pub struct LeveledCompactionConfig {
    /// Maximum size for L0 before triggering compaction (bytes)
    pub l0_compaction_threshold: usize,
    /// Size multiplier between levels
    pub level_multiplier: usize,
    /// Target size for L1 (bytes)
    pub l1_target_size: usize,
    /// Maximum number of levels
    pub max_levels: usize,
}

impl Default for LeveledCompactionConfig {
    fn default() -> Self {
        Self {
            l0_compaction_threshold: 4 * 1024 * 1024, // 4MB
            level_multiplier: 10,
            l1_target_size: 10 * 1024 * 1024, // 10MB
            max_levels: 7,
        }
    }
}

#[derive(Debug, Default)]
pub struct Compactor {
    config: LeveledCompactionConfig,
}

impl Compactor {
    pub fn new() -> Self {
        Self::with_config(LeveledCompactionConfig::default())
    }

    pub fn with_config(config: LeveledCompactionConfig) -> Self {
        Self { config }
    }

    /// Pick a compaction based on leveled compaction strategy
    ///
    /// Strategy:
    /// 1. If L0 has too many files or exceeds size threshold, compact L0 -> L1
    /// 2. Otherwise, find the level that exceeds its target size and compact to next level
    ///
    /// # Arguments
    /// * `files` - All SST files in the manifest
    /// * `cf_id` - Column family ID to compact (files are filtered by this ID)
    /// * `cf_level_multiplier` - Level size multiplier from CF config
    /// * `cf_target_file_size` - Target file size from CF config
    pub fn pick_leveled_compaction(
        &self,
        files: &[FileMeta],
        cf_id: u32,
        cf_level_multiplier: usize,
        cf_target_file_size: usize,
    ) -> Option<CompactionPlan> {
        if files.is_empty() {
            return None;
        }

        // Filter files for this CF only
        let cf_files: Vec<&FileMeta> = files.iter().filter(|f| f.cf_id == cf_id).collect();
        if cf_files.is_empty() {
            return None;
        }

        // Group files by level
        let mut levels: Vec<Vec<&FileMeta>> = vec![Vec::new(); self.config.max_levels];
        for file in cf_files {
            if (file.level as usize) < self.config.max_levels {
                levels[file.level as usize].push(file);
            }
        }

        // Check L0 first (special case - can have overlapping files)
        let l0_size: u64 = levels[0].iter().map(|f| f.size_bytes).sum();
        let l0_file_count = levels[0].len();

        if l0_size > self.config.l0_compaction_threshold as u64 || l0_file_count >= 4 {
            // With L0 sublevels, we have two strategies:
            // 1. If file count is high (>=4), compact all sublevels for aggressive cleanup
            // 2. If just size threshold exceeded, compact oldest sublevel incrementally

            let compact_all_sublevels = l0_file_count >= 4;

            let input_files: Vec<String> = if compact_all_sublevels {
                // Compact all L0 files when file count is high
                levels[0].iter().map(|f| f.name.clone()).collect()
            } else {
                // Group L0 files by sublevel and pick oldest
                let mut l0_by_sublevel: std::collections::BTreeMap<u32, Vec<&FileMeta>> =
                    std::collections::BTreeMap::new();
                for file in &levels[0] {
                    l0_by_sublevel.entry(file.sublevel).or_default().push(file);
                }

                // Pick the oldest sublevel (lowest number) that has files
                let (_sublevel, sublevel_files) = match l0_by_sublevel.iter().next() {
                    Some((sl, files)) => (*sl, files.clone()),
                    None => return None,
                };

                sublevel_files.iter().map(|f| f.name.clone()).collect()
            };

            if input_files.is_empty() {
                return None;
            }

            // Find L1 files that overlap with the selected L0 files' key range
            let selected_files: Vec<&&FileMeta> = levels[0]
                .iter()
                .filter(|f| input_files.contains(&f.name))
                .collect();

            let l0_smallest = selected_files
                .iter()
                .filter_map(|f| f.smallest_key.as_ref())
                .min_by(|a, b| a.cmp(b));
            let l0_largest = selected_files
                .iter()
                .filter_map(|f| f.largest_key.as_ref())
                .max_by(|a, b| a.cmp(b));

            let mut l1_overlapping = Vec::new();
            if let (Some(smallest), Some(largest)) = (l0_smallest, l0_largest) {
                for file in &levels[1] {
                    if let (Some(file_smallest), Some(file_largest)) =
                        (&file.smallest_key, &file.largest_key)
                    {
                        // Check if ranges overlap
                        if file_smallest.as_slice() <= largest.as_slice()
                            && file_largest.as_slice() >= smallest.as_slice()
                        {
                            l1_overlapping.push(file.name.clone());
                        }
                    }
                }
            }

            let mut all_inputs = input_files;
            all_inputs.extend(l1_overlapping);

            return Some(CompactionPlan {
                input_files: all_inputs,
                output_files: vec![],
                source_level: 0,
                target_level: 1,
            });
        }

        // Check other levels (L1 onwards)
        for level in 1..self.config.max_levels - 1 {
            let level_size: u64 = levels[level].iter().map(|f| f.size_bytes).sum();
            let target_size = self.level_target_size_for_cf(
                level as u32,
                cf_level_multiplier,
                cf_target_file_size,
            );

            if level_size > target_size as u64 {
                // Pick the largest file from this level
                if let Some(largest_file) = levels[level].iter().max_by_key(|f| f.size_bytes) {
                    let input_files = vec![largest_file.name.clone()];

                    // Find overlapping files in next level
                    let mut next_level_overlapping = Vec::new();
                    if let (Some(smallest), Some(largest)) =
                        (&largest_file.smallest_key, &largest_file.largest_key)
                    {
                        for file in &levels[level + 1] {
                            if let (Some(file_smallest), Some(file_largest)) =
                                (&file.smallest_key, &file.largest_key)
                            {
                                if file_smallest.as_slice() <= largest.as_slice()
                                    && file_largest.as_slice() >= smallest.as_slice()
                                {
                                    next_level_overlapping.push(file.name.clone());
                                }
                            }
                        }
                    }

                    let mut all_inputs = input_files;
                    all_inputs.extend(next_level_overlapping);

                    return Some(CompactionPlan {
                        input_files: all_inputs,
                        output_files: vec![],
                        source_level: level as u32,
                        target_level: (level + 1) as u32,
                    });
                }
            }
        }

        None
    }

    /// Pick a manual compaction for a specific level
    ///
    /// Compacts all files at the specified level to the next level.
    /// Used for manual compaction triggered by user request.
    ///
    /// # Arguments
    /// * `files` - All SST files in the manifest
    /// * `cf_id` - Column family ID to compact
    /// * `target_level` - The level to compact (files from this level go to target_level + 1)
    /// * `cf_level_multiplier` - Level size multiplier from CF config
    /// * `cf_target_file_size` - Target file size from CF config
    pub fn pick_manual_compaction_level(
        &self,
        files: &[FileMeta],
        cf_id: u32,
        target_level: u32,
        _cf_level_multiplier: usize,
        _cf_target_file_size: usize,
    ) -> Option<CompactionPlan> {
        if files.is_empty() || target_level as usize >= self.config.max_levels {
            return None;
        }

        // Filter files for this CF and level
        let cf_files: Vec<&FileMeta> = files
            .iter()
            .filter(|f| f.cf_id == cf_id && f.level == target_level)
            .collect();

        if cf_files.is_empty() {
            return None;
        }

        let input_files: Vec<String> = cf_files.iter().map(|f| f.name.clone()).collect();

        Some(CompactionPlan {
            input_files,
            output_files: vec![],
            source_level: target_level,
            target_level: target_level + 1,
        })
    }

    /// Pick a manual compaction for a specific key range
    ///
    /// Compacts all files overlapping the specified key range from L0 to L1.
    /// This is similar to the automatic leveled compaction but triggered manually.
    ///
    /// # Arguments
    /// * `files` - All SST files in the manifest
    /// * `cf_id` - Column family ID to compact
    /// * `start_key` - Start of key range (inclusive), None means from beginning
    /// * `end_key` - End of key range (exclusive), None means to end
    /// * `_cf_level_multiplier` - Level size multiplier from CF config (unused for manual)
    /// * `_cf_target_file_size` - Target file size from CF config (unused for manual)
    pub fn pick_manual_compaction_range(
        &self,
        files: &[FileMeta],
        cf_id: u32,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        _cf_level_multiplier: usize,
        _cf_target_file_size: usize,
    ) -> Option<CompactionPlan> {
        if files.is_empty() {
            return None;
        }

        // For manual range compaction, compact L0 files overlapping the range to L1
        // This is safer than trying to compact all levels at once
        let overlapping_l0_files: Vec<&FileMeta> = files
            .iter()
            .filter(|f| {
                if f.cf_id != cf_id || f.level != 0 {
                    return false;
                }

                // Check if file overlaps with requested range
                match (&f.smallest_key, &f.largest_key) {
                    (Some(file_min), Some(file_max)) => {
                        // File range: [file_min, file_max]
                        // Requested range: [start_key, end_key)

                        // Check if ranges overlap
                        let overlaps = match (start_key, end_key) {
                            (Some(start), Some(end)) => {
                                // Both bounds specified
                                // Overlap if: file_min < end AND start <= file_max
                                file_min.as_slice() < end && start <= file_max.as_slice()
                            }
                            (Some(start), None) => {
                                // Only start bound: overlap if start <= file_max
                                start <= file_max.as_slice()
                            }
                            (None, Some(end)) => {
                                // Only end bound: overlap if file_min < end
                                file_min.as_slice() < end
                            }
                            (None, None) => {
                                // No bounds: include all files
                                true
                            }
                        };

                        overlaps
                    }
                    _ => false, // Skip files without key range metadata
                }
            })
            .collect();

        if overlapping_l0_files.is_empty() {
            return None;
        }

        let input_files: Vec<String> = overlapping_l0_files
            .iter()
            .map(|f| f.name.clone())
            .collect();

        Some(CompactionPlan {
            input_files,
            output_files: vec![],
            source_level: 0,
            target_level: 1,
        })
    }

    /// Calculate target size for a given level
    #[allow(dead_code)]
    fn level_target_size(&self, level: u32) -> usize {
        if level == 0 {
            self.config.l0_compaction_threshold
        } else {
            self.config.l1_target_size * self.config.level_multiplier.pow(level.saturating_sub(1))
        }
    }

    /// Calculate target size for a given level using CF-specific configuration
    fn level_target_size_for_cf(
        &self,
        level: u32,
        cf_level_multiplier: usize,
        cf_target_file_size: usize,
    ) -> usize {
        if level == 0 {
            self.config.l0_compaction_threshold
        } else {
            // L1 target is based on CF target file size
            // Higher levels scale by CF's level multiplier
            cf_target_file_size * cf_level_multiplier.pow(level.saturating_sub(1))
        }
    }

    pub fn plan_leveling(&self, _level: usize, _files: &[String]) -> CompactionPlan {
        // Placeholder: return an empty plan for now
        CompactionPlan::default()
    }

    /// Pick a compaction based on tombstone density.
    ///
    /// Selects SSTs with high tombstone density for compaction to reclaim space
    /// and improve read performance by removing dead data.
    ///
    /// # Arguments
    /// * `files` - All SST files in the manifest
    /// * `tombstone_threshold` - Minimum tombstone density percentage (0.0-100.0)
    ///   Example: 50.0 means SSTs with >=50% tombstones will be selected
    /// * `max_files` - Maximum number of files to compact at once
    ///
    /// # Returns
    /// CompactionPlan targeting high-density SSTs, or None if no files qualify
    pub fn pick_tombstone_compaction(
        &self,
        files: &[FileMeta],
        tombstone_threshold: f64,
        max_files: usize,
    ) -> Option<CompactionPlan> {
        // Find all high-density files
        let mut high_density: Vec<(&FileMeta, f64)> = files
            .iter()
            .map(|f| (f, f.tombstone_density()))
            .filter(|(_, density)| *density >= tombstone_threshold)
            .collect();

        if high_density.is_empty() {
            return None;
        }

        // Sort by density descending (worst first)
        high_density.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // Take top N files
        let selected: Vec<&FileMeta> = high_density
            .into_iter()
            .take(max_files)
            .map(|(f, _)| f)
            .collect();

        if selected.is_empty() {
            return None;
        }

        // Determine source and target levels
        // Strategy: Compact to the same level (in-place) or next level if mixed
        let source_level = selected[0].level;
        let all_same_level = selected.iter().all(|f| f.level == source_level);

        let target_level = if all_same_level && source_level < (self.config.max_levels as u32 - 1) {
            source_level + 1 // Compact to next level
        } else {
            source_level // In-place compaction (rewrite same level)
        };

        let input_files: Vec<String> = selected.iter().map(|f| f.name.clone()).collect();

        Some(CompactionPlan {
            input_files,
            output_files: Vec::new(), // Generated during execution
            source_level,
            target_level,
        })
    }

    pub fn execute(
        &self,
        _db_path: &std::path::Path,
        plan: CompactionPlan,
    ) -> MidgeResult<Vec<String>> {
        // Placeholder: no-op execution; return outputs as given
        Ok(plan.output_files)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_file(name: &str, level: u32, size: u64, smallest: &[u8], largest: &[u8]) -> FileMeta {
        FileMeta {
            name: name.to_string(),
            level,
            size_bytes: size,
            cf_id: 0, // Default CF for tests
            smallest_key: Some(smallest.to_vec()),
            largest_key: Some(largest.to_vec()),
            smallest_seq: Some(0),
            largest_seq: Some(100),
            sublevel: 0,
            cloud_location: None,
            cloud_checksum: None,
            cloud_uploaded_at: None,
            cloud_state: None,
            point_tombstone_count: 0,
            range_tombstone_count: 0,
            total_entries: 0,
        }
    }

    #[test]
    fn should_pick_l0_compaction_when_size_exceeds_threshold() {
        // Arrange
        let config = LeveledCompactionConfig {
            l0_compaction_threshold: 1000,
            ..Default::default()
        };
        let compactor = Compactor::with_config(config);

        let files = vec![
            make_file("l0_file1.sst", 0, 600, b"a", b"f"),
            make_file("l0_file2.sst", 0, 500, b"d", b"j"),
        ];

        // Act
        let plan = compactor.pick_leveled_compaction(&files, 0, 10, 64 * 1024 * 1024);

        // Assert
        assert!(plan.is_some());

        let plan = plan.unwrap();
        assert_eq!(plan.source_level, 0);
        assert_eq!(plan.target_level, 1);
        assert_eq!(plan.input_files.len(), 2);
    }

    #[test]
    fn should_pick_l0_compaction_when_file_count_exceeds_threshold() {
        // Arrange
        let compactor = Compactor::new();

        let files = vec![
            make_file("l0_file1.sst", 0, 100, b"a", b"b"),
            make_file("l0_file2.sst", 0, 100, b"c", b"d"),
            make_file("l0_file3.sst", 0, 100, b"e", b"f"),
            make_file("l0_file4.sst", 0, 100, b"g", b"h"),
        ];

        // Act
        let plan = compactor.pick_leveled_compaction(&files, 0, 10, 64 * 1024 * 1024);

        // Assert
        assert!(plan.is_some());

        let plan = plan.unwrap();
        assert_eq!(plan.source_level, 0);
        assert_eq!(plan.target_level, 1);
    }

    #[test]
    fn should_include_overlapping_l1_files_when_picking_l0_compaction() {
        // Arrange
        let compactor = Compactor::new();

        let files = vec![
            make_file("l0_file1.sst", 0, 5_000_000, b"d", b"h"),
            make_file("l1_file1.sst", 1, 1_000_000, b"a", b"e"), // Overlaps with L0
            make_file("l1_file2.sst", 1, 1_000_000, b"f", b"j"), // Overlaps with L0
            make_file("l1_file3.sst", 1, 1_000_000, b"k", b"z"), // No overlap
        ];

        // Act
        let plan = compactor
            .pick_leveled_compaction(&files, 0, 10, 64 * 1024 * 1024)
            .unwrap();

        // Assert
        assert_eq!(plan.source_level, 0);
        assert_eq!(plan.target_level, 1);
        assert!(plan.input_files.contains(&"l0_file1.sst".to_string()));
        assert!(plan.input_files.contains(&"l1_file1.sst".to_string()));
        assert!(plan.input_files.contains(&"l1_file2.sst".to_string()));
        assert!(!plan.input_files.contains(&"l1_file3.sst".to_string())); // Should not include non-overlapping
    }

    #[test]
    fn should_return_none_when_no_compaction_needed() {
        // Arrange
        let compactor = Compactor::new();

        let files = vec![
            make_file("l0_file1.sst", 0, 100, b"a", b"b"),
            make_file("l1_file1.sst", 1, 1000, b"c", b"d"),
        ];

        // Act
        let plan = compactor.pick_leveled_compaction(&files, 0, 10, 64 * 1024 * 1024);

        // Assert
        assert!(plan.is_none());
    }

    #[test]
    fn should_compute_level_target_size_with_multiplier() {
        // Arrange
        let config = LeveledCompactionConfig {
            l1_target_size: 10_000_000, // 10MB
            level_multiplier: 10,
            ..Default::default()
        };
        let compactor = Compactor::with_config(config);

        // Act
        let size_l1 = compactor.level_target_size(1);
        let size_l2 = compactor.level_target_size(2);
        let size_l3 = compactor.level_target_size(3);

        // Assert
        assert_eq!(size_l1, 10_000_000);
        assert_eq!(size_l2, 100_000_000); // 10MB * 10
        assert_eq!(size_l3, 1_000_000_000); // 10MB * 10^2
    }

    #[test]
    fn should_pick_ln_compaction_when_level_size_exceeds_target() {
        // Arrange
        let config = LeveledCompactionConfig {
            l1_target_size: 1000,
            level_multiplier: 10,
            ..Default::default()
        };
        let compactor = Compactor::with_config(config);

        let files = vec![
            make_file("l1_file1.sst", 1, 700, b"a", b"f"),
            make_file("l1_file2.sst", 1, 800, b"g", b"m"), // Largest in L1
            make_file("l2_file1.sst", 2, 500, b"h", b"k"), // Overlaps with l1_file2
        ];

        // Act
        let plan = compactor
            .pick_leveled_compaction(&files, 0, 10, 1000)
            .unwrap();

        // Assert
        assert_eq!(plan.source_level, 1);
        assert_eq!(plan.target_level, 2);
        assert!(plan.input_files.contains(&"l1_file2.sst".to_string())); // Largest L1 file
        assert!(plan.input_files.contains(&"l2_file1.sst".to_string())); // Overlapping L2 file
    }

    // === Durability Tests ===

    #[test]
    fn should_include_source_files_in_compaction_plan() {
        // Arrange
        let config = LeveledCompactionConfig {
            l0_compaction_threshold: 1000,
            ..Default::default()
        };
        let compactor = Compactor::with_config(config);
        let files = vec![
            make_file("source1.sst", 0, 600, b"a", b"f"),
            make_file("source2.sst", 0, 500, b"d", b"j"),
        ];

        // Act
        let plan = compactor.pick_leveled_compaction(&files, 0, 10, 64 * 1024 * 1024);

        // Assert
        assert!(plan.is_some(), "Should create compaction plan");
        let plan = plan.unwrap();
        assert_eq!(plan.input_files.len(), 2);
        assert!(plan.input_files.contains(&"source1.sst".to_string()));
        assert!(plan.input_files.contains(&"source2.sst".to_string()));
    }

    #[test]
    fn should_specify_target_level_in_plan() {
        // Arrange
        let config = LeveledCompactionConfig {
            l0_compaction_threshold: 1000,
            ..Default::default()
        };
        let compactor = Compactor::with_config(config);
        let files = vec![make_file("l0_file.sst", 0, 5000, b"a", b"z")];

        // Act
        let plan = compactor.pick_leveled_compaction(&files, 0, 10, 64 * 1024 * 1024);

        // Assert
        assert!(plan.is_some());
        let plan = plan.unwrap();
        assert_eq!(plan.source_level, 0);
        assert_eq!(plan.target_level, 1);
    }

    #[test]
    fn should_handle_empty_file_list_gracefully() {
        // Arrange
        let compactor = Compactor::new();
        let files: Vec<FileMeta> = vec![];

        // Act
        let plan = compactor.pick_leveled_compaction(&files, 0, 10, 64 * 1024 * 1024);

        // Assert
        assert!(
            plan.is_none(),
            "No compaction should be selected for empty files"
        );
    }

    #[test]
    fn should_track_source_and_target_levels() {
        // Arrange
        let config = LeveledCompactionConfig {
            l0_compaction_threshold: 100,
            ..Default::default()
        };
        let compactor = Compactor::with_config(config);
        let files = vec![make_file("l0_file.sst", 0, 200, b"a", b"f")];

        // Act
        let plan = compactor.pick_leveled_compaction(&files, 0, 10, 64 * 1024 * 1024);

        // Assert
        assert!(plan.is_some());
        let plan = plan.unwrap();
        assert_eq!(plan.source_level, 0, "Should compact from L0");
        assert_eq!(plan.target_level, 1, "Should compact to L1");
    }

    #[test]
    fn should_preserve_plan_metadata_for_rollback() {
        // Arrange
        let config = LeveledCompactionConfig {
            l0_compaction_threshold: 100,
            ..Default::default()
        };
        let compactor = Compactor::with_config(config);
        let files = vec![
            make_file("input1.sst", 0, 600, b"a", b"c"),
            make_file("input2.sst", 0, 500, b"d", b"f"),
            make_file("input3.sst", 0, 400, b"g", b"j"),
        ];

        // Act
        let plan = compactor.pick_leveled_compaction(&files, 0, 10, 64 * 1024 * 1024);

        // Assert
        assert!(plan.is_some());
        let plan = plan.unwrap();

        // Plan should preserve all input files for rollback if needed
        assert_eq!(plan.input_files.len(), 3);
        assert!(plan.input_files.contains(&"input1.sst".to_string()));
        assert!(plan.input_files.contains(&"input2.sst".to_string()));
        assert!(plan.input_files.contains(&"input3.sst".to_string()));

        // Should know source/target for proper cleanup
        assert_eq!(plan.source_level, 0);
        assert_eq!(plan.target_level, 1);
    }
}
