//! Compaction strategy and planning
//!
//! Implements leveled compaction for LSM-tree maintenance.
//! Strategy:
//! 1. If L0 exceeds size threshold or file count, compact L0 → L1
//! 2. Otherwise, find level exceeding target size and compact to next level

use crate::metadata::FileMeta;

/// Compaction plan describing input files and target level
#[derive(Debug, Clone)]
pub struct CompactionPlan {
    pub input_files: Vec<String>,
    pub output_files: Vec<String>,
    pub source_level: u32,
    pub target_level: u32,
    pub cf_id: u32,
}

impl CompactionPlan {
    pub fn new(cf_id: u32, source_level: u32, target_level: u32) -> Self {
        Self {
            input_files: Vec::new(),
            output_files: Vec::new(),
            source_level,
            target_level,
            cf_id,
        }
    }
}

/// Configuration for leveled compaction
#[derive(Debug, Clone)]
pub struct LeveledCompactionConfig {
    /// L0 compaction threshold in bytes
    pub l0_compaction_threshold: u64,
    /// L0 file count threshold
    pub l0_file_count_threshold: usize,
    /// Size multiplier between levels
    pub level_multiplier: u64,
    /// Target size for L1 in bytes
    pub l1_target_size: u64,
    /// Maximum number of levels
    pub max_levels: usize,
}

impl Default for LeveledCompactionConfig {
    fn default() -> Self {
        Self {
            l0_compaction_threshold: 4 * 1024 * 1024, // 4MB
            l0_file_count_threshold: 4,
            level_multiplier: 10,
            l1_target_size: 40 * 1024 * 1024, // 40MB (4MB * 10)
            max_levels: 7,
        }
    }
}

/// Compaction planner using leveled strategy
pub struct Compactor {
    pub config: LeveledCompactionConfig,
}

impl Compactor {
    pub fn new() -> Self {
        Self::with_config(LeveledCompactionConfig::default())
    }

    pub fn with_config(config: LeveledCompactionConfig) -> Self {
        Self { config }
    }

    /// Pick a compaction based on leveled compaction strategy
    pub fn pick_compaction(&self, files: &[FileMeta], cf_id: u32) -> Option<CompactionPlan> {
        if files.is_empty() {
            return None;
        }

        // Filter files for this CF
        let cf_files: Vec<&FileMeta> = files.iter().filter(|f| f.cf_id == cf_id).collect();

        if cf_files.is_empty() {
            return None;
        }

        // Group files by level
        let mut levels: Vec<Vec<&FileMeta>> = vec![Vec::new(); self.config.max_levels];
        for file in cf_files {
            let level = file.level as usize;
            if level < self.config.max_levels {
                levels[level].push(file);
            }
        }

        // Check L0 first
        let l0_size: u64 = levels[0].iter().map(|f| f.size_bytes).sum();
        let l0_file_count = levels[0].len();

        if l0_size > self.config.l0_compaction_threshold
            || l0_file_count >= self.config.l0_file_count_threshold
        {
            // Compact all L0 files to L1
            let input_files: Vec<String> = levels[0].iter().map(|f| f.name.clone()).collect();

            if input_files.is_empty() {
                return None;
            }

            // Find overlapping L1 files
            let l0_smallest = levels[0]
                .iter()
                .filter_map(|f| f.smallest_key.as_ref())
                .min_by(|a, b| a.cmp(b));
            let l0_largest = levels[0]
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
                output_files: Vec::new(),
                source_level: 0,
                target_level: 1,
                cf_id,
            });
        }

        // Check other levels (L1 onwards)
        for level in 1..self.config.max_levels - 1 {
            let level_size: u64 = levels[level].iter().map(|f| f.size_bytes).sum();
            let target_size = self.level_target_size(level as u32);

            if level_size > target_size {
                // Compact this level to next level
                let input_files: Vec<String> =
                    levels[level].iter().map(|f| f.name.clone()).collect();

                // Find overlapping files in next level
                let level_smallest = levels[level]
                    .iter()
                    .filter_map(|f| f.smallest_key.as_ref())
                    .min_by(|a, b| a.cmp(b));
                let level_largest = levels[level]
                    .iter()
                    .filter_map(|f| f.largest_key.as_ref())
                    .max_by(|a, b| a.cmp(b));

                let mut next_overlapping = Vec::new();
                if let (Some(smallest), Some(largest)) = (level_smallest, level_largest) {
                    let next_level = level + 1;
                    for file in &levels[next_level] {
                        if let (Some(file_smallest), Some(file_largest)) =
                            (&file.smallest_key, &file.largest_key)
                        {
                            if file_smallest.as_slice() <= largest.as_slice()
                                && file_largest.as_slice() >= smallest.as_slice()
                            {
                                next_overlapping.push(file.name.clone());
                            }
                        }
                    }
                }

                let mut all_inputs = input_files;
                all_inputs.extend(next_overlapping);

                return Some(CompactionPlan {
                    input_files: all_inputs,
                    output_files: Vec::new(),
                    source_level: level as u32,
                    target_level: (level + 1) as u32,
                    cf_id,
                });
            }
        }

        None
    }

    /// Calculate target size for a given level
    fn level_target_size(&self, level: u32) -> u64 {
        if level == 0 {
            return self.config.l0_compaction_threshold;
        }
        if level == 1 {
            return self.config.l1_target_size;
        }
        self.config.l1_target_size * self.config.level_multiplier.pow((level - 1) as u32)
    }
}

impl Default for Compactor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_compactor_with_default_config_when_new() {
        // Arrange
        // Act
        let compactor = Compactor::new();

        // Assert
        assert_eq!(compactor.config.max_levels, 7);
        assert_eq!(compactor.config.l0_compaction_threshold, 4 * 1024 * 1024);
        assert_eq!(compactor.config.level_multiplier, 10);
    }

    #[test]
    fn should_calculate_level_target_sizes_when_multiplying_by_level_multiplier() {
        // Arrange
        let compactor = Compactor::new();

        // Act
        let l0_target = compactor.level_target_size(0);
        let l1_target = compactor.level_target_size(1);
        let l2_target = compactor.level_target_size(2);

        // Assert
        assert_eq!(l1_target, l0_target * 10); // L1 is 10x L0
        assert_eq!(l2_target, l1_target * 10); // L2 is 10x L1
    }

    #[test]
    fn should_return_none_when_no_files_exist() {
        // Arrange
        let compactor = Compactor::new();
        let empty_files = [];

        // Act
        let plan = compactor.pick_compaction(&empty_files, 0);

        // Assert
        assert!(plan.is_none());
    }
}
