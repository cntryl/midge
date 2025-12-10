//! Compaction strategy and planning
//!
//! Implements a classic *leveled compaction* picker for an LSM tree.
//!
//! Goals:
//!   - Compact L0 aggressively (high write amplification, unbounded ingest).
//!   - Maintain size ratio between levels (L1 ≈ threshold; Ln ≈ threshold * multiplier^(n-1)).
//!   - Keep read amplification low by honoring key-range overlap.
//!   - Produce deterministic compaction plans.

use crate::metadata::FileMeta;

/// A compaction plan describing which SSTs to read and which level to promote into.
#[derive(Debug, Clone)]
pub struct CompactionPlan {
    pub input_files: Vec<String>,
    pub output_files: Vec<String>,
    pub source_level: u32,
    pub target_level: u32,
    pub cf_id: u32,
    /// Output SST sequence number (assigned by sequence allocator)
    pub output_seq: u64,
}

impl CompactionPlan {
    pub fn new(cf_id: u32, source_level: u32, target_level: u32) -> Self {
        Self {
            input_files: Vec::new(),
            output_files: Vec::new(),
            source_level,
            target_level,
            cf_id,
            output_seq: 0,
        }
    }

    pub fn with_output_seq(mut self, output_seq: u64) -> Self {
        self.output_seq = output_seq;
        self
    }
}

/// Configuration for leveled compaction.
///
/// Level size rules:
///   L0: special-case, file-count or size-based threshold.
///   L1: explicitly configured target.
///   Ln (n >= 2): L1_target * level_multiplier^(n - 1)
#[derive(Debug, Clone)]
pub struct LeveledCompactionConfig {
    pub l0_compaction_threshold: u64,
    pub l0_file_count_threshold: usize,
    pub level_multiplier: u64,
    pub l1_target_size: u64,
    pub max_levels: usize,
}

impl Default for LeveledCompactionConfig {
    fn default() -> Self {
        Self {
            l0_compaction_threshold: 4 * 1024 * 1024, // 4MB
            l0_file_count_threshold: 4,
            level_multiplier: 10,
            l1_target_size: 40 * 1024 * 1024, // 40MB (4MB*10)
            max_levels: 7,
        }
    }
}

/// Compaction planner implementing leveled compaction.
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

    /// Pick compaction using leveled strategy:
    ///   1. Check L0 → L1 first.
    ///   2. Check L1+ levels for size-overflow.
    ///
    /// NOTE: This picker is deterministic for a given metadata snapshot.
    pub fn pick_compaction(&self, files: &[FileMeta], cf_id: u32) -> Option<CompactionPlan> {
        // Only look at this CF's files
        let cf_files: Vec<&FileMeta> = files.iter().filter(|f| f.cf_id == cf_id).collect();
        if cf_files.is_empty() {
            return None;
        }

        // Group files by level
        let mut levels: Vec<Vec<&FileMeta>> = vec![Vec::new(); self.config.max_levels];
        for file in cf_files {
            let lv = file.level as usize;
            if lv < self.config.max_levels {
                levels[lv].push(file);
            }
        }

        // ---------------------------
        // 1. L0 → L1 (special case)
        // ---------------------------
        let l0_size: u64 = levels[0].iter().map(|f| f.size_bytes).sum();
        let l0_count = levels[0].len();

        if l0_size > self.config.l0_compaction_threshold
            || l0_count >= self.config.l0_file_count_threshold
        {
            return self.plan_zero_level(&levels, cf_id);
        }

        // ---------------------------
        // 2. L1..Ln leveled compaction
        // ---------------------------
        for level in 1..self.config.max_levels - 1 {
            let level_size: u64 = levels[level].iter().map(|f| f.size_bytes).sum();
            let target_size = self.level_target_size(level as u32);

            if level_size > target_size {
                return self.plan_inner_level(&levels, cf_id, level);
            }
        }

        None
    }

    /// Classic leveled size rule:
    ///   level=0: threshold tuned for L0
    ///   level=1: explicitly configured
    ///   level>=2: l1_target_size * (multiplier^(level - 1))
    fn level_target_size(&self, level: u32) -> u64 {
        match level {
            0 => self.config.l0_compaction_threshold,
            1 => self.config.l1_target_size,
            _ => {
                let exp = (level - 1) as u32;
                self.config
                    .l1_target_size
                    .saturating_mul(self.config.level_multiplier.saturating_pow(exp))
            }
        }
    }

    /// Build a compaction plan for L0 → L1.
    fn plan_zero_level(
        &self,
        levels: &[Vec<&FileMeta>],
        cf_id: u32,
    ) -> Option<CompactionPlan> {
        if levels[0].is_empty() {
            return None;
        }

        let mut input_files: Vec<String> = levels[0].iter().map(|f| f.name.clone()).collect();

        // Overlap detection: find L1 files whose ranges overlap L0's range.
        let (min_key, max_key) = smallest_and_largest(levels[0].as_slice())?;
        let mut l1_overlapping = overlap_with_range(&levels[1], &min_key, &max_key);

        input_files.append(&mut l1_overlapping);
        dedupe_sort(&mut input_files);

        Some(CompactionPlan {
            input_files,
            output_files: Vec::new(),
            source_level: 0,
            target_level: 1,
            cf_id,
            output_seq: 0,
        })
    }

    /// Build a compaction plan for level N → N+1.
    fn plan_inner_level(
        &self,
        levels: &[Vec<&FileMeta>],
        cf_id: u32,
        level: usize,
    ) -> Option<CompactionPlan> {
        if levels[level].is_empty() {
            return None;
        }

        let mut input_files: Vec<String> =
            levels[level].iter().map(|f| f.name.clone()).collect();

        // Find overlapping files in next level
        let (min_key, max_key) = smallest_and_largest(levels[level].as_slice())?;
        let mut overlapping = overlap_with_range(&levels[level + 1], &min_key, &max_key);

        input_files.append(&mut overlapping);
        dedupe_sort(&mut input_files);

        Some(CompactionPlan {
            input_files,
            output_files: Vec::new(),
            source_level: level as u32,
            target_level: (level + 1) as u32,
            cf_id,
            output_seq: 0,
        })
    }
}

/// Extract smallest and largest user keys across a slice of FileMeta.
fn smallest_and_largest(files: &[&FileMeta]) -> Option<(Vec<u8>, Vec<u8>)> {
    let smallest = files
        .iter()
        .filter_map(|f| f.smallest_key.clone())
        .min();
    let largest = files
        .iter()
        .filter_map(|f| f.largest_key.clone())
        .max();

    match (smallest, largest) {
        (Some(s), Some(l)) => Some((s, l)),
        _ => None,
    }
}

/// Return names of files whose key-ranges overlap [min_key, max_key].
fn overlap_with_range(files: &[&FileMeta], min_key: &[u8], max_key: &[u8]) -> Vec<String> {
    files
        .iter()
        .filter_map(|f| {
            let fs = f.smallest_key.as_ref()?;
            let fl = f.largest_key.as_ref()?;
            if fs.as_slice() <= max_key && fl.as_slice() >= min_key {
                Some(f.name.clone())
            } else {
                None
            }
        })
        .collect()
}

/// Deduplicate + sort file list for deterministic plan output.
fn dedupe_sort(v: &mut Vec<String>) {
    v.sort();
    v.dedup();
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
        let compactor = Compactor::new();
        assert_eq!(compactor.config.max_levels, 7);
        assert_eq!(compactor.config.l0_compaction_threshold, 4 * 1024 * 1024);
        assert_eq!(compactor.config.level_multiplier, 10);
    }

    #[test]
    fn should_calculate_level_target_sizes_when_multiplying_by_level_multiplier() {
        let compactor = Compactor::new();

        let l0_target = compactor.level_target_size(0); // 4MB
        let l1_target = compactor.level_target_size(1); // 40MB
        let l2_target = compactor.level_target_size(2); // 40MB * 10 = 400MB

        assert_eq!(l1_target, compactor.config.l1_target_size);
        assert_eq!(l1_target, l0_target * 10);
        assert_eq!(l2_target, l1_target * 10);
    }

    #[test]
    fn should_return_none_when_no_files_exist() {
        let compactor = Compactor::new();
        let empty_files = [];
        assert!(compactor.pick_compaction(&empty_files, 0).is_none());
    }
}
