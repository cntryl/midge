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
    /// Oldest active snapshot sequence, if any, used for tombstone retention.
    pub snapshot_horizon: Option<u64>,
}

impl CompactionPlan {
    /// Create a new compaction plan for the given column family and level range.
    pub fn new(cf_id: u32, source_level: u32, target_level: u32) -> Self {
        Self {
            input_files: Vec::new(),
            output_files: Vec::new(),
            source_level,
            target_level,
            cf_id,
            output_seq: 0,
            snapshot_horizon: None,
        }
    }

    pub fn with_output_seq(mut self, output_seq: u64) -> Self {
        self.output_seq = output_seq;
        self
    }

    pub fn with_snapshot_horizon(mut self, snapshot_horizon: Option<u64>) -> Self {
        self.snapshot_horizon = snapshot_horizon;
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

    /// Check if compaction should be triggered based on read amplification.
    ///
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
                let exp = level - 1;
                self.config
                    .l1_target_size
                    .saturating_mul(self.config.level_multiplier.saturating_pow(exp))
            }
        }
    }

    /// Build a compaction plan for L0 → L1.
    ///
    /// **Read-aware compaction**: Prioritizes files by read heat to reduce
    /// read amplification. Hot files (frequently accessed) are compacted first.
    fn plan_zero_level(&self, levels: &[Vec<&FileMeta>], cf_id: u32) -> Option<CompactionPlan> {
        if levels[0].is_empty() {
            return None;
        }

        // Sort L0 files by read heat (hottest first)
        let mut l0_sorted = levels[0].clone();
        l0_sorted.sort_by_key(|f| std::cmp::Reverse(f.get_read_count()));

        // Pick top files to compact (limit batch size for incremental compaction)
        let batch_size = std::cmp::min(l0_sorted.len(), 4); // Max 4 files per batch
        let l0_batch: Vec<&FileMeta> = l0_sorted.into_iter().take(batch_size).collect();

        let mut input_files: Vec<String> = l0_batch.iter().map(|f| f.name.clone()).collect();

        // Overlap detection: find L1 files whose ranges overlap the selected L0 batch
        let (min_key, max_key) = smallest_and_largest(l0_batch.as_slice())?;
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
            snapshot_horizon: None,
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

        let mut input_files: Vec<String> = levels[level].iter().map(|f| f.name.clone()).collect();

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
            snapshot_horizon: None,
        })
    }
}

/// Extract smallest and largest user keys across a slice of FileMeta.
fn smallest_and_largest(files: &[&FileMeta]) -> Option<(Vec<u8>, Vec<u8>)> {
    let smallest = files.iter().filter_map(|f| f.smallest_key.clone()).min();
    let largest = files.iter().filter_map(|f| f.largest_key.clone()).max();

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

    fn make_file(
        name: &str,
        cf_id: u32,
        level: u32,
        size_bytes: u64,
        smallest_key: Option<Vec<u8>>,
        largest_key: Option<Vec<u8>>,
    ) -> FileMeta {
        FileMeta {
            name: name.to_string(),
            cf_id,
            level,
            size_bytes,
            content_crc32c: None,
            sst_seq: 0,
            smallest_key,
            largest_key,
            smallest_seq: None,
            largest_seq: None,
            sublevel: 0,
            read_count: Default::default(),
        }
    }

    // ============================================================================
    // Tests for Compactor initialization invariants
    // ============================================================================

    #[test]
    fn should_create_compactor_with_default_config_when_new() {
        // Arrange
        // (no setup required)

        // Act
        let compactor = Compactor::new();

        // Assert
        assert_eq!(compactor.config.max_levels, 7);
        assert_eq!(compactor.config.l0_compaction_threshold, 4 * 1024 * 1024);
        assert_eq!(compactor.config.level_multiplier, 10);
    }

    #[test]
    fn should_create_compactor_with_default_when_using_default_trait() {
        // Arrange
        // (no setup required)

        // Act
        let compactor = Compactor::default();

        // Assert
        assert_eq!(compactor.config.max_levels, 7);
        assert_eq!(compactor.config.l0_file_count_threshold, 4);
    }

    #[test]
    fn should_create_compactor_with_custom_config() {
        // Arrange
        let config = LeveledCompactionConfig {
            l0_compaction_threshold: 8 * 1024 * 1024,
            l0_file_count_threshold: 8,
            level_multiplier: 5,
            l1_target_size: 80 * 1024 * 1024,
            max_levels: 10,
        };

        // Act
        let compactor = Compactor::with_config(config.clone());

        // Assert
        assert_eq!(compactor.config.l0_compaction_threshold, 8 * 1024 * 1024);
        assert_eq!(compactor.config.level_multiplier, 5);
        assert_eq!(compactor.config.max_levels, 10);
    }

    // ============================================================================
    // Tests for level target size calculation invariant
    // ============================================================================

    #[test]
    fn should_calculate_level_target_sizes_when_multiplying_by_level_multiplier() {
        // Arrange
        let compactor = Compactor::new();

        // Act
        let l0_target = compactor.level_target_size(0); // 4MB
        let l1_target = compactor.level_target_size(1); // 40MB
        let l2_target = compactor.level_target_size(2); // 40MB * 10 = 400MB

        // Assert: exponential growth by multiplier
        assert_eq!(l1_target, compactor.config.l1_target_size);
        assert_eq!(l1_target, l0_target * 10);
        assert_eq!(l2_target, l1_target * 10);
    }

    #[test]
    fn should_use_l0_threshold_for_level_zero() {
        // Arrange
        let compactor = Compactor::new();

        // Act
        let l0_target = compactor.level_target_size(0);

        // Assert
        assert_eq!(l0_target, compactor.config.l0_compaction_threshold);
    }

    #[test]
    fn should_use_l1_target_for_level_one() {
        // Arrange
        let compactor = Compactor::new();

        // Act
        let l1_target = compactor.level_target_size(1);

        // Assert
        assert_eq!(l1_target, compactor.config.l1_target_size);
    }

    #[test]
    fn should_calculate_exponential_level_targets_for_higher_levels() {
        // Arrange
        let compactor = Compactor::new();
        let l1_target = compactor.config.l1_target_size;
        let multiplier = compactor.config.level_multiplier;

        // Act: calculate higher level targets

        // Assert: verify exponential growth
        assert_eq!(compactor.level_target_size(2), l1_target * multiplier);
        assert_eq!(
            compactor.level_target_size(3),
            l1_target * multiplier * multiplier
        );
        assert_eq!(
            compactor.level_target_size(4),
            l1_target * multiplier * multiplier * multiplier
        );
    }

    #[test]
    fn should_handle_saturation_with_large_exponents() {
        // Arrange
        let compactor = Compactor::new();

        // Act: calculate target for very high level
        let high_level_target = compactor.level_target_size(10);

        // Assert: should not panic, should saturate or calculate correctly
        assert!(high_level_target > 0);
    }

    // ============================================================================
    // Tests for LeveledCompactionConfig invariants
    // ============================================================================

    #[test]
    fn should_create_default_config_with_sensible_values() {
        // Arrange
        // (no setup required)

        // Act
        let config = LeveledCompactionConfig::default();

        // Assert
        assert_eq!(config.l0_compaction_threshold, 4 * 1024 * 1024);
        assert_eq!(config.l0_file_count_threshold, 4);
        assert_eq!(config.level_multiplier, 10);
        assert_eq!(config.l1_target_size, 40 * 1024 * 1024);
        assert_eq!(config.max_levels, 7);
    }

    #[test]
    fn should_have_l1_target_as_multiple_of_l0_threshold() {
        // Arrange
        // (no setup required)

        // Act
        let config = LeveledCompactionConfig::default();

        // Assert: L1 = L0 * multiplier
        assert_eq!(
            config.l1_target_size,
            config.l0_compaction_threshold * config.level_multiplier
        );
    }

    // ============================================================================
    // Tests for compaction picking invariants
    // ============================================================================

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

    #[test]
    fn should_return_none_when_no_files_for_cf() {
        // Arrange
        let compactor = Compactor::new();
        let files = vec![
            make_file(
                "file1.sst",
                0,
                0,
                1000,
                Some(b"a".to_vec()),
                Some(b"b".to_vec()),
            ),
            make_file(
                "file2.sst",
                0,
                0,
                1000,
                Some(b"c".to_vec()),
                Some(b"d".to_vec()),
            ),
        ];

        // Act: request compaction for different CF
        let plan = compactor.pick_compaction(&files, 1);

        // Assert
        assert!(plan.is_none());
    }

    #[test]
    fn should_trigger_l0_compaction_when_size_exceeds_threshold() {
        // Arrange
        let compactor = Compactor::new();
        let threshold = compactor.config.l0_compaction_threshold;

        let files = vec![
            make_file(
                "file1.sst",
                0,
                0,
                threshold / 2 + 1,
                Some(b"a".to_vec()),
                Some(b"m".to_vec()),
            ),
            make_file(
                "file2.sst",
                0,
                0,
                threshold / 2 + 1,
                Some(b"n".to_vec()),
                Some(b"z".to_vec()),
            ),
        ];

        // Act
        let plan = compactor.pick_compaction(&files, 0);

        // Assert: should trigger L0 → L1 compaction
        assert!(plan.is_some());
        let plan = plan.unwrap();
        assert_eq!(plan.source_level, 0);
        assert_eq!(plan.target_level, 1);
    }

    #[test]
    fn should_trigger_l0_compaction_when_file_count_exceeds_threshold() {
        // Arrange
        let compactor = Compactor::new();
        let count_threshold = compactor.config.l0_file_count_threshold;

        let mut files = Vec::new();
        for i in 0..count_threshold + 1 {
            files.push(make_file(
                &format!("file{}.sst", i),
                0,
                0,
                1000,
                Some(vec![i as u8]),
                Some(vec![i as u8 + 1]),
            ));
        }

        // Act
        let plan = compactor.pick_compaction(&files, 0);

        // Assert: should trigger due to file count
        assert!(plan.is_some());
        assert_eq!(plan.unwrap().source_level, 0);
    }

    #[test]
    fn should_trigger_inner_level_compaction_when_size_exceeds() {
        // Arrange
        let compactor = Compactor::new();
        let l1_target = compactor.config.l1_target_size;

        let files = vec![
            // L1 files exceeding target size
            make_file(
                "file1.sst",
                0,
                1,
                l1_target / 2 + 1,
                Some(b"a".to_vec()),
                Some(b"m".to_vec()),
            ),
            make_file(
                "file2.sst",
                0,
                1,
                l1_target / 2 + 1,
                Some(b"n".to_vec()),
                Some(b"z".to_vec()),
            ),
        ];

        // Act
        let plan = compactor.pick_compaction(&files, 0);

        // Assert: should trigger L1 → L2 compaction
        assert!(plan.is_some());
        let plan = plan.unwrap();
        assert_eq!(plan.source_level, 1);
        assert_eq!(plan.target_level, 2);
    }

    #[test]
    fn should_prioritize_l0_compaction_over_inner_levels() {
        // Arrange
        let compactor = Compactor::new();
        let l0_threshold = compactor.config.l0_compaction_threshold;
        let l1_target = compactor.config.l1_target_size;

        let files = vec![
            // L0 exceeding threshold
            make_file(
                "l0_file1.sst",
                0,
                0,
                l0_threshold / 2 + 1,
                Some(b"a".to_vec()),
                Some(b"m".to_vec()),
            ),
            make_file(
                "l0_file2.sst",
                0,
                0,
                l0_threshold / 2 + 1,
                Some(b"n".to_vec()),
                Some(b"z".to_vec()),
            ),
            // L1 also exceeding target
            make_file(
                "l1_file1.sst",
                0,
                1,
                l1_target / 2 + 1,
                Some(b"a".to_vec()),
                Some(b"z".to_vec()),
            ),
        ];

        // Act
        let plan = compactor.pick_compaction(&files, 0);

        // Assert: L0 compaction should be picked first
        assert!(plan.is_some());
        assert_eq!(plan.unwrap().source_level, 0);
    }

    #[test]
    fn should_not_compact_when_all_levels_within_threshold() {
        // Arrange
        let compactor = Compactor::new();

        let files = vec![
            make_file(
                "file1.sst",
                0,
                1,
                1000,
                Some(b"a".to_vec()),
                Some(b"z".to_vec()),
            ),
            make_file(
                "file2.sst",
                0,
                2,
                1000,
                Some(b"a".to_vec()),
                Some(b"z".to_vec()),
            ),
        ];

        // Act
        let plan = compactor.pick_compaction(&files, 0);

        // Assert: no compaction triggered
        assert!(plan.is_none());
    }

    // ============================================================================
    // Tests for key range overlap detection invariant
    // ============================================================================

    #[test]
    fn should_include_overlapping_files_in_compaction_plan() {
        // Arrange
        let compactor = Compactor::new();
        let threshold = compactor.config.l0_compaction_threshold;

        let files = vec![
            // L0 file triggering compaction
            make_file(
                "l0_file.sst",
                0,
                0,
                threshold + 1,
                Some(b"b".to_vec()),
                Some(b"c".to_vec()),
            ),
            // L1 file overlapping
            make_file(
                "l1_overlap.sst",
                0,
                1,
                1000,
                Some(b"a".to_vec()),
                Some(b"d".to_vec()),
            ),
            // L1 file non-overlapping
            make_file(
                "l1_no_overlap.sst",
                0,
                1,
                1000,
                Some(b"x".to_vec()),
                Some(b"z".to_vec()),
            ),
        ];

        // Act
        let plan = compactor.pick_compaction(&files, 0);

        // Assert: plan includes overlapping file
        assert!(plan.is_some());
        let plan = plan.unwrap();
        assert!(plan.input_files.contains(&"l0_file.sst".to_string()));
        assert!(plan.input_files.contains(&"l1_overlap.sst".to_string()));
        assert!(!plan.input_files.contains(&"l1_no_overlap.sst".to_string()));
    }

    #[test]
    fn should_handle_files_with_no_key_range() {
        // Arrange
        let compactor = Compactor::new();
        let threshold = compactor.config.l0_compaction_threshold;

        let files = vec![
            make_file("l0_file.sst", 0, 0, threshold + 1, None, None), // No key range
        ];

        // Act
        let plan = compactor.pick_compaction(&files, 0);

        // Assert: should handle gracefully (plan_zero_level returns None if no key range)
        assert!(plan.is_none());
    }

    // ============================================================================
    // Tests for file deduplication invariant
    // ============================================================================

    #[test]
    fn should_deduplicate_files_in_compaction_plan() {
        // Arrange
        let compactor = Compactor::new();
        let threshold = compactor.config.l0_compaction_threshold;

        let files = vec![
            // L0 files
            make_file(
                "l0_file1.sst",
                0,
                0,
                threshold / 2 + 1,
                Some(b"a".to_vec()),
                Some(b"m".to_vec()),
            ),
            make_file(
                "l0_file2.sst",
                0,
                0,
                threshold / 2 + 1,
                Some(b"n".to_vec()),
                Some(b"z".to_vec()),
            ),
        ];

        // Act
        let plan = compactor.pick_compaction(&files, 0);

        // Assert: no duplicates in input_files
        let plan = plan.unwrap();
        let unique_count = plan
            .input_files
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len();
        assert_eq!(unique_count, plan.input_files.len());
    }

    #[test]
    fn should_sort_files_in_compaction_plan() {
        // Arrange
        let compactor = Compactor::new();
        let threshold = compactor.config.l0_compaction_threshold;

        let files = vec![
            make_file(
                "z_file.sst",
                0,
                0,
                threshold / 2 + 1,
                Some(b"z".to_vec()),
                Some(b"zz".to_vec()),
            ),
            make_file(
                "a_file.sst",
                0,
                0,
                threshold / 2 + 1,
                Some(b"a".to_vec()),
                Some(b"aa".to_vec()),
            ),
            make_file(
                "m_file.sst",
                0,
                0,
                threshold / 2 + 1,
                Some(b"m".to_vec()),
                Some(b"mm".to_vec()),
            ),
        ];

        // Act
        let plan = compactor.pick_compaction(&files, 0);

        // Assert: files should be sorted
        let plan = plan.unwrap();
        let mut sorted = plan.input_files.clone();
        sorted.sort();
        assert_eq!(plan.input_files, sorted);
    }

    // ============================================================================
    // Tests for determinism invariant
    // ============================================================================

    #[test]
    fn should_produce_same_plan_for_identical_input() {
        // Arrange
        let compactor = Compactor::new();
        let threshold = compactor.config.l0_compaction_threshold;

        let files = vec![
            make_file(
                "l0_file.sst",
                0,
                0,
                threshold + 1,
                Some(b"a".to_vec()),
                Some(b"z".to_vec()),
            ),
            make_file(
                "l1_file.sst",
                0,
                1,
                1000,
                Some(b"a".to_vec()),
                Some(b"z".to_vec()),
            ),
        ];

        // Act: run pick_compaction twice
        let plan1 = compactor.pick_compaction(&files, 0);
        let plan2 = compactor.pick_compaction(&files, 0);

        // Assert: plans should be identical
        assert_eq!(plan1.is_some(), plan2.is_some());
        if let (Some(p1), Some(p2)) = (plan1, plan2) {
            assert_eq!(p1.input_files, p2.input_files);
            assert_eq!(p1.source_level, p2.source_level);
            assert_eq!(p1.target_level, p2.target_level);
        }
    }

    // ============================================================================
    // Tests for CF filtering invariant
    // ============================================================================

    #[test]
    fn should_only_include_files_from_specified_cf() {
        // Arrange
        let compactor = Compactor::new();
        let threshold = compactor.config.l0_compaction_threshold;

        let files = vec![
            make_file(
                "cf0_file1.sst",
                0,
                0,
                threshold / 2 + 1,
                Some(b"a".to_vec()),
                Some(b"m".to_vec()),
            ),
            make_file(
                "cf0_file2.sst",
                0,
                0,
                threshold / 2 + 1,
                Some(b"n".to_vec()),
                Some(b"z".to_vec()),
            ),
            make_file(
                "cf1_file.sst",
                1,
                0,
                threshold + 1,
                Some(b"a".to_vec()),
                Some(b"z".to_vec()),
            ),
        ];

        // Act
        let plan = compactor.pick_compaction(&files, 0);

        // Assert: only CF 0 files in plan
        assert!(plan.is_some());
        let plan = plan.unwrap();
        assert_eq!(plan.cf_id, 0);
        assert!(!plan.input_files.iter().any(|f| f.contains("cf1")));
    }

    // ============================================================================
    // Tests for CompactionPlan invariants
    // ============================================================================

    #[test]
    fn should_initialize_output_files_empty_in_plan() {
        // Arrange
        // (no setup required)

        // Act
        let plan = CompactionPlan::new(0, 0, 1);

        // Assert
        assert!(plan.output_files.is_empty());
    }

    #[test]
    fn should_initialize_input_files_empty_in_plan() {
        // Arrange
        // (no setup required)

        // Act
        let plan = CompactionPlan::new(0, 0, 1);

        // Assert
        assert!(plan.input_files.is_empty());
    }

    #[test]
    fn should_initialize_output_seq_zero_in_plan() {
        // Arrange
        // (no setup required)

        // Act
        let plan = CompactionPlan::new(0, 0, 1);

        // Assert
        assert_eq!(plan.output_seq, 0);
    }

    #[test]
    fn should_set_output_seq_with_builder() {
        // Arrange
        // (no setup required)

        // Act
        let plan = CompactionPlan::new(0, 0, 1).with_output_seq(42);

        // Assert
        assert_eq!(plan.output_seq, 42);
    }

    #[test]
    fn should_set_levels_correctly_in_plan() {
        // Arrange
        // (no setup required)

        // Act
        let plan = CompactionPlan::new(5, 2, 3);

        // Assert
        assert_eq!(plan.cf_id, 5);
        assert_eq!(plan.source_level, 2);
        assert_eq!(plan.target_level, 3);
    }
}
