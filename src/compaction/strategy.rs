//! Compaction strategy and planning
//!
//! Implements a classic *leveled compaction* picker for an LSM tree.
//!
//! Goals:
//!   - Compact L0 aggressively (high write amplification, unbounded ingest).
//!   - Maintain size ratio between levels (L1 ≈ threshold; Ln ≈ threshold * multiplier^(n-1)).
//!   - Keep read amplification low by honoring key-range overlap.
//!   - Produce deterministic compaction plans.

use crate::common::{MidgeError, MidgeResult};
use crate::metadata::FileMeta;

/// A compaction plan describing which SSTs to read and which level to promote into.
#[derive(Debug, Clone)]
pub struct CompactionPlan {
    pub input_files: Vec<String>,
    #[cfg(test)]
    pub output_files: Vec<String>,
    pub source_level: u32,
    pub target_level: u32,
    pub cf_id: u32,
    /// Output SST sequence number (assigned by sequence allocator)
    pub output_seq: u64,
    /// Oldest active snapshot sequence, if any, used for tombstone retention.
    pub snapshot_horizon: Option<u64>,
    /// Point tombstones may be removed only when every unselected file whose
    /// persisted point-key bounds overlap the plan is absent, so no older
    /// point value can be resurrected at any configured level.
    pub point_tombstone_gc_eligible: bool,
    /// Range tombstones additionally require complete source/target coverage;
    /// persisted legacy bounds are advisory and cannot prove a range delete
    /// does not reach an otherwise unselected file.
    pub range_tombstone_gc_eligible: bool,
}

impl CompactionPlan {
    /// Create a new compaction plan for the given column family and level range.
    #[cfg(test)]
    pub fn new(cf_id: u32, source_level: u32, target_level: u32) -> Self {
        Self {
            input_files: Vec::new(),
            #[cfg(test)]
            output_files: Vec::new(),
            source_level,
            target_level,
            cf_id,
            output_seq: 0,
            snapshot_horizon: None,
            point_tombstone_gc_eligible: false,
            range_tombstone_gc_eligible: false,
        }
    }

    #[cfg(test)]
    pub fn with_output_seq(mut self, output_seq: u64) -> Self {
        self.output_seq = output_seq;
        self
    }

    #[cfg(test)]
    pub fn with_snapshot_horizon(mut self, snapshot_horizon: Option<u64>) -> Self {
        self.snapshot_horizon = snapshot_horizon;
        self
    }

    #[cfg(test)]
    pub fn with_tombstone_gc_eligibility(
        mut self,
        point_tombstone_gc_eligible: bool,
        range_tombstone_gc_eligible: bool,
    ) -> Self {
        self.point_tombstone_gc_eligible = point_tombstone_gc_eligible;
        self.range_tombstone_gc_eligible = range_tombstone_gc_eligible;
        self
    }
}

/// Configuration for leveled compaction.
///
/// Level size rules:
///   L0: special-case, file-count or size-based threshold.
///   L1: explicitly configured target.
///   Ln (n >= 2): `L1_target` * `level_multiplier^(n` - 1)
#[derive(Debug, Clone)]
pub struct LeveledCompactionConfig {
    pub l0_compaction_threshold: u64,
    pub l0_file_count_threshold: usize,
    /// Hard safety bound for the complete source-plus-target overlap closure.
    /// Planning fails closed instead of truncating target overlaps, because a
    /// partial closure can publish overlapping files or resurrect old values.
    pub max_compaction_input_files: usize,
    pub level_multiplier: u64,
    pub l1_target_size: u64,
    pub max_levels: usize,
}

impl Default for LeveledCompactionConfig {
    fn default() -> Self {
        Self {
            l0_compaction_threshold: 4 * 1024 * 1024, // 4MB
            l0_file_count_threshold: 4,
            max_compaction_input_files: 64,
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
    pub fn pick_compaction(
        &self,
        files: &[FileMeta],
        cf_id: u32,
    ) -> MidgeResult<Option<CompactionPlan>> {
        // Only look at this CF's files
        let cf_files: Vec<&FileMeta> = files.iter().filter(|f| f.cf_id == cf_id).collect();
        if cf_files.is_empty() {
            return Ok(None);
        }
        self.validate_file_metadata(&cf_files, cf_id)?;

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
            let target_size =
                self.level_target_size(u32::try_from(level).expect("level index fits in u32"));

            if level_size > target_size {
                return self.plan_inner_level(&levels, cf_id, level);
            }
        }

        Ok(None)
    }

    fn validate_file_metadata(&self, files: &[&FileMeta], cf_id: u32) -> MidgeResult<()> {
        for file in files {
            if file.level as usize >= self.config.max_levels {
                return Err(MidgeError::Corruption(format!(
                    "SST '{}' for column family {cf_id} has unsupported level {}",
                    file.name, file.level
                )));
            }
            let (Some(smallest), Some(largest)) = (&file.smallest_key, &file.largest_key) else {
                return Err(MidgeError::Corruption(format!(
                    "SST '{}' for column family {cf_id} is missing key-range metadata",
                    file.name
                )));
            };
            if smallest > largest {
                return Err(MidgeError::Corruption(format!(
                    "SST '{}' for column family {cf_id} has an inverted key range",
                    file.name
                )));
            }
        }
        Ok(())
    }

    /// Classic leveled size rule:
    ///   level=0: threshold tuned for L0
    ///   level=1: explicitly configured
    ///   level>=2: `l1_target_size` * (multiplier^(level - 1))
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
    fn plan_zero_level(
        &self,
        levels: &[Vec<&FileMeta>],
        cf_id: u32,
    ) -> MidgeResult<Option<CompactionPlan>> {
        if levels[0].is_empty() {
            return Ok(None);
        }

        // Always include the oldest file so read-heat skew cannot starve cold
        // data. Fill the rest of the configured batch with the hottest files.
        let mut l0_sorted = levels[0].clone();
        l0_sorted.sort_by(|left, right| {
            right
                .get_read_count()
                .cmp(&left.get_read_count())
                .then_with(|| file_age_key(left).cmp(&file_age_key(right)))
        });
        let oldest = levels[0]
            .iter()
            .copied()
            .min_by_key(|file| file_age_key(file))
            .expect("non-empty L0 has an oldest file");
        l0_sorted.retain(|file| file.name != oldest.name);

        let batch_size = levels[0]
            .len()
            .min(self.config.l0_file_count_threshold.max(1));
        let mut l0_batch = Vec::with_capacity(batch_size);
        l0_batch.push(oldest);
        l0_batch.extend(l0_sorted.into_iter().take(batch_size.saturating_sub(1)));

        let (input_files, _, _) =
            self.bounded_overlap_closure(&mut l0_batch, &levels[1], cf_id, 0, 1)?;
        let (point_tombstone_gc_eligible, range_tombstone_gc_eligible) =
            Self::tombstone_gc_eligibility(levels, &input_files)?;

        Ok(Some(CompactionPlan {
            input_files,
            #[cfg(test)]
            output_files: Vec::new(),
            source_level: 0,
            target_level: 1,
            cf_id,
            output_seq: 0,
            snapshot_horizon: None,
            point_tombstone_gc_eligible,
            range_tombstone_gc_eligible,
        }))
    }

    /// Build a compaction plan for level N → N+1.
    fn plan_inner_level(
        &self,
        levels: &[Vec<&FileMeta>],
        cf_id: u32,
        level: usize,
    ) -> MidgeResult<Option<CompactionPlan>> {
        if levels[level].is_empty() {
            return Ok(None);
        }

        // Select a bounded, contiguous source-level range. Every overlapping
        // target file is still included below; overlap completeness is a
        // correctness requirement and must not be truncated to meet the cap.
        let mut source_files = levels[level].clone();
        source_files.sort_by(|left, right| {
            left.smallest_key
                .cmp(&right.smallest_key)
                .then_with(|| left.name.cmp(&right.name))
        });
        source_files.truncate(self.config.l0_file_count_threshold.max(1));
        let (input_files, _, _) = self.bounded_overlap_closure(
            &mut source_files,
            &levels[level + 1],
            cf_id,
            u32::try_from(level).expect("level index fits in u32"),
            u32::try_from(level + 1).expect("level index fits in u32"),
        )?;
        let (point_tombstone_gc_eligible, range_tombstone_gc_eligible) =
            Self::tombstone_gc_eligibility(levels, &input_files)?;

        Ok(Some(CompactionPlan {
            input_files,
            #[cfg(test)]
            output_files: Vec::new(),
            source_level: u32::try_from(level).expect("level index fits in u32"),
            target_level: u32::try_from(level + 1).expect("level index fits in u32"),
            cf_id,
            output_seq: 0,
            snapshot_horizon: None,
            point_tombstone_gc_eligible,
            range_tombstone_gc_eligible,
        }))
    }

    fn tombstone_gc_eligibility(
        levels: &[Vec<&FileMeta>],
        input_files: &[String],
    ) -> MidgeResult<(bool, bool)> {
        let selected_names: std::collections::HashSet<_> =
            input_files.iter().map(String::as_str).collect();
        // Target files selected by the overlap closure can extend beyond the
        // original source range. The proof must cover the full output range;
        // otherwise a tombstone in that extension could be dropped while an
        // unselected deeper file still contains the value it masks.
        let selected_files = levels
            .iter()
            .flatten()
            .filter(|file| selected_names.contains(file.name.as_str()))
            .copied()
            .collect::<Vec<_>>();
        let (min_key, max_key) = smallest_and_largest(&selected_files)?;
        let unselected_point_overlap = levels.iter().flatten().any(|file| {
            !selected_names.contains(file.name.as_str())
                && file
                    .smallest_key
                    .as_ref()
                    .zip(file.largest_key.as_ref())
                    .is_some_and(|(smallest, largest)| {
                        smallest.as_slice() <= max_key.as_slice()
                            && largest.as_slice() >= min_key.as_slice()
                    })
        });
        let point_tombstone_gc_eligible = !unselected_point_overlap;

        // Legacy file bounds can omit range-tombstone endpoints. Only a plan
        // containing every file in the column family proves that dropping a
        // range marker cannot expose a covered value outside the selected
        // metadata range.
        let total_files = levels.iter().map(Vec::len).sum::<usize>();
        let range_tombstone_gc_eligible =
            point_tombstone_gc_eligible && selected_names.len() == total_files;
        Ok((point_tombstone_gc_eligible, range_tombstone_gc_eligible))
    }

    fn validate_input_fan_in(
        &self,
        input_files: &[String],
        cf_id: u32,
        source_level: u32,
        target_level: u32,
    ) -> MidgeResult<()> {
        let limit = self.config.max_compaction_input_files.max(1);
        if input_files.len() > limit {
            return Err(MidgeError::ResourceLimit(format!(
                "compaction L{source_level}->L{target_level} for column family {cf_id} requires {} source/overlap inputs, exceeding the configured safety limit {limit}; refusing to truncate the target overlap closure",
                input_files.len()
            )));
        }
        Ok(())
    }

    fn bounded_overlap_closure(
        &self,
        source_files: &mut Vec<&FileMeta>,
        target_files: &[&FileMeta],
        cf_id: u32,
        source_level: u32,
        target_level: u32,
    ) -> MidgeResult<(Vec<String>, Vec<u8>, Vec<u8>)> {
        loop {
            let (min_key, max_key) = smallest_and_largest(source_files.as_slice())?;
            let mut input_files = source_files
                .iter()
                .map(|file| file.name.clone())
                .collect::<Vec<_>>();
            input_files.extend(overlap_with_range(target_files, &min_key, &max_key)?);
            dedupe_sort(&mut input_files);

            match self.validate_input_fan_in(&input_files, cf_id, source_level, target_level) {
                Ok(()) => return Ok((input_files, min_key, max_key)),
                Err(MidgeError::ResourceLimit(_)) if source_files.len() > 1 => {
                    // Decompose the source range and recompute the *complete*
                    // target closure. Target files are never truncated.
                    source_files.pop();
                }
                Err(error) => return Err(error),
            }
        }
    }
}

/// Extract smallest and largest user keys across a slice of `FileMeta`.
fn smallest_and_largest(files: &[&FileMeta]) -> MidgeResult<(Vec<u8>, Vec<u8>)> {
    let smallest = files
        .iter()
        .map(|file| {
            file.smallest_key.clone().ok_or_else(|| {
                MidgeError::Corruption(format!(
                    "SST '{}' is missing its smallest-key metadata",
                    file.name
                ))
            })
        })
        .collect::<MidgeResult<Vec<_>>>()?
        .into_iter()
        .min()
        .ok_or_else(|| MidgeError::Internal("compaction range has no input files".to_string()))?;
    let largest = files
        .iter()
        .map(|file| {
            file.largest_key.clone().ok_or_else(|| {
                MidgeError::Corruption(format!(
                    "SST '{}' is missing its largest-key metadata",
                    file.name
                ))
            })
        })
        .collect::<MidgeResult<Vec<_>>>()?
        .into_iter()
        .max()
        .ok_or_else(|| MidgeError::Internal("compaction range has no input files".to_string()))?;
    Ok((smallest, largest))
}

/// Return names of files whose key-ranges overlap [`min_key`, `max_key`].
fn overlap_with_range(
    files: &[&FileMeta],
    min_key: &[u8],
    max_key: &[u8],
) -> MidgeResult<Vec<String>> {
    files
        .iter()
        .map(|f| {
            let fs = f.smallest_key.as_ref().ok_or_else(|| {
                MidgeError::Corruption(format!(
                    "SST '{}' is missing its smallest-key metadata",
                    f.name
                ))
            })?;
            let fl = f.largest_key.as_ref().ok_or_else(|| {
                MidgeError::Corruption(format!(
                    "SST '{}' is missing its largest-key metadata",
                    f.name
                ))
            })?;
            if fs.as_slice() <= max_key && fl.as_slice() >= min_key {
                Ok(Some(f.name.clone()))
            } else {
                Ok(None)
            }
        })
        .filter_map(std::result::Result::transpose)
        .collect()
}

fn file_age_key(file: &FileMeta) -> (u64, &str) {
    let encoded_sequence = std::path::Path::new(&file.name)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .and_then(|stem| stem.rsplit('_').next())
        .and_then(|sequence| sequence.parse::<u64>().ok());
    (file.sst_seq.max(encoded_sequence.unwrap_or(0)), &file.name)
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
            read_count: std::sync::Arc::default(),
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
        assert_eq!(compactor.config.max_compaction_input_files, 64);
    }

    #[test]
    fn should_create_compactor_with_custom_config() {
        // Arrange
        let config = LeveledCompactionConfig {
            l0_compaction_threshold: 8 * 1024 * 1024,
            l0_file_count_threshold: 8,
            max_compaction_input_files: 32,
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
        assert_eq!(compactor.config.max_compaction_input_files, 32);
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
        let plan = compactor
            .pick_compaction(&empty_files, 0)
            .expect("compaction planning");

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
        let plan = compactor
            .pick_compaction(&files, 1)
            .expect("compaction planning");

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
        let plan = compactor
            .pick_compaction(&files, 0)
            .expect("compaction planning");

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
        for i in 0..=count_threshold {
            let byte = u8::try_from(i).unwrap_or_else(|_| {
                panic!("test fixture key index must fit in u8 for synthetic payload");
            });
            files.push(make_file(
                &format!("file{i}.sst"),
                0,
                0,
                1000,
                Some(vec![byte]),
                Some(vec![byte.wrapping_add(1)]),
            ));
        }

        // Act
        let plan = compactor
            .pick_compaction(&files, 0)
            .expect("compaction planning");

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
        let plan = compactor
            .pick_compaction(&files, 0)
            .expect("compaction planning");

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
        let plan = compactor
            .pick_compaction(&files, 0)
            .expect("compaction planning");

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
        let plan = compactor
            .pick_compaction(&files, 0)
            .expect("compaction planning");

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
        let plan = compactor
            .pick_compaction(&files, 0)
            .expect("compaction planning");

        // Assert: plan includes overlapping file
        assert!(plan.is_some());
        let plan = plan.unwrap();
        assert!(plan.input_files.contains(&"l0_file.sst".to_string()));
        assert!(plan.input_files.contains(&"l1_overlap.sst".to_string()));
        assert!(!plan.input_files.contains(&"l1_no_overlap.sst".to_string()));
    }

    #[test]
    fn should_fail_compaction_planning_when_file_key_range_is_missing() {
        // Arrange
        let compactor = Compactor::new();
        let threshold = compactor.config.l0_compaction_threshold;

        let files = vec![
            make_file("l0_file.sst", 0, 0, threshold + 1, None, None), // No key range
        ];

        // Act
        let error = compactor
            .pick_compaction(&files, 0)
            .expect_err("missing persisted key-range metadata must fail closed");

        // Assert
        assert!(matches!(error, crate::common::MidgeError::Corruption(_)));
    }

    #[test]
    fn should_fail_compaction_planning_when_target_file_key_range_is_missing() {
        // Arrange
        let compactor = Compactor::new();
        let files = vec![
            make_file(
                "source.sst",
                0,
                0,
                compactor.config.l0_compaction_threshold + 1,
                Some(b"a".to_vec()),
                Some(b"z".to_vec()),
            ),
            make_file("unknown-target.sst", 0, 1, 1, None, None),
        ];

        // Act
        let error = compactor
            .pick_compaction(&files, 0)
            .expect_err("unknown target overlap must fail closed");

        // Assert
        assert!(matches!(error, crate::common::MidgeError::Corruption(_)));
    }

    #[test]
    fn should_fail_compaction_planning_when_file_key_range_is_inverted() {
        // Arrange
        let compactor = Compactor::new();
        let files = vec![make_file(
            "inverted.sst",
            0,
            0,
            compactor.config.l0_compaction_threshold + 1,
            Some(b"z".to_vec()),
            Some(b"a".to_vec()),
        )];

        // Act
        let error = compactor
            .pick_compaction(&files, 0)
            .expect_err("inverted range must fail closed");

        // Assert
        assert!(matches!(error, crate::common::MidgeError::Corruption(_)));
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
        let plan = compactor
            .pick_compaction(&files, 0)
            .expect("compaction planning");

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
        let plan = compactor
            .pick_compaction(&files, 0)
            .expect("compaction planning");

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
        let plan1 = compactor
            .pick_compaction(&files, 0)
            .expect("first compaction planning");
        let plan2 = compactor
            .pick_compaction(&files, 0)
            .expect("second compaction planning");

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
        let plan = compactor
            .pick_compaction(&files, 0)
            .expect("compaction planning");

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

    #[test]
    fn should_bound_l1_plus_compaction_batch_size_when_level_grows_large() {
        // Arrange
        let compactor = Compactor::new();
        let mut files = Vec::new();
        for index in 0..12_u8 {
            files.push(make_file(
                &format!("l1_{index:02}.sst"),
                0,
                1,
                compactor.config.l1_target_size,
                Some(vec![index]),
                Some(vec![index]),
            ));
        }

        // Act
        let plan = compactor
            .pick_compaction(&files, 0)
            .expect("compaction planning")
            .expect("oversized L1 should produce a compaction plan");

        // Assert
        assert!(
            plan.input_files.len() <= compactor.config.l0_file_count_threshold,
            "inner-level compaction must keep its input batch bounded: {:?}",
            plan.input_files
        );
    }

    #[test]
    fn should_include_every_target_overlap_when_inner_source_batch_is_bounded() {
        // Arrange
        let compactor = Compactor::new();
        let mut files = Vec::new();
        for index in 0..8_u8 {
            files.push(make_file(
                &format!("source_{index}.sst"),
                0,
                1,
                compactor.config.l1_target_size,
                Some(vec![index]),
                Some(vec![index]),
            ));
        }
        for index in 0..6_u8 {
            files.push(make_file(
                &format!("target_{index}.sst"),
                0,
                2,
                1,
                Some(vec![0]),
                Some(vec![3]),
            ));
        }

        // Act
        let plan = compactor
            .pick_compaction(&files, 0)
            .expect("compaction planning")
            .expect("inner-level plan");

        // Assert
        assert_eq!(
            plan.input_files
                .iter()
                .filter(|name| name.starts_with("source_"))
                .count(),
            compactor.config.l0_file_count_threshold
        );
        assert_eq!(
            plan.input_files
                .iter()
                .filter(|name| name.starts_with("target_"))
                .count(),
            6,
            "target overlap closure must never be truncated to the source cap"
        );
    }

    #[test]
    fn should_enable_tombstone_gc_with_key_range_bottommost_overlap_proof() {
        // Arrange
        let compactor = Compactor::with_config(LeveledCompactionConfig {
            l1_target_size: 1,
            ..LeveledCompactionConfig::default()
        });
        let complete_files = vec![
            make_file(
                "source.sst",
                0,
                1,
                2,
                Some(b"a".to_vec()),
                Some(b"z".to_vec()),
            ),
            make_file(
                "target.sst",
                0,
                2,
                1,
                Some(b"a".to_vec()),
                Some(b"z".to_vec()),
            ),
        ];
        let mut incomplete_files = complete_files.clone();
        incomplete_files.push(make_file(
            "unrelated-target.sst",
            0,
            2,
            1,
            Some(b"zz".to_vec()),
            Some(b"zzz".to_vec()),
        ));
        let mut overlapping_upper_files = complete_files.clone();
        overlapping_upper_files.push(make_file(
            "overlapping-upper.sst",
            0,
            0,
            1,
            Some(b"m".to_vec()),
            Some(b"n".to_vec()),
        ));

        // Act
        let complete = compactor
            .pick_compaction(&complete_files, 0)
            .expect("complete planning")
            .expect("complete plan");
        let incomplete = compactor
            .pick_compaction(&incomplete_files, 0)
            .expect("incomplete planning")
            .expect("incomplete plan");
        let overlapping_upper = compactor
            .pick_compaction(&overlapping_upper_files, 0)
            .expect("overlapping upper-level planning")
            .expect("overlapping upper-level plan");

        // Assert
        assert!(complete.point_tombstone_gc_eligible);
        assert!(complete.range_tombstone_gc_eligible);
        assert!(incomplete.point_tombstone_gc_eligible);
        assert!(!incomplete.range_tombstone_gc_eligible);
        assert!(!overlapping_upper.point_tombstone_gc_eligible);
        assert!(!overlapping_upper.range_tombstone_gc_eligible);
    }

    #[test]
    fn should_retain_point_tombstones_when_selected_target_extends_into_deeper_overlap() {
        // Arrange
        let compactor = Compactor::with_config(LeveledCompactionConfig {
            l1_target_size: 1,
            ..LeveledCompactionConfig::default()
        });
        let files = vec![
            make_file(
                "source.sst",
                0,
                1,
                2,
                Some(b"m".to_vec()),
                Some(b"n".to_vec()),
            ),
            make_file(
                "broad-target.sst",
                0,
                2,
                1,
                Some(b"a".to_vec()),
                Some(b"z".to_vec()),
            ),
            make_file(
                "deeper-value.sst",
                0,
                3,
                1,
                Some(b"a".to_vec()),
                Some(b"b".to_vec()),
            ),
        ];

        // Act
        let plan = compactor
            .pick_compaction(&files, 0)
            .expect("plan compaction")
            .expect("eligible inner-level plan");

        // Assert
        assert_eq!(
            plan.input_files,
            vec!["broad-target.sst".to_string(), "source.sst".to_string()]
        );
        assert!(!plan.point_tombstone_gc_eligible);
        assert!(!plan.range_tombstone_gc_eligible);
    }

    #[test]
    fn should_shrink_source_range_to_fit_complete_target_overlap_within_safety_limit() {
        // Arrange
        let compactor = Compactor::with_config(LeveledCompactionConfig {
            l1_target_size: 1,
            l0_file_count_threshold: 4,
            max_compaction_input_files: 5,
            ..LeveledCompactionConfig::default()
        });
        let mut files = Vec::new();
        for index in 0..4_u8 {
            files.push(make_file(
                &format!("source-{index}.sst"),
                0,
                1,
                1,
                Some(vec![index]),
                Some(vec![index]),
            ));
            files.push(make_file(
                &format!("target-{index}.sst"),
                0,
                2,
                1,
                Some(vec![index]),
                Some(vec![index]),
            ));
        }

        // Act
        let plan = compactor
            .pick_compaction(&files, 0)
            .expect("bounded planning")
            .expect("decomposed inner-level plan");

        // Assert
        assert_eq!(
            plan.input_files,
            vec![
                "source-0.sst".to_string(),
                "source-1.sst".to_string(),
                "target-0.sst".to_string(),
                "target-1.sst".to_string(),
            ]
        );
        assert!(plan.input_files.len() <= compactor.config.max_compaction_input_files);
    }

    #[test]
    fn should_fail_closed_when_complete_target_overlap_exceeds_input_safety_limit() {
        // Arrange
        let compactor = Compactor::with_config(LeveledCompactionConfig {
            l1_target_size: 1,
            l0_file_count_threshold: 4,
            max_compaction_input_files: 8,
            ..LeveledCompactionConfig::default()
        });
        let mut files = vec![make_file(
            "broad-source.sst",
            0,
            1,
            2,
            Some(b"a".to_vec()),
            Some(b"z".to_vec()),
        )];
        for index in 0..8_u8 {
            files.push(make_file(
                &format!("target-{index}.sst"),
                0,
                2,
                1,
                Some(vec![b'a'.saturating_add(index)]),
                Some(vec![b'a'.saturating_add(index)]),
            ));
        }

        // Act
        let error = compactor
            .pick_compaction(&files, 0)
            .expect_err("complete overlap closure exceeds finite fan-in budget");

        // Assert
        assert!(matches!(error, MidgeError::ResourceLimit(_)));
        assert!(error.to_string().contains("refusing to truncate"));
    }

    #[test]
    fn should_eventually_include_oldest_cold_l0_file_when_read_heat_is_skewed() {
        // Arrange
        let compactor = Compactor::new();
        let mut files = Vec::new();
        for sequence in 1..=6_u64 {
            let name = crate::sst::file_name(0, 0, sequence);
            let file = make_file(
                &name,
                0,
                0,
                1,
                Some(vec![u8::try_from(sequence).expect("fixture sequence")]),
                Some(vec![u8::try_from(sequence).expect("fixture sequence")]),
            );
            if sequence > 1 {
                for _ in 0..sequence {
                    file.record_read();
                }
            }
            files.push(file);
        }
        let oldest_cold = crate::sst::file_name(0, 0, 1);

        // Act
        let plan = compactor
            .pick_compaction(&files, 0)
            .expect("compaction planning")
            .expect("L0 threshold should produce a compaction plan");

        // Assert
        assert!(
            plan.input_files.contains(&oldest_cold),
            "every L0 batch must make progress on the oldest cold file"
        );
    }

    #[test]
    fn should_scale_l0_batch_size_with_configured_file_count_threshold() {
        // Arrange
        let configured_threshold = 6;
        let compactor = Compactor::with_config(LeveledCompactionConfig {
            l0_file_count_threshold: configured_threshold,
            ..LeveledCompactionConfig::default()
        });
        let files: Vec<_> = (1..=8_u64)
            .map(|sequence| {
                make_file(
                    &crate::sst::file_name(0, 0, sequence),
                    0,
                    0,
                    1,
                    Some(vec![u8::try_from(sequence).expect("fixture sequence")]),
                    Some(vec![u8::try_from(sequence).expect("fixture sequence")]),
                )
            })
            .collect();

        // Act
        let plan = compactor
            .pick_compaction(&files, 0)
            .expect("compaction planning")
            .expect("L0 threshold should produce a compaction plan");

        // Assert
        assert_eq!(plan.input_files.len(), configured_threshold);
    }
}
