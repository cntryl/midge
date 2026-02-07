pub mod executor;
pub mod merge;
pub mod strategy;

pub use strategy::{CompactionPlan, Compactor, LeveledCompactionConfig};

use crate::common::{MidgeError, MidgeResult};
use std::path::{Path, PathBuf};

/// Executes a compaction plan by streaming merged key/value pairs into one or more
/// output SST files. This function performs:
///   1. Input SST discovery
///   2. Streaming merge across all inputs (sorted, deduped)
///   3. Tombstone filtering
///   4. Delegation to `SstFactory` to create new SST files
///
/// This is intentionally thin — the heavy lifting is inside `executor::*`
/// which performs the actual merge and write pipeline.
///
/// **Important**: `output_dir` must be the CF-specific directory (e.g., `cf_00/`),
/// not the DB root. Output filename is sequence-only: `{seq:08}.sst`.
pub fn execute_compaction(
    plan: &CompactionPlan,
    sst_factory: &dyn crate::sst::SstFactory,
    output_dir: &Path,
    abort_check: Option<&dyn Fn() -> bool>,
) -> MidgeResult<Vec<String>> {
    // --- 1. Collect versions from all input files ---------------------------
    //
    // For now, we load versions into memory. Future: streaming merge iterator.
    let versions = executor::collect_versions(sst_factory, &plan.input_files, abort_check)?;

    if versions.is_empty() {
        return Ok(Vec::new());
    }

    // --- 2. Deduplicate and keep only latest versions -----------------------
    let deduplicated = executor::deduplicate_versions(&versions);

    // --- 3. Filter out tombstones for final output --------------------------
    let final_versions = executor::filter_tombstones(&deduplicated);

    // --- 4. Prepare output file path ----------------------------------------
    let output_file = output_filename(plan, output_dir);
    let output_file_str = output_file.to_str().ok_or(MidgeError::InvalidPath)?;

    // --- 5. Write merged versions to SST ------------------------------------
    executor::write_versions_to_sst(sst_factory, output_file_str, &final_versions, abort_check)?;

    Ok(vec![output_file
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("output.sst")
        .to_owned()])
}

/// Construct the output filename for a completed compaction.
/// This is stable and predictable for crash recovery and manifest logging.
///
/// Follows LSM-tree industry standard: CF → directory, sequence → filename.
/// The directory is assumed to already be CF-specific (e.g., `cf_00/`).
fn output_filename(plan: &CompactionPlan, cf_dir: &Path) -> PathBuf {
    // File naming rules (aligned with RocksDB, TiKV, Pebble):
    // - filename encodes only ordering information (sequence)
    // - zero-padded to maintain lexicographic sort
    // - CF identity is encoded in the directory structure, not the filename
    let name = format!("{:08}.sst", plan.output_seq);
    cf_dir.join(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ============================================================================
    // Tests for output_filename invariant: stable, zero-padded naming
    // ============================================================================

    #[test]
    fn should_format_output_filename_with_zero_padded_sequence() {
        // Arrange
        let plan = CompactionPlan::new(0, 0, 1).with_output_seq(42);
        let cf_dir = Path::new("cf_00");

        // Act
        let filename = output_filename(&plan, cf_dir);

        // Assert: should be zero-padded to 8 digits
        assert_eq!(filename, PathBuf::from("cf_00/00000042.sst"));
    }

    #[test]
    fn should_preserve_cf_directory_path_when_generating_filename() {
        // Arrange
        let plan = CompactionPlan::new(0, 0, 1).with_output_seq(1);
        let cf_dir = Path::new("data/cf_00");

        // Act
        let filename = output_filename(&plan, cf_dir);

        // Assert: directory structure preserved
        assert_eq!(filename, PathBuf::from("data/cf_00/00000001.sst"));
    }

    #[test]
    fn should_handle_large_sequence_numbers_when_formatting() {
        // Arrange
        let plan = CompactionPlan::new(0, 0, 1).with_output_seq(999999999);
        let cf_dir = Path::new("cf_01");

        // Act
        let filename = output_filename(&plan, cf_dir);

        // Assert: large numbers formatted correctly
        assert_eq!(filename, PathBuf::from("cf_01/999999999.sst"));
    }

    #[test]
    fn should_produce_lexicographically_sortable_filenames() {
        // Arrange: two plans with different sequences
        let plan_low = CompactionPlan::new(0, 0, 1).with_output_seq(1);
        let plan_mid = CompactionPlan::new(0, 0, 1).with_output_seq(10);
        let plan_high = CompactionPlan::new(0, 0, 1).with_output_seq(100);
        let cf_dir = Path::new("cf_00");

        // Act
        let filename_low = output_filename(&plan_low, cf_dir);
        let filename_mid = output_filename(&plan_mid, cf_dir);
        let filename_high = output_filename(&plan_high, cf_dir);

        // Assert: filenames should sort correctly lexicographically
        let mut filenames = vec![
            filename_high
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap()
                .to_string(),
            filename_low
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap()
                .to_string(),
            filename_mid
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap()
                .to_string(),
        ];
        filenames.sort();

        let expected = vec!["00000001.sst", "00000010.sst", "00000100.sst"];
        assert_eq!(filenames, expected);
    }

    #[test]
    fn should_use_sst_extension_when_generating_filename() {
        // Arrange
        let plan = CompactionPlan::new(0, 0, 1).with_output_seq(5);
        let cf_dir = Path::new("cf_00");

        // Act
        let filename = output_filename(&plan, cf_dir);

        // Assert: must end with .sst
        assert!(filename.to_string_lossy().ends_with(".sst"));
    }

    #[test]
    fn should_handle_zero_sequence_number_when_formatting() {
        // Arrange
        let plan = CompactionPlan::new(0, 0, 1).with_output_seq(0);
        let cf_dir = Path::new("cf_00");

        // Act
        let filename = output_filename(&plan, cf_dir);

        // Assert
        assert_eq!(filename, PathBuf::from("cf_00/00000000.sst"));
    }

    // ============================================================================
    // Tests for CompactionPlan builder pattern and invariants
    // ============================================================================

    #[test]
    fn should_create_compaction_plan_with_constructor() {
        // Arrange
        // (constructor args)

        // Act
        let plan = CompactionPlan::new(5, 2, 3);

        // Assert: plan initialized correctly
        assert_eq!(plan.cf_id, 5);
        assert_eq!(plan.source_level, 2);
        assert_eq!(plan.target_level, 3);
        assert!(plan.input_files.is_empty());
        assert!(plan.output_files.is_empty());
        assert_eq!(plan.output_seq, 0);
    }

    #[test]
    fn should_set_output_sequence_when_using_with_output_seq() {
        // Arrange
        let plan = CompactionPlan::new(0, 0, 1);

        // Act
        let plan_with_seq = plan.with_output_seq(123);

        // Assert
        assert_eq!(plan_with_seq.output_seq, 123);
    }

    #[test]
    fn should_allow_chaining_builder_methods_when_using_with_output_seq() {
        // Arrange
        // (builder chaining)

        // Act
        let plan = CompactionPlan::new(1, 0, 1)
            .with_output_seq(456)
            .with_output_seq(789);

        // Assert: last call wins
        assert_eq!(plan.output_seq, 789);
    }

    // ============================================================================
    // Tests for level target size calculations
    // ============================================================================

    #[test]
    fn should_calculate_l0_target_size_correctly() {
        // Arrange
        let config = LeveledCompactionConfig::default();

        // Act
        let l0_target = config.l0_compaction_threshold;

        // Assert
        assert_eq!(l0_target, 4 * 1024 * 1024);
    }

    #[test]
    fn should_calculate_level_multiplier_correctly() {
        // Arrange
        let config = LeveledCompactionConfig::default();

        // Act
        let level_multiplier = config.level_multiplier;

        // Assert
        assert_eq!(level_multiplier, 10);
    }

    #[test]
    fn should_create_leveled_compaction_config_with_default_values() {
        // Arrange
        // (default config)

        // Act
        let config = LeveledCompactionConfig::default();

        // Assert
        assert_eq!(config.l0_compaction_threshold, 4 * 1024 * 1024);
        assert_eq!(config.l0_file_count_threshold, 4);
        assert_eq!(config.level_multiplier, 10);
        assert_eq!(config.l1_target_size, 40 * 1024 * 1024);
        assert_eq!(config.max_levels, 7);
    }

    // ============================================================================
    // Tests for CompactionPlan invariants
    // ============================================================================

    #[test]
    fn should_initialize_empty_file_vectors_when_creating_plan() {
        // Arrange
        // (constructor)

        // Act
        let plan = CompactionPlan::new(0, 0, 1);

        // Assert: input and output file lists should be empty
        assert!(plan.input_files.is_empty());
        assert!(plan.output_files.is_empty());
    }

    #[test]
    fn should_preserve_level_information_in_plan() {
        // Arrange
        let cf_id = 3;
        let source_level = 1;
        let target_level = 2;

        // Act
        let plan = CompactionPlan::new(cf_id, source_level, target_level);

        // Assert
        assert_eq!(plan.cf_id, cf_id);
        assert_eq!(plan.source_level, source_level);
        assert_eq!(plan.target_level, target_level);
    }

    #[test]
    fn should_handle_maximum_column_family_id() {
        // Arrange
        // (constructor with max cf id)

        // Act
        let plan = CompactionPlan::new(u32::MAX, 0, 1).with_output_seq(100);

        // Assert
        assert_eq!(plan.cf_id, u32::MAX);
    }

    #[test]
    fn should_handle_maximum_output_sequence_number() {
        // Arrange
        // (constructor with max output seq)

        // Act
        let plan = CompactionPlan::new(0, 0, 1).with_output_seq(u64::MAX);

        // Assert
        assert_eq!(plan.output_seq, u64::MAX);
    }
}
