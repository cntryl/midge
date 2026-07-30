pub mod executor;
#[cfg(test)]
pub mod merge;
pub mod strategy;

pub use strategy::{CompactionPlan, Compactor, LeveledCompactionConfig};

use crate::common::{MidgeError, MidgeResult};
use std::path::{Path, PathBuf};

/// Executes a compaction plan by merging per-SST streams directly into one
/// output SST file. This function performs:
///   1. Input SST discovery
///   2. Streaming merge across all inputs (sorted, deduped)
///   3. Tombstone filtering
///   4. Delegation to `SstFactory` to create new SST files
///
/// This is intentionally thin — the heavy lifting is inside `executor::*`
/// which performs the actual merge and write pipeline.
///
/// **Important**: `output_dir` must be the CF-specific directory (e.g., `cf_00/`),
/// not the DB root. Output filenames use canonical SST names:
/// `{cf_id:06}_{level:02}_{sequence:020}.sst`.
pub fn execute_compaction(
    plan: &CompactionPlan,
    sst_factory: &dyn crate::sst::SstFactory,
    output_dir: &Path,
    abort_check: Option<&dyn Fn() -> bool>,
) -> MidgeResult<Vec<String>> {
    // --- 1. Materialize reader-provided per-SST streams ---------------------
    //
    // The reader contract currently returns one vector per SST. Keep those
    // independent and merge their heads below rather than building a second
    // all-input vector followed by a deduplicated result vector.
    let (streams, range_tombstones) =
        executor::collect_compaction_stream_inputs(sst_factory, &plan.input_files, abort_check)?;

    if streams.iter().all(Vec::is_empty) && range_tombstones.is_empty() {
        return Err(MidgeError::Internal(
            "compaction produced no output; inputs were not replaced".to_string(),
        ));
    }

    // --- 2. Prepare output file path ----------------------------------------
    let output_file = output_filename(plan, output_dir);
    let output_file_str = output_file.to_str().ok_or(MidgeError::InvalidPath)?;

    // --- 3. K-way merge, deduplicate, and write directly to the output -----
    // This plan does not carry a proof that all deeper levels were included,
    // so the writer retains point tombstones even when a snapshot horizon
    // exists. Range tombstones are retained for the same reason.
    let _written = executor::write_merged_compaction_output_to_sst(
        sst_factory,
        output_file_str,
        streams,
        &range_tombstones,
        abort_check,
    )?;

    Ok(vec![output_file
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("output.sst")
        .to_owned()])
}

/// Construct the output filename for a completed compaction.
/// This is stable and predictable for crash recovery and manifest logging.
///
/// File names encode CF, target level, and creation sequence with fixed-width
/// numeric fields so plain directory/object-store listings sort predictably.
fn output_filename(plan: &CompactionPlan, output_dir: &Path) -> PathBuf {
    output_dir.join(crate::sst::file_name(
        plan.cf_id,
        plan.target_level,
        plan.output_seq,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::traits::SstFactory;
    use tempfile::tempdir;

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

        // Assert: should be fixed-width and lex-sortable
        assert_eq!(
            filename,
            PathBuf::from("cf_00/000000_01_00000000000000000042.sst")
        );
    }

    #[test]
    fn should_format_compaction_output_with_canonical_lex_sortable_sst_name() {
        // Arrange
        let plan = CompactionPlan::new(7, 0, 2).with_output_seq(42);
        let output_dir = Path::new("sst");

        let filename = output_filename(&plan, output_dir);

        // Act
        // Assert
        assert_eq!(
            filename,
            PathBuf::from("sst/000007_02_00000000000000000042.sst")
        );
    }

    #[test]
    fn should_preserve_cf_directory_path_when_generating_filename() {
        // Arrange
        let plan = CompactionPlan::new(0, 0, 1).with_output_seq(1);
        let cf_dir = Path::new("data/cf_00");

        // Act
        let filename = output_filename(&plan, cf_dir);

        // Assert: directory structure preserved
        assert_eq!(
            filename,
            PathBuf::from("data/cf_00/000000_01_00000000000000000001.sst")
        );
    }

    #[test]
    fn should_handle_large_sequence_numbers_when_formatting() {
        // Arrange
        let plan = CompactionPlan::new(0, 0, 1).with_output_seq(999_999_999);
        let cf_dir = Path::new("cf_01");

        // Act
        let filename = output_filename(&plan, cf_dir);

        // Assert: large numbers formatted correctly
        assert_eq!(
            filename,
            PathBuf::from("cf_01/000000_01_00000000000999999999.sst")
        );
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

        let expected = vec![
            "000000_01_00000000000000000001.sst",
            "000000_01_00000000000000000010.sst",
            "000000_01_00000000000000000100.sst",
        ];
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
        assert_eq!(
            filename,
            PathBuf::from("cf_00/000000_01_00000000000000000000.sst")
        );
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

    #[test]
    fn should_preserve_range_tombstone_given_compaction_across_multiple_levels() -> MidgeResult<()>
    {
        // Arrange
        let temp_dir = tempdir()?;
        let fs = std::sync::Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);

        let mut input_writer = factory.create()?;
        input_writer.add_with_meta(b"alpha", None, 11, 2, None)?;
        input_writer.add_range_tombstone(b"cat", b"cow", 7)?;
        crate::sst::fs::finish_writer_to_path(input_writer, &temp_dir.path().join("input.sst"))?;

        let mut plan = CompactionPlan::new(0, 0, 1).with_output_seq(42);
        plan.input_files.push("input.sst".to_string());

        // Act
        let output_names = execute_compaction(&plan, &factory, temp_dir.path(), None)?;

        // Assert
        let output_name = crate::sst::file_name(0, 1, 42);
        assert_eq!(output_names, vec![output_name.clone()]);

        let reader = factory.open(std::path::Path::new(&output_name))?;
        let states = reader.scan_range_state(None, None)?;
        assert!(
            states
                .iter()
                .any(|(_, state)| matches!(state, crate::sst::types::KeyState::Tombstone(_))),
            "point tombstone must be retained without a bottommost proof"
        );

        let range_tombstones = reader.range_tombstones();
        assert_eq!(range_tombstones.len(), 1);
        assert_eq!(range_tombstones[0].start, b"cat".to_vec());
        assert_eq!(range_tombstones[0].end, b"cow".to_vec());

        Ok(())
    }

    #[test]
    fn should_preserve_recent_point_tombstones_when_snapshot_horizon_exists() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempdir()?;
        let fs = std::sync::Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);

        let mut input_writer = factory.create()?;
        input_writer.add_with_meta(b"alpha", Some(b"older"), 5, 0, None)?;
        input_writer.add_with_meta(b"alpha", None, 11, 2, None)?;
        crate::sst::fs::finish_writer_to_path(input_writer, &temp_dir.path().join("input.sst"))?;

        let mut plan = CompactionPlan::new(0, 0, 1)
            .with_output_seq(43)
            .with_snapshot_horizon(Some(10));
        plan.input_files.push("input.sst".to_string());

        // Act
        let output_names = execute_compaction(&plan, &factory, temp_dir.path(), None)?;

        // Assert
        let output_name = crate::sst::file_name(0, 1, 43);
        assert_eq!(output_names, vec![output_name.clone()]);

        let reader = factory.open(std::path::Path::new(&output_name))?;
        match reader.get_state(b"alpha")? {
            crate::sst::types::KeyState::Tombstone(seq) => assert_eq!(seq, 11),
            other => panic!("expected preserved tombstone, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn should_leave_inputs_untouched_when_compaction_is_cancelled() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempdir()?;
        let fs = std::sync::Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        let input_name = "input.sst";
        let input_path = temp_dir.path().join(input_name);
        let mut writer = factory.create()?;
        writer.add_with_meta(b"key", Some(b"value"), 1, 0, None)?;
        crate::sst::fs::finish_writer_to_path(writer, &input_path)?;
        let input_bytes = std::fs::read(&input_path)?;

        let mut plan = CompactionPlan::new(0, 0, 1).with_output_seq(44);
        plan.input_files.push(input_name.to_string());
        let cancelled = || true;

        // Act
        let error = execute_compaction(&plan, &factory, temp_dir.path(), Some(&cancelled))
            .expect_err("cancelled compaction must return an error");

        // Assert
        assert!(matches!(error, MidgeError::Aborted(_)));
        assert_eq!(std::fs::read(&input_path)?, input_bytes);
        assert!(!temp_dir
            .path()
            .join(crate::sst::file_name(0, 1, 44))
            .exists());
        Ok(())
    }

    #[test]
    fn should_clean_temporary_output_given_compaction_write_failure() -> MidgeResult<()> {
        use std::sync::atomic::{AtomicUsize, Ordering};

        // Arrange: make cancellation happen after input collection and the
        // first streaming check, immediately after output finalization.
        let temp_dir = tempdir()?;
        let fs = std::sync::Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        let input_name = "input.sst";
        let input_path = temp_dir.path().join(input_name);
        let mut writer = factory.create()?;
        writer.add_with_meta(b"key", Some(b"value"), 1, 0, None)?;
        crate::sst::fs::finish_writer_to_path(writer, &input_path)?;
        let input_bytes = std::fs::read(&input_path)?;

        let mut plan = CompactionPlan::new(0, 0, 1).with_output_seq(45);
        plan.input_files.push(input_name.to_string());
        let checks = AtomicUsize::new(0);
        let cancelled_after_finalize = || checks.fetch_add(1, Ordering::SeqCst) >= 3;

        // Act
        let error = execute_compaction(
            &plan,
            &factory,
            temp_dir.path(),
            Some(&cancelled_after_finalize),
        )
        .expect_err("late cancellation must not publish staged output");

        // Assert
        assert!(matches!(error, MidgeError::Aborted(_)));
        assert_eq!(std::fs::read(&input_path)?, input_bytes);
        assert!(
            !temp_dir
                .path()
                .join(crate::sst::file_name(0, 1, 45))
                .exists(),
            "aborted output must not survive for a later manifest publication"
        );
        Ok(())
    }
}
