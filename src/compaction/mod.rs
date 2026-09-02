mod config;
pub mod executor;
pub mod strategy;

pub(crate) use config::{
    OpenCompactionConfig, DEFAULT_COMPACTION_MEMORY_LIMIT, DEFAULT_TARGET_SST_SIZE,
};
pub use strategy::{CompactionPlan, Compactor, LeveledCompactionConfig};

#[cfg(test)]
use crate::common::MidgeError;
use crate::common::MidgeResult;
use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;

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
/// `{cf_id:06}_{level:02}_{generation:020}_{partition:010}.sst`.
pub fn execute_compaction(
    plan: &CompactionPlan,
    sst_factory: &dyn crate::sst::SstFactory,
    output_dir: &Path,
    abort_check: Option<&dyn Fn() -> bool>,
) -> MidgeResult<Vec<String>> {
    // An empty plan is a planner no-op and must not create an unreferenced
    // output. The executor separately rejects non-empty plans whose selected
    // inputs decode to no versions or range tombstones.
    if plan.input_files.is_empty() {
        return Ok(Vec::new());
    }

    // --- 1. Open one lazy raw-version cursor per input SST ------------------
    let budget = crate::common::resource_budget::ResourceBudget::new(plan.compaction_memory_limit);
    let inputs = executor::collect_compaction_stream_inputs(
        sst_factory,
        &plan.input_files,
        &budget,
        abort_check,
    )?;

    // --- 2. K-way merge, deduplicate, and write partitioned outputs --------
    executor::write_partitioned_compaction_outputs(
        sst_factory,
        output_dir,
        plan.cf_id,
        plan.target_level,
        plan.output_seq,
        bounded_partition_target_size(plan.target_sst_size, plan.compaction_memory_limit),
        inputs,
        &budget,
        executor::TombstoneGcPolicy {
            snapshot_horizon: plan.snapshot_horizon,
            point_eligible: plan.point_tombstone_gc_eligible,
            range_eligible: plan.range_tombstone_gc_eligible,
        },
        abort_check,
    )
}

fn bounded_partition_target_size(configured_target: usize, compaction_pool: usize) -> usize {
    // Publication currently hands one complete partition to the cloud adapter.
    // Keep the ordinary rollover point below that hard reserve while leaving
    // half the pool for the final block, metadata, and an indivisible key.
    configured_target.min((compaction_pool / 2).max(1))
}

/// Construct the output filename for a completed compaction.
/// This is stable and predictable for crash recovery and manifest logging.
///
/// File names encode CF, target level, and creation sequence with fixed-width
/// numeric fields so plain directory/object-store listings sort predictably.
#[cfg(test)]
fn output_filename(plan: &CompactionPlan, partition: u32, output_dir: &Path) -> PathBuf {
    output_dir.join(crate::sst::compaction_file_name(
        plan.cf_id,
        plan.target_level,
        plan.output_seq,
        partition,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::traits::SstFactory;
    use tempfile::tempdir;

    #[test]
    fn should_bound_partition_target_below_compaction_upload_reserve() {
        // Arrange
        let configured_target = 512 * 1024 * 1024;
        let compaction_pool = 256 * 1024 * 1024;

        // Act
        let target = bounded_partition_target_size(configured_target, compaction_pool);

        // Assert
        assert_eq!(target, 128 * 1024 * 1024);
        assert_eq!(bounded_partition_target_size(4096, compaction_pool), 4096);
    }

    // ============================================================================
    // Tests for output_filename invariant: stable, zero-padded naming
    // ============================================================================

    #[test]
    fn should_format_output_filename_with_zero_padded_sequence() {
        // Arrange
        let plan = CompactionPlan::new(0, 0, 1).with_output_seq(42);
        let cf_dir = Path::new("cf_00");

        // Act
        let filename = output_filename(&plan, 0, cf_dir);

        // Assert: should be fixed-width and lex-sortable
        assert_eq!(
            filename,
            PathBuf::from("cf_00/000000_01_00000000000000000042_0000000000.sst")
        );
    }

    #[test]
    fn should_format_compaction_output_with_canonical_lex_sortable_sst_name() {
        // Arrange
        let plan = CompactionPlan::new(7, 0, 2).with_output_seq(42);
        let output_dir = Path::new("sst");

        let filename = output_filename(&plan, 0, output_dir);

        // Act
        // Assert
        assert_eq!(
            filename,
            PathBuf::from("sst/000007_02_00000000000000000042_0000000000.sst")
        );
    }

    #[test]
    fn should_preserve_cf_directory_path_when_generating_filename() {
        // Arrange
        let plan = CompactionPlan::new(0, 0, 1).with_output_seq(1);
        let cf_dir = Path::new("data/cf_00");

        // Act
        let filename = output_filename(&plan, 0, cf_dir);

        // Assert: directory structure preserved
        assert_eq!(
            filename,
            PathBuf::from("data/cf_00/000000_01_00000000000000000001_0000000000.sst")
        );
    }

    #[test]
    fn should_handle_large_sequence_numbers_when_formatting() {
        // Arrange
        let plan = CompactionPlan::new(0, 0, 1).with_output_seq(999_999_999);
        let cf_dir = Path::new("cf_01");

        // Act
        let filename = output_filename(&plan, 0, cf_dir);

        // Assert: large numbers formatted correctly
        assert_eq!(
            filename,
            PathBuf::from("cf_01/000000_01_00000000000999999999_0000000000.sst")
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
        let filename_low = output_filename(&plan_low, 0, cf_dir);
        let filename_mid = output_filename(&plan_mid, 0, cf_dir);
        let filename_high = output_filename(&plan_high, 0, cf_dir);

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
            "000000_01_00000000000000000001_0000000000.sst",
            "000000_01_00000000000000000010_0000000000.sst",
            "000000_01_00000000000000000100_0000000000.sst",
        ];
        assert_eq!(filenames, expected);
    }

    #[test]
    fn should_handle_zero_sequence_number_when_formatting() {
        // Arrange
        let plan = CompactionPlan::new(0, 0, 1).with_output_seq(0);
        let cf_dir = Path::new("cf_00");

        // Act
        let filename = output_filename(&plan, 0, cf_dir);

        // Assert
        assert_eq!(
            filename,
            PathBuf::from("cf_00/000000_01_00000000000000000000_0000000000.sst")
        );
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

    #[test]
    fn should_produce_no_output_given_empty_compaction_input() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempdir()?;
        let fs = std::sync::Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        let plan = CompactionPlan::new(0, 0, 1).with_output_seq(40);
        let output_path = temp_dir
            .path()
            .join(crate::sst::compaction_file_name(0, 1, 40, 0));

        // Act
        let output_names = execute_compaction(&plan, &factory, temp_dir.path(), None)?;

        // Assert
        assert!(output_names.is_empty());
        assert!(!output_path.exists());
        Ok(())
    }

    #[test]
    fn should_reject_nonempty_plan_when_selected_sst_contains_no_entries() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempdir()?;
        let fs = std::sync::Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        let input = factory.create()?;
        crate::sst::fs::finish_writer_to_path(input, &temp_dir.path().join("empty-input.sst"))?;
        let mut plan = CompactionPlan::new(0, 0, 1).with_output_seq(40);
        plan.input_files.push("empty-input.sst".to_string());

        // Act
        let result = execute_compaction(&plan, &factory, temp_dir.path(), None);

        // Assert
        assert!(matches!(result, Err(MidgeError::Internal(_))));
        assert!(temp_dir.path().join("empty-input.sst").is_file());
        Ok(())
    }

    #[test]
    fn should_not_drop_unexpired_value_given_compaction_time_before_expiration() -> MidgeResult<()>
    {
        // Arrange
        let temp_dir = tempdir()?;
        let fs = std::sync::Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        let expiration = u64::MAX - 1;
        let mut input_writer = factory.create()?;
        input_writer.add_with_meta(b"live", Some(b"value"), 7, 0, Some(expiration))?;
        crate::sst::fs::finish_writer_to_path(input_writer, &temp_dir.path().join("input.sst"))?;
        let mut plan = CompactionPlan::new(0, 0, 1).with_output_seq(41);
        plan.input_files.push("input.sst".to_string());

        // Act
        let output_names = execute_compaction(&plan, &factory, temp_dir.path(), None)?;
        let reader = factory.open(std::path::Path::new(&output_names[0]))?;
        let state = reader.get_state_at_with_time(b"live", u64::MAX, expiration - 1)?;

        // Assert
        assert!(matches!(
            state,
            crate::sst::types::KeyState::Value(ref value, 7, Some(actual), _)
                if value.as_ref() == b"value" && actual == expiration
        ));
        Ok(())
    }

    #[test]
    fn should_preserve_expired_ttl_metadata_given_compaction_then_mask_at_read_time(
    ) -> MidgeResult<()> {
        // Arrange: compaction is deliberately time-independent. It preserves
        // raw TTL metadata so a snapshot can apply its own read timestamp.
        let temp_dir = tempdir()?;
        let fs = std::sync::Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        let expiration = 100;
        let mut writer = factory.create()?;
        writer.add_with_meta(b"expired", Some(b"value"), 9, 0, Some(expiration))?;
        crate::sst::fs::finish_writer_to_path(writer, &temp_dir.path().join("expired.sst"))?;
        let mut plan = CompactionPlan::new(0, 0, 1).with_output_seq(49);
        plan.input_files.push("expired.sst".to_string());

        // Act
        let output_names = execute_compaction(&plan, &factory, temp_dir.path(), None)?;
        let reader = factory.open(std::path::Path::new(&output_names[0]))?;
        let raw = reader.scan_range_raw_state(None, None)?;
        let visible = reader.get_state_at_with_time(b"expired", u64::MAX, expiration + 1)?;

        // Assert
        assert!(matches!(
            raw.as_slice(),
            [(key, crate::sst::types::KeyState::Value(value, 9, Some(actual), _))]
                if key.as_ref() == b"expired"
                    && value.as_ref() == b"value"
                    && *actual == expiration
        ));
        assert!(matches!(visible, crate::sst::types::KeyState::Tombstone(9)));
        Ok(())
    }

    #[test]
    fn should_preserve_tombstone_visibility_given_overlapping_ssts_when_compacting(
    ) -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempdir()?;
        let fs = std::sync::Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        let mut older = factory.create()?;
        older.add_with_meta(b"same", Some(b"old"), 3, 0, None)?;
        crate::sst::fs::finish_writer_to_path(older, &temp_dir.path().join("older.sst"))?;
        let mut newer = factory.create()?;
        newer.add_with_meta(b"same", None, 4, 2, None)?;
        crate::sst::fs::finish_writer_to_path(newer, &temp_dir.path().join("newer.sst"))?;
        let mut plan = CompactionPlan::new(0, 0, 1).with_output_seq(50);
        plan.input_files
            .extend(["older.sst".to_string(), "newer.sst".to_string()]);

        // Act
        let output_names = execute_compaction(&plan, &factory, temp_dir.path(), None)?;
        let reader = factory.open(std::path::Path::new(&output_names[0]))?;

        // Assert
        assert!(matches!(
            reader.get_state(b"same")?,
            crate::sst::types::KeyState::Tombstone(4)
        ));
        Ok(())
    }

    #[test]
    fn should_preserve_latest_version_given_equal_sequence_versions_when_compacting(
    ) -> MidgeResult<()> {
        // Arrange: equal-sequence entries are safe only when their complete
        // logical payload is identical. Conflicting payloads are rejected by
        // the adjacent fail-closed corruption regression.
        let temp_dir = tempdir()?;
        let fs = std::sync::Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        for name in ["first.sst", "second.sst"] {
            let mut writer = factory.create()?;
            writer.add_with_meta(b"same", Some(b"value"), 7, 0, Some(900))?;
            crate::sst::fs::finish_writer_to_path(writer, &temp_dir.path().join(name))?;
        }
        let mut plan = CompactionPlan::new(0, 0, 1).with_output_seq(51);
        plan.input_files
            .extend(["first.sst".to_string(), "second.sst".to_string()]);

        // Act
        let output_names = execute_compaction(&plan, &factory, temp_dir.path(), None)?;
        let reader = factory.open(std::path::Path::new(&output_names[0]))?;
        let versions = reader.scan_range_raw_state(None, None)?;

        // Assert
        assert!(matches!(
            versions.as_slice(),
            [(key, crate::sst::types::KeyState::Value(value, 7, Some(900), _))]
                if key.as_ref() == b"same" && value.as_ref() == b"value"
        ));
        Ok(())
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
        let output_name = crate::sst::compaction_file_name(0, 1, 42, 0);
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
        let output_name = crate::sst::compaction_file_name(0, 1, 43, 0);
        assert_eq!(output_names, vec![output_name.clone()]);

        let reader = factory.open(std::path::Path::new(&output_name))?;
        match reader.get_state(b"alpha")? {
            crate::sst::types::KeyState::Tombstone(seq) => assert_eq!(seq, 11),
            other => panic!("expected preserved tombstone, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn should_drop_obsolete_point_tombstone_when_compaction_has_bottommost_proof() -> MidgeResult<()>
    {
        // Arrange
        let temp_dir = tempdir()?;
        let fs = std::sync::Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        let mut input_writer = factory.create()?;
        input_writer.add_with_meta(b"alpha", None, 5, 2, None)?;
        input_writer.add_with_meta(b"beta", Some(b"live"), 6, 0, None)?;
        crate::sst::fs::finish_writer_to_path(input_writer, &temp_dir.path().join("input.sst"))?;
        let mut plan = CompactionPlan::new(0, 5, 6)
            .with_output_seq(45)
            .with_tombstone_gc_eligibility(true, false);
        plan.input_files.push("input.sst".to_string());

        // Act
        let output_names = execute_compaction(&plan, &factory, temp_dir.path(), None)?;

        // Assert
        let reader = factory.open(std::path::Path::new(&output_names[0]))?;
        assert!(matches!(
            reader.get_state(b"alpha")?,
            crate::sst::types::KeyState::Absent
        ));
        assert!(matches!(
            reader.get_state(b"beta")?,
            crate::sst::types::KeyState::Value(_, 6, _, _)
        ));
        Ok(())
    }

    #[test]
    fn should_reclaim_obsolete_point_tombstone_when_l0_to_l1_is_key_range_bottommost(
    ) -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempdir()?;
        let fs = std::sync::Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        let input_name = crate::sst::file_name(0, 0, 1);
        let mut input_writer = factory.create()?;
        input_writer.add_with_meta(b"alpha", None, 5, 2, None)?;
        input_writer.add_with_meta(b"beta", Some(b"live"), 6, 0, None)?;
        crate::sst::fs::finish_writer_to_path(input_writer, &temp_dir.path().join(&input_name))?;
        let input_size = std::fs::metadata(temp_dir.path().join(&input_name))?.len();
        let files = vec![crate::metadata::FileMeta {
            name: input_name,
            level: 0,
            size_bytes: input_size,
            cf_id: 0,
            smallest_key: Some(b"alpha".to_vec()),
            largest_key: Some(b"beta".to_vec()),
            ..Default::default()
        }];
        let compactor = Compactor::with_config(LeveledCompactionConfig {
            l0_file_count_threshold: 1,
            ..LeveledCompactionConfig::default()
        });
        let mut plan = compactor
            .pick_compaction(&files, 0)?
            .expect("single-file threshold should plan ordinary L0 to L1 compaction");
        plan.output_seq = 50;

        // Act
        let output_names = execute_compaction(&plan, &factory, temp_dir.path(), None)?;

        // Assert
        assert_eq!(plan.target_level, 1);
        assert!(plan.point_tombstone_gc_eligible);
        let reader = factory.open(std::path::Path::new(&output_names[0]))?;
        assert!(matches!(
            reader.get_state(b"alpha")?,
            crate::sst::types::KeyState::Absent
        ));
        assert!(matches!(
            reader.get_state(b"beta")?,
            crate::sst::types::KeyState::Value(_, 6, _, _)
        ));
        Ok(())
    }

    #[test]
    fn should_retain_point_tombstone_when_selected_target_extends_into_deeper_overlap(
    ) -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempdir()?;
        let fs = std::sync::Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        let source_name = crate::sst::file_name(0, 1, 1);
        let target_name = crate::sst::file_name(0, 2, 2);

        let mut source_writer = factory.create()?;
        source_writer.add_with_meta(b"m", Some(b"live"), 6, 0, None)?;
        crate::sst::fs::finish_writer_to_path(source_writer, &temp_dir.path().join(&source_name))?;
        let mut target_writer = factory.create()?;
        target_writer.add_with_meta(b"a", None, 5, 2, None)?;
        target_writer.add_with_meta(b"z", Some(b"edge"), 4, 0, None)?;
        crate::sst::fs::finish_writer_to_path(target_writer, &temp_dir.path().join(&target_name))?;

        let files = vec![
            crate::metadata::FileMeta {
                name: source_name,
                level: 1,
                size_bytes: 2,
                cf_id: 0,
                smallest_key: Some(b"m".to_vec()),
                largest_key: Some(b"m".to_vec()),
                ..Default::default()
            },
            crate::metadata::FileMeta {
                name: target_name,
                level: 2,
                size_bytes: 1,
                cf_id: 0,
                smallest_key: Some(b"a".to_vec()),
                largest_key: Some(b"z".to_vec()),
                ..Default::default()
            },
            crate::metadata::FileMeta {
                name: crate::sst::file_name(0, 3, 3),
                level: 3,
                size_bytes: 1,
                cf_id: 0,
                smallest_key: Some(b"a".to_vec()),
                largest_key: Some(b"b".to_vec()),
                ..Default::default()
            },
        ];
        let compactor = Compactor::with_config(LeveledCompactionConfig {
            l1_target_size: 1,
            ..LeveledCompactionConfig::default()
        });
        let mut plan = compactor
            .pick_compaction(&files, 0)?
            .expect("eligible inner-level compaction");
        plan.output_seq = 50;

        // Act
        let output_names = execute_compaction(&plan, &factory, temp_dir.path(), None)?;

        // Assert
        assert!(!plan.point_tombstone_gc_eligible);
        let reader = factory.open(std::path::Path::new(&output_names[0]))?;
        assert!(matches!(
            reader.get_state(b"a")?,
            crate::sst::types::KeyState::Tombstone(5)
        ));
        Ok(())
    }

    #[test]
    fn should_retain_tombstones_newer_than_snapshot_horizon_even_with_bottommost_proof(
    ) -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempdir()?;
        let fs = std::sync::Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        let mut input_writer = factory.create()?;
        input_writer.add_with_meta(b"point", None, 11, 2, None)?;
        input_writer.add_range_tombstone(b"a", b"z", 11)?;
        crate::sst::fs::finish_writer_to_path(input_writer, &temp_dir.path().join("input.sst"))?;
        let mut plan = CompactionPlan::new(0, 5, 6)
            .with_output_seq(49)
            .with_snapshot_horizon(Some(10))
            .with_tombstone_gc_eligibility(true, true);
        plan.input_files.push("input.sst".to_string());

        // Act
        let output_names = execute_compaction(&plan, &factory, temp_dir.path(), None)?;

        // Assert
        let reader = factory.open(std::path::Path::new(&output_names[0]))?;
        assert!(matches!(
            reader.get_state(b"point")?,
            crate::sst::types::KeyState::Tombstone(11)
        ));
        assert_eq!(reader.range_tombstones().len(), 1);
        assert_eq!(reader.range_tombstones()[0].seq, 11);
        Ok(())
    }

    #[test]
    fn should_remove_covered_value_before_dropping_obsolete_range_tombstone() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempdir()?;
        let fs = std::sync::Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        let mut value_writer = factory.create()?;
        value_writer.add_with_meta(b"middle", Some(b"deleted"), 1, 0, None)?;
        value_writer.add_with_meta(b"zulu", Some(b"live"), 3, 0, None)?;
        crate::sst::fs::finish_writer_to_path(value_writer, &temp_dir.path().join("values.sst"))?;
        let mut tombstone_writer = factory.create()?;
        tombstone_writer.add_range_tombstone(b"a", b"z", 2)?;
        crate::sst::fs::finish_writer_to_path(
            tombstone_writer,
            &temp_dir.path().join("range-delete.sst"),
        )?;
        let mut plan = CompactionPlan::new(0, 5, 6)
            .with_output_seq(46)
            .with_tombstone_gc_eligibility(true, true);
        plan.input_files
            .extend(["values.sst".to_string(), "range-delete.sst".to_string()]);

        // Act
        let output_names = execute_compaction(&plan, &factory, temp_dir.path(), None)?;

        // Assert
        let reader = factory.open(std::path::Path::new(&output_names[0]))?;
        assert!(matches!(
            reader.get_state(b"middle")?,
            crate::sst::types::KeyState::Absent
        ));
        assert!(reader.range_tombstones().is_empty());
        assert!(matches!(
            reader.get_state(b"zulu")?,
            crate::sst::types::KeyState::Value(_, 3, _, _)
        ));
        Ok(())
    }

    #[test]
    fn should_publish_remove_only_compaction_when_all_entries_are_obsolete_tombstones(
    ) -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempdir()?;
        let fs = std::sync::Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        let mut input_writer = factory.create()?;
        input_writer.add_with_meta(b"deleted", None, 5, 2, None)?;
        crate::sst::fs::finish_writer_to_path(input_writer, &temp_dir.path().join("input.sst"))?;
        let mut plan = CompactionPlan::new(0, 5, 6)
            .with_output_seq(47)
            .with_tombstone_gc_eligibility(true, false);
        plan.input_files.push("input.sst".to_string());

        // Act
        let output_names = execute_compaction(&plan, &factory, temp_dir.path(), None)?;

        // Assert
        assert!(output_names.is_empty());
        assert!(!temp_dir
            .path()
            .join(crate::sst::file_name(0, 6, 47))
            .exists());
        Ok(())
    }

    #[test]
    fn should_fail_compaction_when_equal_key_and_sequence_have_conflicting_values(
    ) -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempdir()?;
        let fs = std::sync::Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        for (name, value) in [
            ("first.sst", b"first".as_slice()),
            ("second.sst", b"second"),
        ] {
            let mut writer = factory.create()?;
            writer.add_with_meta(b"same", Some(value), 7, 0, None)?;
            crate::sst::fs::finish_writer_to_path(writer, &temp_dir.path().join(name))?;
        }
        let mut plan = CompactionPlan::new(0, 0, 1).with_output_seq(48);
        plan.input_files
            .extend(["first.sst".to_string(), "second.sst".to_string()]);

        // Act
        let error = execute_compaction(&plan, &factory, temp_dir.path(), None)
            .expect_err("conflicting logical versions must not use input order as authority");

        // Assert
        assert!(matches!(error, MidgeError::Corruption(_)));
        assert!(!temp_dir
            .path()
            .join(crate::sst::compaction_file_name(0, 1, 48, 0))
            .exists());
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
            .join(crate::sst::compaction_file_name(0, 1, 44, 0))
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
                .join(crate::sst::compaction_file_name(0, 1, 45, 0))
                .exists(),
            "aborted output must not survive for a later manifest publication"
        );
        Ok(())
    }

    #[test]
    fn should_stream_compaction_finalization_when_finish_bytes_is_rejected() -> MidgeResult<()> {
        // Arrange
        struct RejectFinishBytesWriter {
            inner: Box<dyn crate::sst::traits::DynSstWriter>,
        }

        impl crate::sst::traits::DynSstWriter for RejectFinishBytesWriter {
            fn add(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()> {
                self.inner.add(key, value)
            }

            fn add_with_meta(
                &mut self,
                key: &[u8],
                value: Option<&[u8]>,
                seq: u64,
                op_type: u8,
                expiration: Option<u64>,
            ) -> MidgeResult<()> {
                self.inner
                    .add_with_meta(key, value, seq, op_type, expiration)
            }

            fn add_sorted_with_meta(
                &mut self,
                key: &[u8],
                value: Option<&[u8]>,
                seq: u64,
                op_type: u8,
                expiration: Option<u64>,
            ) -> MidgeResult<()> {
                self.inner
                    .add_sorted_with_meta(key, value, seq, op_type, expiration)
            }

            fn add_range_tombstone(
                &mut self,
                start: &[u8],
                end: &[u8],
                seq: u64,
            ) -> MidgeResult<()> {
                self.inner.add_range_tombstone(start, end, seq)
            }

            fn finish_to_path(self: Box<Self>, path: &Path) -> MidgeResult<()> {
                self.inner.finish_to_path(path)
            }

            fn finish_bytes(self: Box<Self>) -> MidgeResult<Vec<u8>> {
                Err(MidgeError::Internal(
                    "finish_bytes must not be used by compaction".to_string(),
                ))
            }
        }

        struct RejectFinishBytesFactory {
            inner: crate::sst::FsSstFactoryIo,
        }

        impl crate::sst::traits::SstFactory for RejectFinishBytesFactory {
            fn create(&self) -> MidgeResult<Box<dyn crate::sst::traits::DynSstWriter>> {
                Ok(Box::new(RejectFinishBytesWriter {
                    inner: self.inner.create()?,
                }))
            }

            fn open(&self, path: &Path) -> MidgeResult<Box<dyn crate::sst::traits::SstReaderExt>> {
                self.inner.open(path)
            }
        }

        let temp_dir = tempdir()?;
        let fs = std::sync::Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let base_factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        let mut input = base_factory.create()?;
        input.add_with_meta(b"key", Some(b"value"), 7, 0, None)?;
        crate::sst::fs::finish_writer_to_path(input, &temp_dir.path().join("input.sst"))?;
        let factory = RejectFinishBytesFactory {
            inner: base_factory,
        };
        let mut plan = CompactionPlan::new(0, 0, 1).with_output_seq(46);
        plan.input_files.push("input.sst".to_string());

        // Act
        let outputs = execute_compaction(&plan, &factory, temp_dir.path(), None)?;

        // Assert
        assert_eq!(outputs.len(), 1);
        let reader = factory.open(Path::new(&outputs[0]))?;
        assert_eq!(reader.get(b"key")?.as_deref(), Some(b"value".as_slice()));
        Ok(())
    }

    #[test]
    fn should_partition_compaction_output_at_soft_target_between_user_keys() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempdir()?;
        let fs = std::sync::Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        let mut input = factory.create()?;
        let value = vec![b'v'; 512];
        for index in 0..96u64 {
            let key = format!("key-{index:04}");
            input.add_with_meta(key.as_bytes(), Some(&value), index + 1, 0, None)?;
        }
        crate::sst::fs::finish_writer_to_path(input, &temp_dir.path().join("large-input.sst"))?;
        let mut plan = CompactionPlan::new(7, 0, 1).with_output_seq(52);
        plan.target_sst_size = 4096;
        plan.input_files.push("large-input.sst".to_string());

        // Act
        let outputs = execute_compaction(&plan, &factory, temp_dir.path(), None)?;

        // Assert
        assert!(
            outputs.len() >= 4,
            "input larger than three targets must partition"
        );
        let mut observed_keys = Vec::new();
        for (partition, output) in outputs.iter().enumerate() {
            assert_eq!(
                output,
                &crate::sst::compaction_file_name(
                    7,
                    1,
                    52,
                    u32::try_from(partition).expect("partition ordinal fits")
                )
            );
            let size = std::fs::metadata(temp_dir.path().join(output))?.len();
            let allowance =
                u64::try_from(value.len() + 4096 + 16 * 1024).expect("allowance fits in u64");
            assert!(
                size <= u64::try_from(plan.target_sst_size).unwrap_or(u64::MAX) + allowance,
                "partition {partition} size {size} exceeded target plus entry/block/metadata allowance"
            );
            observed_keys.extend(
                factory
                    .open(Path::new(output))?
                    .scan_range_raw_state(None, None)?
                    .into_iter()
                    .map(|(key, _)| key.to_vec()),
            );
        }
        assert_eq!(observed_keys.len(), 96);
        assert!(observed_keys.windows(2).all(|pair| pair[0] < pair[1]));
        Ok(())
    }

    #[test]
    fn should_include_filter_metadata_when_partitioning_many_small_keys() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempdir()?;
        let fs = std::sync::Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        let mut input = factory.create()?;
        for index in 0..50_000u64 {
            let key = format!("structured-key-{index:020}");
            input.add_with_meta(key.as_bytes(), Some(b"v"), index + 1, 0, None)?;
        }
        crate::sst::fs::finish_writer_to_path(input, &temp_dir.path().join("small-keys.sst"))?;
        let mut plan = CompactionPlan::new(8, 0, 1).with_output_seq(53);
        plan.target_sst_size = 64 * 1024;
        plan.input_files.push("small-keys.sst".to_string());

        // Act
        let outputs = execute_compaction(&plan, &factory, temp_dir.path(), None)?;

        // Assert
        assert!(outputs.len() > 1);
        let allowance = u64::try_from(4096 + 16 * 1024).expect("allowance fits in u64");
        for output in outputs {
            let size = std::fs::metadata(temp_dir.path().join(output))?.len();
            assert!(
                size <= u64::try_from(plan.target_sst_size).unwrap_or(u64::MAX) + allowance,
                "partition size {size} exceeded target plus block/metadata allowance"
            );
        }
        Ok(())
    }

    #[test]
    fn should_fragment_range_tombstone_at_compaction_partition_boundaries() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempdir()?;
        let fs = std::sync::Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        let mut input = factory.create()?;
        let value = vec![b'v'; 512];
        for index in 0..96u64 {
            let key = format!("key-{index:04}");
            input.add_with_meta(key.as_bytes(), Some(&value), index + 1, 0, None)?;
        }
        input.add_range_tombstone(b"key-0010", b"key-0080", 200)?;
        crate::sst::fs::finish_writer_to_path(input, &temp_dir.path().join("range-input.sst"))?;
        let mut plan = CompactionPlan::new(0, 0, 1).with_output_seq(53);
        plan.target_sst_size = 4096;
        plan.input_files.push("range-input.sst".to_string());

        // Act
        let outputs = execute_compaction(&plan, &factory, temp_dir.path(), None)?;
        let mut fragments = outputs
            .iter()
            .flat_map(|output| {
                factory
                    .open(Path::new(output))
                    .expect("open partition")
                    .range_tombstones()
            })
            .collect::<Vec<_>>();
        fragments.sort_by(|left, right| left.start.cmp(&right.start));

        // Assert
        assert!(fragments.len() > 1);
        assert_eq!(
            fragments.first().map(|item| item.start.as_slice()),
            Some(b"key-0010".as_slice())
        );
        assert_eq!(
            fragments.last().map(|item| item.end.as_slice()),
            Some(b"key-0080".as_slice())
        );
        assert!(fragments
            .windows(2)
            .all(|pair| pair[0].end == pair[1].start));
        assert!(fragments.iter().all(|fragment| fragment.seq == 200));
        Ok(())
    }

    #[test]
    fn should_include_range_tombstone_metadata_in_partition_rollover() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempdir()?;
        let fs = std::sync::Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096)
            .with_compression_policy(crate::sst::compression::CompressionPolicy::None);
        let mut input = factory.create()?;
        let value = vec![b'v'; 256];
        let keys = (0..512u64)
            .map(|index| format!("key-{index:04}-{index:0112x}").into_bytes())
            .collect::<Vec<_>>();
        for (index, key) in keys.iter().enumerate() {
            input.add_with_meta(
                key,
                Some(&value),
                u64::try_from(index).expect("index fits") + 1,
                0,
                None,
            )?;
        }
        for index in 0..256usize {
            input.add_range_tombstone(
                &keys[index],
                &keys[index + 256],
                u64::try_from(index).expect("index fits") + 1_000,
            )?;
        }
        crate::sst::fs::finish_writer_to_path(
            input,
            &temp_dir.path().join("metadata-heavy-input.sst"),
        )?;
        let mut plan = CompactionPlan::new(0, 0, 1).with_output_seq(54);
        plan.target_sst_size = 64 * 1024;
        plan.input_files
            .push("metadata-heavy-input.sst".to_string());

        // Act
        let outputs = execute_compaction(&plan, &factory, temp_dir.path(), None)?;

        // Assert
        assert!(outputs.len() > 1);
        let allowance =
            u64::try_from(value.len() + 4096 + 16 * 1024).expect("partition allowance fits in u64");
        for output in outputs {
            let size = std::fs::metadata(temp_dir.path().join(output))?.len();
            assert!(
                size <= u64::try_from(plan.target_sst_size).unwrap_or(u64::MAX) + allowance,
                "partition size {size} exceeded target plus indivisible entry/block allowance"
            );
        }
        Ok(())
    }

    #[test]
    fn should_stream_tombstone_only_compaction_output() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempdir()?;
        let fs = std::sync::Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        let mut input = factory.create()?;
        input.add_range_tombstone(b"a", b"z", 17)?;
        crate::sst::fs::finish_writer_to_path(
            input,
            &temp_dir.path().join("tombstone-only-input.sst"),
        )?;
        let mut plan = CompactionPlan::new(0, 0, 1).with_output_seq(54);
        plan.input_files
            .push("tombstone-only-input.sst".to_string());

        // Act
        let outputs = execute_compaction(&plan, &factory, temp_dir.path(), None)?;

        // Assert
        assert_eq!(outputs.len(), 1);
        let reader = factory.open(Path::new(&outputs[0]))?;
        assert_eq!(reader.range_tombstones().len(), 1);
        assert_eq!(
            reader.range_tombstones()[0],
            crate::sst::types::RangeTombstone::new(b"a".to_vec(), b"z".to_vec(), 17)
        );
        Ok(())
    }

    #[test]
    fn should_never_split_equal_user_keys_across_compaction_partitions() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempdir()?;
        let fs = std::sync::Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        let mut input = factory.create()?;
        let large_value = vec![b'x'; 8 * 1024];
        input.add_with_meta(b"a", Some(&large_value), 1, 0, None)?;
        for sequence in 2..=33 {
            input.add_with_meta(
                b"same",
                Some(format!("same-{sequence:02}").as_bytes()),
                sequence,
                0,
                None,
            )?;
        }
        input.add_with_meta(b"z", Some(&large_value), 34, 0, None)?;
        crate::sst::fs::finish_writer_to_path(input, &temp_dir.path().join("same-key.sst"))?;
        let mut plan = CompactionPlan::new(0, 0, 1).with_output_seq(55);
        plan.target_sst_size = 4096;
        plan.input_files.push("same-key.sst".to_string());

        // Act
        let outputs = execute_compaction(&plan, &factory, temp_dir.path(), None)?;
        let keys_by_partition: Vec<Vec<Vec<u8>>> = outputs
            .iter()
            .map(|output| {
                factory
                    .open(Path::new(output))
                    .expect("open partition")
                    .scan_range_raw_state(None, None)
                    .expect("scan partition")
                    .into_iter()
                    .map(|(key, _)| key.to_vec())
                    .collect()
            })
            .collect();

        // Assert
        assert!(outputs.len() >= 2);
        assert_eq!(
            keys_by_partition
                .iter()
                .filter(|keys| keys.iter().any(|key| key == b"same"))
                .count(),
            1
        );
        let same_state = outputs
            .iter()
            .find_map(|output| {
                factory
                    .open(Path::new(output))
                    .expect("open partition")
                    .get_state(b"same")
                    .ok()
                    .filter(|state| !matches!(state, crate::sst::types::KeyState::Absent))
            })
            .expect("latest same-key version");
        assert!(matches!(
            same_state,
            crate::sst::types::KeyState::Value(value, 33, None, 0)
                if value.as_ref() == b"same-33"
        ));
        Ok(())
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn should_remove_every_completed_partition_when_cancelled_during_rollover() -> MidgeResult<()> {
        // Arrange
        struct CountingWriter {
            inner: Box<dyn crate::sst::traits::DynSstWriter>,
            finalized: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        }

        impl crate::sst::traits::DynSstWriter for CountingWriter {
            fn estimated_size_bytes(&self) -> usize {
                self.inner.estimated_size_bytes()
            }

            fn add(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()> {
                self.inner.add(key, value)
            }

            fn add_with_meta(
                &mut self,
                key: &[u8],
                value: Option<&[u8]>,
                seq: u64,
                op_type: u8,
                expiration: Option<u64>,
            ) -> MidgeResult<()> {
                self.inner
                    .add_with_meta(key, value, seq, op_type, expiration)
            }

            fn add_sorted_with_meta(
                &mut self,
                key: &[u8],
                value: Option<&[u8]>,
                seq: u64,
                op_type: u8,
                expiration: Option<u64>,
            ) -> MidgeResult<()> {
                self.inner
                    .add_sorted_with_meta(key, value, seq, op_type, expiration)
            }

            fn add_range_tombstone(
                &mut self,
                start: &[u8],
                end: &[u8],
                seq: u64,
            ) -> MidgeResult<()> {
                self.inner.add_range_tombstone(start, end, seq)
            }

            fn finish_to_path(self: Box<Self>, path: &Path) -> MidgeResult<()> {
                let Self { inner, finalized } = *self;
                inner.finish_to_path(path)?;
                finalized.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }

            fn finish_bytes(self: Box<Self>) -> MidgeResult<Vec<u8>> {
                self.inner.finish_bytes()
            }
        }

        struct CountingFactory {
            inner: crate::sst::FsSstFactoryIo,
            finalized: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        }

        impl crate::sst::traits::SstFactory for CountingFactory {
            fn create(&self) -> MidgeResult<Box<dyn crate::sst::traits::DynSstWriter>> {
                Ok(Box::new(CountingWriter {
                    inner: self.inner.create()?,
                    finalized: std::sync::Arc::clone(&self.finalized),
                }))
            }

            fn create_for_compaction(
                &self,
                budget: crate::common::resource_budget::ResourceBudget,
            ) -> MidgeResult<Box<dyn crate::sst::traits::DynSstWriter>> {
                Ok(Box::new(CountingWriter {
                    inner: self.inner.create_for_compaction(budget)?,
                    finalized: std::sync::Arc::clone(&self.finalized),
                }))
            }

            fn open(&self, path: &Path) -> MidgeResult<Box<dyn crate::sst::traits::SstReaderExt>> {
                self.inner.open(path)
            }
        }

        let temp_dir = tempdir()?;
        let fs = std::sync::Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let base_factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        let mut input = base_factory.create()?;
        let value = vec![b'v'; 512];
        for index in 0..96u64 {
            input.add_with_meta(
                format!("key-{index:04}").as_bytes(),
                Some(&value),
                index + 1,
                0,
                None,
            )?;
        }
        let input_path = temp_dir.path().join("cancel-input.sst");
        crate::sst::fs::finish_writer_to_path(input, &input_path)?;
        let input_bytes = std::fs::read(&input_path)?;
        let finalized = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let factory = CountingFactory {
            inner: base_factory,
            finalized: std::sync::Arc::clone(&finalized),
        };
        let mut plan = CompactionPlan::new(0, 0, 1).with_output_seq(56);
        plan.target_sst_size = 4096;
        plan.input_files.push("cancel-input.sst".to_string());
        let abort = || finalized.load(std::sync::atomic::Ordering::SeqCst) > 0;

        // Act
        let error = execute_compaction(&plan, &factory, temp_dir.path(), Some(&abort))
            .expect_err("cancellation after first rollover must reject the output set");

        // Assert
        assert!(matches!(error, MidgeError::Aborted(_)));
        assert_eq!(finalized.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(std::fs::read(&input_path)?, input_bytes);
        assert!(
            (0..4).all(|partition| !temp_dir
                .path()
                .join(crate::sst::compaction_file_name(0, 1, 56, partition))
                .exists()),
            "cancelled output set must leave no authoritative-looking partition"
        );
        Ok(())
    }

    #[test]
    fn should_keep_recorded_compaction_bytes_within_pool_for_aggregate_inputs_larger_than_pool(
    ) -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempdir()?;
        let fs = std::sync::Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        let value = vec![b'x'; 512];
        let mut input_names = Vec::new();
        let mut logical_input_bytes = 0usize;
        for input_index in 0..4u64 {
            let name = format!("resource-input-{input_index}.sst");
            let mut writer = factory.create()?;
            for key_index in 0..384u64 {
                let key = format!("{input_index}-key-{key_index:04}");
                logical_input_bytes = logical_input_bytes
                    .saturating_add(key.len())
                    .saturating_add(value.len());
                writer.add_with_meta(
                    key.as_bytes(),
                    Some(&value),
                    input_index.saturating_mul(1000).saturating_add(key_index),
                    0,
                    None,
                )?;
            }
            crate::sst::fs::finish_writer_to_path(writer, &temp_dir.path().join(&name))?;
            input_names.push(name);
        }
        let pool_limit = 128usize * 1024;
        let target_sst_size = 4096;
        assert!(logical_input_bytes > pool_limit.saturating_mul(4));
        assert!(logical_input_bytes > target_sst_size);
        let budget = crate::common::resource_budget::ResourceBudget::new(pool_limit);
        let inputs =
            executor::collect_compaction_stream_inputs(&factory, &input_names, &budget, None)?;

        // Act
        let outputs = executor::write_partitioned_compaction_outputs(
            &factory,
            temp_dir.path(),
            0,
            1,
            54,
            target_sst_size,
            inputs,
            &budget,
            executor::TombstoneGcPolicy {
                snapshot_horizon: None,
                point_eligible: false,
                range_eligible: false,
            },
            None,
        )?;

        // Assert
        assert!(outputs.len() > 1);
        assert!(budget.peak() <= pool_limit);
        Ok(())
    }
}
