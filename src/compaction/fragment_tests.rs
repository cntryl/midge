use super::*;
use crate::sst::traits::{RawSstVersion, SstFactory};
use crate::types::{EntryType, RangeTombstone};
use std::sync::Arc;

fn fragment_fixture() -> (
    Arc<crate::io::MockFs>,
    crate::sst::FsSstFactoryIo,
    CompactionPlan,
) {
    let fs = Arc::new(crate::io::MockFs::new());
    let factory = crate::sst::FsSstFactoryIo::new(fs.clone(), 4096);
    let mut plan = CompactionPlan::new(0, 2, 2).with_output_seq(10);
    plan.purpose = strategy::CompactionPurpose::Fragment {
        split_key: b"m".to_vec(),
    };
    plan.add_test_source("source.sst");
    plan.target_sst_size = usize::MAX;
    (fs, factory, plan)
}

fn read_fragment_versions(
    factory: &dyn SstFactory,
    names: &[String],
) -> MidgeResult<Vec<RawSstVersion>> {
    let mut versions = Vec::new();
    for name in names {
        versions.extend(
            factory
                .open(Path::new(name))?
                .raw_version_cursor(None, None)?
                .collect::<MidgeResult<Vec<_>>>()?,
        );
    }
    Ok(versions)
}

fn fragment_metadata(
    factory: &dyn SstFactory,
    names: &[String],
) -> MidgeResult<Vec<crate::metadata::FileMeta>> {
    names
        .iter()
        .map(|name| {
            let summary = crate::sst::fs::SstFileIo::summarize_with_fs_for_compaction(
                name,
                factory.output_fs(),
                crate::common::resource_budget::ResourceBudget::new(
                    DEFAULT_COMPACTION_MEMORY_LIMIT,
                ),
            )?;
            Ok(crate::metadata::FileMeta {
                name: name.clone(),
                level: 2,
                size_bytes: summary.size_bytes,
                smallest_key: Some(summary.smallest_key),
                largest_key: Some(summary.largest_key),
                key_bounds_complete: true,
                ..Default::default()
            })
        })
        .collect()
}

#[test]
fn should_preserve_source_history_when_fragmenting_across_a_range() -> MidgeResult<()> {
    // Arrange
    let (fs, factory, mut plan) = fragment_fixture();
    let mut writer = factory.create()?;
    for (key, value, sequence, kind, expiry) in [
        (
            b"a",
            Some(b"new".as_slice()),
            8,
            EntryType::Put,
            Some(20_000),
        ),
        (
            b"a",
            Some(b"old".as_slice()),
            3,
            EntryType::Put,
            Some(10_000),
        ),
        (b"b", None, 6, EntryType::Delete, None),
        (b"b", Some(b"masked".as_slice()), 2, EntryType::Put, None),
        (b"z", Some(b"right".as_slice()), 9, EntryType::Put, None),
    ] {
        writer.add_sorted_with_meta(key, value, sequence, kind, expiry)?;
    }
    writer.add_range_tombstone(b"b", b"y", 7)?;
    writer.finish_to_path(Path::new("source.sst"))?;
    let original = read_fragment_versions(&factory, &plan.source_files)?;
    plan.snapshot_horizon = Some(4);

    // Act
    let outputs = execute_compaction(&plan, &factory, Path::new(""), None)?;

    // Assert
    assert_eq!(
        outputs.len(),
        2,
        "the requested interior cut must be honored"
    );
    assert_eq!(read_fragment_versions(&factory, &outputs)?, original);
    assert_eq!(
        factory.open(Path::new(&outputs[0]))?.range_tombstones(),
        vec![RangeTombstone {
            start: b"b".to_vec(),
            end: b"m".to_vec(),
            seq: 7
        }]
    );
    assert_eq!(
        factory.open(Path::new(&outputs[1]))?.range_tombstones(),
        vec![RangeTombstone {
            start: b"m".to_vec(),
            end: b"y".to_vec(),
            seq: 7
        }]
    );
    assert!(fs.get_file("source.sst").is_some());
    Ok(())
}

#[test]
fn should_cut_a_range_only_source_when_the_boundary_has_no_input_event() -> MidgeResult<()> {
    // Arrange
    let (_fs, factory, plan) = fragment_fixture();
    let mut writer = factory.create()?;
    writer.add_range_tombstone(b"a", b"z", 11)?;
    writer.finish_to_path(Path::new("source.sst"))?;

    // Act
    let outputs = execute_compaction(&plan, &factory, Path::new(""), None)?;

    // Assert
    assert_eq!(outputs.len(), 2);
    for (name, start, end) in [(&outputs[0], b"a", b"m"), (&outputs[1], b"m", b"z")] {
        assert_eq!(
            factory.open(Path::new(name))?.range_tombstones(),
            vec![RangeTombstone {
                start: start.to_vec(),
                end: end.to_vec(),
                seq: 11
            }]
        );
    }
    Ok(())
}

#[test]
fn should_cleanup_unpublished_fragments_when_staging_fails() -> MidgeResult<()> {
    // Arrange
    let (fs, factory, plan) = fragment_fixture();
    let mut writer = factory.create()?;
    writer.add_with_meta(b"a", Some(b"left"), 1, EntryType::Put, None)?;
    writer.add_with_meta(b"z", Some(b"right"), 2, EntryType::Put, None)?;
    writer.finish_to_path(Path::new("source.sst"))?;
    let budget = crate::common::resource_budget::ResourceBudget::new(plan.compaction_memory_limit);
    for fail_on in [1, 2] {
        let calls = std::cell::Cell::new(0);
        let sink = |_name: &str,
                    _path: &Path,
                    _budget: &crate::common::resource_budget::ResourceBudget| {
            calls.set(calls.get() + 1);
            if calls.get() == fail_on {
                Err(MidgeError::Aborted(
                    "injected fragment staging failure".into(),
                ))
            } else {
                Ok(())
            }
        };

        // Act
        let result = execute_compaction_at_target(
            &plan,
            CompactionResources {
                target_sst_size: plan.target_sst_size,
                source_fan_in: 4,
                budget: budget.clone(),
            },
            &factory,
            Path::new(""),
            None,
            Some(&sink),
            None,
        );

        // Assert
        assert!(matches!(result, Err(MidgeError::Aborted(_))));
        assert_eq!(calls.get(), fail_on);
        assert!(fs.get_file("source.sst").is_some());
        for ordinal in 0..2 {
            assert!(fs
                .get_file(&crate::cloud_layout::compaction_file_name(
                    0, 2, 10, ordinal
                ))
                .is_none());
        }
        assert_eq!(budget.used(), 0);
    }
    Ok(())
}

#[test]
fn should_reject_fragmentation_when_the_plan_shape_is_invalid() {
    // Arrange
    let (_fs, factory, plan) = fragment_fixture();
    let mut invalid = Vec::new();
    for level in [0, 3] {
        let mut candidate = plan.clone();
        candidate.source_level = level;
        invalid.push(candidate);
    }
    for purpose in [
        strategy::CompactionPurpose::Merge,
        strategy::CompactionPurpose::Repair,
    ] {
        let mut candidate = plan.clone();
        candidate.purpose = purpose;
        candidate.target_level = 3;
        if candidate.purpose == strategy::CompactionPurpose::Merge {
            candidate.target_level = candidate.source_level;
        }
        invalid.push(candidate);
    }
    let mut empty = plan.clone();
    empty.source_files.clear();
    empty.input_files.clear();
    invalid.push(empty);
    let mut multiple = plan.clone();
    multiple.add_test_source("other.sst");
    invalid.push(multiple);
    let mut targeted = plan.clone();
    targeted.target_files.push("target.sst".into());
    targeted.input_files =
        strategy::combined_input_files(&targeted.source_files, &targeted.target_files);
    invalid.push(targeted);
    for range_gc in [false, true] {
        let mut candidate = plan.clone();
        candidate.point_tombstone_gc_eligible = !range_gc;
        candidate.range_tombstone_gc_eligible = range_gc;
        invalid.push(candidate);
    }

    for candidate in invalid {
        // Act
        let result = execute_compaction(&candidate, &factory, Path::new(""), None);

        // Assert
        assert!(matches!(result, Err(MidgeError::Corruption(_))));
    }
}

#[test]
fn should_retain_source_when_the_fragment_boundary_is_not_strictly_interior() -> MidgeResult<()> {
    // Arrange
    let (fs, factory, mut plan) = fragment_fixture();
    let mut writer = factory.create()?;
    writer.add_with_meta(b"a", Some(b"left"), 1, EntryType::Put, None)?;
    writer.add_with_meta(b"z", Some(b"right"), 2, EntryType::Put, None)?;
    writer.finish_to_path(Path::new("source.sst"))?;

    for split_key in [b"".to_vec(), b"a".to_vec(), b"z".to_vec(), b"zz".to_vec()] {
        plan.purpose = strategy::CompactionPurpose::Fragment { split_key };
        // Act
        let result = execute_compaction(&plan, &factory, Path::new(""), None);

        // Assert
        assert!(matches!(result, Err(MidgeError::Corruption(_))));
        assert!(fs.get_file("source.sst").is_some());
        for ordinal in 0..2 {
            assert!(fs
                .get_file(&crate::cloud_layout::compaction_file_name(
                    0, 2, 10, ordinal
                ))
                .is_none());
        }
    }
    Ok(())
}

#[test]
fn should_reject_shadowed_conflicts_when_fragmenting_raw_history() -> MidgeResult<()> {
    // Arrange
    let (fs, factory, plan) = fragment_fixture();
    let mut writer = factory.create()?;
    for (key, value, sequence) in [
        (b"a", b"left".as_slice(), 1),
        (b"z", b"new", 9),
        (b"z", b"old-one", 3),
        (b"z", b"old-two", 3),
    ] {
        writer.add_sorted_with_meta(key, Some(value), sequence, EntryType::Put, None)?;
    }
    writer.finish_to_path(Path::new("source.sst"))?;

    // Act
    let result = execute_compaction(&plan, &factory, Path::new(""), None);

    // Assert
    assert!(matches!(result, Err(MidgeError::Corruption(_))));
    assert!(fs.get_file("source.sst").is_some());
    for ordinal in 0..2 {
        assert!(fs
            .get_file(&crate::cloud_layout::compaction_file_name(
                0, 2, 10, ordinal
            ))
            .is_none());
    }
    Ok(())
}

#[test]
fn should_keep_fragment_ordinals_contiguous_when_a_soft_roll_precedes_an_empty_cut(
) -> MidgeResult<()> {
    // Arrange
    let (_fs, factory, mut plan) = fragment_fixture();
    plan.target_sst_size = 1;
    let mut writer = factory.create()?;
    writer.add_range_tombstone(b"a", b"b", 1)?;
    writer.add_with_meta(b"z", Some(b"right"), 2, EntryType::Put, None)?;
    writer.finish_to_path(Path::new("source.sst"))?;

    // Act
    let outputs = execute_compaction(&plan, &factory, Path::new(""), None)?;

    // Assert
    assert_eq!(
        outputs,
        (0..2)
            .map(|ordinal| crate::cloud_layout::compaction_file_name(0, 2, 10, ordinal))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        read_fragment_versions(&factory, &outputs)?,
        read_fragment_versions(&factory, &plan.source_files)?
    );
    assert_eq!(
        factory.open(Path::new(&outputs[0]))?.range_tombstones(),
        vec![RangeTombstone {
            start: b"a".to_vec(),
            end: b"b".to_vec(),
            seq: 1
        }]
    );
    assert_eq!(
        factory.open(Path::new(&outputs[1]))?.range_tombstones(),
        [] as [RangeTombstone; 0]
    );
    Ok(())
}

#[test]
fn should_retain_source_when_the_fragment_boundary_exceeds_its_memory_budget() -> MidgeResult<()> {
    // Arrange
    let (fs, factory, mut plan) = fragment_fixture();
    plan.purpose = strategy::CompactionPurpose::Fragment {
        split_key: vec![b'm'; 128 * 1024],
    };
    let mut writer = factory.create()?;
    writer.add_with_meta(b"a", Some(b"left"), 1, EntryType::Put, None)?;
    writer.add_with_meta(b"z", Some(b"right"), 2, EntryType::Put, None)?;
    writer.finish_to_path(Path::new("source.sst"))?;
    let budget = crate::common::resource_budget::ResourceBudget::new(64 * 1024);

    // Act
    let result = execute_compaction_at_target(
        &plan,
        CompactionResources {
            target_sst_size: plan.target_sst_size,
            source_fan_in: 4,
            budget: budget.clone(),
        },
        &factory,
        Path::new(""),
        None,
        None,
        None,
    );

    // Assert
    assert!(matches!(result, Err(MidgeError::ResourceLimit(message))
        if message.contains("fragment split boundary")));
    assert!(fs.get_file("source.sst").is_some());
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[test]
fn should_cleanup_fragments_when_a_raw_key_group_exceeds_the_staging_limit() -> MidgeResult<()> {
    // Arrange
    let (fs, factory, plan) = fragment_fixture();
    let mut writer = factory.create()?;
    writer.add_with_meta(b"a", Some(b"left"), 1, EntryType::Put, None)?;
    writer.add_with_meta(b"z", Some(&vec![7; 128 * 1024]), 2, EntryType::Put, None)?;
    writer.finish_to_path(Path::new("source.sst"))?;
    let budget = crate::common::resource_budget::ResourceBudget::new(plan.compaction_memory_limit);

    // Act
    let result = execute_compaction_at_target(
        &plan,
        CompactionResources {
            target_sst_size: plan.target_sst_size,
            source_fan_in: 4,
            budget: budget.clone(),
        },
        &factory,
        Path::new(""),
        None,
        None,
        Some(64 * 1024),
    );

    // Assert
    assert!(matches!(result, Err(MidgeError::ResourceLimit(_))));
    assert!(fs.get_file("source.sst").is_some());
    for ordinal in 0..2 {
        assert!(fs
            .get_file(&crate::cloud_layout::compaction_file_name(
                0, 2, 10, ordinal
            ))
            .is_none());
    }
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[test]
fn should_assign_endpoint_events_to_the_right_fragment_when_cutting_at_a_range_boundary(
) -> MidgeResult<()> {
    // Arrange
    let (_fs, factory, mut plan) = fragment_fixture();
    plan.target_sst_size = 1;
    let mut writer = factory.create()?;
    writer.add_range_tombstone(b"a", b"m", 1)?;
    writer.add_range_tombstone(b"m", b"z", 2)?;
    writer.add_sorted_with_meta(b"m", Some(b"new"), 4, EntryType::Put, Some(99))?;
    writer.add_sorted_with_meta(b"m", None, 3, EntryType::Delete, None)?;
    writer.finish_to_path(Path::new("source.sst"))?;

    // Act
    let outputs = execute_compaction(&plan, &factory, Path::new(""), None)?;

    // Assert
    assert_eq!(outputs.len(), 2);
    assert_eq!(
        read_fragment_versions(&factory, &outputs[..1])?,
        [] as [RawSstVersion; 0]
    );
    assert_eq!(
        read_fragment_versions(&factory, &outputs[1..])?,
        read_fragment_versions(&factory, &plan.source_files)?
    );
    for (name, start, end, seq) in [(&outputs[0], b"a", b"m", 1), (&outputs[1], b"m", b"z", 2)] {
        assert_eq!(
            factory.open(Path::new(name))?.range_tombstones(),
            vec![RangeTombstone {
                start: start.to_vec(),
                end: end.to_vec(),
                seq
            }]
        );
    }
    Ok(())
}

#[test]
fn should_keep_the_final_key_with_its_ending_range_when_fragmenting() -> MidgeResult<()> {
    // Arrange
    let (_fs, factory, mut plan) = fragment_fixture();
    plan.target_sst_size = 1;
    let mut writer = factory.create()?;
    for (key, sequence) in [(b"a", 1), (b"n", 2), (b"z", 3)] {
        writer.add_with_meta(key, Some(b"value"), sequence, EntryType::Put, None)?;
    }
    writer.add_range_tombstone(b"a", b"z", 4)?;
    writer.finish_to_path(Path::new("source.sst"))?;
    let neighbor = "neighbor.sst".to_owned();
    let mut writer = factory.create()?;
    writer.add_with_meta(b"z", Some(b"neighbor"), 5, EntryType::Put, None)?;
    writer.add_with_meta(b"zz", Some(b"neighbor"), 6, EntryType::Put, None)?;
    writer.finish_to_path(Path::new(&neighbor))?;
    let original = fragment_metadata(&factory, &["source.sst".to_owned(), neighbor.clone()])?;
    assert_eq!(
        layout::repair_components(&original.iter().collect::<Vec<_>>()),
        [] as [Vec<usize>; 0]
    );

    // Act
    let outputs = execute_compaction(&plan, &factory, Path::new(""), None)?;

    // Assert: a separate last-key partition would share the outer endpoint
    // with this ending range and an existing adjacent source-level neighbor.
    assert_eq!(outputs.len(), 2);
    let mut names = outputs.clone();
    names.push(neighbor);
    let replacement = fragment_metadata(&factory, &names)?;
    assert_eq!(
        layout::repair_components(&replacement.iter().collect::<Vec<_>>()),
        [] as [Vec<usize>; 0]
    );
    assert_eq!(
        read_fragment_versions(&factory, &outputs)?,
        read_fragment_versions(&factory, &plan.source_files)?
    );
    assert_eq!(
        factory.open(Path::new(&outputs[1]))?.range_tombstones(),
        vec![RangeTombstone {
            start: b"m".to_vec(),
            end: b"z".to_vec(),
            seq: 4
        }]
    );
    Ok(())
}
