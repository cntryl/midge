use super::*;

const MIB: u64 = 1024 * 1024;
const WORK_TARGET: usize = 512 * 1024 * 1024;

fn bounded_compactor() -> Compactor {
    let mut compactor = Compactor::with_config(LeveledCompactionConfig {
        l0_file_count_threshold: 3,
        ..LeveledCompactionConfig::default()
    });
    compactor.set_work_target_bytes(Some(WORK_TARGET));
    compactor
}

fn selected_bytes(plan: &CompactionPlan, files: &[FileMeta]) -> u64 {
    files
        .iter()
        .filter(|file| plan.input_files.contains(&file.name))
        .map(|file| file.size_bytes)
        .sum()
}

fn file(name: &str, level: u32, bounds: (&[u8], &[u8]), size_bytes: u64) -> FileMeta {
    FileMeta {
        name: name.to_string(),
        cf_id: 0,
        level,
        size_bytes,
        smallest_key: Some(bounds.0.to_vec()),
        largest_key: Some(bounds.1.to_vec()),
        key_bounds_complete: true,
        ..FileMeta::default()
    }
}

fn broad_source_layout() -> Vec<FileMeta> {
    let mut files = vec![
        file("source-0", 2, (b"b", b"y"), 128 * MIB),
        file("source-1", 2, (b"z", b"zz"), 128 * MIB),
        file("source-2", 2, (b"zzz", b"zzzz"), 128 * MIB),
        file("source-3", 2, (b"zzzzz", b"zzzzzz"), 32 * MIB),
    ];
    for (index, bounds) in [
        (b"a".as_slice(), b"d".as_slice()),
        (b"e", b"h"),
        (b"i", b"l"),
        (b"m", b"p"),
        (b"q", b"t"),
        (b"u", b"x"),
    ]
    .into_iter()
    .enumerate()
    {
        files.push(file(&format!("target-{index}"), 3, bounds, 128 * MIB));
    }
    files
}

#[test]
fn should_fragment_source_when_single_file_overlaps_excessive_target_bytes() {
    // Arrange: the retained failing layout has one broad 128MiB source whose
    // complete target span alone is 768MiB; shrinking the source batch is not enough.
    let compactor = bounded_compactor();
    let files = broad_source_layout();

    // Act
    let plan = compactor
        .pick_deepest_inner_compaction(&files, 0)
        .expect("valid complete metadata")
        .expect("overfull source level");

    // Assert: preparation replaces only the source and preserves its level.
    assert_eq!(plan.source_level, 2);
    assert_eq!(plan.target_level, 2);
    assert_eq!(plan.input_files, vec!["source-0".to_string()]);
    assert_eq!(plan.source_files, plan.input_files);
    assert_eq!(plan.target_files, [] as [String; 0]);
    assert!(!plan.point_tombstone_gc_eligible);
    assert!(!plan.range_tombstone_gc_eligible);
    assert_eq!(
        plan.purpose,
        CompactionPurpose::Fragment {
            split_key: b"m".to_vec()
        }
    );
}

#[test]
fn should_shrink_source_batch_when_single_source_work_fits() {
    // Arrange
    let compactor = bounded_compactor();
    let mut files = broad_source_layout();
    for (file, (smallest, largest)) in files.iter_mut().take(3).zip([
        (b"b".as_slice(), b"d".as_slice()),
        (b"e", b"h"),
        (b"i", b"l"),
    ]) {
        file.smallest_key = Some(smallest.to_vec());
        file.largest_key = Some(largest.to_vec());
    }

    // Act
    let plan = compactor
        .pick_deepest_inner_compaction(&files, 0)
        .unwrap()
        .unwrap();

    // Assert: the complete singleton overlap fits without a preparatory rewrite.
    assert_eq!(plan.purpose, CompactionPurpose::Merge);
    assert_eq!(plan.source_files, vec!["source-0".to_string()]);
    assert_eq!(plan.target_files, vec!["target-0".to_string()]);
    assert_eq!(selected_bytes(&plan, &files), 256 * MIB);
}

#[test]
fn should_keep_complete_overlap_when_source_has_no_interior_target_boundary() {
    // Arrange
    let compactor = bounded_compactor();
    let mut files = broad_source_layout();
    files.retain(|file| file.level != 3);
    files.push(file("single-target", 3, (b"a", b"zzzzzzz"), 768 * MIB));

    // Act
    let plan = compactor
        .pick_deepest_inner_compaction(&files, 0)
        .unwrap()
        .unwrap();

    // Assert: missing split proof never truncates an indivisible target closure.
    assert_eq!(plan.purpose, CompactionPurpose::Merge);
    assert_eq!(plan.source_files, vec!["source-0".to_string()]);
    assert_eq!(plan.target_files, vec!["single-target".to_string()]);
    assert!(selected_bytes(&plan, &files) > 512 * MIB);
}

#[test]
fn should_preserve_complete_closure_when_fragmentation_bounds_are_unproven() {
    // Arrange
    let compactor = bounded_compactor();
    for (name, complete, smallest) in [
        ("source-0", false, b"b".as_slice()),
        ("source-1", true, b"c"),
        ("target-0", false, b"a"),
        ("target-1", true, b"b"),
    ] {
        let mut files = broad_source_layout();
        let changed = files.iter_mut().find(|file| file.name == name).unwrap();
        changed.key_bounds_complete = complete;
        changed.smallest_key = Some(smallest.to_vec());

        // Act
        let plan = compactor
            .pick_deepest_inner_compaction(&files, 0)
            .unwrap()
            .unwrap();

        // Assert: legacy bounds and defective layouts retain conservative work.
        assert_eq!(plan.purpose, CompactionPurpose::Merge, "{name}");
        assert_eq!(plan.source_files, vec!["source-0".to_string()]);
        assert_eq!(plan.target_files.len(), 6);
    }
}

#[test]
fn should_advance_fragmentation_when_prior_cut_remains_an_inclusive_bound() {
    // Arrange: clipped range markers can leave the prior split as largest_key.
    let compactor = bounded_compactor();
    let mut files = broad_source_layout();
    files[3].size_bytes = 128 * MIB;
    files[0].largest_key = Some(b"m".to_vec());
    files[0].size_bytes = 64 * MIB;

    // Act
    let next = compactor
        .pick_deepest_inner_compaction(&files, 0)
        .unwrap()
        .unwrap();
    files[0].largest_key = Some(b"i".to_vec());
    files[0].size_bytes = 32 * MIB;
    let settled = compactor
        .pick_deepest_inner_compaction(&files, 0)
        .unwrap()
        .unwrap();

    // Assert: the cut moves strictly inside and ordinary promotion then resumes.
    assert_eq!(
        next.purpose,
        CompactionPurpose::Fragment {
            split_key: b"i".to_vec()
        }
    );
    assert_eq!(settled.purpose, CompactionPurpose::Merge);
    assert_eq!(settled.source_files, vec!["source-0".to_string()]);
    assert_eq!(settled.target_files.len(), 3);
    assert!(selected_bytes(&settled, &files) <= 512 * MIB);
}

#[test]
fn should_keep_source_batch_when_work_exactly_matches_target() {
    // Arrange
    let compactor = bounded_compactor();
    let mut files = broad_source_layout();
    files.retain(|file| file.level != 3);
    files.push(file("single-target", 3, (b"a", b"zzzz"), 128 * MIB));

    // Act
    let plan = compactor
        .pick_deepest_inner_compaction(&files, 0)
        .unwrap()
        .unwrap();

    // Assert
    assert_eq!(plan.purpose, CompactionPurpose::Merge);
    assert_eq!(plan.source_files.len(), 3);
    assert_eq!(selected_bytes(&plan, &files), 512 * MIB);
}

#[test]
fn should_keep_existing_plan_when_work_target_is_disabled() {
    // Arrange
    let mut compactor = bounded_compactor();
    compactor.set_work_target_bytes(None);
    let files = broad_source_layout();

    // Act
    let plan = compactor
        .pick_deepest_inner_compaction(&files, 0)
        .unwrap()
        .unwrap();

    // Assert
    assert_eq!(plan.purpose, CompactionPurpose::Merge);
    assert_eq!(plan.source_files.len(), 3);
    assert_eq!(plan.target_files.len(), 6);
}

#[test]
fn should_keep_equality_chain_when_no_further_interior_split_exists() {
    // Arrange: a retained range endpoint can connect the complete target chain.
    let compactor = bounded_compactor();
    let mut files = broad_source_layout();
    files[0].largest_key = Some(b"d".to_vec());
    for (index, target) in files.iter_mut().filter(|file| file.level == 3).enumerate() {
        if index > 0 {
            target.smallest_key = Some(vec![b"dhlpt"[index - 1]]);
        }
    }

    // Act
    let plan = compactor
        .pick_deepest_inner_compaction(&files, 0)
        .unwrap()
        .unwrap();

    // Assert: the planner neither repeats the old cut nor truncates endpoint closure.
    assert_eq!(plan.purpose, CompactionPurpose::Merge);
    assert_eq!(plan.source_files, vec!["source-0".to_string()]);
    assert_eq!(plan.target_files.len(), 6);
    assert!(selected_bytes(&plan, &files) > 512 * MIB);
}

#[test]
fn should_reject_overflowing_work_estimate_when_input_sizes_exceed_counter() {
    // Arrange
    let mut compactor = bounded_compactor();
    compactor.set_work_target_bytes(Some(usize::MAX));
    let first = file("first", 2, (b"a", b"b"), u64::MAX);
    let second = file("second", 2, (b"c", b"d"), 1);

    // Act
    let oversized = compactor.exceeds_work_target(&[&first, &second], &[], &[]);

    // Assert
    assert!(oversized);
}
