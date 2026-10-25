use super::*;
use crate::io::{Fs, MockFs};
use crate::metadata::FileMeta;
use crate::sst::{FsSstFactoryIo, SstFactory};
use crate::types::EntryType;
use std::sync::Arc;

fn parent(name: &str, lower: &[u8], upper: &[u8], bytes: u64) -> FileMeta {
    FileMeta {
        name: name.into(),
        level: 2,
        size_bytes: bytes,
        smallest_key: Some(lower.to_vec()),
        largest_key: Some(upper.to_vec()),
        key_bounds_complete: true,
        ..Default::default()
    }
}

fn source_factory(with_range: bool) -> crate::MidgeResult<FsSstFactoryIo> {
    let fs: Arc<dyn Fs> = Arc::new(MockFs::new());
    let factory = FsSstFactoryIo::new(fs, 4096);
    let mut writer = factory.create()?;
    for key in [b"a", b"b", b"d", b"e", b"g", b"h"] {
        // Equal user-key versions must remain a single reduction group.
        writer.add_with_meta(key, Some(b"old"), 1, EntryType::Put, None)?;
        writer.add_with_meta(key, Some(b"new"), 2, EntryType::Put, None)?;
    }
    if with_range {
        writer.add_range_tombstone(b"a", b"z", 0)?;
    }
    writer.finish_to_path(Path::new("source.sst"))?;
    Ok(factory)
}

#[test]
fn should_bound_grandparent_overlap_when_complete_disjoint_spans_are_feasible(
) -> crate::MidgeResult<()> {
    // Arrange
    let factory = source_factory(false)?;
    let parents = [
        parent("one", b"a", b"c", 10),
        parent("two", b"d", b"f", 10),
        parent("three", b"g", b"z", 10),
    ];
    let mut plan = CompactionPlan::new(0, 0, 1).with_output_seq(948);
    plan.add_test_source("source.sst");
    plan.grandparent_overlap = GrandparentOverlap::from_files(parents.iter(), 10);

    // Act
    let outputs = execute_compaction(&plan, &factory, Path::new(""), None)?;

    // Assert
    assert_eq!(outputs.len(), 3);
    let mut keys_seen = Vec::new();
    for name in outputs {
        let summary = crate::sst::fs::SstFileIo::summarize_with_fs(&name, factory.output_fs())?;
        let overlap: u64 = parents
            .iter()
            .filter(|parent| {
                parent.smallest_key.as_ref().unwrap() <= &summary.largest_key
                    && parent.largest_key.as_ref().unwrap() >= &summary.smallest_key
            })
            .map(|parent| parent.size_bytes)
            .sum();
        assert!(overlap <= 10);
        let reader = factory.open(Path::new(&name))?;
        for (key, state) in reader.scan_range_raw_state(None, None)? {
            assert!(
                matches!(state, crate::types::KeyState::Value(value, 2, _, _) if value.as_ref() == b"new")
            );
            keys_seen.push(key);
        }
    }
    assert_eq!(keys_seen.len(), 6);
    Ok(())
}

#[test]
fn should_preserve_range_coverage_when_grandparent_boundaries_roll_outputs(
) -> crate::MidgeResult<()> {
    // Arrange
    let factory = source_factory(true)?;
    let parents = [
        parent("one", b"a", b"c", 10),
        parent("two", b"d", b"f", 10),
        parent("three", b"g", b"z", 10),
    ];
    let mut plan = CompactionPlan::new(0, 0, 1).with_output_seq(949);
    plan.add_test_source("source.sst");
    plan.grandparent_overlap = GrandparentOverlap::from_files(parents.iter(), 10);

    // Act
    let outputs = execute_compaction(&plan, &factory, Path::new(""), None)?;

    // Assert
    let mut ranges = Vec::new();
    for name in outputs {
        ranges.extend(factory.open(Path::new(&name))?.range_tombstones());
    }
    ranges.sort_by(|left, right| left.start.cmp(&right.start));
    assert_eq!(ranges.len(), 3);
    assert_eq!(ranges.first().unwrap().start, b"a");
    assert_eq!(ranges.last().unwrap().end, b"z");
    assert!(ranges.windows(2).all(|pair| pair[0].end == pair[1].start));
    assert!(ranges.iter().all(|range| range.seq == 0));
    Ok(())
}

#[test]
fn should_keep_indivisible_key_group_when_adjacent_grandparents_exceed_soft_limit(
) -> crate::MidgeResult<()> {
    // Arrange: both inclusive grandparent spans contain `d`, so even the
    // smallest legal output for that key exceeds the configured soft limit.
    let factory = source_factory(false)?;
    let parents = [
        parent("left", b"c", b"d", 10),
        parent("right", b"d", b"e", 10),
    ];
    let mut plan = CompactionPlan::new(0, 0, 1).with_output_seq(951);
    plan.add_test_source("source.sst");
    plan.grandparent_overlap = GrandparentOverlap::from_files(parents.iter(), 10);

    // Act
    let outputs = execute_compaction(&plan, &factory, Path::new(""), None)?;

    // Assert: the soft-limit exception does not split either version of `d`.
    let mut d_outputs = Vec::new();
    for name in &outputs {
        let reader = factory.open(Path::new(name))?;
        if let Some((_, state)) = reader
            .scan_range_raw_state(Some(b"d"), Some(b"e"))?
            .into_iter()
            .find(|(key, _)| key.as_ref() == b"d")
        {
            d_outputs.push((name, state));
        }
    }
    assert_eq!(d_outputs.len(), 1);
    assert!(matches!(
        &d_outputs[0].1,
        crate::types::KeyState::Value(value, 2, _, _) if value.as_ref() == b"new"
    ));
    let d_overlap: u64 = parents
        .iter()
        .filter(|parent| {
            parent.smallest_key.as_ref().unwrap().as_slice() <= b"d".as_slice()
                && parent.largest_key.as_ref().unwrap().as_slice() >= b"d".as_slice()
        })
        .map(|parent| parent.size_bytes)
        .sum();
    assert_eq!(d_overlap, 20);
    assert!(d_overlap > plan.grandparent_overlap.max_bytes);
    Ok(())
}

#[test]
fn should_allow_one_oversized_grandparent_without_splitting_every_key() -> crate::MidgeResult<()> {
    // Arrange
    let factory = source_factory(true)?;
    let parents = [parent("oversized", b"a", b"z", 100)];
    let mut plan = CompactionPlan::new(0, 0, 1).with_output_seq(950);
    plan.add_test_source("source.sst");
    plan.grandparent_overlap = GrandparentOverlap::from_files(parents.iter(), 10);

    // Act
    let outputs = execute_compaction(&plan, &factory, Path::new(""), None)?;

    // Assert: the soft target cannot shrink the individual grandparent.
    assert_eq!(outputs.len(), 1);
    let reader = factory.open(Path::new(&outputs[0]))?;
    assert_eq!(reader.scan_range_raw_state(None, None)?.len(), 6);
    assert_eq!(reader.range_tombstones()[0].end, b"z");
    Ok(())
}
