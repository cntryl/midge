//! Cloud actor admission using the retained stage-02 S3 overlap shape.

use super::{tests::create_test_cloud_event_loop, EventLoop};
use crate::common::MidgeResult;
use crate::compaction::strategy::CompactionPurpose;
use crate::metadata::FileMeta;
use crate::storage::hybrid::policy::StorageBudgetPolicy;
use std::sync::Arc;

const MIB: usize = 1024 * 1024;
const SOURCE_GENERATION: u64 = 11_583_807;
const TARGET_GENERATION: u64 = 11_255_563;
type Position = (u8, u64, u16);

fn key((client, transaction, operation): Position) -> Vec<u8> {
    format!("midge:w:01:{client:02}:00:{transaction:012}:{operation:05}").into_bytes()
}

fn name(level: u32, generation: u64, partition: u32) -> String {
    crate::cloud_layout::compaction_file_name(1, level, generation, partition)
}

fn file(
    level: u32,
    generation: u64,
    partition: u32,
    bounds: (Position, Position),
    size_bytes: u64,
) -> FileMeta {
    FileMeta {
        name: name(level, generation, partition),
        cf_id: 1,
        level,
        size_bytes,
        smallest_key: Some(key(bounds.0)),
        largest_key: Some(key(bounds.1)),
        key_bounds_complete: true,
        ..FileMeta::default()
    }
}

fn observed_sources() -> Vec<FileMeta> {
    [
        (((0, 56_826, 0), (0, 61_833, 15)), 22_280_662),
        (((0, 61_833, 16), (1, 61_886, 1)), 22_280_929),
        (((1, 61_886, 2), (1, 62_310, 31)), 1_891_146),
    ]
    .into_iter()
    .enumerate()
    .map(|(partition, (bounds, bytes))| {
        file(
            1,
            SOURCE_GENERATION,
            u32::try_from(partition).unwrap(),
            bounds,
            bytes,
        )
    })
    .collect()
}

fn observed_targets() -> Vec<FileMeta> {
    [
        (((0, 54_160, 11), (1, 2_341, 22)), 22_280_995),
        (((1, 2_341, 23), (1, 7_349, 1)), 22_280_634),
        (((1, 7_349, 2), (1, 12_356, 19)), 22_280_687),
        (((1, 12_356, 20), (1, 17_363, 18)), 22_280_858),
        (((1, 17_363, 19), (1, 22_370, 28)), 22_280_916),
        (((1, 22_370, 29), (1, 27_378, 12)), 22_280_677),
        (((1, 27_378, 13), (1, 32_385, 26)), 22_280_607),
        (((1, 32_385, 27), (1, 37_393, 9)), 22_280_501),
        (((1, 37_393, 10), (1, 42_400, 18)), 22_281_079),
        (((1, 42_400, 19), (1, 47_408, 0)), 22_280_763),
        (((1, 47_408, 1), (1, 52_415, 20)), 22_280_882),
        (((1, 52_415, 21), (1, 57_093, 31)), 20_816_330),
    ]
    .into_iter()
    .enumerate()
    .map(|(partition, (bounds, bytes))| {
        file(
            2,
            TARGET_GENERATION,
            u32::try_from(partition).unwrap() + 1,
            bounds,
            bytes,
        )
    })
    .collect()
}

fn observed_cloud_layout(
    storage_policy: StorageBudgetPolicy,
    configured_target: usize,
    compaction_config: crate::compaction::LeveledCompactionConfig,
) -> MidgeResult<EventLoop> {
    let local_bytes = storage_policy.max_local_bytes;
    let mut el = create_test_cloud_event_loop(storage_policy)?;
    assert_eq!(el.state.create_cf("observed-workload".into())?, 1);
    el.state.manifest.test_mut().files = observed_sources();
    el.state
        .manifest
        .test_mut()
        .files
        .extend(observed_targets());
    let factory = super::resources::compaction_sst_factory(
        Arc::clone(&el.state.fs),
        crate::codec::CompressionPolicy::default(),
    );
    el.actors.compaction_actor = crate::runtime::actors::CompactionActor::new_with_config(
        Arc::new(factory),
        compaction_config,
    );
    // Use the observed engine execution limits. The fixture backend's upload
    // copies determine its own immutable partition target, unlike HTTP's target.
    // Reattach through production wiring after applying those execution limits.
    el.actors
        .compaction_actor
        .set_execution_limits(configured_target, 256 * MIB);
    let storage = Arc::clone(
        el.actors
            .cloud_coordinator
            .hybrid_storage
            .as_ref()
            .expect("real filesystem-backed cloud fixture"),
    );
    storage.enable_ephemeral_sst_cache(local_bytes);
    el.set_hybrid_storage(storage);
    el.state.set_compaction_enabled(true);
    Ok(el)
}

#[test]
fn should_shrink_cloud_source_batch_when_observed_overlap_exceeds_publication_work_target(
) -> MidgeResult<()> {
    // Arrange: 3 L1 inputs plus their 12 complete L2 overlaps total 312 MB.
    // Cloud's immutable-file workspace is stricter than the local pool/2 goal.
    let mut el = observed_cloud_layout(
        StorageBudgetPolicy::default(),
        128 * MIB,
        Default::default(),
    )?;
    let storage = el.actors.cloud_coordinator.hybrid_storage.as_ref().unwrap();
    let work_target = storage.immutable_file_partition_target(256 * MIB) * 4;
    assert!(u64::try_from(work_target).unwrap() < 312_357_666);

    // Act: use the actor configured by EventLoop and its attached cloud store.
    let plan = el
        .actors
        .compaction_actor
        .check_compaction(&el.state)?
        .expect("observed overfull L1 requires maintenance");

    // Assert: the complete first singleton overlap fits; no target is truncated.
    assert_eq!(plan.purpose, CompactionPurpose::Merge);
    assert_eq!((plan.source_level, plan.target_level), (1, 2));
    assert_eq!(plan.source_files, vec![name(1, SOURCE_GENERATION, 0)]);
    assert_eq!(plan.target_files, vec![name(2, TARGET_GENERATION, 1)]);
    assert_eq!(plan.input_files.len(), 2);
    let input_bytes: u64 = el
        .state
        .manifest
        .files
        .iter()
        .filter(|file| plan.input_files.contains(&file.name))
        .map(|file| file.size_bytes)
        .sum();
    assert_eq!(input_bytes, 44_561_657);
    assert!(input_bytes <= u64::try_from(work_target).unwrap());
    Ok(())
}

#[test]
fn should_fragment_observed_broad_source_when_cloud_singleton_overlap_exceeds_work_target(
) -> MidgeResult<()> {
    // Arrange: the retained second source alone overlaps all 12 targets. A
    // later disjoint source keeps L1 overfull after the earlier narrow source
    // is gone; its presence must not force another 288 MB complete merge.
    let mut el = observed_cloud_layout(
        StorageBudgetPolicy::default(),
        128 * MIB,
        crate::compaction::LeveledCompactionConfig {
            // This regression exercises the retained legacy fallback. Current
            // conforming overlap spans stream a complete merge instead.
            max_grandparent_overlap_bytes: 0,
            ..Default::default()
        },
    )?;
    let earlier = name(1, SOURCE_GENERATION, 0);
    el.state
        .manifest
        .test_mut()
        .files
        .retain(|file| file.name != earlier);
    el.state.manifest.test_mut().files.push(file(
        1,
        SOURCE_GENERATION,
        3,
        ((2, 0, 0), (2, 1, 31)),
        u64::try_from(20 * MIB).unwrap(),
    ));
    let targets_before: Vec<_> = el
        .state
        .manifest
        .files
        .iter()
        .filter(|file| file.level == 2)
        .cloned()
        .collect();

    // Act: the same configured actor must prepare a source-only rewrite.
    let plan = el
        .actors
        .compaction_actor
        .check_compaction(&el.state)?
        .expect("continued overfull L1 requires maintenance");

    // Assert: split on a real interior target boundary, retaining all targets.
    assert_eq!(
        plan.purpose,
        CompactionPurpose::Fragment {
            split_key: key((1, 27_378, 13)),
        }
    );
    assert_eq!((plan.source_level, plan.target_level), (1, 1));
    assert_eq!(plan.source_files, vec![name(1, SOURCE_GENERATION, 1)]);
    assert_eq!(plan.input_files, plan.source_files);
    assert_eq!(plan.target_files, [] as [String; 0]);
    assert!(!plan.point_tombstone_gc_eligible && !plan.range_tombstone_gc_eligible);
    assert!(targets_before.iter().all(|target| el
        .state
        .manifest
        .files
        .iter()
        .any(|current| current.same_identity(target))));
    Ok(())
}

fn disjoint_cloud_layout(local_bytes: u64, configured_target: usize) -> MidgeResult<EventLoop> {
    let mut el = observed_cloud_layout(
        StorageBudgetPolicy::new(local_bytes),
        configured_target,
        Default::default(),
    )?;
    // Eleven healthy, disjoint 4 MiB inputs leave L1 over its 40 MiB goal.
    // The default four-source batch costs 16 MiB, with no target overlap.
    el.state.manifest.test_mut().files = (0..11)
        .map(|partition| {
            let client = u8::try_from(partition).unwrap() + 2;
            file(
                1,
                SOURCE_GENERATION,
                partition,
                ((client, 0, 0), (client, 1, 31)),
                u64::try_from(4 * MIB).unwrap(),
            )
        })
        .collect();
    Ok(el)
}

fn selected_plan(el: &mut EventLoop) -> MidgeResult<crate::compaction::CompactionPlan> {
    el.actors
        .compaction_actor
        .check_compaction(&el.state)?
        .ok_or_else(|| crate::common::MidgeError::Internal("fixture has no cloud debt".into()))
}

#[test]
fn should_honor_configured_sst_target_when_cloud_publication_work_is_larger() -> MidgeResult<()> {
    // Arrange: a 2 MiB configured target permits 8 MiB of preferred input work.
    let mut el = disjoint_cloud_layout(u64::try_from(2_048 * MIB).unwrap(), 2 * MIB)?;

    // Act
    let plan = selected_plan(&mut el)?;

    // Assert: one complete 4 MiB source fits; the original 16 MiB batch does not.
    assert_eq!(plan.purpose, CompactionPurpose::Merge);
    assert_eq!(plan.source_files, vec![name(1, SOURCE_GENERATION, 0)]);
    assert_eq!(plan.input_files, plan.source_files);
    assert_eq!(plan.target_files, [] as [String; 0]);
    Ok(())
}

#[test]
fn should_honor_local_staging_window_when_cloud_publication_work_is_larger() -> MidgeResult<()> {
    // Arrange: the 16 MiB local budget gives a 2 MiB soft staging target;
    // its 8 MiB preferred input allowance is stricter than publication memory.
    let mut el = disjoint_cloud_layout(u64::try_from(16 * MIB).unwrap(), 128 * MIB)?;

    // Act
    let plan = selected_plan(&mut el)?;

    // Assert: retain overlap completeness while reducing the source batch.
    assert_eq!(plan.purpose, CompactionPurpose::Merge);
    assert_eq!(plan.source_files, vec![name(1, SOURCE_GENERATION, 0)]);
    assert_eq!(plan.input_files, plan.source_files);
    assert_eq!(plan.target_files, [] as [String; 0]);
    Ok(())
}
