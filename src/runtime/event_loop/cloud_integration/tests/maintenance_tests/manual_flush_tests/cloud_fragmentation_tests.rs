//! Source-history preservation through remote compute and conditional publication.

use super::*;
use crate::common::resource_budget::ResourceBudget;
use crate::compaction::strategy::CompactionPurpose;
use crate::compaction::LeveledCompactionConfig;
use crate::runtime::actors::compaction::CompactionActor;
use crate::runtime::RuntimeState;
use crate::sst::traits::{DynSstWriter, RawSstVersion};
use crate::sst::SstFactory as _;
use crate::types::RangeTombstone;
use std::path::Path;

const POOL: usize = 8 * 1024 * 1024;
const TARGET: usize = 64 * 1024;

#[derive(Debug, PartialEq, Eq)]
struct SourceHistory {
    versions: Vec<RawSstVersion>,
    ranges: Vec<RangeTombstone>,
}

fn seed_fragment_layout(el: &mut EventLoop) -> MidgeResult<Vec<crate::metadata::FileMeta>> {
    let writer = source_writer(el)?;
    let mut files = vec![publish_seed(el, 1, 100, writer)?];
    for (index, key) in [b"a1", b"b1", b"c1", b"d1", b"e1", b"f1"]
        .into_iter()
        .enumerate()
    {
        let mut writer = local_factory(el).create()?;
        writer.add_with_meta(
            key,
            Some(&vec![key[0]; 128 * 1024]),
            81,
            EntryType::Put,
            None,
        )?;
        files.push(publish_seed(
            el,
            2,
            u64::try_from(index).unwrap() + 101,
            writer,
        )?);
    }
    Ok(files)
}

fn source_writer(el: &EventLoop) -> MidgeResult<Box<dyn DynSstWriter>> {
    let mut writer = local_factory(el).create()?;
    for (key, value, sequence, kind, expiration) in [
        (b"a0", Some(b"left".as_slice()), 88, EntryType::Put, None),
        (
            b"b0",
            Some(b"live".as_slice()),
            90,
            EntryType::Put,
            Some(u64::MAX),
        ),
        (
            b"b0",
            Some(b"expired".as_slice()),
            82,
            EntryType::Put,
            Some(1),
        ),
        (b"c0", None, 89, EntryType::Delete, None),
        (b"c0", Some(b"masked".as_slice()), 83, EntryType::Put, None),
        (b"f9", Some(b"right".as_slice()), 91, EntryType::Put, None),
    ] {
        writer.add_sorted_with_meta(key, value, sequence, kind, expiration)?;
    }
    writer.add_range_tombstone(b"a0", b"f9", 87)?;
    Ok(writer)
}

fn local_factory(el: &EventLoop) -> crate::sst::FsSstFactoryIo {
    crate::sst::FsSstFactoryIo::new(Arc::clone(&el.state.fs), 4096)
        .with_compression_policy(crate::codec::CompressionPolicy::None)
}

fn publish_seed(
    el: &mut EventLoop,
    level: u32,
    generation: u64,
    writer: Box<dyn DynSstWriter>,
) -> MidgeResult<crate::metadata::FileMeta> {
    let name = crate::cloud_layout::file_name(0, level, generation);
    writer.finish_to_path(&el.state.host_sst_dir().join(&name))?;
    let path = crate::sst::fs::sst_key(&name);
    let summary = crate::sst::fs::SstFileIo::summarize_with_fs_for_compaction(
        &path.0,
        Arc::clone(&el.state.fs),
        ResourceBudget::new(POOL),
    )?;
    let (_, checksum) = crate::sst::fs::file_identity_with_fs(&el.state.fs, Path::new(&path.0))?;
    let hybrid = el.actors.cloud_coordinator.hybrid_storage.as_ref().unwrap();
    hybrid.publish_immutable_file(
        crate::storage::hybrid::backend::ImmutableFileSource {
            key: &crate::cloud_layout::object_key(&name),
            fs: el.state.fs.as_ref(),
            path: &path,
            size: summary.size_bytes,
            checksum,
        },
        &ResourceBudget::new(POOL),
    )?;
    // Only cloud bytes can satisfy the real worker's source reads.
    el.state
        .fs
        .remove_file(&path)
        .map_err(FsError::into_midge)?;
    let file = crate::metadata::FileMeta {
        name,
        cf_id: 0,
        level,
        size_bytes: summary.size_bytes,
        content_crc32c: Some(checksum),
        smallest_key: Some(summary.smallest_key),
        largest_key: Some(summary.largest_key),
        smallest_seq: Some(summary.smallest_seq),
        largest_seq: Some(summary.largest_seq),
        key_bounds_complete: true,
        ..Default::default()
    };
    el.state.manifest.add_file(file.clone());
    el.state.manifest.advance_next_sst_seq(0, generation + 1);
    el.state.sequence = el.state.sequence.max(summary.largest_seq);
    Ok(file)
}

fn install_cloud_actor(el: &mut EventLoop, overlap_limit: u64) {
    let storage = Arc::clone(el.actors.cloud_coordinator.hybrid_storage.as_ref().unwrap());
    let reads = Arc::new(crate::storage::remote_sst::RemoteSstFs::new(
        Arc::clone(&el.state.fs),
        storage.remote_sst_backend(),
        TEST_WAIT,
    ));
    let factory = crate::runtime::event_loop::resources::compaction_sst_factory(
        reads,
        crate::codec::CompressionPolicy::None,
    );
    let mut actor = CompactionActor::new_with_config(
        Arc::new(factory),
        LeveledCompactionConfig {
            l0_file_count_threshold: 4,
            l1_target_size: 1,
            max_levels: 3,
            max_grandparent_overlap_bytes: overlap_limit,
            ..Default::default()
        },
    );
    actor.set_execution_limits(TARGET, POOL);
    el.actors.compaction_actor = actor;
    // Refresh the work target through actual cloud attachment, after limits.
    el.set_hybrid_storage(storage);
    el.state.set_compaction_enabled(true);
    el.state.limits.l0_compaction_trigger = 4;
}

fn read_history(
    factory: &dyn crate::sst::SstFactory,
    names: &[String],
) -> MidgeResult<SourceHistory> {
    let mut versions = Vec::new();
    let mut ranges = Vec::new();
    for name in names {
        let reader = factory.open(Path::new(name))?;
        ranges.extend(reader.range_tombstones());
        versions.extend(
            reader
                .raw_version_cursor(None, None)?
                .collect::<MidgeResult<Vec<_>>>()?,
        );
    }
    versions.sort_by(|left, right| left.key.cmp(&right.key).then(right.seq.cmp(&left.seq)));
    ranges.sort_by(|left, right| {
        left.start
            .cmp(&right.start)
            .then(left.end.cmp(&right.end))
            .then(left.seq.cmp(&right.seq))
    });
    Ok(SourceHistory { versions, ranges })
}

fn read_original_history(el: &EventLoop, source: &str) -> MidgeResult<SourceHistory> {
    let remote = Arc::new(crate::storage::remote_sst::RemoteSstFs::new(
        Arc::clone(&el.state.fs),
        el.actors
            .cloud_coordinator
            .hybrid_storage
            .as_ref()
            .unwrap()
            .remote_sst_backend(),
        TEST_WAIT,
    ));
    let factory = crate::runtime::event_loop::resources::compaction_sst_factory(
        remote,
        crate::codec::CompressionPolicy::None,
    );
    read_history(&factory, &[source.to_string()])
}

fn recover_remote_history(
    el: EventLoop,
    metadata: &MetadataFixture,
    outputs: &[String],
    source: &str,
    targets: &[crate::metadata::FileMeta],
    target_level: u32,
) -> MidgeResult<SourceHistory> {
    let remote = el
        .actors
        .cloud_coordinator
        .hybrid_storage
        .as_ref()
        .unwrap()
        .remote_sst_backend();
    drop(el);
    let directory = tempfile::tempdir()?;
    let store = metadata
        .lease
        .get_leader_store()
        .expect("real conditional authority");
    crate::runtime::cloud_startup::CloudStartupRecovery::hydrate_cloud_metadata(
        &metadata.cloud,
        store.as_ref(),
        &crate::io::real::test_root(directory.path()),
        directory.path(),
        crate::config::RecoveryPolicy::Strict,
    )?;
    let mut recovered = RuntimeState::try_new(
        directory.path(),
        false,
        crate::config::RecoveryPolicy::Strict,
    )?;
    recovered.replay_intent_log()?;
    assert!(recovered.intent_log.is_empty());
    assert!(!recovered
        .manifest
        .files
        .iter()
        .any(|file| file.name == source));
    for target in targets {
        assert!(recovered
            .manifest
            .files
            .iter()
            .any(|file| file.same_identity(target)));
    }
    for name in outputs {
        assert!(recovered
            .manifest
            .files
            .iter()
            .any(|file| file.name == *name && file.level == target_level));
        assert!(!recovered
            .fs
            .exists(&crate::sst::fs::sst_key(name))
            .map_err(FsError::into_midge)?);
    }
    let reads = Arc::new(crate::storage::remote_sst::RemoteSstFs::new(
        Arc::clone(&recovered.fs),
        remote,
        TEST_WAIT,
    ));
    let factory = crate::runtime::event_loop::resources::compaction_sst_factory(
        reads,
        crate::codec::CompressionPolicy::None,
    );
    read_history(&factory, outputs)
}

#[test]
fn should_recover_raw_source_history_when_cloud_fragment_publication_commits() -> MidgeResult<()> {
    // Arrange: the source fits one ordinary output, but overlaps excessive remote targets.
    let (mut el, worker, _) = cloud_debt(0)?;
    // This retained broad source predates the configured output overlap
    // budget and still requires source-only fragmentation.
    install_cloud_actor(&mut el, u64::try_from(TARGET * 4).unwrap());
    let files = seed_fragment_layout(&mut el)?;
    let source = &files[0];
    let targets = &files[1..];
    let mut metadata = metadata_fixture(&mut el)?;
    let original = read_original_history(&el, &source.name)?;
    let plan = el
        .actors
        .compaction_actor
        .check_manual_compaction(&el.state)?
        .expect("real cloud debt");
    let CompactionPurpose::Fragment { split_key } = &plan.purpose else {
        panic!("actual cloud actor must select source fragmentation: {plan:?}");
    };
    let cut = split_key.clone();
    assert_eq!(plan.input_files, vec![source.name.clone()]);
    assert_eq!((plan.source_level, plan.target_level), (1, 1));
    assert!(source.size_bytes < u64::try_from(TARGET).unwrap());

    // Act: hold actual conditional publication after remote compute has uploaded both fragments.
    el.launch_compaction(plan)?;
    el.state.set_compaction_enabled(false);
    let completion = worker
        .recv_timeout(TEST_WAIT)
        .expect("real remote fragment compute");
    let RuntimeMsg::CompactionComplete {
        succeeded: true,
        ref output_ssts,
        ..
    } = completion
    else {
        panic!("real remote fragmentation failed: {completion:?}");
    };
    let outputs = output_ssts.clone();
    assert_eq!(outputs.len(), 2);
    assert!(outputs
        .iter()
        .all(|name| remote_sst_path_for_test(&el, name).exists()));
    metadata.backend.armed.store(true, Ordering::Release);
    let (_, messages) = crossbeam::channel::unbounded();
    el.handle_runtime_msg(completion, &messages);
    metadata
        .reached
        .recv_timeout(TEST_WAIT)
        .expect("actual publisher authority read held");
    let before_commit = read_committed_manifest(&metadata)?;
    let retained = before_commit
        .files
        .iter()
        .any(|file| file.same_identity(source))
        && !before_commit
            .files
            .iter()
            .any(|file| outputs.contains(&file.name))
        && remote_sst_path_for_test(&el, &source.name).exists()
        && el.state.active_compactions.load(Ordering::Acquire) == 1;
    metadata.release.release();
    settle_owned_work(&mut el, &worker);

    // Assert: fresh metadata and absent local cache still recover every raw source record.
    assert!(retained);
    assert_eq!(original.versions.len(), 6);
    assert!(original.versions.iter().any(|row| row.is_tombstone));
    assert!(original
        .versions
        .iter()
        .any(|row| row.expiration == Some(1)));
    assert!(original
        .versions
        .iter()
        .any(|row| row.expiration == Some(u64::MAX)));
    assert_eq!(
        original.ranges,
        vec![RangeTombstone::new(b"a0".to_vec(), b"f9".to_vec(), 87)]
    );
    let recovered = recover_remote_history(el, &metadata, &outputs, &source.name, targets, 1)?;
    assert_eq!(recovered.versions, original.versions);
    assert_eq!(
        recovered.ranges,
        vec![
            RangeTombstone::new(b"a0".to_vec(), cut.clone(), 87),
            RangeTombstone::new(cut, b"f9".to_vec(), 87),
        ]
    );
    Ok(())
}

#[test]
fn should_publish_complete_cloud_merge_when_conforming_overlap_exceeds_soft_work_target(
) -> MidgeResult<()> {
    // Arrange: the overlap is legal under the current output budget. Its
    // input bytes exceed the preferred work target, which is not a hard cap.
    let (mut el, worker, _) = cloud_debt(0)?;
    install_cloud_actor(&mut el, 320 * 1024 * 1024);
    let files = seed_fragment_layout(&mut el)?;
    let source = files[0].name.clone();
    let mut metadata = metadata_fixture(&mut el)?;
    let plan = el
        .actors
        .compaction_actor
        .check_manual_compaction(&el.state)?
        .expect("real cloud debt");
    assert_eq!(plan.purpose, CompactionPurpose::Merge);
    assert_eq!((plan.source_level, plan.target_level), (1, 2));
    assert_eq!(plan.input_files.len(), 7);
    assert_eq!(plan.target_files.len(), 6);
    assert!(
        files.iter().map(|file| file.size_bytes).sum::<u64>() > u64::try_from(TARGET * 4).unwrap()
    );

    // Act: the real worker reads remote inputs and uploads the actual merge
    // before publication may replace the complete input set.
    el.launch_compaction(plan)?;
    el.state.set_compaction_enabled(false);
    let completion = worker.recv_timeout(TEST_WAIT).expect("real remote merge");
    let RuntimeMsg::CompactionComplete {
        succeeded: true,
        ref output_ssts,
        ..
    } = completion
    else {
        panic!("remote merge failed: {completion:?}");
    };
    let outputs = output_ssts.clone();
    metadata.backend.armed.store(true, Ordering::Release);
    el.handle_runtime_msg(completion, &crossbeam::channel::never());
    metadata
        .reached
        .recv_timeout(TEST_WAIT)
        .expect("held merge publication");
    let committed_before = read_committed_manifest(&metadata)?;
    let retained_inputs = files.iter().all(|input| {
        committed_before
            .files
            .iter()
            .any(|file| file.same_identity(input))
            && remote_sst_path_for_test(&el, &input.name).exists()
    });
    let no_partial_outputs = committed_before
        .files
        .iter()
        .all(|file| !outputs.contains(&file.name));
    metadata.release.release();
    settle_owned_work(&mut el, &worker);

    // Assert: the full merge replaces source and targets together. Cold
    // recovery sees the latest values and no values masked by range deletion.
    assert!(retained_inputs);
    assert!(no_partial_outputs);
    assert_eq!(outputs.len(), 1);
    assert!(el
        .state
        .manifest
        .files
        .iter()
        .all(|file| !files.iter().any(|old| file.same_identity(old))));
    let recovered = recover_remote_history(el, &metadata, &outputs, &source, &[], 2)?;
    let visible = recovered
        .versions
        .iter()
        .map(|row| (row.key.as_slice(), row.value.as_deref()))
        .collect::<Vec<_>>();
    assert_eq!(
        visible,
        vec![
            (b"a0".as_slice(), Some(b"left".as_slice())),
            (b"b0", Some(b"live")),
            (b"f9", Some(b"right"))
        ]
    );
    assert_eq!(recovered.ranges, [] as [RangeTombstone; 0]);
    Ok(())
}
