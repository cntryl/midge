//! Real source-fragment computation and serialized manifest publication.

use super::{CompactionCoordinator, EventLoop};
use crate::common::{MidgeError, MidgeResult};
use crate::compaction::strategy::CompactionPurpose;
use crate::compaction::{CompactionPlan, LeveledCompactionConfig};
use crate::runtime::actors::compaction::CompactionActor;
use crate::runtime::{RuntimeMsg, RuntimeState};
use crate::sst::traits::RawSstVersion;
use crate::sst::SstFactory as _;
use crate::types::EntryType;
use std::sync::Arc;
use std::time::Duration;

const WAIT: Duration = Duration::from_secs(10);
const SOURCE_KEYS: [&[u8]; 6] = [b"a0", b"b0", b"c0", b"d0", b"e0", b"f9"];

struct FragmentFixture {
    directory: tempfile::TempDir,
    el: EventLoop,
    completions: crossbeam::channel::Receiver<RuntimeMsg>,
    source: String,
    targets: Vec<crate::metadata::FileMeta>,
}

impl FragmentFixture {
    fn new() -> MidgeResult<Self> {
        let directory = tempfile::tempdir()?;
        let state = RuntimeState::new(directory.path().to_path_buf(), false);
        let mut el = EventLoop::new(
            state,
            false,
            Arc::new(crate::runtime::ResponseRouter::new()),
            crate::runtime::RuntimeConfig::default(),
            crate::runtime::event_loop::FlushWorkerMode::Inline,
        )?;
        let factory = super::super::resources::compaction_sst_factory(
            Arc::clone(&el.state.fs),
            crate::codec::CompressionPolicy::None,
        );
        let mut actor = CompactionActor::new_with_config(
            Arc::new(factory),
            LeveledCompactionConfig {
                l0_file_count_threshold: 2,
                l1_target_size: 1,
                max_levels: 3,
                ..Default::default()
            },
        );
        actor.set_execution_limits(64 * 1024, 8 * 1024 * 1024);
        actor.configure_work_target(None);
        el.actors.compaction_actor = actor;
        el.state.set_compaction_enabled(true);
        el.state.limits.l0_compaction_trigger = 2;
        el.state.limits.max_immutable_memtables = 0;
        let (messages, completions) = crossbeam::channel::unbounded();
        el.pipeline.worker_msg_tx = Some(messages);
        let mut fixture = Self {
            directory,
            el,
            completions,
            source: String::new(),
            targets: Vec::new(),
        };
        fixture.source = fixture.seed(1, 1, &SOURCE_KEYS, 128)?.name;
        for (index, key) in [b"a1", b"b1", b"c1", b"d1", b"e1", b"f1"]
            .into_iter()
            .enumerate()
        {
            let file = fixture.seed(2, u64::try_from(index).unwrap() + 2, &[key], 128 * 1024)?;
            fixture.targets.push(file);
        }
        fixture.persist()?;
        Ok(fixture)
    }

    fn seed(
        &mut self,
        level: u32,
        sequence: u64,
        keys: &[&[u8]],
        value_bytes: usize,
    ) -> MidgeResult<crate::metadata::FileMeta> {
        let name = crate::cloud_layout::file_name(0, level, sequence);
        std::fs::create_dir_all(self.el.state.host_sst_dir())?;
        let factory = crate::sst::FsSstFactoryIo::new(Arc::clone(&self.el.state.fs), 4096)
            .with_compression_policy(crate::codec::CompressionPolicy::None);
        let mut writer = factory.create()?;
        for key in keys {
            let historical_key = level == 1 && sequence == 1 && *key == b"c0";
            writer.add_with_meta(
                key,
                Some(&vec![key[0]; value_bytes]),
                7,
                EntryType::Put,
                historical_key.then_some(u64::MAX),
            )?;
            if historical_key {
                writer.add_with_meta(
                    key,
                    Some(&vec![42; value_bytes]),
                    6,
                    EntryType::Put,
                    Some(1),
                )?;
            }
        }
        let path = self.el.state.host_sst_dir().join(&name);
        writer.finish_to_path(&path)?;
        let bytes = std::fs::read(path)?;
        let file = crate::metadata::FileMeta {
            name,
            cf_id: 0,
            level,
            size_bytes: u64::try_from(bytes.len()).unwrap(),
            content_crc32c: Some(crc32c::crc32c(&bytes)),
            smallest_key: keys.first().map(|key| key.to_vec()),
            largest_key: keys.last().map(|key| key.to_vec()),
            smallest_seq: Some(if level == 1 && sequence == 1 { 6 } else { 7 }),
            largest_seq: Some(7),
            key_bounds_complete: true,
            ..Default::default()
        };
        self.el.state.manifest.add_file(file.clone());
        self.el.state.sequence = 7;
        Ok(file)
    }

    fn persist(&self) -> MidgeResult<()> {
        crate::metadata::ManifestPersistence::save(&self.el.state.db_path, &self.el.state.manifest)
            .map_err(MidgeError::Internal)
    }

    fn plan(&mut self) -> MidgeResult<CompactionPlan> {
        self.el
            .actors
            .compaction_actor
            .check_manual_compaction(&self.el.state)?
            .ok_or_else(|| MidgeError::Internal("fixture has no compaction debt".into()))
    }

    fn compute(&mut self, plan: CompactionPlan) -> MidgeResult<RuntimeMsg> {
        self.el.launch_compaction(plan)?;
        let completion = self
            .completions
            .recv_timeout(WAIT)
            .expect("real compute completion");
        assert!(matches!(
            &completion,
            RuntimeMsg::CompactionComplete { succeeded: true, output_ssts, .. }
                if output_ssts.len() >= 2
        ));
        Ok(completion)
    }

    fn publish(&mut self, completion: RuntimeMsg) {
        let (_, unused) = crossbeam::channel::unbounded();
        self.el.handle_runtime_msg(completion, &unused);
        assert!(self.el.state.intent_log.is_empty());
        assert!(!self.el.publication.publication_gate.is_active());
        assert!(!self.el.authority.compaction_fence.is_degraded());
    }

    fn raw_rows(&self, names: &[String]) -> MidgeResult<Vec<RawSstVersion>> {
        raw_rows(&self.el.state, names)
    }

    fn recover_rows(self) -> MidgeResult<Vec<RawSstVersion>> {
        let Self { directory, el, .. } = self;
        drop(el);
        let mut recovered = RuntimeState::try_new(
            directory.path(),
            false,
            crate::config::RecoveryPolicy::Strict,
        )?;
        recovered.replay_intent_log()?;
        assert!(recovered.intent_log.is_empty());
        let names = recovered
            .manifest
            .files
            .iter()
            .map(|file| file.name.clone())
            .collect::<Vec<_>>();
        raw_rows(&recovered, &names)
    }

    fn drain(&mut self) -> MidgeResult<()> {
        for _ in 0..64 {
            let Some(plan) = self
                .el
                .actors
                .compaction_actor
                .check_manual_compaction(&self.el.state)?
            else {
                return Ok(());
            };
            self.el.launch_compaction(plan)?;
            let completion = self
                .completions
                .recv_timeout(WAIT)
                .expect("debt compute completion");
            assert!(matches!(
                &completion,
                RuntimeMsg::CompactionComplete {
                    succeeded: true,
                    ..
                }
            ));
            self.publish(completion);
        }
        panic!("bounded source fragmentation must eventually drain the debt");
    }
}

fn raw_rows(state: &RuntimeState, names: &[String]) -> MidgeResult<Vec<RawSstVersion>> {
    let factory = super::super::resources::compaction_sst_factory(
        Arc::clone(&state.fs),
        crate::codec::CompressionPolicy::None,
    );
    let mut rows = Vec::new();
    for name in names {
        rows.extend(
            factory
                .open(std::path::Path::new(name))?
                .raw_version_cursor(None, None)?
                .collect::<MidgeResult<Vec<_>>>()?,
        );
    }
    rows.sort_by(|left, right| left.key.cmp(&right.key).then(right.seq.cmp(&left.seq)));
    Ok(rows)
}

#[test]
fn should_recover_fragmented_source_when_critical_l0_work_interrupts_promotion() -> MidgeResult<()>
{
    crate::failpoints::with_read_gate(|| {
        // Arrange: a broad source overlaps six genuine target SSTs beyond the work target.
        let mut fixture = FragmentFixture::new()?;
        let before = fixture.raw_rows(&[fixture.source.clone()])?;
        assert_eq!(
            before
                .iter()
                .filter(|row| row.key == b"c0")
                .map(|row| (row.seq, row.expiration))
                .collect::<Vec<_>>(),
            vec![(7, Some(u64::MAX)), (6, Some(1))],
        );
        let plan = fixture.plan()?;
        assert!(matches!(plan.purpose, CompactionPurpose::Fragment { .. }));
        assert_eq!(plan.input_files, vec![fixture.source.clone()]);
        assert_eq!((plan.source_level, plan.target_level), (1, 1));
        let completion = fixture.compute(plan)?;
        let outputs = match &completion {
            RuntimeMsg::CompactionComplete { output_ssts, .. } => output_ssts.clone(),
            _ => unreachable!(),
        };

        // Act: ingest becomes critical while the source-only worker owns the slot.
        for index in 0..3 {
            let key = format!("z{index}");
            fixture.seed(0, 100 + index, &[key.as_bytes()], 128)?;
        }
        assert!(fixture.el.state.has_critical_l0_debt(0));
        let names = fixture
            .el
            .state
            .manifest
            .files
            .iter()
            .map(|file| file.name.clone())
            .collect::<Vec<_>>();
        let mut expected = fixture.raw_rows(&names)?;
        // Ordinary debt-draining merges may collect the unpinned older version.
        expected.retain(|row| row.seq == 7);
        fixture.publish(completion);

        // Assert: exact source rows survive, every target identity stays untouched, and L0 wins.
        assert_eq!(fixture.raw_rows(&outputs)?, before);
        assert!(!fixture
            .el
            .state
            .manifest
            .files
            .iter()
            .any(|file| file.name == fixture.source));
        for target in &fixture.targets {
            assert!(fixture
                .el
                .state
                .manifest
                .files
                .iter()
                .any(|file| file.same_identity(target)));
        }
        let next = fixture.plan()?;
        assert_eq!(next.source_level, 0);
        fixture.el.launch_compaction(next)?;
        let completion = fixture
            .completions
            .recv_timeout(WAIT)
            .expect("critical L0 completion");
        fixture.publish(completion);
        fixture.drain()?;
        assert!(!fixture.el.state.has_critical_l0_debt(0));
        assert_eq!(expected.len(), 15);
        assert_eq!(fixture.recover_rows()?, expected);
        Ok(())
    })
}

#[test]
fn should_reject_fragment_publication_when_source_or_replacement_changes() -> MidgeResult<()> {
    crate::failpoints::with_read_gate(|| {
        // Arrange: use actual worker outputs and the captured live source identity.
        let mut fixture = FragmentFixture::new()?;
        let plan = fixture.plan()?;
        assert!(matches!(plan.purpose, CompactionPurpose::Fragment { .. }));
        let completion = fixture.compute(plan)?;
        fixture.el.actors.compaction_actor.join_completed_worker();
        let (inputs, outputs, target_level) = match &completion {
            RuntimeMsg::CompactionComplete {
                input_ssts,
                output_ssts,
                target_level,
                ..
            } => (input_ssts.clone(), output_ssts.clone(), *target_level),
            _ => unreachable!(),
        };
        let (_, _, good) = CompactionCoordinator::validate_publication_start(
            &mut fixture.el,
            &inputs,
            &outputs,
            0,
            target_level,
        )?;

        // Act: an output outside captured bounds and a reused source name are not accepted.
        let mut bad = good.clone();
        bad[0].smallest_key = Some(b"00".to_vec());
        let replacement = CompactionCoordinator::validate_fragment_replacement(
            &fixture.el,
            &inputs,
            &bad,
            0,
            target_level,
        );
        let source = fixture
            .el
            .state
            .manifest
            .test_mut()
            .files
            .iter_mut()
            .find(|file| file.name == fixture.source)
            .unwrap();
        source.content_crc32c = Some(source.content_crc32c.unwrap() ^ 1);
        let stale = CompactionCoordinator::validate_publication_start(
            &mut fixture.el,
            &inputs,
            &outputs,
            0,
            target_level,
        );

        // Assert: neither failed guard creates durable publication authority or removes inputs.
        assert!(matches!(replacement, Err(MidgeError::Fenced(_))));
        assert!(matches!(stale, Err(MidgeError::Fenced(_))));
        assert!(fixture.el.state.intent_log.is_empty());
        assert!(!fixture.el.publication.publication_gate.is_active());
        assert!(fixture
            .el
            .state
            .host_sst_dir()
            .join(&fixture.source)
            .exists());
        fixture
            .el
            .actors
            .compaction_actor
            .finish_publication(&mut fixture.el.state, &inputs, &[]);
        Ok(())
    })
}

#[test]
fn should_reject_fragment_publication_when_cut_or_source_neighbors_change() -> MidgeResult<()> {
    crate::failpoints::with_read_gate(|| {
        // Arrange: a real source-only worker has finished, without publishing its output.
        let mut fixture = FragmentFixture::new()?;
        let plan = fixture.plan()?;
        assert!(matches!(plan.purpose, CompactionPurpose::Fragment { .. }));
        let completion = fixture.compute(plan)?;
        fixture.el.actors.compaction_actor.join_completed_worker();
        let (inputs, outputs, target_level) = match &completion {
            RuntimeMsg::CompactionComplete {
                input_ssts,
                output_ssts,
                target_level,
                ..
            } => (input_ssts.clone(), output_ssts.clone(), *target_level),
            _ => unreachable!(),
        };
        let (_, _, good) = CompactionCoordinator::validate_publication_start(
            &mut fixture.el,
            &inputs,
            &outputs,
            0,
            target_level,
        )?;
        let mut collapsed = vec![good[0].clone()];
        collapsed[0].largest_key = good.last().unwrap().largest_key.clone();

        // Act: a contained but unsplit replacement fails; a new overlapping neighbor also fences.
        let missing_cut = CompactionCoordinator::validate_fragment_replacement(
            &fixture.el,
            &inputs,
            &collapsed,
            0,
            target_level,
        );
        let neighbor = fixture.seed(1, 200, &[b"b0", b"e0"], 128)?;
        let changed_layout = CompactionCoordinator::validate_publication_start(
            &mut fixture.el,
            &inputs,
            &outputs,
            0,
            target_level,
        );

        // Assert: changed topology cannot turn a source-only rewrite into overlap repair.
        assert!(matches!(missing_cut, Err(MidgeError::Fenced(_))));
        assert!(matches!(changed_layout, Err(MidgeError::Fenced(_))));
        assert!(fixture.el.state.intent_log.is_empty());
        assert!(!fixture.el.publication.publication_gate.is_active());
        assert!(fixture
            .el
            .state
            .manifest
            .files
            .iter()
            .any(|file| file.same_identity(&neighbor)));
        assert!(fixture
            .el
            .state
            .manifest
            .files
            .iter()
            .any(|file| file.name == fixture.source));
        fixture
            .el
            .actors
            .compaction_actor
            .finish_publication(&mut fixture.el.state, &inputs, &[]);
        Ok(())
    })
}

#[cfg(feature = "failpoints")]
#[test]
fn should_recover_fragment_when_publication_stops_after_durable_manifest_batch() -> MidgeResult<()>
{
    // Arrange: real source fragmentation has completed under the ordinary publication owner.
    let (mut fixture, expected, completion) = crate::failpoints::with_read_gate(|| {
        let mut fixture = FragmentFixture::new()?;
        let names = fixture
            .el
            .state
            .manifest
            .files
            .iter()
            .map(|file| file.name.clone())
            .collect::<Vec<_>>();
        let expected = fixture.raw_rows(&names)?;
        let plan = fixture.plan()?;
        assert!(matches!(plan.purpose, CompactionPurpose::Fragment { .. }));
        let completion = fixture.compute(plan)?;
        fixture.el.actors.compaction_actor.join_completed_worker();
        Ok::<_, MidgeError>((fixture, expected, completion))
    })?;
    let _guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    let (inputs, outputs) = match &completion {
        RuntimeMsg::CompactionComplete {
            input_ssts,
            output_ssts,
            ..
        } => (input_ssts.clone(), output_ssts.clone()),
        _ => unreachable!(),
    };
    fail::cfg(
        "midge::compaction::inject_failure_after_manifest_batch",
        "return",
    )
    .expect("interrupt actual durable manifest batch");

    // Act: leave the new manifest durable while its recovery intent remains.
    let (_, unused) = crossbeam::channel::unbounded();
    fixture.el.handle_runtime_msg(completion, &unused);
    fail::remove("midge::compaction::inject_failure_after_manifest_batch");

    // Assert: a fresh runtime reconciles the same intent and preserves every raw version.
    assert!(fixture
        .el
        .state
        .has_compaction_publication_intent(&inputs, &outputs));
    assert!(fixture.el.authority.compaction_fence.is_degraded());
    assert!(outputs
        .iter()
        .all(|name| fixture.el.state.host_sst_dir().join(name).exists()));
    assert!(!fixture
        .el
        .state
        .manifest
        .files
        .iter()
        .any(|file| file.name == fixture.source));
    assert_eq!(fixture.recover_rows()?, expected);
    scenario.teardown();
    Ok(())
}

#[test]
fn should_start_next_deep_level_job_after_publication_without_flush_or_timer() -> MidgeResult<()> {
    // Arrange
    let mut fixture = FragmentFixture::new()?;
    fixture.seed(1, 8, &[b"z0"], 128)?;
    fixture.persist()?;
    fixture
        .el
        .actors
        .compaction_actor
        .set_l0_file_count_threshold(1);
    fixture
        .el
        .pipeline
        .background_compaction_schedule
        .defer_for(Duration::from_secs(3600));
    assert!(!fixture.el.state.has_any_critical_l0_debt());
    let first = fixture
        .el
        .actors
        .compaction_actor
        .check_compaction(&fixture.el.state)?
        .expect("first deep-level debt");
    fixture.el.launch_compaction(first)?;
    let completion = fixture
        .completions
        .recv_timeout(WAIT)
        .expect("first compute completion");
    assert!(matches!(
        completion,
        RuntimeMsg::CompactionComplete {
            succeeded: true,
            ..
        }
    ));

    // Act
    fixture.publish(completion);
    fixture.el.run_background_compaction_maintenance_if_due();
    let next = fixture
        .completions
        .recv_timeout(WAIT)
        .expect("follow-up before the one-hour timer");

    // Assert: a real second job publishes and both generations survive restart.
    assert!(matches!(
        next,
        RuntimeMsg::CompactionComplete {
            succeeded: true,
            ..
        }
    ));
    fixture.publish(next);
    let recovered = fixture.recover_rows()?;
    for key in SOURCE_KEYS {
        assert!(recovered.iter().any(|row| row.key == key && row.seq == 7));
    }
    assert!(recovered.iter().any(|row| row.key == b"z0"));
    Ok(())
}
