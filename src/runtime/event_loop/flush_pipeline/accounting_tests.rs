//! Real actor completions and immutable ownership; no seeded successful delta.

use super::*;
use crate::common::{MidgeError, MidgeResult, OperationDeadline};
use crate::metadata::accounting::{Medium, Origin, Snapshot};
use crate::types::KeyState;

const KEY: &[u8] = b"accounted-immutable";
const VALUE: &[u8] = b"actual-built-published-installed-row";
const WAIT: Duration = Duration::from_secs(10);

struct Fixture {
    event_loop: EventLoop,
    _directory: tempfile::TempDir,
}

impl Fixture {
    fn new(flush_memory_limit: usize) -> MidgeResult<Self> {
        let directory = tempfile::tempdir()?;
        let state = crate::runtime::RuntimeState::new(directory.path().to_path_buf(), false);
        let config = crate::runtime::RuntimeConfig {
            background_compaction: false,
            flush_memory_limit,
            ..crate::runtime::RuntimeConfig::default()
        };
        let event_loop = EventLoop::new(
            state,
            false,
            Arc::new(crate::runtime::ResponseRouter::new()),
            config,
            crate::runtime::event_loop::FlushWorkerMode::Inline,
        )?;
        Ok(Self {
            event_loop,
            _directory: directory,
        })
    }

    fn freeze(&mut self) -> MidgeResult<u64> {
        self.event_loop.state.sequence = 1;
        self.event_loop
            .state
            .get_cf(0)
            .expect("default family")
            .memtable
            .put_with_seq(KEY.to_vec(), VALUE.to_vec(), 1, None)?;
        Ok(self
            .event_loop
            .freeze_active_memtable(0)?
            .expect("nonempty actual memtable"))
    }

    fn snapshot(&self) -> Snapshot {
        self.event_loop
            .state
            .metadata_accounting()
            .handle()
            .snapshot()
    }

    fn receive(&self) -> FlushWorkerResult {
        self.event_loop
            .flush_worker_result_rx
            .recv_timeout(WAIT)
            .expect("actual owned worker completion")
    }

    fn verify_installed(&self, delta: &FlushPublicationDelta) -> MidgeResult<()> {
        assert!(self
            .event_loop
            .state
            .immutable_flush_by_id(delta.identity.flush_id)
            .is_none());
        let installed: Vec<_> = self
            .event_loop
            .state
            .manifest
            .files
            .iter()
            .filter(|file| file.name == delta.file_meta.name)
            .collect();
        assert_eq!(installed.len(), 1);
        let fs = Arc::new(
            crate::io::RealFs::new(&self.event_loop.state.sst_dir).map_err(FsError::into_midge)?,
        );
        let reader = crate::sst::FsSstFactoryIo::new(fs, 64 * 1024)
            .open(std::path::Path::new(&delta.file_meta.name))?;
        assert_eq!(
            reader.get_state(KEY)?,
            KeyState::Value(
                bytes::Bytes::from_static(VALUE),
                1,
                None,
                crate::types::EntryType::Put
            )
        );
        assert_eq!(
            std::fs::metadata(self.event_loop.state.sst_dir.join(&delta.file_meta.name))?.len(),
            delta.file_meta.size_bytes
        );
        Ok(())
    }
}

fn ordinary(snapshot: &Snapshot) -> &crate::metadata::accounting::Counters {
    &snapshot
        .bucket(Origin::OrdinaryLocalFlush, Medium::Persistent)
        .counters
}

fn successful_publish(fixture: &mut Fixture) -> FlushPublicationDelta {
    let FlushWorkerResult::Publish(completion) = fixture.receive() else {
        panic!("expected real publication completion");
    };
    let delta = completion
        .result
        .as_ref()
        .expect("real publisher success")
        .clone();
    fixture
        .event_loop
        .handle_flush_worker_result(FlushWorkerResult::Publish(completion));
    delta
}

#[test]
fn should_credit_installed_sst_once_when_actual_mirror_receipt_is_replayed() -> MidgeResult<()> {
    crate::failpoints::with_read_gate(|| {
        // Arrange: a real actor writes, publishes and mirrors this local row.
        let mut fixture = Fixture::new(8 * 1024 * 1024)?;
        let before = fixture.snapshot();
        let flush_id = fixture.freeze()?;
        fixture.event_loop.schedule_next_flush_worker();
        let FlushWorkerResult::Build(build) = fixture.receive() else {
            panic!("expected actual build completion");
        };
        assert!(build.result.is_ok(), "build result: {:?}", build.result);
        assert_eq!(ordinary(&fixture.snapshot()).publication_attempts, 0);
        fixture
            .event_loop
            .handle_flush_worker_result(FlushWorkerResult::Build(build));
        let delta = successful_publish(&mut fixture);
        let FlushWorkerResult::Mirror(mirror) = fixture.receive() else {
            panic!("expected actual mirror completion");
        };
        assert!(
            matches!(&mirror.result, Ok(false)),
            "actual mirror: {:?}",
            mirror.result
        );
        let replay = FlushMirrorCompletion {
            delta: mirror.delta.clone(),
            reservation: mirror.reservation,
            result: mirror.result.as_ref().copied().map_err(MidgeError::replay),
        };

        // Act: install the genuine receipt, then deliver its exact duplicate.
        fixture
            .event_loop
            .handle_flush_worker_result(FlushWorkerResult::Mirror(mirror));
        fixture.verify_installed(&delta)?;
        let installed = fixture.snapshot();
        fixture
            .event_loop
            .handle_flush_worker_result(FlushWorkerResult::Mirror(replay));

        // Assert: actual immutable removal credits bytes and timing only once.
        assert!(fixture
            .event_loop
            .state
            .immutable_flush_by_id(flush_id)
            .is_none());
        assert_eq!(
            serde_json::to_value(fixture.snapshot()).expect("serializable snapshot"),
            serde_json::to_value(&installed).expect("serializable snapshot")
        );
        let delta_metrics = installed.delta(&before).expect("same monotonic owner");
        let counters = ordinary(&delta_metrics);
        assert_eq!(counters.publication_attempts, 1);
        assert_eq!(counters.publication_failures, 0);
        assert_eq!(counters.flush_committed_count, 1);
        assert_eq!(
            counters.flush_committed_sst_bytes,
            delta.file_meta.size_bytes
        );
        assert!(counters.flush_full_publication_elapsed_ns > 0);
        assert_eq!(delta_metrics.incomplete_observations, 0);
        Ok(())
    })
}

struct FailedPublication {
    flush_id: u64,
    name: String,
    original: crate::runtime::state::FlushPublicationAccounting,
    failed: FlushPublishCompletion,
    replay: FlushPublishCompletion,
}

fn fail_actual_publication(fixture: &mut Fixture) -> MidgeResult<FailedPublication> {
    let flush_id = fixture.freeze()?;
    fixture.event_loop.schedule_next_flush_worker();
    let FlushWorkerResult::Build(build) = fixture.receive() else {
        panic!("expected actual build completion");
    };
    assert!(build.result.is_ok(), "build result: {:?}", build.result);
    let name = fixture
        .event_loop
        .state
        .immutable_flush_by_id(flush_id)
        .expect("accepted generation")
        .1
        .sst_name
        .clone()
        .expect("reserved name");
    let path = fixture.event_loop.state.sst_dir.join(&name);
    std::fs::create_dir(&path)?;
    fixture
        .event_loop
        .handle_flush_worker_result(FlushWorkerResult::Build(build));
    let original = fixture
        .event_loop
        .state
        .immutable_flush_by_id(flush_id)
        .expect("accepted publisher")
        .1
        .accounting;
    assert_eq!(original.origin, Origin::OrdinaryLocalFlush);
    assert!(original.started.is_some());
    let FlushWorkerResult::Publish(failed) = fixture.receive() else {
        panic!("expected failed actual publisher");
    };
    let error = failed
        .result
        .as_ref()
        .expect_err("directory is not a canonical SST");
    let replay = FlushPublishCompletion {
        identity: failed.identity,
        reservation: failed.reservation,
        publish_ns: failed.publish_ns,
        result: Err(error.replay()),
    };

    Ok(FailedPublication {
        flush_id,
        name,
        original,
        failed,
        replay,
    })
}

#[test]
fn should_preserve_original_publication_when_actual_retry_runs_during_shutdown() -> MidgeResult<()>
{
    crate::failpoints::with_read_gate(|| {
        // Arrange: a genuine SST build is retained after a real destination error.
        let mut fixture = Fixture::new(8 * 1024 * 1024)?;
        let before = fixture.snapshot();
        let FailedPublication {
            flush_id,
            name,
            original,
            failed,
            replay,
        } = fail_actual_publication(&mut fixture)?;
        let path = fixture.event_loop.state.sst_dir.join(&name);
        // Act: finish that real failed attempt once; retry the same immutable during drain.
        fixture
            .event_loop
            .handle_flush_worker_result(FlushWorkerResult::Publish(failed));
        let once = fixture.snapshot();
        fixture
            .event_loop
            .handle_flush_worker_result(FlushWorkerResult::Publish(replay));
        assert_eq!(
            serde_json::to_value(fixture.snapshot()).expect("serializable snapshot"),
            serde_json::to_value(&once).expect("serializable snapshot")
        );
        let retained = fixture
            .event_loop
            .state
            .immutable_flush_by_id(flush_id)
            .expect("failed generation retained")
            .1;
        assert_eq!(retained.accounting.origin, original.origin);
        assert_eq!(retained.accounting.started, original.started);
        assert!(retained.accounting.attempt_started.is_none());
        assert_eq!(retained.sst_name.as_deref(), Some(name.as_str()));
        assert!(retained
            .built
            .as_ref()
            .expect("real built output retained")
            .staging_path
            .is_file());
        assert_eq!(ordinary(&once).publication_attempts, 1);
        assert_eq!(ordinary(&once).publication_failures, 1);
        assert_eq!(ordinary(&once).flush_committed_count, 0);
        std::fs::remove_dir(&path)?;
        fixture.event_loop.state.make_immutable_flush_retry_due(0);
        fixture.event_loop.shutting_down = true;
        fixture
            .event_loop
            .schedule_next_flush_worker_during_shutdown();
        let retried = fixture
            .event_loop
            .state
            .immutable_flush_by_id(flush_id)
            .expect("retried publisher")
            .1
            .accounting;
        assert_eq!(retried.started, original.started);
        assert_eq!(retried.origin, original.origin);
        assert!(retried.attempt_started.is_some());
        let delta = successful_publish(&mut fixture);
        fixture
            .event_loop
            .drain_shutdown_flush_pipeline_within(&OperationDeadline::from_budget(WAIT))?;

        // Assert: safe actual publication is attributed to its original logical operation.
        fixture.verify_installed(&delta)?;
        assert_eq!(delta.file_meta.name, name);
        let after = fixture.snapshot().delta(&before).expect("same owner");
        assert_eq!(ordinary(&after).publication_attempts, 2);
        assert_eq!(ordinary(&after).publication_failures, 1);
        assert_eq!(ordinary(&after).flush_committed_count, 1);
        assert_eq!(
            ordinary(&after).flush_committed_sst_bytes,
            delta.file_meta.size_bytes
        );
        assert_eq!(
            after
                .bucket(Origin::Shutdown, Medium::Persistent)
                .counters
                .flush_committed_count,
            0
        );
        assert_eq!(after.incomplete_observations, 0);
        Ok(())
    })
}

#[test]
fn should_not_count_publication_when_actual_actor_build_exhausts_its_memory() -> MidgeResult<()> {
    crate::failpoints::with_read_gate(|| {
        // Arrange: the actual actor's resource budget cannot create a writer.
        let mut fixture = Fixture::new(1)?;
        let before = fixture.snapshot();
        let flush_id = fixture.freeze()?;
        fixture.event_loop.schedule_next_flush_worker();

        // Act: forward the genuinely failed build and retain the immutable row.
        let FlushWorkerResult::Build(completion) = fixture.receive() else {
            panic!("expected actual actor build failure");
        };
        assert!(
            matches!(&completion.result, Err(MidgeError::ResourceLimit(_))),
            "actual build result: {:?}",
            completion.result
        );
        fixture
            .event_loop
            .handle_flush_worker_result(FlushWorkerResult::Build(completion));

        // Assert: build work does not invent an accepted publication or committed SST.
        let retained = fixture
            .event_loop
            .state
            .immutable_flush_by_id(flush_id)
            .expect("failed build retains immutable")
            .1;
        assert_eq!(retained.accounting.origin, Origin::OrdinaryLocalFlush);
        assert!(retained.accounting.started.is_none());
        assert!(retained.accounting.attempt_started.is_none());
        assert!(retained.built.is_none());
        assert_eq!(
            retained.memtable.get_key_state_at(KEY, 1)?,
            KeyState::Value(
                bytes::Bytes::from_static(VALUE),
                1,
                None,
                crate::types::EntryType::Put
            )
        );
        let after = fixture.snapshot().delta(&before).expect("same owner");
        assert_eq!(ordinary(&after).publication_attempts, 0);
        assert_eq!(ordinary(&after).flush_committed_count, 0);
        assert_eq!(ordinary(&after).flush_committed_sst_bytes, 0);
        assert_eq!(after.incomplete_observations, 0);
        assert!(fixture.event_loop.state.manifest.files.is_empty());
        Ok(())
    })
}
