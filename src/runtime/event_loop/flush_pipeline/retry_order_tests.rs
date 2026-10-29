//! Retained-generation ordering through real flush workers and publication.

use super::*;
use crate::common::{MidgeResult, OperationDeadline};
use crate::metadata::frontiers::FamilyFrontier;
use std::sync::Mutex;
use std::time::Instant;

struct HeldRetryClock(Mutex<Instant>);

impl crate::runtime::retry_schedule::RetryClock for HeldRetryClock {
    fn now(&self) -> Instant {
        *self.0.lock().unwrap()
    }
}

impl HeldRetryClock {
    fn advance(&self, duration: Duration) {
        *self.0.lock().unwrap() += duration;
    }
}

struct Fixture {
    el: EventLoop,
    directory: tempfile::TempDir,
    other: u32,
}

impl Fixture {
    fn new() -> MidgeResult<Self> {
        let directory = tempfile::tempdir()?;
        let mut state = crate::runtime::RuntimeState::new(directory.path().to_path_buf(), false);
        let other = state.create_cf("other".into())?;
        let checkpoint = state
            .manifest_store
            .save_snapshot_for(Origin::Recovery, &state.manifest)?;
        state.manifest.adopt_checkpoint(checkpoint);
        let el = EventLoop::new(
            state,
            false,
            Arc::new(crate::runtime::ResponseRouter::new()),
            crate::runtime::RuntimeConfig {
                background_compaction: false,
                flush_concurrency: 2,
                ..crate::runtime::RuntimeConfig::default()
            },
            crate::runtime::event_loop::FlushWorkerMode::Inline,
        )?;
        Ok(Self {
            el,
            directory,
            other,
        })
    }

    fn freeze(&mut self, cf_id: u32, sequence: u64, key: &[u8]) -> MidgeResult<u64> {
        self.el.state.sequence = sequence;
        self.el.state.applied_sequence = sequence;
        self.el.state.get_cf(cf_id).unwrap().memtable.put_with_seq(
            key.to_vec(),
            key.to_vec(),
            sequence,
            None,
        )?;
        Ok(self.el.freeze_active_memtable(cf_id)?.unwrap())
    }

    fn progress(&mut self) {
        self.el.schedule_next_flush_worker();
        self.el.drain_inline_flush_worker();
    }

    fn fail_oldest(&mut self, clock: Arc<HeldRetryClock>) -> MidgeResult<u64> {
        let oldest = self.freeze(0, 1, b"oldest")?;
        self.el
            .state
            .immutable_flush_by_id_mut(oldest)
            .unwrap()
            .1
            .retry = crate::runtime::retry_schedule::RetrySchedule::with_clock(
            Duration::from_millis(10),
            clock,
        );
        fail::cfg("midge::manifest::inject_no_space_on_add_sst_edit", "return").unwrap();
        self.progress();
        fail::remove("midge::manifest::inject_no_space_on_add_sst_edit");
        let retained = self.el.state.immutable_flush_by_id(oldest).unwrap().1;
        assert_eq!(retained.phase, ImmutableFlushPhase::RetryPending);
        assert_eq!(retained.failures, 1);
        Ok(oldest)
    }

    fn frontier(&self, cf_id: u32) -> FamilyFrontier {
        self.el.state.manifest.flush_frontiers().get(cf_id).unwrap()
    }

    fn verify_recovery(self) -> MidgeResult<()> {
        let Self {
            el,
            directory,
            other,
        } = self;
        drop(el);
        let mut engine = crate::Engine::open(
            crate::OpenOptions::local(directory.path())
                .background_compaction(false)
                .build()?,
        )?;
        for (cf_id, keys) in [
            (0, vec![b"oldest".as_slice(), b"younger"]),
            (other, vec![b"other"]),
        ] {
            let tx = engine.begin_tx(cf_id, crate::TransactionMode::ReadOnly)?;
            for key in keys {
                assert_eq!(tx.get(key)?.as_deref(), Some(key));
            }
        }
        engine.shutdown(Duration::from_secs(5))?;
        Ok(())
    }
}

#[test]
fn should_keep_younger_family_flush_behind_retained_retry_while_other_family_publishes(
) -> MidgeResult<()> {
    // Arrange: the real AddSst failure retains the original completed output.
    let _guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    let mut fixture = Fixture::new()?;
    let clock = Arc::new(HeldRetryClock(Mutex::new(Instant::now())));
    let oldest = fixture.fail_oldest(clock.clone())?;
    let retained = fixture.el.state.immutable_flush_by_id(oldest).unwrap().1;
    let original_name = retained.sst_name.clone().unwrap();
    let retry_delay = retained.retry.remaining().unwrap();
    let original_started = retained.started_at;
    let younger = fixture.freeze(0, 2, b"younger")?;
    fixture.freeze(fixture.other, 3, b"other")?;
    let request_id = 91_987;
    let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
    let response = fixture.el.routing.router.register_with_deadline(
        request_id,
        "FlushMemtable",
        Instant::now(),
        deadline,
    );
    fixture.el.publication.flush_barrier_waiters.insert(
        0,
        vec![crate::runtime::event_loop::flush::FlushBarrierWaiter {
            request_id,
            frontier: 2,
        }],
    );

    // Act: the same-family head cannot become ready until its clock advances.
    fixture.progress();

    // Assert: unrelated physical publication progresses without covering gaps.
    assert_eq!(
        fixture.frontier(0),
        FamilyFrontier::Live {
            through_sequence: 0
        }
    );
    assert_eq!(
        fixture.frontier(fixture.other),
        FamilyFrontier::Live {
            through_sequence: 3
        }
    );
    assert!(fixture
        .el
        .state
        .immutable_flush_by_id(younger)
        .unwrap()
        .1
        .sst_name
        .is_none());
    assert!(!fixture.el.state.has_due_immutable_flush());
    assert_eq!(
        fixture.el.state.flush_retry_deadline_timeout(),
        Some(retry_delay)
    );
    assert_eq!(
        fixture
            .el
            .routing
            .router
            .request_deadline(request_id, Duration::ZERO),
        Some(deadline)
    );
    assert!(response.try_recv().is_err());
    let retained = fixture.el.state.immutable_flush_by_id(oldest).unwrap().1;
    assert_eq!(retained.sst_name.as_ref(), Some(&original_name));
    assert_eq!(retained.sequence, 1);
    assert_eq!(retained.started_at, original_started);

    clock.advance(retry_delay);
    fixture.progress();
    assert_eq!(
        fixture.frontier(0),
        FamilyFrontier::Live {
            through_sequence: 2
        }
    );
    assert!(matches!(
        response.recv_timeout(Duration::from_secs(5)),
        Ok(RuntimeResponse::Ok { request_id: 91_987 })
    ));
    let mut names = fixture
        .el
        .state
        .manifest
        .files
        .iter()
        .filter(|file| file.cf_id == 0)
        .map(|file| file.name.clone())
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(
        names,
        [original_name, crate::cloud_layout::file_name(0, 0, 2)]
    );
    scenario.teardown();
    fixture.verify_recovery()
}
