use super::*;
use crate::common::resource_budget::{ResourceBudget, ResourceReservation};
use crate::runtime::RuntimeResponse;

struct ContendedFlush {
    el: EventLoop,
    memory: ResourceBudget,
    held: Option<ResourceReservation>,
    response: crossbeam::channel::Receiver<RuntimeResponse>,
    id: u64,
}

impl ContendedFlush {
    fn new(timeout: Duration) -> MidgeResult<Self> {
        let mut el =
            crate::runtime::event_loop::tests::create_test_cloud_event_loop(Default::default())?;
        el.prefetch_flush_sst_names_if_due();
        el.pipeline.inline_flush_worker = false;
        el.routing.runtime_response_timeout = timeout;
        let memory = el
            .actors
            .cloud_coordinator
            .hybrid_storage
            .as_ref()
            .unwrap()
            .maintenance_memory()
            .unwrap();
        let held = memory.reserve(memory.limit(), "held sibling maintenance work")?;
        el.state.get_cf(0).unwrap().memtable.put_with_seq(
            b"key".to_vec(),
            b"value".to_vec(),
            1,
            None,
        )?;
        el.state.sequence = 1;
        el.state.applied_sequence = 1;
        let id = el.freeze_active_memtable(0)?.unwrap();
        let response = el
            .routing
            .router
            .register(91001, "flush awaiting metadata capture");
        el.publication.flush_barrier_waiters.insert(
            0,
            vec![crate::runtime::event_loop::flush::FlushBarrierWaiter {
                request_id: 91001,
                frontier: 1,
            }],
        );
        el.schedule_next_flush_worker();
        let deadline = Instant::now() + Duration::from_secs(5);
        while el.pipeline.pending_flush_mirrors.is_empty() {
            el.drain_flush_worker_results();
            assert!(
                Instant::now() < deadline,
                "actual uploaded flush must reach capture admission"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok(Self {
            el,
            memory,
            held: Some(held),
            response,
            id,
        })
    }

    fn retry_due(&mut self) {
        std::thread::sleep(CAPTURE_RETRY_DELAY + Duration::from_millis(1));
        self.el.retry_flush_mirrors_if_due();
    }

    fn assert_local_bytes_retained(&mut self) -> MidgeResult<()> {
        let (_, flush) = self.el.state.immutable_flush_by_id(self.id).unwrap();
        assert!(flush.built.is_some());
        assert!(self
            .el
            .state
            .manifest_has_file(flush.sst_name.as_ref().unwrap()));
        let snapshot = self.el.create_read_snapshot(0).unwrap();
        assert_eq!(
            snapshot.get(b"key", 1)?.as_deref(),
            Some(b"value".as_slice())
        );
        Ok(())
    }
}

#[test]
fn should_retain_uploaded_flush_owner_and_waiters_until_shared_capture_memory_is_released(
) -> MidgeResult<()> {
    // Arrange: real build, upload and local edit have completed; only the
    // configured common maintenance pool prevents control-cut admission.
    let mut fixture = ContendedFlush::new(Duration::from_secs(5))?;
    let accepted = fixture.el.pipeline.pending_flush_mirrors[0].task.deadline;
    let published = fixture.el.state.flush_metrics.publish_count;
    fixture.assert_local_bytes_retained()?;
    // Act: repeated admission contention preserves this exact logical owner.
    fixture.retry_due();
    assert_eq!(fixture.el.pipeline.pending_flush_mirrors.len(), 1);
    assert_eq!(
        fixture.el.pipeline.pending_flush_mirrors[0].task.deadline,
        accepted
    );
    assert!(fixture.response.try_recv().is_err());
    assert!(fixture.el.actors.flush_actor.owns_pipeline(0, fixture.id));
    assert!(!fixture.el.actors.flush_actor.is_inflight());
    assert_eq!(
        fixture
            .el
            .state
            .immutable_flush_by_id(fixture.id)
            .unwrap()
            .1
            .failures,
        0
    );
    assert!(!fixture.el.publication.publication_gate.is_active());
    drop(fixture.held.take());
    fixture.retry_due();
    let deadline = Instant::now() + Duration::from_secs(5);
    while fixture.el.state.immutable_flush_by_id(fixture.id).is_some() {
        fixture.el.drain_flush_worker_results();
        assert!(
            Instant::now() < deadline,
            "admitted mirror settles the original flush"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    // Assert: no SST re-upload or second publish phase was needed; the waiter
    // completes only after the real mirror, and every owner is released.
    assert!(matches!(
        fixture.response.recv_timeout(Duration::from_secs(1)),
        Ok(RuntimeResponse::Ok { request_id: 91001 })
    ));
    assert_eq!(fixture.el.state.flush_metrics.publish_count, published);
    assert!(fixture.el.pipeline.pending_flush_mirrors.is_empty());
    assert!(!fixture.el.actors.flush_actor.owns_pipeline(0, fixture.id));
    assert!(fixture.memory.peak() <= fixture.memory.limit());
    Ok(())
}

#[test]
fn should_expire_original_flush_mirror_deadline_without_resetting_queued_allowance(
) -> MidgeResult<()> {
    // Arrange
    let mut fixture = ContendedFlush::new(Duration::from_secs(1))?;
    let accepted = fixture.el.pipeline.pending_flush_mirrors[0].task.deadline;
    // Act: a different default cannot extend this admitted operation.
    fixture.el.routing.runtime_response_timeout = Duration::from_secs(30);
    std::thread::sleep(accepted.remaining() + Duration::from_millis(1));
    fixture.el.retry_flush_mirrors_if_due();
    // Assert: timeout is terminal for the original caller, while the accepted
    // SST, local edit and immutable data survive for conservative recovery.
    assert!(matches!(
        fixture.response.recv_timeout(Duration::from_secs(1)),
        Ok(RuntimeResponse::Error {
            error: crate::MidgeError::Timeout(_),
            ..
        })
    ));
    assert!(fixture.el.pipeline.pending_flush_mirrors.is_empty());
    assert!(!fixture.el.actors.flush_actor.owns_pipeline(0, fixture.id));
    assert_eq!(
        fixture
            .el
            .state
            .immutable_flush_by_id(fixture.id)
            .unwrap()
            .1
            .failures,
        1
    );
    fixture.assert_local_bytes_retained()?;
    Ok(())
}
