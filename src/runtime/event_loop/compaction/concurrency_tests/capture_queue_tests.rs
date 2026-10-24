use super::*;
use crate::common::resource_budget::ResourceBudget;

fn capture_fixture() -> MidgeResult<(ConcurrentFixture, ResourceBudget, String)> {
    let mut fixture = ConcurrentFixture::new(16 * 1024 * 1024, false)?;
    let input = fixture.file(1, 1, b"a", b"accepted", 1)?;
    fixture.persist()?;
    fixture
        .event_loop
        .actors
        .cloud_coordinator
        .cloud_metadata_storage = Some(Arc::new(crate::storage::cloud::CloudStorage::with_mock()));
    let memory = ResourceBudget::new(1024 * 1024);
    fixture
        .event_loop
        .publication
        .metadata_publication_lock
        .configure_capture_memory(memory.clone());
    Ok((fixture, memory, input))
}

fn wait_for_capture_retry(fixture: &mut ConcurrentFixture) {
    let until = Instant::now() + Duration::from_secs(5);
    while !CompactionCoordinator::has_pending_capture(&fixture.event_loop) {
        fixture.progress();
        assert!(
            Instant::now() < until,
            "capture did not retain its contention"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn should_preserve_accepted_compaction_when_capture_memory_is_temporarily_owned() -> MidgeResult<()>
{
    crate::failpoints::with_read_gate(|| {
        // Arrange: compute may finish, but another owner occupies the capture pool.
        let (mut fixture, memory, input) = capture_fixture()?;
        let held = memory.reserve(memory.limit(), "held metadata workspace")?;
        let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
        let _caller = fixture
            .event_loop
            .routing
            .router
            .register(91_971, "CompactAll");
        fixture
            .event_loop
            .state
            .pending_compaction_waits
            .insert(91_971, deadline);
        let plan = fixture.plan(&input, 1, 2);
        fixture.event_loop.launch_compaction(plan)?;

        // Act: retry admission repeatedly without resetting the accepted deadline.
        wait_for_capture_retry(&mut fixture);
        for _ in 0..3 {
            std::thread::sleep(crate::runtime::event_loop::HYBRID_STORAGE_POLL_INTERVAL);
            fixture.progress();
        }

        // Assert: the native compute owner, input and request allowance survive.
        let pending = fixture
            .event_loop
            .publication
            .compaction_publication
            .get()
            .unwrap();
        assert_eq!(pending.capture_retry.unwrap().deadline, deadline);
        assert_eq!(pending.manual_deadline, Some(deadline));
        assert_eq!(fixture.event_loop.actors.compaction_actor.active_count(), 1);
        assert!(!fixture
            .event_loop
            .actors
            .compaction_publish_actor
            .is_inflight());
        assert!(fixture.event_loop.state.manifest_has_file(&input));
        assert!(!fixture.event_loop.publication.publication_gate.is_active());
        fixture.assert_rows(&[(b"a", b"accepted")])?;
        drop(held);
        fixture.drain();
        assert!(!fixture.event_loop.state.manifest_has_file(&input));
        assert_eq!(memory.used(), 0);
        fixture.restart(&[(b"a", b"accepted")])
    })
}

#[test]
fn should_fail_expired_capture_admission_without_deleting_accepted_input() -> MidgeResult<()> {
    crate::failpoints::with_read_gate(|| {
        // Arrange: the original short allowance is consumed by actual contention.
        let (mut fixture, memory, input) = capture_fixture()?;
        let held = memory.reserve(memory.limit(), "held metadata workspace")?;
        let deadline = OperationDeadline::from_budget(Duration::from_secs(2));
        let _caller = fixture
            .event_loop
            .routing
            .router
            .register(91_972, "CompactAll");
        fixture
            .event_loop
            .state
            .pending_compaction_waits
            .insert(91_972, deadline);
        let plan = fixture.plan(&input, 1, 2);
        fixture.event_loop.launch_compaction(plan)?;
        wait_for_capture_retry(&mut fixture);

        // Act: keep progressing until the actual absolute deadline expires.
        let until = Instant::now() + Duration::from_secs(5);
        while fixture
            .event_loop
            .publication
            .compaction_publication
            .is_active()
        {
            fixture.progress();
            assert!(
                Instant::now() < until,
                "expired capture must settle without a fresh allowance"
            );
            std::thread::sleep(Duration::from_millis(1));
        }

        // Assert: no provider task started or authority switch deleted the source.
        assert!(deadline.is_expired());
        assert!(fixture.event_loop.state.manifest_has_file(&input));
        assert!(fixture
            .event_loop
            .state
            .host_sst_dir()
            .join(&input)
            .exists());
        assert_eq!(fixture.event_loop.actors.compaction_actor.active_count(), 0);
        assert!(!fixture
            .event_loop
            .actors
            .compaction_publish_actor
            .is_inflight());
        fixture.assert_rows(&[(b"a", b"accepted")])?;
        drop(held);
        fixture.restart(&[(b"a", b"accepted")])
    })
}

#[test]
fn should_retain_sibling_compute_owner_when_another_capture_retry_expires() -> MidgeResult<()> {
    // Arrange: A waits for capture memory; B owns a disjoint native worker.
    let guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    let (mut fixture, memory, first) = capture_fixture()?;
    let second = fixture.file(3, 2, b"z", b"sibling", 2)?;
    fixture.persist()?;
    fixture.event_loop.routing.runtime_response_timeout = Duration::from_secs(2);
    let held_memory = memory.reserve(memory.limit(), "held metadata workspace")?;
    let first_plan = fixture.plan(&first, 1, 2);
    fixture.event_loop.launch_compaction(first_plan)?;
    wait_for_capture_retry(&mut fixture);
    let deadline = fixture
        .event_loop
        .publication
        .compaction_publication
        .get()
        .unwrap()
        .capture_retry
        .unwrap()
        .deadline;
    let held_compute = HeldComputes::new();
    let second_plan = fixture.plan(&second, 3, 4);
    fixture.event_loop.launch_compaction(second_plan)?;
    held_compute.enter();
    assert_eq!(fixture.event_loop.actors.compaction_actor.active_count(), 2);

    // Act: expire A's original allowance while the facade still selects B.
    while !deadline.is_expired() {
        std::thread::sleep(Duration::from_millis(1));
    }
    CompactionCoordinator::retry_publication_capture(&mut fixture.event_loop);

    // Assert: only A settles; B's exact registry/input ownership survives.
    assert!(!fixture
        .event_loop
        .publication
        .compaction_publication
        .is_active());
    assert_eq!(fixture.event_loop.actors.compaction_actor.active_count(), 1);
    let protected = fixture
        .event_loop
        .actors
        .compaction_actor
        .protected_sst_names();
    assert!(
        protected.contains(&second),
        "held sibling lost its input guard"
    );
    assert!(!protected.contains(&first), "expired owner was not settled");
    assert!(fixture
        .event_loop
        .state
        .compaction
        .compacting_ssts
        .contains(&second));
    assert!(!fixture
        .event_loop
        .state
        .compaction
        .compacting_ssts
        .contains(&first));
    assert!(fixture.event_loop.state.manifest_has_file(&first));
    assert!(fixture.event_loop.state.manifest_has_file(&second));
    drop(held_memory);
    held_compute.release.send(()).unwrap();
    fixture.drain();
    assert!(!fixture.event_loop.state.manifest_has_file(&second));
    drop(held_compute);
    scenario.teardown();
    drop(guard);
    fixture.restart(&[(b"a", b"accepted"), (b"z", b"sibling")])
}
