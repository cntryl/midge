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
fn should_retain_accepted_compaction_and_original_deadline_until_capture_memory_is_released(
) -> MidgeResult<()> {
    // Arrange: compute may finish, but another owner occupies the capture pool.
    let (mut fixture, memory, input) = capture_fixture()?;
    let held = memory.reserve(memory.limit(), "held metadata workspace")?;
    let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
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
}

#[test]
fn should_fail_expired_capture_admission_without_deleting_accepted_input() -> MidgeResult<()> {
    // Arrange: the original short allowance is consumed by actual contention.
    let (mut fixture, memory, input) = capture_fixture()?;
    let held = memory.reserve(memory.limit(), "held metadata workspace")?;
    let deadline = OperationDeadline::from_budget(Duration::from_secs(2));
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
}
