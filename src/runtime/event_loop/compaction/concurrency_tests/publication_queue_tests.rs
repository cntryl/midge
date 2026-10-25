use super::*;

const PUBLICATION_HOOK: &str = "slice7::after_compaction_output_durable_before_manifest_publish";

struct HeldPublication {
    entered: crossbeam::channel::Receiver<()>,
    release: crossbeam::channel::Sender<()>,
}

impl HeldPublication {
    fn new() -> Self {
        let (entered, arrived) = crossbeam::channel::bounded(1);
        let (release, resumed) = crossbeam::channel::unbounded();
        let first = std::sync::atomic::AtomicBool::new(true);
        fail::cfg_callback(PUBLICATION_HOOK, move || {
            if first.swap(false, Ordering::AcqRel) {
                entered.send(()).expect("publication reached");
                resumed
                    .recv_timeout(Duration::from_secs(10))
                    .expect("publication released");
            }
        })
        .expect("hold actual publication worker");
        Self {
            entered: arrived,
            release,
        }
    }
}

impl Drop for HeldPublication {
    fn drop(&mut self) {
        let _ = self.release.send(());
        fail::remove(PUBLICATION_HOOK);
    }
}

fn wait_for_publication(fixture: &mut ConcurrentFixture, held: &HeldPublication) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        fixture.progress();
        if held.entered.try_recv().is_ok() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "first publisher did not reach hold"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn should_retain_ready_sibling_compute_while_first_publisher_has_released_local_gate(
) -> MidgeResult<()> {
    // Arrange: two disjoint physical jobs, with only the first publisher held.
    let _guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    let mut fixture = ConcurrentFixture::new(16 * 1024 * 1024, false)?;
    let first = fixture.file(1, 1, b"a", b"first", 1)?;
    let second = fixture.file(3, 2, b"z", b"second", 2)?;
    fixture.persist()?;
    let held = HeldPublication::new();
    let first_plan = fixture.plan(&first, 1, 2);
    fixture.event_loop.launch_compaction(first_plan)?;
    wait_for_publication(&mut fixture, &held);
    let token = fixture
        .event_loop
        .publication
        .compaction_publication
        .get()
        .unwrap()
        .token
        .clone();
    assert!(!fixture.event_loop.publication.publication_gate.is_active());

    // Act: dispatch the sibling's real notification and polling while the
    // sole publication slot is occupied. Its native result must remain ready.
    let second_plan = fixture.plan(&second, 3, 4);
    fixture.event_loop.launch_compaction(second_plan)?;
    let deadline = Instant::now() + Duration::from_secs(5);
    while !fixture
        .event_loop
        .actors
        .compaction_actor
        .has_ready_compute()
    {
        fixture.progress();
        assert!(Instant::now() < deadline, "sibling compute did not finish");
        std::thread::sleep(Duration::from_millis(1));
    }
    fixture.progress();

    // Assert: neither the first publication nor sibling input is settled by
    // the other job. Releasing the publisher allows both exact jobs to finish.
    assert_eq!(
        fixture
            .event_loop
            .publication
            .compaction_publication
            .get()
            .unwrap()
            .token,
        token
    );
    assert_eq!(fixture.event_loop.actors.compaction_actor.active_count(), 2);
    assert!(fixture
        .event_loop
        .actors
        .compaction_actor
        .has_ready_compute());
    assert!(fixture.event_loop.state.manifest_has_file(&first));
    assert!(fixture.event_loop.state.manifest_has_file(&second));
    fixture.assert_rows(&[(b"a", b"first"), (b"z", b"second")])?;
    held.release
        .send(())
        .expect("release actual first publisher");
    fixture.drain();
    drop(held);
    fixture.restart(&[(b"a", b"first"), (b"z", b"second")])?;
    scenario.teardown();
    Ok(())
}
