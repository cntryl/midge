use super::*;

fn idle_cloud_loop() -> crate::common::MidgeResult<EventLoop> {
    let el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    el.hybrid_storage
        .as_ref()
        .unwrap()
        .enable_ephemeral_sst_cache(64 * 1024 * 1024);
    Ok(el)
}

#[test]
fn should_complete_manual_waiter_when_prune_event_precedes_worker_exit(
) -> crate::common::MidgeResult<()> {
    // Arrange: hold the worker after it queues its terminal event. Consuming
    // that event cannot yet reap the handle or release the publication gate.
    let mut el = idle_cloud_loop()?;
    let response = el.router.register(91_008, "CompactAll");
    el.state
        .pending_compaction_waits
        .lock()
        .insert(91_008, "CompactAll".into());
    el.cloud_wal.prune_inflight.insert(42);
    el.publication_gate.active = true;
    let storage = el.hybrid_storage.clone().unwrap();
    let (event_sent, event_ready) = std::sync::mpsc::channel();
    let (release_worker, release) = std::sync::mpsc::channel();
    el.cloud_wal_prune_worker = Some(std::thread::spawn(move || {
        storage.queue_cloud_wal_prune_attempt_failed(42, "yielded proof work".into());
        event_sent.send(()).expect("report terminal event queued");
        release.recv().expect("release completed worker");
    }));
    event_ready.recv_timeout(Duration::from_secs(1)).unwrap();
    el.tick_hybrid_storage();
    el.drain_hybrid_storage_events();
    assert!(el.cloud_wal.prune_inflight.is_empty());
    assert!(!el.cloud_wal_prune_worker.as_ref().unwrap().is_finished());
    assert!(el.publication_gate.active);
    let active_worker_poll = el.idle_progress_timeout();
    let (requests, request_rx) = crossbeam::channel::unbounded();

    // Act: the worker exits without another event or user request. Only the
    // runtime's own wake policy can finish the queued manual obligation.
    let runtime = std::thread::spawn(move || {
        el.run(&request_rx);
        el
    });
    release_worker.send(()).unwrap();
    let completion = response.recv_timeout(Duration::from_secs(1));
    // Always release the test loop, including the RED timeout path.
    drop(requests);
    let el = runtime
        .join()
        .expect("runtime exits after request channel closes");

    // Assert
    assert!(matches!(completion, Ok(RuntimeResponse::Ok { request_id: 91_008 })),
        "manual completion needs no additional request or 30-second maintenance tick: {completion:?}");
    assert!(active_worker_poll.is_some_and(|timeout| timeout <= Duration::from_millis(5)));
    assert!(el.cloud_wal_prune_worker.is_none());
    assert!(!el.publication_gate.active);
    assert!(el.state.pending_compaction_waits.lock().is_empty());
    Ok(())
}

#[test]
fn should_observe_finished_prune_worker_when_no_other_work_can_wake_runtime(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = idle_cloud_loop()?;
    el.publication_gate.active = true;
    el.cloud_wal_prune_worker = Some(std::thread::spawn(|| {}));
    let deadline = Instant::now() + Duration::from_secs(1);
    while !el.cloud_wal_prune_worker.as_ref().unwrap().is_finished() {
        assert!(Instant::now() < deadline, "worker must finish");
        std::thread::yield_now();
    }

    // Act
    let actionable = el.has_actionable_work();

    // Assert
    assert!(
        actionable,
        "a completed handle must wake publication-gate cleanup"
    );
    Ok(())
}
