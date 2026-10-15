use super::*;
use crate::storage::{StorageBackend, StorageCallback};

/// Several versions keep the retained WAL nonempty while metadata is committed.
const WAL_DEBT_RECORDS: usize = 128;

struct SlowWalRanges {
    inner: Arc<crate::storage::filesystem::FileSystem>,
    calls: Arc<AtomicUsize>,
    pause: Option<Arc<FirstWalRangePause>>,
}

struct FirstWalRangePause {
    started: crossbeam::channel::Sender<()>,
    release: crossbeam::channel::Receiver<()>,
    claimed: std::sync::atomic::AtomicBool,
}

impl StorageBackend for SlowWalRanges {
    fn submit_range_read_request(
        &self,
        request: crate::storage::StorageRequest,
        range: std::ops::Range<u64>,
        callback: crate::storage::RangeReadCallback,
    ) {
        self.inner
            .submit_range_read_request(request, range, callback);
    }

    fn submit_range_head_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: StorageCallback,
    ) {
        if request.key.starts_with("wal/") {
            let inner = self.inner.clone();
            let calls = self.calls.clone();
            let pause = self.pause.clone();
            std::thread::spawn(move || {
                if let Some(pause) = pause {
                    if !pause.claimed.swap(true, Ordering::AcqRel) {
                        pause.started.send(()).expect("signal paused target HEAD");
                        pause
                            .release
                            .recv_timeout(Duration::from_secs(3))
                            .expect("release paused target HEAD");
                    }
                }
                calls.fetch_add(1, Ordering::AcqRel);
                inner.submit_range_head_request(request, callback);
            });
        } else {
            self.inner.submit_range_head_request(request, callback);
        }
    }

    fn submit_metadata_read_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::MetadataReadCallback,
    ) {
        self.inner.submit_metadata_read_request(request, callback);
    }

    fn submit_head_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: StorageCallback,
    ) {
        self.inner.submit_head_request(request, callback);
    }

    fn submit_delete_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: StorageCallback,
    ) {
        self.inner.submit_delete_request(request, callback);
    }

    fn submit_write_request(
        &self,
        request: crate::storage::StorageRequest,
        data: Vec<u8>,
        callback: crate::storage::StorageCallback,
    ) {
        self.inner.submit_write_request(request, data, callback);
    }
}

#[test]
fn should_preserve_full_compaction_allowance_when_retirement_is_idle(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let (mut el, _) = local_debt()?;
    let plan = el
        .actors
        .compaction_actor
        .check_manual_compaction(&el.state)?
        .unwrap();

    // Act
    let prepared = el.prepare_compaction_plan_for_launch(plan)?;

    // Assert
    assert_eq!(
        prepared.compaction_memory_limit,
        el.actors.compaction_actor.compaction_memory_limit()
    );
    Ok(())
}

#[test]
fn should_retain_cloud_wal_when_shutdown_would_start_fresh_reclamation(
) -> crate::common::MidgeResult<()> {
    // Arrange: accepted data is already covered by SSTs and remote WAL. No
    // reclamation worker owns an in-flight retirement or storage mutation yet.
    let (mut el, _worker, _) = cloud_debt_with_wal_records(1, WAL_DEBT_RECORDS)?;
    let cloud = Arc::new(SlowWalRanges {
        inner: Arc::new(crate::storage::filesystem::FileSystem::new(
            el.state.db_path.join("cloud_store"),
        )?),
        calls: Arc::new(AtomicUsize::new(0)),
        pause: None,
    });
    let hybrid = Arc::new(crate::storage::HybridStorage::with_policy(
        cloud.clone(),
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    ));
    hybrid.enable_ephemeral_sst_cache(64 * 1024 * 1024);
    el.set_hybrid_storage(hybrid);
    el.routing.runtime_response_timeout = Duration::from_secs(3);
    el.shutdown.shutdown_cloud_drain_timeout = Duration::from_secs(3);
    el.actors
        .compaction_actor
        .set_execution_limits(1024 * 1024, 1024 * 1024);
    el.state.set_compaction_enabled(false);
    el.actors.cloud_coordinator.cloud_maintenance.next =
        crate::runtime::event_loop::cloud_maintenance::MaintenanceTask::WalRetirement;
    let last = wal_debt_last_sequence(WAL_DEBT_RECORDS);
    assert_eq!(test_flush_frontier(&el.state.manifest, 0), last);
    assert_eq!(el.state.wal.frontiers.cloud_durable(), last);
    assert_eq!(el.state.active_compactions.load(Ordering::Acquire), 0);
    assert!(!el.actors.flush_actor.is_inflight());
    assert!(el.actors.cloud_coordinator.cloud_wal_prune_worker.is_none());
    assert!(el
        .state
        .column_families
        .values()
        .all(|cf| { cf.memtable.size_bytes() == 0 && cf.immutable_flushes.is_empty() }));
    let wal_path = remote_wal_path_for_test(&el, 81);
    let catalog_path = el
        .state
        .db_path
        .join("cloud_store/wal/publication-catalog.v1.json");
    let catalog_before = std::fs::read(&catalog_path)?;
    let request_id = 91_203;
    let response = el.routing.router.register(request_id, "Shutdown");

    // Act
    let outcome = el.handle_shutdown(Some(request_id));

    // Assert: terminal shutdown joins work already owned, but retaining WAL
    // authority is safer than spending its caller budget on fresh cleanup.
    assert_eq!(outcome, crate::runtime::event_loop::HandleOutcome::Break);
    assert!(matches!(
        response.try_recv(),
        Ok(RuntimeResponse::Ok {
            request_id: response_id
        }) if response_id == request_id
    ));
    assert_eq!(
        cloud.calls.load(Ordering::Acquire),
        0,
        "shutdown must not admit a fresh optional WAL retirement after durability settles"
    );
    assert!(
        wal_path.exists(),
        "unretired WAL recovery authority must survive"
    );
    assert_eq!(std::fs::read(catalog_path)?, catalog_before);
    assert!(el.actors.cloud_coordinator.cloud_wal.acks().contains(81));
    assert!(el.actors.cloud_coordinator.cloud_wal_prune_worker.is_none());
    Ok(())
}

#[test]
fn should_retain_wal_when_shutdown_completion_requests_optional_reclamation(
) -> crate::common::MidgeResult<()> {
    // Arrange: flush and upload completions share this reclamation entry
    // point, even before shutdown reaches its final worker join.
    let (mut el, _worker, _) = cloud_debt_with_wal_records(1, WAL_DEBT_RECORDS)?;
    el.actors
        .compaction_actor
        .set_execution_limits(1024 * 1024, 1024 * 1024);
    el.state.set_compaction_enabled(false);
    el.actors.cloud_coordinator.cloud_maintenance.next =
        crate::runtime::event_loop::cloud_maintenance::MaintenanceTask::WalRetirement;
    let wal_path = remote_wal_path_for_test(&el, 81);
    assert!(wal_path.exists());
    assert!(el.actors.cloud_coordinator.cloud_wal_prune_worker.is_none());
    el.shutdown.shutting_down = true;

    // Act
    el.prune_cloud_wal_segments_covered_by_manifest();
    let admitted = el.actors.cloud_coordinator.cloud_wal_prune_worker.is_some();
    el.join_cloud_wal_prune_worker();

    // Assert
    assert!(
        !admitted,
        "a shutdown completion must not admit another optional WAL retirement"
    );
    assert!(wal_path.exists());
    assert!(el.actors.cloud_coordinator.cloud_wal.acks().contains(81));
    Ok(())
}

#[test]
fn should_join_owned_wal_retirement_head_before_acknowledging_shutdown(
) -> crate::common::MidgeResult<()> {
    // Arrange: hold one already-admitted target HEAD open. Its worker owns the
    // publication gate before shutdown begins, so it must retain that ownership
    // until the provider completes rather than being detached at shutdown.
    let (mut el, _worker, _) = cloud_debt_with_wal_records(1, WAL_DEBT_RECORDS)?;
    let (read_started, started) = crossbeam::channel::bounded(1);
    let (release, read_release) = crossbeam::channel::bounded(1);
    let cloud = Arc::new(SlowWalRanges {
        inner: Arc::new(crate::storage::filesystem::FileSystem::new(
            el.state.db_path.join("cloud_store"),
        )?),
        calls: Arc::new(AtomicUsize::new(0)),
        pause: Some(Arc::new(FirstWalRangePause {
            started: read_started,
            release: read_release,
            claimed: std::sync::atomic::AtomicBool::new(false),
        })),
    });
    let hybrid = Arc::new(crate::storage::HybridStorage::with_policy(
        cloud.clone(),
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    ));
    hybrid.enable_ephemeral_sst_cache(64 * 1024 * 1024);
    el.set_hybrid_storage(hybrid);
    el.routing.runtime_response_timeout = Duration::from_secs(3);
    el.shutdown.shutdown_cloud_drain_timeout = Duration::from_secs(3);
    el.actors
        .compaction_actor
        .set_execution_limits(1024 * 1024, 1024 * 1024);
    el.state.set_compaction_enabled(false);
    el.prune_cloud_wal_segments_covered_by_manifest();
    started
        .recv_timeout(Duration::from_secs(1))
        .expect("the owned retirement entered its target HEAD");
    assert!(el.actors.cloud_coordinator.cloud_wal_prune_worker.is_some());
    assert!(el.publication.publication_gate.is_active());
    let request_id = 91_204;
    let response = el.routing.router.register(request_id, "Shutdown");
    let (shutdown_started, shutdown_entered) = crossbeam::channel::bounded(1);

    // Act: the negative receive is bounded coordination with a deliberately
    // held provider callback, not an assertion about provider throughput.
    let shutdown = std::thread::spawn(move || {
        EventLoop::set_cloud_wal_prune_join_hook_for_test(move || {
            let _ = shutdown_started.send(());
        });
        let outcome = el.handle_shutdown(Some(request_id));
        (outcome, el)
    });
    shutdown_entered
        .recv_timeout(Duration::from_secs(1))
        .expect("shutdown entered the owned preflight join");
    let premature_response = response.recv_timeout(Duration::from_millis(100));
    release.send(()).expect("finish the owned provider read");
    let (outcome, el) = shutdown.join().expect("shutdown joined owned workers");

    // Assert: Engine may release its lease only after this event loop returns.
    // An already-owned read must finish before that return and its final ack.
    assert!(matches!(
        premature_response,
        Err(crossbeam::channel::RecvTimeoutError::Timeout)
    ));
    assert_eq!(outcome, crate::runtime::event_loop::HandleOutcome::Break);
    assert!(matches!(
        response.try_recv(),
        Ok(RuntimeResponse::Ok {
            request_id: response_id
        }) if response_id == request_id
    ));
    assert!(cloud.calls.load(Ordering::Acquire) > 0);
    assert!(el.actors.cloud_coordinator.cloud_wal_prune_worker.is_none());
    Ok(())
}
