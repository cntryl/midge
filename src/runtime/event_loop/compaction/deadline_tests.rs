use super::*;
use crate::common::{MidgeError, MidgeResult};
use crate::runtime::event_loop::tests::{
    create_test_cloud_event_loop, create_test_local_event_loop,
};
use crate::runtime::RuntimeMsg;
use std::time::Instant;

#[test]
fn should_refuse_manual_compaction_when_route_budget_expired_in_queue() -> MidgeResult<()> {
    crate::failpoints::with_read_gate(|| {
        // Arrange: the registered caller spent its allowance before dispatch.
        let mut event_loop = create_test_local_event_loop()?;
        event_loop.routing.runtime_response_timeout = Duration::from_secs(1);
        let started = Instant::now().checked_sub(Duration::from_secs(2)).unwrap();
        let response =
            event_loop
                .routing
                .router
                .register_at_for_test(91_711, "CompactAll", started);

        // Act
        CompactionCoordinator::compact_all(&mut event_loop, 91_711);
        let outcome = response.try_recv();

        // Assert
        assert!(
            matches!(
                outcome,
                Ok(RuntimeResponse::Error {
                    error: MidgeError::Timeout(_),
                    ..
                })
            ),
            "expired caller must not receive a fresh allowance: {outcome:?}"
        );
        assert!(event_loop.state.pending_compaction_waits.is_empty());
        assert_eq!(
            event_loop
                .state
                .active_compactions
                .load(std::sync::atomic::Ordering::Acquire),
            0
        );
        Ok(())
    })
}

#[test]
fn should_expire_manual_waiter_when_maintenance_remains_blocked() -> MidgeResult<()> {
    crate::failpoints::with_read_gate(|| {
        // Arrange: a genuine publication owner prevents admitting caller work.
        let mut event_loop = create_test_cloud_event_loop(
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        )?;
        event_loop
            .actors
            .cloud_coordinator
            .hybrid_storage
            .as_ref()
            .unwrap()
            .enable_ephemeral_sst_cache(64 * 1024 * 1024);
        assert!(event_loop.cloud_maintenance_enabled());
        event_loop.routing.runtime_response_timeout = Duration::from_secs(1);
        let response = event_loop.routing.router.register_at_for_test(
            91_712,
            "CompactAll",
            Instant::now().checked_sub(Duration::from_secs(2)).unwrap(),
        );
        event_loop.state.pending_compaction_waits.insert(
            91_712,
            event_loop
                .routing
                .router
                .request_deadline(91_712, event_loop.routing.runtime_response_timeout)
                .unwrap(),
        );
        let owner = crate::runtime::event_loop::coordination::ManifestPublicationOwner::WalPrune;
        assert!(event_loop
            .publication
            .publication_gate
            .try_acquire(owner.clone()));

        // Act: a fair progress pass must observe caller expiry even behind the gate.
        event_loop.run_request_fairness_slot();
        let outcome = response.try_recv();
        let gate_retained = event_loop.publication.publication_gate.is_active();
        event_loop.publication.publication_gate.release(&owner);
        event_loop.join_cloud_wal_prune_worker();

        // Assert
        assert!(
            matches!(
                outcome,
                Ok(RuntimeResponse::Error {
                    error: MidgeError::Timeout(_),
                    ..
                })
            ),
            "blocked maintenance cannot renew caller time: {outcome:?}"
        );
        assert!(
            gate_retained,
            "caller expiry must not release another owner"
        );
        assert!(event_loop.state.pending_compaction_waits.is_empty());
        Ok(())
    })
}

#[test]
fn should_wake_idle_runtime_when_blocked_manual_waiter_budget_expires() -> MidgeResult<()> {
    crate::failpoints::with_read_gate(|| {
        // Arrange: no worker completion or new caller can wake this queued obligation.
        let (requests, request_rx) = crossbeam::channel::unbounded();
        let (_workers, worker_rx) = crossbeam::channel::unbounded();
        let (initialized, initialization) = crossbeam::channel::bounded(1);

        // Act: use the actual idle select loop, preserving the other publication owner.
        let runtime = std::thread::spawn(move || {
            run_blocked_manual_waiter(request_rx, worker_rx, initialized)
        });
        let setup = initialization
            .recv_timeout(Duration::from_secs(10))
            .expect("idle runtime initialized");
        let outcome = setup.response.recv_timeout(Duration::from_secs(5));
        let elapsed = setup.started.elapsed();
        drop(requests);
        let (gate_retained, waits_empty) = runtime.join().expect("join idle event loop")?;

        // Assert
        assert!(setup.initial_wake.is_some_and(|wake| wake <= setup.budget));
        assert!(
            matches!(
                outcome,
                Ok(RuntimeResponse::Error {
                    error: MidgeError::Timeout(_),
                    ..
                })
            ),
            "idle queued route must expire without another event: {outcome:?}"
        );
        assert!(
            elapsed >= setup.budget && elapsed < Duration::from_secs(5),
            "observed wait: {elapsed:?}"
        );
        assert!(gate_retained);
        assert!(waits_empty);
        Ok(())
    })
}

struct IdleWaiterSetup {
    response: crossbeam::channel::Receiver<RuntimeResponse>,
    started: Instant,
    budget: Duration,
    initial_wake: Option<Duration>,
}

fn run_blocked_manual_waiter(
    request_rx: crossbeam::channel::Receiver<RuntimeMsg>,
    worker_rx: crossbeam::channel::Receiver<RuntimeMsg>,
    initialized: crossbeam::channel::Sender<IdleWaiterSetup>,
) -> MidgeResult<(bool, bool)> {
    let mut event_loop = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    event_loop
        .actors
        .cloud_coordinator
        .hybrid_storage
        .as_ref()
        .unwrap()
        .enable_ephemeral_sst_cache(64 * 1024 * 1024);
    let owner = crate::runtime::event_loop::coordination::ManifestPublicationOwner::WalPrune;
    assert!(event_loop
        .publication
        .publication_gate
        .try_acquire(owner.clone()));
    let started = Instant::now();
    let budget = Duration::from_secs(2);
    let deadline = OperationDeadline::from_start(started, budget);
    let response =
        event_loop
            .routing
            .router
            .register_with_deadline(91_713, "CompactAll", started, deadline);
    event_loop
        .state
        .pending_compaction_waits
        .insert(91_713, deadline);
    initialized
        .send(IdleWaiterSetup {
            response,
            started,
            budget,
            initial_wake: event_loop.idle_progress_timeout(),
        })
        .expect("report initialized idle runtime");
    event_loop.run(&request_rx, &worker_rx);
    let gate_retained = event_loop.publication.publication_gate.is_active();
    event_loop.publication.publication_gate.release(&owner);
    event_loop.join_cloud_wal_prune_worker();
    Ok((
        gate_retained,
        event_loop.state.pending_compaction_waits.is_empty(),
    ))
}
