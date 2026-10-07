//! Actual handle/router contracts without an event loop or compaction worker.
//!
//! These tests observe queued requests and response ownership only. The real
//! compaction pipeline fixtures establish publication and durability behavior.

use crate::common::OperationDeadline;
use crate::runtime::{next_request_id, Runtime, RuntimeHandle, RuntimeMsg, RuntimeResponse};
use std::thread;
use std::time::{Duration, Instant};

const OBSERVATION_WAIT: Duration = Duration::from_secs(10);

fn observe_queued_deadline(
    handle: &RuntimeHandle,
    request_id: u64,
) -> Option<(Instant, OperationDeadline)> {
    let observation_deadline = Instant::now() + OBSERVATION_WAIT;
    loop {
        if let (Some(registered_at), Some(deadline)) = (
            handle.router.registered_at(request_id),
            handle
                .router
                .request_deadline(request_id, handle.runtime_response_timeout),
        ) {
            if !handle.msg_tx.is_empty() {
                return Some((registered_at, deadline));
            }
        }
        if handle.router.abandoned_requests_total() > 0 || Instant::now() >= observation_deadline {
            return None;
        }
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn should_preserve_manual_caller_budget_when_queued_compaction_outlives_response_wait() {
    // Arrange: retain the actual queue without starting an event loop.
    let (runtime, handle) = Runtime::new();
    let request_id = next_request_id().expect("allocate manual request");
    let caller_budget = Duration::from_secs(3);
    let caller_handle = handle.clone();

    // Act: the actual caller registers, submits, then abandons its response.
    let caller = thread::spawn(move || {
        let started = Instant::now();
        let result = caller_handle
            .send_and_wait_timeout(RuntimeMsg::CompactAll { request_id }, caller_budget);
        (result, started.elapsed())
    });
    let observed = observe_queued_deadline(&handle, request_id);
    let (result, elapsed) = caller.join().expect("join bounded manual caller");
    let after_abandonment = handle
        .router
        .request_deadline(request_id, handle.runtime_response_timeout);
    handle.router.complete(RuntimeResponse::Ok { request_id });
    let abandoned = handle.router.abandoned_requests_total();
    let late = handle.router.late_responses_total();
    let pending = handle.router.pending_len();
    handle.lifecycle.mark_closed();
    drop(runtime);

    // Assert: the accepted route carries the short original clock, not defaults.
    let (registered_at, deadline) = observed.expect("observe actual accepted queue entry");
    assert_eq!(
        deadline,
        OperationDeadline::from_start(registered_at, caller_budget)
    );
    assert!(caller_budget < handle.runtime_response_timeout);
    assert!(deadline.is_expired());
    assert!(
        matches!(result, Ok(None)),
        "actual caller result: {result:?}"
    );
    assert!(elapsed >= caller_budget);
    assert!(elapsed < OBSERVATION_WAIT, "caller elapsed: {elapsed:?}");
    assert!(after_abandonment.is_none());
    assert_eq!(pending, 0);
    assert_eq!(abandoned, 1);
    assert_eq!(late, 1);
}

#[test]
fn should_keep_original_queue_deadline_when_later_lookup_uses_another_default_budget() {
    // Arrange: queue an actual caller with a generous observation window.
    let (runtime, handle) = Runtime::new();
    let request_id = next_request_id().expect("allocate manual request");
    let caller_budget = Duration::from_secs(5);
    let caller_handle = handle.clone();

    // Act: leave the submitted message queued before reading the route again.
    let caller = thread::spawn(move || {
        caller_handle.send_and_wait_timeout(RuntimeMsg::CompactAll { request_id }, caller_budget)
    });
    let observed = observe_queued_deadline(&handle, request_id);
    thread::sleep(Duration::from_millis(50));
    let later_short = handle
        .router
        .request_deadline(request_id, Duration::from_millis(1));
    let later_long = handle
        .router
        .request_deadline(request_id, Duration::from_secs(100));
    handle.router.complete(RuntimeResponse::Ok { request_id });
    let result = caller.join().expect("join completed manual caller");
    let after_completion = handle.router.request_deadline(request_id, caller_budget);
    let abandoned = handle.router.abandoned_requests_total();
    let late = handle.router.late_responses_total();
    handle.lifecycle.mark_closed();
    drop(runtime);

    // Assert: queue time and caller identity cannot be refreshed by lookup.
    let (registered_at, deadline) = observed.expect("observe actual accepted queue entry");
    assert_eq!(
        deadline,
        OperationDeadline::from_start(registered_at, caller_budget)
    );
    assert_eq!(later_short, Some(deadline));
    assert_eq!(later_long, Some(deadline));
    assert!(matches!(
        result,
        Ok(Some(RuntimeResponse::Ok { request_id: actual })) if actual == request_id
    ));
    assert!(after_completion.is_none());
    assert_eq!(abandoned, 0);
    assert_eq!(late, 0);
}

#[test]
fn should_retain_registration_clock_when_direct_manual_route_uses_configured_fallback() {
    // Arrange: direct legacy registration predates its finite configured budget.
    let (runtime, handle) = Runtime::new();
    let request_id = next_request_id().expect("allocate direct manual request");
    let registered_at = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();
    let configured_budget = Duration::from_millis(250);
    let receiver = handle
        .router
        .register_at_for_test(request_id, "CompactAll", registered_at);

    // Act: resolve the compatibility clock without registering a fresh caller.
    let first = handle
        .router
        .request_deadline(request_id, configured_budget);
    let second = handle
        .router
        .request_deadline(request_id, configured_budget);
    handle.router.cancel(request_id);
    let disconnected = receiver.recv();
    handle.lifecycle.mark_closed();
    drop(runtime);

    // Assert: a queued direct fixture receives no new allowance at lookup.
    let expected = OperationDeadline::from_start(registered_at, configured_budget);
    assert_eq!(first, Some(expected));
    assert_eq!(second, Some(expected));
    assert!(expected.is_expired());
    assert!(disconnected.is_err());
}

#[test]
fn should_refuse_missing_deadline_when_manual_response_route_was_cancelled() {
    // Arrange: use an actual router owned by an unstarted runtime.
    let (runtime, handle) = Runtime::new();
    let request_id = next_request_id().expect("allocate cancelled manual request");
    let never_registered = handle
        .router
        .request_deadline(request_id, handle.runtime_response_timeout);
    let receiver = handle.router.register(request_id, "CompactAll");
    let original_registration = handle.router.registered_at(request_id);

    // Act: cancellation removes the obligation rather than renewing its clock.
    handle.router.cancel(request_id);
    let cancelled = handle
        .router
        .request_deadline(request_id, handle.runtime_response_timeout);
    let disconnected = receiver.recv();
    let pending = handle.router.pending_len();
    handle.lifecycle.mark_closed();
    drop(runtime);

    // Assert: missing/cancelled routes cannot become unbounded background work.
    assert!(original_registration.is_some());
    assert!(never_registered.is_none());
    assert!(cancelled.is_none());
    assert!(disconnected.is_err());
    assert_eq!(pending, 0);
}
