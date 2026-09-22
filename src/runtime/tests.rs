use super::*;
use crate::common::MidgeError;
use crate::runtime::TestRuntimeMsg;
use crate::wal::DurabilityPolicy;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[cfg(feature = "failpoints")]
const RUNTIME_PANIC_CHILD: &str = "MIDGE_RUNTIME_PANIC_CHILD";

#[test]
fn should_reject_new_transactions_given_runtime_is_closing_when_beginning() {
    // Arrange
    let lifecycle = Arc::new(RuntimeLifecycle::new());
    let transaction_guard = lifecycle.acquire().expect("acquire transaction guard");

    // Act
    lifecycle.begin_shutdown();

    // Assert
    assert_eq!(lifecycle.state(), RuntimeLifecycleState::Closing);
    assert_eq!(lifecycle.active_transaction_count(), 1);
    assert!(matches!(lifecycle.acquire(), Err(MidgeError::Busy(_))));

    drop(transaction_guard);
    lifecycle.mark_closed();
    assert_eq!(lifecycle.state(), RuntimeLifecycleState::Closed);
}

#[test]
fn should_allow_shutdown_retry_when_initial_wait_times_out() {
    // Arrange: model a running worker that accepts the shutdown request but
    // deliberately withholds its response past the first deadline.
    let (runtime, handle) = Runtime::new();
    handle.lifecycle.mark_running();

    // Act
    let first = handle.shutdown(Duration::from_millis(5));

    // Assert
    assert!(matches!(first, Err(MidgeError::Timeout(_))));
    assert_eq!(handle.lifecycle.state(), RuntimeLifecycleState::Closing);
    assert!(
        handle.lifecycle.shutdown_response_is_available(),
        "timed-out shutdown must retain its response receiver for retry"
    );

    handle.lifecycle.mark_closed();
    assert!(handle.shutdown(Duration::from_millis(5)).is_ok());
    drop(runtime);
}

#[test]
fn should_return_timeout_error_but_leave_lease_held_given_second_concurrent_shutdown_caller_when_first_still_waiting(
) {
    // Arrange: keep the synthetic runtime marked as running without starting an
    // event loop, so the first caller owns the shutdown response wait until its
    // deadline. A running lifecycle represents the runtime whose Engine-level
    // fencing resources must remain held.
    let (runtime, handle) = Runtime::new();
    handle.lifecycle.mark_running();
    let first_handle = handle.clone();
    let (first_result_tx, first_result_rx) = std::sync::mpsc::channel();
    let first = thread::spawn(move || {
        first_result_tx
            .send(first_handle.shutdown(Duration::from_secs(1)))
            .expect("send first shutdown result");
    });
    let pending_deadline = std::time::Instant::now() + Duration::from_secs(1);
    while handle.router.pending_len() == 0 {
        assert!(
            std::time::Instant::now() < pending_deadline,
            "first shutdown caller did not register its response"
        );
        thread::yield_now();
    }

    // Act: this caller has its own much shorter deadline. It must not inherit
    // the first caller's blocking response-receiver mutex hold.
    let second = handle.shutdown(Duration::from_millis(20));

    // Assert
    assert!(matches!(second, Err(MidgeError::Timeout(_))));
    assert!(
        matches!(
            first_result_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ),
        "second shutdown caller waited for the first caller to finish"
    );
    assert!(handle
        .lifecycle
        .running
        .load(std::sync::atomic::Ordering::Acquire));
    assert!(matches!(
        first_result_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("receive first shutdown result"),
        Err(MidgeError::Timeout(_))
    ));
    first.join().expect("join first shutdown caller");

    handle.lifecycle.mark_closed();
    drop(runtime);
}

#[test]
fn should_replay_identical_terminal_error_to_every_shutdown_caller() {
    // Arrange
    let (runtime, handle) = Runtime::new();
    handle.lifecycle.mark_running();
    let first_handle = handle.clone();
    let first = thread::spawn(move || first_handle.shutdown(Duration::from_secs(1)));
    let pending_deadline = std::time::Instant::now() + Duration::from_secs(1);
    while handle.router.pending_len() == 0 {
        assert!(
            std::time::Instant::now() < pending_deadline,
            "shutdown caller did not register its response"
        );
        thread::yield_now();
    }

    // Act: model the event loop's terminal durability failure, then ask a later
    // caller to observe the already-completed shutdown result.
    handle
        .router
        .fail_all("synthetic terminal shutdown failure");
    let first_result = first.join().expect("join first shutdown caller");
    let replayed_result = handle.shutdown(Duration::from_millis(20));

    // Assert
    for result in [first_result, replayed_result] {
        assert!(matches!(
            result,
            Err(MidgeError::Internal(message))
                if message == "synthetic terminal shutdown failure"
        ));
    }

    handle.lifecycle.mark_closed();
    drop(runtime);
}

#[test]
fn should_transfer_shutdown_response_receiver_after_owner_times_out() {
    // Arrange
    let (runtime, handle) = Runtime::new();
    handle.lifecycle.mark_running();
    let first_handle = handle.clone();
    let first = thread::spawn(move || first_handle.shutdown(Duration::from_millis(20)));
    let state_deadline = std::time::Instant::now() + Duration::from_secs(1);
    let request_id = loop {
        let shutdown = handle
            .lifecycle
            .shutdown
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let super::lifecycle::ShutdownState::Receiving { request_id } = &*shutdown {
            break *request_id;
        }
        drop(shutdown);
        assert!(
            std::time::Instant::now() < state_deadline,
            "first shutdown caller did not begin receiving"
        );
        thread::yield_now();
    };
    let second_handle = handle.clone();
    let second = thread::spawn(move || second_handle.shutdown(Duration::from_secs(1)));

    // Act: after the owner returns its receiver to the coordinator, complete
    // the original request for the still-waiting caller that takes ownership.
    let first_result = first.join().expect("join first shutdown caller");
    handle.router.complete(RuntimeResponse::Ok { request_id });
    let second_result = second.join().expect("join second shutdown caller");
    let replayed_result = handle.shutdown(Duration::ZERO);

    // Assert
    assert!(matches!(first_result, Err(MidgeError::Timeout(_))));
    assert!(second_result.is_ok());
    assert!(replayed_result.is_ok());

    handle.lifecycle.mark_closed();
    drop(runtime);
}

fn create_column_family_during_write_flood(
    handle: &RuntimeHandle,
    timeout: Duration,
) -> RuntimeResponse {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "control request could not enter the runtime during write flood"
        );
        match handle.send_and_wait_timeout(
            RuntimeMsg::ManifestCreateColumnFamily {
                request_id: next_request_id().expect("allocate DDL request id"),
                name: "flood-control".to_string(),
            },
            remaining,
        ) {
            Ok(Some(response)) => return response,
            Ok(None) => panic!("admitted control request starved behind write flood"),
            Err(MidgeError::WriteStall(_)) => thread::yield_now(),
            Err(error) => panic!("unexpected control request error: {error}"),
        }
    }
}

#[test]
fn should_process_shutdown_promptly_given_continuous_write_flood_when_shutdown_requested() {
    // Arrange
    let temp_dir = tempfile::TempDir::new().expect("create runtime directory");
    let (runtime, _) = Runtime::new();
    let state = RuntimeState::new(temp_dir.path().to_path_buf(), true);
    let (mut runtime, handle) = runtime
        .start_with_config(state, RuntimeConfig::default())
        .expect("start runtime");
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut writers = Vec::new();
    for writer_id in 0..2_u64 {
        let writer_handle = handle.clone();
        let writer_stop = Arc::clone(&stop);
        let writer_accepted = Arc::clone(&accepted);
        writers.push(thread::spawn(move || {
            let mut ordinal = 0_u64;
            while !writer_stop.load(std::sync::atomic::Ordering::Acquire) {
                let request_id = next_request_id().expect("allocate write request id");
                let result = writer_handle.send(RuntimeMsg::ApplyTransaction {
                    request_id,
                    ops: vec![TransactionOp::Put {
                        cf_id: 0,
                        key: bytes::Bytes::from(format!("flood-{writer_id}-{ordinal}")),
                        value: bytes::Bytes::from_static(b"v"),
                        ttl_seconds: None,
                        insert_only: false,
                    }],
                    assertions: Vec::new(),
                    durability_policy: Some(DurabilityPolicy::BestEffort),
                    start_sequence: None,
                    conflict_policy: ConflictPolicy::LastWriteWins,
                    response_tx: None,
                });
                match result {
                    Ok(()) => {
                        writer_accepted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        ordinal = ordinal.saturating_add(1);
                    }
                    Err(MidgeError::WriteStall(_)) => thread::yield_now(),
                    Err(MidgeError::Busy(_)) => break,
                    Err(error) => panic!("unexpected write flood error: {error}"),
                }
            }
        }));
    }
    let flood_deadline = std::time::Instant::now() + Duration::from_secs(2);
    while accepted.load(std::sync::atomic::Ordering::Acquire) < 100 {
        assert!(
            std::time::Instant::now() < flood_deadline,
            "write flood did not reach the runtime"
        );
        thread::yield_now();
    }

    // Act: a control request queued amid the flood must make progress, and the
    // terminal shutdown marker must close admission within its own budget.
    let control_started = std::time::Instant::now();
    let control = create_column_family_during_write_flood(&handle, Duration::from_secs(1));
    let control_elapsed = control_started.elapsed();
    let shutdown_started = std::time::Instant::now();
    let shutdown = handle.shutdown(Duration::from_secs(2));
    let shutdown_elapsed = shutdown_started.elapsed();
    stop.store(true, std::sync::atomic::Ordering::Release);
    for writer in writers {
        writer.join().expect("join write flood worker");
    }

    // Assert
    assert!(matches!(
        control,
        RuntimeResponse::ColumnFamilyCreated { .. }
    ));
    assert!(
        control_elapsed < Duration::from_secs(1),
        "control request starved behind write flood for {control_elapsed:?}"
    );
    assert!(
        shutdown.is_ok(),
        "shutdown failed under write flood: {shutdown:?}"
    );
    assert!(
        shutdown_elapsed < Duration::from_secs(1),
        "shutdown marker starved behind write flood for {shutdown_elapsed:?}"
    );
    assert!(matches!(
        handle.send(RuntimeMsg::RetryGc),
        Err(MidgeError::Busy(_))
    ));
    assert!(runtime.wait_for_exit(Duration::from_secs(1)));
}

#[cfg(feature = "failpoints")]
#[test]
fn should_answer_pending_router_request_given_event_loop_panic_when_awaiting_response() {
    if std::env::var(RUNTIME_PANIC_CHILD).as_deref() != Ok("router") {
        run_runtime_panic_child(
            "router",
            "runtime::tests::should_answer_pending_router_request_given_event_loop_panic_when_awaiting_response",
        );
        return;
    }

    // Arrange
    let scenario = fail::FailScenario::setup();
    let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let callback_fired = Arc::clone(&fired);
    fail::cfg_callback(
        "midge::runtime::before_get_runtime_metrics_response",
        move || {
            callback_fired.store(true, std::sync::atomic::Ordering::SeqCst);
            panic!("deterministic router-response panic");
        },
    )
    .expect("configure router-response panic");
    let temp_dir = tempfile::TempDir::new().expect("create runtime directory");
    let (runtime, _) = Runtime::new();
    let state = RuntimeState::new(temp_dir.path().to_path_buf(), true);
    let (mut runtime, handle) = runtime
        .start_with_config(state, RuntimeConfig::default())
        .expect("start runtime");

    // Act
    let response = handle
        .send_and_wait(RuntimeMsg::GetRuntimeMetrics {
            request_id: next_request_id().expect("allocate metrics request id"),
        })
        .expect("panic recovery should answer routed request");

    // Assert
    assert!(fired.load(std::sync::atomic::Ordering::SeqCst));
    assert!(matches!(
        response,
        RuntimeResponse::Error {
            error: MidgeError::Internal(message),
            ..
        } if message == "runtime event loop panicked before responding"
    ));
    assert!(runtime.wait_for_exit(Duration::from_secs(1)));
    fail::remove("midge::runtime::before_get_runtime_metrics_response");
    scenario.teardown();
}

#[cfg(feature = "failpoints")]
#[test]
fn should_answer_pending_apply_transaction_request_given_event_loop_panic_when_awaiting_response() {
    if std::env::var(RUNTIME_PANIC_CHILD).as_deref() != Ok("apply") {
        run_runtime_panic_child(
            "apply",
            "runtime::tests::should_answer_pending_apply_transaction_request_given_event_loop_panic_when_awaiting_response",
        );
        return;
    }

    // Arrange
    let scenario = fail::FailScenario::setup();
    let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let callback_fired = Arc::clone(&fired);
    fail::cfg_callback("midge::runtime::strict_group_before_collect", move || {
        callback_fired.store(true, std::sync::atomic::Ordering::SeqCst);
        panic!("deterministic inline-response panic");
    })
    .expect("configure apply-transaction panic");
    let temp_dir = tempfile::TempDir::new().expect("create runtime directory");
    let (runtime, _) = Runtime::new();
    let state = RuntimeState::new(temp_dir.path().to_path_buf(), false);
    let config = RuntimeConfig {
        wal_durability_policy: DurabilityPolicy::Strict,
        ..RuntimeConfig::default()
    };
    let (mut runtime, handle) = runtime
        .start_with_config(state, config)
        .expect("start runtime");
    let submission = TransactionSubmission {
        ops: vec![TransactionOp::Put {
            cf_id: 0,
            key: bytes::Bytes::from_static(b"panic-key"),
            value: bytes::Bytes::from_static(b"panic-value"),
            ttl_seconds: None,
            insert_only: false,
        }],
        assertions: Vec::new(),
        durability_policy: Some(DurabilityPolicy::Strict),
        start_sequence: None,
        conflict_policy: ConflictPolicy::LastWriteWins,
    };

    // Act
    let response = handle.send_apply_transaction_and_wait(
        next_request_id().expect("allocate transaction request id"),
        submission,
    );

    // Assert: routed transactions receive the same explicit panic response as
    // every other request instead of waiting for their response deadline.
    assert!(fired.load(std::sync::atomic::Ordering::SeqCst));
    assert!(matches!(
        response,
        Ok(RuntimeResponse::Error {
            error: MidgeError::Internal(message),
            ..
        }) if message == "runtime event loop panicked before responding"
    ));
    assert!(runtime.wait_for_exit(Duration::from_secs(1)));
    fail::remove("midge::runtime::strict_group_before_collect");
    scenario.teardown();
}

#[cfg(feature = "failpoints")]
#[test]
fn should_finish_accepted_transaction_given_caller_times_out_before_event_loop_resumes() {
    if std::env::var(RUNTIME_PANIC_CHILD).as_deref() != Ok("accepted-timeout") {
        run_runtime_panic_child(
            "accepted-timeout",
            "runtime::tests::should_finish_accepted_transaction_given_caller_times_out_before_event_loop_resumes",
        );
        return;
    }

    // Arrange: pause the event loop after it accepts and prepares a strict
    // transaction but before it can append the WAL record or answer.
    let scenario = fail::FailScenario::setup();
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let release_gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let callback_gate = Arc::clone(&release_gate);
    fail::cfg_callback("midge::runtime::strict_group_before_collect", move || {
        entered_tx
            .send(())
            .expect("signal accepted transaction boundary");
        let (released, changed) = &*callback_gate;
        let mut released = released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while !*released {
            released = changed
                .wait(released)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    })
    .expect("configure accepted transaction boundary");
    let temp_dir = tempfile::TempDir::new().expect("create runtime directory");
    let (runtime, _) = Runtime::new();
    let state = RuntimeState::new(temp_dir.path().to_path_buf(), false);
    let config = RuntimeConfig {
        wal_durability_policy: DurabilityPolicy::Strict,
        runtime_response_timeout: Duration::from_millis(20),
        ..RuntimeConfig::default()
    };
    let (mut runtime, handle) = runtime
        .start_with_config(state, config)
        .expect("start runtime");
    let caller_handle = handle.clone();
    let request_id = next_request_id().expect("allocate transaction request id");
    let submission = TransactionSubmission {
        ops: vec![TransactionOp::Put {
            cf_id: 0,
            key: bytes::Bytes::from_static(b"accepted-timeout-key"),
            value: bytes::Bytes::from_static(b"accepted-timeout-value"),
            ttl_seconds: None,
            insert_only: false,
        }],
        assertions: Vec::new(),
        durability_policy: Some(DurabilityPolicy::Strict),
        start_sequence: None,
        conflict_policy: ConflictPolicy::LastWriteWins,
    };
    let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
    let caller = thread::spawn(move || {
        let result = caller_handle.send_apply_transaction_and_wait(request_id, submission);
        result_tx.send(result).expect("send transaction result");
    });
    entered_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("event loop must accept transaction before caller deadline");

    // Act: keep the accepted request paused until its caller has returned.
    let caller_result = result_rx.recv_timeout(Duration::from_secs(1));
    let (released, changed) = &*release_gate;
    *released
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
    changed.notify_all();
    caller.join().expect("join transaction caller");

    // Assert: timeout ends only the wait. The event loop still commits the WAL
    // and memtable mutation, then exposes its unmatched completion as late.
    assert!(matches!(
        caller_result,
        Ok(Err(MidgeError::Timeout(message)))
            if message.contains("ApplyTransaction")
                && message.contains(&format!("request_id={request_id}"))
    ));
    assert_strict_runtime_value(&handle, b"accepted-timeout-key", b"accepted-timeout-value");
    assert_eq!(handle.router.abandoned_requests_total(), 1);
    assert_eq!(handle.router.late_responses_total(), 1);

    handle.shutdown(Duration::from_secs(1)).expect("shutdown");
    assert!(runtime.wait_for_exit(Duration::from_secs(1)));
    fail::remove("midge::runtime::strict_group_before_collect");
    scenario.teardown();
}

#[cfg(feature = "failpoints")]
fn assert_strict_runtime_value(handle: &RuntimeHandle, key: &[u8], expected: &[u8]) {
    let sequence = match handle
        .send_and_wait_timeout(
            RuntimeMsg::Test(TestRuntimeMsg::GetCurrentSequence {
                request_id: next_request_id().expect("allocate sequence request id"),
            }),
            Duration::from_secs(1),
        )
        .expect("query sequence after accepted transaction")
        .expect("sequence response before deadline")
    {
        RuntimeResponse::CurrentSequence { sequence, .. } => sequence,
        other => panic!("expected current sequence, got {other:?}"),
    };
    let read = handle
        .send_and_wait_timeout(
            RuntimeMsg::Test(TestRuntimeMsg::Read {
                request_id: next_request_id().expect("allocate read request id"),
                cf_id: 0,
                key: key.to_vec(),
                sequence,
                requested_durability: crate::types::ReadDurability::Strict,
            }),
            Duration::from_secs(1),
        )
        .expect("read accepted transaction")
        .expect("read response before deadline");
    assert!(matches!(
        read,
        RuntimeResponse::ReadValue {
            value: Some(value),
            ..
        } if value == expected
    ));
}

#[cfg(feature = "failpoints")]
fn run_runtime_panic_child(scenario: &str, test_name: &str) {
    let output = std::process::Command::new(
        std::env::current_exe().expect("locate runtime unit-test executable"),
    )
    .arg("--exact")
    .arg(test_name)
    .arg("--nocapture")
    .arg("--test-threads=1")
    .env(RUNTIME_PANIC_CHILD, scenario)
    .output()
    .expect("run isolated runtime panic child");
    assert!(
        output.status.success(),
        "runtime panic child '{scenario}' failed; stdout={}; stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn should_not_block_snapshot_release_when_closing_queue_is_full() {
    // Arrange
    let (runtime, handle) = Runtime::new();
    assert!(handle.snapshot_pins.register(7, 11, Vec::new()));
    for _ in 0..RUNTIME_QUEUE_CAPACITY {
        handle
            .msg_tx
            .try_send(RuntimeMsg::RetryGc)
            .expect("fill bounded runtime queue");
    }
    handle.lifecycle.begin_shutdown();

    // Act
    let started = std::time::Instant::now();
    let removed = handle.unregister_snapshot_pin(7);

    // Assert
    assert!(removed);
    assert!(
        started.elapsed() < Duration::from_millis(100),
        "snapshot release blocked on the full closing queue"
    );
    drop(runtime);
}

// =========== next_request_id Tests ===========

#[test]
fn should_generate_unique_request_ids() {
    // Arrange
    // (no setup)

    // Act
    let id1 = next_request_id().expect("id");
    let id2 = next_request_id().expect("id");
    let id3 = next_request_id().expect("id");

    // Assert
    assert_ne!(id1, id2);
    assert_ne!(id2, id3);
    assert_ne!(id1, id3);
}

#[test]
fn should_increment_request_ids_monotonically() {
    // Arrange
    // (no setup)

    // Act
    let id1 = next_request_id().expect("id");
    let id2 = next_request_id().expect("id");
    let id3 = next_request_id().expect("id");

    // Assert
    assert!(id1 < id2);
    assert!(id2 < id3);
}

#[test]
fn should_allocate_request_ids_atomically_across_threads() {
    // Arrange
    let handles: Vec<_> = (0..5)
        .map(|_| {
            thread::spawn(|| {
                let mut ids = vec![];
                for _ in 0..20 {
                    ids.push(next_request_id().expect("id"));
                }
                ids
            })
        })
        .collect();

    // Act
    let all_ids: Vec<u64> = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();

    // Assert - All IDs should be unique
    for i in 0..all_ids.len() {
        for j in (i + 1)..all_ids.len() {
            assert_ne!(all_ids[i], all_ids[j]);
        }
    }

    // Should have 100 IDs from 5 threads
    assert_eq!(all_ids.len(), 100);
}

#[test]
fn should_start_from_nonzero() {
    // Arrange
    // (no setup)

    // Act
    let id = next_request_id().expect("id");

    // Assert - Should never be 0
    assert!(id > 0);
}

#[test]
fn should_remain_exhausted_without_reusing_request_ids_after_wrap_boundary() {
    // Arrange
    let counter = std::sync::atomic::AtomicU64::new(u64::MAX);

    // Act
    let final_id = super::protocol::allocate_request_id(&counter);
    let first_exhausted = super::protocol::allocate_request_id(&counter);
    let still_exhausted = super::protocol::allocate_request_id(&counter);

    // Assert
    assert_eq!(final_id.expect("allocate final request ID"), u64::MAX);
    assert!(first_exhausted.is_err());
    assert!(still_exhausted.is_err());
    assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 0);
}

// =========== RuntimeMsg Tests ===========

#[test]
fn should_extract_request_id_from_message() {
    // Arrange
    let msg = RuntimeMsg::Test(TestRuntimeMsg::Noop { request_id: 42 });

    // Act
    let req_id = msg.request_id();

    // Assert
    assert_eq!(req_id, Some(42));
}

#[test]
fn should_return_none_for_shutdown_message() {
    // Arrange
    let msg = RuntimeMsg::Shutdown;

    // Act
    let req_id = msg.request_id();

    // Assert
    assert_eq!(req_id, None);
}

fn write_side_request_response_messages() -> Vec<RuntimeMsg> {
    let file_meta = FileMeta {
        name: "contract.sst".to_string(),
        level: 0,
        size_bytes: 1,
        content_crc32c: None,
        cf_id: 0,
        smallest_key: None,
        largest_key: None,
        smallest_seq: None,
        largest_seq: None,
        key_bounds_complete: false,
    };
    let compaction_plan = CompactionPlan {
        input_files: vec!["input.sst".to_string()],
        source_level: 0,
        target_level: 1,
        cf_id: 0,
    };
    vec![
        RuntimeMsg::FlushMemtable {
            request_id: 1,
            cf_id: 0,
        },
        RuntimeMsg::Test(TestRuntimeMsg::FlushComplete {
            request_id: 2,
            cf_id: 0,
            sst_name: "flushed.sst".to_string(),
            sequence: 10,
        }),
        RuntimeMsg::Test(TestRuntimeMsg::CheckCompaction { request_id: 3 }),
        RuntimeMsg::Test(TestRuntimeMsg::RunCompaction {
            request_id: 4,
            plan: compaction_plan,
        }),
        RuntimeMsg::Test(TestRuntimeMsg::WalAppend {
            request_id: 5,
            cf_id: 0,
            key: b"k".to_vec(),
            value: Some(b"v".to_vec()),
            ttl_seconds: None,
            insert_only: false,
        }),
        RuntimeMsg::Test(TestRuntimeMsg::WalAppendDeleteRange {
            request_id: 6,
            cf_id: 0,
            start_key: b"a".to_vec(),
            end_key: b"z".to_vec(),
            durability_policy: None,
        }),
        RuntimeMsg::Test(TestRuntimeMsg::WalRotate { request_id: 7 }),
        RuntimeMsg::Test(TestRuntimeMsg::WalSyncComplete {
            request_id: 8,
            segment_id: 1,
        }),
        RuntimeMsg::Test(TestRuntimeMsg::CheckGc { request_id: 12 }),
        RuntimeMsg::Test(TestRuntimeMsg::DeleteObsoleteSsts {
            request_id: 13,
            sst_names: vec!["old.sst".to_string()],
        }),
        RuntimeMsg::Test(TestRuntimeMsg::ManifestAddSst {
            request_id: 14,
            file_meta: file_meta.clone(),
        }),
        RuntimeMsg::Test(TestRuntimeMsg::ManifestCompactionComplete {
            request_id: 15,
            removed: vec!["old.sst".to_string()],
            added: vec![file_meta],
        }),
        RuntimeMsg::Test(TestRuntimeMsg::BeginIngest { request_id: 16 }),
        RuntimeMsg::Test(TestRuntimeMsg::EndIngest { request_id: 17 }),
        RuntimeMsg::Test(TestRuntimeMsg::GetIngestState { request_id: 18 }),
        RuntimeMsg::Test(TestRuntimeMsg::GetRuntimeConfig { request_id: 19 }),
    ]
}

fn read_side_request_response_messages() -> Vec<RuntimeMsg> {
    vec![
        RuntimeMsg::Test(TestRuntimeMsg::Read {
            request_id: 20,
            cf_id: 0,
            key: b"k".to_vec(),
            sequence: 1,
            requested_durability: crate::types::ReadDurability::Strict,
        }),
        RuntimeMsg::Test(TestRuntimeMsg::RangeScan {
            request_id: 21,
            cf_id: 0,
            start: b"a".to_vec(),
            end: b"z".to_vec(),
            sequence: 1,
            requested_durability: crate::types::ReadDurability::Strict,
        }),
        RuntimeMsg::Test(TestRuntimeMsg::CaptureReadSnapshot {
            request_id: 22,
            cf_id: 0,
            sequence: 1,
        }),
        RuntimeMsg::Test(TestRuntimeMsg::UnregisterSnapshot { snapshot_id: 23 }),
        RuntimeMsg::Test(TestRuntimeMsg::Noop { request_id: 24 }),
        RuntimeMsg::Test(TestRuntimeMsg::StartupPing { request_id: 25 }),
    ]
}

fn assert_compact_all_protocol_roundtrip() {
    assert_eq!(
        RuntimeMsg::CompactAll { request_id: 4 }.kind_name(),
        "CompactAll"
    );
    assert!(RuntimeMsg::CompactAll { request_id: 7 }
        .request_id()
        .is_some());
    assert_eq!(
        RuntimeMsg::CompactAll { request_id: 8 }.kind_name(),
        "CompactAll"
    );

    let (runtime, _handle) = Runtime::new();
    let state = RuntimeState::new("/tmp/test_compact_all".into(), true);
    let (_runtime, h) = runtime
        .start_with_config(state, RuntimeConfig::default())
        .expect("start runtime");
    let resp = h
        .send_and_wait(RuntimeMsg::CompactAll {
            request_id: next_request_id().expect("id"),
        })
        .expect("compact_all call");
    match resp {
        RuntimeResponse::Ok { .. } => (),
        _ => panic!("CompactAll did not return Ok"),
    }

    h.send(RuntimeMsg::Shutdown).expect("send shutdown");
    assert!(
        RuntimeMsg::Test(TestRuntimeMsg::GetCurrentSequence { request_id: 7 })
            .request_id()
            .is_some()
    );
}

#[test]
fn should_extract_request_id_from_all_request_response_messages() {
    // Arrange
    let messages = write_side_request_response_messages()
        .into_iter()
        .chain(read_side_request_response_messages())
        .collect::<Vec<_>>();

    // Act
    // Assert
    assert!(messages
        .iter()
        .filter(|msg| !matches!(
            msg,
            RuntimeMsg::Test(TestRuntimeMsg::UnregisterSnapshot { .. })
        ))
        .all(|msg| msg.request_id().is_some()));
    assert_eq!(
        RuntimeMsg::Test(TestRuntimeMsg::UnregisterSnapshot { snapshot_id: 23 }).request_id(),
        None
    );
    assert_compact_all_protocol_roundtrip();
}

// =========== RuntimeResponse Tests ===========

#[test]
fn should_extract_request_id_from_response() {
    // Arrange
    let response = RuntimeResponse::Ok { request_id: 42 };

    // Act
    let req_id = response.request_id();

    // Assert
    assert_eq!(req_id, 42);
}

fn runtime_response_fixtures() -> Vec<RuntimeResponse> {
    let snapshot = Arc::new(crate::runtime::read_snapshot::ReadSnapshot::new(
        Arc::new(crate::memtable::SkipListMemtable::new()),
        vec![],
        vec![],
        Arc::new(crate::io::MockFs::new()),
        std::path::PathBuf::new(),
        true,
        0,
    ));
    vec![
        RuntimeResponse::Ok { request_id: 1 },
        RuntimeResponse::Error {
            request_id: 2,
            error: crate::common::MidgeError::Internal("error".to_string()),
        },
        RuntimeResponse::ReadValue {
            request_id: 3,
            value: Some(b"value".to_vec()),
        },
        RuntimeResponse::RangeScanResults {
            request_id: 4,
            results: vec![(b"k".to_vec(), b"v".to_vec())],
        },
        RuntimeResponse::FlushComplete {
            request_id: 5,
            sst_name: "sst".to_string(),
        },
        RuntimeResponse::CompactionComplete {
            request_id: 6,
            output_ssts: vec!["out.sst".to_string()],
        },
        RuntimeResponse::ColumnFamilyCreated {
            request_id: 7,
            cf_id: 0,
        },
        RuntimeResponse::CurrentSequence {
            request_id: 8,
            sequence: 123,
        },
        RuntimeResponse::TransactionApplied {
            request_id: 9,
            last_sequence: 200,
            op_count: 2,
            write_stall_hint: false,
        },
        RuntimeResponse::WalAppended {
            request_id: 10,
            sequence: 201,
        },
        RuntimeResponse::ReadSnapshot {
            request_id: 11,
            snapshot,
        },
        RuntimeResponse::RuntimeConfigSnapshot {
            request_id: 12,
            memtable_size_limit: 1,
            memtable_flush_threshold: 1,
            enable_compaction: true,
            l0_compaction_trigger: 4,
            wal_durability_policy: DurabilityPolicy::Batched,
            wal_batch_config: crate::wal::policy::BatchConfig::default(),
        },
        RuntimeResponse::IngestState {
            request_id: 13,
            ingest_active: true,
        },
    ]
}

fn assert_response_payloads(responses: Vec<RuntimeResponse>) {
    for response in responses {
        match response {
            RuntimeResponse::ReadValue { value, .. } => {
                assert_eq!(value, Some(b"value".to_vec()));
            }
            RuntimeResponse::RangeScanResults { results, .. } => assert_eq!(results.len(), 1),
            RuntimeResponse::FlushComplete { sst_name, .. } => assert_eq!(sst_name, "sst"),
            RuntimeResponse::CompactionComplete { output_ssts, .. } => {
                assert_eq!(output_ssts, vec!["out.sst"]);
            }
            RuntimeResponse::CurrentSequence { sequence, .. } => assert_eq!(sequence, 123),
            RuntimeResponse::WalAppended { sequence, .. } => assert_eq!(sequence, 201),
            RuntimeResponse::ReadSnapshot { snapshot, .. } => assert_eq!(snapshot.cf_id, 0),
            RuntimeResponse::RuntimeConfigSnapshot {
                memtable_size_limit,
                memtable_flush_threshold,
                enable_compaction,
                l0_compaction_trigger,
                wal_durability_policy,
                wal_batch_config,
                ..
            } => {
                assert_eq!(memtable_size_limit, 1);
                assert_eq!(memtable_flush_threshold, 1);
                assert!(enable_compaction);
                assert_eq!(l0_compaction_trigger, 4);
                assert_eq!(wal_durability_policy, DurabilityPolicy::Batched);
                let default_batch = crate::wal::policy::BatchConfig::default();
                assert_eq!(wal_batch_config.max_delay_ms, default_batch.max_delay_ms);
                assert_eq!(wal_batch_config.max_bytes, default_batch.max_bytes);
            }
            RuntimeResponse::IngestState { ingest_active, .. } => assert!(ingest_active),
            _ => {}
        }
    }
}

#[test]
fn should_extract_request_id_from_all_responses() {
    // Arrange
    let responses = runtime_response_fixtures();

    // Act
    let request_ids: Vec<u64> = responses.iter().map(RuntimeResponse::request_id).collect();

    // Assert
    assert_eq!(request_ids, (1..=13).collect::<Vec<_>>());
    assert_response_payloads(responses);
}

// =========== ResponseRouter Tests ===========

#[test]
fn should_register_then_complete_response() {
    // Arrange
    let router = ResponseRouter::new();

    // Act - Register and deliver response
    let rx = router.register(42, "TestRequest");
    router.complete(RuntimeResponse::Ok { request_id: 42 });

    // Assert - Should receive response
    let received = rx.recv().unwrap();
    assert_eq!(received.request_id(), 42);
}

#[test]
fn should_handle_multiple_pending_requests() {
    // Arrange
    let router = Arc::new(ResponseRouter::new());
    let rx1 = router.register(1, "TestRequest");
    let rx2 = router.register(2, "TestRequest");
    let rx3 = router.register(3, "TestRequest");

    // Act - Complete in different order
    router.complete(RuntimeResponse::Ok { request_id: 2 });
    router.complete(RuntimeResponse::Ok { request_id: 1 });
    router.complete(RuntimeResponse::Ok { request_id: 3 });

    // Assert - Should receive correct responses
    assert_eq!(rx1.recv().unwrap().request_id(), 1);
    assert_eq!(rx2.recv().unwrap().request_id(), 2);
    assert_eq!(rx3.recv().unwrap().request_id(), 3);
}

#[test]
fn should_handle_orphaned_response() {
    // Arrange - a receiver registered under a different request id
    let router = ResponseRouter::new();
    let rx = router.register(1, "TestRequest");

    // Act - complete a response with no matching registered request id
    router.complete(RuntimeResponse::Ok { request_id: 999 });

    // Assert - the orphaned completion must be dropped, not misdelivered to an
    // unrelated pending receiver
    assert!(matches!(
        rx.try_recv(),
        Err(crossbeam::channel::TryRecvError::Empty)
    ));
}

// =========== RuntimeHandle Tests ===========

#[test]
fn should_create_runtime_handle() {
    // Arrange
    let (runtime, handle) = Runtime::new();

    // Act - clone the handle
    let handle2 = handle.clone();

    // Assert - both handles submit onto the same underlying channel, so both
    // sends must succeed against the still-live runtime
    assert!(handle
        .send(RuntimeMsg::Test(TestRuntimeMsg::Noop { request_id: 1 }))
        .is_ok());
    assert!(handle2
        .send(RuntimeMsg::Test(TestRuntimeMsg::Noop { request_id: 2 }))
        .is_ok());

    drop(runtime);
}

#[test]
fn should_handle_send_noop_message() {
    // Arrange
    let (runtime, handle) = Runtime::new();
    let msg = RuntimeMsg::Test(TestRuntimeMsg::Noop { request_id: 1 });

    // Act
    let result = handle.send(msg);

    // Assert
    assert!(result.is_ok());
    drop(runtime);
}

#[test]
fn should_detect_closed_channel_on_send() {
    // Arrange
    let (runtime, handle) = Runtime::new();

    // Act - Drop runtime to close channel
    drop(runtime);

    // Wait a moment for channel to close
    thread::sleep(std::time::Duration::from_millis(10));

    // Assert
    let result = handle.send(RuntimeMsg::Test(TestRuntimeMsg::Noop { request_id: 1 }));
    assert!(result.is_err());
}

#[test]
fn should_require_request_id_for_send_wait() {
    // Arrange
    let (runtime, handle) = Runtime::new();
    let msg = RuntimeMsg::Shutdown;

    // Act
    let result = handle.send_and_wait(msg);

    // Assert
    assert!(result.is_err());
    drop(runtime);
}

#[test]
fn should_cleanup_response_route_given_runtime_timeout_when_runtime_accepts_without_responding() {
    // Arrange: keep the message receiver and lifecycle alive without starting
    // an event loop, so the request is accepted but no response is produced.
    let timeout = Duration::from_millis(20);
    let (runtime, handle) = Runtime::new_with_response_timeout(timeout);
    handle.lifecycle.mark_running();
    let request_id = next_request_id().expect("allocate request id");
    let started_at = std::time::Instant::now();

    // Act
    let result = handle.send_and_wait(RuntimeMsg::GetRuntimeMetrics { request_id });

    // Assert
    assert!(
        started_at.elapsed() >= timeout,
        "response wait returned before its configured deadline"
    );
    assert!(
        started_at.elapsed() < Duration::from_secs(1),
        "configured deadline did not bound the response wait"
    );
    assert!(matches!(
        result,
        Err(MidgeError::Timeout(message))
            if message.contains("GetRuntimeMetrics")
                && message.contains(&format!("request_id={request_id}"))
                && message.contains("20ms")
    ));
    assert_eq!(
        handle.router.pending_len(),
        0,
        "timed-out response route must be removed"
    );

    handle.lifecycle.mark_closed();
    drop(runtime);
}

#[test]
fn should_bound_routed_transaction_response_when_runtime_accepts_without_responding() {
    // Arrange
    let timeout = Duration::from_millis(20);
    let (runtime, handle) = Runtime::new_with_response_timeout(timeout);
    handle.lifecycle.mark_running();
    let request_id = next_request_id().expect("allocate request id");
    let submission = TransactionSubmission {
        ops: vec![TransactionOp::Put {
            cf_id: 0,
            key: bytes::Bytes::from_static(b"deadline-key"),
            value: bytes::Bytes::from_static(b"deadline-value"),
            ttl_seconds: None,
            insert_only: false,
        }],
        assertions: Vec::new(),
        durability_policy: None,
        start_sequence: None,
        conflict_policy: ConflictPolicy::LastWriteWins,
    };

    // Act
    let result = handle.send_apply_transaction_and_wait(request_id, submission);

    // Assert
    assert!(matches!(
        result,
        Err(MidgeError::Timeout(message))
            if message.contains("ApplyTransaction")
                && message.contains(&format!("request_id={request_id}"))
                && message.contains("20ms")
    ));

    handle.lifecycle.mark_closed();
    drop(runtime);
}

// =========== Runtime Tests ===========

#[test]
fn should_create_runtime() {
    // Arrange

    // Act
    let (runtime, handle) = Runtime::new();

    // Assert - a freshly created runtime starts life fully open for
    // submissions, before any event loop thread has been spawned
    assert_eq!(handle.lifecycle.state(), RuntimeLifecycleState::Open);

    drop(runtime);
}

#[test]
fn should_shutdown_runtime() {
    // Arrange
    let (runtime, handle) = Runtime::new();

    // Act
    runtime.shutdown();

    // Assert - shutdown must drive the lifecycle to Closed even when no
    // event loop thread was ever started
    assert_eq!(handle.lifecycle.state(), RuntimeLifecycleState::Closed);
}

// =========== CompactionPlan Tests ===========

#[test]
fn should_clone_compaction_plan_preserving_all_fields() {
    // Arrange
    let sst_name = crate::cloud_layout::file_name(0, 0, 1);
    let plan = CompactionPlan {
        input_files: vec![sst_name.clone()],
        source_level: 0,
        target_level: 1,
        cf_id: 3,
    };

    // Act
    let cloned = plan.clone();

    // Assert - clone must be an independent, field-complete copy
    assert_eq!(cloned.input_files, vec![sst_name]);
    assert_eq!(cloned.source_level, plan.source_level);
    assert_eq!(cloned.target_level, plan.target_level);
    assert_eq!(cloned.cf_id, plan.cf_id);
}

// =========== FileMeta Tests ===========

#[test]
fn should_clone_file_meta_preserving_all_fields() {
    // Arrange
    let meta = FileMeta {
        name: "sst.sst".to_string(),
        level: 2,
        size_bytes: 100,
        content_crc32c: Some(0xdead_beef),
        cf_id: 4,
        smallest_key: Some(b"a".to_vec()),
        largest_key: Some(b"z".to_vec()),
        smallest_seq: Some(1),
        largest_seq: Some(100),
        key_bounds_complete: true,
    };

    // Act
    let cloned = meta.clone();

    // Assert - clone must be an independent, field-complete copy
    assert_eq!(cloned.name, meta.name);
    assert_eq!(cloned.level, meta.level);
    assert_eq!(cloned.size_bytes, meta.size_bytes);
    assert_eq!(cloned.content_crc32c, meta.content_crc32c);
    assert_eq!(cloned.cf_id, meta.cf_id);
    assert_eq!(cloned.smallest_key, meta.smallest_key);
    assert_eq!(cloned.largest_key, meta.largest_key);
    assert_eq!(cloned.smallest_seq, meta.smallest_seq);
    assert_eq!(cloned.largest_seq, meta.largest_seq);
    assert_eq!(cloned.key_bounds_complete, meta.key_bounds_complete);
}

#[test]
fn should_report_runtime_timeout_counters_when_callers_time_out() {
    // Arrange
    let temp_dir = tempfile::TempDir::new().expect("create runtime directory");
    let (runtime, _) = Runtime::new();
    let state = RuntimeState::new(temp_dir.path().to_path_buf(), true);
    let (mut runtime, handle) = runtime
        .start_with_config(state, RuntimeConfig::default())
        .expect("start runtime");

    // Act: one caller gives up on its request, and one response arrives with no
    // caller left to receive it.
    let abandoned_id = next_request_id().expect("allocate abandoned request id");
    let _rx = handle.router.register(abandoned_id, "GetRuntimeMetrics");
    handle
        .router
        .abandon(abandoned_id, Duration::from_millis(20));
    handle.router.complete(RuntimeResponse::Ok {
        request_id: abandoned_id,
    });

    let response = handle
        .send_and_wait(RuntimeMsg::GetRuntimeMetrics {
            request_id: next_request_id().expect("allocate metrics request id"),
        })
        .expect("metrics request answered");

    // Assert
    let RuntimeResponse::RuntimeMetricsSnapshot { snapshot, .. } = response else {
        panic!("expected a runtime metrics snapshot, got {response:?}");
    };
    assert_eq!(
        snapshot.abandoned_runtime_requests_total, 1,
        "a caller that timed out must be visible to operators"
    );
    assert_eq!(
        snapshot.late_runtime_responses_total, 1,
        "work completed after its caller gave up must be visible to operators"
    );

    handle.shutdown(Duration::from_secs(1)).expect("shutdown");
    assert!(runtime.wait_for_exit(Duration::from_secs(1)));
}

#[test]
fn should_count_abandoned_request_given_routed_transaction_when_caller_times_out() {
    // Arrange
    let timeout = Duration::from_millis(20);
    let (runtime, handle) = Runtime::new_with_response_timeout(timeout);
    handle.lifecycle.mark_running();
    let request_id = next_request_id().expect("allocate request id");
    let submission = TransactionSubmission {
        ops: vec![TransactionOp::Put {
            cf_id: 0,
            key: bytes::Bytes::from_static(b"abandoned-key"),
            value: bytes::Bytes::from_static(b"abandoned-value"),
            ttl_seconds: None,
            insert_only: false,
        }],
        assertions: Vec::new(),
        durability_policy: None,
        start_sequence: None,
        conflict_policy: ConflictPolicy::LastWriteWins,
    };

    // Act
    let result = handle.send_apply_transaction_and_wait(request_id, submission);

    // Assert
    assert!(matches!(result, Err(MidgeError::Timeout(_))));
    assert_eq!(
        handle.router.abandoned_requests_total(),
        1,
        "a routed transaction timeout must be counted"
    );

    handle.lifecycle.mark_closed();
    drop(runtime);
}

// =========== Runtime gate classification tests ===========
//
// The publication gate and the storage-verification barrier classify messages
// from tables on `RuntimeMsg`. Those tables must list production traffic only:
// test-only hooks reach them through `RuntimeMsg::Test`, which delegates to a
// separate `TestRuntimeMsg` table, so a test build and a release build gate the
// same production messages the same way.

#[test]
fn should_defer_layout_mutating_messages_when_publication_gate_classifies() {
    // Arrange
    let deferred = vec![
        RuntimeMsg::ManifestPersist { request_id: 1 },
        RuntimeMsg::ManifestCreateColumnFamily {
            request_id: 2,
            name: "cf".to_string(),
        },
        RuntimeMsg::ManifestDropColumnFamily {
            request_id: 3,
            cf_id: 0,
            discard_unflushed: false,
        },
        RuntimeMsg::CompactionComplete {
            request_id: 4,
            input_ssts: Vec::new(),
            output_ssts: Vec::new(),
            cf_id: 0,
            target_level: 1,
            succeeded: true,
        },
        RuntimeMsg::CompactAll { request_id: 5 },
        RuntimeMsg::RetryGc,
    ];
    let passed = vec![
        RuntimeMsg::WalSync { request_id: 6 },
        RuntimeMsg::FlushMemtable {
            request_id: 7,
            cf_id: 0,
        },
        RuntimeMsg::GetRuntimeMetrics { request_id: 8 },
        RuntimeMsg::BeginTransaction {
            request_id: 9,
            cf_id: 0,
        },
    ];

    // Act
    let deferred = deferred
        .into_iter()
        .map(|msg| (msg.kind_name(), msg.defers_under_publication_gate()))
        .collect::<Vec<_>>();
    let passed = passed
        .into_iter()
        .map(|msg| (msg.kind_name(), msg.defers_under_publication_gate()))
        .collect::<Vec<_>>();

    // Assert
    for (kind_name, is_deferred) in deferred {
        assert!(
            is_deferred,
            "{} must be deferred by an active publication gate",
            kind_name
        );
    }
    for (kind_name, is_deferred) in passed {
        assert!(
            !is_deferred,
            "{} must pass an active publication gate",
            kind_name
        );
    }
}

#[test]
fn should_classify_test_hooks_through_the_test_table_when_publication_gate_classifies() {
    // Arrange
    let manifest_hook = RuntimeMsg::Test(TestRuntimeMsg::ManifestAddSst {
        request_id: 1,
        file_meta: FileMeta {
            name: "a.sst".to_string(),
            level: 0,
            size_bytes: 1,
            content_crc32c: None,
            cf_id: 0,
            smallest_key: None,
            largest_key: None,
            smallest_seq: None,
            largest_seq: None,
            key_bounds_complete: false,
        },
    });
    let inert_hook = RuntimeMsg::Test(TestRuntimeMsg::Noop { request_id: 2 });

    // Act
    let manifest_hook_is_deferred = manifest_hook.defers_under_publication_gate();
    let inert_hook_is_deferred = inert_hook.defers_under_publication_gate();

    // Assert
    assert!(manifest_hook_is_deferred);
    assert!(!inert_hook_is_deferred);
}

#[test]
fn should_classify_messages_when_verification_barrier_is_active() {
    // Arrange
    let rejected = vec![
        RuntimeMsg::ApplyTransaction {
            request_id: 1,
            ops: Vec::new(),
            assertions: Vec::new(),
            durability_policy: None,
            start_sequence: None,
            conflict_policy: ConflictPolicy::LastWriteWins,
            response_tx: None,
        },
        RuntimeMsg::FlushMemtable {
            request_id: 2,
            cf_id: 0,
        },
        RuntimeMsg::WalSync { request_id: 3 },
        RuntimeMsg::SealWalForCloud {
            request_id: 4,
            sequence: 1,
            wait_for_ack: false,
        },
        RuntimeMsg::ManifestPersist { request_id: 5 },
        RuntimeMsg::CompactAll { request_id: 6 },
    ];
    let deferred = vec![
        RuntimeMsg::CompactionComplete {
            request_id: 7,
            input_ssts: Vec::new(),
            output_ssts: Vec::new(),
            cf_id: 0,
            target_level: 1,
            succeeded: true,
        },
        RuntimeMsg::RetryGc,
    ];
    let allowed = vec![
        RuntimeMsg::GetRuntimeMetrics { request_id: 8 },
        RuntimeMsg::BeginTransaction {
            request_id: 9,
            cf_id: 0,
        },
        RuntimeMsg::CheckWriteStall {
            request_id: 10,
            cf_id: 0,
        },
    ];

    // Act
    let rejected = rejected
        .into_iter()
        .map(|msg| {
            (
                msg.kind_name(),
                msg.request_id(),
                msg.verification_barrier_action(),
            )
        })
        .collect::<Vec<_>>();
    let deferred = deferred
        .into_iter()
        .map(|msg| (msg.kind_name(), msg.verification_barrier_action()))
        .collect::<Vec<_>>();
    let allowed = allowed
        .into_iter()
        .map(|msg| (msg.kind_name(), msg.verification_barrier_action()))
        .collect::<Vec<_>>();

    // Assert
    for (kind_name, request_id, action) in rejected {
        assert!(
            matches!(
                action,
                VerificationBarrierAction::Reject { request_id: rejected_request_id }
                    if Some(rejected_request_id) == request_id
            ),
            "{} must fail fast under a verification barrier, addressed to its own request id",
            kind_name
        );
    }
    for (kind_name, action) in deferred {
        assert_eq!(
            action,
            VerificationBarrierAction::Defer,
            "{} must be parked, not dropped, under a verification barrier",
            kind_name
        );
    }
    for (kind_name, action) in allowed {
        assert_eq!(
            action,
            VerificationBarrierAction::Allow,
            "{} must pass a verification barrier",
            kind_name
        );
    }
}

#[test]
fn should_classify_test_hooks_through_the_test_table_when_verification_barrier_classifies() {
    // Arrange
    let rejected = RuntimeMsg::Test(TestRuntimeMsg::WalRotate { request_id: 1 });
    let deferred = RuntimeMsg::Test(TestRuntimeMsg::WalSyncComplete {
        request_id: 2,
        segment_id: 1,
    });
    let allowed = RuntimeMsg::Test(TestRuntimeMsg::Noop { request_id: 3 });

    // Act
    let rejected_action = rejected.verification_barrier_action();
    let deferred_action = deferred.verification_barrier_action();
    let allowed_action = allowed.verification_barrier_action();

    // Assert
    assert!(matches!(
        rejected_action,
        VerificationBarrierAction::Reject { request_id: 1 }
    ));
    assert_eq!(deferred_action, VerificationBarrierAction::Defer);
    assert_eq!(allowed_action, VerificationBarrierAction::Allow);
}

/// Source-scanning guards for the "one table per build" invariant.
///
/// The production routing and classification tables must be identical in a
/// `cfg(test)` build and a release build. No behavioural unit test can check
/// that: a unit test only ever runs in a `cfg(test)` build, so a `#[cfg(test)]`
/// arm smuggled into a production table is *live* in exactly the build the test
/// observes, and every assertion still passes while the release build silently
/// routes, defers or rejects differently. Reading the source is the only check
/// that fails for the mutation it is meant to catch.
///
/// `include_str!` is used rather than a runtime read so the guard is pinned to
/// the sources this build was compiled from.
mod production_tables_have_no_cfg_test_arms {
    /// Every spelling that makes a table arm differ between builds. Rejecting
    /// only `#[cfg(test)]` is not enough: `#[cfg(not(test))]` is the idiom this
    /// module itself introduced for the uninhabited `TestRuntimeMsg`, so it is
    /// the form a future edit is most likely to reach for, and `cfg!(test)`
    /// smuggles the same divergence through a guard expression.
    const BUILD_DIVERGENT: &[&str] = &[
        "#[cfg(test)]",
        "#[cfg(not(test))]",
        "#[cfg(all(test",
        "#[cfg(any(test",
        "#[cfg_attr(test",
        "cfg!(test)",
    ];

    /// Return the `{ .. }` block that follows `header` (which must end in `{`),
    /// braces balanced.
    fn block_after(source: &str, header: &str) -> String {
        let start = source
            .find(header)
            .unwrap_or_else(|| panic!("`{header}` not found; update this guard"));
        let open = start + header.len() - 1;
        let mut depth = 0usize;
        for (offset, byte) in source.as_bytes()[open..].iter().enumerate() {
            match byte {
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        return source[open..=open + offset].to_string();
                    }
                }
                _ => {}
            }
        }
        panic!("unbalanced braces after `{header}`");
    }

    /// Same, for a header that must be unique in the file — so the guard cannot
    /// be silently pointed at the wrong block by a later edit.
    fn sole_block(source: &str, header: &str) -> String {
        assert_eq!(
            source.matches(header).count(),
            1,
            "`{header}` is no longer unique; update this guard"
        );
        block_after(source, header)
    }

    /// Return the sole block whose header carries no `cfg` attribute, so the
    /// guard cannot be pointed at a `#[cfg(..)]`-gated stub by reordering the
    /// file. `sole_block` cannot be used where the header is legitimately
    /// repeated across gated and ungated impls.
    fn production_block(source: &str, header: &str) -> String {
        let mut found: Option<String> = None;
        for (start, _) in source.match_indices(header) {
            let gated = source[..start]
                .lines()
                .rev()
                .map(str::trim)
                .find(|line| !line.is_empty() && !line.starts_with("///"))
                .is_some_and(|line| line.starts_with("#[cfg"));
            if gated {
                continue;
            }
            assert!(
                found.is_none(),
                "`{header}` has more than one ungated block; update this guard"
            );
            found = Some(block_after(&source[start..], header));
        }
        found.unwrap_or_else(|| panic!("no ungated `{header}` found; update this guard"))
    }

    fn assert_no_cfg_test(block: &str, what: &str) {
        for spelling in BUILD_DIVERGENT {
            assert!(
                !block.contains(spelling),
                "`{what}` contains a `{spelling}` arm: the production table now differs \
                 between a test build and a release build. Route the test-only case through \
                 `RuntimeMsg::Test` / `TestRuntimeMsg` instead."
            );
        }
    }

    #[test]
    fn should_reject_cfg_test_arms_when_scanning_the_protocol_message_tables() {
        // Arrange
        let source = include_str!("protocol.rs");

        // Act
        let message_enum = sole_block(source, "pub enum RuntimeMsg \x7B");
        let classifiers = sole_block(source, "impl RuntimeMsg \x7B");

        // Assert
        assert_no_cfg_test(&message_enum, "enum RuntimeMsg");
        assert_no_cfg_test(&classifiers, "impl RuntimeMsg");
    }

    #[test]
    fn should_reject_cfg_test_arms_when_scanning_the_durability_waiter_table() {
        // Arrange
        let source = include_str!("durability.rs");

        // Act
        let waiters = sole_block(source, "pub enum DurabilityWaiter \x7B");

        // Assert
        assert_no_cfg_test(&waiters, "enum DurabilityWaiter");
    }

    #[test]
    fn should_reject_cfg_test_arms_when_scanning_the_durability_completion_tables() {
        // Arrange
        let source = include_str!("event_loop/durability_sync.rs");

        // Act
        let completion = production_block(source, "impl EventLoop \x7B");

        // Assert
        assert_no_cfg_test(&completion, "durability_sync::EventLoop");
    }

    #[test]
    fn should_reject_cfg_test_arms_when_scanning_the_shutdown_waiter_table() {
        // Arrange
        let source = include_str!("event_loop/shutdown.rs");

        // Act
        let waiter_request_id = sole_block(
            source,
            "fn shutdown_waiter_request_id(waiter: &DurabilityWaiter) -> Option<u64> \x7B",
        );

        // Assert
        assert_no_cfg_test(&waiter_request_id, "shutdown_waiter_request_id");
    }

    fn injected_build_divergent_arms_are_rejected(
        block: &str,
        what: &str,
    ) -> Result<(), &'static str> {
        for attribute in ["#[cfg(test)]", "#[cfg(not(test))]"] {
            let mut injected = block.to_string();
            let insert_at = injected
                .rfind('}')
                .unwrap_or_else(|| panic!("{what} block must end with a closing brace"));
            injected.insert_str(insert_at, &format!("    {attribute}\n"));
            let result = std::panic::catch_unwind(|| assert_no_cfg_test(&injected, what));
            if result.is_ok() {
                return Err(attribute);
            }
        }
        Ok(())
    }

    #[test]
    fn should_reject_injected_build_divergent_arms_in_the_durability_waiter_table() {
        // Arrange
        let source = include_str!("durability.rs");
        let waiters = sole_block(source, "pub enum DurabilityWaiter \x7B");

        // Act
        let result = injected_build_divergent_arms_are_rejected(&waiters, "enum DurabilityWaiter");

        // Assert
        assert_eq!(
            result,
            Ok(()),
            "enum DurabilityWaiter guard must reject every injected build-divergent arm"
        );
    }

    #[test]
    fn should_reject_injected_build_divergent_arms_in_the_durability_completion_tables() {
        // Arrange
        let source = include_str!("event_loop/durability_sync.rs");
        let completion = production_block(source, "impl EventLoop \x7B");

        // Act
        let result =
            injected_build_divergent_arms_are_rejected(&completion, "durability_sync::EventLoop");

        // Assert
        assert_eq!(
            result,
            Ok(()),
            "durability_sync::EventLoop guard must reject every injected build-divergent arm"
        );
    }

    #[test]
    fn should_reject_injected_build_divergent_arms_in_the_shutdown_waiter_table() {
        // Arrange
        let source = include_str!("event_loop/shutdown.rs");
        let waiter_request_id = sole_block(
            source,
            "fn shutdown_waiter_request_id(waiter: &DurabilityWaiter) -> Option<u64> \x7B",
        );

        // Act
        let result = injected_build_divergent_arms_are_rejected(
            &waiter_request_id,
            "shutdown_waiter_request_id",
        );

        // Assert
        assert_eq!(
            result,
            Ok(()),
            "shutdown_waiter_request_id guard must reject every injected build-divergent arm"
        );
    }

    #[test]
    fn should_reject_cfg_test_arms_when_scanning_the_dispatch_routing_table() {
        // Arrange
        let source = include_str!("event_loop/dispatch.rs");

        // Act
        // Selected by the absence of a `cfg` attribute rather than by source
        // order: both the `#[cfg(test)]` extension and the `#[cfg(not(test))]`
        // stub share this header, and either could be moved above the
        // production impl.
        let production_impl = production_block(source, "impl RuntimeDispatcher \x7B");

        // Assert
        assert_no_cfg_test(&production_impl, "impl RuntimeDispatcher");
    }

    #[test]
    fn should_reject_cfg_test_arms_when_scanning_the_production_write_drain() {
        // Arrange
        let source = include_str!("event_loop/write_batch.rs");

        // Act
        let write_path = sole_block(source, "impl EventLoop \x7B");

        // Assert
        assert_no_cfg_test(&write_path, "write_batch::EventLoop");
    }

    #[test]
    fn should_reject_cfg_test_arms_when_scanning_the_verification_barrier_gate() {
        // Arrange
        let source = include_str!("event_loop/verification.rs");

        // Act
        let gate = sole_block(source, "impl EventLoop \x7B");

        // Assert
        assert_no_cfg_test(&gate, "verification::EventLoop");
    }
}
