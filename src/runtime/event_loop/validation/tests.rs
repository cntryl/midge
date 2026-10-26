use super::*;
use crate::runtime::event_loop::flush::FlushCoordinator;
use crate::sst::read_path_metrics::SstReadObserver as _;
use bytes::Bytes;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

mod spilled_tests;

struct HeldCallback {
    release: Sender<()>,
    reached: Receiver<()>,
    calls: Arc<AtomicUsize>,
    hook: &'static str,
}

impl HeldCallback {
    fn first(hook: &'static str) -> Self {
        let (entered, reached) = crossbeam::channel::bounded(1);
        let (release, released) = crossbeam::channel::unbounded();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        fail::cfg_callback(hook, move || {
            if observed.fetch_add(1, Ordering::SeqCst) == 0 {
                let _ = entered.send(());
                released
                    .recv_timeout(Duration::from_secs(30))
                    .expect("release held validation test stage");
            }
        })
        .expect("hold first validation test stage");
        Self {
            release,
            reached,
            calls,
            hook,
        }
    }
}

impl Drop for HeldCallback {
    fn drop(&mut self) {
        let _ = self.release.send(());
        fail::remove(self.hook);
    }
}

fn request(request_id: u64, key: &'static [u8], checked: bool) -> ApplyTransactionRequest {
    ApplyTransactionRequest {
        request_id,
        ops: vec![TransactionOp::Put {
            cf_id: 0,
            key: Bytes::from_static(key),
            value: Bytes::from_static(b"value"),
            ttl_seconds: None,
            insert_only: false,
        }],
        assertions: Vec::new(),
        durability_policy: Some(crate::wal::DurabilityPolicy::Strict),
        start_sequence: checked.then_some(0),
        conflict_policy: if checked {
            ConflictPolicy::AbortOnWriteConflict
        } else {
            ConflictPolicy::LastWriteWins
        },
        validation: None,
    }
}

fn apply_and_flush(event_loop: &mut EventLoop, request: ApplyTransactionRequest) {
    let request_id = request.request_id;
    let response = event_loop
        .routing
        .router
        .register(request_id, "validation fixture write");
    WalCoordinator::apply_transaction(event_loop, &crossbeam::channel::never(), request, None);
    assert!(matches!(
        response.recv_timeout(Duration::from_secs(10)),
        Ok(RuntimeResponse::TransactionApplied { .. })
    ));
    let flush = event_loop
        .routing
        .router
        .register(request_id + 100, "validation fixture flush");
    FlushCoordinator::flush_memtable(event_loop, request_id + 100, 0);
    assert!(matches!(
        flush.recv_timeout(Duration::from_secs(10)),
        Ok(RuntimeResponse::Ok { .. })
    ));
    assert!(!event_loop.state.manifest.files.is_empty());
}

#[test]
fn should_retry_detached_validation_when_intervening_conflicting_write_is_flushed(
) -> MidgeResult<()> {
    // Arrange
    let checked = request(94_601, b"conflict", true);
    // Act
    assert_intervening_flushed_write_is_seen(checked, |error| {
        matches!(error, MidgeError::WriteConflict(_))
    })?;
    // Assert: the shared schedule requires the original typed conflict.
    Ok(())
}

#[test]
fn should_wake_event_loop_when_detached_validation_finishes() -> MidgeResult<()> {
    // Arrange
    let (_directory, mut event_loop, sst_opens) = create_counted_event_loop()?;
    apply_and_flush(&mut event_loop, request(94_600, b"seed", false));
    let (wake_tx, wake_rx) = crossbeam::channel::unbounded();
    event_loop.pipeline.worker_msg_tx = Some(wake_tx);
    sst_opens.event_loop_opens.store(0, Ordering::SeqCst);
    let mut checked = request(94_601, b"new-key", true);
    checked.start_sequence = Some(event_loop.state.applied_sequence);
    checked.durability_policy = Some(crate::wal::DurabilityPolicy::BestEffort);
    let response = event_loop
        .routing
        .router
        .register(94_601, "detached validation wake");

    // Act
    WalCoordinator::apply_transaction(&mut event_loop, &crossbeam::channel::never(), checked, None);
    let wake = wake_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("validation worker must wake the event loop");

    // Assert
    assert!(matches!(&wake, RuntimeMsg::ValidationReady));
    assert!(event_loop.pipeline.validation.has_ready_result());
    crate::runtime::event_loop::dispatch::RuntimeDispatcher::handle(
        &mut event_loop,
        wake,
        &crossbeam::channel::never(),
    );
    assert!(matches!(
        response.recv_timeout(Duration::from_secs(10)),
        Ok(RuntimeResponse::TransactionApplied { .. })
    ));
    assert_eq!(sst_opens.event_loop_opens.load(Ordering::SeqCst), 0);
    Ok(())
}

#[test]
fn should_retry_detached_insert_only_validation_when_intervening_write_is_flushed(
) -> MidgeResult<()> {
    // Arrange
    let mut insert = request(94_601, b"conflict", false);
    if let TransactionOp::Put { insert_only, .. } = &mut insert.ops[0] {
        *insert_only = true;
    }
    // Act
    assert_intervening_flushed_write_is_seen(
        insert,
        |error| matches!(error, MidgeError::InvalidArgument(message) if message == "key already exists"),
    )?;
    // Assert: the shared schedule requires the existing duplicate outcome.
    Ok(())
}

#[test]
fn should_retry_detached_assertion_validation_when_intervening_write_is_flushed() -> MidgeResult<()>
{
    // Arrange
    let mut asserting = request(94_601, b"unrelated", false);
    asserting.assertions.push(crate::runtime::KeyAssertion {
        cf_id: 0,
        key: Bytes::from_static(b"conflict"),
    });
    // Act
    assert_intervening_flushed_write_is_seen(asserting, |error| {
        matches!(error, MidgeError::WriteConflict(_))
    })?;
    // Assert: the shared schedule rejects the stale assertion before allocation.
    Ok(())
}

#[test]
fn should_retry_detached_range_validation_when_intervening_write_is_flushed() -> MidgeResult<()> {
    // Arrange
    let mut deleting = request(94_601, b"unused", true);
    deleting.ops = vec![TransactionOp::DeleteRange {
        cf_id: 0,
        start_key: Bytes::from_static(b"a"),
        end_key: Bytes::from_static(b"z"),
    }];
    // Act
    assert_intervening_flushed_write_is_seen(deleting, |error| {
        matches!(error, MidgeError::WriteConflict(_))
    })?;
    // Assert: the shared schedule detects the whole intervening range conflict.
    Ok(())
}

fn assert_intervening_flushed_write_is_seen(
    mut checked: ApplyTransactionRequest,
    expected_error: fn(&MidgeError) -> bool,
) -> MidgeResult<()> {
    // Arrange
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = super::super::tests::create_test_local_event_loop()?;
    apply_and_flush(&mut event_loop, request(94_600, b"seed", false));
    checked.start_sequence = Some(event_loop.state.applied_sequence);
    let diagnostics = Arc::clone(&event_loop.state.diagnostics);
    let initial_opens = diagnostics.sst_metrics().reader_cache_misses();
    let held = HeldCallback::first("midge::validation::before_snapshot_reads");
    let response = event_loop
        .routing
        .router
        .register(94_601, "conflict checked commit");

    // Act: no SST opens on the loop while the read worker is held. A blind
    // write and its flush progress, removing the intervening active memtable.
    WalCoordinator::apply_transaction(&mut event_loop, &crossbeam::channel::never(), checked, None);
    held.reached
        .recv_timeout(Duration::from_secs(10))
        .expect("read worker reached hold");
    apply_and_flush(&mut event_loop, request(94_602, b"conflict", false));
    assert_eq!(
        diagnostics.sst_metrics().reader_cache_misses(),
        initial_opens
    );
    held.release.send(()).expect("release first read pass");
    let deadline = Instant::now() + Duration::from_secs(10);
    let result = loop {
        event_loop.drain_validation_results(&crossbeam::channel::never());
        if let Ok(response) = response.try_recv() {
            break response;
        }
        assert!(Instant::now() < deadline, "validation did not settle");
        std::thread::sleep(Duration::from_millis(1));
    };

    // Assert: the stale negative read cannot authorize the write. The new
    // source generation detects the conflicting version after its flush.
    assert!(
        matches!(&result, RuntimeResponse::Error { error, .. } if expected_error(error)),
        "unexpected validation result: {result:?}"
    );
    assert!(held.calls.load(Ordering::SeqCst) >= 2);
    assert!(diagnostics.sst_metrics().reader_cache_misses() > initial_opens);
    assert_eq!(event_loop.state.applied_sequence, 6);
    assert_eq!(event_loop.state.sequence, 6);
    let snapshot = event_loop.create_read_snapshot(0).expect("current family");
    assert_eq!(snapshot.get(b"unrelated", 6)?, None);
    assert_eq!(snapshot.get(b"seed", 6)?, Some(b"value".to_vec()));
    assert_eq!(snapshot.get(b"conflict", 6)?, Some(b"value".to_vec()));
    Ok(())
}

#[test]
fn should_retry_detached_validation_after_earlier_pending_group_becomes_visible() -> MidgeResult<()>
{
    // Arrange: a caller's detached read starts at the fully applied seed cut.
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = super::super::tests::create_test_local_event_loop()?;
    apply_and_flush(&mut event_loop, request(94_600, b"seed", false));
    event_loop.pipeline.inline_flush_worker = false;
    let validation = HeldCallback::first("midge::validation::before_snapshot_reads");
    let sync = HeldCallback::first("midge::wal::before_physical_sync");
    let checked_response = event_loop
        .routing
        .router
        .register(94_601, "pending conflict check");
    let mut checked = request(94_601, b"conflict", true);
    checked.start_sequence = Some(3);
    WalCoordinator::apply_transaction(&mut event_loop, &crossbeam::channel::never(), checked, None);
    validation
        .reached
        .recv_timeout(Duration::from_secs(10))
        .expect("held validation");
    let writer_response = event_loop
        .routing
        .router
        .register(94_602, "earlier strict group");

    // Act: the blind writer allocates the earlier WAL unit and waits in sync.
    // The completed old proof must queue without acknowledging the caller.
    WalCoordinator::apply_transaction(
        &mut event_loop,
        &crossbeam::channel::never(),
        request(94_602, b"conflict", false),
        None,
    );
    wait_for_stage(&mut event_loop, &sync.reached);
    validation.release.send(()).expect("release old validation");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !event_loop.pipeline.validation.has_ready_result() {
        assert!(Instant::now() < deadline, "validation read did not finish");
        std::thread::sleep(Duration::from_millis(1));
    }
    event_loop.drain_validation_results(&crossbeam::channel::never());
    assert!(checked_response.try_recv().is_err());
    assert!(writer_response.try_recv().is_err());
    assert_eq!(event_loop.state.applied_sequence, 3);
    let old = event_loop.create_read_snapshot(0).expect("old family view");
    assert_eq!(old.get(b"conflict", 3)?, None);
    sync.release.send(()).expect("release earlier strict unit");
    let writer = settle_validation_request(&mut event_loop, &writer_response);
    let checked = settle_validation_request(&mut event_loop, &checked_response);

    // Assert: the actual earlier group is whole before the retry can reject
    // the caller. Its rejected operation allocates no newer sequence.
    assert!(matches!(
        writer,
        RuntimeResponse::TransactionApplied {
            last_sequence: 6,
            ..
        }
    ));
    assert!(matches!(
        checked,
        RuntimeResponse::Error {
            error: MidgeError::WriteConflict(_),
            ..
        }
    ));
    assert!(validation.calls.load(Ordering::SeqCst) >= 2);
    assert_eq!(event_loop.state.applied_sequence, 6);
    assert_eq!(event_loop.state.sequence, 6);
    assert_eq!(old.get(b"conflict", 3)?, None);
    assert_eq!(old.get(b"conflict", 6)?, Some(b"value".to_vec()));
    Ok(())
}

fn wait_for_stage(event_loop: &mut EventLoop, reached: &Receiver<()>) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        event_loop.progress_wal_pipeline();
        if reached.try_recv().is_ok() {
            return;
        }
        assert!(Instant::now() < deadline, "WAL did not reach held stage");
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn settle_validation_request(
    event_loop: &mut EventLoop,
    response: &Receiver<RuntimeResponse>,
) -> RuntimeResponse {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        event_loop.progress_wal_pipeline();
        event_loop.drain_validation_results(&crossbeam::channel::never());
        event_loop.process_pending_message(&crossbeam::channel::never());
        if let Ok(response) = response.try_recv() {
            return response;
        }
        assert!(Instant::now() < deadline, "validation did not settle");
        std::thread::sleep(Duration::from_millis(1));
    }
}
