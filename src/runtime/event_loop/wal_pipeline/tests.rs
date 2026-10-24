use super::*;
use crate::runtime::{ConflictPolicy, TestRuntimeMsg};
use bytes::Bytes;
use crossbeam::channel::{Receiver, Sender};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

struct HeldSync {
    reached: Receiver<()>,
    release: Sender<()>,
}

impl HeldSync {
    fn new() -> Self {
        let (entered, reached) = crossbeam::channel::bounded(1);
        let (release, released) = crossbeam::channel::unbounded();
        let calls = Arc::new(AtomicUsize::new(0));
        fail::cfg_callback("midge::wal::before_physical_sync", move || {
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                entered.send(()).expect("signal held physical sync");
                released
                    .recv_timeout(Duration::from_secs(30))
                    .expect("release physical sync");
            }
        })
        .expect("configure physical sync hold");
        Self { reached, release }
    }
}

impl Drop for HeldSync {
    fn drop(&mut self) {
        let _ = self.release.send(());
        fail::remove("midge::wal::before_physical_sync");
    }
}

fn write(request_id: u64, keys: &[&'static [u8]], strict: bool) -> RuntimeMsg {
    RuntimeMsg::ApplyTransaction {
        request_id,
        ops: keys
            .iter()
            .map(|key| TransactionOp::Put {
                cf_id: 0,
                key: Bytes::from_static(key),
                value: Bytes::from_static(b"value"),
                ttl_seconds: None,
                insert_only: false,
            })
            .collect(),
        assertions: Vec::new(),
        durability_policy: Some(if strict {
            crate::wal::DurabilityPolicy::Strict
        } else {
            crate::wal::DurabilityPolicy::BestEffort
        }),
        start_sequence: None,
        conflict_policy: ConflictPolicy::LastWriteWins,
        response_tx: None,
    }
}

fn settle(event_loop: &mut EventLoop, response: &Receiver<RuntimeResponse>) -> RuntimeResponse {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        event_loop.progress_wal_pipeline();
        if let Some(message) = event_loop.routing.pending_msg.take() {
            event_loop.process_restored_one(message, &crossbeam::channel::never());
        }
        if let Ok(response) = response.try_recv() {
            return response;
        }
        assert!(Instant::now() < deadline, "WAL prefix did not settle");
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn applied_cut(event_loop: &mut EventLoop, response: &Receiver<RuntimeResponse>) -> u64 {
    match settle(event_loop, response) {
        RuntimeResponse::TransactionApplied { last_sequence, .. } => last_sequence,
        response => panic!("transaction failed: {response:?}"),
    }
}

fn begin_snapshot(
    response: &Receiver<RuntimeResponse>,
) -> (u64, Arc<crate::runtime::read_snapshot::ReadSnapshot>) {
    match response.recv_timeout(Duration::from_secs(1)) {
        Ok(RuntimeResponse::BeginTransactionResult {
            start_sequence,
            snapshot: Some(snapshot),
            ..
        }) => (start_sequence, snapshot),
        response => panic!("begin transaction did not progress: {response:?}"),
    }
}

#[test]
fn should_serve_old_cut_and_queue_later_writes_when_strict_fsync_is_held() -> MidgeResult<()> {
    // Arrange: construct the !Send loop on its owning thread. The physical
    // writer inherits this test's failpoint gate and owns the native effect.
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = super::super::tests::create_test_local_event_loop()?;
    event_loop.pipeline.inline_flush_worker = false;
    let held = HeldSync::new();
    let strict = event_loop.routing.router.register(94_000, "strict group");
    let later = event_loop
        .routing
        .router
        .register(94_001, "later best effort");
    let rotate = event_loop.routing.router.register(94_002, "queued rotate");
    let begin = event_loop
        .routing
        .router
        .register(94_003, "begin cache miss");
    let control = event_loop
        .routing
        .router
        .register(94_004, "control progress");
    let empty = crossbeam::channel::never();
    let previous_segment = event_loop.state.wal.open_segment();

    // Act: only physical fsync is paused. A fresh transaction and control
    // request must complete while the later write and rotation stay queued.
    event_loop.process_one(write(94_000, &[b"first", b"second"], true), &empty);
    held.reached
        .recv_timeout(Duration::from_secs(10))
        .expect("held sync");
    event_loop.process_one(write(94_001, &[b"later"], false), &empty);
    event_loop.process_one(
        RuntimeMsg::Test(TestRuntimeMsg::WalRotate { request_id: 94_002 }),
        &empty,
    );
    event_loop.process_one(
        RuntimeMsg::BeginTransaction {
            request_id: 94_003,
            cf_id: 0,
        },
        &empty,
    );
    event_loop.process_one(
        RuntimeMsg::Test(TestRuntimeMsg::Noop { request_id: 94_004 }),
        &empty,
    );
    let (start_sequence, old) = begin_snapshot(&begin);

    // Assert: the allocated sequence is not a visibility cut. No caller can
    // observe half of A or let B cross A's unapplied prefix.
    assert_eq!(start_sequence, 0);
    assert!(event_loop.state.sequence > start_sequence);
    assert_eq!(event_loop.state.applied_sequence, start_sequence);
    assert_eq!(old.get(b"first", start_sequence)?, None);
    assert_eq!(old.get(b"second", start_sequence)?, None);
    assert!(matches!(control.try_recv(), Ok(RuntimeResponse::Ok { .. })));
    assert!(strict.try_recv().is_err());
    assert!(later.try_recv().is_err());
    assert!(rotate.try_recv().is_err());
    assert_eq!(event_loop.state.wal.open_segment(), previous_segment);

    held.release.send(()).expect("release sync");
    let first_cut = applied_cut(&mut event_loop, &strict);
    let later_cut = applied_cut(&mut event_loop, &later);
    assert!(later_cut > first_cut);
    assert!(matches!(
        settle(&mut event_loop, &rotate),
        RuntimeResponse::Ok { .. }
    ));
    let current = event_loop
        .create_read_snapshot(0)
        .expect("current snapshot");
    for key in [b"first".as_slice(), b"second", b"later"] {
        assert_eq!(current.get(key, later_cut)?, Some(b"value".to_vec()));
        assert_eq!(old.get(key, start_sequence)?, None);
    }
    assert!(event_loop.state.wal.open_segment() > previous_segment);
    Ok(())
}
