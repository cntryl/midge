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
    hook: &'static str,
}

impl HeldSync {
    fn at(hook: &'static str) -> Self {
        let (entered, reached) = crossbeam::channel::bounded(1);
        let (release, released) = crossbeam::channel::unbounded();
        let calls = Arc::new(AtomicUsize::new(0));
        fail::cfg_callback(hook, move || {
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                entered.send(()).expect("signal held physical sync");
                released
                    .recv_timeout(Duration::from_secs(30))
                    .expect("release physical sync");
            }
        })
        .expect("configure physical sync hold");
        Self {
            reached,
            release,
            hook,
        }
    }
}

impl Drop for HeldSync {
    fn drop(&mut self) {
        let _ = self.release.send(());
        fail::remove(self.hook);
    }
}

struct HeldSyncs {
    reached: Receiver<usize>,
    release: Sender<usize>,
    released_calls: AtomicUsize,
    hook: &'static str,
}

impl HeldSyncs {
    fn first_two(hook: &'static str) -> Self {
        let (entered, reached) = crossbeam::channel::bounded(2);
        let (release, released) = crossbeam::channel::unbounded();
        let calls = Arc::new(AtomicUsize::new(0));
        fail::cfg_callback(hook, move || {
            let call = calls.fetch_add(1, Ordering::SeqCst);
            if call < 2 {
                entered.send(call).expect("signal held physical sync");
                let released_call = released
                    .recv_timeout(Duration::from_secs(30))
                    .expect("release held physical sync");
                assert_eq!(released_call, call);
            }
        })
        .expect("configure two physical sync holds");
        Self {
            reached,
            release,
            released_calls: AtomicUsize::new(0),
            hook,
        }
    }

    fn release(&self, call: usize) {
        self.released_calls.fetch_or(1 << call, Ordering::SeqCst);
        self.release.send(call).expect("send physical sync release");
    }
}

impl Drop for HeldSyncs {
    fn drop(&mut self) {
        let released = self.released_calls.load(Ordering::SeqCst);
        for call in 0..2 {
            if released & (1 << call) == 0 {
                let _ = self.release.try_send(call);
            }
        }
        fail::remove(self.hook);
    }
}

pub(in crate::runtime::event_loop) fn write(
    request_id: u64,
    keys: &[&'static [u8]],
    strict: bool,
) -> RuntimeMsg {
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

fn spill_write(
    event_loop: &EventLoop,
    request_id: u64,
    keys: &[&'static [u8]],
) -> MidgeResult<RuntimeMsg> {
    let pool = Arc::new(crate::runtime::transaction_spill::TransactionMemoryPool::new(16));
    let mut intents = crate::runtime::transaction_spill::TransactionWriteSet::new(
        pool,
        &crate::io::real::test_root(&event_loop.state.db_path),
        false,
        request_id,
    );
    for key in keys {
        intents.push(TransactionOp::Put {
            cf_id: 0,
            key: Bytes::from_static(key),
            value: Bytes::from_static(b"spilled-value"),
            ttl_seconds: None,
            insert_only: false,
        })?;
    }
    Ok(RuntimeMsg::ApplySpilledTransaction {
        request_id,
        source: intents.take_source(),
        assertions: Vec::new(),
        durability_policy: Some(crate::wal::DurabilityPolicy::Strict),
        start_sequence: 0,
        conflict_policy: ConflictPolicy::LastWriteWins,
        response_tx: None,
    })
}

pub(in crate::runtime::event_loop) fn settle(
    event_loop: &mut EventLoop,
    response: &Receiver<RuntimeResponse>,
) -> RuntimeResponse {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        event_loop.progress_wal_pipeline();
        event_loop.process_pending_message(&crossbeam::channel::never());
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

pub(in crate::runtime::event_loop) fn begin_snapshot(
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
fn should_preserve_ordered_visibility_when_strict_fsync_is_held() -> MidgeResult<()> {
    // Arrange
    let held_stage = "midge::wal::before_physical_sync";
    // Act
    assert_ordered_progress_when_held(held_stage)?;
    // Assert: propagate the complete prefix-order fixture's assertions.
    Ok(())
}

#[test]
fn should_preserve_ordered_visibility_when_writer_encoding_is_held() -> MidgeResult<()> {
    // Arrange
    let held_stage = "midge::wal::before_writer_encode";
    // Act
    assert_ordered_progress_when_held(held_stage)?;
    // Assert: propagate the complete prefix-order fixture's assertions.
    Ok(())
}

#[test]
fn should_reject_conflicting_follower_after_held_strict_group_applies() -> MidgeResult<()> {
    // Arrange: a follower observes the original cut and conflicts with a
    // point write whose strict group is still waiting for physical fsync.
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = super::super::tests::create_test_local_event_loop()?;
    event_loop.pipeline.inline_flush_worker = false;
    let held = HeldSync::at("midge::wal::before_physical_sync");
    let first = event_loop
        .routing
        .router
        .register(94_005, "first strict group");
    let follower = event_loop
        .routing
        .router
        .register(94_006, "conflicting strict follower");
    let empty = crossbeam::channel::never();
    let observed_cut = event_loop.state.applied_sequence;

    // Act: queue the overlapping AbortOnWriteConflict transaction while the
    // first group is durable only in the future, but not yet applied.
    event_loop.process_one(write(94_005, &[b"held-conflict"], true), &empty);
    wait_for_hold(&mut event_loop, &held);
    event_loop.process_one(
        RuntimeMsg::ApplyTransaction {
            request_id: 94_006,
            ops: vec![
                TransactionOp::Put {
                    cf_id: 0,
                    key: Bytes::from_static(b"held-conflict"),
                    value: Bytes::from_static(b"follower-value"),
                    ttl_seconds: None,
                    insert_only: false,
                },
                TransactionOp::Put {
                    cf_id: 0,
                    key: Bytes::from_static(b"follower-only"),
                    value: Bytes::from_static(b"must-not-apply"),
                    ttl_seconds: None,
                    insert_only: false,
                },
            ],
            assertions: Vec::new(),
            durability_policy: Some(crate::wal::DurabilityPolicy::Strict),
            start_sequence: Some(observed_cut),
            conflict_policy: ConflictPolicy::AbortOnWriteConflict,
            response_tx: None,
        },
        &empty,
    );

    // Assert the follower is retained before the prefix owner settles it.
    assert_eq!(event_loop.state.applied_sequence, observed_cut);
    assert_eq!(event_loop.actors.wal_actor.append_calls(), 1);
    assert!(first.try_recv().is_err());
    assert!(follower.try_recv().is_err());
    let before_apply = event_loop
        .create_read_snapshot(0)
        .expect("pre-apply snapshot");
    assert_eq!(before_apply.get(b"held-conflict", observed_cut)?, None);
    assert_eq!(before_apply.get(b"follower-only", observed_cut)?, None);

    held.release.send(()).expect("release first sync");
    let first_cut = applied_cut(&mut event_loop, &first);
    assert!(first_cut > observed_cut);
    let follower_result = settle(&mut event_loop, &follower);
    assert!(matches!(
        follower_result,
        RuntimeResponse::Error {
            request_id: 94_006,
            error: crate::common::MidgeError::WriteConflict(_),
        }
    ));

    // The rejected whole transaction contributes neither an overwrite nor
    // its unrelated second operation to the newly published read cut.
    assert_eq!(event_loop.state.applied_sequence, first_cut);
    assert_eq!(event_loop.state.sequence, first_cut);
    assert_eq!(event_loop.actors.wal_actor.append_calls(), 1);
    let after_apply = event_loop
        .create_read_snapshot(0)
        .expect("post-apply snapshot");
    assert_eq!(
        after_apply.get(b"held-conflict", first_cut)?,
        Some(b"value".to_vec())
    );
    assert_eq!(after_apply.get(b"follower-only", first_cut)?, None);
    Ok(())
}

fn wait_for_hold(event_loop: &mut EventLoop, held: &HeldSync) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        event_loop.progress_wal_pipeline();
        if held.reached.try_recv().is_ok() {
            return;
        }
        assert!(Instant::now() < deadline, "writer did not reach held stage");
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn wait_for_sync_hold(event_loop: &mut EventLoop, held: &HeldSyncs, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        event_loop.progress_wal_pipeline();
        event_loop.process_pending_message(&crossbeam::channel::never());
        if let Ok(call) = held.reached.try_recv() {
            assert_eq!(call, expected, "unexpected physical sync hold order");
            return;
        }
        assert!(
            Instant::now() < deadline,
            "writer did not reach held sync {expected}"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn assert_ordered_progress_when_held(hook: &'static str) -> MidgeResult<()> {
    // Arrange: construct the !Send loop on its owning thread. The physical
    // writer inherits this test's failpoint gate and owns the native effect.
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = super::super::tests::create_test_local_event_loop()?;
    event_loop.pipeline.inline_flush_worker = false;
    let held = HeldSync::at(hook);
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
    wait_for_hold(&mut event_loop, &held);
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

#[test]
fn should_preserve_unchanged_append_failure_when_writer_proves_durable_rollback() -> MidgeResult<()>
{
    // Arrange
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = super::super::tests::create_test_local_event_loop()?;
    event_loop.pipeline.inline_flush_worker = false;
    let path = event_loop.state.db_path.clone();
    let response = event_loop
        .routing
        .router
        .register(94_010, "rolled back raw append");
    fail::cfg("midge::wal::partial_write_then_no_space", "return")
        .expect("partial append injection");

    // Act
    event_loop.process_one(
        write(94_010, &[b"not-appended"], true),
        &crossbeam::channel::never(),
    );
    let outcome = settle(&mut event_loop, &response);
    fail::remove("midge::wal::partial_write_then_no_space");

    // Assert: the exact append callback carries the writer's durable rollback
    // proof. A later generic sync error wrapper must not erase that proof.
    assert!(matches!(
        outcome,
        RuntimeResponse::Error {
            error: MidgeError::NoSpace(_),
            ..
        }
    ));
    assert_eq!(event_loop.state.applied_sequence, 0);
    drop(event_loop);
    let recovered = crate::runtime::state::RuntimeState::try_new(
        path,
        false,
        crate::config::RecoveryPolicy::Strict,
    )?;
    assert_eq!(
        recovered.get_cf(0).unwrap().memtable.get(b"not-appended")?,
        None
    );
    Ok(())
}

#[test]
fn should_keep_wal_runtime_state_unchanged_until_owner_drains_completion() -> MidgeResult<()> {
    // Arrange
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = super::super::tests::create_test_local_event_loop()?;
    event_loop.pipeline.inline_flush_worker = false;
    let held = HeldSync::at("midge::wal::before_physical_sync");
    let diagnostics = Arc::clone(&event_loop.state.diagnostics);
    let physical_before = diagnostics.counters().wal_fsync_count;
    let durable_before = event_loop.state.wal.frontiers.local_durable();
    let applied_before = event_loop.state.applied_sequence;
    let response = event_loop
        .routing
        .router
        .register(97_900, "WAL owner publication");
    event_loop.process_one(
        write(97_900, &[b"owner-only"], true),
        &crossbeam::channel::never(),
    );
    wait_for_hold(&mut event_loop, &held);

    // Act: allow the actual writer fsync, deliberately without running a
    // single runtime completion or bookkeeping turn afterwards.
    held.release.send(()).expect("release physical writer");
    let deadline = Instant::now() + Duration::from_secs(10);
    while diagnostics.counters().wal_fsync_count == physical_before {
        assert!(
            Instant::now() < deadline,
            "physical writer did not complete"
        );
        std::thread::sleep(Duration::from_millis(1));
    }

    // Assert: worker completion is evidence for the owner, not publication.
    assert_eq!(
        event_loop.state.wal.frontiers.local_durable(),
        durable_before
    );
    assert_eq!(event_loop.state.applied_sequence, applied_before);
    assert_eq!(event_loop.state.total_memtable_bytes, 0);
    assert!(response.try_recv().is_err());
    assert!(matches!(
        settle(&mut event_loop, &response),
        RuntimeResponse::TransactionApplied { .. }
    ));
    assert!(event_loop.state.wal.frontiers.local_durable() > durable_before);
    assert!(event_loop.state.applied_sequence > applied_before);
    assert!(event_loop.state.total_memtable_bytes > 0);
    Ok(())
}

#[test]
fn should_share_next_sync_when_resident_writers_queue_behind_a_held_strict_group() -> MidgeResult<()>
{
    // Arrange
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = super::super::tests::create_test_local_event_loop()?;
    event_loop.pipeline.inline_flush_worker = false;
    let held = HeldSyncs::first_two("midge::wal::before_physical_sync");
    let before = event_loop.state.diagnostics.counters().wal_fsync_count;
    let first = event_loop
        .routing
        .router
        .register(94_020, "first strict group");
    let second = event_loop
        .routing
        .router
        .register(94_021, "queued second unit");
    let third = event_loop
        .routing
        .router
        .register(94_022, "queued third unit");
    let wal_path = crate::wal::segment_key(event_loop.state.wal.open_segment());
    event_loop.process_one(
        write(94_020, &[b"first-group"], true),
        &crossbeam::channel::never(),
    );
    wait_for_sync_hold(&mut event_loop, &held, 0);
    let first_sync_end = event_loop
        .state
        .fs
        .metadata(&wal_path)
        .map_err(crate::io::FsError::into_midge)?
        .len;

    // Act: both later writers enter the bounded deferred queue before the
    // first receipt is released. They must be collected into the next group.
    event_loop.process_one(
        write(94_021, &[b"next-a"], true),
        &crossbeam::channel::never(),
    );
    event_loop.process_one(
        write(94_022, &[b"next-b"], true),
        &crossbeam::channel::never(),
    );
    assert!(second.try_recv().is_err());
    assert!(third.try_recv().is_err());
    held.release(0);
    wait_for_sync_hold(&mut event_loop, &held, 1);

    // The next group's append is already in the writer tail while its fsync is
    // held. The ledger must still record only the exact first sync prefix.
    let writer_end = event_loop
        .state
        .fs
        .metadata(&wal_path)
        .map_err(crate::io::FsError::into_midge)?
        .len;
    let durable_bytes = event_loop
        .state
        .wal
        .history
        .ledger()
        .open_member()
        .durable_bytes;
    assert!(writer_end > first_sync_end);
    assert_eq!(durable_bytes, Some(first_sync_end));
    assert!(durable_bytes.is_some_and(|durable| durable < writer_end));

    held.release(1);
    let first_cut = applied_cut(&mut event_loop, &first);
    let second_cut = applied_cut(&mut event_loop, &second);
    let third_cut = applied_cut(&mut event_loop, &third);

    // Assert: one physical writer sync per group; exact ledger durability is
    // separate and is deliberately not counted by this writer-only metric.
    assert_eq!(
        event_loop.state.diagnostics.counters().wal_fsync_count - before,
        2
    );
    assert!(first_cut < second_cut && second_cut < third_cut);
    assert_eq!(event_loop.state.applied_sequence, third_cut);
    assert_eq!(
        event_loop
            .state
            .wal
            .history
            .ledger()
            .open_member()
            .durable_bytes,
        Some(
            event_loop
                .state
                .fs
                .metadata(&wal_path)
                .map_err(crate::io::FsError::into_midge)?
                .len
        )
    );
    let snapshot = event_loop.create_read_snapshot(0).unwrap();
    for key in [b"first-group".as_slice(), b"next-a", b"next-b"] {
        assert_eq!(snapshot.get(key, third_cut)?, Some(b"value".to_vec()));
    }
    Ok(())
}

#[test]
fn should_preserve_lookahead_rotation_before_newer_deferred_writers() -> MidgeResult<()> {
    // Arrange: coalescing observes the rotation before either later write.
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = super::super::tests::create_test_local_event_loop()?;
    event_loop.pipeline.inline_flush_worker = false;
    let held = HeldSync::at("midge::wal::before_physical_sync");
    let first = event_loop
        .routing
        .router
        .register(94_030, "strict before rotation");
    let rotate = event_loop
        .routing
        .router
        .register(94_031, "older rotation barrier");
    let second = event_loop
        .routing
        .router
        .register(94_032, "newer queued write");
    let (queued, incoming) = crossbeam::channel::unbounded();
    queued
        .send(RuntimeMsg::Test(TestRuntimeMsg::WalRotate {
            request_id: 94_031,
        }))
        .unwrap();
    let initial_segment = event_loop.state.wal.open_segment();
    event_loop.process_one(write(94_030, &[b"before-rotate"], true), &incoming);
    wait_for_hold(&mut event_loop, &held);
    event_loop.process_one(write(94_032, &[b"after-rotate"], true), &incoming);

    // Act: the real run-loop handoff leaves an older layout barrier parked.
    assert!(event_loop.process_pending_message(&incoming).is_none());
    assert!(rotate.try_recv().is_err());
    assert_eq!(event_loop.state.wal.open_segment(), initial_segment);
    held.release.send(()).expect("release earlier WAL group");
    let first_cut = applied_cut(&mut event_loop, &first);

    // Assert: the rotation is acknowledged before a newer deferred writer is
    // even sequenced, and the later write belongs to the replacement segment.
    assert!(matches!(rotate.try_recv(), Ok(RuntimeResponse::Ok { .. })));
    assert!(event_loop.state.wal.open_segment() > initial_segment);
    assert_eq!(event_loop.state.applied_sequence, first_cut);
    assert!(second.try_recv().is_err());
    assert!(applied_cut(&mut event_loop, &second) > first_cut);
    Ok(())
}

#[test]
fn should_preserve_prefix_order_when_spill_separates_resident_groups() -> MidgeResult<()> {
    // Arrange
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = super::super::tests::create_test_local_event_loop()?;
    event_loop.pipeline.inline_flush_worker = false;
    let held = HeldSync::at("midge::wal::before_physical_sync");
    let first = event_loop
        .routing
        .router
        .register(94_040, "resident before spill");
    let spilled = event_loop
        .routing
        .router
        .register(94_041, "streaming spill barrier");
    let later = event_loop
        .routing
        .router
        .register(94_042, "resident after spill");
    let spill = spill_write(&event_loop, 94_041, &[b"spill-a", b"spill-b"])?;
    event_loop.process_one(
        write(94_040, &[b"before-spill"], true),
        &crossbeam::channel::never(),
    );
    wait_for_hold(&mut event_loop, &held);
    let allocated_before_spill = event_loop.state.sequence;

    // Act
    event_loop.process_one(spill, &crossbeam::channel::never());
    event_loop.process_one(
        write(94_042, &[b"after-spill"], true),
        &crossbeam::channel::never(),
    );

    // Assert: neither barrier nor following write may allocate over a parked
    // earlier group. Streaming remains the bounded compatibility path.
    assert_eq!(event_loop.state.sequence, allocated_before_spill);
    assert_eq!(event_loop.state.applied_sequence, 0);
    assert!(spilled.try_recv().is_err());
    assert!(later.try_recv().is_err());
    held.release.send(()).expect("release resident prefix");
    let first_cut = applied_cut(&mut event_loop, &first);
    let spill_cut = applied_cut(&mut event_loop, &spilled);
    let later_cut = applied_cut(&mut event_loop, &later);
    assert!(first_cut < spill_cut && spill_cut < later_cut);
    let snapshot = event_loop.create_read_snapshot(0).unwrap();
    for key in [b"spill-a".as_slice(), b"spill-b"] {
        assert_eq!(
            snapshot.get(key, later_cut)?,
            Some(b"spilled-value".to_vec())
        );
    }
    Ok(())
}

#[test]
fn should_fence_every_family_after_partial_spill_without_advancing_global_visible_cut(
) -> MidgeResult<()> {
    // Arrange
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = super::super::tests::create_test_local_event_loop()?;
    event_loop.state.column_families.insert(
        1,
        crate::runtime::state::ColumnFamilyState::new(1, "other".into()),
    );
    let path = event_loop.state.db_path.clone();
    let spill = spill_write(&event_loop, 94_050, &[b"partial-first", b"partial-second"])?;
    let failed = event_loop
        .routing
        .router
        .register(94_050, "partial streaming apply");
    fail::cfg("midge::wal::spilled_apply_op", "1*off->return").expect("fail second spilled effect");

    // Act: complete the strict WAL unit, then fail its second memtable effect.
    event_loop.process_one(spill, &crossbeam::channel::never());
    fail::remove("midge::wal::spilled_apply_op");
    assert!(matches!(
        failed.try_recv(),
        Ok(RuntimeResponse::Error {
            error: MidgeError::OutcomeUnknown(_),
            ..
        })
    ));
    let family = event_loop.state.get_cf(0).unwrap();
    assert_eq!(
        family.memtable.get(b"partial-first")?,
        Some(b"spilled-value".to_vec())
    );
    assert_eq!(family.memtable.get(b"partial-second")?, None);
    let allocated = event_loop.state.sequence;
    let rejected = event_loop
        .routing
        .router
        .register(94_051, "cross-family cut jump");
    let mut next = write(94_051, &[b"unrelated-family"], false);
    if let RuntimeMsg::ApplyTransaction { ops, .. } = &mut next {
        if let TransactionOp::Put { cf_id, .. } = &mut ops[0] {
            *cf_id = 1;
        }
    }
    event_loop.process_one(next, &crossbeam::channel::never());

    // Assert: a different family cannot make the first partial key visible.
    assert!(matches!(
        rejected.try_recv(),
        Ok(RuntimeResponse::Error {
            error: MidgeError::Fenced(_),
            ..
        })
    ));
    assert_eq!(event_loop.state.sequence, allocated);
    assert_eq!(event_loop.state.applied_sequence, 0);
    let old = event_loop.create_read_snapshot(0).unwrap();
    assert_eq!(old.get(b"partial-first", 0)?, None);
    assert_eq!(old.get(b"partial-second", 0)?, None);
    assert_eq!(
        event_loop
            .state
            .get_cf(1)
            .unwrap()
            .memtable
            .get(b"unrelated-family")?,
        None
    );
    drop(event_loop);
    let recovered = crate::runtime::state::RuntimeState::try_new(
        path,
        false,
        crate::config::RecoveryPolicy::Strict,
    )?;
    for key in [b"partial-first".as_slice(), b"partial-second"] {
        assert_eq!(
            recovered.get_cf(0).unwrap().memtable.get(key)?,
            Some(b"spilled-value".to_vec())
        );
    }
    Ok(())
}

#[test]
fn should_drain_earlier_group_before_applying_oversized_resident_transaction() -> MidgeResult<()> {
    // Arrange: raw+encoded retention exceeds the asynchronous group budget,
    // while the existing resident WAL/replay envelope still accepts the unit.
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = super::super::tests::create_test_local_event_loop()?;
    event_loop.pipeline.inline_flush_worker = false;
    let held = HeldSync::at("midge::wal::before_physical_sync");
    let first = event_loop
        .routing
        .router
        .register(94_060, "strict before large resident");
    let large = event_loop
        .routing
        .router
        .register(94_061, "large compatibility transaction");
    let value_len = MAX_INFLIGHT_GROUP_BYTES / 4;
    let mut message = write(94_061, &[b"large-resident"], true);
    if let RuntimeMsg::ApplyTransaction { ops, .. } = &mut message {
        if let TransactionOp::Put { value, .. } = &mut ops[0] {
            *value = Bytes::from(vec![b'X'; value_len]);
        }
    }
    event_loop.process_one(
        write(94_060, &[b"before-large"], true),
        &crossbeam::channel::never(),
    );
    wait_for_hold(&mut event_loop, &held);
    let allocated = event_loop.state.sequence;

    // Act
    event_loop.process_one(message, &crossbeam::channel::never());

    // Assert: the compatibility barrier cannot cross the parked prefix.
    assert_eq!(event_loop.state.sequence, allocated);
    assert_eq!(event_loop.state.applied_sequence, 0);
    assert!(large.try_recv().is_err());
    held.release.send(()).expect("release predecessor");
    let first_cut = applied_cut(&mut event_loop, &first);
    let large_cut = applied_cut(&mut event_loop, &large);
    assert!(large_cut > first_cut);
    let snapshot = event_loop.create_read_snapshot(0).unwrap();
    let value = snapshot
        .get(b"large-resident", large_cut)?
        .expect("large value");
    assert_eq!(value.len(), value_len);
    assert!(value.iter().all(|byte| *byte == b'X'));
    Ok(())
}
