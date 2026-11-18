//! Opt-in amortized sync floor (#904): the ledger floor edit may trail a strict
//! group's acknowledged fsync, but never covers a byte no fsync completed and
//! never lands on a segment it does not belong to.

use super::tests::{settle, write};
use super::*;
use crate::config::AmortizedSyncFloor;
use crate::runtime::TestRuntimeMsg;
use crate::wal::format::HEADER_LEN;

const HEADER: u64 = HEADER_LEN as u64;

fn window(bytes: u64, delay: Duration) -> AmortizedSyncFloor {
    AmortizedSyncFloor {
        max_unrecorded_bytes: bytes,
        max_unrecorded_delay: delay,
    }
}

fn loop_with_floor(config: AmortizedSyncFloor) -> MidgeResult<EventLoop> {
    let mut event_loop = super::super::tests::create_test_local_event_loop()?;
    event_loop.pipeline.inline_flush_worker = false;
    event_loop.actors.wal_actor.set_sync_floor(Some(config));
    Ok(event_loop)
}

fn commit_strict(event_loop: &mut EventLoop, request_id: u64, key: &'static [u8]) -> u64 {
    let response = event_loop
        .routing
        .router
        .register(request_id, "strict commit under an amortized floor");
    event_loop.process_one(
        write(request_id, &[key], true),
        &crossbeam::channel::never(),
    );
    match settle(event_loop, &response) {
        RuntimeResponse::TransactionApplied { last_sequence, .. } => last_sequence,
        response => panic!("strict commit failed: {response:?}"),
    }
}

fn recorded_floor(event_loop: &EventLoop) -> Option<u64> {
    event_loop
        .state
        .wal
        .history
        .ledger()
        .open_member()
        .durable_bytes
}

fn open_segment_len(event_loop: &EventLoop) -> MidgeResult<u64> {
    let path = crate::wal::segment_key(event_loop.state.wal.open_segment());
    Ok(event_loop
        .state
        .fs
        .metadata(&path)
        .map_err(crate::io::FsError::into_midge)?
        .len)
}

fn ledger_file_syncs(event_loop: &EventLoop) -> u64 {
    event_loop
        .state
        .wal
        .history
        .ledger_sync_counts()
        .file_completed
}

fn ledger_proofs(event_loop: &EventLoop) -> u64 {
    event_loop
        .state
        .diagnostics
        .counters()
        .wal_sync_phases
        .ledger_proof_count
}

fn leaked_key(ordinal: usize) -> &'static [u8] {
    Box::leak(
        format!("floor-key-{ordinal:04}")
            .into_bytes()
            .into_boxed_slice(),
    )
}

#[test]
fn should_skip_ledger_edit_when_strict_sync_is_inside_floor_window() -> MidgeResult<()> {
    // Arrange: warm the sequence reservation so only floor edits are counted.
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = loop_with_floor(window(1024 * 1024, Duration::from_secs(10)))?;
    commit_strict(&mut event_loop, 95_000, b"warmup");
    let ledger_syncs = ledger_file_syncs(&event_loop);
    let wal_fsyncs = event_loop.state.diagnostics.counters().wal_fsync_count;
    let proofs = ledger_proofs(&event_loop);

    // Act
    let first = commit_strict(&mut event_loop, 95_001, b"inside-a");
    let second = commit_strict(&mut event_loop, 95_002, b"inside-b");

    // Assert: both groups are acknowledged after their own fsync, the ledger
    // was not written, and the deferred end is exactly the synced length.
    let counters = event_loop.state.diagnostics.counters();
    let segment = event_loop.state.wal.open_segment();
    let len = open_segment_len(&event_loop)?;
    assert!(second > first);
    assert_eq!(counters.wal_fsync_count - wal_fsyncs, 2);
    assert_eq!(ledger_file_syncs(&event_loop), ledger_syncs);
    assert_eq!(ledger_proofs(&event_loop), proofs);
    assert_eq!(recorded_floor(&event_loop), Some(HEADER));
    assert!(len > HEADER);
    assert_eq!(
        event_loop.actors.wal_actor.unrecorded_floor_for_test(),
        Some((segment, len))
    );
    Ok(())
}

#[test]
fn should_record_floor_when_unrecorded_bytes_reach_threshold() -> MidgeResult<()> {
    // Arrange
    const THRESHOLD: u64 = 4 * 1024;
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = loop_with_floor(window(THRESHOLD, Duration::from_secs(10)))?;
    commit_strict(&mut event_loop, 95_100, b"warmup");
    let proofs = ledger_proofs(&event_loop);
    let mut commits = 0_usize;

    // Act: commit until the ledger floor moves.
    let recorded = loop {
        commits += 1;
        commit_strict(
            &mut event_loop,
            95_100 + commits as u64,
            leaked_key(commits),
        );
        let len = open_segment_len(&event_loop)?;
        let floor = recorded_floor(&event_loop);
        if floor != Some(HEADER) {
            break (floor, len);
        }
        // Assert (inside the window): the floor never moved early.
        assert!(
            len - HEADER < THRESHOLD,
            "{} unrecorded bytes exceed the {THRESHOLD}-byte window",
            len - HEADER
        );
        assert_eq!(
            event_loop
                .actors
                .wal_actor
                .unrecorded_floor_for_test()
                .map(|(_, end)| end),
            Some(len)
        );
        assert!(commits < 500, "the floor never reached the threshold");
    };

    // Assert: the edit recorded the exact synced end, and cleared the window.
    assert!(commits > 1, "the first small commit must stay deferred");
    assert_eq!(recorded.0, Some(recorded.1));
    assert!(recorded.1 - HEADER >= THRESHOLD);
    assert_eq!(
        event_loop.actors.wal_actor.unrecorded_floor_for_test(),
        None
    );
    assert_eq!(ledger_proofs(&event_loop) - proofs, 1);
    Ok(())
}

#[test]
fn should_record_exact_floor_when_rotating_with_unrecorded_window() -> MidgeResult<()> {
    // Arrange
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = loop_with_floor(window(1024 * 1024, Duration::from_secs(10)))?;
    commit_strict(&mut event_loop, 95_300, b"warmup");
    commit_strict(&mut event_loop, 95_301, b"before-rotation");
    let old_segment = event_loop.state.wal.open_segment();
    let old_len = open_segment_len(&event_loop)?;
    assert!(event_loop
        .actors
        .wal_actor
        .unrecorded_floor_for_test()
        .is_some());
    let rotate = event_loop.routing.router.register(95_302, "rotate");

    // Act
    event_loop.process_one(
        RuntimeMsg::Test(TestRuntimeMsg::WalRotate { request_id: 95_302 }),
        &crossbeam::channel::never(),
    );
    assert!(matches!(
        settle(&mut event_loop, &rotate),
        RuntimeResponse::Ok { .. }
    ));
    commit_strict(&mut event_loop, 95_303, b"after-rotation");

    // Assert: the sealed segment is exact, the deferred end did not follow the
    // rotation, and the new segment starts a fresh window of its own.
    let ledger = event_loop.state.wal.history.ledger();
    let sealed = ledger
        .member_facts()
        .find(|member| member.id() == old_segment)
        .and_then(|member| member.sealed.as_ref())
        .expect("the rotated segment is sealed");
    assert_eq!(sealed.data_len, old_len);
    let new_segment = event_loop.state.wal.open_segment();
    assert!(new_segment > old_segment);
    assert_eq!(recorded_floor(&event_loop), Some(HEADER));
    assert_eq!(
        event_loop.actors.wal_actor.unrecorded_floor_for_test(),
        Some((new_segment, open_segment_len(&event_loop)?))
    );
    Ok(())
}

#[test]
fn should_discard_deferred_floor_when_it_belongs_to_another_segment() -> MidgeResult<()> {
    // Arrange: plant a deferred end far past anything the new segment holds, as
    // if a rotation had failed to clear it.
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = loop_with_floor(window(1, Duration::from_secs(10)))?;
    commit_strict(&mut event_loop, 95_400, b"warmup");
    let rotate = event_loop.routing.router.register(95_401, "rotate");
    event_loop.process_one(
        RuntimeMsg::Test(TestRuntimeMsg::WalRotate { request_id: 95_401 }),
        &crossbeam::channel::never(),
    );
    settle(&mut event_loop, &rotate);
    let old_segment = event_loop.state.wal.open_segment() - 1;
    event_loop
        .actors
        .wal_actor
        .plant_unrecorded_floor_for_test(old_segment, 1_000_000);

    // Act
    commit_strict(&mut event_loop, 95_402, b"new-segment");

    // Assert: the floor recorded on the new segment is its own synced end.
    let len = open_segment_len(&event_loop)?;
    assert!(len < 1_000_000);
    assert_eq!(recorded_floor(&event_loop), Some(len));
    assert_eq!(
        event_loop.actors.wal_actor.unrecorded_floor_for_test(),
        None
    );
    Ok(())
}

#[test]
fn should_clear_deferred_floor_when_an_exact_sync_records_the_floor() -> MidgeResult<()> {
    // Arrange
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = loop_with_floor(window(1024 * 1024, Duration::from_secs(10)))?;
    commit_strict(&mut event_loop, 95_500, b"warmup");
    commit_strict(&mut event_loop, 95_501, b"deferred");
    assert_eq!(recorded_floor(&event_loop), Some(HEADER));

    // Act: a ticketed barrier is always exact.
    event_loop.sync_current_wal()?;

    // Assert
    assert_eq!(
        recorded_floor(&event_loop),
        Some(open_segment_len(&event_loop)?)
    );
    assert_eq!(
        event_loop.actors.wal_actor.unrecorded_floor_for_test(),
        None
    );
    Ok(())
}

#[test]
fn should_clear_deferred_floor_when_the_wal_is_fenced() -> MidgeResult<()> {
    // Arrange
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = loop_with_floor(window(1024 * 1024, Duration::from_secs(10)))?;
    commit_strict(&mut event_loop, 95_600, b"warmup");
    commit_strict(&mut event_loop, 95_601, b"deferred");
    assert!(event_loop
        .actors
        .wal_actor
        .unrecorded_floor_for_test()
        .is_some());

    // Act
    event_loop
        .actors
        .wal_actor
        .fence_transition(&mut event_loop.state, "test fence");

    // Assert
    assert_eq!(
        event_loop.actors.wal_actor.unrecorded_floor_for_test(),
        None
    );
    Ok(())
}

fn buffered_write(request_id: u64, key: &'static [u8]) -> RuntimeMsg {
    let mut message = write(request_id, &[key], false);
    if let RuntimeMsg::ApplyTransaction {
        durability_policy, ..
    } = &mut message
    {
        *durability_policy = Some(crate::wal::DurabilityPolicy::Batched);
    }
    message
}

/// Keep the batched-window sync out of the way so only the floor timer acts.
fn silence_batched_sync(event_loop: &mut EventLoop) {
    event_loop.actors.wal_actor.set_durability(
        crate::wal::DurabilityPolicy::Batched,
        crate::wal::policy::BatchConfig {
            max_delay_ms: 3_600_000,
            max_bytes: usize::MAX,
        },
    );
}

#[test]
fn should_bound_idle_wait_by_delay_without_writing_when_delay_has_not_elapsed() -> MidgeResult<()> {
    // Arrange: the maximum delay cannot elapse during the test.
    const DELAY: Duration = Duration::from_secs(10);
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = loop_with_floor(window(1024 * 1024, DELAY))?;
    silence_batched_sync(&mut event_loop);
    commit_strict(&mut event_loop, 95_690, b"warmup");
    commit_strict(&mut event_loop, 95_691, b"idle-tail");
    let ledger_syncs = ledger_file_syncs(&event_loop);

    // Act
    let timeout = event_loop.idle_progress_timeout();
    event_loop.record_due_sync_floor();

    // Assert
    assert!(timeout.is_some_and(|wait| wait <= DELAY), "{timeout:?}");
    assert_eq!(recorded_floor(&event_loop), Some(HEADER));
    assert_eq!(ledger_file_syncs(&event_loop), ledger_syncs);
    assert!(event_loop
        .actors
        .wal_actor
        .unrecorded_floor_for_test()
        .is_some());
    Ok(())
}

#[test]
fn should_not_report_floor_deadline_when_the_transition_is_not_ready() -> MidgeResult<()> {
    // Arrange: an overdue deferral while a sync ticket holds the protocol
    // phase non-Ready and the actor is still Open.
    const DELAY: Duration = Duration::from_millis(20);
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = loop_with_floor(window(1024 * 1024, DELAY))?;
    silence_batched_sync(&mut event_loop);
    commit_strict(&mut event_loop, 95_650, b"warmup");
    commit_strict(&mut event_loop, 95_651, b"overdue");
    std::thread::sleep(DELAY + Duration::from_millis(30));
    let generation = event_loop.pipeline.durability.current_key();
    let _ticket = event_loop.pipeline.wal_transition.begin_sync(generation)?;
    assert!(event_loop.actors.wal_actor.is_open());

    // Act
    let actionable = event_loop.has_actionable_work();
    let timeout = event_loop.idle_progress_timeout();

    // Assert: the timer cannot write now, so it must not spin the loop.
    assert!(!actionable, "an unwritable floor is not actionable work");
    assert_ne!(timeout, Some(Duration::ZERO));
    Ok(())
}

#[test]
fn should_record_floor_on_idle_tail_when_delay_elapses() -> MidgeResult<()> {
    // Arrange
    const DELAY: Duration = Duration::from_millis(40);
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = loop_with_floor(window(1024 * 1024, DELAY))?;
    silence_batched_sync(&mut event_loop);
    commit_strict(&mut event_loop, 95_700, b"warmup");
    commit_strict(&mut event_loop, 95_701, b"idle-tail");
    let len = open_segment_len(&event_loop)?;
    let ledger_syncs = ledger_file_syncs(&event_loop);

    // Act: once the delay has elapsed one idle pass records the exact end.
    std::thread::sleep(DELAY + Duration::from_millis(40));
    let actionable = event_loop.has_actionable_work();
    event_loop.progress_pass(&crossbeam::channel::never());

    // Assert
    assert!(actionable, "a due floor edit is actionable work");
    assert_eq!(recorded_floor(&event_loop), Some(len));
    assert_eq!(ledger_file_syncs(&event_loop) - ledger_syncs, 1);
    assert_eq!(
        event_loop.actors.wal_actor.unrecorded_floor_for_test(),
        None
    );
    assert_eq!(event_loop.actors.wal_actor.floor_deadline_timeout(), None);
    Ok(())
}

#[test]
fn should_never_record_floor_past_completed_sync_when_later_append_is_queued() -> MidgeResult<()> {
    // Arrange: a buffered append sits in the segment past the last fsync.
    const DELAY: Duration = Duration::from_millis(50);
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = loop_with_floor(window(1024 * 1024, DELAY))?;
    silence_batched_sync(&mut event_loop);
    commit_strict(&mut event_loop, 95_800, b"warmup");
    commit_strict(&mut event_loop, 95_801, b"synced");
    let synced_end = open_segment_len(&event_loop)?;
    let response = event_loop
        .routing
        .router
        .register(95_802, "buffered append");
    event_loop.process_one(
        buffered_write(95_802, b"unsynced"),
        &crossbeam::channel::never(),
    );
    settle(&mut event_loop, &response);
    let writer_end = open_segment_len(&event_loop)?;
    assert!(writer_end > synced_end);

    // Act
    std::thread::sleep(DELAY + Duration::from_millis(30));
    event_loop.record_due_sync_floor();

    // Assert: the edit covers the completed fsync, never the queued append.
    assert_eq!(recorded_floor(&event_loop), Some(synced_end));
    assert!(synced_end < writer_end);
    Ok(())
}

#[test]
fn should_discard_stale_deferred_floor_when_timer_runs_on_another_segment() -> MidgeResult<()> {
    // Arrange
    const DELAY: Duration = Duration::from_millis(30);
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = loop_with_floor(window(1024 * 1024, DELAY))?;
    silence_batched_sync(&mut event_loop);
    commit_strict(&mut event_loop, 95_900, b"warmup");
    let rotate = event_loop.routing.router.register(95_901, "rotate");
    event_loop.process_one(
        RuntimeMsg::Test(TestRuntimeMsg::WalRotate { request_id: 95_901 }),
        &crossbeam::channel::never(),
    );
    settle(&mut event_loop, &rotate);
    let old_segment = event_loop.state.wal.open_segment() - 1;
    event_loop
        .actors
        .wal_actor
        .plant_unrecorded_floor_for_test(old_segment, 1_000_000);
    std::thread::sleep(DELAY + Duration::from_millis(20));

    // Act
    event_loop.record_due_sync_floor();

    // Assert: nothing was recorded on the new segment and the stale end is gone.
    assert_eq!(recorded_floor(&event_loop), Some(HEADER));
    assert_eq!(
        event_loop.actors.wal_actor.unrecorded_floor_for_test(),
        None
    );
    assert!(!event_loop.actors.wal_actor.is_fenced());
    Ok(())
}

#[test]
fn should_record_exact_floor_when_storage_verification_barrier_begins() -> MidgeResult<()> {
    // Arrange
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = loop_with_floor(window(1024 * 1024, Duration::from_secs(10)))?;
    commit_strict(&mut event_loop, 96_000, b"warmup");
    commit_strict(&mut event_loop, 96_001, b"deferred");
    let len = open_segment_len(&event_loop)?;
    let wal_fsyncs = event_loop.state.diagnostics.counters().wal_fsync_count;
    assert_eq!(recorded_floor(&event_loop), Some(HEADER));
    let begin = event_loop
        .routing
        .router
        .register(96_002, "begin verification");

    // Act
    event_loop.process_one(
        RuntimeMsg::BeginStorageVerification { request_id: 96_002 },
        &crossbeam::channel::never(),
    );
    let response = settle(&mut event_loop, &begin);

    // Assert: the barrier answers only after the floor is exact, and it did
    // not need a second fsync because the deferred end was already synced.
    assert!(matches!(
        response,
        RuntimeResponse::StorageVerificationBarrier { .. }
    ));
    assert_eq!(recorded_floor(&event_loop), Some(len));
    assert_eq!(
        event_loop.actors.wal_actor.unrecorded_floor_for_test(),
        None
    );
    assert_eq!(
        event_loop.state.diagnostics.counters().wal_fsync_count,
        wal_fsyncs
    );
    Ok(())
}

#[test]
fn should_not_write_ledger_when_storage_verification_barrier_begins_with_exact_floor(
) -> MidgeResult<()> {
    // Arrange: the default configuration records every strict sync exactly.
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = super::super::tests::create_test_local_event_loop()?;
    event_loop.pipeline.inline_flush_worker = false;
    commit_strict(&mut event_loop, 96_100, b"warmup");
    commit_strict(&mut event_loop, 96_101, b"exact");
    let ledger_syncs = ledger_file_syncs(&event_loop);
    let begin = event_loop
        .routing
        .router
        .register(96_102, "begin verification");

    // Act
    event_loop.process_one(
        RuntimeMsg::BeginStorageVerification { request_id: 96_102 },
        &crossbeam::channel::never(),
    );
    settle(&mut event_loop, &begin);

    // Assert: with the option off the barrier is exactly as before.
    assert_eq!(ledger_file_syncs(&event_loop), ledger_syncs);
    Ok(())
}
