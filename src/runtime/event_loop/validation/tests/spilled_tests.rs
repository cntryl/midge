use super::*;
use crate::runtime::event_loop::wal::SpilledTransactionRequest;

#[path = "spilled_source_memory.rs"]
mod source_memory;

fn spill_request(
    event_loop: &EventLoop,
    request_id: u64,
    ops: Vec<TransactionOp>,
) -> MidgeResult<SpilledTransactionRequest> {
    let pool = Arc::new(crate::runtime::transaction_spill::TransactionMemoryPool::new(0));
    let mut intents = crate::runtime::transaction_spill::TransactionWriteSet::new(
        pool,
        &event_loop.state.fs,
        false,
        request_id,
    );
    for op in ops {
        intents.push(op)?;
    }
    Ok(SpilledTransactionRequest {
        request_id,
        source: intents.take_source(),
        assertions: Vec::new(),
        durability_policy: Some(crate::wal::DurabilityPolicy::Strict),
        start_sequence: event_loop.state.applied_sequence,
        conflict_policy: ConflictPolicy::AbortOnWriteConflict,
        validation: None,
    })
}

fn seed_spanning_sst(event_loop: &mut EventLoop) {
    let mut seed = request(946_000, b"a", false);
    seed.ops.extend(request(0, b"z", false).ops);
    apply_and_flush(event_loop, seed);
    assert_eq!(event_loop.state.applied_sequence, 4);
}

#[test]
fn should_retry_spilled_point_validation_when_intervening_conflict_is_flushed() -> MidgeResult<()> {
    assert_spilled_flushed_conflict(false, false, false)
}

#[test]
fn should_retry_spilled_insert_validation_when_intervening_value_is_flushed() -> MidgeResult<()> {
    assert_spilled_flushed_conflict(true, false, false)
}

#[test]
fn should_retry_spilled_assertion_validation_when_intervening_write_is_flushed() -> MidgeResult<()>
{
    assert_spilled_flushed_conflict(false, true, false)
}

#[test]
fn should_retry_spilled_range_validation_when_intervening_write_is_flushed() -> MidgeResult<()> {
    assert_spilled_flushed_conflict(false, false, true)
}

fn assert_spilled_flushed_conflict(
    insert_only: bool,
    assertion: bool,
    range: bool,
) -> MidgeResult<()> {
    // Arrange: the missing key lies inside an actual SST's bounds, so the
    // production validation read must query its contents rather than skip it.
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = super::super::super::tests::create_test_local_event_loop()?;
    seed_spanning_sst(&mut event_loop);
    let held = HeldCallback::first("midge::validation::before_spill_snapshot_reads");
    let mut ops = request(0, if assertion { b"n" } else { b"m" }, false).ops;
    if insert_only {
        if let TransactionOp::Put { insert_only, .. } = &mut ops[0] {
            *insert_only = true;
        }
    }
    if range {
        ops = vec![TransactionOp::DeleteRange {
            cf_id: 0,
            start_key: Bytes::from_static(b"a"),
            end_key: Bytes::from_static(b"z"),
        }];
    }
    let mut checked = spill_request(&event_loop, 946_001, ops)?;
    if insert_only || assertion {
        checked.conflict_policy = ConflictPolicy::LastWriteWins;
    }
    if assertion {
        checked.assertions.push(crate::runtime::KeyAssertion {
            cf_id: 0,
            key: Bytes::from_static(b"m"),
        });
    }
    let response = event_loop
        .routing
        .router
        .register(946_001, "checked spilled commit");
    let diagnostics = Arc::clone(&event_loop.state.diagnostics);
    let opens = diagnostics.sst_metrics().reader_cache_misses();

    // Act: the loop changes and flushes history while the detached spill
    // source and its original SST cut remain owned by the held worker.
    WalCoordinator::apply_spilled_transaction(
        &mut event_loop,
        &crossbeam::channel::never(),
        checked,
        None,
    );
    held.reached
        .recv_timeout(Duration::from_secs(10))
        .expect("spill read is held");
    apply_and_flush(&mut event_loop, request(946_002, b"m", false));
    assert_eq!(diagnostics.sst_metrics().reader_cache_misses(), opens);
    assert!(response.try_recv().is_err());
    assert!(event_loop.pipeline.validation.memory.used() > 0);
    held.release.send(()).expect("release spill validation");
    let result = settle_validation_request(&mut event_loop, &response);

    // Assert: only the existing outcome is returned; no part of the rejected
    // transaction allocates, appends or becomes visible, including on restart.
    assert!(
        matches!(&result, RuntimeResponse::Error { error, .. } if if insert_only {
        matches!(error, MidgeError::InvalidArgument(message) if message == "key already exists")
    } else { matches!(error, MidgeError::WriteConflict(_)) }),
        "{result:?}"
    );
    assert!(held.calls.load(Ordering::SeqCst) >= 2);
    assert!(diagnostics.sst_metrics().reader_cache_misses() > opens);
    assert_rejected_spill_recovers(event_loop)
}

fn assert_rejected_spill_recovers(event_loop: EventLoop) -> MidgeResult<()> {
    let path = event_loop.state.db_path.clone();
    assert_eq!(event_loop.state.applied_sequence, 7);
    assert_eq!(event_loop.state.sequence, 7);
    assert_eq!(event_loop.pipeline.validation.memory.used(), 0);
    let snapshot = event_loop.create_read_snapshot(0).unwrap();
    assert_eq!(snapshot.get(b"m", 7)?, Some(b"value".to_vec()));
    assert_eq!(snapshot.get(b"n", 7)?, None);
    drop(event_loop);
    let recovered = crate::runtime::state::RuntimeState::try_new(
        path,
        false,
        crate::config::RecoveryPolicy::Strict,
    )?;
    assert_eq!(recovered.applied_sequence, 7);
    let family = recovered.get_cf(0).unwrap();
    let snapshot = ReadSnapshot::new(
        Arc::clone(&family.memtable),
        family.immutable_memtables.clone(),
        recovered.manifest.files.clone(),
        Arc::clone(&recovered.fs),
        std::path::PathBuf::from(crate::sst::fs::SST_DIR),
        false,
        u64::MAX,
    );
    assert_eq!(snapshot.get(b"m", 7)?, Some(b"value".to_vec()));
    assert_eq!(snapshot.get(b"n", 7)?, None);
    Ok(())
}

struct ManualClock(std::sync::atomic::AtomicU64);
impl crate::common::time::Clock for ManualClock {
    fn now_millis(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }
}

#[test]
fn should_admit_spilled_insert_when_ttl_expires_after_detached_reads_without_new_sequence(
) -> MidgeResult<()> {
    // Arrange
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = super::super::super::tests::create_test_local_event_loop()?;
    let clock = Arc::new(ManualClock(std::sync::atomic::AtomicU64::new(1_000)));
    event_loop
        .state
        .install_ttl_clock(Arc::new(crate::common::time::ObservedClock::new(
            clock.clone(),
        )));
    let mut seed = request(946_010, b"m", false);
    if let TransactionOp::Put { ttl_seconds, .. } = &mut seed.ops[0] {
        *ttl_seconds = Some(1);
    }
    apply_and_flush(&mut event_loop, seed);
    let before = event_loop.create_read_snapshot(0).unwrap();
    let held = HeldCallback::first("midge::validation::after_spill_snapshot_reads");
    let mut ops = request(0, b"m", false).ops;
    if let TransactionOp::Put {
        insert_only, value, ..
    } = &mut ops[0]
    {
        *insert_only = true;
        *value = Bytes::from_static(b"replacement");
    }
    let mut insert = spill_request(&event_loop, 946_011, ops)?;
    insert.conflict_policy = ConflictPolicy::LastWriteWins;
    let response = event_loop
        .routing
        .router
        .register(946_011, "TTL crossing spilled insert");

    // Act: no source or sequence changes; only the engine clock advances.
    WalCoordinator::apply_spilled_transaction(
        &mut event_loop,
        &crossbeam::channel::never(),
        insert,
        None,
    );
    held.reached
        .recv_timeout(Duration::from_secs(10))
        .expect("completed real TTL read");
    clock.0.store(3_000, Ordering::Release);
    assert_eq!(event_loop.state.applied_sequence, 3);
    held.release.send(()).expect("admit after expiry");
    let result = settle_validation_request(&mut event_loop, &response);

    // Assert: an old-time duplicate cannot become a conservative conflict.
    assert!(matches!(
        result,
        RuntimeResponse::TransactionApplied {
            last_sequence: 6,
            ..
        }
    ));
    assert_eq!(held.calls.load(Ordering::SeqCst), 1);
    assert_eq!(before.get(b"m", 3)?, Some(b"value".to_vec()));
    assert_eq!(
        event_loop.create_read_snapshot(0).unwrap().get(b"m", 6)?,
        Some(b"replacement".to_vec())
    );
    assert_eq!(event_loop.pipeline.validation.memory.used(), 0);
    Ok(())
}

#[test]
fn should_preserve_spilled_error_order_when_earlier_ttl_duplicate_remains_live() -> MidgeResult<()>
{
    assert_ttl_error_order(false)
}

#[test]
fn should_report_later_spilled_conflict_when_earlier_ttl_duplicate_expires() -> MidgeResult<()> {
    assert_ttl_error_order(true)
}

fn assert_ttl_error_order(expire: bool) -> MidgeResult<()> {
    // Arrange: the first operation's duplicate is time-dependent; the
    // second operation's post-start conflict is permanent at this cut.
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = super::super::super::tests::create_test_local_event_loop()?;
    let clock = Arc::new(ManualClock(std::sync::atomic::AtomicU64::new(1_000)));
    event_loop
        .state
        .install_ttl_clock(Arc::new(crate::common::time::ObservedClock::new(
            clock.clone(),
        )));
    let mut seed = request(946_020, b"m", false);
    if let TransactionOp::Put { ttl_seconds, .. } = &mut seed.ops[0] {
        *ttl_seconds = Some(1);
    }
    apply_and_flush(&mut event_loop, seed);
    apply_and_flush(&mut event_loop, request(946_021, b"q", false));
    let mut ops = request(0, b"m", false).ops;
    if let TransactionOp::Put { insert_only, .. } = &mut ops[0] {
        *insert_only = true;
    }
    ops.extend(request(0, b"q", false).ops);
    let mut checked = spill_request(&event_loop, 946_022, ops)?;
    checked.start_sequence = 3;
    let response = event_loop
        .routing
        .router
        .register(946_022, "ordered spilled validation failures");
    let held = HeldCallback::first("midge::validation::after_spill_snapshot_reads");

    // Act
    WalCoordinator::apply_spilled_transaction(
        &mut event_loop,
        &crossbeam::channel::never(),
        checked,
        None,
    );
    held.reached
        .recv_timeout(Duration::from_secs(10))
        .expect("real static and TTL reads");
    if expire {
        clock.0.store(3_000, Ordering::Release);
    }
    held.release.send(()).expect("release classified errors");
    let result = settle_validation_request(&mut event_loop, &response);

    // Assert: expiry changes which existing error comes first, without a
    // conservative conflict and without any partial WAL or memtable effect.
    assert!(
        matches!(&result, RuntimeResponse::Error { error, .. } if if expire {
        matches!(error, MidgeError::WriteConflict(_))
    } else { matches!(error, MidgeError::InvalidArgument(message) if message == "key already exists") })
    );
    assert_eq!(event_loop.state.sequence, 6);
    assert_eq!(event_loop.state.applied_sequence, 6);
    assert_eq!(held.calls.load(Ordering::SeqCst), 1);
    assert_eq!(event_loop.pipeline.validation.memory.used(), 0);
    Ok(())
}

#[test]
fn should_retry_spilled_validation_when_flush_changes_sources_without_advancing_cut(
) -> MidgeResult<()> {
    // Arrange
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = super::super::super::tests::create_test_local_event_loop()?;
    seed_spanning_sst(&mut event_loop);
    let write = event_loop
        .routing
        .router
        .register(946_030, "unflushed source change fixture");
    WalCoordinator::apply_transaction(
        &mut event_loop,
        &crossbeam::channel::never(),
        request(946_030, b"q", false),
        None,
    );
    assert!(matches!(
        write.recv_timeout(Duration::from_secs(10)),
        Ok(RuntimeResponse::TransactionApplied { .. })
    ));
    let checked = spill_request(&event_loop, 946_031, request(0, b"m", false).ops)?;
    let response = event_loop
        .routing
        .router
        .register(946_031, "source generation handoff");
    let held = HeldCallback::first("midge::validation::before_spill_snapshot_reads");

    // Act: move the captured active source to an SST without another write.
    WalCoordinator::apply_spilled_transaction(
        &mut event_loop,
        &crossbeam::channel::never(),
        checked,
        None,
    );
    held.reached
        .recv_timeout(Duration::from_secs(10))
        .expect("held source generation");
    let cut = event_loop.state.applied_sequence;
    let flush = event_loop
        .routing
        .router
        .register(946_032, "source-only flush");
    FlushCoordinator::flush_memtable(&mut event_loop, 946_032, 0);
    assert!(matches!(
        flush.recv_timeout(Duration::from_secs(10)),
        Ok(RuntimeResponse::Ok { .. })
    ));
    assert_eq!(event_loop.state.applied_sequence, cut);
    held.release.send(()).expect("retry changed source tuple");
    let result = settle_validation_request(&mut event_loop, &response);

    // Assert: an unchanged sequence alone cannot bless a stale source tuple.
    assert!(held.calls.load(Ordering::SeqCst) >= 2);
    assert!(matches!(
        result,
        RuntimeResponse::TransactionApplied {
            last_sequence: 10,
            ..
        }
    ));
    let snapshot = event_loop.create_read_snapshot(0).unwrap();
    assert_eq!(snapshot.get(b"m", 10)?, Some(b"value".to_vec()));
    assert_eq!(snapshot.get(b"q", 10)?, Some(b"value".to_vec()));
    assert_eq!(event_loop.pipeline.validation.memory.used(), 0);
    Ok(())
}

#[test]
fn should_keep_large_supported_spill_values_without_raising_queued_validation_budget(
) -> MidgeResult<()> {
    // Arrange: a legal individual spilled value exceeds the entire existing
    // retained-metadata queue budget. It is decoded only on the one worker.
    let mut event_loop = super::super::super::tests::create_test_local_event_loop()?;
    seed_spanning_sst(&mut event_loop);
    let value = Bytes::from(vec![3; 9 * 1024 * 1024]);
    let checksum = crc32c::crc32c(&value);
    let checked = spill_request(
        &event_loop,
        946_040,
        vec![TransactionOp::Put {
            cf_id: 0,
            key: Bytes::from_static(b"m"),
            value,
            ttl_seconds: None,
            insert_only: true,
        }],
    )?;
    let response = event_loop
        .routing
        .router
        .register(946_040, "large streamed value");

    // Act
    WalCoordinator::apply_spilled_transaction(
        &mut event_loop,
        &crossbeam::channel::never(),
        checked,
        None,
    );
    let result = settle_validation_request(&mut event_loop, &response);

    // Assert: payload size does not become a new fixed queue-admission limit.
    assert!(matches!(
        result,
        RuntimeResponse::TransactionApplied {
            last_sequence: 7,
            ..
        }
    ));
    assert_eq!(
        event_loop.pipeline.validation.memory.limit(),
        8 * 1024 * 1024
    );
    assert_eq!(event_loop.pipeline.validation.memory.used(), 0);
    let snapshot = event_loop.create_read_snapshot(0).unwrap();
    let stored = snapshot.get_bytes(b"m", 7)?.expect("whole large value");
    assert_eq!(stored.len(), 9 * 1024 * 1024);
    assert_eq!(crc32c::crc32c(&stored), checksum);
    Ok(())
}

#[test]
fn should_wait_for_consumed_spill_completion_before_reading_another_workspace() -> MidgeResult<()> {
    // Arrange: both jobs must execute real SST queries, but only one result
    // may retain decoded error/input memory before the next read starts.
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = super::super::super::tests::create_test_local_event_loop()?;
    seed_spanning_sst(&mut event_loop);
    let first = spill_request(&event_loop, 946_050, request(0, b"m", false).ops)?;
    let second = spill_request(&event_loop, 946_051, request(0, b"n", false).ops)?;
    let first_response = event_loop
        .routing
        .router
        .register(946_050, "first streamed workspace");
    let second_response = event_loop
        .routing
        .router
        .register(946_051, "second streamed workspace");
    let held = HeldCallback::first("midge::validation::after_spill_snapshot_reads");

    // Act: let the first real read finish while the loop has not consumed its
    // completion. The queued second job must still own only its input metadata.
    WalCoordinator::apply_spilled_transaction(
        &mut event_loop,
        &crossbeam::channel::never(),
        first,
        None,
    );
    held.reached
        .recv_timeout(Duration::from_secs(10))
        .expect("first real read");
    WalCoordinator::apply_spilled_transaction(
        &mut event_loop,
        &crossbeam::channel::never(),
        second,
        None,
    );
    held.release.send(()).expect("finish first streamed result");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !event_loop.pipeline.validation.has_ready_result() {
        assert!(Instant::now() < deadline, "first read did not finish");
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(held.calls.load(Ordering::SeqCst), 1);
    assert!(first_response.try_recv().is_err());
    assert!(second_response.try_recv().is_err());
    let first = settle_validation_request(&mut event_loop, &first_response);
    let second = settle_validation_request(&mut event_loop, &second_response);

    // Assert: both caller operations complete whole and source/cut retry
    // handles the second job captured before the first was published.
    assert!(matches!(
        first,
        RuntimeResponse::TransactionApplied {
            last_sequence: 7,
            ..
        }
    ));
    assert!(matches!(
        second,
        RuntimeResponse::TransactionApplied {
            last_sequence: 10,
            ..
        }
    ));
    assert!(held.calls.load(Ordering::SeqCst) >= 3);
    let snapshot = event_loop.create_read_snapshot(0).unwrap();
    assert_eq!(snapshot.get(b"m", 10)?, Some(b"value".to_vec()));
    assert_eq!(snapshot.get(b"n", 10)?, Some(b"value".to_vec()));
    assert_eq!(event_loop.pipeline.validation.memory.used(), 0);
    Ok(())
}

#[test]
fn should_keep_original_spill_deadline_after_detached_reads_complete() -> MidgeResult<()> {
    // Arrange
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = super::super::super::tests::create_test_local_event_loop()?;
    seed_spanning_sst(&mut event_loop);
    let checked = spill_request(&event_loop, 946_060, request(0, b"m", false).ops)?;
    let held = HeldCallback::first("midge::validation::after_spill_snapshot_reads");
    let deadline = OperationDeadline::from_budget(Duration::from_millis(100));
    let response = event_loop.routing.router.register_with_deadline(
        946_060,
        "original streamed deadline",
        Instant::now(),
        deadline,
    );

    // Act: completion handoff cannot restart the caller's original budget.
    WalCoordinator::apply_spilled_transaction(
        &mut event_loop,
        &crossbeam::channel::never(),
        checked,
        None,
    );
    held.reached
        .recv_timeout(Duration::from_secs(10))
        .expect("completed spill reads");
    while !deadline.is_expired() {
        std::thread::sleep(Duration::from_millis(1));
    }
    held.release.send(()).expect("release expired caller");
    let result = settle_validation_request(&mut event_loop, &response);

    // Assert: no sequence or WAL effect was admitted after expiration.
    assert!(matches!(
        result,
        RuntimeResponse::Error {
            error: MidgeError::Timeout(_),
            ..
        }
    ));
    assert_eq!(event_loop.state.sequence, 4);
    assert_eq!(event_loop.state.applied_sequence, 4);
    assert_eq!(
        event_loop.create_read_snapshot(0).unwrap().get(b"m", 4)?,
        None
    );
    assert_eq!(event_loop.pipeline.validation.memory.used(), 0);
    Ok(())
}

#[test]
fn should_reject_completed_spill_validation_after_writer_epoch_changes() -> MidgeResult<()> {
    // Arrange
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = super::super::super::tests::create_test_local_event_loop()?;
    seed_spanning_sst(&mut event_loop);
    let checked = spill_request(&event_loop, 946_070, request(0, b"m", false).ops)?;
    let response = event_loop
        .routing
        .router
        .register(946_070, "stale streamed writer epoch");
    let held = HeldCallback::first("midge::validation::after_spill_snapshot_reads");

    // Act: a read result from an earlier epoch cannot authorize a WAL effect.
    WalCoordinator::apply_spilled_transaction(
        &mut event_loop,
        &crossbeam::channel::never(),
        checked,
        None,
    );
    held.reached
        .recv_timeout(Duration::from_secs(10))
        .expect("completed old epoch reads");
    event_loop.state.writer_epoch += 1;
    held.release.send(()).expect("complete stale validation");
    let result = settle_validation_request(&mut event_loop, &response);

    // Assert
    assert!(matches!(
        result,
        RuntimeResponse::Error {
            error: MidgeError::Fenced(_),
            ..
        }
    ));
    assert_eq!(event_loop.state.sequence, 4);
    assert_eq!(event_loop.state.applied_sequence, 4);
    assert_eq!(
        event_loop.create_read_snapshot(0).unwrap().get(b"m", 4)?,
        None
    );
    assert_eq!(event_loop.pipeline.validation.memory.used(), 0);
    Ok(())
}
