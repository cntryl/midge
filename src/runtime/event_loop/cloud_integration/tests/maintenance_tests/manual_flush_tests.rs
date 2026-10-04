use super::*;
use crate::common::{MidgeError, MidgeResult};
use crate::types::KeyState;

mod controlled_storage;
mod paused_compactor;
mod publication_orders;
mod retained_work_tests;

const MANUAL_KEY: &[u8] = b"manual-flush-during-compute";
const MANUAL_VALUE: &[u8] = b"acknowledged-value";
const TEST_WAIT: Duration = Duration::from_secs(3);

fn acknowledge_put(el: &mut EventLoop, cf_id: u32, request_id: u64) -> MidgeResult<u64> {
    let response = el.router.register(request_id, "ApplyTransaction");
    let (_, messages) = crossbeam::channel::unbounded();
    el.handle_runtime_msg(
        RuntimeMsg::ApplyTransaction {
            request_id,
            ops: vec![crate::runtime::TransactionOp::Put {
                cf_id,
                key: Bytes::from_static(MANUAL_KEY),
                value: Bytes::from_static(MANUAL_VALUE),
                ttl_seconds: None,
                insert_only: false,
            }],
            assertions: Vec::new(),
            durability_policy: Some(DurabilityPolicy::CloudAsync),
            start_sequence: None,
            conflict_policy: ConflictPolicy::LastWriteWins,
            response_tx: None,
        },
        &messages,
    );
    match response.recv_timeout(TEST_WAIT) {
        Ok(RuntimeResponse::TransactionApplied {
            last_sequence,
            op_count: 1,
            ..
        }) => {
            // The acknowledged sequence names TxnCommit. This single Put's
            // operation sequence immediately precedes that marker.
            last_sequence.checked_sub(1).ok_or_else(|| {
                MidgeError::Internal(
                    "one-Put acknowledgement omitted its operation sequence".into(),
                )
            })
        }
        actual => Err(MidgeError::Internal(format!(
            "expected actual transaction acknowledgement: {actual:?}"
        ))),
    }
}

fn submit_manual_flush(
    el: &mut EventLoop,
    cf_id: u32,
    request_id: u64,
) -> crossbeam::channel::Receiver<RuntimeResponse> {
    let response = el.router.register(request_id, "FlushMemtable");
    let (_, messages) = crossbeam::channel::unbounded();
    el.handle_runtime_msg(RuntimeMsg::FlushMemtable { request_id, cf_id }, &messages);
    response
}

fn flush_while_compute_is_held(el: &mut EventLoop) -> Result<(), String> {
    for phase in ["build", "publish", "mirror"] {
        let completion = el
            .flush_worker_result_rx
            .recv_timeout(TEST_WAIT)
            .map_err(|error| format!("flush {phase} while compactor held: {error}"))?;
        el.handle_flush_worker_result(completion);
    }
    Ok(())
}

fn journal_len(el: &EventLoop) -> MidgeResult<u64> {
    match el
        .state
        .fs
        .metadata(&crate::io::FsPath::new(crate::metadata::files::JOURNAL))
    {
        Ok(metadata) => Ok(metadata.len),
        Err(FsError::NotFound(_)) => Ok(0),
        Err(error) => Err(error.into_midge()),
    }
}

fn settle_owned_work(el: &mut EventLoop, worker: &crossbeam::channel::Receiver<RuntimeMsg>) {
    let (_, messages) = crossbeam::channel::unbounded();
    let deadline = Instant::now() + TEST_WAIT;
    let mut failures = Vec::new();
    loop {
        while let Ok(message) = worker.try_recv() {
            el.handle_runtime_msg(message, &messages);
        }
        while let Ok(completion) = el.flush_worker_result_rx.try_recv() {
            record_flush_failure(&completion, &mut failures);
            el.handle_flush_worker_result(completion);
        }
        while let Ok(completion) = el.compaction_publish_result_rx.try_recv() {
            if let Err(error) = &completion.result {
                failures.push(format!("compaction {:?}: {error}", completion.phase));
            }
            CompactionCoordinator::handle_publication_completion(el, completion);
        }
        if let Some(message) = el.pending_msg.take() {
            el.process_restored_one(message, &messages);
        }
        el.tick_hybrid_storage();
        el.drain_hybrid_storage_events();
        el.reap_cloud_wal_prune_worker();
        if el.state.active_compactions.load(Ordering::Acquire) == 0
            && !el.flush_actor.is_inflight()
            && !el.compaction_publish_actor.is_inflight()
            && el.compaction_publication.get().is_none()
            && el
                .state
                .column_families
                .values()
                .all(|cf| cf.immutable_flushes.is_empty())
            && el.pending_msg.is_none()
            && el.publication_gate.deferred_messages_is_empty()
        {
            // Settle already accepted ownership; another optional prune turn
            // would keep a completed fixture busy without draining its work.
            el.join_cloud_wal_prune_worker();
            assert!(
                !el.publication_gate.is_active(),
                "{}",
                describe_settle_state(el)
            );
            return;
        }
        el.schedule_next_flush_worker();
        assert!(
            Instant::now() < deadline,
            "released accepted workers must settle: {}; failures={failures:?}",
            describe_settle_state(el)
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn record_flush_failure(
    completion: &crate::runtime::actors::flush::FlushWorkerResult,
    failures: &mut Vec<String>,
) {
    use crate::runtime::actors::flush::FlushWorkerResult;
    let (phase, error) = match completion {
        FlushWorkerResult::Build(completion) => ("build", completion.result.as_ref().err()),
        FlushWorkerResult::Publish(completion) => ("publish", completion.result.as_ref().err()),
        FlushWorkerResult::Mirror(completion) => ("mirror", completion.result.as_ref().err()),
    };
    if let Some(error) = error {
        failures.push(format!("flush {phase}: {error}"));
    }
}

fn describe_settle_state(el: &EventLoop) -> String {
    let flushes: Vec<_> = el
        .state
        .column_families
        .iter()
        .flat_map(|(cf_id, cf)| {
            cf.immutable_flushes.iter().map(|flush| {
                (
                    *cf_id,
                    flush.flush_id,
                    flush.phase,
                    flush.sst_name.as_deref(),
                    flush.failures,
                )
            })
        })
        .collect();
    let publication = el.compaction_publication.get().is_some();
    format!(
        "db={:?}, compute={}, flush_worker={}, publisher={}, publication={publication:?}, gate={}, deferred={:?}, pending={:?}, prune={}, flushes={flushes:?}",
        el.state.db_path,
        el.state.active_compactions.load(Ordering::Acquire),
        el.flush_actor.is_inflight(),
        el.compaction_publish_actor.is_inflight(),
        el.publication_gate.is_active(),
        el.publication_gate.deferred_request_ids(),
        el.pending_msg.as_ref().and_then(RuntimeMsg::request_id),
        el.cloud_coordinator.cloud_wal_prune_worker.is_some(),
    )
}

fn manifest_row_states(
    el: &EventLoop,
    cf_id: u32,
    key: &[u8],
) -> Vec<(String, MidgeResult<KeyState>)> {
    el.state
        .manifest
        .files
        .iter()
        .filter(|file| file.cf_id == cf_id)
        .map(|file| {
            let state = el
                .compaction_actor
                .open_sst_reader(&std::path::Path::new("sst").join(&file.name))
                .and_then(|reader| reader.get_state_at(key, u64::MAX));
            (file.name.clone(), state)
        })
        .collect()
}

fn row_matches(states: &[(String, MidgeResult<KeyState>)], value: &[u8], sequence: u64) -> bool {
    states.iter().any(|(_, state)| {
        matches!(state, Ok(KeyState::Value(bytes, seq, None, EntryType::Put))
            if bytes.as_ref() == value && *seq == sequence)
    })
}

fn manifest_has_row(el: &EventLoop, cf_id: u32, key: &[u8], value: &[u8], sequence: u64) -> bool {
    let states = manifest_row_states(el, cf_id, key);
    let found = row_matches(&states, value, sequence);
    if !found {
        eprintln!("SST row mismatch cf={cf_id} key={key:?} sequence={sequence} states={states:?}");
    }
    found
}

fn assert_acknowledged_and_recovered_rows(el: &EventLoop, cf_id: u32, sequence: u64) {
    assert!(manifest_has_row(
        el,
        cf_id,
        MANUAL_KEY,
        MANUAL_VALUE,
        sequence
    ));
    assert!(manifest_has_row(
        el,
        cf_id,
        b"prune-candidate",
        b"value",
        81
    ));
}

#[test]
fn should_complete_manual_cloud_flush_while_compaction_compute_waits() -> MidgeResult<()> {
    // Arrange: align seeded durable metadata with the next real write's allocator.
    let (mut el, worker, _) = cloud_debt(4)?;
    el.state.sequence = el.state.manifest.last_persisted_sequence;
    let (mut release, reached) = paused_compactor::install(&mut el)?;
    let plan = el
        .compaction_actor
        .check_manual_compaction(&el.state)?
        .expect("real cloud debt plan");
    let cf_id = plan.cf_id;
    let inputs = plan.input_files.clone();
    el.launch_compaction(plan)?;
    reached
        .recv_timeout(TEST_WAIT)
        .expect("actual compactor finalized and paused");
    el.state.set_compaction_enabled(false);
    let sequence = acknowledge_put(&mut el, cf_id, 91_301)?;

    // Act: no synthetic waiter/active marker and no compute cancellation.
    let response = submit_manual_flush(&mut el, cf_id, 91_302);
    let flush_id = el.state.get_cf(cf_id).unwrap().immutable_flushes[0].flush_id;
    let flushed = flush_while_compute_is_held(&mut el);
    let before_release = response.try_recv();
    let compactor_owned = el.state.active_compactions.load(Ordering::Acquire) == 1;
    let inputs_retained = inputs
        .iter()
        .all(|name| remote_sst_path_for_test(&el, name).exists());
    let prune_not_started = el.cloud_coordinator.cloud_wal_prune_worker.is_none();
    let states_before_release = manifest_row_states(&el, cf_id, MANUAL_KEY);
    let row_before_release = row_matches(&states_before_release, MANUAL_VALUE, sequence);
    release.release();
    settle_owned_work(&mut el, &worker);

    // Assert: clean up the real compactor even on the pre-fix receive timeout.
    assert!(
        flushed.is_ok(),
        "accepted explicit flush must finish before compute release: {flushed:?}"
    );
    assert!(
        matches!(
            before_release,
            Ok(RuntimeResponse::Ok { request_id: 91_302 })
        ),
        "acknowledged-data barrier must answer before compute release: {before_release:?}"
    );
    assert!(compactor_owned && inputs_retained && prune_not_started);
    assert!(
        row_before_release,
        "exact SST row must exist before compute release: cf={cf_id} key={MANUAL_KEY:?} sequence={sequence} states={states_before_release:?}"
    );
    assert!(el.state.immutable_flush_by_id(flush_id).is_none());
    assert!(manifest_has_row(
        &el,
        cf_id,
        MANUAL_KEY,
        MANUAL_VALUE,
        sequence
    ));
    assert!(manifest_has_row(
        &el,
        cf_id,
        b"prune-candidate",
        b"value",
        81
    ));
    assert_eq!(el.state.flush_metrics.publish_count, 1);
    Ok(())
}

#[test]
fn should_keep_optional_maintenance_blocked_while_compaction_computes() -> MidgeResult<()> {
    // Arrange
    let (mut el, worker, _) = cloud_debt(4)?;
    el.state.sequence = el.state.manifest.last_persisted_sequence;
    let (mut release, reached) = paused_compactor::install(&mut el)?;
    let plan = el
        .compaction_actor
        .check_manual_compaction(&el.state)?
        .expect("real cloud debt plan");
    let cf_id = plan.cf_id;
    el.launch_compaction(plan)?;
    reached
        .recv_timeout(TEST_WAIT)
        .expect("actual compute pause");
    el.state.set_compaction_enabled(false);
    acknowledge_put(&mut el, cf_id, 91_303)?;
    el.freeze_active_memtable(cf_id)?;
    let journal_before = journal_len(&el)?;

    // Act
    let turn = el.schedule_cloud_maintenance();
    let no_flush = !el.flush_actor.is_inflight();
    let no_prune = el.cloud_coordinator.cloud_wal_prune_worker.is_none();
    let journal_after = journal_len(&el)?;
    release.release();
    settle_owned_work(&mut el, &worker);

    // Assert
    assert!(turn.is_none());
    assert!(no_flush && no_prune);
    assert_eq!(
        journal_before, journal_after,
        "automatic debt must not allocate an overlap name"
    );
    assert!(el.flush_barrier_waiters.is_empty());
    Ok(())
}
