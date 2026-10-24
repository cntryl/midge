use super::*;

#[test]
fn should_publish_accepted_immutable_without_manual_waiter_while_cloud_compaction_waits(
) -> MidgeResult<()> {
    // Arrange: a genuine compactor retains its input set and staging reservation.
    let (mut el, worker, _) = cloud_debt(4)?;
    // The retirement fixture advances the WAL ledger while seeding remote debt.
    // Align its initially empty durability generation before issuing live writes.
    assert!(el
        .pipeline
        .durability
        .drain_all_waiters_and_reset(el.state.wal.open_segment())
        .is_empty());
    el.state.limits.max_immutable_memtables = 1;
    el.state.sequence = test_flush_frontier(&el.state.manifest, 0);
    let (mut release, reached) = paused_compactor::install(&mut el);
    let plan = el
        .actors
        .compaction_actor
        .check_manual_compaction(&el.state)?
        .expect("real cloud debt plan");
    let cf_id = plan.cf_id;
    let inputs = plan.input_files.clone();
    el.launch_compaction(plan)?;
    let deadline = Instant::now() + TEST_WAIT;
    while reached.try_recv().is_err() {
        el.reap_metadata_mirror();
        assert!(Instant::now() < deadline, "actual compute pause");
        std::thread::sleep(Duration::from_millis(1));
    }
    el.state.set_compaction_enabled(false);
    let sequence = acknowledge_put(&mut el, cf_id, 91_401)?;
    el.freeze_active_memtable(cf_id)?;
    let flush_id = el.state.get_cf(cf_id).unwrap().immutable_flushes[0].flush_id;
    assert!(el.publication.flush_barrier_waiters.is_empty());
    let admission = el.routing.router.register(91_402, "WaitForWriteStallClear");
    let (_, messages) = crossbeam::channel::unbounded();
    el.handle_runtime_msg(
        RuntimeMsg::WaitForWriteStallClear {
            request_id: 91_402,
            cf_id,
        },
        &messages,
    );
    let initially_stalled = admission.try_recv().is_err();

    // Act: ordinary maintenance must release accepted memory before compute ends.
    el.schedule_next_flush_worker();
    let flushed = flush_while_compute_is_held(&mut el);
    let states_before_release = manifest_row_states(&el, cf_id, MANUAL_KEY);
    let row_before_release = row_matches(&states_before_release, MANUAL_VALUE, sequence);
    let immutable_released = el
        .state
        .get_cf(cf_id)
        .unwrap()
        .immutable_memtables
        .is_empty();
    let compactor_owned = el.state.active_compactions.load(Ordering::Acquire) == 1;
    let inputs_retained = inputs
        .iter()
        .all(|name| remote_sst_path_for_test(&el, name).exists());
    let prune_not_started = el.actors.cloud_coordinator.cloud_wal_prune_worker.is_none();
    let no_manual_waiter = el.publication.flush_barrier_waiters.is_empty();
    let admission_released = admission.try_recv();
    let resumed_write = acknowledge_put(&mut el, cf_id, 91_403);
    let resumed_publication = if let Ok(resumed_sequence) = resumed_write.as_ref() {
        el.freeze_active_memtable(cf_id)?;
        el.schedule_next_flush_worker();
        let outcome = flush_while_compute_is_held(&mut el);
        let row = manifest_has_row(&el, cf_id, MANUAL_KEY, MANUAL_VALUE, *resumed_sequence);
        Some((outcome, row))
    } else {
        None
    };
    release.release();
    settle_owned_work(&mut el, &worker);

    // Assert: exact data and input ownership survive both actual publications.
    assert!(initially_stalled);
    assert!(matches!(
        admission_released,
        Ok(RuntimeResponse::Ok { request_id: 91_402 })
    ));
    assert!(
        flushed.is_ok(),
        "automatic flush before compute release: {flushed:?}"
    );
    assert!(
        row_before_release,
        "exact accepted row before release: {states_before_release:?}"
    );
    assert!(immutable_released && compactor_owned && inputs_retained && prune_not_started);
    assert!(no_manual_waiter);
    assert!(
        resumed_write.is_ok(),
        "transaction ACK before compute release: {resumed_write:?}"
    );
    assert!(matches!(resumed_publication, Some((Ok(()), true))));
    assert!(el.state.immutable_flush_by_id(flush_id).is_none());
    assert_acknowledged_and_recovered_rows(&el, cf_id, sequence);
    assert!(manifest_has_row(
        &el,
        cf_id,
        MANUAL_KEY,
        MANUAL_VALUE,
        resumed_write?
    ));
    assert_eq!(el.state.flush_metrics.publish_count, 2);
    Ok(())
}
