use super::*;
use crate::io::FsError;
use crate::runtime::event_loop::compaction::CompactionCoordinator;

mod compaction_notification_tests;
mod manual_flush_tests;
mod quantum_tests;
mod wake_tests;

fn cloud_debt(
    files_per_cf: u64,
) -> crate::common::MidgeResult<(EventLoop, crossbeam::channel::Receiver<RuntimeMsg>, u32)> {
    cloud_debt_with_wal_records(files_per_cf, 1)
}

/// The last sequence of the WAL debt `cloud_debt_with_wal_records` seeds
/// with `records` records.
fn wal_debt_last_sequence(records: usize) -> u64 {
    80 + u64::try_from(records).expect("record count fits u64")
}

fn cloud_debt_with_wal_records(
    files_per_cf: u64,
    wal_record_count: usize,
) -> crate::common::MidgeResult<(EventLoop, crossbeam::channel::Receiver<RuntimeMsg>, u32)> {
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    el.actors
        .cloud_coordinator
        .hybrid_storage
        .as_ref()
        .unwrap()
        .enable_ephemeral_sst_cache(64 * 1024 * 1024);
    let reads = Arc::new(crate::storage::remote_sst::RemoteSstFs::new(
        Arc::clone(&el.state.fs),
        el.actors
            .cloud_coordinator
            .hybrid_storage
            .as_ref()
            .unwrap()
            .remote_sst_backend(),
        Duration::from_secs(3),
    ));
    el.actors.compaction_actor = crate::runtime::actors::CompactionActor::new(Arc::new(
        crate::runtime::event_loop::resources::compaction_sst_factory(
            reads,
            crate::codec::CompressionPolicy::default(),
        ),
    ));
    let other = el.state.create_cf("remaining-debt".into())?;
    // Sequences strictly increase within a segment (#843): the records are
    // versions 81.. of one key, and one more SST holds every version past
    // 81, so each record stays exactly covered.
    let last = wal_debt_last_sequence(wal_record_count);
    let records = (81..=last)
        .map(|sequence| {
            crate::wal::WalRecord::new(
                crate::wal::WalOpKind::Put,
                Bytes::from_static(b"prune-candidate"),
                Some(Bytes::from_static(b"value")),
                sequence,
            )
        })
        .collect();
    seed_cloud_prune_candidate_with_records(&mut el, 81, last, records);
    el.state.wal.frontiers.set_cloud_durable_for_test(last);
    if last > 81 {
        add_valid_versions_manifest_sst_for_test(&mut el, "wal-debt-versions.sst", 82..=last);
    }
    for cf_id in [0, other] {
        el.state
            .manifest
            .test_mut()
            .next_sst_seqs
            .insert(cf_id, files_per_cf + 1);
        for number in 1..=files_per_cf {
            let name = crate::cloud_layout::file_name(cf_id, 0, number);
            let bytes = add_valid_manifest_sst_for_test(&mut el, &name, 81);
            el.state.manifest.test_mut().files.last_mut().unwrap().cf_id = cf_id;
            write_test_file(el.state.host_sst_dir().join(&name), &bytes);
        }
    }
    publish_test_flush_frontier(&mut el, 0, last);
    // Seed setup reserves both families through real journal/mirror work,
    // before tests measure their accepted asynchronous compute/publication.
    for _ in [0, other] {
        el.prefetch_flush_sst_names_if_due();
    }
    let (tx, rx) = crossbeam::channel::unbounded();
    el.pipeline.worker_msg_tx = Some(tx);
    el.pipeline.inline_flush_worker = false;
    Ok((el, rx, other))
}

fn complete_compaction(el: &mut EventLoop, worker: &crossbeam::channel::Receiver<RuntimeMsg>) {
    let completion = worker
        .recv_timeout(Duration::from_secs(3))
        .expect("compaction completion");
    let (_tx, rx) = crossbeam::channel::unbounded();
    el.handle_runtime_msg(completion, &rx);
}

fn complete_flush(el: &mut EventLoop) {
    for phase in ["build", "publish", "manifest mirror", "intent clear"] {
        let completion = el
            .pipeline
            .flush_worker_result_rx
            .recv_timeout(Duration::from_secs(3))
            .unwrap_or_else(|error| panic!("flush {phase}: {error}"));
        el.handle_flush_worker_result(completion);
    }
}

#[test]
fn should_resume_due_flush_when_compaction_debt_remains_in_another_family(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let (mut el, worker, other) = cloud_debt(4)?;
    el.state.set_compaction_enabled(true);
    queue_generation_for_maintenance_test(&mut el, 82)?;
    let flush_id = el.state.get_cf(0).unwrap().immutable_flushes[0].flush_id;
    el.state.mark_immutable_flush_failed(flush_id).unwrap();
    el.state.make_immutable_flush_retry_due(0);
    el.actors.cloud_coordinator.cloud_maintenance.next =
        crate::runtime::event_loop::cloud_maintenance::MaintenanceTask::Compaction;

    // Act
    el.schedule_next_flush_worker();
    complete_compaction(&mut el, &worker);

    // Assert: another eligible compaction cannot take the retirement/flush turns.
    assert_eq!(el.state.active_compactions.load(Ordering::Acquire), 0);
    assert_eq!(
        el.state
            .manifest
            .files
            .iter()
            .filter(|file| file.cf_id == other && file.level == 0)
            .count(),
        4
    );
    drain_prune_completion_for_test(&mut el);
    assert!(!el.actors.cloud_coordinator.cloud_wal.acks().contains(81));
    el.schedule_next_flush_worker();
    assert!(el.actors.flush_actor.is_inflight());
    complete_flush(&mut el);
    assert!(el.state.get_cf(0).unwrap().immutable_memtables.is_empty());
    assert_eq!(el.state.flush_metrics.publish_count, 1);
    assert_eq!(el.state.flush_metrics.failures_total, 1);
    Ok(())
}

#[test]
fn should_keep_compact_all_pending_while_fair_turns_service_other_work(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let (mut el, worker, _) = cloud_debt(1)?;
    el.state.set_compaction_enabled(false);
    queue_generation_for_maintenance_test(&mut el, 82)?;
    let request_id = 91_001;
    let response = el.routing.router.register(request_id, "CompactAll");

    // Act
    CompactionCoordinator::compact_all(&mut el, request_id);

    // Assert: explicit entry also respects the preferred flush turn.
    assert!(el.actors.flush_actor.is_inflight());
    assert_eq!(el.state.active_compactions.load(Ordering::Acquire), 0);
    assert!(response.try_recv().is_err());
    complete_flush(&mut el);
    complete_compaction(&mut el, &worker);
    let premature = response.try_recv();
    assert!(
        premature.is_err(),
        "another CF still has manual debt: {premature:?}"
    );
    drain_prune_completion_for_test(&mut el);
    el.schedule_next_flush_worker();
    complete_compaction(&mut el, &worker);
    drain_prune_completion_for_test(&mut el);
    el.schedule_next_flush_worker();
    manual_flush_tests::settle_owned_work(&mut el, &worker);
    let outcome = response.recv_timeout(Duration::from_secs(1));
    assert!(
        matches!(outcome, Ok(RuntimeResponse::Ok { request_id: 91_001 })),
        "{outcome:?}"
    );
    assert!(el.state.manifest.files.iter().all(|file| file.level > 0));
    assert!(el.state.pending_compaction_waits.is_empty());
    Ok(())
}

#[test]
fn should_fail_queued_manual_waiter_when_compaction_cannot_launch_after_its_turn(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let (mut el, _, _) = cloud_debt(1)?;
    queue_generation_for_maintenance_test(&mut el, 82)?;
    let request_id = 91_002;
    let response = el.routing.router.register(request_id, "CompactAll");
    CompactionCoordinator::compact_all(&mut el, request_id);

    // Act
    el.authority.compaction_fence.degrade();
    complete_flush(&mut el);

    // Assert
    assert!(matches!(
        response.recv_timeout(Duration::from_secs(1)),
        Ok(RuntimeResponse::Error {
            error: crate::common::MidgeError::Fenced(_),
            ..
        })
    ));
    assert!(el.state.pending_compaction_waits.is_empty());
    Ok(())
}

fn local_debt() -> crate::common::MidgeResult<(EventLoop, crossbeam::channel::Receiver<RuntimeMsg>)>
{
    let mut el = crate::runtime::event_loop::tests::create_test_local_event_loop()?;
    let other = el.state.create_cf("remaining-debt".into())?;
    for cf_id in [0, other] {
        let name = crate::cloud_layout::file_name(cf_id, 0, 1);
        let bytes = valid_sst_bytes_for_test(b"local-debt", b"value", 81);
        write_test_file(el.state.host_sst_dir().join(&name), &bytes);
        el.state
            .manifest
            .test_mut()
            .files
            .push(crate::metadata::FileMeta {
                name,
                cf_id,
                level: 0,
                size_bytes: bytes.len() as u64,
                content_crc32c: Some(crc32c::crc32c(&bytes)),
                smallest_key: Some(b"local-debt".to_vec()),
                largest_key: Some(b"local-debt".to_vec()),
                smallest_seq: Some(81),
                largest_seq: Some(81),
                key_bounds_complete: true,
                ..Default::default()
            });
        el.state.manifest.test_mut().next_sst_seqs.insert(cf_id, 2);
    }
    let (tx, rx) = crossbeam::channel::unbounded();
    el.pipeline.worker_msg_tx = Some(tx);
    Ok((el, rx))
}

#[test]
fn should_leave_below_threshold_local_debt_when_no_manual_obligation_exists(
) -> crate::common::MidgeResult<()> {
    crate::failpoints::with_read_gate(|| {
        // Arrange
        let (mut el, worker) = local_debt()?;
        el.state.set_compaction_enabled(true);
        let plan = el
            .actors
            .compaction_actor
            .check_manual_compaction(&el.state)?
            .unwrap();
        el.launch_compaction(plan)?;

        // Act
        complete_compaction(&mut el, &worker);
        manual_flush_tests::settle_owned_work(&mut el, &worker);
        el.schedule_one_background_compaction_if_needed("ordinary policy regression")?;

        // Assert
        assert_eq!(el.state.active_compactions.load(Ordering::Acquire), 0);
        assert_eq!(
            el.state
                .manifest
                .files
                .iter()
                .filter(|file| file.level == 0)
                .count(),
            1
        );
        assert!(worker.try_recv().is_err());
        Ok(())
    })
}

#[test]
fn should_finish_requested_local_debt_when_background_compaction_is_disabled(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let (mut el, worker) = local_debt()?;
    el.state.set_compaction_enabled(false);
    let response = el.routing.router.register(91_007, "CompactAll");

    // Act
    CompactionCoordinator::compact_all(&mut el, 91_007);
    complete_compaction(&mut el, &worker);
    assert!(
        response.try_recv().is_err(),
        "the other CF still needs compaction"
    );
    complete_compaction(&mut el, &worker);

    // Assert
    assert!(matches!(
        response.try_recv(),
        Ok(RuntimeResponse::Ok { request_id: 91_007 })
    ));
    assert!(el.state.manifest.files.iter().all(|file| file.level > 0));
    assert_eq!(el.state.active_compactions.load(Ordering::Acquire), 0);
    Ok(())
}
