use super::*;
use crate::runtime::event_loop::compaction::CompactionCoordinator;

fn cloud_debt(
    files_per_cf: u64,
) -> crate::common::MidgeResult<(EventLoop, crossbeam::channel::Receiver<RuntimeMsg>, u32)> {
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    el.hybrid_storage
        .as_ref()
        .unwrap()
        .enable_ephemeral_sst_cache(64 * 1024 * 1024);
    let reads = Arc::new(crate::storage::remote_sst::RemoteSstFs::new(
        Arc::new(crate::io::RealFs::new(&el.state.db_path)?),
        el.hybrid_storage.as_ref().unwrap().remote_sst_backend(),
        Duration::from_secs(3),
    ));
    el.compaction_actor = crate::runtime::actors::CompactionActor::new(Arc::new(
        crate::sst::FsSstFactoryIo::new(reads, 64 * 1024)
            .with_compaction_scratch_directory(el.state.sst_dir.join(".flush-staging")),
    ));
    let other = el.state.create_cf("remaining-debt".into())?;
    seed_cloud_prune_candidate(&mut el, 81, 81);
    el.state.wal.cloud_durable_seq = 81;
    for cf_id in [0, other] {
        el.state
            .manifest
            .next_sst_seqs
            .insert(cf_id, files_per_cf + 1);
        for number in 1..=files_per_cf {
            let name = crate::sst::file_name(cf_id, 0, number);
            let bytes = add_valid_manifest_sst_for_test(&mut el, &name, 81);
            el.state.manifest.files.last_mut().unwrap().cf_id = cf_id;
            write_test_file(el.state.sst_dir.join(&name), &bytes);
        }
    }
    let (tx, rx) = crossbeam::channel::unbounded();
    el.worker_msg_tx = Some(tx);
    el.inline_flush_worker = false;
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
    for phase in ["build", "publish"] {
        let completion = el
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
    el.cloud_maintenance.next =
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
    assert!(!el.cloud_wal.acked_segments.contains_key(&81));
    el.schedule_next_flush_worker();
    assert!(el.flush_actor.is_inflight());
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
    let response = el.router.register(request_id, "CompactAll");

    // Act
    CompactionCoordinator::compact_all(&mut el, request_id);

    // Assert: explicit entry also respects the preferred flush turn.
    assert!(el.flush_actor.is_inflight());
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
    assert!(matches!(
        response.recv_timeout(Duration::from_secs(1)),
        Ok(RuntimeResponse::Ok { request_id: 91_001 })
    ));
    assert!(el.state.manifest.files.iter().all(|file| file.level > 0));
    assert!(el.state.pending_compaction_waits.lock().is_empty());
    Ok(())
}

#[test]
fn should_fail_queued_manual_waiter_when_compaction_cannot_launch_after_its_turn(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let (mut el, _, _) = cloud_debt(1)?;
    queue_generation_for_maintenance_test(&mut el, 82)?;
    let request_id = 91_002;
    let response = el.router.register(request_id, "CompactAll");
    CompactionCoordinator::compact_all(&mut el, request_id);

    // Act
    el.compaction_publication_degraded = true;
    complete_flush(&mut el);

    // Assert
    assert!(matches!(
        response.recv_timeout(Duration::from_secs(1)),
        Ok(RuntimeResponse::Error {
            error: crate::common::MidgeError::Fenced(_),
            ..
        })
    ));
    assert!(el.state.pending_compaction_waits.lock().is_empty());
    Ok(())
}

#[test]
fn should_abort_manual_obligation_when_ingest_interrupts_successful_worker_completion(
) -> crate::common::MidgeResult<()> {
    // Arrange: the result is ready before the ingest epoch changes, so the
    // successful worker cannot observe the later cancellation itself.
    let (mut el, worker, _) = cloud_debt(1)?;
    let manual = el.router.register(91_003, "CompactAll");
    CompactionCoordinator::compact_all(&mut el, 91_003);
    let completion = worker
        .recv_timeout(Duration::from_secs(3))
        .expect("successful worker result before ingest");
    assert!(matches!(
        &completion,
        RuntimeMsg::CompactionComplete {
            succeeded: true,
            ..
        }
    ));
    let ingest = el.router.register(91_004, "BeginIngest");

    // Act
    el.handle_begin_ingest(91_004);
    assert!(
        ingest.try_recv().is_err(),
        "the active worker must drain first"
    );
    let (_tx, rx) = crossbeam::channel::unbounded();
    el.handle_runtime_msg(completion, &rx);

    // Assert
    assert!(matches!(
        ingest.try_recv(),
        Ok(RuntimeResponse::Ok { request_id: 91_004 })
    ));
    assert!(matches!(
        manual.try_recv(),
        Ok(RuntimeResponse::Error {
            error: crate::common::MidgeError::Aborted(_),
            ..
        })
    ));
    assert_eq!(el.state.active_compactions.load(Ordering::Acquire), 0);
    assert!(el.state.pending_compaction_waits.lock().is_empty());
    assert!(el.state.manifest.files.iter().any(|file| file.level == 0));
    assert!(worker.try_recv().is_err());
    Ok(())
}

#[test]
fn should_abort_queued_manual_obligation_when_ingest_precedes_its_first_worker(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let (mut el, _, _) = cloud_debt(1)?;
    queue_generation_for_maintenance_test(&mut el, 82)?;
    let manual = el.router.register(91_005, "CompactAll");
    CompactionCoordinator::compact_all(&mut el, 91_005);
    assert!(el.flush_actor.is_inflight());
    let ingest = el.router.register(91_006, "BeginIngest");

    // Act
    el.handle_begin_ingest(91_006);

    // Assert
    assert!(matches!(
        ingest.try_recv(),
        Ok(RuntimeResponse::Ok { request_id: 91_006 })
    ));
    assert!(matches!(
        manual.try_recv(),
        Ok(RuntimeResponse::Error {
            error: crate::common::MidgeError::Aborted(_),
            ..
        })
    ));
    assert_eq!(el.state.active_compactions.load(Ordering::Acquire), 0);
    assert!(el.state.pending_compaction_waits.lock().is_empty());
    Ok(())
}

fn local_debt() -> crate::common::MidgeResult<(EventLoop, crossbeam::channel::Receiver<RuntimeMsg>)>
{
    let mut el = crate::runtime::event_loop::tests::create_test_local_event_loop()?;
    let other = el.state.create_cf("remaining-debt".into())?;
    for cf_id in [0, other] {
        let name = crate::sst::file_name(cf_id, 0, 1);
        let bytes = valid_sst_bytes_for_test(b"local-debt", b"value", 81);
        write_test_file(el.state.sst_dir.join(&name), &bytes);
        el.state.manifest.files.push(crate::metadata::FileMeta {
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
        el.state.manifest.next_sst_seqs.insert(cf_id, 2);
    }
    let (tx, rx) = crossbeam::channel::unbounded();
    el.worker_msg_tx = Some(tx);
    Ok((el, rx))
}

#[test]
fn should_leave_below_threshold_local_debt_when_no_manual_obligation_exists(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let (mut el, worker) = local_debt()?;
    el.state.set_compaction_enabled(true);
    let plan = el
        .compaction_actor
        .check_manual_compaction(&el.state)?
        .unwrap();
    el.launch_compaction(plan)?;

    // Act
    complete_compaction(&mut el, &worker);
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
}

#[test]
fn should_finish_requested_local_debt_when_background_compaction_is_disabled(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let (mut el, worker) = local_debt()?;
    el.state.set_compaction_enabled(false);
    let response = el.router.register(91_007, "CompactAll");

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
