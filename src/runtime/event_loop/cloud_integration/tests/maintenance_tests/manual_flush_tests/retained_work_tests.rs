use super::*;
use crate::runtime::actors::flush::{FlushActor, FlushWorkerResult};

fn canonical_identity(el: &EventLoop, flush_id: u64) -> (String, u64) {
    let (_, flush) = el.state.immutable_flush_by_id(flush_id).unwrap();
    (
        flush.sst_name.clone().expect("name reserved before build"),
        flush.sst_seq.expect("sequence reserved before build"),
    )
}

fn replace_flush_worker(el: &mut EventLoop, memory_bytes: usize) -> MidgeResult<()> {
    el.flush_actor.shutdown_and_join()?;
    let (tx, rx) = crossbeam::channel::unbounded();
    el.flush_actor = FlushActor::new_with_memory_limit(
        &el.state.sst_dir,
        false,
        crate::codec::CompressionPolicy::default(),
        tx,
        memory_bytes,
    )?;
    el.flush_worker_result_rx = rx;
    Ok(())
}

fn receive_build_after_owned_work(
    el: &mut EventLoop,
    response: &crossbeam::channel::Receiver<RuntimeResponse>,
) -> MidgeResult<FlushWorkerResult> {
    let (_, messages) = crossbeam::channel::unbounded();
    let deadline = Instant::now() + TEST_WAIT;
    loop {
        if let Ok(completion) = el.flush_worker_result_rx.try_recv() {
            return Ok(completion);
        }
        if let Ok(reply) = response.try_recv() {
            return Err(MidgeError::Internal(format!(
                "flush answered before actual Build admission: {reply:?}; {}",
                describe_settle_state(el)
            )));
        }
        el.tick_hybrid_storage();
        el.drain_hybrid_storage_events();
        el.reap_cloud_wal_prune_worker();
        if let Some(message) = el.pending_msg.take() {
            el.process_restored_one(message, &messages);
        }
        el.schedule_next_flush_worker();
        if Instant::now() >= deadline {
            return Err(MidgeError::Internal(format!(
                "actual Build admission did not finish: {}",
                describe_settle_state(el)
            )));
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn should_wait_for_owned_retirement_before_starting_manual_cloud_flush() -> MidgeResult<()> {
    // Arrange
    let (mut el, worker, _) = cloud_debt(1)?;
    el.state.sequence = el.state.manifest.last_persisted_sequence;
    el.state.set_compaction_enabled(false);
    let (_, mut release, reached) = controlled_storage::install(&mut el, true, false)?;
    let sequence = acknowledge_put(&mut el, 0, 91_321)?;
    el.cloud_coordinator.cloud_maintenance.next =
        crate::runtime::event_loop::cloud_maintenance::MaintenanceTask::WalRetirement;
    el.schedule_cloud_maintenance();
    reached
        .recv_timeout(TEST_WAIT)
        .expect("actual owned preflight WAL range");
    let journal_before = journal_len(&el)?;
    let cursor_before = el.state.sst_names.cursor.clone();

    // Act
    let response = submit_manual_flush(&mut el, 0, 91_322);
    let still_owned = el.cloud_coordinator.cloud_wal_prune_worker.is_some();
    let no_flush = !el.flush_actor.is_inflight();
    let accepted = el.state.get_cf(0).is_some_and(|cf| {
        cf.immutable_flushes.len() == 1
            && cf.immutable_flushes[0].sst_name.is_none()
            && cf.immutable_flushes[0].sst_seq.is_none()
    }) && el
        .flush_barrier_waiters
        .get(&0)
        .is_some_and(|waiters| waiters.iter().any(|waiter| waiter.request_id == 91_322));
    let prune_owner = el
        .publication_gate
        .is_owned_by(&crate::runtime::event_loop::coordination::ManifestPublicationOwner::WalPrune);
    let no_reply = response.try_recv().is_err();
    let unchanged_names = cursor_before == el.state.sst_names.cursor;
    let journal_after = journal_len(&el)?;
    release.release();
    settle_owned_work(&mut el, &worker);

    // Assert
    assert!(still_owned && no_flush && accepted && prune_owner && no_reply && unchanged_names);
    assert_eq!(journal_before, journal_after);
    assert!(matches!(
        response.recv_timeout(TEST_WAIT),
        Ok(RuntimeResponse::Ok { request_id: 91_322 })
    ));
    assert!(manifest_has_row(&el, 0, MANUAL_KEY, MANUAL_VALUE, sequence));
    assert_eq!(el.state.flush_metrics.publish_count, 1);
    Ok(())
}

#[test]
fn should_preserve_reserved_flush_identity_when_actual_build_retries() -> MidgeResult<()> {
    // Arrange
    let (mut el, worker, _) = cloud_debt(1)?;
    el.state.sequence = el.state.manifest.last_persisted_sequence;
    el.state.set_compaction_enabled(false);
    replace_flush_worker(&mut el, 1)?;
    let sequence = acknowledge_put(&mut el, 0, 91_331)?;
    let first_response = submit_manual_flush(&mut el, 0, 91_332);
    let flush_id = el.state.get_cf(0).unwrap().immutable_flushes[0].flush_id;
    let completion = receive_build_after_owned_work(&mut el, &first_response)?;
    let identity = canonical_identity(&el, flush_id);
    let cursor = el.state.sst_names.cursor.clone();
    let real_failure = matches!(&completion, FlushWorkerResult::Build(result) if matches!(result.result, Err(MidgeError::ResourceLimit(_))));

    // Act
    el.handle_flush_worker_result(completion);
    let retained_identity = canonical_identity(&el, flush_id);
    let original_error = first_response.recv_timeout(TEST_WAIT);
    replace_flush_worker(&mut el, 16 * 1024 * 1024)?;
    el.state.make_immutable_flush_retry_due(0);
    let retried = submit_manual_flush(&mut el, 0, 91_333);
    let identity_on_retry = canonical_identity(&el, flush_id);
    settle_owned_work(&mut el, &worker);

    // Assert
    assert!(real_failure);
    assert!(matches!(
        original_error,
        Ok(RuntimeResponse::Error {
            error: MidgeError::ResourceLimit(_),
            ..
        })
    ));
    assert_eq!(retained_identity, identity);
    assert_eq!(identity_on_retry, identity);
    assert_eq!(el.state.sst_names.cursor, cursor);
    assert_eq!(el.state.flush_metrics.enqueued_total, 1);
    assert_eq!(el.state.flush_metrics.build_count, 2);
    assert_eq!(el.state.flush_metrics.publish_count, 1);
    assert!(matches!(
        retried.recv_timeout(TEST_WAIT),
        Ok(RuntimeResponse::Ok { request_id: 91_333 })
    ));
    assert_eq!(
        el.state
            .manifest
            .files
            .iter()
            .filter(|file| file.name == identity.0)
            .count(),
        1
    );
    assert!(manifest_has_row(&el, 0, MANUAL_KEY, MANUAL_VALUE, sequence));
    Ok(())
}

#[test]
fn should_preserve_built_flush_identity_when_actual_upload_retries() -> MidgeResult<()> {
    // Arrange
    let (mut el, worker, _) = cloud_debt(1)?;
    el.state.sequence = el.state.manifest.last_persisted_sequence;
    el.state.set_compaction_enabled(false);
    let (cloud, _release, _) = controlled_storage::install(&mut el, false, true)?;
    let sequence = acknowledge_put(&mut el, 0, 91_341)?;
    let first_response = submit_manual_flush(&mut el, 0, 91_342);
    let flush_id = el.state.get_cf(0).unwrap().immutable_flushes[0].flush_id;
    let build = receive_build_after_owned_work(&mut el, &first_response)?;
    let identity = canonical_identity(&el, flush_id);
    let cursor = el.state.sst_names.cursor.clone();
    el.handle_flush_worker_result(build);
    let publish = el
        .flush_worker_result_rx
        .recv_timeout(TEST_WAIT)
        .expect("real failed Publish");
    let actual_timeout = matches!(&publish, FlushWorkerResult::Publish(result) if matches!(result.result, Err(MidgeError::Timeout(_))));

    // Act
    el.handle_flush_worker_result(publish);
    let retained = el
        .state
        .immutable_flush_by_id(flush_id)
        .is_some_and(|(_, flush)| flush.built.is_some());
    let retained_identity = canonical_identity(&el, flush_id);
    let original_error = first_response.recv_timeout(TEST_WAIT);
    el.state.make_immutable_flush_retry_due(0);
    let retried = submit_manual_flush(&mut el, 0, 91_343);
    let retry_identity = canonical_identity(&el, flush_id);
    settle_owned_work(&mut el, &worker);

    // Assert
    assert!(actual_timeout && retained);
    assert!(matches!(
        original_error,
        Ok(RuntimeResponse::Error {
            error: MidgeError::Timeout(_),
            ..
        })
    ));
    assert_eq!(retained_identity, identity);
    assert_eq!(retry_identity, identity);
    assert_eq!(el.state.sst_names.cursor, cursor);
    assert_eq!(el.state.flush_metrics.enqueued_total, 1);
    assert_eq!(el.state.flush_metrics.build_count, 1);
    assert_eq!(el.state.flush_metrics.publish_count, 2);
    assert!(matches!(
        retried.recv_timeout(TEST_WAIT),
        Ok(RuntimeResponse::Ok { request_id: 91_343 })
    ));
    assert_eq!(
        *cloud.uploads.lock().unwrap(),
        vec![crate::cloud_layout::object_key(&identity.0); 2]
    );
    assert_eq!(
        el.state
            .manifest
            .files
            .iter()
            .filter(|file| file.name == identity.0)
            .count(),
        1
    );
    assert!(manifest_has_row(&el, 0, MANUAL_KEY, MANUAL_VALUE, sequence));
    Ok(())
}
