use super::*;
use crate::lease::PrimaryLease as _;
use crate::runtime::actors::flush::{FlushActor, FlushWorkerResult};
use crate::storage::cloud::{CloudStorage, MockCloudBackend};
use std::path::Path;

struct HeldRetryClock(Instant);

impl crate::runtime::retry_schedule::RetryClock for HeldRetryClock {
    fn now(&self) -> Instant {
        self.0
    }
}

fn replace_flush_worker(el: &mut EventLoop, memory_bytes: usize) -> MidgeResult<()> {
    el.actors.flush_actor.shutdown_and_join()?;
    let (tx, rx) = crossbeam::channel::unbounded();
    el.actors.flush_actor = FlushActor::new_with_memory_limit(
        Arc::clone(&el.state.fs),
        false,
        crate::codec::CompressionPolicy::default(),
        tx,
        memory_bytes,
    )?;
    el.pipeline.flush_worker_result_rx = rx;
    Ok(())
}

fn canonical_identity(el: &EventLoop, flush_id: u64) -> (String, u64) {
    let (_, flush) = el.state.immutable_flush_by_id(flush_id).unwrap();
    (
        flush
            .sst_name
            .clone()
            .expect("identity before actual build"),
        flush.sst_seq.expect("sequence before actual build"),
    )
}

fn receive_failed_build(el: &mut EventLoop) -> bool {
    let FlushWorkerResult::Build(failed) = el
        .pipeline
        .flush_worker_result_rx
        .recv_timeout(TEST_WAIT)
        .expect("actual failed Build")
    else {
        panic!("expected the actual automatic Build result");
    };
    let failed_with_charge =
        matches!(&failed.result, Err(MidgeError::ResourceLimit(_))) && failed.reservation.is_some();
    el.handle_flush_worker_result(FlushWorkerResult::Build(failed));
    failed_with_charge
}

fn retain_retry_until_due(
    el: &mut EventLoop,
    cf_id: u32,
    flush: &crate::runtime::state::ImmutableFlush,
) -> bool {
    let retained = el
        .state
        .immutable_flush_by_id_mut(flush.flush_id)
        .unwrap()
        .1;
    retained.retry.defer_for(Duration::from_secs(60));
    let retained_immutable = Arc::ptr_eq(&retained.memtable, &flush.memtable)
        && retained.phase == crate::runtime::state::ImmutableFlushPhase::RetryPending
        && retained.failures == 1
        && retained.built.is_none();
    retained_immutable
        && el
            .state
            .get_cf(cf_id)
            .unwrap()
            .immutable_memtables
            .iter()
            .any(|memtable| Arc::ptr_eq(memtable, &flush.memtable))
}

fn assert_compute_owner(el: &EventLoop, inputs: &[String], reserved_bytes: u64) {
    let budget = el
        .actors
        .cloud_coordinator
        .hybrid_storage
        .as_ref()
        .unwrap()
        .budget_snapshot();
    assert_eq!(el.state.active_compactions.load(Ordering::Acquire), 1);
    assert_eq!(
        budget.usage.compaction_staging_reserved_bytes,
        reserved_bytes
    );
    assert!(reserved_bytes > 0);
    assert_eq!(budget.usage.reservations, 1);
    assert_eq!(budget.usage.flush_staging_reserved_bytes, 0);
    assert!(inputs.iter().all(|name| {
        el.state.compaction.compacting_ssts.contains(name)
            && remote_sst_path_for_test(el, name).exists()
    }));
    assert!(el
        .actors
        .compaction_actor
        .captured_input_metadata()
        .iter()
        .all(|captured| {
            el.state
                .manifest
                .files
                .iter()
                .any(|file| file.same_identity(captured))
        }));
    assert!(el.publication.flush_barrier_waiters.is_empty());
    assert!(el.actors.cloud_coordinator.cloud_wal_prune_worker.is_none());
}

fn assert_committed_retry_row(
    el: &EventLoop,
    cloud: &CloudStorage,
    lease: &crate::lease::CloudStorageLease,
    name: &str,
    sequence: u64,
) -> MidgeResult<()> {
    let directory = tempfile::tempdir()?;
    let store = lease
        .get_leader_store()
        .expect("actual conditional metadata authority");
    crate::runtime::cloud_startup::CloudStartupRecovery::hydrate_cloud_metadata(
        cloud,
        store.as_ref(),
        &crate::io::real::test_root(directory.path()),
        directory.path(),
        crate::config::RecoveryPolicy::Strict,
    )?;
    let committed = crate::metadata::ManifestPersistence::load(directory.path())
        .map_err(MidgeError::Internal)?;
    let file = committed
        .files
        .iter()
        .find(|file| file.name == name)
        .expect("committed automatic flush output");
    let local = el
        .state
        .manifest
        .files
        .iter()
        .find(|file| file.name == name)
        .unwrap();
    assert!(file.same_identity(local));
    let reader = el
        .actors
        .compaction_actor
        .open_sst_reader(&Path::new("sst").join(&file.name))?;
    assert!(matches!(reader.get_state_at(MANUAL_KEY, u64::MAX)?,
        KeyState::Value(value, actual, None, EntryType::Put)
            if value.as_ref() == MANUAL_VALUE && actual == sequence));
    assert!(test_flush_frontier(&committed, file.cf_id) >= sequence);
    Ok(())
}

#[test]
fn should_publish_automatic_retry_without_releasing_cloud_compaction_ownership() -> MidgeResult<()>
{
    // Arrange: the existing real worker budget failure must retain accepted data.
    let (mut el, worker, _) = cloud_debt(4)?;
    el.state.sequence = test_flush_frontier(&el.state.manifest, 0);
    let cloud = Arc::new(CloudStorage::new_with_timeout(
        Arc::new(MockCloudBackend::new()),
        String::new(),
        TEST_WAIT,
    ));
    let lease = attach_provider_metadata_lease(&mut el, Arc::clone(&cloud), "automatic-retry");
    let (mut release, reached) = paused_compactor::install(&mut el);
    let plan = el
        .actors
        .compaction_actor
        .check_manual_compaction(&el.state)?
        .expect("real cloud plan");
    let cf_id = plan.cf_id;
    let inputs = plan.input_files.clone();
    el.launch_compaction(plan)?;
    reached
        .recv_timeout(TEST_WAIT)
        .expect("actual compute pause");
    el.state.set_compaction_enabled(false);
    let reserved = el
        .actors
        .cloud_coordinator
        .hybrid_storage
        .as_ref()
        .unwrap()
        .budget_snapshot()
        .usage
        .compaction_staging_reserved_bytes;
    replace_flush_worker(&mut el, 1)?;
    let sequence = acknowledge_put(&mut el, cf_id, 91_431)?;
    el.freeze_active_memtable(cf_id)?;
    let flush = el.state.get_cf(cf_id).unwrap().immutable_flushes[0].clone();
    el.state
        .immutable_flush_by_id_mut(flush.flush_id)
        .unwrap()
        .1
        .retry = crate::runtime::retry_schedule::RetrySchedule::with_clock(
        Duration::from_secs(60),
        Arc::new(HeldRetryClock(Instant::now())),
    );

    // Act: dispatch the genuine failed Build, then retry through ordinary maintenance.
    el.schedule_next_flush_worker();
    let identity = canonical_identity(&el, flush.flush_id);
    let cursor = el.state.sst_names.cursor.clone();
    let failed_with_charge = receive_failed_build(&mut el);
    let retained_memory = retain_retry_until_due(&mut el, cf_id, &flush);
    let unpublished = el.state.flush_metrics.publish_count == 0
        && !el
            .state
            .manifest
            .files
            .iter()
            .any(|file| file.name == identity.0);
    let retry_waited =
        el.schedule_cloud_maintenance().is_none() && !el.actors.flush_actor.is_inflight();
    assert_compute_owner(&el, &inputs, reserved);
    assert_eq!(canonical_identity(&el, flush.flush_id), identity);
    assert_eq!(flush.memtable.get(MANUAL_KEY)?, Some(MANUAL_VALUE.to_vec()));
    replace_flush_worker(&mut el, 16 * 1024 * 1024)?;
    el.state.make_immutable_flush_retry_due(cf_id);
    el.schedule_next_flush_worker();
    let retried = flush_while_compute_is_held(&mut el);
    let released_immutable = el.state.immutable_flush_by_id(flush.flush_id).is_none();
    assert_compute_owner(&el, &inputs, reserved);
    let committed = assert_committed_retry_row(&el, &cloud, &lease, &identity.0, sequence);
    release.release();
    settle_owned_work(&mut el, &worker);

    // Assert: verified failed-build cleanup releases only its own empty-output token.
    assert!(failed_with_charge && retained_memory && unpublished && retry_waited);
    assert!(
        retried.is_ok(),
        "automatic retry before compute release: {retried:?}"
    );
    assert!(released_immutable);
    committed?;
    assert_eq!(el.state.sst_names.cursor, cursor);
    assert_eq!(el.state.flush_metrics.enqueued_total, 1);
    assert_eq!(el.state.flush_metrics.failures_total, 1);
    assert_eq!(el.state.flush_metrics.build_count, 2);
    assert_eq!(el.state.flush_metrics.publish_count, 1);
    assert_acknowledged_and_recovered_rows(&el, cf_id, sequence);
    Ok(())
}
