use super::*;
use crate::storage::{
    MetadataReadCallback, RangeReadCallback, StorageBackend, StorageCallback, StorageObjectMetadata,
};

struct SlowWalRanges {
    inner: Arc<crate::storage::filesystem::FileSystem>,
    calls: Arc<AtomicUsize>,
}

impl StorageBackend for SlowWalRanges {
    fn submit_read_range(
        &self,
        key: &str,
        start: u64,
        end: u64,
        expected: StorageObjectMetadata,
        timeout: Duration,
        callback: RangeReadCallback,
    ) {
        if key.starts_with("wal/") {
            let inner = self.inner.clone();
            let key = key.to_string();
            let calls = self.calls.clone();
            std::thread::spawn(move || {
                // One successful provider request exceeds the cooperative
                // quantum but fits its unchanged three-second hard deadline.
                std::thread::sleep(Duration::from_millis(250));
                calls.fetch_add(1, Ordering::AcqRel);
                inner.submit_read_range(&key, start, end, expected, timeout, callback);
            });
        } else {
            self.inner
                .submit_read_range(key, start, end, expected, timeout, callback);
        }
    }

    fn submit_range_head(&self, key: &str, timeout: Duration, callback: StorageCallback) {
        self.inner.submit_range_head(key, timeout, callback);
    }

    fn submit_read_with_metadata(
        &self,
        key: &str,
        timeout: Duration,
        callback: MetadataReadCallback,
    ) {
        self.inner.submit_read_with_metadata(key, timeout, callback);
    }

    fn submit_read(&self, key: &str, callback: StorageCallback) {
        self.inner.submit_read(key, callback);
    }

    fn submit_write(&self, key: &str, data: Vec<u8>, callback: StorageCallback) {
        self.inner.submit_write(key, data, callback);
    }

    fn submit_write_with_headers(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: StorageCallback,
    ) {
        self.inner
            .submit_write_with_headers(key, data, headers, callback);
    }

    fn submit_delete(&self, key: &str, callback: StorageCallback) {
        self.inner.submit_delete(key, callback);
    }

    fn submit_delete_with_headers(
        &self,
        key: &str,
        headers: Vec<(String, String)>,
        callback: StorageCallback,
    ) {
        self.inner
            .submit_delete_with_headers(key, headers, callback);
    }

    fn submit_list(&self, prefix: &str, callback: StorageCallback) {
        self.inner.submit_list(prefix, callback);
    }

    fn submit_head(&self, key: &str, callback: StorageCallback) {
        self.inner.submit_head(key, callback);
    }
}

#[test]
fn should_release_shared_maintenance_turn_when_retirement_proof_outlasts_its_quantum(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let (mut el, worker, _) = cloud_debt_with_wal_records(4, 30_000)?;
    let cloud = Arc::new(SlowWalRanges {
        inner: Arc::new(crate::storage::filesystem::FileSystem::new(
            el.state.db_path.join("cloud_store"),
        )?),
        calls: Arc::new(AtomicUsize::new(0)),
    });
    let local = Arc::new(crate::storage::filesystem::FileSystem::new(
        el.state.db_path.join("hybrid_local"),
    )?);
    let hybrid = Arc::new(crate::storage::HybridStorage::with_policy(
        local,
        cloud.clone(),
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    ));
    hybrid.enable_ephemeral_sst_cache(64 * 1024 * 1024);
    el.hybrid_storage = Some(hybrid);
    el.runtime_response_timeout = Duration::from_secs(3);
    el.shutdown_cloud_drain_timeout = Duration::from_secs(3);
    el.compaction_actor
        .set_execution_limits(1024 * 1024, 1024 * 1024);
    el.state.set_compaction_enabled(true);
    queue_generation_for_maintenance_test(&mut el, 82)?;
    let flush_id = el.state.get_cf(0).unwrap().immutable_flushes[0].flush_id;
    el.state.mark_immutable_flush_failed(flush_id).unwrap();
    el.state.make_immutable_flush_retry_due(0);
    el.cloud_maintenance.next =
        crate::runtime::event_loop::cloud_maintenance::MaintenanceTask::WalRetirement;

    // Act: the proof has far more ranges than fit a turn. Observe actual
    // worker completion, rather than checking the configured duration alone.
    let started = Instant::now();
    el.schedule_next_flush_worker();
    assert!(el.cloud_wal_prune_worker.is_some());
    assert!(matches!(
        el.available_compaction_memory(),
        Err(crate::common::MidgeError::Busy(_))
    ));
    while el.cloud_wal_prune_worker.is_some() && started.elapsed() < Duration::from_millis(1500) {
        el.tick_hybrid_storage();
        std::thread::sleep(Duration::from_millis(2));
    }

    // Assert: successful slow reads count as progress, then due work gets a
    // turn while the unfinished WAL proof retains its recovery authority.
    assert!(
        el.cloud_wal_prune_worker.is_none(),
        "retirement held the shared turn for {:?}",
        started.elapsed()
    );
    assert!(cloud.calls.load(Ordering::Acquire) > 0);
    assert!(el.cloud_wal.acked_segments.contains_key(&81));
    assert!(remote_wal_path_for_test(&el, 81).exists());
    assert_compaction_shares_retained_budget(&mut el)?;
    el.schedule_next_flush_worker();
    assert!(el.flush_actor.is_inflight());
    complete_flush(&mut el);
    assert!(el.state.get_cf(0).unwrap().immutable_memtables.is_empty());
    assert_eq!(el.state.flush_metrics.publish_count, 1);
    assert_eq!(el.state.active_compactions.load(Ordering::Acquire), 1);
    complete_compaction(&mut el, &worker);
    assert!(el.state.manifest.files.iter().any(|file| file.level > 0));
    assert!(el.cloud_wal.acked_segments.contains_key(&81));
    Ok(())
}

fn assert_compaction_shares_retained_budget(el: &mut EventLoop) -> crate::common::MidgeResult<()> {
    let retained = el.cloud_wal_prune_progress.retained_bytes().unwrap();
    assert!(retained > 0, "the yielded proof must keep resumable state");
    let configured = el.compaction_actor.compaction_memory_limit();
    let target = el.compaction_actor.target_sst_size();
    let plan = el.compaction_actor.check_compaction(&el.state)?.unwrap();
    let prepared = el.prepare_compaction_plan_for_launch(plan)?;
    assert_eq!(
        prepared.compaction_memory_limit + retained,
        configured,
        "paused proof and compaction execution share one configured allowance"
    );
    el.compaction_actor
        .set_execution_limits(target, retained - 1);
    let input_count = el.state.manifest.files.len();
    assert!(matches!(
        el.prepare_compaction_plan_for_launch(prepared),
        Err(crate::common::MidgeError::ResourceLimit(_))
    ));
    assert_eq!(el.state.manifest.files.len(), input_count);
    el.compaction_actor.set_execution_limits(target, configured);
    Ok(())
}

#[test]
fn should_preserve_full_compaction_allowance_when_no_proof_state_is_retained(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let (mut el, _) = local_debt()?;
    let plan = el
        .compaction_actor
        .check_manual_compaction(&el.state)?
        .unwrap();

    // Act
    let prepared = el.prepare_compaction_plan_for_launch(plan)?;

    // Assert
    assert_eq!(el.cloud_wal_prune_progress.retained_bytes(), Some(0));
    assert_eq!(
        prepared.compaction_memory_limit,
        el.compaction_actor.compaction_memory_limit()
    );
    Ok(())
}
