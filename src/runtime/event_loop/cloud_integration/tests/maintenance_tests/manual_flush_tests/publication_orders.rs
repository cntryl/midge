use super::*;
use crate::lease::PrimaryLease as _;
use crate::runtime::actors::compaction::publication::CompactionPublishActor;
use crate::runtime::actors::flush::FlushWorkerResult;
use crate::runtime::event_loop::coordination::ManifestPublicationOwner;
use crate::storage::cloud::{CloudBackend, CloudCallback, CloudStorage, MockCloudBackend};

struct PublisherReadGate {
    reached: crossbeam::channel::Sender<()>,
    release: crossbeam::channel::Receiver<()>,
}

struct PublisherBackend {
    inner: MockCloudBackend,
    gate: Mutex<Option<PublisherReadGate>>,
    armed: AtomicBool,
    puts: AtomicUsize,
}

impl PublisherBackend {
    fn pause_publisher_read(&self) {
        if !self.armed.load(Ordering::Acquire)
            || std::thread::current().name() != Some("midge-compaction-publish")
        {
            return;
        }
        let gate = self
            .gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(gate) = gate {
            let _ = gate.reached.send(());
            let _ = gate.release.recv();
        }
    }
}

impl CloudBackend for PublisherBackend {
    fn submit_get(&self, key: &str, callback: CloudCallback) {
        self.pause_publisher_read();
        self.inner.submit_get(key, callback);
    }

    fn submit_get_with_metadata(&self, key: &str, callback: CloudCallback) {
        self.pause_publisher_read();
        self.inner.submit_get_with_metadata(key, callback);
    }

    fn submit_get_range(&self, key: &str, start: u64, end: Option<u64>, callback: CloudCallback) {
        self.inner.submit_get_range(key, start, end, callback);
    }

    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        self.puts.fetch_add(1, Ordering::AcqRel);
        self.inner.submit_put(key, data, headers, callback);
    }

    fn submit_delete(&self, key: &str, headers: Vec<(String, String)>, callback: CloudCallback) {
        self.inner.submit_delete(key, headers, callback);
    }

    fn submit_list(&self, prefix: &str, callback: CloudCallback) {
        self.inner.submit_list(prefix, callback);
    }

    fn submit_head(&self, key: &str, callback: CloudCallback) {
        self.inner.submit_head(key, callback);
    }
}

struct PublisherRelease(Option<crossbeam::channel::Sender<()>>);

impl PublisherRelease {
    fn release(&mut self) {
        if let Some(release) = self.0.take() {
            let _ = release.send(());
        }
    }
}

impl Drop for PublisherRelease {
    fn drop(&mut self) {
        self.release();
    }
}

struct MetadataFixture {
    cloud: Arc<CloudStorage>,
    backend: Arc<PublisherBackend>,
    lease: Arc<crate::lease::CloudStorageLease>,
    release: PublisherRelease,
    reached: crossbeam::channel::Receiver<()>,
}

fn metadata_fixture(el: &mut EventLoop) -> MidgeResult<MetadataFixture> {
    let (reached, arrived) = crossbeam::channel::bounded(1);
    let (release, resumed) = crossbeam::channel::bounded(1);
    let backend = Arc::new(PublisherBackend {
        inner: MockCloudBackend::new(),
        gate: Mutex::new(Some(PublisherReadGate {
            reached,
            release: resumed,
        })),
        armed: AtomicBool::new(false),
        puts: AtomicUsize::new(0),
    });
    let cloud = Arc::new(CloudStorage::new_with_timeout(
        backend.clone(),
        String::new(),
        TEST_WAIT,
    ));
    let lease = attach_provider_metadata_lease(el, Arc::clone(&cloud), "manual-compaction-order");
    assert_eq!(el.state.writer_epoch, lease.epoch());
    let (publisher_tx, publisher_rx) = crossbeam::channel::unbounded();
    el.compaction_publish_actor = CompactionPublishActor::new(publisher_tx, false)?;
    el.compaction_publish_result_rx = publisher_rx;
    Ok(MetadataFixture {
        cloud,
        backend,
        lease,
        release: PublisherRelease(Some(release)),
        reached: arrived,
    })
}

fn force_next_reservation_block(el: &mut EventLoop, cf_id: u32) {
    let exclusive_bound = el.state.sst_names.reserved_through[&cf_id];
    el.state.sst_names.cursor.insert(cf_id, exclusive_bound);
}

fn assigned_name(el: &EventLoop, flush_id: u64) -> Option<(String, u64)> {
    let (_, flush) = el.state.immutable_flush_by_id(flush_id)?;
    Some((flush.sst_name.clone()?, flush.sst_seq?))
}

#[derive(Debug, PartialEq, Eq)]
struct NameWorkSnapshot {
    cursor: u64,
    bound: u64,
    journal_bytes: u64,
    provider_puts: usize,
}

fn name_work_snapshot(
    el: &EventLoop,
    metadata: &MetadataFixture,
    cf_id: u32,
) -> MidgeResult<NameWorkSnapshot> {
    Ok(NameWorkSnapshot {
        cursor: el.state.sst_names.cursor[&cf_id],
        bound: el.state.sst_names.reserved_through[&cf_id],
        journal_bytes: journal_len(el)?,
        provider_puts: metadata.backend.puts.load(Ordering::Acquire),
    })
}

fn read_committed_manifest(metadata: &MetadataFixture) -> MidgeResult<crate::metadata::Manifest> {
    let directory = tempfile::tempdir()?;
    let store = metadata
        .lease
        .get_leader_store()
        .expect("real provider leader store");
    crate::runtime::cloud_startup::CloudStartupRecovery::hydrate_cloud_metadata(
        &metadata.cloud,
        store.as_ref(),
        directory.path(),
        crate::config::RecoveryPolicy::Strict,
    )?;
    crate::metadata::ManifestPersistence::load(directory.path()).map_err(MidgeError::Internal)
}

fn assert_committed_files(
    el: &EventLoop,
    metadata: &MetadataFixture,
    cf_id: u32,
    flush_name: &str,
) -> MidgeResult<()> {
    let committed = read_committed_manifest(metadata)?;
    let expected: std::collections::BTreeSet<_> = el
        .state
        .manifest
        .files
        .iter()
        .map(|file| (file.name.clone(), file.size_bytes, file.content_crc32c))
        .collect();
    let actual: std::collections::BTreeSet<_> = committed
        .files
        .iter()
        .map(|file| (file.name.clone(), file.size_bytes, file.content_crc32c))
        .collect();
    assert_eq!(
        actual, expected,
        "hydrate actual committed snapshot and journal"
    );
    assert_eq!(
        committed.next_sst_seqs, el.state.manifest.next_sst_seqs,
        "committed name reservations must survive either publication order"
    );
    assert_eq!(
        committed.last_persisted_sequence,
        el.state.manifest.last_persisted_sequence
    );
    assert_eq!(
        committed
            .files
            .iter()
            .filter(|file| file.name == flush_name)
            .count(),
        1
    );
    assert!(committed
        .files
        .iter()
        .any(|file| file.cf_id == cf_id && file.level == 1));
    Ok(())
}

#[test]
fn should_reuse_prereserved_flush_name_when_compaction_publication_wins_build_race(
) -> MidgeResult<()> {
    // Arrange: force a genuine durable name-block mirror before building.
    let (mut el, worker, _) = cloud_debt(4)?;
    el.state.sequence = el.state.manifest.last_persisted_sequence;
    let mut metadata = metadata_fixture(&mut el)?;
    let (mut compute_release, finalized) = paused_compactor::install(&mut el)?;
    let plan = el
        .compaction_actor
        .check_manual_compaction(&el.state)?
        .expect("real cloud plan");
    let cf_id = plan.cf_id;
    let inputs = plan.input_files.clone();
    el.launch_compaction(plan)?;
    finalized
        .recv_timeout(TEST_WAIT)
        .expect("compute pause after real finalization");
    el.state.set_compaction_enabled(false);
    force_next_reservation_block(&mut el, cf_id);
    let bound_before = el.state.sst_names.reserved_through[&cf_id];
    let sequence = acknowledge_put(&mut el, cf_id, 91_311)?;

    // Act: the event loop owns name reservation before the worker's result.
    let response = submit_manual_flush(&mut el, cf_id, 91_312);
    let flush_id = el.state.get_cf(cf_id).unwrap().immutable_flushes[0].flush_id;
    let assigned = assigned_name(&el, flush_id);
    let build = el
        .flush_worker_result_rx
        .recv_timeout(TEST_WAIT)
        .expect("actual flush Build");
    if assigned.is_none() {
        // A lane-only implementation must fail this control without blocking
        // its event-loop mirror behind an intentionally held publisher.
        el.handle_flush_worker_result(build);
        compute_release.release();
        settle_owned_work(&mut el, &worker);
        panic!("canonical flush identity must be durable before Build completion");
    }
    let (name, sst_seq) = assigned.expect("captured assigned identity");
    metadata.backend.armed.store(true, Ordering::Release);
    compute_release.release();
    complete_compaction(&mut el, &worker);
    metadata
        .reached
        .recv_timeout(TEST_WAIT)
        .expect("actual publisher GET held");
    let names_before = name_work_snapshot(&el, &metadata, cf_id)?;
    el.handle_flush_worker_result(build);
    let parked = el
        .state
        .immutable_flush_by_id(flush_id)
        .is_some_and(|(_, flush)| {
            flush.phase == crate::runtime::state::ImmutableFlushPhase::Queued
                && flush
                    .built
                    .as_ref()
                    .is_some_and(|built| built.reservation.is_some())
        });
    let identity_unchanged = assigned_name(&el, flush_id) == Some((name.clone(), sst_seq));
    let names_after = name_work_snapshot(&el, &metadata, cf_id)?;
    let inputs_retained = inputs
        .iter()
        .all(|input| remote_sst_path_for_test(&el, input).exists());
    let no_early_ok = response.try_recv().is_err();
    metadata.release.release();
    settle_owned_work(&mut el, &worker);

    // Assert: all actual workers were released before the safety assertions.
    assert!(names_before.bound > bound_before && sst_seq == bound_before);
    assert_eq!(names_after, names_before);
    assert!(parked && identity_unchanged && inputs_retained && no_early_ok);
    assert!(matches!(
        response.recv_timeout(TEST_WAIT),
        Ok(RuntimeResponse::Ok { request_id: 91_312 })
    ));
    assert_acknowledged_and_recovered_rows(&el, cf_id, sequence);
    assert_eq!(el.state.flush_metrics.publish_count, 1);
    assert_committed_files(&el, &metadata, cf_id, &name)
}

#[test]
fn should_defer_compaction_completion_when_manual_flush_publishes_first() -> MidgeResult<()> {
    // Arrange
    let (mut el, worker, _) = cloud_debt(4)?;
    el.state.sequence = el.state.manifest.last_persisted_sequence;
    let metadata = metadata_fixture(&mut el)?;
    let (mut compute_release, finalized) = paused_compactor::install(&mut el)?;
    let plan = el
        .compaction_actor
        .check_manual_compaction(&el.state)?
        .expect("real cloud plan");
    let cf_id = plan.cf_id;
    let inputs = plan.input_files.clone();
    el.launch_compaction(plan)?;
    finalized
        .recv_timeout(TEST_WAIT)
        .expect("real compute pause");
    el.state.set_compaction_enabled(false);
    let sequence = acknowledge_put(&mut el, cf_id, 91_313)?;

    // Act: acquire Flush publication before dispatching real compute completion.
    let response = submit_manual_flush(&mut el, cf_id, 91_314);
    let flush_id = el.state.get_cf(cf_id).unwrap().immutable_flushes[0].flush_id;
    let (name, _) = assigned_name(&el, flush_id).expect("canonical identity before build");
    let build = el
        .flush_worker_result_rx
        .recv_timeout(TEST_WAIT)
        .expect("actual Build");
    assert!(matches!(build, FlushWorkerResult::Build(_)));
    el.handle_flush_worker_result(build);
    let flush_owner = ManifestPublicationOwner::Flush { flush_id };
    let owner_before = el.publication_gate.is_owned_by(&flush_owner);
    compute_release.release();
    complete_compaction(&mut el, &worker);
    let completion_deferred = el.publication_gate.deferred_messages_len() == 1;
    let no_compaction_publication = el.compaction_publication.get().is_none();
    let inputs_retained = inputs
        .iter()
        .all(|input| remote_sst_path_for_test(&el, input).exists());
    let no_early_ok = response.try_recv().is_err();
    settle_owned_work(&mut el, &worker);

    // Assert
    assert!(
        owner_before
            && completion_deferred
            && no_compaction_publication
            && inputs_retained
            && no_early_ok
    );
    assert!(matches!(
        response.recv_timeout(TEST_WAIT),
        Ok(RuntimeResponse::Ok { request_id: 91_314 })
    ));
    assert_acknowledged_and_recovered_rows(&el, cf_id, sequence);
    assert_eq!(el.state.flush_metrics.publish_count, 1);
    assert_committed_files(&el, &metadata, cf_id, &name)
}
