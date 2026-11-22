use super::*;
use crate::runtime::event_loop::manifest::ManifestCoordinator;
use crate::runtime::event_loop::tests::create_test_cloud_event_loop;
use crate::runtime::event_loop::wal::{ApplyTransactionRequest, WalCoordinator};
use crate::runtime::{ConflictPolicy, RuntimeResponse, TransactionOp};
use crate::storage::hybrid::policy::StorageBudgetPolicy;
use bytes::Bytes;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

struct HeldMirror {
    release: crossbeam::channel::Sender<()>,
    entered: crossbeam::channel::Receiver<()>,
}

impl HeldMirror {
    fn new(boundary: &'static str) -> Self {
        let (entered_tx, entered) = crossbeam::channel::bounded(1);
        let (release, release_rx) = crossbeam::channel::bounded(1);
        let held = AtomicBool::new(false);
        fail::cfg_callback(boundary, move || {
            if !held.swap(true, Ordering::AcqRel) {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            }
        })
        .unwrap();
        Self { release, entered }
    }

    fn await_entry(&self) {
        self.entered
            .recv_timeout(Duration::from_secs(2))
            .expect("metadata mirror reached provider boundary");
    }
}

impl Drop for HeldMirror {
    fn drop(&mut self) {
        let _ = self.release.try_send(());
    }
}

struct HeldMetadataObjectStore {
    inner: Arc<crate::storage::filesystem::FileSystem>,
    entered: crossbeam::channel::Sender<()>,
    release: crossbeam::channel::Receiver<()>,
    armed: Arc<AtomicBool>,
    held: AtomicBool,
}

impl crate::storage::StorageBackend for HeldMetadataObjectStore {
    crate::storage::forward_storage_backend!(inner;
        submit_range_read_request,
        submit_range_head_request,
        submit_metadata_read_request,
        submit_head_request,
        submit_delete_request,
    );

    fn submit_write_request(
        &self,
        request: crate::storage::StorageRequest,
        data: Vec<u8>,
        callback: crate::storage::StorageCallback,
    ) {
        if self.armed.load(Ordering::Acquire) && !self.held.swap(true, Ordering::AcqRel) {
            self.entered.send(()).expect("signal provider write");
            self.release
                .recv_timeout(Duration::from_secs(10))
                .expect("release held provider write");
        }
        self.inner.submit_write_request(request, data, callback);
    }
}

impl crate::storage::inventory::ObjectInventory for HeldMetadataObjectStore {
    fn keys_page(
        &self,
        prefix: &str,
        page: crate::storage::cloud::CloudListPageRequest,
        scope: Option<&crate::common::DeadlineScope>,
    ) -> MidgeResult<Option<crate::storage::cloud::CloudListPage>> {
        crate::storage::inventory::ObjectInventory::keys_page(&*self.inner, prefix, page, scope)
    }

    fn object_size(
        &self,
        key: &str,
        scope: Option<&crate::common::DeadlineScope>,
    ) -> MidgeResult<Option<u64>> {
        crate::storage::inventory::ObjectInventory::object_size(&*self.inner, key, scope)
    }

    fn keys_under(
        &self,
        prefix: &str,
        scope: Option<&crate::common::DeadlineScope>,
    ) -> MidgeResult<Vec<String>> {
        crate::storage::inventory::ObjectInventory::keys_under(&*self.inner, prefix, scope)
    }
}

impl HeldMetadataObjectStore {
    fn wrap(
        inner: Arc<crate::storage::filesystem::FileSystem>,
        entered: crossbeam::channel::Sender<()>,
        release: crossbeam::channel::Receiver<()>,
        armed: Arc<AtomicBool>,
    ) -> Arc<dyn crate::storage::inventory::ObjectStore> {
        Arc::new(Self {
            inner,
            entered,
            release,
            armed,
            held: AtomicBool::new(false),
        })
    }
}

fn seed_compaction_name_source(event_loop: &mut EventLoop) -> MidgeResult<String> {
    seed_manifest_sst(event_loop, 0, 1)
}

fn seed_manifest_sst(event_loop: &mut EventLoop, cf_id: u32, seq: u64) -> MidgeResult<String> {
    use crate::sst::SstFactory as _;
    let name = crate::cloud_layout::file_name(cf_id, 0, seq);
    let factory = crate::sst::FsSstFactoryIo::new(
        crate::io::subdirectory(Arc::clone(&event_loop.state.fs), "sst"),
        4096,
    );
    let mut writer = factory.create()?;
    writer.add_sorted_with_meta(
        b"key",
        Some(b"value"),
        1,
        crate::types::EntryType::Put,
        None,
    )?;
    let finalized = writer
        .finish_to_path_with_summary(std::path::Path::new(&name))?
        .unwrap();
    let summary = finalized.summary.unwrap();
    event_loop
        .state
        .manifest
        .test_mut()
        .files
        .push(crate::metadata::FileMeta {
            name: name.clone(),
            cf_id,
            level: 0,
            sst_seq: seq,
            size_bytes: finalized.size_bytes,
            content_crc32c: Some(finalized.content_crc32c),
            smallest_key: Some(summary.smallest_key),
            largest_key: Some(summary.largest_key),
            smallest_seq: Some(1),
            largest_seq: Some(1),
            key_bounds_complete: true,
            ..Default::default()
        });
    crate::metadata::ManifestPersistence::save_with_fs(
        &event_loop.state.fs,
        &event_loop.state.manifest,
    )
    .map_err(MidgeError::Internal)?;
    Ok(name)
}

#[test]
fn should_preserve_accepted_compaction_reservation_when_its_mirror_finishes_under_another_owner(
) -> MidgeResult<()> {
    let _guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    // Arrange: reserve a real source's output generation with a held mirror.
    let mut event_loop =
        crate::runtime::event_loop::tests::create_test_cloud_event_loop_with_reserved_names(
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
            Some(100),
        )?;
    let source = seed_compaction_name_source(&mut event_loop)?;
    event_loop.pipeline.inline_flush_worker = false;
    let (completed, _results) = crossbeam::channel::unbounded();
    event_loop.pipeline.worker_msg_tx = Some(completed);
    let _waiter = event_loop
        .routing
        .router
        .register(91101, "accepted compaction name");
    let deadline = OperationDeadline::from_budget(Duration::from_secs(20));
    event_loop
        .state
        .pending_compaction_waits
        .insert(91101, deadline);
    let held = HeldMirror::new("midge::metadata_mirror::before_provider");
    let mut plan = crate::compaction::CompactionPlan::new(0, 0, 1);
    plan.input_files.push(source.clone());
    plan.source_files.push(source);
    event_loop.launch_compaction(plan)?;
    held.await_entry();
    let owner = crate::runtime::event_loop::coordination::ManifestPublicationOwner::WalPrune;
    assert!(event_loop
        .publication
        .publication_gate
        .try_acquire(owner.clone()));
    // Act: mirror success cannot fail or replace the accepted generation while
    // an unrelated short local publication turn owns manifest mutation.
    held.release.try_send(()).unwrap();
    event_loop.drain_metadata_mirror_within(&deadline)?;
    let waiting = &event_loop
        .actors
        .cloud_coordinator
        .metadata_mirror
        .pending_compactions;
    assert_eq!(waiting.len(), 1);
    assert_eq!(waiting[0].0.output_seq, 100);
    assert_eq!(waiting[0].1, Some(deadline));
    assert_eq!(event_loop.state.sst_names.cursor[&0], 101);
    assert_eq!(event_loop.actors.compaction_actor.active_count(), 0);
    assert!(!event_loop.has_ready_reserved_compaction());
    event_loop.publication.publication_gate.release(&owner);
    assert!(event_loop.has_ready_reserved_compaction());
    event_loop.resume_reserved_compaction_if_ready();
    // Assert: retry uses the owned original deadline/name, spends no new cursor,
    // and starts the actual job exactly once after the short turn is released.
    assert!(event_loop
        .actors
        .cloud_coordinator
        .metadata_mirror
        .pending_compactions
        .is_empty());
    assert_eq!(
        event_loop
            .actors
            .compaction_actor
            .accepted_output_generation(0, 1)?,
        100
    );
    assert_eq!(
        event_loop
            .actors
            .compaction_actor
            .manual_deadline_for_generation(0, 1, 100)?,
        Some(deadline)
    );
    assert_eq!(event_loop.state.sst_names.cursor[&0], 101);
    fail::remove("midge::metadata_mirror::before_provider");
    scenario.teardown();
    Ok(())
}

#[test]
fn should_keep_foreground_progress_available_while_metadata_provider_is_held() -> MidgeResult<()> {
    // Arrange
    let (entered, entered_rx) = crossbeam::channel::bounded(1);
    let (release_tx, release_rx) = crossbeam::channel::bounded(1);
    let armed = Arc::new(AtomicBool::new(false));
    let store_armed = Arc::clone(&armed);
    let mut event_loop =
        crate::runtime::event_loop::tests::create_test_cloud_event_loop_with_store_factory(
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
            None,
            move |inner| HeldMetadataObjectStore::wrap(inner, entered, release_rx, store_armed),
        )?;
    armed.store(true, Ordering::Release);
    event_loop.pipeline.inline_flush_worker = false;
    let persisted = event_loop
        .routing
        .router
        .register(90701, "held manifest persist");
    let committed = event_loop
        .routing
        .router
        .register(90702, "local commit during mirror");
    ManifestCoordinator::persist(
        &mut event_loop,
        90701,
        crate::metadata::accounting::Origin::Administration,
    );
    entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("metadata mirror entered the injected provider");
    let (_sender, messages) = crossbeam::channel::unbounded();

    // Act: hold the provider request while local transaction progress continues.
    let started = Instant::now();
    WalCoordinator::apply_transaction(
        &mut event_loop,
        &messages,
        ApplyTransactionRequest {
            request_id: 90702,
            validation: None,
            ops: vec![TransactionOp::Put {
                cf_id: 0,
                key: Bytes::from_static(b"mirror-local"),
                value: Bytes::from_static(b"visible"),
                ttl_seconds: None,
                insert_only: false,
            }],
            assertions: Vec::new(),
            durability_policy: Some(crate::wal::DurabilityPolicy::BestEffort),
            start_sequence: None,
            conflict_policy: ConflictPolicy::LastWriteWins,
        },
        None,
    );
    let snapshot = event_loop.create_read_snapshot(0).unwrap();
    let value = snapshot.get(b"mirror-local", event_loop.state.applied_sequence)?;
    let elapsed = started.elapsed();

    // Assert
    assert!(matches!(
        committed.try_recv(),
        Ok(RuntimeResponse::TransactionApplied { .. })
    ));
    assert_eq!(value, Some(b"visible".to_vec()));
    assert!(
        elapsed < Duration::from_millis(100),
        "local work waited {elapsed:?}"
    );
    assert!(persisted.try_recv().is_err());
    let epoch = event_loop.state.writer_epoch;
    event_loop.complete_metadata_mirror(1, epoch); // early/forged completion
    event_loop.complete_metadata_mirror(2, epoch); // unrelated operation
    assert!(event_loop
        .actors
        .cloud_coordinator
        .metadata_mirror
        .is_running());
    release_tx.try_send(()).unwrap();
    event_loop
        .drain_metadata_mirror_within(&OperationDeadline::from_budget(Duration::from_secs(5)))?;
    assert!(matches!(
        persisted.try_recv(),
        Ok(RuntimeResponse::Ok { request_id: 90701 })
    ));
    event_loop.complete_metadata_mirror(1, epoch); // duplicate is inert
    assert!(!event_loop.publication.publication_gate.is_active());
    Ok(())
}

#[test]
fn should_retain_uncovered_names_when_a_completed_metadata_mirror_loses_authority(
) -> MidgeResult<()> {
    let _guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    // Arrange
    let mut event_loop = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    event_loop.pipeline.inline_flush_worker = false;
    let healthy = Arc::new(AtomicBool::new(true));
    event_loop.authority.fencing.lease_healthy = Some(Arc::clone(&healthy));
    let held = HeldMirror::new("midge::metadata_mirror::before_completion");
    let deadline = OperationDeadline::from_budget(Duration::from_secs(5));

    // Act: the remote operation may have committed, but its loop handoff is
    // not permitted to cover names after this writer has lost authority.
    assert!(event_loop.begin_sst_name_reservation(
        0,
        1,
        Some(&deadline),
        crate::metadata::accounting::Origin::CloudFlush
    )?);
    held.await_entry();
    healthy.store(false, Ordering::Release);
    held.release.try_send(()).unwrap();
    event_loop.drain_metadata_mirror_within(&deadline)?;
    event_loop.complete_metadata_mirror(1, event_loop.state.writer_epoch);

    // Assert
    assert!(!event_loop.state.sst_names.reserved_through.contains_key(&0));
    assert!(!event_loop.publication.publication_gate.is_active());
    fail::remove("midge::metadata_mirror::before_completion");
    scenario.teardown();
    Ok(())
}

#[test]
fn should_preserve_the_accepted_flush_while_name_reservation_is_pending() -> MidgeResult<()> {
    let _guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    // Arrange
    let mut event_loop = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    event_loop.pipeline.inline_flush_worker = false;
    event_loop.state.get_cf(0).unwrap().memtable.put_with_seq(
        b"pending-name".to_vec(),
        b"value".to_vec(),
        1,
        None,
    )?;
    let flush_id = event_loop.freeze_active_memtable(0)?.unwrap();
    let held = HeldMirror::new("midge::metadata_mirror::before_provider");

    // Act
    event_loop.schedule_next_flush_worker_during_shutdown();
    held.await_entry();
    let (_, pending) = event_loop.state.immutable_flush_by_id(flush_id).unwrap();

    // Assert: admission did not turn a provider wait into a failed flush, and
    // retry starts from the same cursor rather than allocating another block.
    assert_eq!(pending.failures, 0);
    assert_eq!(
        pending.phase,
        crate::runtime::state::ImmutableFlushPhase::Queued
    );
    assert_eq!(event_loop.state.sst_names.cursor.get(&0), Some(&1));
    held.release.try_send(()).unwrap();
    event_loop
        .drain_metadata_mirror_within(&OperationDeadline::from_budget(Duration::from_secs(5)))?;
    event_loop.reserve_sst_name_durably(0, 1)?;
    assert_eq!(event_loop.state.sst_names.cursor.get(&0), Some(&1));
    assert_eq!(event_loop.state.manifest.next_sst_seqs.get(&0), Some(&17));
    fail::remove("midge::metadata_mirror::before_provider");
    scenario.teardown();
    Ok(())
}

#[test]
fn should_reconcile_an_admitted_mirror_once_when_its_caller_abandons_the_result() -> MidgeResult<()>
{
    let _guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    // Arrange: a genuine journal edit makes this a new shared generation.
    let mut event_loop = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    event_loop.pipeline.inline_flush_worker = false;
    let edit = crate::metadata::ManifestEdit::BumpNextSstSeq {
        cf_id: 0,
        next_seq: 64,
    };
    let edit_id = event_loop
        .state
        .manifest_store
        .append_for(crate::metadata::accounting::Origin::Administration, &edit)?;
    event_loop.state.manifest.set_next_sst_seq(0, 64);
    event_loop.state.manifest.note_applied_journal_edit(edit_id);
    let store = event_loop.authority.fencing.leader_store.clone().unwrap();
    let before = store.read_committed_metadata(Duration::from_secs(5))?;
    let response = event_loop
        .routing
        .router
        .register(90703, "abandoned metadata mirror");
    let held = HeldMirror::new("midge::metadata_mirror::before_completion");
    ManifestCoordinator::persist(
        &mut event_loop,
        90703,
        crate::metadata::accounting::Origin::Administration,
    );
    held.await_entry();

    // Act: caller departure after the provider effect cannot discard its owner.
    event_loop.routing.router.cancel(90703);
    assert!(event_loop
        .actors
        .cloud_coordinator
        .metadata_mirror
        .is_running());
    event_loop.complete_metadata_mirror(1, event_loop.state.writer_epoch + 1);
    assert!(event_loop
        .actors
        .cloud_coordinator
        .metadata_mirror
        .is_running());
    held.release.try_send(()).unwrap();
    event_loop
        .drain_metadata_mirror_within(&OperationDeadline::from_budget(Duration::from_secs(5)))?;
    let after = store.read_committed_metadata(Duration::from_secs(5))?;
    event_loop.complete_metadata_mirror(1, event_loop.state.writer_epoch);

    // Assert: duplicate handoffs cannot republish or revive a cancelled route.
    assert_ne!(before, after);
    assert_eq!(
        store.read_committed_metadata(Duration::from_secs(5))?,
        after
    );
    assert!(response.try_recv().is_err());
    assert!(!event_loop.publication.publication_gate.is_active());
    fail::remove("midge::metadata_mirror::before_completion");
    scenario.teardown();
    Ok(())
}

#[test]
fn should_fail_the_original_flush_attempt_when_name_publication_loses_authority() -> MidgeResult<()>
{
    let _guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    // Arrange
    let mut event_loop = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    event_loop.pipeline.inline_flush_worker = false;
    let healthy = Arc::new(AtomicBool::new(true));
    event_loop.authority.fencing.lease_healthy = Some(Arc::clone(&healthy));
    event_loop.state.get_cf(0).unwrap().memtable.put_with_seq(
        b"fenced-name".to_vec(),
        b"value".to_vec(),
        1,
        None,
    )?;
    let flush_id = event_loop.freeze_active_memtable(0)?.unwrap();
    let held = HeldMirror::new("midge::metadata_mirror::before_provider");
    event_loop.schedule_next_flush_worker_during_shutdown();
    held.await_entry();

    // Act
    healthy.store(false, Ordering::Release);
    held.release.try_send(()).unwrap();
    event_loop
        .drain_metadata_mirror_within(&OperationDeadline::from_budget(Duration::from_secs(5)))?;

    // Assert: failure applies once to the retained immutable and leaves its
    // name uncovered. It must not become an unbounded successful Busy retry.
    let (_, flush) = event_loop.state.immutable_flush_by_id(flush_id).unwrap();
    assert_eq!(flush.failures, 1);
    assert_eq!(
        flush.phase,
        crate::runtime::state::ImmutableFlushPhase::RetryPending
    );
    assert!(!event_loop.state.sst_names.reserved_through.contains_key(&0));
    event_loop.complete_metadata_mirror(1, event_loop.state.writer_epoch);
    assert_eq!(
        event_loop
            .state
            .immutable_flush_by_id(flush_id)
            .unwrap()
            .1
            .failures,
        1
    );
    fail::remove("midge::metadata_mirror::before_provider");
    scenario.teardown();
    Ok(())
}

fn sst_is_resident(event_loop: &EventLoop, name: &str) -> bool {
    event_loop
        .state
        .fs
        .metadata(&crate::sst::fs::sst_key(name))
        .is_ok()
}

fn wait_until(what: &str, mut condition: impl FnMut() -> bool) {
    let limit = Instant::now() + Duration::from_secs(5);
    while !condition() {
        assert!(Instant::now() < limit, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn queued_reclamation(event_loop: &EventLoop) -> Vec<String> {
    let mut names = event_loop.actors.gc_actor.manifest_reclamation_snapshot();
    names.sort();
    names
}

/// Drops family A to start a held reclamation mirror, then drops `later`
/// more families while it is held.
fn assert_reclamation_mirror_releases_only_covered_names(later: usize) -> MidgeResult<()> {
    let _guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    // Arrange
    let mut event_loop = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let (_sender, messages) = crossbeam::channel::unbounded();
    let mut families = Vec::new();
    for index in 0..=later {
        let request_id = 91000 + u64::try_from(index).unwrap();
        let name = format!("mirror-drop-{index}");
        let created = event_loop.routing.router.register(request_id, "create");
        ManifestCoordinator::create_column_family(&mut event_loop, &messages, request_id, &name);
        let response = created.try_recv();
        assert!(
            matches!(response, Ok(RuntimeResponse::ColumnFamilyCreated { .. })),
            "{response:?}"
        );
        let cf_id = event_loop
            .state
            .manifest
            .get_column_family_by_name(&name)
            .unwrap()
            .id;
        families.push((cf_id, seed_manifest_sst(&mut event_loop, cf_id, 1)?));
    }
    event_loop.pipeline.inline_flush_worker = false;
    let held = HeldMirror::new("midge::metadata_mirror::before_provider");
    let deadline = OperationDeadline::from_budget(Duration::from_secs(10));

    // Act: the later families are dropped while the mirror for the first
    // family's drop owns publication.
    for (index, (cf_id, _)) in families.iter().enumerate() {
        let request_id = 91100 + u64::try_from(index).unwrap();
        let _response = event_loop.routing.router.register(request_id, "drop");
        ManifestCoordinator::drop_column_family(
            &mut event_loop,
            &messages,
            request_id,
            *cf_id,
            false,
        );
        if index == 0 {
            held.await_entry();
        }
    }
    held.release.try_send(()).unwrap();
    event_loop.drain_metadata_mirror_within(&deadline)?;

    // Assert: the first mirror's cut still lists the later families, so only
    // the first family's SST may go and their names stay queued.
    let (first, rest) = families.split_first().unwrap();
    let mut expected = rest
        .iter()
        .map(|(_, name)| name.clone())
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(queued_reclamation(&event_loop), expected);
    wait_until("the first dropped family's SST deletion", || {
        !sst_is_resident(&event_loop, &first.1)
    });
    // All names would drain in one worker batch; give it time to reach the
    // later ones before judging.
    std::thread::sleep(Duration::from_millis(300));
    for (_, name) in rest {
        assert!(
            sst_is_resident(&event_loop, name),
            "{name} was deleted although the published manifest still references it"
        );
    }

    // A later mirror whose cut includes the later drops releases them.
    wait_until("the follow-up mirror to release the later SSTs", || {
        event_loop.retry_manifest_reclamation_if_due();
        event_loop.reap_metadata_mirror();
        rest.iter()
            .all(|(_, name)| !sst_is_resident(&event_loop, name))
    });
    assert_eq!(queued_reclamation(&event_loop), Vec::<String>::new());
    scenario.teardown();
    Ok(())
}

#[test]
fn should_retain_second_dropped_family_ssts_when_first_reclamation_mirror_completes(
) -> MidgeResult<()> {
    assert_reclamation_mirror_releases_only_covered_names(1)
}

#[test]
fn should_retain_two_later_dropped_family_ssts_when_first_reclamation_mirror_completes(
) -> MidgeResult<()> {
    assert_reclamation_mirror_releases_only_covered_names(2)
}

/// Drops one family so its reclamation is queued while a held mirror owns
/// publication, and returns the event loop with that mirror still in flight.
fn queue_reclamation_behind_held_mirror(
    event_loop: &mut EventLoop,
    held: &HeldMirror,
) -> MidgeResult<String> {
    let (_sender, messages) = crossbeam::channel::unbounded();
    let created = event_loop.routing.router.register(92000, "create");
    ManifestCoordinator::create_column_family(event_loop, &messages, 92000, "busy-drop");
    assert!(matches!(
        created.try_recv(),
        Ok(RuntimeResponse::ColumnFamilyCreated { .. })
    ));
    let cf_id = event_loop
        .state
        .manifest
        .get_column_family_by_name("busy-drop")
        .unwrap()
        .id;
    let sst = seed_manifest_sst(event_loop, cf_id, 1)?;
    event_loop.pipeline.inline_flush_worker = false;
    let _response = event_loop.routing.router.register(92001, "drop");
    ManifestCoordinator::drop_column_family(event_loop, &messages, 92001, cf_id, false);
    held.await_entry();
    Ok(sst)
}

#[test]
fn should_defer_reclamation_without_anomaly_when_retry_meets_a_running_mirror() -> MidgeResult<()> {
    let _guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    // Arrange
    let mut event_loop = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let held = HeldMirror::new("midge::metadata_mirror::before_provider");
    let sst = queue_reclamation_behind_held_mirror(&mut event_loop, &held)?;
    assert!(event_loop
        .actors
        .cloud_coordinator
        .metadata_mirror
        .is_running());
    let deadline = OperationDeadline::from_budget(Duration::from_secs(10));

    // Act: the retry encounters the running mirror.
    crate::runtime::event_loop::gc::GcCoordinator::retry_within(&mut event_loop, &deadline);

    // Assert: contention is a deferral, not a persistence anomaly.
    assert!(!event_loop.state.persistence_anomaly_detected());
    assert!(event_loop.actors.gc_actor.has_manifest_reclamation());
    held.release.try_send(()).unwrap();
    event_loop.drain_metadata_mirror_within(&deadline)?;
    crate::runtime::event_loop::gc::GcCoordinator::retry_within(&mut event_loop, &deadline);
    event_loop.drain_metadata_mirror_within(&deadline)?;
    wait_until("reclamation to delete the dropped family's SST", || {
        !sst_is_resident(&event_loop, &sst)
    });
    assert_eq!(queued_reclamation(&event_loop), Vec::<String>::new());
    assert!(!event_loop.state.persistence_anomaly_detected());
    assert_eq!(
        event_loop.state.health(),
        crate::config::EngineHealth::Healthy
    );
    scenario.teardown();
    Ok(())
}

#[test]
fn should_mark_persistence_anomaly_when_reclamation_mirror_start_fails_for_a_real_reason(
) -> MidgeResult<()> {
    let _guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    // Arrange: a queued name, no mirror running, and a failing local persist.
    let mut event_loop = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let sst = seed_manifest_sst(&mut event_loop, 0, 8)?;
    event_loop.actors.gc_actor.queue_manifest_reclamation([sst]);
    let deadline = OperationDeadline::from_budget(Duration::from_secs(10));
    fail::cfg("midge::manifest::persist", "return").unwrap();

    // Act
    crate::runtime::event_loop::gc::GcCoordinator::retry_within(&mut event_loop, &deadline);

    // Assert
    assert!(event_loop.state.persistence_anomaly_detected());
    assert_eq!(
        event_loop.state.health(),
        crate::config::EngineHealth::Degraded
    );
    fail::remove("midge::manifest::persist");
    scenario.teardown();
    Ok(())
}

#[test]
fn should_defer_reclamation_without_anomaly_when_retry_meets_a_running_retention_mirror(
) -> MidgeResult<()> {
    let _guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    // Arrange: a queued reclamation name while a Retention mirror (not a
    // Reclamation one) is in flight.
    let mut event_loop = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let sst = seed_manifest_sst(&mut event_loop, 0, 7)?;
    event_loop
        .state
        .manifest
        .test_mut()
        .files
        .retain(|file| file.name != sst);
    event_loop.pipeline.inline_flush_worker = false;
    event_loop
        .actors
        .gc_actor
        .queue_manifest_reclamation([sst.clone()]);
    let held = HeldMirror::new("midge::metadata_mirror::before_provider");
    let deadline = OperationDeadline::from_budget(Duration::from_secs(10));
    assert!(event_loop.start_metadata_mirror(MirrorContinuation::Retention, deadline)?);
    held.await_entry();

    // Act
    crate::runtime::event_loop::gc::GcCoordinator::retry_within(&mut event_loop, &deadline);

    // Assert: deferred quietly with the queue and the file retained.
    assert!(!event_loop.state.persistence_anomaly_detected());
    assert_eq!(queued_reclamation(&event_loop), vec![sst.clone()]);
    assert!(sst_is_resident(&event_loop, &sst));
    held.release.try_send(()).unwrap();
    event_loop.drain_metadata_mirror_within(&deadline)?;
    crate::runtime::event_loop::gc::GcCoordinator::retry_within(&mut event_loop, &deadline);
    event_loop.drain_metadata_mirror_within(&deadline)?;
    assert_eq!(queued_reclamation(&event_loop), Vec::<String>::new());
    assert!(!event_loop.state.persistence_anomaly_detected());
    scenario.teardown();
    Ok(())
}

#[test]
fn should_mark_reclamation_anomaly_when_metadata_cut_admission_fails() -> MidgeResult<()> {
    // Arrange: retain a captured flush intent and remove it without publishing
    // the terminal receipt. No event-loop mirror owns publication.
    let mut event_loop = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let sst = seed_manifest_sst(&mut event_loop, 0, 8)?;
    event_loop
        .state
        .manifest
        .test_mut()
        .files
        .retain(|file| file.name != sst);
    let flush_name = seed_manifest_sst(&mut event_loop, 0, 9)?;
    let flush_file = event_loop
        .state
        .manifest
        .files
        .iter()
        .find(|file| file.name == flush_name)
        .unwrap();
    let flush_file = crate::runtime::FileMeta::from(flush_file);
    event_loop
        .state
        .record_flush_publication_intent(0, 9, &flush_file)?;
    crate::runtime::actors::ManifestActor::persist(&mut event_loop.state)?;
    event_loop
        .actors
        .gc_actor
        .queue_manifest_reclamation([sst.clone()]);
    event_loop.pipeline.inline_flush_worker = false;
    let deadline = OperationDeadline::from_budget(Duration::from_secs(10));
    let lock = &event_loop.publication.metadata_publication_lock;
    let cut = lock.capture_control_cut(
        &event_loop.state.fs,
        event_loop.state.writer_epoch,
        &deadline,
    )?;
    event_loop
        .state
        .clear_flush_publication_intent(&flush_name)?;
    assert!(matches!(
        lock.capture_control_cut(&event_loop.state.fs, event_loop.state.writer_epoch, &deadline),
        Err(MidgeError::Busy(message)) if message.contains("without selected ManifestPublished proof")
    ));

    // Act
    crate::runtime::event_loop::gc::GcCoordinator::retry_within(&mut event_loop, &deadline);

    // Assert
    assert!(event_loop.state.persistence_anomaly_detected());
    assert!(sst_is_resident(&event_loop, &sst));
    assert_eq!(queued_reclamation(&event_loop), vec![sst]);
    drop(cut);
    Ok(())
}

fn read_selected_manifest(event_loop: &EventLoop) -> MidgeResult<crate::metadata::Manifest> {
    let deadline = event_loop.event_loop_cloud_deadline();
    let store = event_loop.authority.fencing.leader_store.as_ref().unwrap();
    let head = store.read_committed_metadata(deadline.remaining())?;
    let crate::lease::CloudMetadataHead::Committed(generation) = head else {
        panic!("expected selected metadata generation, got {head:?}");
    };
    let cloud = event_loop
        .actors
        .cloud_coordinator
        .cloud_metadata_storage
        .as_ref()
        .unwrap();
    let bodies = crate::runtime::hybrid_persistence::read_committed_metadata_bodies_within(
        cloud,
        &generation,
        &deadline,
    )?;
    crate::metadata::ManifestPersistence::decode_bodies(
        &bodies[crate::metadata::files::MANIFEST_SNAPSHOT],
        bodies
            .get(crate::metadata::files::JOURNAL)
            .map(Vec::as_slice),
        &crate::common::DeadlineScope::new(deadline),
    )
}

fn progress_after_held_mirror(event_loop: &mut EventLoop, held: &HeldMirror) -> MidgeResult<()> {
    // A reservation would incidentally publish the DDL and mask a lost retry.
    event_loop.state.sst_names.prefetch_retry_at = Some(Instant::now() + Duration::from_secs(60));
    held.release.try_send(()).unwrap();
    let deadline = event_loop.event_loop_cloud_deadline();
    event_loop.drain_metadata_mirror_within(&deadline)?;
    event_loop.progress_pass(&crossbeam::channel::never());
    event_loop.drain_metadata_mirror_within(&deadline)
}

#[test]
fn should_publish_committed_create_when_retention_mirror_finishes() -> MidgeResult<()> {
    let _guard = crate::failpoints::test_failpoint_guard();
    let _scenario = fail::FailScenario::setup();
    // Arrange
    let mut event_loop = create_test_cloud_event_loop(StorageBudgetPolicy::default())?;
    event_loop.pipeline.inline_flush_worker = false;
    let held = HeldMirror::new("midge::metadata_mirror::before_provider");
    let deadline = event_loop.event_loop_cloud_deadline();
    assert!(event_loop.start_metadata_mirror(MirrorContinuation::Retention, deadline)?);
    held.await_entry();
    let created = event_loop
        .routing
        .router
        .register(92_010, "create during retention");

    // Act
    ManifestCoordinator::create_column_family(
        &mut event_loop,
        &crossbeam::channel::never(),
        92_010,
        "deferred-create",
    );
    assert!(matches!(
        created.try_recv(),
        Ok(RuntimeResponse::ColumnFamilyCreated { .. })
    ));
    progress_after_held_mirror(&mut event_loop, &held)?;

    // Assert
    assert!(read_selected_manifest(&event_loop)?
        .get_column_family_by_name("deferred-create")
        .is_some());
    assert_eq!(
        event_loop.state.health(),
        crate::config::EngineHealth::Healthy
    );
    Ok(())
}

#[test]
fn should_publish_committed_empty_drop_when_retention_mirror_finishes() -> MidgeResult<()> {
    let _guard = crate::failpoints::test_failpoint_guard();
    let _scenario = fail::FailScenario::setup();
    // Arrange
    let mut event_loop = create_test_cloud_event_loop(StorageBudgetPolicy::default())?;
    let messages = crossbeam::channel::never();
    let created = event_loop
        .routing
        .router
        .register(92_011, "create empty family");
    ManifestCoordinator::create_column_family(&mut event_loop, &messages, 92_011, "deferred-drop");
    let RuntimeResponse::ColumnFamilyCreated { cf_id, .. } = created.try_recv().unwrap() else {
        panic!("expected empty family to be created");
    };
    assert!(read_selected_manifest(&event_loop)?
        .get_column_family_by_name("deferred-drop")
        .is_some());
    event_loop.pipeline.inline_flush_worker = false;
    let held = HeldMirror::new("midge::metadata_mirror::before_provider");
    let deadline = event_loop.event_loop_cloud_deadline();
    assert!(event_loop.start_metadata_mirror(MirrorContinuation::Retention, deadline)?);
    held.await_entry();
    let dropped = event_loop
        .routing
        .router
        .register(92_012, "drop during retention");

    // Act
    ManifestCoordinator::drop_column_family(&mut event_loop, &messages, 92_012, cf_id, false);
    assert!(matches!(dropped.try_recv(), Ok(RuntimeResponse::Ok { .. })));
    progress_after_held_mirror(&mut event_loop, &held)?;

    // Assert
    assert!(read_selected_manifest(&event_loop)?
        .get_column_family_by_name("deferred-drop")
        .is_none());
    assert_eq!(
        event_loop.state.health(),
        crate::config::EngineHealth::Healthy
    );
    Ok(())
}

#[test]
fn should_publish_committed_create_when_shutdown_joins_publication_owner() -> MidgeResult<()> {
    // Arrange: the WAL-prune join still owns its publication token. Shutdown
    // has to reap the worker before it can capture the deferred DDL cut.
    let mut event_loop = create_test_cloud_event_loop(StorageBudgetPolicy::default())?;
    event_loop.pipeline.inline_flush_worker = false;
    let owner = crate::runtime::event_loop::coordination::ManifestPublicationOwner::WalPrune;
    assert!(event_loop.publication.publication_gate.try_acquire(owner));
    event_loop.actors.cloud_coordinator.cloud_wal_prune_worker = Some(std::thread::spawn(|| {}));
    let created = event_loop
        .routing
        .router
        .register(92_013, "create during prune");
    let shutdown = event_loop
        .routing
        .router
        .register(92_014, "shutdown after deferred create");

    // Act
    ManifestCoordinator::create_column_family(
        &mut event_loop,
        &crossbeam::channel::never(),
        92_013,
        "shutdown-create",
    );
    assert!(matches!(
        created.try_recv(),
        Ok(RuntimeResponse::ColumnFamilyCreated { .. })
    ));
    assert!(read_selected_manifest(&event_loop)?
        .get_column_family_by_name("shutdown-create")
        .is_none());
    event_loop.handle_shutdown(Some(92_014));

    // Assert
    assert!(matches!(
        shutdown.try_recv(),
        Ok(RuntimeResponse::Ok { .. })
    ));
    assert!(read_selected_manifest(&event_loop)?
        .get_column_family_by_name("shutdown-create")
        .is_some());
    Ok(())
}

#[test]
fn should_keep_healthy_when_compaction_deadline_expires_after_name_reservation() -> MidgeResult<()>
{
    let _guard = crate::failpoints::test_failpoint_guard();
    let _scenario = fail::FailScenario::setup();
    // Arrange: reserve a real output name. Another owner holds the gate
    // across mirror completion so the accepted plan cannot start compute.
    let mut event_loop =
        crate::runtime::event_loop::tests::create_test_cloud_event_loop_with_reserved_names(
            StorageBudgetPolicy::default(),
            Some(100),
        )?;
    let source = seed_compaction_name_source(&mut event_loop)?;
    event_loop.pipeline.inline_flush_worker = false;
    let request_id = 92_015;
    let waiter = event_loop
        .routing
        .router
        .register(request_id, "reserved compaction deadline");
    let deadline = OperationDeadline::from_budget(Duration::from_secs(2));
    event_loop
        .state
        .pending_compaction_waits
        .insert(request_id, deadline);
    let held = HeldMirror::new("midge::metadata_mirror::before_provider");
    let mut plan = crate::compaction::CompactionPlan::new(0, 0, 1);
    plan.input_files.push(source.clone());
    plan.source_files.push(source);
    event_loop.launch_compaction(plan)?;
    held.await_entry();
    let owner = crate::runtime::event_loop::coordination::ManifestPublicationOwner::WalPrune;
    assert!(event_loop
        .publication
        .publication_gate
        .try_acquire(owner.clone()));
    held.release.try_send(()).unwrap();
    event_loop.drain_metadata_mirror_within(&deadline)?;
    assert!(event_loop.state.sst_names.reserved_through[&0] > 100);
    std::thread::sleep(deadline.remaining());

    // Act
    event_loop.publication.publication_gate.release(&owner);
    event_loop.resume_reserved_compaction_if_ready();

    // Assert
    assert!(matches!(
        waiter.try_recv(),
        Ok(RuntimeResponse::Error {
            error: MidgeError::Timeout(_),
            ..
        })
    ));
    assert_eq!(event_loop.actors.compaction_actor.active_count(), 0);
    assert_eq!(
        event_loop.state.health(),
        crate::config::EngineHealth::Healthy
    );
    Ok(())
}
