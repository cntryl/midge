use super::*;
use crate::runtime::event_loop::manifest::ManifestCoordinator;
use crate::runtime::event_loop::tests::create_test_cloud_event_loop;
use crate::runtime::event_loop::wal::{ApplyTransactionRequest, WalCoordinator};
use crate::runtime::{ConflictPolicy, RuntimeResponse, TransactionOp};
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

fn seed_compaction_name_source(event_loop: &mut EventLoop) -> MidgeResult<String> {
    use crate::sst::SstFactory as _;
    let name = crate::cloud_layout::file_name(0, 0, 1);
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
            cf_id: 0,
            level: 0,
            sst_seq: 1,
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
    let _guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    // Arrange
    let mut event_loop = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    event_loop.pipeline.inline_flush_worker = false;
    let held = HeldMirror::new("midge::metadata_mirror::before_provider");
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
    held.await_entry();
    let (_sender, messages) = crossbeam::channel::unbounded();

    // Act: no provider release is available until both local outcomes exist.
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
    held.release.try_send(()).unwrap();
    event_loop
        .drain_metadata_mirror_within(&OperationDeadline::from_budget(Duration::from_secs(5)))?;
    assert!(matches!(
        persisted.try_recv(),
        Ok(RuntimeResponse::Ok { request_id: 90701 })
    ));
    event_loop.complete_metadata_mirror(1, epoch); // duplicate is inert
    assert!(!event_loop.publication.publication_gate.is_active());
    fail::remove("midge::metadata_mirror::before_provider");
    scenario.teardown();
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
