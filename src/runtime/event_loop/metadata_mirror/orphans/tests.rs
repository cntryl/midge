use super::*;
use crate::runtime::event_loop::tests::create_test_cloud_event_loop_with_reserved_names;
use crate::storage::test_support::StorageBackendTestExt;
use std::time::Duration;

fn fixture() -> MidgeResult<EventLoop> {
    create_test_cloud_event_loop_with_reserved_names(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        Some(100),
    )
}

fn put(event_loop: &EventLoop, name: &str) {
    let (tx, rx) = std::sync::mpsc::channel();
    event_loop
        .actors
        .cloud_coordinator
        .hybrid_storage
        .as_ref()
        .unwrap()
        .remote_sst_backend()
        .write_for_test(
            &crate::cloud_layout::object_key(name),
            b"unpublished".to_vec(),
            tx,
        );
    assert!(matches!(
        rx.recv().unwrap(),
        crate::storage::StorageEvent::WriteComplete {
            result: crate::storage::StorageOutcome::Ok(()),
            ..
        }
    ));
}

fn exists(event_loop: &EventLoop, name: &str) -> bool {
    event_loop
        .actors
        .cloud_coordinator
        .hybrid_storage
        .as_ref()
        .unwrap()
        .remote_range_metadata_optional_within(
            &crate::cloud_layout::object_key(name),
            &OperationDeadline::from_budget(Duration::from_secs(5)),
        )
        .unwrap()
        .is_some()
}

fn run_sweep(event_loop: &mut EventLoop) -> MidgeResult<()> {
    let task = event_loop.periodic_retention_continuation();
    assert!(matches!(task, MirrorContinuation::OrphanSweep(_)));
    event_loop.pipeline.inline_flush_worker = false;
    let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
    assert!(event_loop.start_metadata_mirror(task, deadline)?);
    event_loop.drain_metadata_mirror_within(&deadline)
}

#[test]
fn should_exclude_same_epoch_prefetched_names_when_startup_floor_is_immutable() -> MidgeResult<()> {
    // Arrange: startup captured 100; maintenance subsequently publishes 116.
    let mut event_loop = fixture()?;
    event_loop.prefetch_flush_sst_names_if_due();
    assert_eq!(event_loop.state.sst_names.startup_floor[&0], 100);
    assert_eq!(event_loop.state.manifest.next_sst_seqs[&0], 116);
    let old = crate::cloud_layout::file_name(0, 1, 99);
    let admitted = crate::cloud_layout::file_name(0, 1, 105);
    put(&event_loop, &old);
    put(&event_loop, &admitted);

    // Act
    run_sweep(&mut event_loop)?;

    // Assert: the moving durable reservation can never expand orphan authority.
    assert!(!exists(&event_loop, &old));
    assert!(exists(&event_loop, &admitted));
    let stats = event_loop.state.diagnostics.counters();
    assert!(stats.cloud_orphan_list_attempts <= 1);
    assert!(stats.cloud_orphan_probes <= 8);
    assert!(stats.cloud_orphan_delete_attempts <= 8);
    Ok(())
}

#[test]
fn should_retain_every_captured_source_before_orphan_deletion() -> MidgeResult<()> {
    // Arrange
    let mut event_loop = fixture()?;
    let pinned = crate::cloud_layout::file_name(0, 1, 10);
    let intent = crate::cloud_layout::file_name(0, 1, 11);
    let input = crate::cloud_layout::file_name(0, 1, 12);
    let output = crate::cloud_layout::compaction_file_name(0, 2, 13, 0);
    let flush_name = crate::cloud_layout::file_name(0, 0, 14);
    let orphan = crate::cloud_layout::file_name(0, 1, 15);
    for name in [&pinned, &intent, &input, &output, &flush_name, &orphan] {
        put(&event_loop, name);
    }
    event_loop.state.snapshots.max_snapshot_lifetime = Duration::ZERO;
    event_loop
        .state
        .register_snapshot(943, 0, vec![pinned.clone()]);
    event_loop
        .state
        .append_intent(crate::runtime::IntentLogEntry::CompactionPlanned {
            input_files: vec![intent.clone()],
            output_level: 2,
        })?;
    let mut plan = crate::compaction::CompactionPlan::new(0, 1, 2).with_output_seq(13);
    plan.input_files = vec![input.clone()];
    event_loop
        .actors
        .cloud_coordinator
        .metadata_mirror
        .pending_compactions
        .push_back((plan, None));
    let memtable = Arc::clone(&event_loop.state.get_cf(0).unwrap().memtable);
    let tracked = event_loop
        .state
        .track_new_immutable_flush(0, memtable, 0)
        .unwrap();
    let (_, flush) = event_loop
        .state
        .immutable_flush_by_id_mut(tracked.flush_id)
        .unwrap();
    flush.sst_name = Some(flush_name.clone());
    flush.sst_seq = Some(14);
    event_loop
        .actors
        .cloud_coordinator
        .metadata_mirror
        .pending_flush = Some(tracked.flush_id);

    // Act
    run_sweep(&mut event_loop)?;

    // Assert: absence from the selected file list does not override any owner.
    for name in [&pinned, &intent, &input, &output, &flush_name] {
        assert!(exists(&event_loop, name));
    }
    assert!(!exists(&event_loop, &orphan));
    event_loop.state.unregister_snapshot(943);
    crate::runtime::IntentPersistence::save_with_fs(&event_loop.state.fs, &[])
        .map_err(MidgeError::Internal)?;
    event_loop.state.intent_log.clear();
    event_loop
        .actors
        .cloud_coordinator
        .metadata_mirror
        .pending_compactions
        .clear();
    event_loop
        .actors
        .cloud_coordinator
        .metadata_mirror
        .pending_flush = None;
    event_loop
        .state
        .immutable_flush_by_id_mut(tracked.flush_id)
        .unwrap()
        .1
        .sst_name = None;
    run_sweep(&mut event_loop)?;
    assert!(!exists(&event_loop, &pinned));
    assert!(!exists(&event_loop, &intent));
    Ok(())
}

#[test]
fn should_revisit_late_predecessor_put_without_deleting_successor_names() -> MidgeResult<()> {
    // Arrange
    let mut event_loop = fixture()?;
    let old = crate::cloud_layout::file_name(0, 1, 50);
    let successor = crate::cloud_layout::file_name(0, 1, 100);
    put(&event_loop, &old);
    put(&event_loop, &successor);
    run_sweep(&mut event_loop)?;
    assert!(!exists(&event_loop, &old));

    // Act: an already admitted predecessor mutation may finish after takeover.
    put(&event_loop, &old);
    run_sweep(&mut event_loop)?;

    // Assert: it recreates only a rediscoverable leak, never selected history.
    assert!(!exists(&event_loop, &old));
    assert!(exists(&event_loop, &successor));
    Ok(())
}

#[test]
fn should_retain_sst_candidates_without_valid_deletion_proof() -> MidgeResult<()> {
    // Arrange
    let mut event_loop = fixture()?;
    let retired = crate::cloud_layout::file_name(0, 1, 20);
    let foreign_family = crate::cloud_layout::file_name(1, 1, 21);
    for name in [&retired, &foreign_family, "unknown.sst"] {
        put(&event_loop, name);
    }
    let add = crate::metadata::ManifestEdit::AddSst(crate::metadata::FileMeta {
        name: retired.clone(),
        level: 1,
        cf_id: 0,
        sst_seq: 20,
        size_bytes: 11,
        content_crc32c: None,
        ..Default::default()
    });
    let remove = crate::metadata::ManifestEdit::RemoveSst {
        name: retired.clone(),
    };
    for edit in [&add, &remove] {
        let id = event_loop
            .state
            .manifest_store
            .append_for(crate::metadata::accounting::Origin::Administration, edit)?;
        event_loop.state.manifest.apply_edit(edit)?;
        event_loop.state.manifest.note_applied_journal_edit(id);
    }

    // Act
    run_sweep(&mut event_loop)?;

    // Assert: broad discovery cannot upgrade an uncertain original proof, an
    // unknown family boundary or an unrecognized filename into delete authority.
    for name in [&retired, &foreign_family, "unknown.sst"] {
        assert!(exists(&event_loop, name));
    }
    assert_eq!(
        event_loop
            .state
            .diagnostics
            .counters()
            .cloud_orphan_delete_attempts,
        0
    );
    Ok(())
}

#[test]
fn should_retain_orphan_when_selected_reference_bytes_cannot_be_verified() -> MidgeResult<()> {
    // Arrange
    let mut event_loop = fixture()?;
    let orphan = crate::cloud_layout::file_name(0, 1, 20);
    put(&event_loop, &orphan);
    let store = event_loop.authority.fencing.leader_store.as_ref().unwrap();
    let crate::lease::CloudMetadataHead::Committed(generation) =
        store.read_committed_metadata(Duration::from_secs(5))?
    else {
        panic!("selected references")
    };
    let cloud = event_loop
        .actors
        .cloud_coordinator
        .cloud_metadata_storage
        .as_ref()
        .unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.write_for_test(
        &generation.objects[0].object_key,
        b"unverifiable".to_vec(),
        tx,
    );
    assert!(matches!(
        rx.recv().unwrap(),
        crate::storage::StorageEvent::WriteComplete {
            result: crate::storage::StorageOutcome::Ok(()),
            ..
        }
    ));

    // Act
    run_sweep(&mut event_loop)?;

    // Assert
    assert!(exists(&event_loop, &orphan));
    assert_eq!(
        event_loop.state.diagnostics.counters().cloud_orphan_probes,
        0
    );
    Ok(())
}

#[test]
fn should_keep_unprobed_page_tail_until_later_bounded_sweep_completions() -> MidgeResult<()> {
    // Arrange
    let mut event_loop = fixture()?;
    let names = (1..=25)
        .map(|sequence| crate::cloud_layout::file_name(0, 1, sequence))
        .collect::<Vec<_>>();
    for name in &names {
        put(&event_loop, name);
    }

    // Act: a single 32-key page needs four independently owned completions.
    for _ in 0..4 {
        let before = event_loop.state.diagnostics.counters();
        run_sweep(&mut event_loop)?;
        let after = event_loop.state.diagnostics.counters();
        assert!(after.cloud_orphan_list_attempts - before.cloud_orphan_list_attempts <= 1);
        assert!(after.cloud_orphan_probes - before.cloud_orphan_probes <= 8);
        assert!(after.cloud_orphan_delete_attempts - before.cloud_orphan_delete_attempts <= 8);
    }

    // Assert
    for name in &names {
        assert!(!exists(&event_loop, name));
    }
    let stats = event_loop.state.diagnostics.counters();
    assert_eq!(stats.cloud_orphan_list_attempts, 1);
    assert_eq!(stats.cloud_orphan_delete_attempts, 25);
    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_keep_new_identity_when_an_orphan_changes_after_its_probe() -> MidgeResult<()> {
    let _guard = crate::failpoints::test_failpoint_guard();
    let _scenario = fail::FailScenario::setup();
    // Arrange
    let mut event_loop = fixture()?;
    let orphan = crate::cloud_layout::file_name(0, 1, 20);
    put(&event_loop, &orphan);
    let backend = event_loop
        .actors
        .cloud_coordinator
        .hybrid_storage
        .as_ref()
        .unwrap()
        .remote_sst_backend();
    let key = crate::cloud_layout::object_key(&orphan);
    fail::cfg_callback("midge::cloud::before_compaction_orphan_delete", move || {
        let (tx, rx) = std::sync::mpsc::channel();
        backend.write_for_test(&key, b"replaced".to_vec(), tx);
        assert!(matches!(
            rx.recv().unwrap(),
            crate::storage::StorageEvent::WriteComplete {
                result: crate::storage::StorageOutcome::Ok(()),
                ..
            }
        ));
    })
    .unwrap();

    // Act
    run_sweep(&mut event_loop)?;

    // Assert: this attempt owns only the probed version. A later inventory
    // cycle may separately prove the replacement key permanently unreachable.
    assert!(exists(&event_loop, &orphan));
    assert_eq!(
        event_loop
            .state
            .diagnostics
            .counters()
            .cloud_orphan_delete_attempts,
        1
    );
    assert_eq!(
        event_loop
            .state
            .diagnostics
            .counters()
            .cloud_orphan_retained,
        1
    );
    Ok(())
}
