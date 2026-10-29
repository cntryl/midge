use super::*;

#[test]
fn should_reconcile_catalog_effect_once_when_publication_drain_caller_times_out(
) -> crate::common::MidgeResult<()> {
    let _guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();

    // Arrange: let the real worker commit the catalog, then hold it before it
    // reports completion to the event loop.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let sequence = append_cloud_async_put(&mut el)?;
    let (segment_id, max_sequence) = el.seal_cloud_wal_without_enqueue_for_test()?;
    assert_eq!(sequence, max_sequence);
    copy_local_segment_to_remote_wal_for_test(&el, segment_id);
    el.pipeline.inline_flush_worker = false;

    let work_count = Arc::new(AtomicUsize::new(0));
    let worker_work_count = Arc::clone(&work_count);
    let (entered_tx, entered_rx) = crossbeam::channel::bounded(1);
    let (release_tx, release_rx) = crossbeam::channel::bounded(1);
    let release = ReleasePublicationWork(release_tx);
    fail::cfg_callback("midge::cloud::after_wal_publication_work", move || {
        // The first completion is verification. Hold the second completion,
        // after the authoritative catalog CAS has committed.
        if worker_work_count.fetch_add(1, Ordering::AcqRel) == 1 {
            entered_tx.send(()).expect("announce committed catalog");
            release_rx.recv().expect("release committed catalog worker");
        }
    })
    .expect("hold catalog completion after its remote effect");

    // Act: start the actual verification/CAS worker pair and reap the verify
    // result until the catalog effect is committed but not yet delivered.
    el.handle_storage_event(crate::storage::StorageEvent::CloudAck {
        segment_id,
        max_sequence,
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        el.reap_cloud_publication();
        if entered_rx.try_recv().is_ok() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "publication worker did not reach the committed catalog boundary"
        );
        std::thread::sleep(Duration::from_millis(1));
    }

    let timed_out = el.drain_cloud_publication_within(
        &crate::common::OperationDeadline::from_budget(Duration::from_millis(20)),
    );
    assert!(
        matches!(timed_out, Err(crate::common::MidgeError::Timeout(_))),
        "the drain caller must time out while the committed result remains held: {timed_out:?}"
    );

    // Assert the remote authority moved, while the local applied cut has not.
    assert_eq!(published_wal_segments_for_test(&el)?, vec![segment_id]);
    assert!(el.state.wal.frontiers.cloud_durable() < max_sequence);

    // Release the exact worker, let the event loop reconcile it, and replay a
    // duplicate completion after ownership is gone.
    release
        .0
        .try_send(())
        .expect("release publication completion");
    el.drain_cloud_publication_within(&crate::common::OperationDeadline::from_budget(
        Duration::from_secs(5),
    ))?;
    assert_eq!(el.state.wal.frontiers.cloud_durable(), max_sequence);
    let committed_frontier = el.state.wal.frontiers.cloud_durable();
    el.handle_storage_event(crate::storage::StorageEvent::CloudWalPublicationComplete {
        operation_id: 2,
        writer_epoch: el.state.writer_epoch,
    });

    assert_eq!(el.state.wal.frontiers.cloud_durable(), committed_frontier);
    assert_eq!(published_wal_segments_for_test(&el)?, vec![segment_id]);
    fail::remove("midge::cloud::after_wal_publication_work");
    scenario.teardown();
    Ok(())
}

#[test]
fn should_ignore_stale_epoch_completion_for_active_publication_operation(
) -> crate::common::MidgeResult<()> {
    // Arrange: route actual worker completion messages through a test receiver
    // so a stale-epoch message can precede the valid completion.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let cloud_fs = Arc::new(
        crate::storage::filesystem::FileSystem::new(el.state.db_path.join("cloud_store"))
            .expect("open cloud test backend"),
    );
    let cloud: Arc<dyn crate::storage::StorageBackend> = cloud_fs;
    let (storage_event_tx, storage_event_rx) = crossbeam::channel::unbounded();
    let storage = Arc::new(crate::storage::HybridStorage::new_with_event_sender(
        cloud,
        storage_event_tx,
    ));
    el.set_hybrid_storage(storage);
    el.actors.cloud_coordinator.hybrid_storage_events = Some(storage_event_rx.clone());

    let sequence = append_cloud_async_put(&mut el)?;
    let (segment_id, max_sequence) = el.seal_cloud_wal_without_enqueue_for_test()?;
    assert_eq!(sequence, max_sequence);
    copy_local_segment_to_remote_wal_for_test(&el, segment_id);
    el.pipeline.inline_flush_worker = false;
    el.handle_storage_event(crate::storage::StorageEvent::CloudAck {
        segment_id,
        max_sequence,
    });

    // Act: first accept the actual verification completion, then intercept
    // the actual publication completion after its catalog CAS has finished.
    let verify = storage_event_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("verification worker completion");
    let (verify_id, verify_epoch) = match verify {
        crate::storage::StorageEvent::CloudWalPublicationComplete {
            operation_id,
            writer_epoch,
        } => (operation_id, writer_epoch),
        other => panic!("unexpected verification event: {other:?}"),
    };
    assert_eq!(verify_id, 1);
    assert_eq!(verify_epoch, el.state.writer_epoch);
    el.handle_storage_event(crate::storage::StorageEvent::CloudWalPublicationComplete {
        operation_id: verify_id,
        writer_epoch: verify_epoch,
    });

    let publish = storage_event_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("catalog publication worker completion");
    let (publish_id, publish_epoch) = match publish {
        crate::storage::StorageEvent::CloudWalPublicationComplete {
            operation_id,
            writer_epoch,
        } => (operation_id, writer_epoch),
        other => panic!("unexpected publication event: {other:?}"),
    };
    assert_eq!(publish_id, 2);
    assert_eq!(publish_epoch, el.state.writer_epoch);
    assert_eq!(published_wal_segments_for_test(&el)?, vec![segment_id]);
    assert!(el.state.wal.frontiers.cloud_durable() < max_sequence);

    // The exact operation id is still active, but the old epoch must not own
    // or consume its ready result.
    el.handle_storage_event(crate::storage::StorageEvent::CloudWalPublicationComplete {
        operation_id: publish_id,
        writer_epoch: publish_epoch.wrapping_sub(1),
    });

    // Assert: the valid completion remains consumable and applies once.
    assert!(el.state.wal.frontiers.cloud_durable() < max_sequence);
    el.handle_storage_event(crate::storage::StorageEvent::CloudWalPublicationComplete {
        operation_id: publish_id,
        writer_epoch: publish_epoch,
    });
    assert_eq!(el.state.wal.frontiers.cloud_durable(), max_sequence);
    assert_eq!(published_wal_segments_for_test(&el)?, vec![segment_id]);
    Ok(())
}
