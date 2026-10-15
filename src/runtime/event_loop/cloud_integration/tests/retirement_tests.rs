use super::*;

#[test]
fn should_preserve_ack_when_retirement_completion_has_no_matching_admission(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    seed_cloud_prune_candidate(&mut el, 1, 10);

    // Act: a stale success must not erase published authority.
    el.handle_storage_event(crate::storage::StorageEvent::CloudWalPruneComplete {
        segment_id: 1,
        result: crate::storage::StorageOutcome::Ok(()),
    });

    // Assert
    assert_eq!(
        el.actors.cloud_coordinator.cloud_wal.acks().max_sequence(1),
        Some(10)
    );
    Ok(())
}

#[test]
fn should_preserve_wal_when_only_the_local_flush_frontier_advanced(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    seed_cloud_prune_candidate(&mut el, 1, 10);
    el.state.wal.frontiers.set_cloud_durable_for_test(10);
    set_test_flush_frontier(&mut el, 0, 10);

    // Act
    el.prune_cloud_wal_segments_covered_by_manifest();
    drain_prune_completion_for_test(&mut el);

    // Assert
    assert!(el.actors.cloud_coordinator.cloud_wal.acks().contains(1));
    assert!(remote_wal_path_for_test(&el, 1).exists());
    Ok(())
}

#[test]
fn should_preserve_wal_when_catalog_retirement_uses_a_stale_writer_epoch(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    seed_cloud_prune_candidate(&mut el, 1, 10);
    el.state.wal.frontiers.set_cloud_durable_for_test(10);
    add_valid_manifest_sst_for_test(&mut el, "stale-epoch-proof.sst", 10);
    cloud_persistence(el.actors.cloud_coordinator.hybrid_storage.as_ref().unwrap())
        .fence_cloud_wal_catalog(el.state.writer_epoch + 1)?;

    // Act
    el.prune_cloud_wal_segments_covered_by_manifest();
    drain_prune_completion_for_test(&mut el);

    // Assert
    assert!(el.actors.cloud_coordinator.cloud_wal.acks().contains(1));
    assert!(remote_wal_path_for_test(&el, 1).exists());
    Ok(())
}

#[test]
fn should_keep_retired_wal_out_of_history_when_gc_unlink_fails() -> crate::common::MidgeResult<()> {
    // Arrange: a non-empty directory takes the sealed segment's name, so its
    // unlink fails after the ledger stops naming it.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    seed_cloud_prune_candidate(&mut el, 1, 10);
    let path = el
        .state
        .host_wal_dir()
        .join(crate::wal::segment_file_name(1));
    std::fs::remove_file(&path)?;
    std::fs::create_dir_all(path.join("blocker"))?;
    el.actors.cloud_coordinator.cloud_wal.admit_prunes([1]);

    // Act
    el.handle_storage_event(crate::storage::StorageEvent::CloudWalPruneComplete {
        segment_id: 1,
        result: crate::storage::StorageOutcome::Ok(()),
    });

    // Assert: the ledger retired segment 1, so recovery never replays the
    // leftover, which remains charged until physical cleanup succeeds.
    assert_eq!(Some(el.state.wal.history.ledger().log_start()), Some(2));
    assert!(
        path.exists(),
        "the leftover waits for the next open's sweep"
    );
    assert!(!el.actors.cloud_coordinator.cloud_wal.acks().contains(1));
    Ok(())
}
