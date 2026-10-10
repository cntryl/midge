use super::*;

#[test]
fn should_preserve_gc_pin_when_prune_completion_has_no_matching_admission(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    seed_cloud_prune_candidate(&mut el, 1, 10);
    install_prune_gc_pins_for_test(&mut el, 1, 10);

    // Act: a stale success must not erase authority or its proof pin.
    el.handle_storage_event(crate::storage::StorageEvent::CloudWalPruneComplete {
        segment_id: 1,
        result: crate::storage::StorageOutcome::Ok(()),
    });

    // Assert
    assert_eq!(el.wal_transition.tombstone_gc_cutoff(2), 0);
    assert_eq!(
        el.cloud_coordinator.cloud_wal.acked_segments.get(&1),
        Some(&10)
    );
    Ok(())
}

#[test]
fn should_preserve_gc_pin_when_exact_sst_prune_proof_fails() -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    seed_cloud_prune_candidate(&mut el, 1, 10);
    install_prune_gc_pins_for_test(&mut el, 1, 10);
    el.state.wal.frontiers.set_cloud_durable_for_test(10);
    add_manifest_sst_for_test(&mut el, "missing-proof.sst", 10);

    // Act
    el.prune_cloud_wal_segments_covered_by_manifest();
    drain_prune_completion_for_test(&mut el);

    // Assert
    assert_eq!(el.wal_transition.tombstone_gc_cutoff(2), 0);
    assert!(el
        .cloud_coordinator
        .cloud_wal
        .acked_segments
        .contains_key(&1));
    assert!(remote_wal_path_for_test(&el, 1).exists());
    Ok(())
}

#[test]
fn should_preserve_gc_pin_when_catalog_retirement_uses_a_stale_writer_epoch(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    seed_cloud_prune_candidate(&mut el, 1, 10);
    install_prune_gc_pins_for_test(&mut el, 1, 10);
    el.state.wal.frontiers.set_cloud_durable_for_test(10);
    add_valid_manifest_sst_for_test(&mut el, "stale-epoch-proof.sst", 10);
    cloud_persistence(el.cloud_coordinator.hybrid_storage.as_ref().unwrap())
        .fence_cloud_wal_catalog(el.state.writer_epoch + 1)?;

    // Act
    el.prune_cloud_wal_segments_covered_by_manifest();
    drain_prune_completion_for_test(&mut el);

    // Assert
    assert_eq!(el.wal_transition.tombstone_gc_cutoff(2), 0);
    assert!(el
        .cloud_coordinator
        .cloud_wal
        .acked_segments
        .contains_key(&1));
    assert!(remote_wal_path_for_test(&el, 1).exists());
    Ok(())
}
