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
    assert_eq!(el.cloud_coordinator.cloud_wal.acked().get(&1), Some(&10));
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
    assert!(el.cloud_coordinator.cloud_wal.acked().contains_key(&1));
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
    assert!(el.cloud_coordinator.cloud_wal.acked().contains_key(&1));
    assert!(remote_wal_path_for_test(&el, 1).exists());
    Ok(())
}

#[test]
fn should_release_gc_pin_when_ledger_retires_segment_whose_local_unlink_fails(
) -> crate::common::MidgeResult<()> {
    // Arrange: a non-empty directory takes the sealed segment's name, so its
    // unlink fails after the ledger stops naming it.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    seed_cloud_prune_candidate(&mut el, 1, 10);
    install_prune_gc_pins_for_test(&mut el, 1, 10);
    let path = el
        .state
        .host_wal_dir()
        .join(crate::wal::segment_file_name(1));
    std::fs::remove_file(&path)?;
    std::fs::create_dir_all(path.join("blocker"))?;
    el.cloud_coordinator.cloud_wal.prune_inflight.insert(1);

    // Act
    el.handle_storage_event(crate::storage::StorageEvent::CloudWalPruneComplete {
        segment_id: 1,
        result: crate::storage::StorageOutcome::Ok(()),
    });

    // Assert: the ledger retired segment 1, so recovery never replays the
    // leftover and its tombstone pin can go.
    assert_eq!(
        el.wal_actor
            .ledger()
            .map(crate::wal::ledger::WalLedger::log_start),
        Some(2)
    );
    assert!(
        path.exists(),
        "the leftover waits for the next open's sweep"
    );
    assert_eq!(el.wal_transition.tombstone_gc_cutoff(2), 10);
    assert!(!el.cloud_coordinator.cloud_wal.acked().contains_key(&1));
    Ok(())
}
