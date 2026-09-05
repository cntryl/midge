use super::*;

#[test]
fn should_protect_spilled_transaction_participants_until_all_generations_publish() -> MidgeResult<()>
{
    // Arrange
    let directory = tempfile::tempdir()?;
    let setup = crate::storage::test_support::build_cloud_backed_filesystem_simulation(
        directory.path(),
        Some(1024 * 1024),
    )?;
    setup.hybrid_storage.enable_ephemeral_sst_cache(1024 * 1024);
    let mut state = RuntimeState::new(directory.path().to_path_buf(), false);
    state.wal.current_segment_id = 7;
    let secondary = state.create_cf("spilled-floor".into())?;
    let mut actor = WalActor::new(
        directory.path().join("wal"),
        DurabilityPolicy::CloudAsync,
        BatchConfig::default(),
        false,
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    actor.set_storage_budget(setup.hybrid_storage.clone());
    let mut writes = crate::runtime::transaction_spill::TransactionWriteSet::new(
        Arc::new(crate::runtime::transaction_spill::TransactionMemoryPool::new(0)),
        directory.path(),
        false,
        1,
    )
    .with_storage_budget(Some(setup.hybrid_storage.clone()));
    for cf_id in [0, secondary] {
        writes.push(crate::runtime::TransactionOp::Put {
            cf_id,
            key: Bytes::from_static(b"key"),
            value: Bytes::from_static(b"value"),
            ttl_seconds: None,
            insert_only: false,
        })?;
    }
    let source = writes.take_source();
    assert!(
        setup
            .hybrid_storage
            .budget_snapshot()
            .usage
            .transaction_spill_bytes
            > 0
    );
    let wal_path = directory
        .path()
        .join("wal")
        .join(crate::wal::ACTIVE_FILE_NAME);
    assert_eq!(
        std::fs::metadata(&wal_path)?.len(),
        0,
        "an open caller transaction has not emitted WAL frames"
    );

    // Act
    actor.append_spilled_transaction(
        &mut state,
        &source,
        SpilledTransactionAppendParams {
            request_id: 1,
            assertions: Vec::new(),
            durability_policy: Some(DurabilityPolicy::CloudAsync),
            start_sequence: 0,
            conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
        },
    )?;
    let observed =
        crate::wal::cloud_segment::inspect_bytes("active", &std::fs::read(wal_path)?).unwrap();
    state.wal.current_segment_id = 9;
    let sequence = state.sequence;
    let mut frozen = Vec::new();
    for cf_id in [0, secondary] {
        let table = state.get_cf(cf_id).unwrap().memtable.clone();
        state
            .track_new_immutable_flush(cf_id, table.clone(), sequence)
            .unwrap();
        state.get_cf_mut(cf_id).unwrap().memtable = Arc::new(crate::sst::SkipListMemtable::new());
        frozen.push((cf_id, table));
    }
    let before_publication = state.cloud_wal_recovery_floor_segment();
    state
        .complete_immutable_flush(frozen[0].0, &frozen[0].1)
        .unwrap();
    let after_first_family = state.cloud_wal_recovery_floor_segment();
    state
        .complete_immutable_flush(frozen[1].0, &frozen[1].1)
        .unwrap();

    // Assert
    assert_eq!(observed.data_records.len(), 2);
    assert_eq!(before_publication, Some(7));
    assert_eq!(after_first_family, Some(7));
    assert_eq!(state.cloud_wal_recovery_floor_segment(), Some(9));
    Ok(())
}

#[test]
fn should_keep_conservative_generation_floor_when_reconstructing_startup_memtables(
) -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let mut state = RuntimeState::new(directory.path().to_path_buf(), false);
    let mut actor = WalActor::new(
        directory.path().join("wal"),
        DurabilityPolicy::CloudAsync,
        BatchConfig::default(),
        false,
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    let prepared = prepare_put_transaction(
        &mut actor,
        &mut state,
        1,
        b"recovered",
        b"value",
        DurabilityPolicy::CloudAsync,
    )?;
    actor.append_prepared_transactions(&mut state, vec![prepared])?;
    drop(actor);
    drop(state);
    let wal_directory = directory.path().join("wal");
    std::fs::rename(
        wal_directory.join(crate::wal::ACTIVE_FILE_NAME),
        wal_directory.join(crate::wal::segment_file_name(7)),
    )?;

    // Act
    let mut recovered = RuntimeState::try_new(
        directory.path().to_path_buf(),
        false,
        crate::config::RecoveryPolicy::Strict,
    )?;
    assert_eq!(recovered.wal.current_segment_id, 8);
    recovered.wal.current_segment_id = 50;
    let table = recovered.get_cf(0).unwrap().memtable.clone();
    let generation = recovered
        .track_new_immutable_flush(0, table, recovered.sequence)
        .unwrap();

    // Assert
    assert!(recovered.wal_recovery_records_replayed > 0);
    assert_eq!(generation.first_wal_segment, Some(1));
    assert_eq!(recovered.cloud_wal_recovery_floor_segment(), Some(1));
    Ok(())
}
